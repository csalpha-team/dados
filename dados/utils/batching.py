"""Execução de flows silver grandes em lotes por ano.

Os flows de PAM, PEVS e PPM foram escritos como um passo único sobre a tabela
inteira: ``extract`` traz todos os anos de uma vez, ``transform`` copia o
dataframe, ``validate`` converte as colunas métricas para ``Decimal`` (um objeto
Python por célula) e só então ``load`` insere. Na PAM isso chega a ~5,7 GB de
pico — o suficiente para o processo ser morto por OOM em qualquer re-execução.

Aqui o mesmo trabalho é feito por fatia de anos. O pico passa a ser o tamanho de
um lote, não o da tabela.

Por que ``ano`` é chave de partição segura nesses datasets:

* ``fix_ibge_digits``/``fix_ibge_x_digit`` imputam o sentinela ``X`` pela média
  do grupo UF × (``ano``, ``produto``) — o grupo nunca cruza anos;
* ``products_weight_ratio_fix`` e ``currency_fix_series`` são linha a linha
  (dependem do ``ano`` da própria linha);
* o PK dessas tabelas inclui ``ano``, então a checagem de duplicidade por lote
  é tão forte quanto a checagem global.

Um flow que particione por ano precisa satisfazer essas três condições. Se uma
transformação passar a depender de outros anos (média móvel, deflator
encadeado, interpolação temporal), esta estratégia deixa de valer.

A carga vai para uma tabela de staging e só no fim é renomeada para o destino,
dentro de uma transação. O caminho antigo — ``create_table(drop_if_exists=True)``
seguido de ``append`` — deixava a tabela destino truncada ou pela metade quando
um lote falhava; aqui a tabela publicada só muda se todos os lotes passarem.
"""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Sequence

import pandas as pd

from dados.raw.utils.postgres_interactions import PostgresETL

DEFAULT_ANOS_POR_LOTE = 5


def listar_anos(
    db: PostgresETL,
    table: str,
    *,
    schema: str | None = None,
    coluna_ano: str = "ano",
) -> List[int]:
    """Anos distintos presentes na tabela de origem, em ordem crescente."""
    esquema = schema or db.schema
    df = db.download_data(
        f"SELECT DISTINCT CAST({coluna_ano} AS INTEGER) AS ano "
        f"FROM {esquema}.{table} ORDER BY ano"
    )
    return [int(ano) for ano in df["ano"].tolist()]


def agrupar_anos(anos: Sequence[int], anos_por_lote: int) -> List[List[int]]:
    """Fatia a lista de anos em lotes de tamanho fixo."""
    if anos_por_lote < 1:
        raise ValueError("anos_por_lote precisa ser >= 1")
    return [
        list(anos[i : i + anos_por_lote]) for i in range(0, len(anos), anos_por_lote)
    ]


class CargaEmStaging:
    """Acumula lotes numa tabela de staging e publica o destino no fim.

    Uso::

        with CargaEmStaging(db, TABLE, columns) as carga:
            for lote in lotes:
                carga.append(df_lote)
            carga.publicar()

    Sair do contexto sem chamar ``publicar`` descarta o staging e deixa a tabela
    destino intacta.
    """

    def __init__(
        self,
        db: PostgresETL,
        table: str,
        columns: Dict[str, str],
        *,
        max_workers: int = 1,
        chunk_size: int = 35_399,
        sufixo: str = "__stg",
    ) -> None:
        self.db = db
        self.table = table
        self.columns = columns
        self.max_workers = max_workers
        self.chunk_size = chunk_size
        self.staging = f"{table}{sufixo}"
        if len(self.staging) > 63:
            raise ValueError(
                f"nome da tabela de staging excede 63 caracteres: {self.staging}"
            )
        self.linhas = 0
        self._publicado = False

    def __enter__(self) -> "CargaEmStaging":
        self.db.create_table(self.staging, self.columns, drop_if_exists=True)
        return self

    def append(self, df: pd.DataFrame) -> int:
        self.db.load_data(
            self.staging,
            df,
            if_exists="append",
            chunk_size=self.chunk_size,
            max_workers=self.max_workers,
        )
        self.linhas += len(df)
        return self.linhas

    def publicar(self) -> int:
        """Troca a tabela destino pela de staging numa única transação."""
        esquema = self.db.schema
        self.db.execute_query(
            f"DROP TABLE IF EXISTS {esquema}.{self.table}; "
            f"ALTER TABLE {esquema}.{self.staging} RENAME TO {self.table};",
            commit=True,
        )
        self._publicado = True
        return self.linhas

    def descartar(self) -> None:
        self.db.execute_query(
            f"DROP TABLE IF EXISTS {self.db.schema}.{self.staging}", commit=True
        )

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if not self._publicado:
            self.descartar()


def executar_em_lotes_por_ano(
    *,
    anos: Sequence[int],
    extrair: Callable[[Sequence[int]], pd.DataFrame],
    transformar: Callable[[pd.DataFrame], pd.DataFrame],
    validar: Callable[[pd.DataFrame], pd.DataFrame],
    carga: CargaEmStaging,
    log: Any,
    anos_por_lote: int = DEFAULT_ANOS_POR_LOTE,
) -> int:
    """Roda extract→transform→validate→load lote a lote e publica no fim.

    Devolve o total de linhas carregadas. Levanta a primeira exceção encontrada,
    deixando a tabela destino como estava.
    """
    lotes = agrupar_anos(anos, anos_por_lote)
    total = 0

    for numero, lote in enumerate(lotes, start=1):
        rotulo = f"{lote[0]}-{lote[-1]}" if len(lote) > 1 else str(lote[0])
        log.info("lote.start", lote=numero, de=len(lotes), anos=rotulo)

        df = extrair(lote)
        if df.empty:
            log.info("lote.vazio", lote=numero, anos=rotulo)
            continue

        df = transformar(df)
        df = validar(df)
        carga.append(df)
        total += len(df)

        log.info(
            "lote.done",
            lote=numero,
            de=len(lotes),
            anos=rotulo,
            linhas=len(df),
            acumulado=total,
        )
        del df

    if total == 0:
        raise ValueError("nenhum lote produziu linhas — origem vazia?")

    carga.publicar()
    return total
