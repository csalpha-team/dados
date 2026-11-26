"""Silver flow: IBGE PEVS — extração vegetal (Amazônia Legal).

Pivots the long-form raw landing into one row per (id_municipio, ano, produto)
with ``quantidade_produzida`` and ``valor_producao`` columns, applies the IBGE
non-numeric digit fix and currency deflator, and lands at
``$DB_SILVER_ZONE.al_ibge_pevs.produtos_extracao_vegetal``.
"""

from __future__ import annotations

import os
from decimal import Decimal
from typing import Sequence

import pandas as pd
from dotenv import load_dotenv

from dados.raw.utils.postgres_interactions import PostgresETL
from dados.silver.al_ibge_pevs.models import AlIbgePevsExtracaoVegetal
from dados.silver.constants.produtos import (
    PEVS_DENSIDADE_TON_M3,
    dicionario_produtos_pevs,
)
from dados.silver.utils import (
    currency_fix_series,
    fix_ibge_digits,
    pevs_volume_to_weight,
)
from dados.utils.batching import (
    CargaEmStaging,
    executar_em_lotes_por_ano,
    listar_anos,
)
from dados.utils.logging import get_logger
from dados.utils.parallel import validate_rows
from dados.utils.pydantic_postgres import pydantic_to_postgres_columns

load_dotenv()

DATASET_ID = "al_ibge_pevs"
ZONE = "silver"
TABLE = "produtos_extracao_vegetal"
RAW_TABLE = "produtos_extracao_vegetal"

PK_COLS = ["id_municipio", "ano", "produto"]

# Tabelas grandes: validação e carga fatiadas em threads, e o flow inteiro
# executado em lotes de anos (ver dados/utils/batching.py), para que o pico
# de memória seja o de um lote e não o da tabela inteira.
LOAD_MAX_WORKERS = 4
ANOS_POR_LOTE = 5

COLUMN_RENAME = {
    "Quantidade produzida na extração vegetal": "quantidade_produzida",
    "Valor da produção na extração vegetal": "valor_producao",
}

log = get_logger(dataset_id=DATASET_ID, zone=ZONE)


def extract(db: PostgresETL, anos: Sequence[int]) -> pd.DataFrame:
    # Pivot in SQL: the raw landing is ~3M rows long-format; reshaping in
    # pandas via pivot_table OOMs on small workers. Conditional aggregation
    # in Postgres keeps the X/.. sentinels intact for fix_ibge_digits downstream.
    query = f"""
        SELECT
            CAST(ano AS INTEGER) AS ano,
            id_municipio,
            id_produto,
            produto,
            MAX(CASE WHEN nome_variavel = 'Quantidade produzida na extração vegetal'
                     THEN valor END) AS quantidade_produzida,
            MAX(CASE WHEN nome_variavel = 'Valor da produção na extração vegetal'
                     THEN valor END) AS valor_producao,
            MAX(CASE WHEN nome_variavel = 'Quantidade produzida na extração vegetal'
                     THEN unidade_medida END) AS unidade_medida
        FROM {DATASET_ID}.{RAW_TABLE}
        WHERE CAST(ano AS INTEGER) = ANY(%s)
        GROUP BY ano, id_municipio, id_produto, produto
    """
    return db.download_data(query, params=(list(anos),))


def transform(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # Mapeia pelo id da categoria (classificação 193), não pelo rótulo: o IBGE
    # renumera os rótulos ao inserir produtos novos e o nome deixa de casar.
    df["produto"] = df["id_produto"].astype(str).map(dicionario_produtos_pevs)

    nao_mapeados = df.loc[df["produto"].isna(), "id_produto"].astype(str).unique()
    if len(nao_mapeados):
        log.error(
            "transform.error",
            reason="produto_sem_mapeamento",
            ids=sorted(nao_mapeados.tolist()),
        )
        raise ValueError(
            "Produtos da PEVS sem entrada em dicionario_produtos_pevs "
            f"(ids da classificação 193): {sorted(nao_mapeados.tolist())}. "
            "Atualize dados/silver/constants/produtos.py."
        )

    df = fix_ibge_digits(df, ["quantidade_produzida", "valor_producao"], PK_COLS)

    # Normaliza produtos madeireiros (m³) para toneladas; demais (t) e contagem
    # ('Mil árvores') ficam intactos. Dirigido pela unidade vinda dos metadados.
    df = pevs_volume_to_weight(df, PEVS_DENSIDADE_TON_M3)

    # currency_fix_series é vetorizada: deflaciona as moedas históricas
    # (Cruzeiros/Cruzados/Cruzados Novos/Cruzeiros Reais) para a base atual (Mil Reais).
    df["valor_producao"] = currency_fix_series(df["ano"], df["valor_producao"])

    return df[list(AlIbgePevsExtracaoVegetal.model_fields.keys())]


def validate(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        log.error("validate.error", reason="empty_dataframe")
        raise ValueError("transform produced an empty dataframe")

    dupes = df.duplicated(subset=PK_COLS, keep=False)
    if dupes.any():
        log.error("validate.error", reason="duplicate_pk", count=int(dupes.sum()))
        raise ValueError(f"Found {int(dupes.sum())} rows duplicating PK {PK_COLS}")

    for col in ("quantidade_produzida", "valor_producao"):
        df[col] = df[col].apply(lambda v: None if pd.isna(v) else Decimal(str(v)))

    # Produtos sem unidade nos metadados (ex. 'total') vêm como NaN/''/'NaN' → None.
    df["unidade_medida"] = df["unidade_medida"].apply(
        lambda v: None if pd.isna(v) or str(v).strip() in ("", "NaN", "nan") else v
    )

    validate_rows(df, AlIbgePevsExtracaoVegetal, max_workers=LOAD_MAX_WORKERS)
    return df


def flow() -> None:
    log.info("flow.start", table=TABLE, anos_por_lote=ANOS_POR_LOTE)
    try:
        with (
            PostgresETL(
                host="localhost",
                database=os.getenv("DB_RAW_ZONE"),
                user=os.getenv("POSTGRES_USER"),
                password=os.getenv("POSTGRES_PASSWORD"),
                schema=DATASET_ID,
            ) as origem,
            PostgresETL(
                host="localhost",
                database=os.getenv("DB_SILVER_ZONE"),
                user=os.getenv("POSTGRES_USER"),
                password=os.getenv("POSTGRES_PASSWORD"),
                schema=DATASET_ID,
            ) as destino,
        ):
            anos = listar_anos(origem, RAW_TABLE)
            log.info("extract.anos", anos=len(anos), de=min(anos), ate=max(anos))

            columns = pydantic_to_postgres_columns(AlIbgePevsExtracaoVegetal)
            with CargaEmStaging(
                destino, TABLE, columns, max_workers=LOAD_MAX_WORKERS
            ) as carga:
                total = executar_em_lotes_por_ano(
                    anos=anos,
                    extrair=lambda lote: extract(origem, lote),
                    transformar=transform,
                    validar=validate,
                    carga=carga,
                    log=log,
                    anos_por_lote=ANOS_POR_LOTE,
                )
            log.info("load.done", rows=total)
    except Exception as exc:
        log.exception("flow.error", error=str(exc))
        raise
    log.info("flow.end", rows=total)


if __name__ == "__main__":
    flow()
