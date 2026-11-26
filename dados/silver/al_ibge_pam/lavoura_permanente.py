"""Silver flow: IBGE PAM — lavoura permanente (Amazônia Legal).

Pivots the long-form raw landing into one row per (id_municipio, ano, produto)
with one column per indicator, applies the IBGE non-numeric digit fix and the
currency deflator, then lands at ``$DB_SILVER_ZONE.al_ibge_pam.lavoura_permanente``.
"""

from __future__ import annotations

import os
from decimal import Decimal
from typing import Sequence

import pandas as pd
from dotenv import load_dotenv

from dados.raw.utils.postgres_interactions import PostgresETL
from dados.silver.al_ibge_pam.models import AlIbgePamLavouraPermanente
from dados.silver.constants.produtos import dicionario_produtos_pam_permanente
from dados.silver.utils import (
    currency_fix_series,
    fix_ibge_digits,
    products_weight_ratio_fix,
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

DATASET_ID = "al_ibge_pam"
ZONE = "silver"
TABLE = "lavoura_permanente"

PK_COLS = ["id_municipio", "ano", "produto"]

# Tabelas grandes: validação e carga fatiadas em threads, e o flow inteiro
# executado em lotes de anos (ver dados/utils/batching.py). Sem isso o pico de
# memória desta tabela passa de 5 GB — a conversão das colunas métricas para
# ``Decimal`` cria um objeto Python por célula.
LOAD_MAX_WORKERS = 4
ANOS_POR_LOTE = 5
METRIC_COLS = [
    "quantidade_produzida",
    "valor_producao",
    "area_destinada_colheita",
    "area_colhida",
    "rendimento_medio_producao",
]

log = get_logger(dataset_id=DATASET_ID, zone=ZONE)


def extract(db: PostgresETL, anos: Sequence[int]) -> pd.DataFrame:
    """Pivota no Postgres os anos do lote.

    Pivot in SQL: keeps memory bounded and preserves X/.. sentinels for
    fix_ibge_digits. O filtro por ano é o que mantém o lote pequeno.
    """
    query = f"""
        SELECT
            CAST(ano AS INTEGER) AS ano,
            id_municipio,
            produto,
            MAX(CASE WHEN nome_variavel = 'Quantidade produzida' THEN valor END) AS quantidade_produzida,
            MAX(CASE WHEN nome_variavel = 'Valor da produção' THEN valor END) AS valor_producao,
            MAX(CASE WHEN nome_variavel = 'Área destinada à colheita' THEN valor END) AS area_destinada_colheita,
            MAX(CASE WHEN nome_variavel = 'Área colhida' THEN valor END) AS area_colhida,
            MAX(CASE WHEN nome_variavel = 'Rendimento médio da produção' THEN valor END) AS rendimento_medio_producao,
            MAX(CASE WHEN nome_variavel = 'Quantidade produzida' THEN unidade_medida END) AS unidade_medida
        FROM {DATASET_ID}.{TABLE}
        WHERE CAST(ano AS INTEGER) = ANY(%s)
        GROUP BY ano, id_municipio, produto
    """
    return db.download_data(query, params=(list(anos),))


def transform(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df = fix_ibge_digits(df, METRIC_COLS, PK_COLS)

    # Conversão temporal das frutíferas (pré-2001): IBGE informava abacate, banana,
    # caqui, laranja… em mil frutos (banana em mil cachos) com rendimento em frutos/ha;
    # de 2001 em diante passaram a toneladas e kg/ha. products_weight_ratio_fix usa os
    # nomes ORIGINAIS do IBGE (DICIONARIO_DE_PROPORCOES), por isso roda ANTES do map de
    # padronização. Exige quantidade_produzida e area_colhida numéricas.
    df[["quantidade_produzida", "area_colhida"]] = df[
        ["quantidade_produzida", "area_colhida"]
    ].apply(pd.to_numeric, errors="coerce")
    df = df.apply(products_weight_ratio_fix, axis=1)

    produto_original = df["produto"]
    df["produto"] = produto_original.map(dicionario_produtos_pam_permanente)

    nao_mapeados = produto_original.loc[df["produto"].isna()].unique()
    if len(nao_mapeados):
        log.error(
            "transform.error",
            reason="produto_sem_mapeamento",
            produtos=sorted(nao_mapeados.tolist()),
        )
        raise ValueError(
            "Produtos da PAM (lavoura permanente) sem entrada em dicionario_produtos_pam_permanente: "
            f"{sorted(nao_mapeados.tolist())}. "
            "Atualize dados/silver/constants/produtos.py."
        )

    # currency_fix_series é vetorizada: deflaciona as moedas históricas
    # (Cruzeiros/Cruzados/Cruzados Novos/Cruzeiros Reais) para a base atual (Mil Reais).
    df["valor_producao"] = currency_fix_series(df["ano"], df["valor_producao"])
    return df[list(AlIbgePamLavouraPermanente.model_fields.keys())]


def validate(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        log.error("validate.error", reason="empty_dataframe")
        raise ValueError("transform produced an empty dataframe")

    dupes = df.duplicated(subset=PK_COLS, keep=False)
    if dupes.any():
        log.error("validate.error", reason="duplicate_pk", count=int(dupes.sum()))
        raise ValueError(f"Found {int(dupes.sum())} rows duplicating PK {PK_COLS}")

    for col in METRIC_COLS:
        df[col] = df[col].apply(lambda v: None if pd.isna(v) else Decimal(str(v)))

    df["unidade_medida"] = df["unidade_medida"].apply(
        lambda v: None if pd.isna(v) or str(v).strip() in ("", "NaN", "nan") else v
    )

    validate_rows(df, AlIbgePamLavouraPermanente, max_workers=LOAD_MAX_WORKERS)
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
            anos = listar_anos(origem, TABLE)
            log.info("extract.anos", anos=len(anos), de=min(anos), ate=max(anos))

            columns = pydantic_to_postgres_columns(AlIbgePamLavouraPermanente)
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
