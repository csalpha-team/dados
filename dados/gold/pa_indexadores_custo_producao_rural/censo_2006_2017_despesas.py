"""Gold flow: pa_indexadores_custo_producao_rural — Censo 2006+2017 despesas."""

from __future__ import annotations

import os

import pandas as pd
from dotenv import load_dotenv

from dados.gold.pa_indexadores_custo_producao_rural.models import (
    PaIndexadoresCustoDespesasCenso2006_2017,
)
from dados.gold.pa_indexadores_producao_rural._common import (
    coerce_decimal,
    enrich_with_regiao,
)
from dados.raw.utils.postgres_interactions import PostgresETL
from dados.utils.logging import get_logger
from dados.utils.pydantic_postgres import pydantic_to_postgres_columns

load_dotenv()

DATASET_ID = "pa_indexadores_custo_producao_rural"
ZONE = "gold"
TABLE = "censo_2006_2017_despesas"

SILVER_SCHEMA = "al_ibge_censoagro"
SILVER_TABLE_2006 = "tbl_1909_2006"
SILVER_TABLE_2017 = "tbl_6899_2017"

MODEL = PaIndexadoresCustoDespesasCenso2006_2017
NUMERIC_COLS = ["quantidade_estabelecimentos_fizeram_despesa", "valor_despesa"]

log = get_logger(dataset_id=DATASET_ID, zone=ZONE)


def extract() -> pd.DataFrame:
    query = f"""
        SELECT
            ano, id_municipio, tipo_agricultura, tipo_despesa,
            quantidade_estabelecimentos_fizeram_despesa,
            ROUND(valor_despesa::numeric, 2) AS valor_despesa
        FROM {SILVER_SCHEMA}.{SILVER_TABLE_2006}
        WHERE id_municipio LIKE '15%'
        UNION ALL
        SELECT
            ano, id_municipio, tipo_agricultura, tipo_despesa,
            quantidade_estabelecimentos_fizeram_despesa,
            ROUND(valor_despesa::numeric, 2) AS valor_despesa
        FROM {SILVER_SCHEMA}.{SILVER_TABLE_2017}
        WHERE id_municipio LIKE '15%'
    """
    with PostgresETL(
        host="localhost",
        database=os.getenv("DB_SILVER_ZONE"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        schema=SILVER_SCHEMA,
    ) as db:
        return db.download_data(query)


def transform(df: pd.DataFrame) -> pd.DataFrame:
    df = enrich_with_regiao(df)
    return df[list(MODEL.model_fields.keys())].copy()


def validate(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        log.error("validate.error", reason="empty_dataframe", table=TABLE)
        raise ValueError(f"transform produced empty dataframe for {TABLE}")
    df = coerce_decimal(df, NUMERIC_COLS)
    [MODEL(**r) for r in df.to_dict("records")]
    return df


def load(df: pd.DataFrame) -> None:
    columns = pydantic_to_postgres_columns(MODEL)
    with PostgresETL(
        host="localhost",
        database=os.getenv("DB_GOLD_ZONE"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        schema=DATASET_ID,
    ) as db:
        db.create_table(TABLE, columns, drop_if_exists=True)
        db.load_data(TABLE, df, if_exists="append")


def flow() -> None:
    log.info("flow.start", table=TABLE)
    try:
        df = extract()
        log.info("extract.done", rows=len(df), table=TABLE)
        df = transform(df)
        log.info("transform.done", rows=len(df), table=TABLE)
        df = validate(df)
        log.info("validate.done", rows=len(df), table=TABLE)
        load(df)
        log.info("load.done", rows=len(df), table=TABLE)
    except Exception as exc:
        log.exception("flow.error", error=str(exc), table=TABLE)
        raise
    log.info("flow.end", rows=len(df), table=TABLE)


if __name__ == "__main__":
    flow()
