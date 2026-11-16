"""Silver flow: RAIS — unidades produtivas do Pará por região de integração.

Tipa e valida a aterrissagem de ``$DB_RAW_ZONE.pa_rf_rais.up_rais`` (agregada no
BigQuery por ano, região de integração e subclasse CNAE) e materializa em
``$DB_SILVER_ZONE.pa_rf_rais.up_rais``.

Valores monetários ficam em reais correntes do ano de referência — a correção
pelo IPCA é responsabilidade da camada gold (``br_coeficientes_renda``).
"""

from __future__ import annotations

import os
from decimal import Decimal

import pandas as pd
from dotenv import load_dotenv

from dados.raw.utils.postgres_interactions import PostgresETL
from dados.silver.pa_rf_rais.models import PaRfRaisUpRais
from dados.utils.logging import get_logger
from dados.utils.pydantic_postgres import pydantic_to_postgres_columns

load_dotenv()

DATASET_ID = "pa_rf_rais"
ZONE = "silver"
TABLE = "up_rais"
RAW_TABLE = "up_rais"

PK_COLS = ["ano", "regiao_integracao", "cnae_2_subclasse"]

log = get_logger(dataset_id=DATASET_ID, zone=ZONE)


def extract() -> pd.DataFrame:
    query = f"""
        SELECT
            CAST(ano AS INTEGER) AS ano,
            regiao_integracao,
            cnae_2_subclasse,
            cnae_2_subclasse_descricao_subclasse,
            cnae_2_subclasse_descricao_secao,
            remuneracao_media_dezembro,
            quantidade_vinculos_ativos,
            massa_salarial
        FROM {DATASET_ID}.{RAW_TABLE}
    """
    with PostgresETL(
        host="localhost",
        database=os.getenv("DB_RAW_ZONE"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        schema=DATASET_ID,
    ) as db:
        return db.download_data(query)


def transform(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["cnae_2_subclasse"] = df["cnae_2_subclasse"].astype(str).str.strip()
    df["quantidade_vinculos_ativos"] = (
        pd.to_numeric(df["quantidade_vinculos_ativos"], errors="coerce")
        .fillna(0)
        .astype(int)
    )
    return df[list(PaRfRaisUpRais.model_fields.keys())]


def validate(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        log.error("validate.error", reason="empty_dataframe")
        raise ValueError("transform produced an empty dataframe")

    dupes = df.duplicated(subset=PK_COLS, keep=False)
    if dupes.any():
        log.error("validate.error", reason="duplicate_pk", count=int(dupes.sum()))
        raise ValueError(f"Found {int(dupes.sum())} rows duplicating PK {PK_COLS}")

    for col in ("remuneracao_media_dezembro", "massa_salarial"):
        df[col] = df[col].apply(lambda v: None if pd.isna(v) else Decimal(str(v)))
    for col in (
        "cnae_2_subclasse_descricao_subclasse",
        "cnae_2_subclasse_descricao_secao",
    ):
        df[col] = df[col].apply(lambda v: None if pd.isna(v) else str(v))

    [PaRfRaisUpRais(**r) for r in df.to_dict("records")]
    return df


def load(df: pd.DataFrame) -> None:
    columns = pydantic_to_postgres_columns(PaRfRaisUpRais)
    with PostgresETL(
        host="localhost",
        database=os.getenv("DB_SILVER_ZONE"),
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
        log.info("extract.done", rows=len(df))
        df = transform(df)
        log.info("transform.done", rows=len(df))
        df = validate(df)
        log.info("validate.done", rows=len(df))
        load(df)
        log.info("load.done", rows=len(df))
    except Exception as exc:
        log.exception("flow.error", error=str(exc))
        raise
    log.info("flow.end", rows=len(df))


if __name__ == "__main__":
    flow()
