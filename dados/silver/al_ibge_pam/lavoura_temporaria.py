"""Silver flow: IBGE PAM — lavoura temporária (Amazônia Legal)."""

from __future__ import annotations

import os
from decimal import Decimal

import pandas as pd
from dotenv import load_dotenv

from dados.raw.utils.postgres_interactions import PostgresETL
from dados.silver.models.al_ibge_pam import AlIbgePamLavouraTemporaria
from dados.silver.constants.produtos import dicionario_produtos_pam_temporaria
from dados.silver.utils import currency_fix, fix_ibge_digits
from dados.utils.logging import get_logger
from dados.utils.pydantic_postgres import pydantic_to_postgres_columns

load_dotenv()

DATASET_ID = "al_ibge_pam"
ZONE = "silver"
TABLE = "lavoura_temporaria"

PK_COLS = ["id_municipio", "ano", "produto"]
METRIC_COLS = [
    "quantidade_produzida",
    "valor_producao",
    "area_plantada",
    "area_colhida",
    "rendimento_medio_producao",
]

log = get_logger(dataset_id=DATASET_ID, zone=ZONE)


def extract() -> pd.DataFrame:
    # Pivot in SQL: keeps memory bounded and preserves X/.. sentinels for fix_ibge_digits.
    query = f"""
        SELECT
            CAST(ano AS INTEGER) AS ano,
            id_municipio,
            produto,
            MAX(CASE WHEN nome_variavel = 'Quantidade produzida' THEN valor END) AS quantidade_produzida,
            MAX(CASE WHEN nome_variavel = 'Valor da produção' THEN valor END) AS valor_producao,
            MAX(CASE WHEN nome_variavel = 'Área plantada' THEN valor END) AS area_plantada,
            MAX(CASE WHEN nome_variavel = 'Área colhida' THEN valor END) AS area_colhida,
            MAX(CASE WHEN nome_variavel = 'Rendimento médio da produção' THEN valor END) AS rendimento_medio_producao
        FROM {DATASET_ID}.{TABLE}
        GROUP BY ano, id_municipio, produto
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
    df["produto"] = df["produto"].map(dicionario_produtos_pam_temporaria)

    df = fix_ibge_digits(df, METRIC_COLS, PK_COLS)

    df["valor_producao"] = df["valor_producao"].astype("float")
    df["valor_producao"] = df["valor_producao"].apply(
        lambda x: currency_fix(x) if isinstance(x, str) else x
    )
    return df[list(AlIbgePamLavouraTemporaria.model_fields.keys())]


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

    [AlIbgePamLavouraTemporaria(**r) for r in df.to_dict("records")]
    return df


def load(df: pd.DataFrame) -> None:
    columns = pydantic_to_postgres_columns(AlIbgePamLavouraTemporaria)
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
