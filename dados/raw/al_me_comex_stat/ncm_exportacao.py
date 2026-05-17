"""Raw flow: ME Comex Stat — NCM exportação (Pará subset).

Source: BigQuery ``basedosdados.br_me_comex_stat.ncm_exportacao``.
Lands into ``$DB_RAW_ZONE.al_me_comex_stat.ncm_exportacao``.
"""
from __future__ import annotations

import os

import basedosdados as bd
import pandas as pd
from dotenv import load_dotenv

from dados.raw.utils.postgres_interactions import PostgresETL
from dados.utils.logging import get_logger

load_dotenv()

DATASET_ID = "al_me_comex_stat"
ZONE = "raw"
TABLE = "ncm_exportacao"

QUERY = """
select *
from basedosdados.br_me_comex_stat.ncm_exportacao
where
  sigla_uf_ncm = 'PA' and
  id_ncm IN ('11062000', '19030000')
  and ano = 2023;
"""

COLUMNS_DDL = {
    "ano": "INTEGER",
    "mes": "INTEGER",
    "id_ncm": "VARCHAR(255)",
    "id_unidade": "VARCHAR(255)",
    "id_pais": "VARCHAR(255)",
    "sigla_pais_iso3": "VARCHAR(3)",
    "sigla_uf_ncm": "VARCHAR(2)",
    "id_via": "VARCHAR(255)",
    "id_urf": "VARCHAR(255)",
    "quantidade_estatistica": "NUMERIC",
    "peso_liquido_kg": "NUMERIC",
    "valor_fob_dolar": "NUMERIC",
}

log = get_logger(dataset_id=DATASET_ID, zone=ZONE)


def extract() -> pd.DataFrame:
    billing_id = os.getenv("BASEDOSDADADOS_PROJECT_ID")
    log.info("extract.bq.start")
    df = bd.read_sql(query=QUERY, billing_project_id=billing_id)
    log.info("extract.bq.done", rows=len(df))
    return df


def validate(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        log.error("validate.error", reason="empty_dataframe")
        raise ValueError("extract produced an empty dataframe")
    missing = set(COLUMNS_DDL.keys()) - set(df.columns)
    if missing:
        log.error("validate.error", missing_columns=sorted(missing))
        raise ValueError(f"Missing required columns: {sorted(missing)}")
    return df


def transform(df: pd.DataFrame) -> pd.DataFrame:
    return df[list(COLUMNS_DDL.keys())]


def load(df: pd.DataFrame) -> None:
    with PostgresETL(
        host="localhost",
        database=os.getenv("DB_RAW_ZONE"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        schema=DATASET_ID,
    ) as db:
        db.create_table(TABLE, COLUMNS_DDL, if_not_exists=True)
        db.load_data(TABLE, df, if_exists="replace")


def flow() -> None:
    log.info("flow.start", table=TABLE)
    try:
        df = extract();    log.info("extract.done", rows=len(df))
        df = validate(df); log.info("validate.done", rows=len(df))
        df = transform(df);log.info("transform.done", rows=len(df))
        load(df);          log.info("load.done", rows=len(df))
    except Exception as exc:
        log.exception("flow.error", error=str(exc))
        raise
    log.info("flow.end", rows=len(df))


if __name__ == "__main__":
    flow()
