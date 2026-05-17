"""Raw flow: BR diretorios — Nomenclatura Comum do Mercosul (NCM).

Source: BigQuery ``basedosdados.br_bd_diretorios_mundo.nomenclatura_comum_mercosul``.
Lands into ``$DB_RAW_ZONE.br_csalpha_diretorios_brasil.nomenclatura_comum_mercosul``.
"""

from __future__ import annotations

import os

import basedosdados as bd
import pandas as pd
from dotenv import load_dotenv

from dados.raw.utils.postgres_interactions import PostgresETL
from dados.utils.logging import get_logger

load_dotenv()

DATASET_ID = "br_csalpha_diretorios_brasil"
ZONE = "raw"
TABLE = "nomenclatura_comum_mercosul"

QUERY = """
select *
FROM basedosdados.br_bd_diretorios_mundo.nomenclatura_comum_mercosul;
"""

COLUMNS_DDL = {
    "id_ncm": "VARCHAR(256)",
    "id_unidade": "VARCHAR(256)",
    "id_sh6": "VARCHAR(256)",
    "id_ppe": "VARCHAR(256)",
    "id_ppi": "VARCHAR(256)",
    "id_fator_agregado_ncm": "VARCHAR(256)",
    "id_cgce_n3": "VARCHAR(256)",
    "id_isic_classe": "VARCHAR(256)",
    "id_siit": "VARCHAR(256)",
    "id_cuci_item": "VARCHAR(256)",
    "nome_unidade": "TEXT",
    "nome_ncm_portugues": "TEXT",
    "nome_ncm_espanhol": "TEXT",
    "nome_ncm_ingles": "TEXT",
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
        df = extract()
        log.info("extract.done", rows=len(df))
        df = validate(df)
        log.info("validate.done", rows=len(df))
        df = transform(df)
        log.info("transform.done", rows=len(df))
        load(df)
        log.info("load.done", rows=len(df))
    except Exception as exc:
        log.exception("flow.error", error=str(exc))
        raise
    log.info("flow.end", rows=len(df))


if __name__ == "__main__":
    flow()
