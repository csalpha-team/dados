"""Raw flow: BR diretorios — CNAE 2.0 bioeconomia subset.

Source: BigQuery ``basedosdados.br_bd_diretorios_brasil.cnae_2``.
Lands into ``$DB_RAW_ZONE.br_csalpha_diretorios_brasil.cnae_2``.
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
TABLE = "cnae_2"

QUERY = """
select *
FROM basedosdados.br_bd_diretorios_brasil.cnae_2
where subclasse IN  (
    '1063500', '0112102', '0112199', '0116499', '0119906', '1065101',
    '0119906', '1041400', '0133401', '4729699', '0220999', '0220903',
    '0220904', '0220905', '1031700', '1032599', '1033301', '1042200',
    '4633801', '4637199', '0135100', '4623105', '1093701', '1033302',
    '4637103', '0142300', '3839401', '4637106', '1053800', '0311601',
    '0311602', '0312401', '0312402', '1020101', '0142300', '0141502',
    '0159801', '0210199', '0210105', '0210106', '0210199', '0220901',
    '0230600', '1069400', '1095300', '1122499', '1312000', '1322700',
    '2029100', '2040100', '3101200', '4634603', '4635499', '4637104',
    '4637106', '4671100', '4722902'
    ) or
    classe IN (
    '01199', '01334', '01393', '02209', '03116', '03124',
    '03213', '03221', '10201', '11119', '16102', '46320'
    );
"""

COLUMNS_DDL = {
    "subclasse": "VARCHAR(256)",
    "descricao_subclasse": "TEXT",
    "classe": "VARCHAR(256)",
    "descricao_classe": "TEXT",
    "grupo": "VARCHAR(256)",
    "descricao_grupo": "TEXT",
    "divisao": "VARCHAR(256)",
    "descricao_divisao": "TEXT",
    "secao": "VARCHAR(256)",
    "descricao_secao": "TEXT",
    "indicador_cnae_2_0": "INTEGER",
    "indicador_cnae_2_1": "INTEGER",
    "indicador_cnae_2_2": "INTEGER",
    "indicador_cnae_2_3": "INTEGER",
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
