"""Raw flow: IBGE POF — tabela 6970 (despesas por tipo, situação do domicílio).

Source: IBGE Agregados API, table 6970 (period 2018, classifications 1 + 12190).
Lands one row per (tipo_despesa, situacao_domicilio, localidade, variavel)
into ``$DB_RAW_ZONE.br_ibge_pof.tbl_6970``.
"""
from __future__ import annotations

import os

import pandas as pd
import requests
from dotenv import load_dotenv

from dados.raw.br_ibge_pof.utils import parse_json_pof
from dados.raw.utils.postgres_interactions import PostgresETL
from dados.utils.logging import get_logger

load_dotenv()

DATASET_ID = "br_ibge_pof"
ZONE = "raw"
TABLE = "tbl_6970"

URL = (
    "https://servicodados.ibge.gov.br/api/v3/agregados/6970/periodos/2018"
    "/variaveis/1201|1204?localidades=N1[all]&classificacao=1[1,2]|12190[all]"
)

COLUMNS_DDL = {
    "tipo_despesa": "VARCHAR(255)",
    "situacao_domicilio": "VARCHAR(255)",
    "localidade": "VARCHAR(255)",
    "variavel": "VARCHAR(255)",
    "unidade": "VARCHAR(255)",
    "ano": "VARCHAR(255)",
    "valor": "VARCHAR(255)",
}

log = get_logger(dataset_id=DATASET_ID, zone=ZONE)


def extract() -> pd.DataFrame:
    log.info("extract.api.start", agregado="6970")
    raw_json = requests.get(url=URL).json()
    df = parse_json_pof(raw_json)
    log.info("extract.api.done", rows=len(df))
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
