"""Raw flow: IBGE POF — tabela 6715, ano 2018.

Source: IBGE Agregados API, table 6715, period 2018, classification 12190.
Lands one row per (UF, categoria) into ``$DB_RAW_ZONE.br_ibge_pof.tbl_6715_2018``.
"""

from __future__ import annotations

import os

import pandas as pd
import requests
from dotenv import load_dotenv

from dados.raw.utils.postgres_interactions import PostgresETL
from dados.utils.logging import get_logger

load_dotenv()

DATASET_ID = "br_ibge_pof"
ZONE = "raw"
TABLE = "tbl_6715_2018"

URL = (
    "https://servicodados.ibge.gov.br/api/v3/agregados/6715/periodos/2018"
    "/variaveis/1201|1204?localidades=N3[11,12,13,14,15,16,17,21,51]"
    "&classificacao=12190[all]"
)

# TODO: The original script (2018_tbl_6715.py) only issued the GET request and
# never landed data — no compatible parser util exists for this table shape.
# Wire a parse_* helper in `dados/raw/br_ibge_pof/utils.py` and update
# COLUMNS_DDL before running this flow.
COLUMNS_DDL: dict[str, str] = {
    "id_variavel": "VARCHAR(255)",
    "nome_variavel": "VARCHAR(255)",
    "unidade_medida": "VARCHAR(255)",
    "classificacao_nome": "VARCHAR(255)",
    "id_categoria": "VARCHAR(255)",
    "nome_categoria": "VARCHAR(255)",
    "id_localidade": "VARCHAR(255)",
    "nome_localidade": "VARCHAR(255)",
    "nivel_nome": "VARCHAR(255)",
    "ano": "VARCHAR(255)",
    "valor": "VARCHAR(255)",
}

log = get_logger(dataset_id=DATASET_ID, zone=ZONE)


def extract() -> pd.DataFrame:
    log.info("extract.api.start", agregado="6715", periodo="2018")
    _ = requests.get(url=URL).json()
    # TODO: missing parser — original script left the JSON unparsed.
    log.error("extract.todo", reason="no parser available for tbl_6715")
    raise NotImplementedError(
        "tbl_6715_2018: original script never parsed/landed data. "
        "Implement a parser in dados/raw/br_ibge_pof/utils.py before running."
    )


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
