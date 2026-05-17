"""Raw flow: IBGE PIA — tabela 1988 (Pesquisa Industrial Anual, UF, série antiga).

Source: IBGE Agregados API, table 1988 (1996-2007, CNAE 11939).
Lands one row per (UF, categoria, ano, variavel) into
``$DB_RAW_ZONE.br_ibge_pia.tbl_1988``.
"""
from __future__ import annotations

import os

import pandas as pd
from dotenv import load_dotenv

from dados.raw.constants.geografia import UF_ID_SIGLA
from dados.raw.br_ibge_pia.utils import download_json, parse_pia_json_to_table
from dados.raw.utils.postgres_interactions import PostgresETL
from dados.utils.logging import get_logger

load_dotenv()

DATASET_ID = "br_ibge_pia"
ZONE = "raw"
TABLE = "tbl_1988"

URL = (
    "https://servicodados.ibge.gov.br/api/v3/agregados/1988/periodos/"
    "1996|1997|1998|1999|2000|2001|2002|2003|2004|2005|2006|2007"
    "/variaveis/706|631|673|834|835|836|837|838|839|840|810|811"
    "?localidades=N3[{}]&classificacao=11939[all]"
)

COLUMNS_DDL = {
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
    log.info("extract.api.start", agregado="1988")
    raw_jsons = download_json(URL, UF_ID_SIGLA)
    parsed = [parse_pia_json_to_table(j) for j in raw_jsons]
    df = pd.concat(parsed, ignore_index=True)
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
