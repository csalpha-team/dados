"""Gold flow: pa_indexadores_producao_rural — Censo Agro 2006, lavoura temporária."""

from __future__ import annotations

import pandas as pd
from dotenv import load_dotenv

from dados.gold.models.pa_indexadores_producao_rural import (
    PaIndexadoresLavouraTemporariaCenso2006,
)
from dados.gold.pa_indexadores_producao_rural._common import (
    coerce_decimal,
    enrich_with_regiao,
    extract_silver,
    load_gold,
    log,
)

load_dotenv()

TABLE = "lavoura_temporaria_censo_2006"
SILVER_SCHEMA = "al_ibge_censoagro"
SILVER_TABLE = "tbl_2337_2006"

MODEL = PaIndexadoresLavouraTemporariaCenso2006
NUMERIC_COLS = [
    "quantidade_estabelecimentos",
    "quantidade_produzida",
    "quantidade_vendida",
    "valor_producao",
]


def extract() -> pd.DataFrame:
    query = f"""
        SELECT
            ano,
            id_municipio,
            tipo_agricultura,
            produto,
            quantidade_estabelecimentos,
            quantidade_produzida,
            quantidade_vendida,
            valor_producao
        FROM {SILVER_SCHEMA}.{SILVER_TABLE}
        WHERE id_municipio LIKE '15%'
    """
    return extract_silver(query, SILVER_SCHEMA)


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
    load_gold(TABLE, df, MODEL)


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
