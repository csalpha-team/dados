"""Gold flow: pa_indexadores_producao_rural — PPM efetivo dos rebanhos."""

from __future__ import annotations

import pandas as pd
from dotenv import load_dotenv

from dados.gold.pa_indexadores_producao_rural.models import (
    PaIndexadoresEfetivoRebanhosPpm,
)
from dados.gold.pa_indexadores_producao_rural._common import (
    coerce_decimal,
    enrich_with_regiao,
    extract_silver,
    load_gold,
    log,
)

load_dotenv()

TABLE = "efetivo_rebanhos_ppm"
SILVER_SCHEMA = "al_ibge_ppm"
SILVER_TABLE = "efetivo_rebanhos"

MODEL = PaIndexadoresEfetivoRebanhosPpm
NUMERIC_COLS = ["efetivo_rebanhos"]


def extract() -> pd.DataFrame:
    # Mesmo recorte dos demais indexadores rurais: municípios do Pará
    # (id_municipio LIKE '15%'), excluindo as linhas de total da fonte.
    query = f"""
        SELECT
            ano,
            id_municipio,
            produto,
            efetivo_rebanhos
        FROM {SILVER_SCHEMA}.{SILVER_TABLE}
        WHERE id_municipio LIKE '15%'
          AND produto !~ '^[0-9]'
          AND produto !~* 'total'
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
