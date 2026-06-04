"""Shared helpers for pa_indexadores_producao_rural gold flows.

Every flow in this dataset reads a silver table filtered to Pará municipalities
(``id_municipio LIKE '15%'``) and enriches it with the integration-region (``RI``)
directory from the silver table ``br_csalpha_diretorios_brasil.regioes_integracao``
(see :func:`enrich_with_regiao`).
"""

from __future__ import annotations

import os
from decimal import Decimal
from typing import Iterable

import pandas as pd

from dados.raw.utils.postgres_interactions import PostgresETL
from dados.utils.logging import get_logger
from dados.utils.pydantic_postgres import pydantic_to_postgres_columns

DATASET_ID = "pa_indexadores_producao_rural"
ZONE = "gold"

# Diretório de regiões de integração do Pará (zona silver). Fonte única do
# mapeamento município → RI; substitui o antigo dict hardcoded em utils.py
# (que tinha 6 id_municipio incorretos) e dispensa a leitura do BigQuery.
REGIOES_SCHEMA = "br_csalpha_diretorios_brasil"
REGIOES_TABLE = "regioes_integracao"
_REGIOES_QUERY = f"""
    SELECT id_municipio, nome, sigla_uf, nome_regiao_integracao
    FROM {REGIOES_SCHEMA}.{REGIOES_TABLE}
"""

log = get_logger(dataset_id=DATASET_ID, zone=ZONE)


def extract_silver(query: str, schema: str) -> pd.DataFrame:
    """Run ``query`` against ``DB_SILVER_ZONE`` and return a dataframe."""
    with PostgresETL(
        host="localhost",
        database=os.getenv("DB_SILVER_ZONE"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        schema=schema,
    ) as db:
        return db.download_data(query)


def fetch_regioes_integracao() -> pd.DataFrame:
    """Read the Pará integration-region directory from the silver zone."""
    log.info("extract.regioes.start", source=f"silver.{REGIOES_SCHEMA}")
    with PostgresETL(
        host="localhost",
        database=os.getenv("DB_SILVER_ZONE"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        schema=REGIOES_SCHEMA,
    ) as db:
        return db.download_data(_REGIOES_QUERY)


def enrich_with_regiao(data: pd.DataFrame) -> pd.DataFrame:
    """Left-join the silver RI directory (nome, sigla_uf, região) by id_municipio."""
    regioes = fetch_regioes_integracao()
    return data.merge(regioes, on="id_municipio", how="left")


def coerce_decimal(df: pd.DataFrame, cols: Iterable[str]) -> pd.DataFrame:
    for col in cols:
        if col in df.columns:
            df[col] = df[col].apply(lambda v: None if pd.isna(v) else Decimal(str(v)))
    return df


def load_gold(table: str, df: pd.DataFrame, model) -> None:
    columns = pydantic_to_postgres_columns(model)
    with PostgresETL(
        host="localhost",
        database=os.getenv("DB_GOLD_ZONE"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        schema=DATASET_ID,
    ) as db:
        db.create_table(table, columns, drop_if_exists=True)
        db.load_data(table, df, if_exists="append")
