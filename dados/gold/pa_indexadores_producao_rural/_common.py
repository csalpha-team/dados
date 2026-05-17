"""Shared helpers for pa_indexadores_producao_rural gold flows.

Every flow in this dataset reads a silver table filtered to Pará municipalities
(``id_municipio LIKE '15%'``), enriches with a municipality directory pulled
from Base dos Dados (BigQuery), and maps the result onto the integration
region (``RI``) dictionary in :mod:`dados.gold.pa_indexadores_producao_rural.utils`.
"""
from __future__ import annotations

import os
from decimal import Decimal
from typing import Iterable

import basedosdados as bd
import pandas as pd

from dados.gold.pa_indexadores_producao_rural.utils import (
    dicionario_regioes_integracao,
)
from dados.raw.utils.postgres_interactions import PostgresETL
from dados.utils.logging import get_logger
from dados.utils.pydantic_postgres import pydantic_to_postgres_columns

DATASET_ID = "pa_indexadores_producao_rural"
ZONE = "gold"

_MUNICIPIOS_QUERY = """
    SELECT id_municipio, nome, sigla_uf
    FROM `basedosdados.br_bd_diretorios_brasil.municipio`
    WHERE amazonia_legal = 1
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


def fetch_municipios_amazonia_legal() -> pd.DataFrame:
    """Download the Amazônia Legal municipality directory from BigQuery."""
    log.info("extract.municipios.start", source="basedosdados")
    return bd.read_sql(
        _MUNICIPIOS_QUERY,
        billing_project_id=os.getenv("BASEDOSDADADOS_PROJECT_ID"),
    )


def enrich_with_regiao(data: pd.DataFrame) -> pd.DataFrame:
    """Left-join municipality info and map the integration region."""
    municipios = fetch_municipios_amazonia_legal()
    data = data.merge(municipios, on="id_municipio", how="left")
    data["nome_regiao_integracao"] = data["id_municipio"].map(
        dicionario_regioes_integracao
    )
    return data


def coerce_decimal(df: pd.DataFrame, cols: Iterable[str]) -> pd.DataFrame:
    for col in cols:
        if col in df.columns:
            df[col] = df[col].apply(
                lambda v: None if pd.isna(v) else Decimal(str(v))
            )
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
