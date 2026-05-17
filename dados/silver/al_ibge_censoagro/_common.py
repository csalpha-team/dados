"""Shared helpers for IBGE Censo Agropecuário silver flows.

The 13 censoagro flows all follow the same shape: read a long-format raw table,
SQL-pivot it into one row per PK with one column per indicator, map
``tipo_agricultura`` to the standardized label, apply ``fix_ibge_digits``,
validate against a pydantic model, and write to silver. Keeping the shared
plumbing here avoids 13× copies of the same scaffolding.
"""
from __future__ import annotations

import os
from decimal import Decimal
from typing import Type

import pandas as pd
from pydantic import BaseModel

from dados.raw.utils.postgres_interactions import PostgresETL
from dados.utils.pydantic_postgres import pydantic_to_postgres_columns

DATASET_ID = "al_ibge_censoagro"
ZONE = "silver"

# 2006 and 2017 use different labels for the same concept.
TIPO_AGRICULTURA_2006 = {
    "Agricultura familiar - Lei 11.326": "agricultura familiar",
    "Agricultura não familiar": "agricultura não familiar",
}
TIPO_AGRICULTURA_2017 = {
    "Agricultura familiar - sim": "agricultura familiar",
    "Agricultura familiar - não": "agricultura não familiar",
}


def download_raw(query: str) -> pd.DataFrame:
    with PostgresETL(
        host="localhost",
        database=os.getenv("DB_RAW_ZONE"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        schema=DATASET_ID,
    ) as db:
        return db.download_data(query)


def write_silver(table: str, df: pd.DataFrame, model: Type[BaseModel]) -> None:
    columns = pydantic_to_postgres_columns(model)
    with PostgresETL(
        host="localhost",
        database=os.getenv("DB_SILVER_ZONE"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        schema=DATASET_ID,
    ) as db:
        db.create_table(table, columns, drop_if_exists=True)
        db.load_data(table, df, if_exists="append")


def coerce_decimals(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for col in cols:
        df[col] = df[col].apply(lambda v: None if pd.isna(v) else Decimal(str(v)))
    return df


def assert_pk_unique(df: pd.DataFrame, pk: list[str]) -> None:
    dupes = df.duplicated(subset=pk, keep=False)
    if dupes.any():
        raise ValueError(
            f"Found {int(dupes.sum())} rows duplicating PK {pk} in censoagro silver"
        )
