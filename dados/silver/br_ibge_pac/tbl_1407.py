"""Silver flow: IBGE PAC — table 1407 (commerce indicators by region/CNAE).

Pivots the long-form raw landing into one row per ``(ano, unidade_geografica,
divisao_grupo_cnae_2)`` with one column per indicator, and lands the result at
``$DB_SILVER_ZONE.br_ibge_pac.tbl_1407``.
"""

from __future__ import annotations

import os
from decimal import Decimal

import pandas as pd
from dotenv import load_dotenv

from dados.raw.utils.postgres_interactions import PostgresETL
from dados.silver.models.br_ibge_pac import BrIbgePacTbl1407
from dados.utils.logging import get_logger
from dados.utils.pydantic_postgres import pydantic_to_postgres_columns

load_dotenv()

DATASET_ID = "br_ibge_pac"
ZONE = "silver"
TABLE = "tbl_1407"

PK_COLS = ["ano", "unidade_geografica", "divisao_grupo_cnae_2"]
NULL_TOKENS = ("..", "...", "-", "X")

COLUMN_RENAME = {
    "nome_categoria_comercio": "divisao_grupo_cnae_2",
    "nome_categoria_regiao": "unidade_geografica",
    "Gastos com salários, retiradas e outras remunerações em empresas comerciais": "valor_gastos_salarios_remuneracoes",
    "Margem de comercialização em empresas comerciais": "margem_comercializacao",
    "Número de unidades locais com receita de revenda": "quantidade_unidades_empresas_receita_revenda",
    "Pessoal ocupado em 31/12 em empresas comerciais": "pessoal_ocupado_31_12",
    "Receita bruta de revenda de mercadorias": "valor_receita_bruta_revenda",
}

log = get_logger(dataset_id=DATASET_ID, zone=ZONE)


def extract() -> pd.DataFrame:
    query = f"""
        SELECT
            nome_variavel,
            nome_categoria_comercio,
            nome_categoria_regiao,
            CAST(ano AS INTEGER) AS ano,
            valor
        FROM {DATASET_ID}.{TABLE}
    """
    with PostgresETL(
        host="localhost",
        database=os.getenv("DB_RAW_ZONE"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        schema=DATASET_ID,
    ) as db:
        return db.download_data(query)


def transform(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["valor"] = df["valor"].apply(lambda x: 0 if x in NULL_TOKENS else x)

    df = df.pivot_table(
        index=["ano", "nome_categoria_regiao", "nome_categoria_comercio"],
        columns="nome_variavel",
        values="valor",
        aggfunc="sum",
    ).reset_index()
    df.columns.name = None

    df = df.rename(columns=COLUMN_RENAME)
    return df[list(BrIbgePacTbl1407.model_fields.keys())]


def validate(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        log.error("validate.error", reason="empty_dataframe")
        raise ValueError("transform produced an empty dataframe")

    dupes = df.duplicated(subset=PK_COLS, keep=False)
    if dupes.any():
        log.error("validate.error", reason="duplicate_pk", count=int(dupes.sum()))
        raise ValueError(f"Found {int(dupes.sum())} rows duplicating PK {PK_COLS}")

    decimal_cols = [c for c in df.columns if c not in PK_COLS]
    for col in decimal_cols:
        df[col] = df[col].apply(lambda v: None if pd.isna(v) else Decimal(str(v)))

    rows = df.to_dict("records")
    [BrIbgePacTbl1407(**r) for r in rows]
    return df


def load(df: pd.DataFrame) -> None:
    columns = pydantic_to_postgres_columns(BrIbgePacTbl1407)
    with PostgresETL(
        host="localhost",
        database=os.getenv("DB_SILVER_ZONE"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        schema=DATASET_ID,
    ) as db:
        db.create_table(TABLE, columns, drop_if_exists=True)
        db.load_data(TABLE, df, if_exists="append")


def flow() -> None:
    log.info("flow.start", table=TABLE)
    try:
        df = extract()
        log.info("extract.done", rows=len(df))
        df = transform(df)
        log.info("transform.done", rows=len(df))
        df = validate(df)
        log.info("validate.done", rows=len(df))
        load(df)
        log.info("load.done", rows=len(df))
    except Exception as exc:
        log.exception("flow.error", error=str(exc))
        raise
    log.info("flow.end", rows=len(df))


if __name__ == "__main__":
    flow()
