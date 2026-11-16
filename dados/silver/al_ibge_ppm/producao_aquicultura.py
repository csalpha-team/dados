"""Silver flow: IBGE PPM — produção da aquicultura (Amazônia Legal).

Pivota a aterrissagem longa de ``raw.al_ibge_ppm.producao_aquicultura`` em uma linha por
(id_municipio, ano, produto), aplica o tratamento de dígitos não numéricos do
IBGE e a normalização de moedas históricas, e materializa em
``$DB_SILVER_ZONE.al_ibge_ppm.producao_aquicultura``.

A unidade da quantidade varia por produto e vem dos metadados do IBGE já na
camada raw; ela é preservada em ``unidade_medida``.
"""

from __future__ import annotations

import os
from decimal import Decimal

import pandas as pd
from dotenv import load_dotenv

from dados.raw.utils.postgres_interactions import PostgresETL
from dados.silver.al_ibge_ppm.models import AlIbgePpmProducaoAquicultura
from dados.silver.utils import currency_fix_series, fix_ibge_digits
from dados.utils.logging import get_logger
from dados.utils.parallel import validate_rows
from dados.utils.pydantic_postgres import pydantic_to_postgres_columns

load_dotenv()

DATASET_ID = "al_ibge_ppm"
ZONE = "silver"
TABLE = "producao_aquicultura"
RAW_TABLE = "producao_aquicultura"

PK_COLS = ["id_municipio", "ano", "produto"]

# Tabelas grandes: validação e carga fatiadas em threads.
LOAD_MAX_WORKERS = 4
VARIAVEL_QUANTIDADE = "Produção da aquicultura"
VARIAVEL_VALOR = "Valor da produção"

log = get_logger(dataset_id=DATASET_ID, zone=ZONE)


def extract() -> pd.DataFrame:
    query = f"""
        SELECT
            CAST(ano AS INTEGER) AS ano,
            id_municipio,
            produto,
            MAX(CASE WHEN nome_variavel = '{VARIAVEL_QUANTIDADE}'
                     THEN valor END) AS producao,
            MAX(CASE WHEN nome_variavel = '{VARIAVEL_VALOR}'
                     THEN valor END) AS valor_producao,
            MAX(CASE WHEN nome_variavel = '{VARIAVEL_QUANTIDADE}'
                     THEN unidade_medida END) AS unidade_medida
        FROM {DATASET_ID}.{RAW_TABLE}
        GROUP BY ano, id_municipio, produto
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
    df = fix_ibge_digits(df, ["producao", "valor_producao"], PK_COLS)

    # currency_fix_series é vetorizada e converte as moedas
    # históricas para a base atual em Mil Reais.
    df["valor_producao"] = currency_fix_series(df["ano"], df["valor_producao"])

    return df[list(AlIbgePpmProducaoAquicultura.model_fields.keys())]


def validate(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        log.error("validate.error", reason="empty_dataframe")
        raise ValueError("transform produced an empty dataframe")

    dupes = df.duplicated(subset=PK_COLS, keep=False)
    if dupes.any():
        log.error("validate.error", reason="duplicate_pk", count=int(dupes.sum()))
        raise ValueError(f"Found {int(dupes.sum())} rows duplicating PK {PK_COLS}")

    for col in ("producao", "valor_producao"):
        df[col] = df[col].apply(lambda v: None if pd.isna(v) else Decimal(str(v)))

    df["unidade_medida"] = df["unidade_medida"].apply(
        lambda v: None if pd.isna(v) or str(v).strip() in ("", "NaN", "nan") else v
    )

    validate_rows(df, AlIbgePpmProducaoAquicultura, max_workers=LOAD_MAX_WORKERS)
    return df


def load(df: pd.DataFrame) -> None:
    columns = pydantic_to_postgres_columns(AlIbgePpmProducaoAquicultura)
    with PostgresETL(
        host="localhost",
        database=os.getenv("DB_SILVER_ZONE"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        schema=DATASET_ID,
    ) as db:
        db.create_table(TABLE, columns, drop_if_exists=True)
        db.load_data(TABLE, df, if_exists="append", max_workers=LOAD_MAX_WORKERS)


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
