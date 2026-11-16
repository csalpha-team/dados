"""Silver flow: IBGE PPM — efetivo dos rebanhos (Amazônia Legal).

Pivota a aterrissagem longa do raw em uma linha por (id_municipio, ano, produto)
com ``efetivo_rebanhos``, aplica o tratamento de dígitos não numéricos do IBGE e
materializa em ``$DB_SILVER_ZONE.al_ibge_ppm.efetivo_rebanhos``.

A PPM traz apenas a variável 105 (Efetivo dos rebanhos, em cabeças) — não há
valor monetário nesta tabela, então não se aplica correção de moeda.
"""

from __future__ import annotations

import os
from decimal import Decimal

import pandas as pd
from dotenv import load_dotenv

from dados.raw.utils.postgres_interactions import PostgresETL
from dados.silver.al_ibge_ppm.models import AlIbgePpmEfetivoRebanhos
from dados.silver.utils import fix_ibge_digits
from dados.utils.logging import get_logger
from dados.utils.parallel import validate_rows
from dados.utils.pydantic_postgres import pydantic_to_postgres_columns

load_dotenv()

DATASET_ID = "al_ibge_ppm"
ZONE = "silver"
TABLE = "efetivo_rebanhos"
RAW_TABLE = "efetivo_rebanhos"

PK_COLS = ["id_municipio", "ano", "produto"]

# Tabelas grandes: validação e carga fatiadas em threads.
LOAD_MAX_WORKERS = 4

log = get_logger(dataset_id=DATASET_ID, zone=ZONE)


def extract() -> pd.DataFrame:
    # Agregação condicional no Postgres (mesmo padrão da PEVS/PAM): a tabela raw
    # é longa demais para pivotar em pandas e as sentinelas 'X'/'..' precisam
    # chegar intactas no fix_ibge_digits.
    query = f"""
        SELECT
            CAST(ano AS INTEGER) AS ano,
            id_municipio,
            produto,
            MAX(CASE WHEN nome_variavel = 'Efetivo dos rebanhos'
                     THEN valor END) AS efetivo_rebanhos,
            MAX(CASE WHEN nome_variavel = 'Efetivo dos rebanhos'
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
    df = fix_ibge_digits(df, ["efetivo_rebanhos"], PK_COLS)
    return df[list(AlIbgePpmEfetivoRebanhos.model_fields.keys())]


def validate(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        log.error("validate.error", reason="empty_dataframe")
        raise ValueError("transform produced an empty dataframe")

    dupes = df.duplicated(subset=PK_COLS, keep=False)
    if dupes.any():
        log.error("validate.error", reason="duplicate_pk", count=int(dupes.sum()))
        raise ValueError(f"Found {int(dupes.sum())} rows duplicating PK {PK_COLS}")

    df["efetivo_rebanhos"] = df["efetivo_rebanhos"].apply(
        lambda v: None if pd.isna(v) else Decimal(str(v))
    )
    df["unidade_medida"] = df["unidade_medida"].apply(
        lambda v: None if pd.isna(v) or str(v).strip() in ("", "NaN", "nan") else v
    )

    validate_rows(df, AlIbgePpmEfetivoRebanhos, max_workers=LOAD_MAX_WORKERS)
    return df


def load(df: pd.DataFrame) -> None:
    columns = pydantic_to_postgres_columns(AlIbgePpmEfetivoRebanhos)
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
