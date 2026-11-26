"""Silver flow: IBGE PAS — tabela 10762 (série nova, 2024 em diante).

Pivota a aterrissagem longa de ``raw.br_ibge_pas.tbl_10762`` em uma linha por
``(ano, unidade_geografica, divisao_grupo_cnae_2)`` e materializa em
``$DB_SILVER_ZONE.br_ibge_pas.tbl_10762``.

Difere de ``tbl_2715.py`` em dois pontos: a geografia vem de ``nome_localidade``
(níveis N1/N2/N3) e os salários vêm da variável 857. A coluna ``serie`` fica
fixa em "nova" para que o gold nunca misture as duas séries sem querer (Nota
Técnica PAS 01/2026).
"""

from __future__ import annotations

import os
from decimal import Decimal

import pandas as pd
from dotenv import load_dotenv

from dados.raw.utils.postgres_interactions import PostgresETL
from dados.silver.br_ibge_pas.models import BrIbgePasTbl10762
from dados.utils.logging import get_logger
from dados.utils.pydantic_postgres import pydantic_to_postgres_columns

load_dotenv()

DATASET_ID = "br_ibge_pas"
RAW_DATASET_ID = "br_ibge_pas"
ZONE = "silver"
TABLE = "tbl_10762"

PK_COLS = ["ano", "unidade_geografica", "divisao_grupo_cnae_2"]
NULL_TOKENS = ("..", "...", "-", "X")
SERIE = "nova"

COLUMN_RENAME = {
    "nome_categoria": "divisao_grupo_cnae_2",
    "nome_localidade": "unidade_geografica",
    "Número de empresas": "quantidade_empresas",
    "Pessoal ocupado em 31/12": "pessoal_ocupado_31_12",
    "Receita bruta de serviços": "valor_receita_bruta_servicos",
    "Gastos com pessoal - salários, retiradas e outras remunerações": "valor_gastos_salarios_remuneracoes",
}

log = get_logger(dataset_id=DATASET_ID, zone=ZONE)


def extract() -> pd.DataFrame:
    query = f"""
        SELECT
            nome_variavel,
            nome_categoria,
            nome_localidade,
            CAST(ano AS INTEGER) AS ano,
            valor
        FROM {RAW_DATASET_ID}.{TABLE}
    """
    with PostgresETL(
        host="localhost",
        database=os.getenv("DB_RAW_ZONE"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        schema=RAW_DATASET_ID,
    ) as db:
        return db.download_data(query)


def transform(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["valor"] = df["valor"].apply(lambda x: 0 if x in NULL_TOKENS else x)

    df = df.pivot_table(
        index=["ano", "nome_localidade", "nome_categoria"],
        columns="nome_variavel",
        values="valor",
        aggfunc="sum",
    ).reset_index()
    df.columns.name = None

    df = df.rename(columns=COLUMN_RENAME)
    df["serie"] = SERIE
    return df[list(BrIbgePasTbl10762.model_fields.keys())]


def validate(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        log.error("validate.error", reason="empty_dataframe")
        raise ValueError("transform produced an empty dataframe")

    dupes = df.duplicated(subset=PK_COLS, keep=False)
    if dupes.any():
        log.error("validate.error", reason="duplicate_pk", count=int(dupes.sum()))
        raise ValueError(f"Found {int(dupes.sum())} rows duplicating PK {PK_COLS}")

    decimal_cols = [c for c in df.columns if c not in PK_COLS + ["serie"]]
    for col in decimal_cols:
        df[col] = df[col].apply(lambda v: None if pd.isna(v) else Decimal(str(v)))

    [BrIbgePasTbl10762(**r) for r in df.to_dict("records")]
    return df


def load(df: pd.DataFrame) -> None:
    columns = pydantic_to_postgres_columns(BrIbgePasTbl10762)
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
