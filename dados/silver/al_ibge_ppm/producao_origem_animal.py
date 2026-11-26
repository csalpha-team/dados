"""Silver flow: IBGE PPM — produção de origem animal (Amazônia Legal).

Pivota a aterrissagem longa de ``raw.al_ibge_ppm.producao_origem_animal`` em uma linha por
(id_municipio, ano, produto), aplica o tratamento de dígitos não numéricos do
IBGE e a normalização de moedas históricas, e materializa em
``$DB_SILVER_ZONE.al_ibge_ppm.producao_origem_animal``.

A unidade da quantidade varia por produto e vem dos metadados do IBGE já na
camada raw; ela é preservada em ``unidade_medida``.
"""

from __future__ import annotations

import os
from decimal import Decimal
from typing import Sequence

import pandas as pd
from dotenv import load_dotenv

from dados.raw.utils.postgres_interactions import PostgresETL
from dados.silver.al_ibge_ppm.models import AlIbgePpmProducaoOrigemAnimal
from dados.silver.utils import currency_fix_series, fix_ibge_digits
from dados.utils.batching import (
    CargaEmStaging,
    executar_em_lotes_por_ano,
    listar_anos,
)
from dados.utils.logging import get_logger
from dados.utils.parallel import validate_rows
from dados.utils.pydantic_postgres import pydantic_to_postgres_columns

load_dotenv()

DATASET_ID = "al_ibge_ppm"
ZONE = "silver"
TABLE = "producao_origem_animal"
RAW_TABLE = "producao_origem_animal"

PK_COLS = ["id_municipio", "ano", "produto"]

# Tabelas grandes: validação e carga fatiadas em threads, e o flow inteiro
# executado em lotes de anos (ver dados/utils/batching.py), para que o pico
# de memória seja o de um lote e não o da tabela inteira.
LOAD_MAX_WORKERS = 4
ANOS_POR_LOTE = 5
VARIAVEL_QUANTIDADE = "Produção de origem animal"
VARIAVEL_VALOR = "Valor da produção"

log = get_logger(dataset_id=DATASET_ID, zone=ZONE)


def extract(db: PostgresETL, anos: Sequence[int]) -> pd.DataFrame:
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
        WHERE CAST(ano AS INTEGER) = ANY(%s)
        GROUP BY ano, id_municipio, produto
    """
    return db.download_data(query, params=(list(anos),))


def transform(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df = fix_ibge_digits(df, ["producao", "valor_producao"], PK_COLS)

    # currency_fix_series é vetorizada e converte as moedas
    # históricas para a base atual em Mil Reais.
    df["valor_producao"] = currency_fix_series(df["ano"], df["valor_producao"])

    return df[list(AlIbgePpmProducaoOrigemAnimal.model_fields.keys())]


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

    validate_rows(df, AlIbgePpmProducaoOrigemAnimal, max_workers=LOAD_MAX_WORKERS)
    return df


def flow() -> None:
    log.info("flow.start", table=TABLE, anos_por_lote=ANOS_POR_LOTE)
    try:
        with (
            PostgresETL(
                host="localhost",
                database=os.getenv("DB_RAW_ZONE"),
                user=os.getenv("POSTGRES_USER"),
                password=os.getenv("POSTGRES_PASSWORD"),
                schema=DATASET_ID,
            ) as origem,
            PostgresETL(
                host="localhost",
                database=os.getenv("DB_SILVER_ZONE"),
                user=os.getenv("POSTGRES_USER"),
                password=os.getenv("POSTGRES_PASSWORD"),
                schema=DATASET_ID,
            ) as destino,
        ):
            anos = listar_anos(origem, RAW_TABLE)
            log.info("extract.anos", anos=len(anos), de=min(anos), ate=max(anos))

            columns = pydantic_to_postgres_columns(AlIbgePpmProducaoOrigemAnimal)
            with CargaEmStaging(
                destino, TABLE, columns, max_workers=LOAD_MAX_WORKERS
            ) as carga:
                total = executar_em_lotes_por_ano(
                    anos=anos,
                    extrair=lambda lote: extract(origem, lote),
                    transformar=transform,
                    validar=validate,
                    carga=carga,
                    log=log,
                    anos_por_lote=ANOS_POR_LOTE,
                )
            log.info("load.done", rows=total)
    except Exception as exc:
        log.exception("flow.error", error=str(exc))
        raise
    log.info("flow.end", rows=total)


if __name__ == "__main__":
    flow()
