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
from typing import Sequence

import pandas as pd
from dotenv import load_dotenv

from dados.raw.utils.postgres_interactions import PostgresETL
from dados.silver.al_ibge_ppm.models import AlIbgePpmEfetivoRebanhos
from dados.silver.utils import fix_ibge_digits
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
TABLE = "efetivo_rebanhos"
RAW_TABLE = "efetivo_rebanhos"

PK_COLS = ["id_municipio", "ano", "produto"]

# Tabelas grandes: validação e carga fatiadas em threads, e o flow inteiro
# executado em lotes de anos (ver dados/utils/batching.py), para que o pico
# de memória seja o de um lote e não o da tabela inteira.
LOAD_MAX_WORKERS = 4
ANOS_POR_LOTE = 5

log = get_logger(dataset_id=DATASET_ID, zone=ZONE)


def extract(db: PostgresETL, anos: Sequence[int]) -> pd.DataFrame:
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
        WHERE CAST(ano AS INTEGER) = ANY(%s)
        GROUP BY ano, id_municipio, produto
    """
    return db.download_data(query, params=(list(anos),))


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

            columns = pydantic_to_postgres_columns(AlIbgePpmEfetivoRebanhos)
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
