"""Gold flow: pa_coeficientes_custo — coeficientes de custo por região de integração.

Previously this flow read from gold ``pa_indexadores_custo_producao_rural``
(a gold→gold dependency). It now reads silver ``al_ibge_censoagro`` directly
and reuses the municipality enrichment helper shared with the indexadores
pipeline, so the two pipelines depend on the same silver tables instead of
chaining gold→gold.
"""

from __future__ import annotations

import os
from decimal import Decimal
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

from dados.gold.models.pa_coeficientes_custo import (
    PaCoeficientesCustoPreparacaoCamadaCusto,
)
from dados.gold.pa_coeficientes_custo.utils import (
    agregar_coeficientes_regional_mais_recente,
    calcular_coeficientes_municipais,
    carregar_parametros_custo,
    clean_region_name,
    expandir_coeficientes,
)
from dados.gold.pa_indexadores_producao_rural._common import (
    enrich_with_regiao,
    extract_silver,
)
from dados.raw.utils.postgres_interactions import PostgresETL
from dados.utils.logging import get_logger
from dados.utils.pydantic_postgres import pydantic_to_postgres_columns

load_dotenv()

DATASET_ID = "pa_coeficientes_custo"
ZONE = "gold"
TABLE = "preparacao_camada_custo"
CONFIG_PATH = Path(__file__).with_name("parametros_coeficientes_custo.json")

SILVER_SCHEMA = "al_ibge_censoagro"
SILVER_TABLE_2006 = "tbl_1909_2006"
SILVER_TABLE_2017 = "tbl_6899_2017"

MODEL = PaCoeficientesCustoPreparacaoCamadaCusto
PK_COLS = ["ano", "nome_regiao_integracao", "tipo_coeff"]

log = get_logger(dataset_id=DATASET_ID, zone=ZONE)


def extract() -> pd.DataFrame:
    query = f"""
        SELECT
            ano, id_municipio, tipo_agricultura, tipo_despesa,
            quantidade_estabelecimentos_fizeram_despesa,
            ROUND(valor_despesa::numeric, 2) AS valor_despesa
        FROM {SILVER_SCHEMA}.{SILVER_TABLE_2006}
        WHERE id_municipio LIKE '15%'
        UNION ALL
        SELECT
            ano, id_municipio, tipo_agricultura, tipo_despesa,
            quantidade_estabelecimentos_fizeram_despesa,
            ROUND(valor_despesa::numeric, 2) AS valor_despesa
        FROM {SILVER_SCHEMA}.{SILVER_TABLE_2017}
        WHERE id_municipio LIKE '15%'
    """
    df = extract_silver(query, SILVER_SCHEMA)
    return enrich_with_regiao(df)


def transform(df: pd.DataFrame) -> pd.DataFrame:
    if "nome_regiao_integracao" in df.columns:
        df["nome_regiao_integracao"] = df["nome_regiao_integracao"].apply(
            clean_region_name
        )

    valor_para_chave, rename_map, total_expense_label = carregar_parametros_custo(
        CONFIG_PATH
    )

    coeff_agrupados = calcular_coeficientes_municipais(
        df, total_expense_label=total_expense_label
    )
    coeff_agrupados["nome_regiao_integracao"] = coeff_agrupados[
        "nome_regiao_integracao"
    ].replace(rename_map)

    coeff_df = expandir_coeficientes(coeff_agrupados, valor_para_chave)
    final = agregar_coeficientes_regional_mais_recente(coeff_df)

    return final[list(MODEL.model_fields.keys())].copy()


def validate(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        log.error("validate.error", reason="empty_dataframe")
        raise ValueError("transform produced empty dataframe")

    dupes = df.duplicated(subset=PK_COLS, keep=False)
    if dupes.any():
        log.error("validate.error", reason="duplicate_pk", count=int(dupes.sum()))
        raise ValueError(f"Found {int(dupes.sum())} rows duplicating PK {PK_COLS}")

    df["coeff"] = df["coeff"].apply(lambda v: None if pd.isna(v) else Decimal(str(v)))
    [MODEL(**r) for r in df.to_dict("records")]
    return df


def load(df: pd.DataFrame) -> None:
    columns = pydantic_to_postgres_columns(MODEL)
    with PostgresETL(
        host="localhost",
        database=os.getenv("DB_GOLD_ZONE"),
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
