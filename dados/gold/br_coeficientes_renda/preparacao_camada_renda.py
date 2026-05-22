"""Gold flow: br_coeficientes_renda — coeficientes de produtividade e salário.

Reads silver PIA (``tbl_1849``) and PAC (``tbl_1407``), runs the forecasting
pipeline in :mod:`dados.gold.br_coeficientes_renda.utils`, and publishes three
tables: ``preparacao_camada_renda``, ``renda_produtividade``, ``renda_salario``.
"""

from __future__ import annotations

import os
from decimal import Decimal
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

from dados.gold.br_coeficientes_renda.utils import (
    carregar_parametros_renda,
    construir_tabela_saida_renda,
    preparar_dados_coeficientes_renda,
)
from dados.gold.br_coeficientes_renda.models import (
    BrCoeficientesRendaPreparacaoCamadaRenda,
    BrCoeficientesRendaRendaProdutividade,
    BrCoeficientesRendaRendaSalario,
)
from dados.raw.utils.postgres_interactions import PostgresETL
from dados.utils.logging import get_logger
from dados.utils.pydantic_postgres import pydantic_to_postgres_columns

load_dotenv()

DATASET_ID = "br_coeficientes_renda"
ZONE = "gold"
TABLE = "preparacao_camada_renda"
PRODUCTIVITY_TABLE = "renda_produtividade"
SALARY_TABLE = "renda_salario"

CONFIG_PATH = Path(__file__).with_name("parametros_coeficientes_renda.json")

PIA_SOURCE_SCHEMA = os.getenv(
    "INCOME_PIA_SOURCE_SCHEMA", os.getenv("ESQUEMA_ORIGEM_PIA", "br_ibge_pia")
)
PIA_SOURCE_TABLE = os.getenv(
    "INCOME_PIA_SOURCE_TABLE", os.getenv("TABELA_ORIGEM_PIA", "tbl_1849")
)
PAC_SOURCE_SCHEMA = os.getenv(
    "INCOME_PAC_SOURCE_SCHEMA", os.getenv("ESQUEMA_ORIGEM_PAC", "br_ibge_pac")
)
PAC_SOURCE_TABLE = os.getenv(
    "INCOME_PAC_SOURCE_TABLE", os.getenv("TABELA_ORIGEM_PAC", "tbl_1407")
)

PK_MAIN = ["ano", "conta_alfa", "tipo_coeff"]
PK_OUTPUT = ["ano", "conta_alfa"]

log = get_logger(dataset_id=DATASET_ID, zone=ZONE)


def _read_silver(query: str, schema: str) -> pd.DataFrame:
    with PostgresETL(
        host="localhost",
        database=os.getenv("DB_SILVER_ZONE"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        schema=schema,
    ) as db:
        return db.download_data(query)


def extract() -> tuple[pd.DataFrame, pd.DataFrame, tuple]:
    params = carregar_parametros_renda(CONFIG_PATH)

    pia_query = f"""
        SELECT ano, nome_localidade, divisao_grupo_cnae_2,
               pessoal_ocupado_31_12,
               valor_bruto_producao_industrial,
               valor_salarios_remuneracoes
        FROM {PIA_SOURCE_SCHEMA}.{PIA_SOURCE_TABLE}
    """
    pac_query = f"""
        SELECT ano, unidade_geografica, divisao_grupo_cnae_2,
               valor_receita_bruta_revenda, pessoal_ocupado_31_12,
               margem_comercializacao, valor_gastos_salarios_remuneracoes
        FROM {PAC_SOURCE_SCHEMA}.{PAC_SOURCE_TABLE}
    """
    pia_data = _read_silver(pia_query, PIA_SOURCE_SCHEMA)
    pac_data = _read_silver(pac_query, PAC_SOURCE_SCHEMA)
    return pia_data, pac_data, params


def transform(
    payload: tuple[pd.DataFrame, pd.DataFrame, tuple],
) -> dict[str, pd.DataFrame]:
    pia_data, pac_data, params = payload
    sector_mappings, years, aa_production_values, forecast_config = params

    coefficients = preparar_dados_coeficientes_renda(
        pia_data,
        pac_data,
        sector_mappings=sector_mappings,
        years=years,
        aa_production_values=aa_production_values,
        forecast_config=forecast_config,
    )
    productivity = construir_tabela_saida_renda(coefficients, "prod_mon_trab")
    salary = construir_tabela_saida_renda(coefficients, "salario_medio")

    return {
        TABLE: coefficients[
            list(BrCoeficientesRendaPreparacaoCamadaRenda.model_fields.keys())
        ].copy(),
        PRODUCTIVITY_TABLE: productivity[
            list(BrCoeficientesRendaRendaProdutividade.model_fields.keys())
        ].copy(),
        SALARY_TABLE: salary[
            list(BrCoeficientesRendaRendaSalario.model_fields.keys())
        ].copy(),
    }


def _validate_one(
    df: pd.DataFrame, model, pk_cols: list[str], label: str
) -> pd.DataFrame:
    if df.empty:
        log.error("validate.error", reason="empty_dataframe", table=label)
        raise ValueError(f"empty dataframe for {label}")
    dupes = df.duplicated(subset=pk_cols, keep=False)
    if dupes.any():
        log.error(
            "validate.error", reason="duplicate_pk", table=label, count=int(dupes.sum())
        )
        raise ValueError(f"{int(dupes.sum())} dupes in {label} on {pk_cols}")
    df["coeff"] = df["coeff"].apply(lambda v: None if pd.isna(v) else Decimal(str(v)))
    [model(**r) for r in df.to_dict("records")]
    return df


def validate(dfs: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    dfs[TABLE] = _validate_one(
        dfs[TABLE], BrCoeficientesRendaPreparacaoCamadaRenda, PK_MAIN, TABLE
    )
    dfs[PRODUCTIVITY_TABLE] = _validate_one(
        dfs[PRODUCTIVITY_TABLE],
        BrCoeficientesRendaRendaProdutividade,
        PK_OUTPUT,
        PRODUCTIVITY_TABLE,
    )
    dfs[SALARY_TABLE] = _validate_one(
        dfs[SALARY_TABLE],
        BrCoeficientesRendaRendaSalario,
        PK_OUTPUT,
        SALARY_TABLE,
    )
    return dfs


def load(dfs: dict[str, pd.DataFrame]) -> None:
    targets = {
        TABLE: BrCoeficientesRendaPreparacaoCamadaRenda,
        PRODUCTIVITY_TABLE: BrCoeficientesRendaRendaProdutividade,
        SALARY_TABLE: BrCoeficientesRendaRendaSalario,
    }
    with PostgresETL(
        host="localhost",
        database=os.getenv("DB_GOLD_ZONE"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        schema=DATASET_ID,
    ) as db:
        for name, model in targets.items():
            columns = pydantic_to_postgres_columns(model)
            db.create_table(name, columns, drop_if_exists=True)
            db.load_data(name, dfs[name], if_exists="append")


def flow() -> None:
    log.info("flow.start", table=TABLE)
    try:
        payload = extract()
        log.info("extract.done", rows_pia=len(payload[0]), rows_pac=len(payload[1]))
        dfs = transform(payload)
        log.info("transform.done", rows={k: len(v) for k, v in dfs.items()})
        dfs = validate(dfs)
        log.info("validate.done", rows={k: len(v) for k, v in dfs.items()})
        load(dfs)
        log.info("load.done", rows={k: len(v) for k, v in dfs.items()})
    except Exception as exc:
        log.exception("flow.error", error=str(exc))
        raise
    log.info("flow.end", rows={k: len(v) for k, v in dfs.items()})


if __name__ == "__main__":
    flow()
