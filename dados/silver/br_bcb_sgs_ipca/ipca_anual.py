"""Silver flow: build an annual IPCA price-level index from SGS 433."""

from __future__ import annotations

import os
from decimal import Decimal

import pandas as pd
from dotenv import load_dotenv

from dados.raw.br_bcb_sgs_ipca.ipca import SERIES_CODE, SOURCE_NAME
from dados.raw.utils.postgres_interactions import PostgresETL
from dados.silver.br_bcb_sgs_ipca.models import BrBcbSgsIpcaAnual
from dados.utils.logging import get_logger
from dados.utils.pydantic_postgres import pydantic_to_postgres_columns

load_dotenv()

DATASET_ID = "br_bcb_sgs_ipca"
ZONE = "silver"
RAW_TABLE = "ipca_mensal"
TABLE = "ipca_anual"
PK_COLS = ["ano"]

log = get_logger(dataset_id=DATASET_ID, zone=ZONE)


def extract() -> pd.DataFrame:
    query = f"""
        SELECT data_referencia, variacao_mensal_pct, serie_sgs, fonte
        FROM {DATASET_ID}.{RAW_TABLE}
        ORDER BY data_referencia
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
    required = {"data_referencia", "variacao_mensal_pct"}
    missing = required.difference(df.columns)
    if missing:
        raise ValueError(f"Colunas obrigatorias ausentes no IPCA raw: {sorted(missing)}")
    if df.empty:
        raise ValueError("O IPCA raw esta vazio")

    monthly = df.copy()
    monthly["data_referencia"] = pd.to_datetime(
        monthly["data_referencia"], errors="coerce"
    )
    monthly["variacao_mensal_pct"] = pd.to_numeric(
        monthly["variacao_mensal_pct"], errors="coerce"
    )
    if monthly[list(required)].isna().any().any():
        raise ValueError("O IPCA raw contem data ou valor invalido")
    if monthly["data_referencia"].duplicated().any():
        raise ValueError("O IPCA raw contem meses duplicados")
    if (monthly["variacao_mensal_pct"] <= -100).any():
        raise ValueError("O IPCA raw contem variacao menor ou igual a -100%")

    monthly = monthly.sort_values("data_referencia").reset_index(drop=True)
    observed_periods = pd.PeriodIndex(monthly["data_referencia"], freq="M")
    expected_periods = pd.period_range(
        observed_periods.min(), observed_periods.max(), freq="M"
    )
    missing_periods = expected_periods.difference(observed_periods)
    if not missing_periods.empty:
        raise ValueError(
            "O IPCA raw possui meses ausentes: "
            + ", ".join(str(period) for period in missing_periods[:6])
        )

    monthly["fator_mensal"] = 1.0 + monthly["variacao_mensal_pct"] / 100.0
    monthly["indice_mensal"] = 100.0 * monthly["fator_mensal"].cumprod()
    monthly["ano"] = monthly["data_referencia"].dt.year.astype(int)

    annual = (
        monthly.groupby("ano", as_index=False)
        .agg(
            indice_ipca=("indice_mensal", "mean"),
            fator_acumulado_ano=("fator_mensal", "prod"),
            meses_observados=("data_referencia", "size"),
        )
        .sort_values("ano")
        .reset_index(drop=True)
    )
    annual["variacao_acumulada_ano_pct"] = (
        annual.pop("fator_acumulado_ano") - 1.0
    ) * 100.0
    annual["ano_completo"] = annual["meses_observados"].eq(12)
    annual["serie_sgs"] = SERIES_CODE
    annual["fonte"] = SOURCE_NAME
    return annual[list(BrBcbSgsIpcaAnual.model_fields)]


def validate(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        raise ValueError("A transformacao anual do IPCA produziu tabela vazia")
    duplicates = df.duplicated(subset=PK_COLS, keep=False)
    if duplicates.any():
        raise ValueError(f"IPCA anual contem {int(duplicates.sum())} anos duplicados")
    if (pd.to_numeric(df["indice_ipca"], errors="coerce") <= 0).any():
        raise ValueError("IPCA anual contem indice nao positivo")

    validated = df.copy()
    for column in ["indice_ipca", "variacao_acumulada_ano_pct"]:
        validated[column] = validated[column].apply(
            lambda value: None if pd.isna(value) else Decimal(str(value))
        )
    [BrBcbSgsIpcaAnual(**row) for row in validated.to_dict("records")]
    return validated


def load(df: pd.DataFrame) -> None:
    columns = pydantic_to_postgres_columns(BrBcbSgsIpcaAnual)
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
        frame = extract()
        log.info("extract.done", rows=len(frame))
        frame = transform(frame)
        log.info("transform.done", rows=len(frame))
        frame = validate(frame)
        log.info("validate.done", rows=len(frame))
        load(frame)
        log.info("load.done", rows=len(frame))
    except Exception as exc:
        log.exception("flow.error", error=str(exc))
        raise
    log.info("flow.end", rows=len(frame))


if __name__ == "__main__":
    flow()
