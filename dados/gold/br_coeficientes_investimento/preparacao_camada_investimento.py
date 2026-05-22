"""Gold flow: br_coeficientes_investimento — publica coeficientes exógenos.

Os valores versionados em ``coeficientes_investimento.json`` não são calculados
neste ETL: são parâmetros exógenos derivados de frações de custo por unidade de
produção microeconômica estimadas em pesquisa anterior conduzida pelo professor
Francisco de Assis Costa. As frações tratam da necessidade de investimentos
observada no balanço patrimonial (veículos, construção civil/benfeitorias e
máquinas/equipamentos). Esta camada apenas valida e publica os coeficientes.
"""

from __future__ import annotations

import os
from decimal import Decimal
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

from dados.gold.br_coeficientes_investimento.utils import (
    carregar_coeficientes_investimento,
)
from dados.gold.br_coeficientes_investimento.models import (
    BrCoeficientesInvestimentoCoeficientesInvestimento,
)
from dados.raw.utils.postgres_interactions import PostgresETL
from dados.utils.logging import get_logger
from dados.utils.pydantic_postgres import pydantic_to_postgres_columns

load_dotenv()

DATASET_ID = "br_coeficientes_investimento"
ZONE = "gold"
TABLE = "coeficientes_investimento"

DEFAULT_JSON_PATH = Path(__file__).with_name("coeficientes_investimento.json")
MODEL = BrCoeficientesInvestimentoCoeficientesInvestimento
PK_COLS = ["coeff_key"]

log = get_logger(dataset_id=DATASET_ID, zone=ZONE)


def _resolve_json_path() -> Path:
    override = os.getenv("INVESTMENT_COEFFICIENTS_JSON_PATH") or os.getenv(
        "COEFICIENTES_INVESTIMENTO_JSON_PATH"
    )
    return Path(override) if override else DEFAULT_JSON_PATH


def extract() -> pd.DataFrame:
    return carregar_coeficientes_investimento(_resolve_json_path())


def transform(df: pd.DataFrame) -> pd.DataFrame:
    return df[list(MODEL.model_fields.keys())].copy()


def validate(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        log.error("validate.error", reason="empty_dataframe")
        raise ValueError("extract produced an empty dataframe")

    dupes = df.duplicated(subset=PK_COLS, keep=False)
    if dupes.any():
        log.error("validate.error", reason="duplicate_pk", count=int(dupes.sum()))
        raise ValueError(f"Found {int(dupes.sum())} duplicate keys")

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
