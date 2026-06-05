"""Gold flow: br_coeficientes_consumo — valores de consumo a partir da POF.

Reads silver ``br_ibge_pof.tbl_6970`` directly (previously read from gold
``brasil_despesas_familiares`` — a gold→gold violation).
"""

from __future__ import annotations

import os
from decimal import Decimal
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

from dados.gold.br_coeficientes_consumo.utils import (
    construir_valores_consumo,
)
from dados.gold.br_coeficientes_consumo.models import (
    BrCoeficientesConsumoPreparacaoCamadaConsumo,
)
from dados.raw.utils.postgres_interactions import PostgresETL
from dados.utils.logging import get_logger
from dados.utils.pydantic_postgres import pydantic_to_postgres_columns

load_dotenv()

DATASET_ID = "br_coeficientes_consumo"
ZONE = "gold"
TABLE = "preparacao_camada_consumo"

SILVER_SCHEMA = "br_ibge_pof"
SILVER_TABLE = "tbl_6970"
DEFAULT_EQUIVALENCE_PATH = Path(__file__).with_name("equivalencia_despesas.json")

PARAMETROS_CONSUMO = {
    "coluna_chave_mip": "TipoDespesaDestinoProvável",
    "coluna_tipo_despesa_mip": "TiposDeDespesa",
    "variavel_alvo": "Despesa monetária e não monetária média mensal familiar",
    "ano_alvo": 2018,
    "rotulo_urbano": "Urbana",
    "rotulo_rural": "Rural",
    "padrao_estado": "Estad|Estadual",
}

MODEL = BrCoeficientesConsumoPreparacaoCamadaConsumo
PK_COLS = ["ano", "coeff_key"]

log = get_logger(dataset_id=DATASET_ID, zone=ZONE)


def _resolve_equivalence_path() -> Path:
    override = os.getenv("CONSUMPTION_EQUIVALENCE_FILE_PATH") or os.getenv(
        "CAMINHO_ARQUIVO_EQUIVALENCIA_CONSUMO"
    )
    return Path(override) if override else DEFAULT_EQUIVALENCE_PATH


def _load_mip_mapping() -> pd.DataFrame:
    path = _resolve_equivalence_path()
    if not path.exists():
        raise FileNotFoundError(f"Arquivo de equivalência não encontrado: {path}")

    mapping = pd.read_json(path)
    required = [
        PARAMETROS_CONSUMO["coluna_chave_mip"],
        PARAMETROS_CONSUMO["coluna_tipo_despesa_mip"],
    ]
    missing = [c for c in required if c not in mapping.columns]
    if missing:
        raise ValueError(f"Colunas obrigatórias ausentes na equivalência: {missing}")

    mapping = mapping[required].dropna(subset=required).drop_duplicates(subset=required)
    coluna_tipo = PARAMETROS_CONSUMO["coluna_tipo_despesa_mip"]
    mapping[coluna_tipo] = mapping[coluna_tipo].astype("string").str.strip()
    return mapping


def extract() -> tuple[pd.DataFrame, pd.DataFrame]:
    query = f"""
        SELECT ano, variavel, situacao_domicilio, tipo_despesa, valor, unidade_medida
        FROM {SILVER_SCHEMA}.{SILVER_TABLE}
    """
    with PostgresETL(
        host="localhost",
        database=os.getenv("DB_SILVER_ZONE"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        schema=SILVER_SCHEMA,
    ) as db:
        pof_data = db.download_data(query)

    pof_data["tipo_despesa"] = pof_data["tipo_despesa"].astype("string").str.strip()
    return pof_data, _load_mip_mapping()


def transform(payload: tuple[pd.DataFrame, pd.DataFrame]) -> pd.DataFrame:
    pof_data, mip_mapping = payload
    df = construir_valores_consumo(pof_data, mip_mapping, PARAMETROS_CONSUMO)
    return df[list(MODEL.model_fields.keys())].copy()


def validate(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        log.error("validate.error", reason="empty_dataframe")
        raise ValueError("transform produced empty dataframe")

    dupes = df.duplicated(subset=PK_COLS, keep=False)
    if dupes.any():
        log.error("validate.error", reason="duplicate_pk", count=int(dupes.sum()))
        raise ValueError(f"Found {int(dupes.sum())} rows duplicating PK {PK_COLS}")

    df["valor"] = df["valor"].apply(lambda v: None if pd.isna(v) else Decimal(str(v)))
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
        payload = extract()
        log.info("extract.done", rows=len(payload[0]))
        df = transform(payload)
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
