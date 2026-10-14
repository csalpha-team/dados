"""Gold flow: rural employment technical coefficients from Censo Agropecuário 2017.

The coefficient is ``valor_producao / pessoal_ocupado`` for each Pará
municipality and IBGE economic activity group. Both crop groups from the Censo
(temporary and permanent crops) remain distinct records.
"""

from __future__ import annotations

import os
from decimal import Decimal, InvalidOperation

import pandas as pd
from dotenv import load_dotenv

from dados.gold.pa_coeficientes_emprego_rural.models import (
    PaCoeficientesEmpregoRuralCenso2017,
)
from dados.raw.utils.postgres_interactions import PostgresETL
from dados.utils.logging import get_logger
from dados.utils.pydantic_postgres import pydantic_to_postgres_columns

load_dotenv()

DATASET_ID = "pa_coeficientes_emprego_rural"
ZONE = "gold"
TABLE = "censo_2017_grupo_atividade"

SOURCE_SCHEMA = "pa_indexadores_valor_producao_rural"
PESSOAL_OCUPADO_TABLE = "censo_2017_pessoal_ocupado_grupo_atividade"
VALOR_PRODUCAO_TABLE = "censo_2017_valor_producao_grupo_atividade"
KEY_COLS = ["ano", "id_municipio", "grupo_atividade_economica"]
NUMERIC_COLS = [
    "pessoal_ocupado",
    "quantidade_estabelecimentos",
    "quantidade_estabelecimentos_produtivos",
    "valor_producao",
    "coeficiente_tecnico",
]

MODEL = PaCoeficientesEmpregoRuralCenso2017
log = get_logger(dataset_id=DATASET_ID, zone=ZONE)


def _read_gold(query: str) -> pd.DataFrame:
    with PostgresETL(
        host="localhost",
        database=os.getenv("DB_GOLD_ZONE"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        schema=SOURCE_SCHEMA,
    ) as db:
        return db.download_data(query)


def extract() -> tuple[pd.DataFrame, pd.DataFrame]:
    pessoal_ocupado = _read_gold(
        f"""
        SELECT ano, id_municipio, nome, nome_regiao_integracao, sigla_uf,
               grupo_atividade_economica, pessoal_ocupado,
               quantidade_estabelecimentos
        FROM {SOURCE_SCHEMA}.{PESSOAL_OCUPADO_TABLE}
        """
    )
    valor_producao = _read_gold(
        f"""
        SELECT ano, id_municipio, grupo_atividade_economica,
               quantidade_estabelecimentos_produtivos, valor_producao
        FROM {SOURCE_SCHEMA}.{VALOR_PRODUCAO_TABLE}
        """
    )
    return pessoal_ocupado, valor_producao


def _to_decimal(value: object) -> Decimal | None:
    if value is None or pd.isna(value):
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValueError(f"Invalid numeric value for technical coefficient: {value!r}") from exc


def transform(pessoal_ocupado: pd.DataFrame, valor_producao: pd.DataFrame) -> pd.DataFrame:
    for name, df in {
        "pessoal_ocupado": pessoal_ocupado,
        "valor_producao": valor_producao,
    }.items():
        if df.duplicated(KEY_COLS).any():
            raise ValueError(f"Duplicate coefficient keys in {name}")

    df = pessoal_ocupado.merge(
        valor_producao,
        on=KEY_COLS,
        how="outer",
        validate="one_to_one",
        indicator=True,
    )
    unmatched = df.loc[df["_merge"] != "both", KEY_COLS]
    if not unmatched.empty:
        raise ValueError(
            "Pessoal ocupado and VBP do not have the same coefficient keys: "
            f"{unmatched.to_dict('records')}"
        )
    df = df.drop(columns="_merge")

    def calculate_coefficient(row: pd.Series) -> Decimal | None:
        valor_producao = _to_decimal(row["valor_producao"])
        pessoal_ocupado = _to_decimal(row["pessoal_ocupado"])
        if valor_producao is None or pessoal_ocupado is None:
            return None
        if valor_producao <= 0 or pessoal_ocupado <= 0:
            return None
        return valor_producao / pessoal_ocupado

    df["coeficiente_tecnico"] = df.apply(calculate_coefficient, axis=1)
    return df[list(MODEL.model_fields.keys())].copy()


def validate(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        raise ValueError(f"transform produced empty dataframe for {TABLE}")
    for col in NUMERIC_COLS:
        df[col] = df[col].map(_to_decimal)
    [MODEL(**record) for record in df.to_dict("records")]
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
        pessoal_ocupado, valor_producao = extract()
        log.info(
            "extract.done",
            pessoal_ocupado_rows=len(pessoal_ocupado),
            valor_producao_rows=len(valor_producao),
        )
        df = transform(pessoal_ocupado, valor_producao)
        log.info("transform.done", rows=len(df), table=TABLE)
        df = validate(df)
        log.info("validate.done", rows=len(df), table=TABLE)
        load(df)
        log.info("load.done", rows=len(df), table=TABLE)
    except Exception as exc:
        log.exception("flow.error", error=str(exc), table=TABLE)
        raise
    log.info("flow.end", rows=len(df), table=TABLE)


if __name__ == "__main__":
    flow()
