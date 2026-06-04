"""Silver flow: BR diretorios — Regiões de Integração do Pará.

Reads the raw name→region transcription from
``$DB_RAW_ZONE.br_csalpha_diretorios_brasil.regioes_integracao`` and joins the
IBGE municipality directory (BigQuery ``basedosdados.br_bd_diretorios_brasil.
municipio``, filtered to Pará) by a normalized municipality name to attach
``id_municipio``, the canonical ``nome`` and ``sigla_uf``.

Lands at ``$DB_SILVER_ZONE.br_csalpha_diretorios_brasil.regioes_integracao``.
"""

from __future__ import annotations

import os
import unicodedata

import basedosdados as bd
import pandas as pd
from dotenv import load_dotenv

from dados.raw.utils.postgres_interactions import PostgresETL
from dados.silver.br_csalpha_diretorios_brasil.models import RegioesIntegracao
from dados.utils.logging import get_logger
from dados.utils.pydantic_postgres import pydantic_to_postgres_columns

load_dotenv()

DATASET_ID = "br_csalpha_diretorios_brasil"
ZONE = "silver"
TABLE = "regioes_integracao"
RAW_TABLE = "regioes_integracao"

TOTAL_MUNICIPIOS_PA = 144

_MUNICIPIOS_QUERY = """
    SELECT id_municipio, nome, sigla_uf
    FROM `basedosdados.br_bd_diretorios_brasil.municipio`
    WHERE sigla_uf = 'PA'
"""

log = get_logger(dataset_id=DATASET_ID, zone=ZONE)


def _normalize(nome: pd.Series) -> pd.Series:
    """Casefold, strip accents and normalize apostrophes/whitespace for joining."""
    s = nome.astype(str).str.replace("’", "'", regex=False).str.strip().str.lower()
    s = s.map(
        lambda v: "".join(
            c for c in unicodedata.normalize("NFKD", v) if not unicodedata.combining(c)
        )
    )
    return s.str.replace(r"\s+", " ", regex=True)


def extract() -> pd.DataFrame:
    query = f"""
        SELECT nome_municipio, nome_regiao_integracao
        FROM {DATASET_ID}.{RAW_TABLE}
    """
    with PostgresETL(
        host="localhost",
        database=os.getenv("DB_RAW_ZONE"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        schema=DATASET_ID,
    ) as db:
        return db.download_data(query)


def _fetch_municipios() -> pd.DataFrame:
    log.info("extract.municipios.start", source="basedosdados")
    df = bd.read_sql(
        _MUNICIPIOS_QUERY,
        billing_project_id=os.getenv("BASEDOSDADADOS_PROJECT_ID"),
    )
    log.info("extract.municipios.done", rows=len(df))
    return df


def transform(df: pd.DataFrame) -> pd.DataFrame:
    municipios = _fetch_municipios()

    df = df.copy()
    municipios = municipios.copy()
    df["_key"] = _normalize(df["nome_municipio"])
    municipios["_key"] = _normalize(municipios["nome"])

    merged = df.merge(municipios, on="_key", how="left")
    return merged[list(RegioesIntegracao.model_fields.keys())]


def validate(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        log.error("validate.error", reason="empty_dataframe")
        raise ValueError("transform produced an empty dataframe")

    unmatched = int(df["id_municipio"].isna().sum())
    if unmatched:
        log.error("validate.error", reason="unmatched_municipio", count=unmatched)
        raise ValueError(f"{unmatched} municipalities did not match the directory")

    if len(df) != TOTAL_MUNICIPIOS_PA:
        log.error("validate.error", reason="unexpected_count", count=len(df))
        raise ValueError(
            f"Expected {TOTAL_MUNICIPIOS_PA} municipalities, got {len(df)}"
        )

    dupes = df.duplicated(subset=["id_municipio"], keep=False)
    if dupes.any():
        log.error(
            "validate.error", reason="duplicate_id_municipio", count=int(dupes.sum())
        )
        raise ValueError(f"Found {int(dupes.sum())} rows duplicating id_municipio")

    [RegioesIntegracao(**r) for r in df.to_dict("records")]
    return df


def load(df: pd.DataFrame) -> None:
    columns = pydantic_to_postgres_columns(RegioesIntegracao)
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
