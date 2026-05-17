"""Silver flow: IBGE PIA — table 1849 (industrial indicators by UF/CNAE).

Filters to UF-level rows from the raw landing, pivots the indicators wide, and
lands the result at ``$DB_SILVER_ZONE.br_ibge_pia.tbl_1849``.
"""
from __future__ import annotations

import os
from decimal import Decimal

import pandas as pd
from dotenv import load_dotenv

from dados.raw.utils.postgres_interactions import PostgresETL
from dados.silver.models.br_ibge_pia import BrIbgePiaTbl1849
from dados.utils.logging import get_logger
from dados.utils.pydantic_postgres import pydantic_to_postgres_columns

load_dotenv()

DATASET_ID = "br_ibge_pia"
ZONE = "silver"
TABLE = "tbl_1849"

PK_COLS = ["ano", "nome_localidade", "divisao_grupo_cnae_2"]
NULL_TOKENS = ("..", "...", "-", "X")

COLUMN_RENAME = {
    "nome_categoria": "divisao_grupo_cnae_2",
    "Custos com consumo de matérias-primas, materiais auxiliares e componentes": "custos_materias_primas",
    "Encargos sociais e trabalhistas, indenizações e benefícios": "encargos_sociais_trabalhistas",
    "Número de unidades locais": "quantidade_unidades_locais",
    "Pessoal ocupado em 31/12": "pessoal_ocupado_31_12",
    "Receita líquida de vendas de atividades industriais": "receita_liquida_vendas_industriais",
    "Receita líquida de vendas de atividades não industriais": "receita_liquida_vendas_nao_industriais",
    "Salários, retiradas e outras remunerações": "valor_salarios_remuneracoes",
    "Total de custos das operações industriais": "valor_custos_operacoes_industriais",
    "Total de custos e despesas": "valor_custos_despesas",
    "Total de receitas líquidas de vendas": "valor_receitas_liquidas_vendas",
    "Valor bruto da produção industrial": "valor_bruto_producao_industrial",
    "Valor da transformação industrial": "valor_transformacao_industrial",
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
        FROM {DATASET_ID}.{TABLE}
        WHERE nivel_nome = 'Unidade da Federação'
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
    return df[list(BrIbgePiaTbl1849.model_fields.keys())]


def validate(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        log.error("validate.error", reason="empty_dataframe")
        raise ValueError("transform produced an empty dataframe")

    dupes = df.duplicated(subset=PK_COLS, keep=False)
    if dupes.any():
        log.error("validate.error", reason="duplicate_pk", count=int(dupes.sum()))
        raise ValueError(f"Found {int(dupes.sum())} rows duplicating PK {PK_COLS}")

    for col in [c for c in df.columns if c not in PK_COLS]:
        df[col] = df[col].apply(lambda v: None if pd.isna(v) else Decimal(str(v)))

    [BrIbgePiaTbl1849(**r) for r in df.to_dict("records")]
    return df


def load(df: pd.DataFrame) -> None:
    columns = pydantic_to_postgres_columns(BrIbgePiaTbl1849)
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
        df = extract();    log.info("extract.done", rows=len(df))
        df = transform(df);log.info("transform.done", rows=len(df))
        df = validate(df); log.info("validate.done", rows=len(df))
        load(df);          log.info("load.done", rows=len(df))
    except Exception as exc:
        log.exception("flow.error", error=str(exc))
        raise
    log.info("flow.end", rows=len(df))


if __name__ == "__main__":
    flow()
