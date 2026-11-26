"""Silver flow: IBGE PIA — tabela 10454 (série nova, 2024 em diante).

Par da ``tbl_1848`` na série nova: mesmo recorte CNAE fino (divisão **e** grupo,
143 categorias), assim como a 10457 é o par da 1849 (só divisão). Filtra o nível
UF da aterrissagem raw, pivota os indicadores e materializa em
``$DB_SILVER_ZONE.br_ibge_pia.tbl_10454``.

Difere da 1848 nas variáveis: 706 vira 13816 ("Número de unidades locais de
empresas com 5 ou mais pessoas ocupadas") e entra 2086 ("Número de empresas com
5 ou mais pessoas ocupadas"). A coluna ``serie`` fica fixa em "nova" para que o
gold nunca misture as duas séries sem querer (Nota Técnica PIA-Empresa 01/2026):
os valores **não são comparáveis** com os da 1848.

Executa em lotes de anos com publicação atômica (ver ``dados/utils/batching``):
a tabela destino só é trocada se todos os lotes passarem.
"""

from __future__ import annotations

import os
from decimal import Decimal
from typing import Sequence

import pandas as pd
from dotenv import load_dotenv

from dados.raw.utils.postgres_interactions import PostgresETL
from dados.silver.br_ibge_pia.models import BrIbgePiaTbl10454
from dados.utils.batching import (
    CargaEmStaging,
    executar_em_lotes_por_ano,
    listar_anos,
)
from dados.utils.logging import get_logger
from dados.utils.pydantic_postgres import pydantic_to_postgres_columns

load_dotenv()

DATASET_ID = "br_ibge_pia"
ZONE = "silver"
TABLE = "tbl_10454"
SERIE = "nova"
ANOS_POR_LOTE = 5

PK_COLS = ["ano", "nome_localidade", "divisao_grupo_cnae_2"]
NULL_TOKENS = ("..", "...", "-", "X")

COLUMN_RENAME = {
    "nome_categoria": "divisao_grupo_cnae_2",
    "Custos com consumo de matérias-primas, materiais auxiliares e componentes": "custos_materias_primas",
    "Encargos sociais e trabalhistas, indenizações e benefícios": "encargos_sociais_trabalhistas",
    "Número de empresas com 5 ou mais pessoas ocupadas": "quantidade_empresas",
    "Número de unidades locais de empresas com 5 ou mais pessoas ocupadas": "quantidade_unidades_locais",
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


def extract(db: PostgresETL, anos: Sequence[int]) -> pd.DataFrame:
    query = f"""
        SELECT
            nome_variavel,
            nome_categoria,
            nome_localidade,
            CAST(ano AS INTEGER) AS ano,
            valor
        FROM {DATASET_ID}.{TABLE}
        WHERE nivel_nome = 'Unidade da Federação'
          AND CAST(ano AS INTEGER) = ANY(%s)
    """
    return db.download_data(query, params=(list(anos),))


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
    df["serie"] = SERIE
    return df[list(BrIbgePiaTbl10454.model_fields.keys())]


def validate(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        log.error("validate.error", reason="empty_dataframe")
        raise ValueError("transform produced an empty dataframe")

    dupes = df.duplicated(subset=PK_COLS, keep=False)
    if dupes.any():
        log.error("validate.error", reason="duplicate_pk", count=int(dupes.sum()))
        raise ValueError(f"Found {int(dupes.sum())} rows duplicating PK {PK_COLS}")

    for col in [c for c in df.columns if c not in PK_COLS + ["serie"]]:
        df[col] = df[col].apply(lambda v: None if pd.isna(v) else Decimal(str(v)))

    [BrIbgePiaTbl10454(**r) for r in df.to_dict("records")]
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
            anos = listar_anos(origem, TABLE)
            log.info("extract.anos", anos=len(anos), de=min(anos), ate=max(anos))

            columns = pydantic_to_postgres_columns(BrIbgePiaTbl10454)
            with CargaEmStaging(destino, TABLE, columns) as carga:
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
