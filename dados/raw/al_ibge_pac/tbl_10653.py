"""Raw flow: IBGE PAC — tabela 10653 (Pesquisa Anual de Comércio, série nova).

Source: IBGE Agregados API, agregado 10653 — "Dados gerais das empresas
comerciais por grandes regiões e unidades da federação de atuação das empresas e
divisão de comércio e grupo de atividade (série nova)".

O IBGE rompeu a série da PAC no ano de referência 2024 (Nota Técnica PAC
01/2026: mudança na classificação de empresas inativas e migração do Cempre da
RAIS para o eSocial). Os dados até 2023 continuam no agregado 1407
(``tbl_1407.py``) e **não são diretamente comparáveis** com os desta tabela.

Diferença estrutural em relação a 1407: a geografia deixa de ser a classificação
12354 e passa a vir em ``localidades`` (N1/N2/N3); sobra só a classificação
11066 (divisão de comércio e grupo de atividade), com as mesmas 23 categorias.

Lands one row per (localidade, comercio, ano, variavel) into
``$DB_RAW_ZONE.al_ibge_pac.tbl_10653``.
"""

from __future__ import annotations

import os

import pandas as pd
import requests
from dotenv import load_dotenv

from dados.raw.utils.agregados_parser import COLUNAS, parse_agregado_json_to_table
from dados.raw.utils.postgres_interactions import PostgresETL
from dados.utils.logging import get_logger

load_dotenv()

DATASET_ID = "al_ibge_pac"
ZONE = "raw"
TABLE = "tbl_10653"
AGREGADO = "10653"

# Mesmas variáveis da série antiga (1407): unidades locais, pessoal ocupado,
# salários, margem de comercialização e receita bruta de revenda.
VARIAVEIS = "368|312|314|503|866"
LOCALIDADES = "N1[all]|N2[all]|N3[all]"
CLASSIFICACAO = "11066[all]"
PERIODOS = "2024"

URL = (
    f"https://servicodados.ibge.gov.br/api/v3/agregados/{AGREGADO}/periodos/"
    f"{{}}/variaveis/{VARIAVEIS}?localidades={LOCALIDADES}"
    f"&classificacao={CLASSIFICACAO}"
)

COLUMNS_DDL = {coluna: "VARCHAR(255)" for coluna in COLUNAS}

log = get_logger(dataset_id=DATASET_ID, zone=ZONE)


def extract() -> pd.DataFrame:
    log.info("extract.api.start", agregado=AGREGADO, periodos=PERIODOS)
    resposta = requests.get(URL.format(PERIODOS), timeout=600)
    resposta.raise_for_status()
    df = parse_agregado_json_to_table(resposta.json())
    log.info("extract.api.done", rows=len(df))
    return df


def validate(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        log.error("validate.error", reason="empty_dataframe")
        raise ValueError("extract produced an empty dataframe")
    missing = set(COLUMNS_DDL.keys()) - set(df.columns)
    if missing:
        log.error("validate.error", missing_columns=sorted(missing))
        raise ValueError(f"Missing required columns: {sorted(missing)}")
    anos = sorted(df["ano"].unique().tolist())
    if min(anos) < 2024:
        log.error("validate.error", reason="ano_anterior_a_serie_nova", anos=anos)
        raise ValueError(f"serie nova comeca em 2024; recebido: {anos}")
    return df


def transform(df: pd.DataFrame) -> pd.DataFrame:
    return df[list(COLUMNS_DDL.keys())]


def load(df: pd.DataFrame) -> None:
    with PostgresETL(
        host="localhost",
        database=os.getenv("DB_RAW_ZONE"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        schema=DATASET_ID,
    ) as db:
        db.create_table(TABLE, COLUMNS_DDL, if_not_exists=True)
        db.load_data(TABLE, df, if_exists="replace")


def flow() -> None:
    log.info("flow.start", table=TABLE)
    try:
        df = extract()
        log.info("extract.done", rows=len(df))
        df = validate(df)
        log.info("validate.done", rows=len(df))
        df = transform(df)
        log.info("transform.done", rows=len(df))
        load(df)
        log.info("load.done", rows=len(df))
    except Exception as exc:
        log.exception("flow.error", error=str(exc))
        raise
    log.info("flow.end", rows=len(df))


if __name__ == "__main__":
    flow()
