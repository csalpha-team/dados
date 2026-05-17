"""Raw flow: IBGE Censo Agropecuário 2006 — tabela 1909 (despesas por tipo de agricultura).

Source: IBGE Agregados API, agregado 1909, periodo 2006.
Lands one row per (municipio, despesa, tipo_agricultura, ano) into
``$DB_RAW_ZONE.al_ibge_censoagro.tbl_1909_2006``.
"""

from __future__ import annotations

import asyncio
import json
import os

import basedosdados as bd
import pandas as pd
from dotenv import load_dotenv

from dados.raw.al_ibge_censoagro.utils import parse_agrocenso_json
from dados.raw.utils.ibge_api_crawler import async_crawler_ibge_municipio
from dados.raw.utils.postgres_interactions import PostgresETL
from dados.utils.logging import get_logger
from dados.utils.paths import tmp_dir

load_dotenv()

DATASET_ID = "al_ibge_censoagro"
ZONE = "raw"
TABLE = "tbl_1909_2006"

API_URL_BASE = (
    "https://servicodados.ibge.gov.br/api/v3/agregados/{}/periodos/{}/variaveis/{}"
    "?localidades={}[{}]&classificacao={}"
)
AGREGADO = "1909"
PERIODOS = "2006"
VARIAVEIS = "|".join(["2", "1000002", "1996", "1001996"])
NIVEL_GEOGRAFICO = "N6"
CLASSIFICACAO = "210[all]|12896[all]"
ID_PRODUTO_CLASSIFICACAO = "210"
ID_TIPO_AGRICULTURA_CLASSIFICACAO = "12896"
EXPECTED_MUNICIPIOS = 773

COLUMNS_DDL = {
    "id_variavel": "VARCHAR(255)",
    "nome_variavel": "VARCHAR(255)",
    "unidade_medida": "VARCHAR(255)",
    "id_despesa": "VARCHAR(255)",
    "despesa": "VARCHAR(255)",
    "id_tipo_agricultura": "VARCHAR(255)",
    "tipo_agricultura": "VARCHAR(255)",
    "nome_municipio": "VARCHAR(255)",
    "id_municipio": "VARCHAR(255)",
    "ano": "VARCHAR(255)",
    "valor": "VARCHAR(255)",
}

log = get_logger(dataset_id=DATASET_ID, zone=ZONE)


def extract() -> pd.DataFrame:
    input_dir = tmp_dir(DATASET_ID, "input") / TABLE
    input_dir.mkdir(parents=True, exist_ok=True)

    billing_id = os.getenv("BASEDOSDADADOS_PROJECT_ID")
    log.info("extract.municipios.start")
    municipios = bd.read_sql(
        """
        SELECT id_municipio
        FROM `basedosdados.br_bd_diretorios_brasil.municipio`
        WHERE amazonia_legal = 1
        """,
        billing_project_id=billing_id,
    )
    log.info("extract.municipios.done", rows=len(municipios))

    log.info(
        "extract.api.start",
        agregado=AGREGADO,
        variaveis=VARIAVEIS,
        output_dir=str(input_dir),
    )
    asyncio.run(
        async_crawler_ibge_municipio(
            year=PERIODOS,
            variables=VARIAVEIS,
            api_url_base=API_URL_BASE,
            agregado=AGREGADO,
            nivel_geografico=NIVEL_GEOGRAFICO,
            localidades=municipios,
            classificacao=CLASSIFICACAO,
            nome_tabela=TABLE,
            output_dir=input_dir,
        )
    )

    files = os.listdir(input_dir)
    if len(files) != EXPECTED_MUNICIPIOS:
        log.error(
            "extract.error",
            expected=EXPECTED_MUNICIPIOS,
            got=len(files),
            input_dir=str(input_dir),
        )
        raise AssertionError(
            f"Expected {EXPECTED_MUNICIPIOS} JSON files in {input_dir}, got {len(files)}"
        )
    log.info("extract.api.done", files=len(files))

    dfs = []
    for file in files:
        with open(input_dir / file) as f:
            data = json.load(f)
        dfs.append(
            parse_agrocenso_json(
                data,
                id_produto=ID_PRODUTO_CLASSIFICACAO,
                id_tipo_agricultura=ID_TIPO_AGRICULTURA_CLASSIFICACAO,
            )
        )
    df = pd.concat(dfs, ignore_index=True)
    df = df.rename(columns={"id_produto": "id_despesa", "produto": "despesa"})
    return df


def validate(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        log.error("validate.error", reason="empty_dataframe")
        raise ValueError("extract produced an empty dataframe")
    missing = set(COLUMNS_DDL.keys()) - set(df.columns)
    if missing:
        log.error("validate.error", missing_columns=sorted(missing))
        raise ValueError(f"Missing required columns: {sorted(missing)}")
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
