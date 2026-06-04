"""Raw flow: IBGE PEVS — produção da extração vegetal (Amazônia Legal).

Source: IBGE Agregados API, table 289, classification 193 (produtos da extração vegetal).
Lands one row per (municipio, produto, ano, variavel) into
``$DB_RAW_ZONE.al_ibge_pevs.produtos_extracao_vegetal``.
"""

from __future__ import annotations

import asyncio
import json
import os

import basedosdados as bd
import pandas as pd
from dotenv import load_dotenv

from dados.raw.al_ibge_pam.utils import parse_pam_json
from dados.raw.utils.ibge_api_crawler import (
    async_crawler_ibge_municipio,
    get_classificacao_unidades,
)
from dados.raw.utils.postgres_interactions import PostgresETL
from dados.utils.logging import get_logger
from dados.utils.paths import tmp_dir

load_dotenv()

DATASET_ID = "al_ibge_pevs"
ZONE = "raw"
TABLE = "produtos_extracao_vegetal"

API_URL_BASE = (
    "https://servicodados.ibge.gov.br/api/v3/agregados/{}/periodos/{}/variaveis/{}"
    "?localidades={}[{}]&classificacao={}"
)
AGREGADO = "289"
PERIODOS = "all"
VARIAVEIS = "|".join(["144", "145"])
NIVEL_GEOGRAFICO = "N6"
CLASSIFICACAO = "193[all]"
ID_PRODUTO_CLASSIFICACAO = "193"
ID_VARIAVEL_QUANTIDADE = "144"
EXPECTED_MUNICIPIOS = 773  # Amazônia Legal

COLUMNS_DDL = {
    "id_variavel": "VARCHAR(255)",
    "nome_variavel": "VARCHAR(255)",
    "unidade_medida": "VARCHAR(255)",
    "id_produto": "VARCHAR(255)",
    "produto": "VARCHAR(255)",
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
    log.info("extract.api.done", files=len(files))

    dfs = []
    for file in files:
        with open(input_dir / file) as f:
            data = json.load(f)
        dfs.append(parse_pam_json(data, id_produto=ID_PRODUTO_CLASSIFICACAO))
    df = pd.concat(dfs, ignore_index=True)

    # A unidade de medida da PEVS é definida por produto (categoria), não pela
    # variável: a variável 144 traz apenas "Vide categorias da classificação...".
    # Enriquecemos a quantidade com a unidade correta vinda dos metadados,
    # mantendo a unidade de moeda já presente nas linhas de valor (variável 145).
    unidades = get_classificacao_unidades(AGREGADO, ID_PRODUTO_CLASSIFICACAO)
    qty_mask = df["id_variavel"].astype(str) == ID_VARIAVEL_QUANTIDADE
    df.loc[qty_mask, "unidade_medida"] = df.loc[qty_mask, "id_produto"].map(unidades)
    # Produtos sem unidade nos metadados (ex. 'Total') ficam None → NULL no Postgres
    # (evita gravar a string 'NaN' ao serializar via load_data).
    df["unidade_medida"] = df["unidade_medida"].where(
        df["unidade_medida"].notna(), None
    )
    log.info("extract.unidades.done", produtos=len(unidades))
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
