"""Raw flow: RAIS — vínculos ativos (Pará, CNAE 1031700, regiões de integração).

Source: BigQuery ``basedosdados.br_me_rais.microdados_vinculos`` joined with
RAIS dicionario + CNAE 2 diretorio, restricted to PA and CNAE 1031700.
Lands into ``$DB_RAW_ZONE.pa_rf_rais.up_rais``.
"""

from __future__ import annotations

import os

import basedosdados as bd
import pandas as pd
from dotenv import load_dotenv

from dados.raw.utils.postgres_interactions import PostgresETL
from dados.utils.logging import get_logger

load_dotenv()

DATASET_ID = "pa_rf_rais"
ZONE = "raw"
TABLE = "up_rais"

QUERY = """
WITH
dicionario_vinculo_ativo_3112 AS (
    SELECT
        chave AS chave_vinculo_ativo_3112,
        valor AS descricao_vinculo_ativo_3112
    FROM `basedosdados.br_me_rais.dicionario`
    WHERE
        TRUE
        AND nome_coluna = 'vinculo_ativo_3112'
        AND id_tabela = 'microdados_vinculos'
), t2 as (
SELECT
    dados.ano as ano,
    dados.id_municipio AS id_municipio,
    CASE
        WHEN id_municipio IN ('1500347','1501253','1502707','1502764','1503044','1505437','1505551','1506138','1506161','1506583','1506708','1507300','1507755','1508084','1508308') THEN 'Araguaia'
        WHEN id_municipio IN ('1500404','1500503','1501451','1502855','1503002','1503903','1504752','1504802','1505007','1505106','1506005','1506807','1507979') THEN 'Baixo Amazonas'
        WHEN id_municipio IN ('1501576','1501758','1502152','1502772','1502954','1504208','1505494','1505536','1505635','1507151','1507458','1507508') THEN 'Carajás'
        WHEN id_municipio IN ('1501402','1500800','1501501','1504422','1506351') THEN 'Guajará'
        WHEN id_municipio IN ('1502400','1502608','1502905','1503200','1503408','1504000','1504307','1504406','1506500','1506609','1507003','1507102','1507201','1507409','1507466','1507607','1507961','1508209') THEN 'Guamá'
        WHEN id_municipio IN ('1501782','1503093','1503705','1503804','1504976','1505064','1508100') THEN 'Lago de Tucuruí'
        WHEN id_municipio IN ('1500305','1500701','1501105','1501808','1502004','1502509','1502806','1503101','1504505','1504901','1505205','1505700','1505809','1506302','1506401','1507706','1507904') THEN 'Marajó'
        WHEN id_municipio IN ('1500909','1501600','1501709','1501956','1502202','1505304','1505601','1506104','1506112','1506203','1506559','1506906','1507474','1508035','1508407') THEN 'Rio Caeté'
        WHEN id_municipio IN ('1500131','1500958','1501907','1502301','1502756','1502939','1503077','1503457','1503507','1504059','1504950','1505403','1505502','1506187','1508001','1508126') THEN 'Rio Capim'
        WHEN id_municipio IN ('1501006','1503606','1503754','1505031','1506195','1508050') THEN 'Tapajós'
        WHEN id_municipio IN ('1500107','1500206','1501204','1501303','1502103','1503309','1504109','1504604','1504703','1507953') THEN 'Tocantins'
        WHEN id_municipio IN ('1500602','1500859','1501725','1504455','1505486','1505650','1505908','1507805','1508159','1508357') THEN 'Xingu'
        ELSE 'Não classificado'
    END AS regiao_integracao,
    descricao_vinculo_ativo_3112 AS vinculo_ativo_3112,
    dados.valor_remuneracao_dezembro as valor_remuneracao_dezembro,
    dados.cnae_2_subclasse AS cnae_2_subclasse,
    diretorio_cnae_2_subclasse.descricao_subclasse AS cnae_2_subclasse_descricao_subclasse,
    diretorio_cnae_2_subclasse.descricao_secao AS cnae_2_subclasse_descricao_secao
FROM `basedosdados.br_me_rais.microdados_vinculos` AS dados
LEFT JOIN `dicionario_vinculo_ativo_3112`
    ON dados.vinculo_ativo_3112 = chave_vinculo_ativo_3112
LEFT JOIN (SELECT DISTINCT subclasse, descricao_subclasse, descricao_secao FROM `basedosdados.br_bd_diretorios_brasil.cnae_2`) AS diretorio_cnae_2_subclasse
    ON dados.cnae_2_subclasse = diretorio_cnae_2_subclasse.subclasse
WHERE sigla_uf = 'PA'
  AND ano > 1997
  AND vinculo_ativo_3112 = '1'
  AND cnae_2_subclasse = '1031700'
)
SELECT
    t2.ano,
    t2.regiao_integracao,
    t2.cnae_2_subclasse,
    t2.cnae_2_subclasse_descricao_subclasse,
    t2.cnae_2_subclasse_descricao_secao,
    ROUND(AVG(valor_remuneracao_dezembro),2) as remuneracao_media_dezembro,
    COUNT(*) as quantidade_vinculos_ativos,
    ROUND(AVG(valor_remuneracao_dezembro) * COUNT(*),2) as massa_salarial
FROM t2
GROUP BY ALL;
"""

COLUMNS_DDL = {
    "ano": "INTEGER",
    "regiao_integracao": "VARCHAR(255)",
    "cnae_2_subclasse": "VARCHAR(255)",
    "cnae_2_subclasse_descricao_subclasse": "TEXT",
    "cnae_2_subclasse_descricao_secao": "TEXT",
    "remuneracao_media_dezembro": "NUMERIC",
    "quantidade_vinculos_ativos": "INTEGER",
    "massa_salarial": "NUMERIC",
}

log = get_logger(dataset_id=DATASET_ID, zone=ZONE)


def extract() -> pd.DataFrame:
    billing_id = os.getenv("BASEDOSDADADOS_PROJECT_ID")
    log.info("extract.bq.start")
    df = bd.read_sql(query=QUERY, billing_project_id=billing_id)
    log.info("extract.bq.done", rows=len(df))
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
