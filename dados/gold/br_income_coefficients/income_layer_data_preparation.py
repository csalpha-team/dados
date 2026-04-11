from dotenv import load_dotenv
import os
from pathlib import Path

from dados.raw.utils.postgres_interactions import PostgresETL
from dados.gold.br_income_coefficients.utils import (
    build_income_output_table,
    load_income_parameters,
    prepare_income_coefficients_data,
)


load_dotenv()

DATASET_ID = "br_income_coefficients"
TABLE_ID = "income_layer_data_preparation"
PRODUCTIVITY_TABLE_ID = "income_productivity"
SALARY_TABLE_ID = "income_salary"
CONFIG_PATH = Path(__file__).with_name("income_coefficients_parameters.json")

PIA_SOURCE_SCHEMA = os.getenv("INCOME_PIA_SOURCE_SCHEMA", "br_ibge_pia")
PIA_SOURCE_TABLE = os.getenv("INCOME_PIA_SOURCE_TABLE", "tbl_1849")
PAC_SOURCE_SCHEMA = os.getenv("INCOME_PAC_SOURCE_SCHEMA", "br_ibge_pac")
PAC_SOURCE_TABLE = os.getenv("INCOME_PAC_SOURCE_TABLE", "tbl_1407")

(
    sector_mappings,
    years,
    aa_production_values,
    forecast_config,
) = load_income_parameters(CONFIG_PATH)

pia_query = f"""
select
    ano,
    nome_localidade,
    divisao_grupo_cnae_2,
    pessoal_ocupado_31_12,
    valor_bruto_producao_industrial,
    valor_salarios_remuneracoes
from {PIA_SOURCE_SCHEMA}.{PIA_SOURCE_TABLE}
"""

pac_query = f"""
select
    ano,
    unidade_geografica,
    divisao_grupo_cnae_2,
    valor_receita_bruta_revenda,
    pessoal_ocupado_31_12,
    margem_comercializacao,
    valor_gastos_salarios_remuneracoes
from {PAC_SOURCE_SCHEMA}.{PAC_SOURCE_TABLE}
"""

with PostgresETL(
    host="localhost",
    database=os.getenv("DB_SILVER_ZONE"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
    schema=PIA_SOURCE_SCHEMA,
) as db:
    pia_data = db.download_data(pia_query)

with PostgresETL(
    host="localhost",
    database=os.getenv("DB_SILVER_ZONE"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
    schema=PAC_SOURCE_SCHEMA,
) as db:
    pac_data = db.download_data(pac_query)

coefficients_data = prepare_income_coefficients_data(
    pia_data,
    pac_data,
    sector_mappings=sector_mappings,
    years=years,
    aa_production_values=aa_production_values,
    forecast_config=forecast_config,
)

productivity_data = build_income_output_table(coefficients_data, "prod_mon_trab")
salary_data = build_income_output_table(coefficients_data, "salario_medio")

with PostgresETL(
    host="localhost",
    database=os.getenv("DB_GOLD_ZONE"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
    schema=DATASET_ID,
) as db:
    columns = {
        "ano": "integer",
        "conta_alfa": "VARCHAR(255)",
        "tipo_coeff": "VARCHAR(255)",
        "coeff": "numeric",
    }
    output_columns = {
        "ano": "integer",
        "conta_alfa": "VARCHAR(255)",
        "coeff": "numeric",
    }

    db.create_table(TABLE_ID, columns, drop_if_exists=True)
    db.load_data(TABLE_ID, coefficients_data, if_exists="replace")
    db.create_table(PRODUCTIVITY_TABLE_ID, output_columns, drop_if_exists=True)
    db.load_data(PRODUCTIVITY_TABLE_ID, productivity_data, if_exists="replace")
    db.create_table(SALARY_TABLE_ID, output_columns, drop_if_exists=True)
    db.load_data(SALARY_TABLE_ID, salary_data, if_exists="replace")
