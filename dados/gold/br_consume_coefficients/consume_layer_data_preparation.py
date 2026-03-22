from dotenv import load_dotenv
import os
from pathlib import Path
from dados.raw.utils.postgres_interactions import PostgresETL
from dados.gold.br_consume_coefficients.utils import (
    build_consumption_coefficients,
    load_mip_mapping,
)


load_dotenv()

DATASET_ID = "br_consume_coefficients"
TABLE_ID = "consume_layer_data_preparation"
SOURCE_SCHEMA = "brasil_despesas_familiares"
SOURCE_TABLE = "pof_2018_despesas_familiares_situacao_domicilio"
DEFAULT_MIP_PATH = Path(__file__).with_name("mip_coefficients.xlsx")

CONSUMPTION_PARAMETERS = {
    "mip_sheet_name": "DespesasDosSalários",
    "mip_coeff_key_column": "TipoDespesaDestinoProvável",
    "mip_expense_type_column": "TiposDeDespesa",
    "target_variable": "Distribuição da despesa monetária e não monetária média mensal familiar",
    "target_year": 2018,
    "urban_label": "Urbana",
    "rural_label": "Rural",
    "state_pattern": "Estad|Estadual",
}

mip_path_env = os.getenv("MIP_CONSUMPTION_FILE_PATH")
mip_path = Path(mip_path_env) if mip_path_env else DEFAULT_MIP_PATH

mip_mapping = load_mip_mapping(mip_path, CONSUMPTION_PARAMETERS["mip_sheet_name"])

query = f"""
select
    ano,
    variavel,
    situacao_domicilio,
    tipo_despesa,
    valor,
    unidade_medida
from {SOURCE_SCHEMA}.{SOURCE_TABLE}
"""

with PostgresETL(
    host="localhost",
    database=os.getenv("DB_GOLD_ZONE"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
    schema=SOURCE_SCHEMA,
) as db:
    pof_data = db.download_data(query)

coefficients_data = build_consumption_coefficients(
    pof_data,
    mip_mapping,
    CONSUMPTION_PARAMETERS,
)

with PostgresETL(
    host="localhost",
    database=os.getenv("DB_GOLD_ZONE"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
    schema=DATASET_ID,
) as db:
    columns = {
        "ano": "integer",
        "coeff_key": "VARCHAR(255)",
        "coeff": "numeric",
    }

    db.create_table(TABLE_ID, columns, drop_if_exists=True)
    db.load_data(TABLE_ID, coefficients_data, if_exists="replace")
