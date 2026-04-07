from dotenv import load_dotenv
import os
from pathlib import Path

from dados.raw.utils.postgres_interactions import PostgresETL
from dados.gold.br_export_coefficients.utils import (
    build_export_query,
    load_export_parameters,
    prepare_export_coefficients_data,
    save_export_coefficients_json,
)


load_dotenv()

DATASET_ID = "br_export_coefficients"
TABLE_ID = "export_layer_data_preparation"

SOURCE_DATABASE = os.getenv("EXPORT_SOURCE_DATABASE") or os.getenv("DB_RAW_ZONE")
SOURCE_SCHEMA = os.getenv("EXPORT_SOURCE_SCHEMA", "al_me_comex_stat")
SOURCE_TABLE = os.getenv("EXPORT_SOURCE_TABLE", "comex_stat")
NCM_SCHEMA = os.getenv("EXPORT_NCM_SCHEMA", "br_csalpha_diretorios_brasil")
NCM_TABLE = os.getenv("EXPORT_NCM_TABLE", "nomenclatura_comum_mercosul")

CONFIG_PATH = Path(__file__).with_name("export_coefficients_parameters.json")

(
    products_preparations,
    specific_shares,
    years,
    exchange_rate_brl_per_usd,
    target_state,
) = load_export_parameters(CONFIG_PATH)

if not SOURCE_DATABASE:
    raise ValueError(
        "Banco de origem não configurado. Defina EXPORT_SOURCE_DATABASE ou DB_RAW_ZONE."
    )

with PostgresETL(
    host="localhost",
    database=SOURCE_DATABASE,
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
    schema=SOURCE_SCHEMA,
) as db:
    source_query = build_export_query(
        db,
        source_schema=SOURCE_SCHEMA,
        source_table=SOURCE_TABLE,
        ncm_schema=NCM_SCHEMA,
        ncm_table=NCM_TABLE,
    )
    export_data = db.download_data(source_query)

coefficients_data = prepare_export_coefficients_data(
    export_data,
    products_preparations=products_preparations,
    specific_shares=specific_shares,
    years=years,
    exchange_rate_brl_per_usd=exchange_rate_brl_per_usd,
    target_state=target_state,
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
        "produto": "VARCHAR(255)",
        "valor_fob_dolar": "numeric",
        "valor_fob_real": "numeric",
        "coeff": "numeric",
    }

    db.create_table(TABLE_ID, columns, drop_if_exists=True)
    db.load_data(TABLE_ID, coefficients_data, if_exists="replace")

output_json_path_env = os.getenv("EXPORT_COEFFICIENTS_OUTPUT_JSON_PATH")
if output_json_path_env:
    save_export_coefficients_json(coefficients_data, Path(output_json_path_env))
