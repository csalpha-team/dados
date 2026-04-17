from dotenv import load_dotenv
import os
from pathlib import Path

from dados.raw.utils.postgres_interactions import PostgresETL
from dados.gold.br_coeficientes_investimento.utils import carregar_coeficientes_investimento


load_dotenv()

DATASET_ID = "br_coeficientes_investimento"
TABLE_ID = "coeficientes_investimento"
DEFAULT_JSON_PATH = Path(__file__).with_name("coeficientes_investimento.json")

json_path_env = os.getenv("INVESTMENT_COEFFICIENTS_JSON_PATH") or os.getenv(
    "COEFICIENTES_INVESTIMENTO_JSON_PATH"
)
json_path = Path(json_path_env) if json_path_env else DEFAULT_JSON_PATH

coefficients_data = carregar_coeficientes_investimento(json_path)

with PostgresETL(
    host="localhost",
    database=os.getenv("DB_GOLD_ZONE"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
    schema=DATASET_ID,
) as db:
    columns = {
        "coeff_key": "VARCHAR(255)",
        "coeff": "numeric",
    }

    db.create_table(TABLE_ID, columns, drop_if_exists=True)
    db.load_data(TABLE_ID, coefficients_data, if_exists="replace")
