"""Publica coeficientes exogenos de investimento na zona gold.

Os valores versionados em `coeficientes_investimento.json` nao sao calculados
por este ETL. Eles sao parametros exogenos derivados de percentuais de custos
estimados em pesquisa anterior conduzida pelo professor Francisco de Assis
Costa. Esses percentuais expressam a necessidade de investimentos observada no
balanco patrimonial, sobretudo em veiculos, construcao civil e benfeitorias, e
maquinas e equipamentos.

Esta camada existe para versionar esses percentuais no repositorio, carrega-los
como pares `coeff_key`/`coeff` e publica-los na tabela
`br_coeficientes_investimento.coeficientes_investimento`. O conjunto tambem
preserva `InvestPlantio`, usado como coeficiente especifico em etapas que
tratam formacao de plantio.
"""

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

# O JSON eh a fonte versionada dos percentuais consolidados na pesquisa de
# referencia. Esta etapa apenas valida a estrutura e publica os coeficientes.
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
