from dotenv import load_dotenv
import os
from pathlib import Path
import pandas as pd
from dados.raw.utils.postgres_interactions import PostgresETL
from dados.gold.br_coeficientes_consumo.utils import (
    construir_coeficientes_consumo,
)


load_dotenv()

DATASET_ID = "br_coeficientes_consumo"
TABLE_ID = "preparacao_camada_consumo"
SOURCE_SCHEMA = "brasil_despesas_familiares"
SOURCE_TABLE = "pof_2018_despesas_familiares_situacao_domicilio"
DEFAULT_EQUIVALENCE_PATH = Path(__file__).with_name("equivalencia_despesas.json")

PARAMETROS_CONSUMO = {
    "coluna_chave_mip": "TipoDespesaDestinoProvável",
    "coluna_tipo_despesa_mip": "TiposDeDespesa",
    "variavel_alvo": "Distribuição da despesa monetária e não monetária média mensal familiar",
    "ano_alvo": 2018,
    "rotulo_urbano": "Urbana",
    "rotulo_rural": "Rural",
    "padrao_estado": "Estad|Estadual",
}

equivalence_path_env = os.getenv(
    "CONSUMPTION_EQUIVALENCE_FILE_PATH"
) or os.getenv("CAMINHO_ARQUIVO_EQUIVALENCIA_CONSUMO")
equivalence_path = (
    Path(equivalence_path_env) if equivalence_path_env else DEFAULT_EQUIVALENCE_PATH
)

if not equivalence_path.exists():
    raise FileNotFoundError(
        f"Arquivo de equivalência de despesas não encontrado: {equivalence_path}"
    )

mip_mapping = pd.read_json(equivalence_path)
required_mapping_columns = [
    PARAMETROS_CONSUMO["coluna_chave_mip"],
    PARAMETROS_CONSUMO["coluna_tipo_despesa_mip"],
]
missing_mapping_columns = [
    column for column in required_mapping_columns if column not in mip_mapping.columns
]

if missing_mapping_columns:
    raise ValueError(
        "Colunas obrigatórias ausentes no arquivo de equivalência: "
        f"{', '.join(missing_mapping_columns)}"
    )

mip_mapping = mip_mapping[required_mapping_columns].dropna(subset=required_mapping_columns)
mip_mapping = mip_mapping.drop_duplicates(subset=required_mapping_columns)
mip_mapping[PARAMETROS_CONSUMO["coluna_tipo_despesa_mip"]] = mip_mapping[
    PARAMETROS_CONSUMO["coluna_tipo_despesa_mip"]
].astype("string").str.strip()

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

pof_data["tipo_despesa"] = pof_data["tipo_despesa"].astype("string").str.strip()

coefficients_data = construir_coeficientes_consumo(
    pof_data,
    mip_mapping,
    PARAMETROS_CONSUMO,
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
