from dotenv import load_dotenv
import os
from pathlib import Path
from dados.raw.utils.postgres_interactions import PostgresETL
from dados.gold.pa_coeficientes_custo.utils import (
    agregar_coeficientes_regional_mais_recente,
    calcular_coeficientes_municipais,
    clean_region_name,
    expandir_coeficientes,
    carregar_parametros_custo,
)


load_dotenv()

DATASET_ID = "pa_coeficientes_custo"
TABLE_ID = "preparacao_camada_custo"
SOURCE_SCHEMA = "pa_indexadores_custo_producao_rural"
SOURCE_TABLE = "censo_2006_2017_despesas"
CONFIG_PATH = Path(__file__).with_name("parametros_coeficientes_custo.json")


query = f"""
select
    ano,
    id_municipio,
    nome,
    nome_regiao_integracao,
    sigla_uf,
    tipo_despesa,
    quantidade_estabelecimentos_fizeram_despesa,
    valor_despesa
from {SOURCE_SCHEMA}.{SOURCE_TABLE}
"""

with PostgresETL(
    host="localhost",
    database=os.getenv("DB_GOLD_ZONE"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
    schema=SOURCE_SCHEMA,
) as db:
    coeficientes_dados = db.download_data(query)

if "nome_regiao_integracao" in coeficientes_dados.columns:
    coeficientes_dados["nome_regiao_integracao"] = coeficientes_dados[
        "nome_regiao_integracao"
    ].apply(clean_region_name)

valor_para_chave, rename_map, total_expense_label = carregar_parametros_custo(CONFIG_PATH)

coeff_agrupados = calcular_coeficientes_municipais(
    coeficientes_dados,
    total_expense_label=total_expense_label,
)
coeff_agrupados["nome_regiao_integracao"] = coeff_agrupados[
    "nome_regiao_integracao"
].replace(rename_map)

coeff_df = expandir_coeficientes(coeff_agrupados, valor_para_chave)
coeff_df_final = agregar_coeficientes_regional_mais_recente(coeff_df)

result_data = coeff_df_final[["ano", "nome_regiao_integracao", "tipo_coeff", "coeff"]]

with PostgresETL(
    host="localhost",
    database=os.getenv("DB_GOLD_ZONE"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
    schema=DATASET_ID,
) as db:
    columns = {
        "ano": "integer",
        "nome_regiao_integracao": "VARCHAR(255)",
        "tipo_coeff": "VARCHAR(255)",
        "coeff": "numeric",
    }

    db.create_table(TABLE_ID, columns, drop_if_exists=True)
    db.load_data(TABLE_ID, result_data, if_exists="replace")
