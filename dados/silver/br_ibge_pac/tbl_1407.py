from dados.raw.utils.postgres_interactions import (
    PostgresETL,
)
from dotenv import load_dotenv
import os
import pandas as pd

load_dotenv()
TABLE_ID= 'tbl_1407'
DATASET_ID='br_ibge_pac'


query = f"""
select
nome_variavel,
nome_categoria_comercio,
nome_categoria_regiao,
cast(ano as integer) as ano,
valor
from {DATASET_ID}.{TABLE_ID}

"""

with PostgresETL(
    host='localhost', 
    database=os.getenv("DB_RAW_ZONE"), 
    user=os.getenv("POSTGRES_USER"), 
    password=os.getenv("POSTGRES_PASSWORD"),
    schema=DATASET_ID) as db:
    
    data = db.download_data(query)
    
data['valor'] = data.valor.apply(lambda x: 0 if x in ("..", "...", "-", 'X') else x)


#pivotar tabela
data = data.pivot_table(
    index=['ano','nome_categoria_regiao', 'nome_categoria_comercio' ],
    columns=["nome_variavel"],
    values="valor",
    aggfunc="sum"
).reset_index()


cols = {
    "nome_categoria_comercio": "divisao_grupo_cnae_2",
    "nome_categoria_regiao": "unidade_geografica",
    "Gastos com salários, retiradas e outras remunerações em empresas comerciais":
        "valor_gastos_salarios_remuneracoes",
    "Margem de comercialização em empresas comerciais":
        "margem_comercializacao",
    "Número de unidades locais com receita de revenda":
        "quantidade_unidades_empresas_receita_revenda",
    "Pessoal ocupado em 31/12 em empresas comerciais":
        "pessoal_ocupado_31_12",
    "Receita bruta de revenda de mercadorias":
        "valor_receita_bruta_revenda"
}

#renomear colunas
data.rename(columns=cols, inplace=True)    
    
with PostgresETL(
    host='localhost', 
    database=os.getenv("DB_SILVER_ZONE"), 
    user=os.getenv("POSTGRES_USER"), 
    password=os.getenv("POSTGRES_PASSWORD"),
    schema=DATASET_ID) as db:
        
        columns = {
        'ano': 'VARCHAR(255)',
        'unidade_geografica': 'VARCHAR(255)',
        'divisao_grupo_cnae_2': 'VARCHAR(255)',
        'valor_gastos_salarios_remuneracoes': 'VARCHAR(255)',
        'margem_comercializacao': 'VARCHAR(255)',
        'quantidade_unidades_empresas_receita_revenda': 'VARCHAR(255)',
        'pessoal_ocupado_31_12': 'VARCHAR(255)',
        'valor_receita_bruta_revenda': 'VARCHAR(255)'
        }

        db.create_table(f'{TABLE_ID}', columns, drop_if_exists=True)
        
        db.load_data(f'{TABLE_ID}', data, if_exists='replace')