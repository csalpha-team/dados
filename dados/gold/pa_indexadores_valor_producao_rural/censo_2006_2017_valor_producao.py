from dotenv import load_dotenv
import os
import basedosdados as bd
from dados.raw.utils.postgres_interactions import PostgresETL
from dados.gold.pa_indexadores_producao_rural.utils import (
    dicionario_regioes_integracao,
)


load_dotenv()
billing_id = os.getenv("BASEDOSDADADOS_PROJECT_ID")

SILVER_TABLE1="tbl_1931_2006"
SILVER_TABLE2="tbl_6898_2017"
DATASET_ID="pa_indexadores_valor_producao_rural"
TABLE_ID="censo_2006_2017_valor_producao"

query = f"""
select
ano,
id_municipio,
tipo_agricultura,
tipo_producao,
quantidade_estabelecimentos_produtivos,
valor_producao
from al_ibge_censoagro.{SILVER_TABLE1}
where id_municipio like '15%'
Union all
select
ano,
id_municipio,
tipo_agricultura,
tipo_producao,
quantidade_estabelecimentos_produtivos,
valor_producao
from al_ibge_censoagro.{SILVER_TABLE2}
where id_municipio like '15%'
"""

with PostgresETL(
        host='localhost', 
        database=os.getenv("DB_SILVER_ZONE"), 
        user=os.getenv("POSTGRES_USER"), 
        password=os.getenv("POSTGRES_PASSWORD"),
        schema='al_ibge_censoagro') as db:
    
    data = db.download_data(query)
    
print('------ Baixando tabela de municipios ------')
municipios = bd.read_sql(
    """
    SELECT id_municipio, nome, sigla_uf, 
    FROM `basedosdados.br_bd_diretorios_brasil.municipio`
    WHERE amazonia_legal = 1
    """,
    billing_project_id=billing_id,
)

#left join com a tabela de municipios
data = data.merge(municipios, on='id_municipio', how='left')

data['nome_regiao_integracao'] = data['id_municipio'].map(dicionario_regioes_integracao)

data['valor_producao'] = data['valor_producao'].round(2)


with PostgresETL(
        host='localhost', 
        database=os.getenv("DB_GOLD_ZONE"), 
        user=os.getenv("POSTGRES_USER"), 
        password=os.getenv("POSTGRES_PASSWORD"),
        schema=DATASET_ID) as db:
    
    columns = {
        'ano': 'integer',
        'id_municipio': 'VARCHAR(7)',
        'nome': 'VARCHAR(255)',
        'nome_regiao_integracao': 'VARCHAR(255)',
        'sigla_uf': 'VARCHAR(2)',
        'tipo_agricultura': 'VARCHAR(255)',
        'tipo_producao': 'VARCHAR(255)',
        'quantidade_estabelecimentos_produtivos': 'integer',
        'valor_producao': 'numeric',
    }

    db.create_table(TABLE_ID, columns, drop_if_exists=True)
    
    db.load_data(TABLE_ID, data, if_exists='replace')

    

        
      
