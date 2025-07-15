from dotenv import load_dotenv
import os
import basedosdados as bd
from dados.raw.utils.postgres_interactions import PostgresETL
from dados.gold.pa_indexadores_producao_rural.utils import (
    dicionario_regioes_integracao,
)

load_dotenv()
billing_id = os.getenv("BASEDOSDADADOS_PROJECT_ID")



query = """
select
ano,
id_municipio,
produto,
quantidade_produzida,
valor_producao
from al_ibge_pevs.produtos_extracao_vegetal
where id_municipio like '15%' AND
produto !~ '^[0-9]' AND  produto !~ 'total';
"""

with PostgresETL(
    host='localhost', 
    database=os.getenv("DB_SILVER_ZONE"), 
    user=os.getenv("POSTGRES_USER"), 
    password=os.getenv("POSTGRES_PASSWORD"),
    schema='al_ibge_pevs') as db:
    
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

    
with PostgresETL(
    host='localhost', 
    database=os.getenv("DB_GOLD_ZONE"), 
    user=os.getenv("POSTGRES_USER"), 
    password=os.getenv("POSTGRES_PASSWORD"),
    schema='pa_indexadores_producao_rural') as db:
        
        columns = {
            'ano': 'integer',
            'id_municipio': 'VARCHAR(7)',
            'nome': 'VARCHAR(255)',
            'nome_regiao_integracao': 'VARCHAR(255)',
            'sigla_uf': 'VARCHAR(2)',
            'id_municipio': 'VARCHAR(7)',
            'produto': 'VARCHAR(255)',
            'quantidade_produzida': 'numeric',
            'valor_producao': 'numeric',
        }
            
        db.create_table('extracao_vegetal_pevs', columns, drop_if_exists=True)
        
        db.load_data('extracao_vegetal_pevs', data, if_exists='replace')