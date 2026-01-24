from dotenv import load_dotenv
import os
import basedosdados as bd
from dados.raw.utils.postgres_interactions import PostgresETL
from dados.gold.pa_indexadores_producao_rural.utils import (
    dicionario_regioes_integracao,
)


load_dotenv()
billing_id = os.getenv("BASEDOSDADADOS_PROJECT_ID")

TABLE="tbl_6957_2017"

query = f"""
select
ano,
id_municipio,
tipo_agricultura,
produto,
quantidade_estabelecimentos,
quantidade_produzida,
quantidade_vendida as comercio_quantidade_produzida,
quantidade_produzida - quantidade_vendida as autoconsumo_quantidade_vendida,
valor_producao,
valor_venda as comercio_valor_producao,
valor_producao - valor_venda as autoconsumo_valor_producao
from al_ibge_censoagro.{TABLE}
where id_municipio like '15%';

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
            'produto': 'VARCHAR(255)',
            'tipo_agricultura': 'VARCHAR(255)',
            'quantidade_estabelecimentos': 'numeric',
            'quantidade_produzida': 'numeric',
            'comercio_quantidade_produzida' : 'numeric',
            'autoconsumo_quantidade_vendida' : 'numeric',
            'quantidade_vendida': 'numeric',
            'comercio_valor_producao' : 'numeric',
            'autoconsumo_valor_producao' : 'numeric',
            'quantidade_vendida': 'numeric',
            'valor_producao': 'numeric',
        }

    db.create_table('lavoura_temporaria_censo_2017', columns, drop_if_exists=True)
    
    db.load_data('lavoura_temporaria_censo_2017', data, if_exists='replace')

    

        
      
