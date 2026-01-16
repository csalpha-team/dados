from dotenv import load_dotenv
import os
import basedosdados as bd
from dados.raw.utils.postgres_interactions import PostgresETL


load_dotenv()

TABLE_ID= 'tbl_2715'
DATASET_ID='br_ibge_pas'


query = f"""
SELECT  
  *
FROM {DATASET_ID}.{TABLE_ID}
"""

with PostgresETL(
        host='localhost', 
        database=os.getenv("DB_SILVER_ZONE"), 
        user=os.getenv("POSTGRES_USER"), 
        password=os.getenv("POSTGRES_PASSWORD"),
        schema=DATASET_ID) as db:
    
    data = db.download_data(query)
    

with PostgresETL(
        host='localhost', 
        database=os.getenv("DB_GOLD_ZONE"), 
        user=os.getenv("POSTGRES_USER"), 
        password=os.getenv("POSTGRES_PASSWORD"),
        schema='br_servicos') as db:
    
    columns = {
        'ano': 'VARCHAR(255)',
        'unidade_geografica': 'VARCHAR(255)',
        'divisao_grupo_cnae_2': 'VARCHAR(255)',
        'quantidade_empresas' : 'numeric',
        'valor_gastos_salarios_remuneracoes': 'VARCHAR(255)',
        'pessoal_ocupado_31_12': 'VARCHAR(255)',
        'valor_receita_bruta_servicos': 'VARCHAR(255)'
        }


    db.create_table('pas_servicos', columns, drop_if_exists=True)
    
    db.load_data('pas_servicos', data, if_exists='replace')

    

        
      
