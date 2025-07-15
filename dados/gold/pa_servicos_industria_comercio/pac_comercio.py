from dotenv import load_dotenv
import os
import basedosdados as bd
from dados.raw.utils.postgres_interactions import PostgresETL


load_dotenv()

TABLE_ID= 'tbl_1407'
DATASET_ID='br_ibge_pac'


query = f"""
SELECT  
  *
FROM {DATASET_ID}.{TABLE_ID}
where unidade_geografica = 'Pará'
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
        schema='pa_servicos_industria_comercio') as db:
    
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


    db.create_table('pac_comercio', columns, drop_if_exists=True)
    
    db.load_data('pac_comercio', data, if_exists='replace')

    

        
      
