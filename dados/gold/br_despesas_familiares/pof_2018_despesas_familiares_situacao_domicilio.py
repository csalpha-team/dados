from dotenv import load_dotenv
import os
import basedosdados as bd
from dados.raw.utils.postgres_interactions import PostgresETL


load_dotenv()

SILVER_TABLE="tbl_6970"
DATASET_ID='br_despesas_familiares'
TABLE_ID='pof_2018_despesas_familiares_situacao_domicilio'

query = f"""
SELECT  
  *
FROM br_ibge_pof.tbl_6970
"""

with PostgresETL(
        host='localhost', 
        database=os.getenv("DB_SILVER_ZONE"), 
        user=os.getenv("POSTGRES_USER"), 
        password=os.getenv("POSTGRES_PASSWORD"),
        schema='br_ibge_pof') as db:
    
    data = db.download_data(query)
    


with PostgresETL(
        host='localhost', 
        database=os.getenv("DB_GOLD_ZONE"), 
        user=os.getenv("POSTGRES_USER"), 
        password=os.getenv("POSTGRES_PASSWORD"),
        schema=DATASET_ID) as db:
    
    columns = {
            'ano': 'integer',
            'localidade': 'VARCHAR(255)',
            'variavel' : 'VARCHAR(255)',
            'situacao_domicilio': 'VARCHAR(255)',
            'tipo_despesa': 'VARCHAR(255)',
            'valor': 'float',
            'unidade_medida': 'VARCHAR(255)',
        }

    db.create_table(TABLE_ID, columns, drop_if_exists=True)
    
    db.load_data(TABLE_ID, data, if_exists='replace')

    

        
      
