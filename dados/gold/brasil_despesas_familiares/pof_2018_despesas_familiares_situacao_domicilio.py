from dotenv import load_dotenv
import os
import basedosdados as bd
from dados.raw.utils.postgres_interactions import PostgresETL


load_dotenv()

TABLE="tbl_6970"


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
        schema='brasil_despesas_familiares') as db:
    
    columns = {
            'ano': 'integer',
            'localidade': 'VARCHAR(255)',
            'variavel' : 'VARCHAR(255)',
            'situacao_domicilio': 'VARCHAR(255)',
            'tipo_despesa': 'VARCHAR(255)',
            'valor': 'float',
            'unidade_medida': 'VARCHAR(255)',
        }

    db.create_table('pof_2018_despesas_familiares_situacao_domicilio', columns, drop_if_exists=True)
    
    db.load_data('pof_2018_despesas_familiares_situacao_domicilio', data, if_exists='replace')

    

        
      
