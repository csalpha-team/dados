from dotenv import load_dotenv
import os
from dados.raw.utils.postgres_interactions import PostgresETL
from dados.silver.utils import fix_ibge_digits

load_dotenv()
TABLE="tbl_6970"

query = f"""
select
localidade,
variavel,
situacao_domicilio,
tipo_despesa,
unidade,
cast(ano as integer) as ano,
valor
from br_ibge_pof.{TABLE}
"""

print(f"Realizando consulta da tabela:\n{TABLE}\n")
with PostgresETL(
        host='localhost', 
        database=os.getenv("DB_RAW_ZONE"), 
        user=os.getenv("POSTGRES_USER"), 
        password=os.getenv("POSTGRES_PASSWORD"),
        schema='br_ibge_pof') as db:
    
    data = db.download_data(query)
    


print("Renomeando colunas")
cols = {
    "unidade" : "unidade_medida",
}

data.rename(columns=cols, inplace=True)    

    
with PostgresETL(
    host='localhost', 
    database=os.getenv("DB_SILVER_ZONE"), 
    user=os.getenv("POSTGRES_USER"),  
    password=os.getenv("POSTGRES_PASSWORD"),
    schema='br_ibge_pof') as db:
        
        
        columns = {
            'ano': 'integer',
            'localidade': 'VARCHAR(255)',
            'variavel' : 'VARCHAR(255)',
            'situacao_domicilio': 'VARCHAR(255)',
            'tipo_despesa': 'VARCHAR(255)',
            'valor': 'float',
            'unidade_medida': 'VARCHAR(255)',
        }
            
        db.create_table(f'{TABLE}', columns, drop_if_exists=True)
        
        db.load_data(f'{TABLE}', data, if_exists='replace')
      