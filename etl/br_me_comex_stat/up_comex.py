import basedosdados as bd
import os
from dotenv import load_dotenv
from etl.utils.postgres_interactions import PostgresETL

print('Loading environment variables...')
# Loading .env file
load_dotenv()

# setting up env vars
ID = os.getenv("BASEDOSDADADOS_PROJECT_ID")
ROOT_DIR = os.getenv("ROOT_DIR")
os.chdir(ROOT_DIR)

# Querying data
query = """
select *
from basedosdados.br_me_comex_stat.ncm_exportacao
where 
  sigla_uf_ncm = 'PA' and 
  id_ncm IN 
    ('11062000',
     '19030000'
    ) and ano = 2023;
"""

print('Downloading data...')

df = bd.read_sql(
    query = query,
    billing_project_id=ID
)

print('loading data to postgres')

with PostgresETL(
  host='localhost', 
  database=os.getenv("DB_RAW_ZONE"), 
  user=os.getenv("POSTGRES_USER"), 
  password=os.getenv("POSTGRES_PASSWORD"),
  schema=os.getenv("DB_RAW_ZONE")) as db:
    
    columns = {
        'ano': 'INTEGER',
        'mes': 'INTEGER',
        'id_ncm': 'VARCHAR(255)',
        'id_unidade': 'VARCHAR(255)',
        'id_pais': 'VARCHAR(255)',
        'sigla_pais_iso3': 'VARCHAR(3)',
        'sigla_uf_ncm': 'VARCHAR(2)',
        'id_via': 'VARCHAR(255)',
        'id_urf': 'VARCHAR(255)',
        'quantidade_estatistica': 'NUMERIC',
        'peso_liquido_kg': 'NUMERIC',
        'valor_fob_dolar': 'NUMERIC'
    }
          
    db.create_table('comex_stat', columns, if_not_exists=True)
    
    db.load_data('comex_stat', df, if_exists='append')


print('Data loaded')


