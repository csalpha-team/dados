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
SCHEMA = 'br_ibge_pevs'


query = """
select
  ano,
  id_municipio,
  tipo_produto,
  produto,
  unidade,
  quantidade,
  valor
from basedosdados.br_ibge_pevs.producao_extracao_vegetal
where id_municipio IN (
  select id_municipio
  from basedosdados.br_bd_diretorios_brasil.municipio
  where amazonia_legal = 1
)
;

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
  schema=SCHEMA) as db:
    
    columns = {
      'ano' : 'INTEGER',
      'id_municipio' : 'VARCHAR(7)',
      'tipo_produto' : 'VARCHAR(100)',
      'produto' : 'VARCHAR(100)',
      'unidade' : 'VARCHAR(100)',
      'quantidade' : 'FLOAT',
      'valor' : 'FLOAT',
    }
          
    db.create_table('producao_extracao_vegetal', columns, if_not_exists=True)
    
    db.load_data('producao_extracao_vegetal', df, if_exists='replace')


print('Data loaded')
