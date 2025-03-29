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
SCHEMA = 'br_ibge_pam'

print('working with lavoura temporaria table')

query = """
select
  ano,
  id_municipio,
  produto,
  area_plantada,
  area_colhida,
  quantidade_produzida,
  rendimento_medio_producao,
  valor_producao
from basedosdados.br_ibge_pam.lavoura_temporaria
where id_municipio IN (
  select id_municipio
  from basedosdados.br_bd_diretorios_brasil.municipio
  where amazonia_legal = 1
);

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
      'produto' : 'VARCHAR(100)',
      'area_plantada' : 'FLOAT',
      'area_colhida' : 'FLOAT',
      'quantidade_produzida' : 'FLOAT',
      'rendimento_medio_producao' : 'FLOAT',
      'valor_producao' : 'FLOAT',
    }
          
    db.create_table('lavoura_temporaria', columns, if_not_exists=True)
    
    db.load_data('lavoura_temporaria', df, if_exists='replace')


print('Data loaded')
del df

query = """
select
  ano,
  id_municipio,
  produto,
  area_destinada_colheita,
  area_colhida,
  quantidade_produzida,
  rendimento_medio_producao,
  valor_producao
from basedosdados.br_ibge_pam.lavoura_permanente
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
      'produto' : 'VARCHAR(100)',
      'area_destinada_colheita' : 'FLOAT',
      'area_colhida' : 'FLOAT',
      'quantidade_produzida' : 'FLOAT',
      'rendimento_medio_producao' : 'FLOAT',
      'valor_producao' : 'FLOAT',
    }
          
    db.create_table('lavoura_permanente', columns, if_not_exists=True)
    
    db.load_data('lavoura_permanente', df, if_exists='replace')


print('Data loaded')
