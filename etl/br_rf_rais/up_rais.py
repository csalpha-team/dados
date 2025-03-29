
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

## número de vínculos
## Massa salaria (nvinc * remnueracao media)

query = """
SELECT 
*
FROM `basedosdados.br_me_rais.microdados_estabelecimentos` as rais
  LEFT JOIN
    (SELECT subclasse, descricao_subclasse, descricao_secao, descricao_divisao  FROM basedosdados.br_bd_diretorios_brasil.cnae_2) diretorio_cnae
  ON 
    rais.cnae_2_subclasse = diretorio_cnae.subclasse
  WHERE 
  sigla_uf = 'PA'
  AND
  cnae_2_subclasse IN (
    '1063500', 
    '0119906',
    '1065101',
    '0119906',
    '1041400',
    '0133401',
    '4729699',
    '0220999',
    '0220903',
    '0220904',
    '0220905',
    '1031700',
    '1033301',
    '1042200',
    '4633801',
    '4637199',
    '0135100',
    '4623105',
    '1093701',
    '1033302',
    '4637103',
    '0142300',
    '3839401',
    '4637106',
    '1053800',
    '0311601',
    '0311602',
    '0312401',
    '0312402',
    '1020101'
  ) AND ano = 2023;
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
  schema='br_me_rais') as db:
    
    columns = {} # TODO
          
    db.create_table('', columns, if_not_exists=True)
    
    db.load_data('', df, if_exists='append')


print('Data loaded')



