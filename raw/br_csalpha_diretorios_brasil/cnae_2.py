import basedosdados as bd
import os
from dotenv import load_dotenv
from raw.utils.postgres_interactions import PostgresETL

print('Loading environment variables...')
# Loading .env file
load_dotenv()

# setting up env vars
ID = os.getenv("BASEDOSDADADOS_PROJECT_ID")
ROOT_DIR = os.getenv("ROOT_DIR")
os.chdir(ROOT_DIR)

# Querying data
#NOTE: faz sentido selecionar a tbl completa e filtrar essa para a zona agregada;
#o diretorios precisam ser completos. 
#! modificar aqui
query = """
select
 *
FROM basedosdados.br_bd_diretorios_brasil.cnae_2
where subclasse IN  (
    '1063500', 
    '0112102',
    '0112199',
    '0116499',
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
    '1032599',
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
    '1020101',
    '0142300',
    '0141502',
    '0159801',
    '0210199',
    '0210105',
    '0210106',
    '0210199',
    '0220901',
    '0230600',
    '1069400',
    '1095300',
    '1122499',
    '1312000',
    '1322700',
    '2029100',
    '2040100',
    '3101200',
    '4634603',
    '4635499',
    '4637104',
    '4637106',
    '4671100',
    '4722902'
    
    ) or 
    classe IN (
    '01199',
    '01334',
    '01393',
    '02209',
    '03116',
    '03124',
    '03213',
    '03221',
    '10201',
    '11119',
    '16102',
    '46320'
    
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
  database=os.getenv("DB_TRUSTED_ZONE"), 
  user=os.getenv("POSTGRES_USER"), 
  password=os.getenv("POSTGRES_PASSWORD"),
  schema='br_csalpha_diretorios_brasil') as db:
    
    columns = {
        "subclasse": "VARCHAR(256)",
        "descricao_subclasse": "TEXT",
        "classe": "VARCHAR(256)",
        "descricao_classe": "TEXT",
        "grupo": "VARCHAR(256)",
        "descricao_grupo": "TEXT",
        "divisao": "VARCHAR(256)",
        "descricao_divisao": "TEXT",
        "secao": "VARCHAR(256)",
        "descricao_secao": "TEXT",
        "indicador_cnae_2_0": "INTEGER",
        "indicador_cnae_2_1": "INTEGER",
        "indicador_cnae_2_2": "INTEGER",
        "indicador_cnae_2_3": "INTEGER",    
    } 
          
    db.create_table('cnae_2_bioeconomia', columns, if_not_exists=True)
    
    db.load_data('cnae_2_bioeconomia', df, if_exists='append')


print('Data loaded')


