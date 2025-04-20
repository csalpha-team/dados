import basedosdados as bd
import os
from dotenv import load_dotenv
from raw.utils.postgres_interactions import PostgresETL
import pandas as pd
from raw.br_csalpha_diretorios_brasil.utils import process_ncm_codes


print('Loading environment variables...')
# Loading .env file
load_dotenv()

# setting up env vars
ID = os.getenv("BASEDOSDADADOS_PROJECT_ID")
ROOT_DIR = os.getenv("ROOT_DIR")
os.chdir(ROOT_DIR)


path = 'etl/br_csalpha_diretorios_brasil/prodlist_industria.xlsx'
df = pd.read_excel(path, header=None)

df.dropna(how='all', inplace=True)
df['id_cnae'] = None
df['descricao_cnae'] = None

pattern = r"CNAE\s*([\d\.\-]+):\s*(.+)"
mask = df[1].str.contains(pattern, na=False, regex=True)
extracted = df.loc[mask, 1].str.extract(pattern, expand=True)

# Se a extração estiver correta, atribua as colunas extraídas
df.loc[mask, ['id_cnae', 'descricao_cnae']] = extracted.values
df[['id_cnae', 'descricao_cnae']] = df[['id_cnae', 'descricao_cnae']].ffill()

# Remove as linhas que contêm "CNAE" na coluna 1
df = df[~df[1].astype(str).str.contains("CNAE", na=False)]
df = df[~df[1].astype(str).str.contains("PRODLIST", na=False)]

df.rename(columns={
    1: 'id_prodlist',
    2: 'descricao_prodlist',
    3: 'unidade_medida_prodlist',
    4: 'id_ncm',
    5: 'atualizacao'
}, inplace=True)


df['id_ncm'] = df['id_ncm'].str.split('+')


# Process the NCM codes
df['id_ncm'] = df['id_ncm'].apply(process_ncm_codes)
df = df.explode('id_ncm')
df['id_cnae'] = df['id_cnae'].str.replace('.','').str.replace('-', '')
df['id_ncm'] = df['id_ncm'].str.replace('.','')
df['id_prodlist'] = df['id_prodlist'].str.replace('.','')

colums = ['id_prodlist', 'id_ncm',  'descricao_prodlist', 'id_cnae', 'descricao_cnae', 'unidade_medida_prodlist',  ]

df = df[colums]


with PostgresETL(
  host='localhost', 
  database=os.getenv("DB_TRUSTED_ZONE"), 
  user=os.getenv("POSTGRES_USER"), 
  password=os.getenv("POSTGRES_PASSWORD"),
  schema='br_csalpha_diretorios_brasil') as db:
    
     
    columns = {
        'id_prodlist': 'VARCHAR(255)',
        'descricao_prodlist': 'TEXT',
        'unidade_medida_prodlist': 'TEXT',
        'id_ncm': 'VARCHAR(255)',
        'id_cnae': 'VARCHAR(255)',
        'descricao_cnae': 'TEXT',
        'atualizacao': 'TEXT',
    }
          
          
    db.create_table('prodlist_industria', columns, if_not_exists=True)
    
    db.load_data('prodlist_industria', df, if_exists='replace')


print('Data loaded')


