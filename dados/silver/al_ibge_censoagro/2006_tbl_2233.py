from dotenv import load_dotenv
import os
from dados.raw.utils.postgres_interactions import PostgresETL
from dados.silver.utils import (
    fix_ibge_digits,
)

from dados.silver.al_ibge_pevs.utils import (
    dicionario_protudos_censo_6949_2233
)

load_dotenv()

query = """
select
nome_variavel,
produto,
tipo_agricultura,
id_municipio,
cast(ano as integer) as ano,
valor
from al_ibge_censoagro.tbl_2233_2006;

"""

with PostgresETL(
        host='localhost', 
        database=os.getenv("DB_RAW_ZONE"), 
        user=os.getenv("POSTGRES_USER"), 
        password=os.getenv("POSTGRES_PASSWORD"),
        schema='al_ibge_pevs') as db:
    
    data = db.download_data(query)
    
    
    
#pivotar tabela
data = data.pivot_table(
    index=["id_municipio", "ano", "produto", "tipo_agricultura"],
    columns=["nome_variavel"],
    values="valor",
    aggfunc="sum"
).reset_index()

# renomear colunas
cols = {
    
    "Número de estabelecimentos agropecuários": "quantidade_estabelecimentos",
    "Quantidade colhida": "quantidade_produzida",
    "Quantidade vendida": "quantidade_vendida",
    "Valor da produção": "valor_producao",
    "Valor das vendas": "valor_venda",
}

data.rename(columns=cols, inplace=True)    

COLUNAS_PARA_TRATAR = list(cols.values())

data = fix_ibge_digits(COLUNAS_PARA_TRATAR, data)
data['produto'] = data['produto'].map(dicionario_protudos_censo_6949_2233)

# Padroniza tipo agricultura
dicionario_tipo_agricultura = {
    "Agricultura familiar - Lei 11.326": "agricultura familiar",
    "Agricultura não familiar": "agricultura não familiar",
    "Total": "total",
}

data["tipo_agricultura"] = data.tipo_agricultura.map(dicionario_tipo_agricultura)


data = data[['ano', 'id_municipio',  'produto', 'tipo_agricultura',
       'quantidade_estabelecimentos', 'quantidade_produzida',
       'quantidade_vendida', 'valor_producao',
        'valor_venda',]]
    
with PostgresETL(
    host='localhost', 
    database=os.getenv("DB_SILVER_ZONE"), 
    user=os.getenv("POSTGRES_USER"), 
    password=os.getenv("POSTGRES_PASSWORD"),
    schema='al_ibge_censoagro') as db:
        
        
        columns = {
            'ano': 'integer',
            'id_municipio': 'VARCHAR(7)',
            'produto': 'VARCHAR(255)',
            'tipo_agricultura': 'VARCHAR(255)',
            'quantidade_estabelecimentos': 'integer',
            'quantidade_produzida': 'integer',
            'quantidade_vendida': 'integer',
            'valor_producao': 'numeric',
            'valor_venda': 'numeric',
        }
            
        db.create_table('tbl_2233_2006', columns, drop_if_exists=True)
        
        db.load_data('tbl_2233_2006', data, if_exists='replace')
      
