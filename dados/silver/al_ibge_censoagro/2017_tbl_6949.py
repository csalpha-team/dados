
import dotenv
import os
from dados.raw.utils.postgres_interactions import PostgresETL
from dados.silver.utils import (
    fix_ibge_digits,
)

from dados.silver.al_ibge_pevs.utils import (
    dicionario_protudos_censo_6949_2233
)

dotenv.load_dotenv()


query = """
select
nome_variavel,
produto,
tipo_agricultura,
id_municipio,
cast(ano as integer) as ano,
valor
from al_ibge_censoagro.tbl_6949_2017;

"""


with PostgresETL(
        host='localhost', 
        database=os.getenv("DB_RAW_ZONE"), 
        user=os.getenv("POSTGRES_USER"), 
        password=os.getenv("POSTGRES_PASSWORD"),
        schema='al_ibge_censoagro') as db:
    
    data = db.download_data(query)
    


data = data.pivot_table(
    index=["id_municipio", "ano", "produto", "tipo_agricultura"],
    columns=["nome_variavel"],
    values="valor",
    aggfunc="sum"
).reset_index()


cols = {
    
    "Número de estabelecimentos agropecuários com produtos da extração vegetal": "quantidade_estabelecimentos",
    "Quantidade produzida na extração vegetal": "quantidade_produzida",
    "Quantidade vendida de produtos da extração vegetal": "quantidade_vendida",
    "Valor da produção na extração vegetal": "valor_producao",
    "Valor da venda de produtos da extração vegetal": "valor_venda",
}

data.rename(columns=cols, inplace=True)    

COLUNAS_PARA_TRATAR = list(cols.values())

data = fix_ibge_digits(COLUNAS_PARA_TRATAR, data)

#Padroniza nome de produtos
data["produto"] = data["produto"].map(dicionario_protudos_censo_6949_2233)

#NOTE: PEGAR SOMENTE CATEGORIAS - SIM E NAO QUE REPRESENTAM A TOTALIDADE DOS SUBGRUPOS
    
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
            
        db.create_table('tbl_6949_2017', columns, if_not_exists=True)
        
        db.load_data('tbl_6949_2017', data, if_exists='replace')


