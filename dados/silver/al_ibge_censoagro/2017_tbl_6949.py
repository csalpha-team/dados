
import dotenv
import os
from dados.raw.utils.postgres_interactions import PostgresETL
from dados.silver.utils import (
    fix_ibge_digits,
    check_duplicates,
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
from al_ibge_censoagro.tbl_6949_2017
where tipo_agricultura IN ('Agricultura familiar - sim', 'Agricultura familiar - não');
"""


with PostgresETL(
        host='localhost', 
        database=os.getenv("DB_RAW_ZONE"), 
        user=os.getenv("POSTGRES_USER"), 
        password=os.getenv("POSTGRES_PASSWORD"),
        schema='al_ibge_censoagro') as db:
    
    data = db.download_data(query)
    


# Checar existência de duplicatas por segurança
columns_index = ["id_municipio", "ano", "produto", "tipo_agricultura", "nome_variavel"]

check_duplicates(data, columns_index)

#pivotar tabela
data = data.pivot_table(
    index=columns_index[0:4],
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

#Padroniza nome de produtos
data["produto"] = data["produto"].map(dicionario_protudos_censo_6949_2233)

dicionario_tipo_agricultura = {
    "Agricultura familiar - sim": "agricultura familiar",
    "Agricultura familiar - não": "agricultura não familiar",
}

#rename categorias and sum to new categorias
data['tipo_agricultura'] = data['tipo_agricultura'].map(dicionario_tipo_agricultura)

data = data[data['tipo_agricultura'].isin(["agricultura familiar", "agricultura não familiar"])]

data = fix_ibge_digits(data,list(cols.values()), ['id_municipio', 'ano', 'produto', 'tipo_agricultura'])


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


