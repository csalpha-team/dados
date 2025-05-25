
import dotenv
import os
from dados.raw.utils.postgres_interactions import PostgresETL
from dados.silver.utils import (
    fix_ibge_digits,
    check_duplicates,
)

from dados.silver.padronizacao_produtos import (
    dicionario_produtos_censo_6957_2337
)

dotenv.load_dotenv()

TABLE="tbl_6957_2017"

query = f"""
select
nome_variavel,
produto,
tipo_agricultura,
id_municipio,
cast(ano as integer) as ano,
valor
from al_ibge_censoagro.{TABLE}
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
    
    "Número de estabelecimentos agropecuários com lavoura temporária": "quantidade_estabelecimentos",
    "Quantidade produzida nas lavouras temporárias": "quantidade_produzida",
    "Quantidade vendida das lavouras temporárias": "quantidade_vendida",
    "Valor da produção das lavouras temporárias": "valor_producao",
    "Valor da venda das lavouras temporárias": "valor_venda",
    "Área colhida nas lavouras temporárias": "area_colhida"
}

data.rename(columns=cols, inplace=True)    


#Padroniza nome de produtos
data["produto"] = data["produto"].map(dicionario_produtos_censo_6957_2337)

dicionario_tipo_agricultura = {
    "Agricultura familiar - sim": "agricultura familiar",
    "Agricultura familiar - não": "agricultura não familiar",
}

#rename categorias and sum to new categorias
data['tipo_agricultura'] = data['tipo_agricultura'].map(dicionario_tipo_agricultura)

data = data[data['tipo_agricultura'].isin(["agricultura familiar", "agricultura não familiar"])]

data = fix_ibge_digits(data,list(cols.values()), ['id_municipio', 'ano', 'produto', 'tipo_agricultura'],div_column="quantidade_estabelecimentos")


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
            'area_colhida': 'numeric',

        }
            
        db.create_table(f'{TABLE}', columns, if_not_exists=True)
        
        db.load_data(f'{TABLE}', data, if_exists='replace')

