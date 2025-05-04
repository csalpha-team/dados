from dotenv import load_dotenv
import os
from dados.raw.utils.postgres_interactions import PostgresETL
from dados.silver.utils import (
    fix_ibge_digits,
    check_duplicates,
)

from dados.silver.padronizacao_produtos import (
    dicionario_produtos_censo_6955_2518
)


load_dotenv()
TABLE="tbl_6955_2017"


query = f"""
select
nome_variavel,
produto,
tipo_agricultura,
id_municipio,
cast(ano as integer) as ano,
valor
from al_ibge_censoagro.{TABLE}
where tipo_agricultura IN ('Agricultura familiar - sim', 'Agricultura familiar - não') AND
nome_variavel IN (
'Área colhida nas lavouras permanentes nos estabelecimentos agropecuários com 50 pés e mais existentes',
'Área total existente na data de referência nas lavouras permanentes nos estabelecimentos agropecuários com 50 pés e mais existentes',
'Número de estabelecimentos agropecuários com 50 pés e mais existentes da lavoura permanente',
'Quantidade produzida nas lavouras permanentes nos estabelecimentos agropecuários com 50 pés e mais existentes',
'Quantidade vendida das lavouras permanentes nos estabelecimentos agropecuários com 50 pés e mais existentes',
'Valor da produção das lavouras permanentes nos estabelecimentos agropecuários com 50 pés e mais existentes',
'Valor da venda das lavouras permanentes nos estabelecimentos agropecuários com 50 pés e mais existentes'    
);
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

"Área colhida nas lavouras permanentes nos estabelecimentos agropecuários com 50 pés e mais existentes" : "area_colhida",
"Área total existente na data de referência nas lavouras permanentes nos estabelecimentos agropecuários com 50 pés e mais existentes": "area_plantada",
"Número de estabelecimentos agropecuários com 50 pés e mais existentes da lavoura permanente" : "quantidade_estabelecimentos",
"Quantidade produzida nas lavouras permanentes nos estabelecimentos agropecuários com 50 pés e mais existentes" : "quantidade_produzida",
"Quantidade vendida das lavouras permanentes nos estabelecimentos agropecuários com 50 pés e mais existentes" : "quantidade_vendida",
"Valor da produção das lavouras permanentes nos estabelecimentos agropecuários com 50 pés e mais existentes" : "valor_producao",
"Valor da venda das lavouras permanentes nos estabelecimentos agropecuários com 50 pés e mais existentes"  : "valor_venda"  
   
}

data.rename(columns=cols, inplace=True)    

#Padroniza nome de produtos
data["produto"] = data["produto"].map(dicionario_produtos_censo_6955_2518)

dicionario_tipo_agricultura = {
    "Agricultura familiar - sim": "agricultura familiar",
    "Agricultura familiar - não": "agricultura não familiar",
}

#rename categorias and sum to new categorias
data['tipo_agricultura'] = data['tipo_agricultura'].map(dicionario_tipo_agricultura)

data = data[data['tipo_agricultura'].isin(["agricultura familiar", "agricultura não familiar"])]

data = fix_ibge_digits(data,list(cols.values()), ['id_municipio', 'ano', 'produto', 'tipo_agricultura'], div_column="quantidade_estabelecimentos")


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
            'valor_venda' : 'numeric', 
            'area_colhida': 'numeric',
            'area_plantada': 'numeric',

        }
            
        db.create_table(f'{TABLE}', columns, drop_if_exists=True)
        
        db.load_data(f'{TABLE}', data, if_exists='replace')


