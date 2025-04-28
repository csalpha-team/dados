from dados.raw.utils.postgres_interactions import (
    PostgresETL,
)
from dados.silver.utils import (
    currency_fix,
    fix_ibge_digits,
)
from dotenv import load_dotenv
import os
import pandas as pd
from dados.silver.al_ibge_pevs.utils import (
    dicionario_produtos_pevs, 
)

load_dotenv()

query = """
select
nome_variavel,
produto,
id_municipio,
cast(ano as integer) as ano,
valor
from al_ibge_pevs.produtos_extracao_vegetal
where id_municipio like '15%';
"""

with PostgresETL(
        host='localhost', 
        database=os.getenv("DB_RAW_ZONE"), 
        user=os.getenv("POSTGRES_USER"), 
        password=os.getenv("POSTGRES_PASSWORD"),
        schema='al_ibge_pevs') as db:
    
    data = db.download_data(query)
    print(data.columns)
    
#pivotar tabela
data = data.pivot_table(
    index=["id_municipio", "ano", "produto"],
    columns=["nome_variavel"],
    values="valor",
    aggfunc="sum"
).reset_index()

#renomear colunas
data.rename(columns={
    "Quantidade produzida na extração vegetal": "quantidade",
    "Valor da produção na extração vegetal": "valor",
}, inplace=True)    

COLUNAS_PARA_TRATAR = [
    "quantidade",
    "valor"
]

data = fix_ibge_digits(COLUNAS_PARA_TRATAR, data)

#Fazer correçoes Monetárias
data["valor"] = data["valor"].astype("float")

# Apply currency_fix only to non-null and non-float values
data["valor"] = data["valor"].apply(lambda x: currency_fix(x) if isinstance(x, str) else x)

#Padroniza nome de produtos
data["produto"] = data["produto"].map(dicionario_produtos_pevs)

print(data.columns)
    
with PostgresETL(
    host='localhost', 
    database=os.getenv("DB_SILVER_ZONE"), 
    user=os.getenv("POSTGRES_USER"), 
    password=os.getenv("POSTGRES_PASSWORD"),
    schema='al_ibge_pevs') as db:
        
        
        columns = {
            'ano': 'integer',
            'id_municipio': 'VARCHAR(7)',
            'produto': 'VARCHAR(255)',
            'quantidade': 'numeric',
            'valor': 'numeric',
        }
            
            
        db.create_table('produtos_extracao_vegetal', columns, if_not_exists=True)
        
        db.load_data('produtos_extracao_vegetal', data, if_exists='replace')