from dados.raw.utils.postgres_interactions import (
    PostgresETL,
)
from dados.silver.utils import (
    currency_fix,
    fix_ibge_digits,
    check_duplicates,
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
from al_ibge_pevs.produtos_extracao_vegetal;
"""

with PostgresETL(
    host='localhost', 
    database=os.getenv("DB_RAW_ZONE"), 
    user=os.getenv("POSTGRES_USER"), 
    password=os.getenv("POSTGRES_PASSWORD"),
    schema='al_ibge_pevs') as db:
    
    data = db.download_data(query)
    

# Checar existência de duplicatas por segurança
columns_index = ["id_municipio", "ano", "produto", "nome_variavel"]

check_duplicates(data, columns_index)

#pivotar tabela
data = data.pivot_table(
    index=columns_index[0:3],
    columns=["nome_variavel"],
    values="valor",
    aggfunc="sum"
).reset_index()


cols = {
    "Quantidade produzida na extração vegetal": "quantidade",
    "Valor da produção na extração vegetal": "valor",
}
#renomear colunas
data.rename(columns=cols, inplace=True)    

#Padroniza nome de produtos
data["produto"] = data["produto"].map(dicionario_produtos_pevs)

# conserta dígitos do IBGE
data = fix_ibge_digits(data,list(cols.values()), columns_index[0:3])

# Aplica correção monetária
data["valor"] = data["valor"].astype("float")
data["valor"] = data["valor"].apply(lambda x: currency_fix(x) if isinstance(x, str) else x)


    
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