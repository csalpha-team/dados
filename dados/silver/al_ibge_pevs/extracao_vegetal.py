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
from dados.silver.padronizacao_produtos import (
    dicionario_produtos_pevs
)

load_dotenv()
TABLE = 'produtos_extracao_vegetal'

query = f"""
select
nome_variavel,
produto,
id_municipio,
cast(ano as integer) as ano,
valor
from al_ibge_pevs.{TABLE};
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
    "Quantidade produzida na extração vegetal": "quantidade_produzida",
    "Valor da produção na extração vegetal": "valor_producao",
}
#renomear colunas
data.rename(columns=cols, inplace=True)    

#Padroniza nome de produtos
data["produto"] = data["produto"].map(dicionario_produtos_pevs)

# conserta dígitos do IBGE
data = fix_ibge_digits(data,list(cols.values()), columns_index[0:3])

# Aplica correção monetária
data["valor_producao"] = data["valor_producao"].astype("float")
data["valor_producao"] = data["valor_producao"].apply(lambda x: currency_fix(x) if isinstance(x, str) else x)


    
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
            'quantidade_produzida': 'numeric',
            'valor_producao': 'numeric',
        }
            
            
        db.create_table(f'{TABLE}', columns, drop_if_exists=True)
        
        db.load_data(f'{TABLE}', data, if_exists='replace')