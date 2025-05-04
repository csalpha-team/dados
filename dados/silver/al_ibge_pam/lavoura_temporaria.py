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
    dicionario_produtos_pam_temporaria
)

load_dotenv()
TABLE = 'lavoura_temporaria'

#TODO: Refatorar lógica do código para processar a pipe inteira em chunks 

query = f"""
select
nome_variavel,
produto,
id_municipio,
cast(ano as integer) as ano,
valor
from al_ibge_pam.{TABLE}
where id_municipio like '15%';
"""

with PostgresETL(
    host='localhost', 
    database=os.getenv("DB_RAW_ZONE"), 
    user=os.getenv("POSTGRES_USER"), 
    password=os.getenv("POSTGRES_PASSWORD"),
    schema='al_ibge_pam') as db:
    
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
    "Área colhida": "area_colhida",
    "Área plantada": "area_plantada",
    "Quantidade produzida" : "quantidade_produzida",
    "Rendimento médio da produção": "rendimento_medio_producao",
    "Valor da produção" : "valor_producao"
}


data.rename(columns=cols, inplace=True)    
print(data.columns)

#Padroniza nome de produtos
data["produto"] = data["produto"].map(dicionario_produtos_pam_temporaria)

# conserta dígitos do IBGE
data = fix_ibge_digits(data,list(cols.values()), columns_index[0:3])

# Aplica correção monetária
data["valor_producao"] = data["valor_producao"].astype("float")
data["valor_producao"] = data["valor_producao"].apply(lambda x: currency_fix(x) if isinstance(x, str) else x)

data = data[
    ['ano', 
     'id_municipio', 
     'produto', 
     'quantidade_produzida', 
     'valor_producao', 
     'area_plantada',  
     'area_colhida', 
     'rendimento_medio_producao'
    ]
    ]

with PostgresETL(
    host='localhost', 
    database=os.getenv("DB_SILVER_ZONE"), 
    user=os.getenv("POSTGRES_USER"), 
    password=os.getenv("POSTGRES_PASSWORD"),
    schema='al_ibge_pam') as db:
        
        
        columns = {
            'ano': 'integer',
            'id_municipio': 'VARCHAR(7)',
            'produto': 'VARCHAR(255)',
            'quantidade_produzida': 'numeric',
            'valor_producao': 'numeric',
            'area_plantada' : 'numeric',
            'area_colhida' : 'numeric',
            'rendimento_medio_producao' : 'numeric',
        }

        db.create_table(f'{TABLE}', columns, drop_if_exists=True)
        
        db.load_data(f'{TABLE}', data, if_exists='replace')