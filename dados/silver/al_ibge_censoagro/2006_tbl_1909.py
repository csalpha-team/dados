from dotenv import load_dotenv
import os
from dados.raw.utils.postgres_interactions import PostgresETL
from dados.silver.utils import (
    fix_ibge_digits,
    check_duplicates,
)


load_dotenv()
TABLE="tbl_1909_2006"

query = f"""
select
nome_variavel,
despesa,
tipo_agricultura,
id_municipio,
cast(ano as integer) as ano,
valor
from al_ibge_censoagro.{TABLE}
where tipo_agricultura IN ('Agricultura familiar - Lei 11.326', 'Agricultura não familiar') and
nome_variavel IN ('Número de estabelecimentos agropecuários que realizaram despesas', 'Valor das despesas realizadas pelos estabelecimentos agropecuários');
"""


with PostgresETL(
        host='localhost', 
        database=os.getenv("DB_RAW_ZONE"), 
        user=os.getenv("POSTGRES_USER"), 
        password=os.getenv("POSTGRES_PASSWORD"),
        schema='al_ibge_censoagro') as db:
    
    data = db.download_data(query)
    
    

# Checar existência de duplicatas por segurança
columns_index = ["id_municipio", "ano", "despesa", "tipo_agricultura", "nome_variavel"]

check_duplicates(data, columns_index)

#pivotar tabela
data = data.pivot_table(
    index=columns_index[0:4],
    columns=["nome_variavel"],
    values="valor",
    aggfunc="sum"
).reset_index()

print(data.columns)
print(data.head())
# renomear colunas


cols = {
    'despesa': 'tipo_despesa',
    'Número de estabelecimentos agropecuários que realizaram despesas': "quantidade_estabelecimentos_fizeram_despesa",
    'Valor das despesas realizadas pelos estabelecimentos agropecuários': "valor_despesa",

}

data.rename(columns=cols, inplace=True)    


# Padroniza tipo agricultura
dicionario_tipo_agricultura = {
    "Agricultura familiar - Lei 11.326": "agricultura familiar",
    "Agricultura não familiar": "agricultura não familiar",
}


data["tipo_agricultura"] = data.tipo_agricultura.map(dicionario_tipo_agricultura)


data = fix_ibge_digits(data,list(cols.values()), ['id_municipio', 'ano', 'tipo_despesa', 'tipo_agricultura'], div_column="quantidade_estabelecimentos_fizeram_despesa")



data = data[['ano', 'id_municipio',  'tipo_agricultura',
       'quantidade_estabelecimentos_fizeram_despesa', 'tipo_despesa', 'valor_despesa']]
    
with PostgresETL(
    host='localhost', 
    database=os.getenv("DB_SILVER_ZONE"), 
    user=os.getenv("POSTGRES_USER"), 
    password=os.getenv("POSTGRES_PASSWORD"),
    schema='al_ibge_censoagro') as db:
        
        
        columns = {
            'ano': 'integer',
            'id_municipio': 'VARCHAR(7)',
            'tipo_agricultura': 'VARCHAR(255)',
            'quantidade_estabelecimentos_fizeram_despesa': 'integer',
            'tipo_despesa': 'VARCHAR(255)',
            'valor_despesa': 'numeric',
        }
            
        db.create_table(f'{TABLE}', columns, drop_if_exists=True)
        
        db.load_data(f'{TABLE}', data, if_exists='replace')
      
