from dotenv import load_dotenv
import os
from dados.raw.utils.postgres_interactions import PostgresETL
from dados.silver.utils import (
    fix_ibge_digits,
    check_duplicates,
)


load_dotenv()

TABLE_ID = 'tbl_1931_2006'

query = f"""
SELECT 
ano,
nome_variavel,
tipo_agricultura,
tipo_producao,
id_municipio,
valor
FROM al_ibge_censoagro.{TABLE_ID}
where tipo_agricultura IN ('Agricultura familiar - Lei 11.326', 'Agricultura não familiar') AND
tipo_agricultura != 'Total';
"""


with PostgresETL(
        host='localhost', 
        database=os.getenv("DB_RAW_ZONE"), 
        user=os.getenv("POSTGRES_USER"), 
        password=os.getenv("POSTGRES_PASSWORD"),
        schema='al_ibge_censoagro') as db:
    
    data = db.download_data(query)
    
# Checar existência de duplicatas por segurança
columns_index = ["id_municipio", "ano",  "tipo_agricultura", "tipo_producao", "nome_variavel"]

check_duplicates(data, columns_index)

#pivotar tabela
data = data.pivot_table(
    index=columns_index[0:4],
    columns=["nome_variavel"],
    values="valor",
    aggfunc="sum"
).reset_index()

# renomear colunas
cols = {
    "Valor da produção dos estabelecimentos agropecuários": "valor_producao",
    "Número de estabelecimentos agropecuários com produção": "quantidade_estabelecimentos_produtivos", 
}

data.rename(columns=cols, inplace=True)    

# Padroniza tipo agricultura
dicionario_tipo_agricultura = {
    "Agricultura familiar - Lei 11.326": "agricultura familiar",
    "Agricultura não familiar": "agricultura não familiar",
}


data["tipo_agricultura"] = data.tipo_agricultura.map(dicionario_tipo_agricultura)


data = fix_ibge_digits(data,list(cols.values()), ['id_municipio', 'ano', 'tipo_agricultura', 'tipo_producao'], div_column='quantidade_estabelecimentos_produtivos')



data = data[['ano', 'id_municipio',  'tipo_agricultura', 'tipo_producao',
       'valor_producao', 'quantidade_estabelecimentos_produtivos',]]

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
            'tipo_producao': 'VARCHAR(255)',
            'quantidade_estabelecimentos_produtivos': 'INTEGER',
            'valor_producao': 'FLOAT',
        }
            
        db.create_table(TABLE_ID, columns, drop_if_exists=True)
        
        db.load_data(TABLE_ID, data, if_exists='replace')
      
