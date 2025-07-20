from dotenv import load_dotenv
import os
from dados.raw.utils.postgres_interactions import PostgresETL
from dados.silver.utils import (
    fix_ibge_digits,
    check_duplicates,
)


load_dotenv()

query = """
SELECT 
ano,
nome_variavel,
tipo_agricultura,
id_municipio,
valor
FROM al_ibge_censoagro.tbl_2782_2006
where aspecto_pessoal_ocupado = 'Total' AND
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
columns_index = ["id_municipio", "ano",  "tipo_agricultura", "nome_variavel"]

check_duplicates(data, columns_index)

#pivotar tabela
data = data.pivot_table(
    index=columns_index[0:3],
    columns=["nome_variavel"],
    values="valor",
    aggfunc="sum"
).reset_index()

# renomear colunas
cols = {
    "Pessoal ocupado em estabelecimentos agropecuários em 31/12 com laço de parentesco com o produtor": "pessoal_total_ocupado_familia",
    "Pessoal ocupado em estabelecimentos agropecuários em 31/12 com 14 anos e mais de idade e com laço de parentesco com o produtor": "pessoal_ocupado_mais_14_anos_familia",
}

data.rename(columns=cols, inplace=True)    

# Padroniza tipo agricultura
dicionario_tipo_agricultura = {
    "Agricultura familiar - Lei 11.326": "agricultura familiar",
    "Agricultura não familiar": "agricultura não familiar",
}


data["tipo_agricultura"] = data.tipo_agricultura.map(dicionario_tipo_agricultura)


data = fix_ibge_digits(data,list(cols.values()), ['id_municipio', 'ano', 'tipo_agricultura'])



data = data[['ano', 'id_municipio',  'tipo_agricultura',
       'pessoal_ocupado_mais_14_anos_familia', 'pessoal_total_ocupado_familia',]]


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
            'pessoal_total_ocupado_familia': 'INTEGER',
            'pessoal_ocupado_mais_14_anos_familia': 'INTEGER',
        }
            
        db.create_table('tbl_2782_2006', columns, drop_if_exists=True)
        
        db.load_data('tbl_2782_2006', data, if_exists='replace')
      
