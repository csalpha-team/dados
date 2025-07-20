from dotenv import load_dotenv
import os
from dados.raw.utils.postgres_interactions import PostgresETL
from dados.silver.utils import (
    fix_ibge_digits,
    check_duplicates,
)


load_dotenv()

TABLE_ID = 'tbl_6885_2017'

query = f"""
SELECT 
ano,
nome_variavel,
tipo_agricultura,
id_municipio,
valor
FROM al_ibge_censoagro.{TABLE_ID}
where tipo_agricultura IN ('Agricultura familiar - sim', 'Agricultura familiar - não') AND
faixa_idade = 'Total' AND
nome_variavel IN (
    'Pessoal ocupado em estabelecimentos agropecuários',
    'Número de estabelecimentos agropecuários com pessoal ocupado',
    'Pessoal ocupado em estabelecimentos agropecuários com laço de parentesco com o produtor',
    'Número de estabelecimentos agropecuários com pessoal ocupado com laço de parentesco com o produtor',
    'Pessoal ocupado em estabelecimentos agropecuários sem laço de parentesco com o produtor',
    'Número de estabelecimentos agropecuários com pessoal ocupado sem laço de parentesco com o produtor');
"""


with PostgresETL(
        host='localhost', 
        database=os.getenv("DB_RAW_ZONE"), 
        user=os.getenv("POSTGRES_USER"), 
        password=os.getenv("POSTGRES_PASSWORD"),
        schema='al_ibge_censoagro') as db:
    
    data = db.download_data(query)
    
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
    "Pessoal ocupado em estabelecimentos agropecuários": "pessoal_total_ocupado",
    "Número de estabelecimentos agropecuários com pessoal ocupado": "quantidade_total_estabecimentos",
    "Pessoal ocupado em estabelecimentos agropecuários com laço de parentesco com o produtor" : "pessoal_ocupado_familia",
    "Número de estabelecimentos agropecuários com pessoal ocupado com laço de parentesco com o produtor" : "quantidade_estabecimentos_pessoal_ocupado_familia",
    "Pessoal ocupado em estabelecimentos agropecuários sem laço de parentesco com o produtor" : "pessoal_ocupado_fora_familia",
    "Número de estabelecimentos agropecuários com pessoal ocupado sem laço de parentesco com o produtor": "quantidade_estabecimentos_pessoal_ocupado_fora_familia",

   
}

data.rename(columns=cols, inplace=True)    

# Padroniza tipo agricultura
dicionario_tipo_agricultura = {
    "Agricultura familiar - sim": "agricultura familiar",
    "Agricultura familiar - não": "agricultura não familiar",
}

data["tipo_agricultura"] = data.tipo_agricultura.map(dicionario_tipo_agricultura)

#Para fazer esse fix ibge digits aqui preciso seleiconar as variaveis de quantidade de temporario e afins,
#tratar elas primeiro 

data = fix_ibge_digits(data,['pessoal_total_ocupado'], ['id_municipio', 'ano', 'tipo_agricultura'],div_column='quantidade_total_estabecimentos')
data = fix_ibge_digits(data,['pessoal_ocupado_familia'], ['id_municipio', 'ano', 'tipo_agricultura'],div_column='quantidade_estabecimentos_pessoal_ocupado_familia')
data = fix_ibge_digits(data,['pessoal_ocupado_fora_familia'], ['id_municipio', 'ano', 'tipo_agricultura'],div_column='quantidade_total_estabecimentos')



data = data[
    ['ano', 
     'id_municipio',  
     'tipo_agricultura',
    'pessoal_total_ocupado', 
    'quantidade_total_estabecimentos',
    'pessoal_ocupado_familia',
    'quantidade_estabecimentos_pessoal_ocupado_familia',
    'pessoal_ocupado_fora_familia', 
    'quantidade_estabecimentos_pessoal_ocupado_fora_familia']]


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
            'pessoal_total_ocupado': 'INTEGER',
            'quantidade_total_estabecimentos': 'INTEGER',
            'pessoal_ocupado_familia': 'INTEGER',
            'quantidade_estabecimentos_pessoal_ocupado_familia' : 'INTEGER',
            'pessoal_ocupado_fora_familia' : 'INTEGER',
            'quantidade_estabecimentos_pessoal_ocupado_fora_familia' : 'INTEGER'
        }
            
        db.create_table(TABLE_ID, columns, drop_if_exists=True)
        
        db.load_data(TABLE_ID, data, if_exists='replace')
      
