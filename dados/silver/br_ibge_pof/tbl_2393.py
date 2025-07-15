from dotenv import load_dotenv
import os
from dados.raw.utils.postgres_interactions import PostgresETL


load_dotenv()
TABLE="tbl_2393"

query = f"""
select
nome_variavel,
unidade_medida,
classificacao_nome,
nome_categoria,
nome_localidade,
nivel_nome,
cast(ano as integer) as ano,
valor
from br_ibge_pof.{TABLE}
"""


with PostgresETL(
        host='localhost', 
        database=os.getenv("DB_RAW_ZONE"), 
        user=os.getenv("POSTGRES_USER"), 
        password=os.getenv("POSTGRES_PASSWORD"),
        schema='br_ibge_pof') as db:
    
    data = db.download_data(query)
    
    

# Checar existência de duplicatas por segurança

#pivotar tabela
data = data.pivot_table(
    index=['ano','unidade_medida', 'nome_categoria', 'nome_localidade' ],
    columns=["nome_variavel"],
    values="valor",
    aggfunc="sum"
).reset_index()


# renomear colunas
cols = {
    "Aquisição alimentar domiciliar per capita anual": "quantidade_aquisicao_alimentar_per_capta",
    "nome_categoria": "categoria_alimento",
    "nome_localidade": "uf",
}

data.rename(columns=cols, inplace=True)    


data = data[['ano', 'uf',  'categoria_alimento', 'quantidade_aquisicao_alimentar_per_capta',
       'unidade_medida',]]
    
with PostgresETL(
    host='localhost', 
    database=os.getenv("DB_SILVER_ZONE"), 
    user=os.getenv("POSTGRES_USER"), 
    password=os.getenv("POSTGRES_PASSWORD"),
    schema='br_ibge_pof') as db:
        
        
        columns = {
            'ano': 'integer',
            'uf': 'VARCHAR(255)',
            'categoria_alimento': 'VARCHAR(255)',
            'quantidade_aquisicao_alimentar_per_capta': 'VARCHAR(255)',
            'unidade_medida': 'VARCHAR(255)',
        }
            
        db.create_table(f'{TABLE}', columns, drop_if_exists=True)
        
        db.load_data(f'{TABLE}', data, if_exists='replace')
      
https://www.iadb.org/document.cfm?id=EZIDB0001365-866096730-21