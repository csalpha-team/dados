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
ano,
id_municipio,
produto,
quantidade,
valor
from al_ibge_pevs.produtos_extracao_vegetal
where id_municipio like '15%';
"""
#unidade_medida

with PostgresETL(
        host='localhost', 
        database=os.getenv("DB_TRUSTED_ZONE"), 
        user=os.getenv("POSTGRES_USER"), 
        password=os.getenv("POSTGRES_PASSWORD"),
        schema='al_ibge_pevs') as db:
    
    data = db.download_data(query)


    
with PostgresETL(
    host='localhost', 
    database=os.getenv("DB_AGREGATED_ZONE"), 
    user=os.getenv("POSTGRES_USER"), 
    password=os.getenv("POSTGRES_PASSWORD"),
    schema='pa_indexadores_producao_rural') as db:
        
        
        columns = {
            'ano': 'integer',
            'id_municipio': 'VARCHAR(7)',
            'produto': 'VARCHAR(255)',
            'quantidade': 'numeric',
            'valor': 'numeric',
        }
            
            
        db.create_table('pevs_extracao_vegetal', columns, if_not_exists=True)
        
        db.load_data('pevs_extracao_vegetal', data, if_exists='replace')