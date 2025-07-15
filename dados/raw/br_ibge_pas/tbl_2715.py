
import os
import pandas as pd
import json
import requests
from dotenv import load_dotenv
from dados.raw.utils.postgres_interactions import PostgresETL
from dados.raw.al_ibge_pac.utils import parse_pac_json_to_table
#https://servicodados.ibge.gov.br/api/docs/agregados?versao=3#api-bq
#https://sidra.ibge.gov.br/tabela/2715
URL = 'https://servicodados.ibge.gov.br/api/v3/agregados/2715/periodos/{}/variaveis/630|631|672|673?localidades=N1[all]&classificacao=12354[all]|12355[all]'

TABLE_ID= 'tbl_2715'

load_dotenv()

if __name__ == "__main__":
        
    raw_jsons = list()
    
    for year in range(2007, 2024):
        url_formata = URL.format(year)
        data = requests.get(url_formata).json() 
        raw_jsons.append(data)
        del data
        
    parsed_data = []
    
    for json in raw_jsons:
        df = parse_pac_json_to_table(json)
        parsed_data.append(df)
        
    del raw_jsons
    
    df = pd.concat(parsed_data, ignore_index=True)
    
    print(df.head(), df.shape)
    del parsed_data
    
    with PostgresETL(
        host='localhost', 
        database=os.getenv("DB_RAW_ZONE"), 
        user=os.getenv("POSTGRES_USER"), 
        password=os.getenv("POSTGRES_PASSWORD"),
        schema='br_ibge_pas') as db:
            
            columns = {
                'id_variavel': 'VARCHAR(255)',
                'nome_variavel': 'VARCHAR(255)',
                'unidade_medida': 'VARCHAR(255)',
                'id_classificacao_regiao': 'VARCHAR(255)',
                'nome_classificacao_regiao': 'VARCHAR(255)',
                'id_categoria_regiao': 'VARCHAR(255)',
                'nome_categoria_regiao': 'VARCHAR(255)',
                'id_classificacao_comercio': 'VARCHAR(255)',
                'nome_classificacao_comercio': 'VARCHAR(255)',
                'id_categoria_comercio': 'VARCHAR(255)',
                'nome_categoria_comercio': 'VARCHAR(255)',
                'id_localidade': 'VARCHAR(255)',
                'nome_localidade': 'VARCHAR(255)',
                'id_nivel': 'VARCHAR(255)',
                'nome_nivel': 'VARCHAR(255)',
                'ano': 'VARCHAR(255)',
                'valor': 'VARCHAR(255)',
            }
                 
            db.create_table(TABLE_ID, columns, drop_if_exists=True)
            
            db.load_data(TABLE_ID, df, if_exists='replace')
      
    