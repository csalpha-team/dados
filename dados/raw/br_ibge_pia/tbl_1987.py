
import os
import pandas as pd
import json
from dotenv import load_dotenv
from dados.raw.utils.postgres_interactions import PostgresETL
from dados.dicionarios_institucionais import uf_id_sigla
from dados.raw.br_ibge_pia.utils import parse_pia_json_to_table,download_json

URL = 'https://servicodados.ibge.gov.br/api/v3/agregados/1987/periodos/1996|1997|1998|1999|2000|2001|2002|2003|2004|2005|2006|2007/variaveis/706|631|673|834|835|836|837|838|839|840|810|811?localidades=N3[{}]&classificacao=11939[all]'
TABLE_ID= 'tbl_1987'


load_dotenv()

if __name__ == "__main__":
    
    
    raw_jsons = download_json(URL, uf_id_sigla)   
    
    parsed_data = []
    
    for json in raw_jsons:
        parsed_data.append(parse_pia_json_to_table(json))
    
    del raw_jsons
    
    df = pd.concat(parsed_data, ignore_index=True)
    
    del parsed_data
    
    
    print('------ Carregando tabela no Banco de Dados ------')        
    with PostgresETL(
        host='localhost', 
        database=os.getenv("DB_RAW_ZONE"), 
        user=os.getenv("POSTGRES_USER"), 
        password=os.getenv("POSTGRES_PASSWORD"),
        schema='br_ibge_pia') as db:
            
            columns = {
                'id_variavel': 'VARCHAR(255)',
                'nome_variavel': 'VARCHAR(255)',
                'unidade_medida': 'VARCHAR(255)',
                'classificacao_nome': 'VARCHAR(255)',
                'id_categoria': 'VARCHAR(255)',
                'nome_categoria': 'VARCHAR(255)',
                'id_localidade': 'VARCHAR(255)',
                'nome_localidade': 'VARCHAR(255)',
                'nivel_nome': 'VARCHAR(255)',
                'ano': 'VARCHAR(255)',
                'valor': 'VARCHAR(255)',
            }
                 
            db.create_table(TABLE_ID, columns, drop_if_exists=True)
            
            db.load_data(TABLE_ID, df, if_exists='replace')
      