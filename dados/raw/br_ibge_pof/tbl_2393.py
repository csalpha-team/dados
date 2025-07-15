import os
from dotenv import load_dotenv
import requests

from dados.raw.br_ibge_pia.utils import parse_pia_json_to_table,download_json
from dados.dicionarios_institucionais import uf_id_sigla

from dados.raw.utils.postgres_interactions import PostgresETL

#https://servicodados.ibge.gov.br/api/docs/agregados?versao=3#api-bq
#NOTE: esta tabela só é disponibilizada a nível de UF. Uma única requisição consegue exatrair todos os valores
#NOTE: diferente das demais tabelas. Logo, será utilizado um simples get

URL = 'https://servicodados.ibge.gov.br/api/v3/agregados/2393/periodos/2002|2008|2018/variaveis/1207?localidades=N3[11,12,13,14,15,16,17,21,51]&classificacao=217[all]'
TABLE_ID = 'tbl_2393'

load_dotenv()

if __name__ == "__main__":
    #dump json
    raw_jsons = requests.get(url=URL).json()  
    
    parsed_data = parse_pia_json_to_table(raw_jsons)

    
    print('------ Carregando tabela no Banco de Dados ------')        
    with PostgresETL(
        host='localhost', 
        database=os.getenv("DB_RAW_ZONE"), 
        user=os.getenv("POSTGRES_USER"), 
        password=os.getenv("POSTGRES_PASSWORD"),
        schema='br_ibge_pof') as db:
            
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
            
            db.load_data(TABLE_ID, parsed_data, if_exists='replace')