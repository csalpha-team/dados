import os
from dotenv import load_dotenv
import requests

from dados.raw.br_ibge_pof.utils import parse_json_pof

from dados.raw.utils.postgres_interactions import PostgresETL

#https://servicodados.ibge.gov.br/api/docs/agregados?versao=3#api-bq
#NOTE: esta tabela só é disponibilizada a nível de UF. Uma única requisição consegue exatrair todos os valores
#NOTE: diferente das demais tabelas. Logo, será utilizado um simples get

URL = 'https://servicodados.ibge.gov.br/api/v3/agregados/6970/periodos/2018/variaveis/1201|1204?localidades=N1[all]&classificacao=1[1,2]|12190[all]'
TABLE_ID = 'tbl_6970'

load_dotenv()

if __name__ == "__main__":
    #dump json
    raw_jsons = requests.get(url=URL).json()  
    
    parsed_data = parse_json_pof(raw_jsons)

    print('------ Carregando tabela no Banco de Dados ------')

    with PostgresETL( 
        host='localhost', 
        database=os.getenv("DB_RAW_ZONE"), 
        user=os.getenv("POSTGRES_USER"), 
        password=os.getenv("POSTGRES_PASSWORD"),
        schema='br_ibge_pof') as db:
            
            columns = {
                'tipo_despesa': 'VARCHAR(255)',
                'situacao_domicilio': 'VARCHAR(255)',
                'localidade': 'VARCHAR(255)',
                'variavel': 'VARCHAR(255)',
                'unidade': 'VARCHAR(255)',
                'ano': 'VARCHAR(255)',
                'valor': 'VARCHAR(255)',
            }
                 
            db.create_table(TABLE_ID, columns, drop_if_exists=True)
            
            db.load_data(TABLE_ID, parsed_data, if_exists='replace')