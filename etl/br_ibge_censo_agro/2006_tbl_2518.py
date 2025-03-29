import asyncio
import pandas as pd
import basedosdados as bd
import dotenv
import json
import os
import tqdm
from etl.utils.ibge_api_crawler import (
    async_crawler_censoagro,
)
from etl.br_ibge_censo_agro.utils import parse_agrocenso_json
from etl.utils.postgres_interactions import PostgresETL


dotenv.load_dotenv()
billing_id = os.getenv("BASEDOSDADADOS_PROJECT_ID")

#https://servicodados.ibge.gov.br/api/docs/agregados?versao=3#api-bq
API_URL_BASE        = "https://servicodados.ibge.gov.br/api/v3/agregados/{}/periodos/{}/variaveis/{}?localidades={}[{}]&classificacao={}[{}]"
AGREGADO         = "2518"
PERIODOS         = "2006"
VARIAVEIS        = "|".join(["2381","2382","2384","2383","2390","2385", "2386", "2387", "2388", "2389"])
NIVEL_GEOGRAFICO = "N6"
LOCALIDADES      = "all"
CLASSIFICACAO    = "227"
CATEGORIAS       = "all"


if __name__ == "__main__":
    
    print('------ Baixando tabela de municipios ------')
    municipios = bd.read_sql(
        """
        SELECT id_municipio
        FROM `basedosdados.br_bd_diretorios_brasil.municipio`
        WHERE amazonia_legal = 1
        """,
        billing_project_id=billing_id,
    )
    
    print('------ Baixando dados da API ------')
    asyncio.run(
        async_crawler_censoagro(
            year=PERIODOS, 
            variables=VARIAVEIS,
            categorias=CATEGORIAS,
            api_url_base=API_URL_BASE,
            agregado=AGREGADO,
            nivel_geografico=NIVEL_GEOGRAFICO,
            localidades=municipios,
            classificacao=CLASSIFICACAO,
        )
    )
    
    files = os.listdir("../json")
    
    df = pd.DataFrame()
    
    print('------ Fazendo o parse dos arquivos JSON ------')
    for file in tqdm.tqdm(files):
        
        with open(f"../json/{file}", "r") as f:
            data = json.load(f)
        
            tbl = parse_agrocenso_json(
                data,
                tipo_classificacao="Produtos da lavoura permanente"
            )
            
            df = pd.concat([df, tbl], ignore_index=True)
            
            del tbl
    
    print('------ Carregando tabela no Banco de Dados ------')        
    with PostgresETL(
        host='localhost', 
        database=os.getenv("DB_RAW_ZONE"), 
        user=os.getenv("POSTGRES_USER"), 
        password=os.getenv("POSTGRES_PASSWORD"),
        schema='br_ibge_censoagro') as db:
            
            
            columns = {
                'id_variavel': 'VARCHAR(255)',
                'nome_variavel': 'VARCHAR(255)',
                'unidade_medida': 'VARCHAR(255)',
                'id_produto': 'VARCHAR(255)',
                'nome_produto': 'VARCHAR(255)',
                'nome_municipio': 'VARCHAR(255)',
                'id_municipio': 'VARCHAR(255)',
                'ano': 'VARCHAR(255)',
                'valor': 'VARCHAR(255)',
            }
                
                
            db.create_table('tbl_2518_2006', columns, if_not_exists=True)
            
            db.load_data('tbl_2518_2006', df, if_exists='replace')
      