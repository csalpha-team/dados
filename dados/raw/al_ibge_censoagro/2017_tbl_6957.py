import asyncio
import pandas as pd
import basedosdados as bd
import dotenv
import json
import os
import tqdm
from dados.raw.utils.ibge_api_crawler import (
    async_crawler_ibge_municipio,
)
from dados.raw.al_ibge_censoagro.utils import parse_agrocenso_json
from dados.raw.utils.postgres_interactions import PostgresETL


dotenv.load_dotenv()
billing_id = os.getenv("BASEDOSDADADOS_PROJECT_ID")

#https://servicodados.ibge.gov.br/api/docs/agregados?versao=3#api-bq
API_URL_BASE        = "https://servicodados.ibge.gov.br/api/v3/agregados/{}/periodos/{}/variaveis/{}?localidades={}[{}]&classificacao={}"
AGREGADO         = "6957"
PERIODOS         = "2017"
VARIAVEIS        = "|".join(["10084","10085","10086","10087","10088","10089"])
NIVEL_GEOGRAFICO = "N6"
LOCALIDADES      = "all"
CLASSIFICACAO    = "226[all]|829[all]"
nome_tabela = "tbl_6957_2017"


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
        async_crawler_ibge_municipio(
            year=PERIODOS, 
            variables=VARIAVEIS,
            api_url_base=API_URL_BASE,
            agregado=AGREGADO,
            nivel_geografico=NIVEL_GEOGRAFICO,
            localidades=municipios,
            classificacao=CLASSIFICACAO,
            nome_tabela=nome_tabela,
        )
    )
    
    files = os.listdir(f"../tmp/{nome_tabela}")
    
    df_list = []
    
    print('------ Fazendo o parse dos arquivos JSON ------')
    for file in tqdm.tqdm(files):
        
        with open(f"../tmp/{nome_tabela}/{file}", "r") as f:
            data = json.load(f)
        
            tbl = parse_agrocenso_json(data, id_produto='226', id_tipo_agricultura='829')
            
            del data
            
            df_list.append(tbl)

            del tbl
            
            
    df = pd.concat(df_list, ignore_index=True)
    
    print('------ Carregando tabela no Banco de Dados ------')        
    with PostgresETL(
        host='localhost', 
        database=os.getenv("DB_RAW_ZONE"), 
        user=os.getenv("POSTGRES_USER"), 
        password=os.getenv("POSTGRES_PASSWORD"),
        schema='al_ibge_censoagro') as db:
            
            
            columns = {
                'id_variavel': 'VARCHAR(255)',
                'nome_variavel': 'VARCHAR(255)',
                'unidade_medida': 'VARCHAR(255)',
                'id_produto': 'VARCHAR(255)',
                'produto': 'VARCHAR(255)',
                'id_tipo_agricultura': 'VARCHAR(255)',
                'tipo_agricultura': 'VARCHAR(255)',
                'nome_municipio': 'VARCHAR(255)',
                'id_municipio': 'VARCHAR(255)',
                'ano': 'VARCHAR(255)',
                'valor': 'VARCHAR(255)',
            }
               
                
                
            db.create_table('tbl_6957_2017', columns, if_not_exists=True)
            
            db.load_data('tbl_6957_2017', df, if_exists='replace')
      
