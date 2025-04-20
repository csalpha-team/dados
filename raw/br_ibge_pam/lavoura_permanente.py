import os
import asyncio
import pandas as pd
import json
import basedosdados as bd
from raw.utils.ibge_api_crawler import (
    async_crawler_ibge_municipio
    
)

from raw.br_ibge_pam.utils import (
    parse_pam_json,
)
from dotenv import load_dotenv
from raw.utils.postgres_interactions import PostgresETL

#TODO 
#REFATORAR LOGICA DE UPLOAD DOS DADOS; FAZER PROCESSAMENTO EM CHUNKS COM APPEND
#GERAR URLS PRONTAS; E ITERAR SOBRE COM APPEND;

load_dotenv()
billing_id = os.getenv("BASEDOSDADADOS_PROJECT_ID")

API_URL_BASE        = "https://servicodados.ibge.gov.br/api/v3/agregados/{}/periodos/{}/variaveis/{}?localidades={}[{}]&classificacao={}"
AGREGADO            = "1613" # É a tabela no SIDRA
PERIODOS            = 'all'
VARIAVEIS           = "|".join(["2313", "1002313", "216", "1000216", "214", "112","215",
                       "1000215"]) # As variáveis da tabela
NIVEL_GEOGRAFICO    = "N6" # N6 = Municipal
LOCALIDADES         = "all"
CLASSIFICACAO       = "82[all]" # Código pré-definido por agregado

nome_tabela = 'lavoura_permanente'

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
    
    print('------ Fazendo o parse dos arquivos JSON ------')
    files = os.listdir(f"../tmp/{nome_tabela}")
    
    assert len(files) == 772, 'Existem 772 municípios na Amazônia Legal. Deveriam existir 772 items na lista de dfs. Verifique se o download foi feito corretamente.'

    df_list = []
    
    for file in files:
        
        with open(f"../tmp/{nome_tabela}/{file}", "r") as f:
            
            data = json.load(f)
            
            print(f"fazendo parsing do json com base no arquivo: {file}...")
            tbl = parse_pam_json(data, id_produto="82")

            #NOTE: essas operações serão feitas na etapa seguinte
            #print("Tratando a questão dos pesos dos produtos... Impacto em: quantidade_producao e rendimento_medio_producao")
            #tbl = tbl.apply(products_weight_ratio_fix, axis=1)
            #print("Aplicando a correção nominal retroativa da moeda... Impacto: valor_producao")
            #tbl["valor_producao"] = tbl.apply(currency_fix, axis=1)
            #print("Transformações finalizadas!")
            
            df_list.append(tbl)
            print("Adicionando o DataFrame à lista de DataFrames...")
            del tbl


    df = pd.concat(df_list, ignore_index=True)

    print('------ Carregando tabela no Banco de Dados ------')        

    with PostgresETL(
        host='localhost', 
        database=os.getenv("DB_RAW_ZONE"), 
        user=os.getenv("POSTGRES_USER"), 
        password=os.getenv("POSTGRES_PASSWORD"),
        schema='al_ibge_pam') as db:
            
            
            columns = {
                'id_variavel': 'VARCHAR(255)',
                'nome_variavel': 'VARCHAR(255)',
                'unidade_medida': 'VARCHAR(255)',
                'id_produto': 'VARCHAR(255)',
                'produto': 'VARCHAR(255)',
                'nome_municipio': 'VARCHAR(255)',
                'id_municipio': 'VARCHAR(255)',
                'ano': 'VARCHAR(255)',
                'valor': 'VARCHAR(255)',
            }
                
                
                
            db.create_table(nome_tabela, columns, if_not_exists=True)
            
            db.load_data(nome_tabela, df, if_exists='replace')
        