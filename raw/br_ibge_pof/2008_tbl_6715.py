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

#https://servicodados.ibge.gov.br/api/docs/agregados?versao=3#api-bq
API_URL_BASE        = "https://servicodados.ibge.gov.br/api/v3/agregados/{}/periodos/{}/variaveis/{}?localidades={}[{}]&classificacao={}"
AGREGADO         = "6715"
PERIODOS         = [2018]
VARIAVEIS        = "|".join(["1201","1204"])
NIVEL_GEOGRAFICO = "N1"
LOCALIDADES      = "all"
CLASSIFICACAO    = "339[all]|12190[all]"


if __name__ == "__main__":
    asyncio.run(
        async_crawler(
            years=ANOS_RESTANTES, 
            variables=VARIAVEIS,
            categorias=CATEGORIAS,
            api_url_base=API_URL_BASE,
            agregado=AGREGADO,
            nivel_geografico=NIVEL_GEOGRAFICO,
            localidades=LOCALIDADES,
            classificacao=CLASSIFICACAO,
        )
    )
