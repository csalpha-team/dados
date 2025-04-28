import os
import pandas as pd
import json
import requests
from raw.br_ibge_pam.utils import (
    parse_pam_json,
)
from raw.utils.postgres_interactions import PostgresETL



#https://servicodados.ibge.gov.br/api/docs/agregados?versao=3#api-bq
URL = 'https://servicodados.ibge.gov.br/api/v3/agregados/6715/periodos/2018/variaveis/1201|1204?localidades=N3[11,12,13,14,15,16,17,21,51]&classificacao=12190[all]'


if __name__ == "__main__":
    
    data = requests.get(url=URL).json()
