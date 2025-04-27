
import os
import pandas as pd
import json
import requests
from raw.br_ibge_pam.utils import (
    parse_pam_json,
)
from raw.utils.postgres_interactions import PostgresETL

#https://servicodados.ibge.gov.br/api/docs/agregados?versao=3#api-bq
URL = 'https://servicodados.ibge.gov.br/api/v3/agregados/1407/periodos/2007|2008|2009|2010|2011|2012|2013|2014|2015|2016|2017|2018|2019|2020|2021|2022/variaveis/368|312|314|503|866?localidades=N1[all]&classificacao=12354[106802,106775,106776,106777,106778,106779,106780,106781,106782,106799]|11066[all]'

if __name__ == "__main__":
    data = requests.get(url=URL).json()
    