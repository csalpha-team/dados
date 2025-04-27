
import os
import pandas as pd
import json
import requests
from raw.br_ibge_pam.utils import (
    parse_pam_json,
)
from raw.utils.postgres_interactions import PostgresETL


URL = 'https://servicodados.ibge.gov.br/api/v3/agregados/1849/periodos/2007|2008|2009|2010|2011|2012|2013|2014|2015|2016|2017|2018|2019|2020|2021|2022/variaveis/839|840|810|811?localidades=N3[11,12,13,14,15,16,17,21,51]&classificacao=12762[all]'

if __name__ == "__main__":
    data = requests.get(URL).json()