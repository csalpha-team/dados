import os
import pandas as pd
import json
import basedosdados as bd
import requests

from raw.br_ibge_pam.utils import (
    parse_pam_json,
)

from raw.utils.postgres_interactions import PostgresETL

#https://servicodados.ibge.gov.br/api/docs/agregados?versao=3#api-bq
#NOTE: esta tabela só é disponibilizada a nível de UF. Uma única requisição consegue exatrair todos os valores
#NOTE: diferente das demais tabelas. Logo, será utilizado um simples get

URL = 'https://servicodados.ibge.gov.br/api/v3/agregados/2393/periodos/2002|2008|2018/variaveis/1207?localidades=N3[11,12,13,14,15,16,17,21,51]&classificacao=217[all]'
nome_tabela = 'tbl_2393'


if __name__ == "__main__":
    data = requests.get(url=URL).json()
    #dump json
    with open(f'../tmp/{nome_tabela}.json', 'w') as outfile:
        json.dump(data, outfile)