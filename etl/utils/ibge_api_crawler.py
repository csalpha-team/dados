import aiohttp
import os
import json
from tqdm.asyncio import tqdm
from aiohttp import ClientTimeout, TCPConnector
from tqdm import tqdm
from typing import Dict, Any, List
import asyncio
import pandas as pd


async def fetch(session: aiohttp.ClientSession, url: str) -> Dict[str, Any]:
    """
    Faz uma requisição GET à API e retorna a resposta em formato JSON.

    Parâmetros:
    - session (aiohttp.ClientSession): A sessão do cliente aiohttp.
    - url (str): A URL da API para a qual a requisição será feita.

    Retorna:
    - Dict[str, Any]: A resposta da API em formato JSON.
    """
    async with session.get(url) as response:
        return await response.json()

async def async_crawler(years: List[int], variables: List[str],
    api_url_base: str, agregado: str, nivel_geografico, localidades, classificacao, categorias ) -> None:
    """
    Faz requisições para a API para cada ano, variável e categoria, salvando as respostas em arquivos JSON.

    Parâmetros:
    - years (List[int]): Lista de anos para os quais os dados serão consultados.
    - variables (List[str]): Lista de variáveis da tabela a serem consultadas.
    - categories (List[str]): Lista de categorias a serem consultadas.

    Retorna:
    - None
    """
    for year in years:
        print(f'Consultando dados do ano: {year}')
        async with aiohttp.ClientSession(connector=TCPConnector(limit=100, force_close=True), timeout=ClientTimeout(total=1200)) as session:
            tasks = []
            for variable in variables:
                url = api_url_base.format(agregado, year, variable, nivel_geografico, localidades, classificacao, categorias)
                print(url)
                task = fetch(session, url)
                tasks.append(asyncio.ensure_future(task))
            responses = []
            for future in tqdm(asyncio.as_completed(tasks), total=len(tasks)):
                try:
                    response = await future
                    responses.append(response)
                except asyncio.TimeoutError:
                    print(f"Request timed out for {url}")
            
            os.makedirs('../json', exist_ok=True)
            with open(f'../json/{year}.json', 'a') as f:
                json.dump(responses, f)




async def async_crawler_ibge_municipio(
    year: List[int], 
    variables: List[str],
    api_url_base: str, 
    agregado: str, 
    nivel_geografico: str, 
    localidades: pd.DataFrame, 
    classificacao: List[str],
    nome_tabela: str
    ) -> None:
    """
    Faz requisições para a API para cada ano, variável e categoria, salvando as respostas em arquivos JSON.
    Processa municípios em grupos de 20 para otimizar as requisições.
    Este crawler foi idealizado para extrair dados por município. Essa foi a forma mais geral utilizada
    para contornar a limitação da API do IBGE. 
    """
    
    all_municipios = localidades['id_municipio'].tolist()
    
    batch_size = 30
    
    for i in range(0, len(all_municipios), batch_size):
        batch_municipios = all_municipios[i:i+batch_size]
        print(f'Consultando dados dos municípios: {i+1}-{min(i+batch_size, len(all_municipios))} de {len(all_municipios)}')
        
        async with aiohttp.ClientSession(
            connector=TCPConnector(limit=100, force_close=True), 
            timeout=ClientTimeout(total=1200)
        ) as session:
            tasks = []
            
            for localidade in batch_municipios:
                url = api_url_base.format(
                    agregado, 
                    year, 
                    variables, 
                    nivel_geografico, 
                    localidade,
                    classificacao, 
                )
                print(f"URL for municipio {localidade}: {url}")
                task = fetch(session, url)
                tasks.append((localidade, asyncio.ensure_future(task)))
            
            for localidade, future in tqdm(tasks, total=len(tasks)):
                try:
                    response = await future
                    os.makedirs(f'../tmp/{nome_tabela}', exist_ok=True)
                    with open(f'../tmp/{nome_tabela}/{localidade}.json', 'w') as f:
                        json.dump(response, f)
                except asyncio.TimeoutError:
                    print(f"Request timed out for municipality {localidade}")
                except Exception as e:
                    print(f"Error processing municipality {localidade}: {str(e)}")
        
        await asyncio.sleep(1)




async def async_crawler3(years: List[int], variables: List[str],
    api_url_base: str, agregado: str, nivel_geografico, localidades, classificacao, categorias ) -> None:
    """
    Faz requisições para a API para cada ano, variável e categoria, salvando as respostas em arquivos JSON.

    Parâmetros:
    - years (List[int]): Lista de anos para os quais os dados serão consultados.
    - variables (List[str]): Lista de variáveis da tabela a serem consultadas.
    - categories (List[str]): Lista de categorias a serem consultadas.

    Retorna:
    - None
    """
    for year in years:
        print(f'Consultando dados do ano: {year}')
        async with aiohttp.ClientSession(connector=TCPConnector(limit=100, force_close=True), timeout=ClientTimeout(total=1200)) as session:
            tasks = []
            for variable in variables:
                url = api_url_base.format(agregado, year, variable, nivel_geografico, localidades, classificacao, categorias)
                print(url)
                task = fetch(session, url)
                tasks.append(asyncio.ensure_future(task))
            responses = []
            for future in tqdm(asyncio.as_completed(tasks), total=len(tasks)):
                try:
                    response = await future
                    responses.append(response)
                except asyncio.TimeoutError:
                    print(f"Request timed out for {url}")
            
            os.makedirs('../json', exist_ok=True)
            with open(f'../json/{year}.json', 'a') as f:
                json.dump(responses, f)
