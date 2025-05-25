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

    Raises:
    - aiohttp.ClientResponseError: Quando o servidor retorna um código de status de erro (4xx ou 5xx) após todas as tentativas.
    - aiohttp.ClientError: Quando ocorre um erro de cliente durante a requisição.
    - asyncio.TimeoutError: Quando a requisição excede o tempo limite configurado.
    - Exception: Quando ocorre um erro não tratado ou após falha em todas as tentativas.
    """
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            async with session.get(url) as response:
                # Verifica o status code
                if 500 <= response.status < 600:
                    # Erro do servidor (5xx)
                    retry_count += 1
                    if retry_count < max_retries:
                        print(f"Recebido status {response.status} para {url}. Tentando novamente em 6 segundos... (Tentativa {retry_count}/{max_retries})")
                        await asyncio.sleep(6)
                        continue
                    else:
                        # Esgotou as tentativas
                        raise aiohttp.ClientResponseError(
                            request_info=response.request_info,
                            history=response.history,
                            status=response.status,
                            message=f"Falha após {max_retries} tentativas com status {response.status}",
                        )
                
                # Verifica se o status é de sucesso
                response.raise_for_status()  # Levanta exceção para status codes 4xx ou 5xx
                
                # Retorna o JSON se tudo estiver OK
                return await response.json()
        
        except aiohttp.ClientResponseError as e:
            # Já tratamos os erros 5xx acima, então aqui lidamos apenas com outros erros
            if 500 <= e.status < 600 and retry_count < max_retries:
                retry_count += 1
                print(f"Erro de conexão com status {e.status} para {url}. Tentando novamente em 6 segundos... (Tentativa {retry_count}/{max_retries})")
                await asyncio.sleep(6)
            else:
                # Outros erros ou esgotou as tentativas para 5xx
                print(f"Erro: {e} para {url}")
                raise
                
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            # Erros de conexão ou timeout
            retry_count += 1
            if retry_count < max_retries:
                print(f"Erro de conexão: {str(e)} para {url}. Tentando novamente em 6 segundos... (Tentativa {retry_count}/{max_retries})")
                await asyncio.sleep(6)
            else:
                print(f"Falha após {max_retries} tentativas: {str(e)} para {url}")
                raise
    
    # Não deveria chegar aqui, mas por segurança:
    raise Exception(f"Falha ao buscar {url} após múltiplas tentativas")

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
        
    batch_size = 60
    
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
                #print(f"URL for municipio {localidade}: {url}")
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
