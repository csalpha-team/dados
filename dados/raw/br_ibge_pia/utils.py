
from typing import List, Dict, Any
import pandas as pd
import requests


def parse_pia_json_to_table(json_data: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Parser para converter dados da PIA-IBGE de JSON para DataFrame
    
    Args:
        json_data: Lista com dados das variáveis da PIA
        
    Returns:
        DataFrame com dados estruturados
    """
    
    # Lista para armazenar todos os registros
    records = []
    
    # Processa cada variável no JSON
    for variavel in json_data:
        id_variavel = variavel['id']
        nome_variavel = variavel['variavel']
        unidade_medida = variavel['unidade']
        
        # Processa cada resultado da variável
        for resultado in variavel['resultados']:
            # Obtém informações das classificações (CNAE)
            for classificacao in resultado['classificacoes']:
                classificacao_nome = classificacao['nome']
                
                # Processa cada categoria dentro da classificação
                for id_categoria, nome_categoria in classificacao['categoria'].items():
                    
                    # Processa cada série temporal
                    for serie in resultado['series']:
                        id_localidade = serie['localidade']['id']
                        nome_localidade = serie['localidade']['nome']
                        nivel_nome = serie['localidade']['nivel']['nome']
                        
                        # Processa cada ano na série temporal
                        for ano, valor in serie['serie'].items():
                            record = {
                                'id_variavel': id_variavel,
                                'nome_variavel': nome_variavel,
                                'unidade_medida': unidade_medida,
                                'classificacao_nome': classificacao_nome,
                                'id_categoria': id_categoria,
                                'nome_categoria': nome_categoria,
                                'id_localidade': id_localidade,
                                'nome_localidade': nome_localidade,
                                'nivel_nome': nivel_nome,
                                'ano': int(ano),
                                'valor': valor
                            }
                            records.append(record)
    
    # Converte para DataFrame
    df = pd.DataFrame(records)
    
    return df





def download_json(url: str, uf_id_sigla: dict) -> List[Dict[str, Any]]:
    """
    Função para baixar um JSON de uma URL
    
    Args:
        url: URL do JSON a ser baixado
        
    Returns:
        Dicionário com os dados do JSON
    """
    
    dados =  []
    
    for uf_id, sigla in uf_id_sigla.items():
        url_formatada = url.format(uf_id)
        print(url_formatada)
        print(uf_id)
        print(f"Baixando dados do Estado {sigla} - ({uf_id})")
        req = requests.get(url_formatada)
        
        if req.status_code == 200:
            data = req.json()
            dados.append(data)
        else:
            print(f"Erro ao baixar dados para {sigla} ({uf_id})")
            print(f"Status HTTP: {req.status_code}")
    return dados