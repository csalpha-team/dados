import json
import pandas as pd
from typing import List, Dict, Any

def parse_pac_json_to_table(json_data: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Parser para converter dados da PAC-IBGE de JSON para DataFrame
    
    Args:
        json_data: Lista com dados das variáveis da PAC
        
    Returns:
        DataFrame com dados estruturados
    """
    
    # Lista para armazenar todos os registros
    records = []
    
    # Processa cada variável no JSON
    for variavel in json_data:
        variavel_id = variavel['id']
        variavel_nome = variavel['variavel']
        unidade = variavel['unidade']
        
        # Processa cada resultado da variável
        for resultado in variavel['resultados']:
            
            # Extrai as classificações (geralmente são 2: região/UF e divisão de comércio)
            classificacoes = resultado['classificacoes']
            
            # Primeira classificação (Regiões/UF)
            classificacao_regiao = classificacoes[0]
            classificacao_regiao_id = classificacao_regiao['id']
            classificacao_regiao_nome = classificacao_regiao['nome']
            categoria_regiao_id = list(classificacao_regiao['categoria'].keys())[0]
            categoria_regiao_nome = list(classificacao_regiao['categoria'].values())[0]
            
            # Segunda classificação (Divisão de comércio)
            classificacao_comercio = classificacoes[1]
            classificacao_comercio_id = classificacao_comercio['id']
            classificacao_comercio_nome = classificacao_comercio['nome']
            categoria_comercio_id = list(classificacao_comercio['categoria'].keys())[0]
            categoria_comercio_nome = list(classificacao_comercio['categoria'].values())[0]
            
            # Processa cada série temporal
            for serie in resultado['series']:
                localidade_id = serie['localidade']['id']
                localidade_nome = serie['localidade']['nome']
                nivel_id = serie['localidade']['nivel']['id']
                nivel_nome = serie['localidade']['nivel']['nome']
                
                # Processa cada ano na série temporal
                for ano, valor in serie['serie'].items():
                    record = {
                        # Dados da variável
                        'id_variavel': variavel_id,
                        'nome_variavel': variavel_nome,
                        'unidade_medida': unidade,
                        
                        # Classificação de região/UF
                        'id_classificacao_regiao': classificacao_regiao_id,
                        'nome_classificacao_regiao': classificacao_regiao_nome,
                        'id_categoria_regiao': categoria_regiao_id,
                        'nome_categoria_regiao': categoria_regiao_nome,
                        
                        # Classificação de comércio
                        'id_classificacao_comercio': classificacao_comercio_id,
                        'nome_classificacao_comercio': classificacao_comercio_nome,
                        'id_categoria_comercio': categoria_comercio_id,
                        'nome_categoria_comercio': categoria_comercio_nome,
                        
                        # Localização
                        'id_localidade': localidade_id,
                        'nome_localidade': localidade_nome,
                        'id_nivel': nivel_id,
                        'nome_nivel': nivel_nome,
                        
                        # Dados temporais
                        'ano': int(ano),
                        'valor': valor,
\
                    }
                    records.append(record)
    
    # Converte para DataFrame
    df = pd.DataFrame(records)
    
    return df
