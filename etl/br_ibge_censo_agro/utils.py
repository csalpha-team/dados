
import json
import pandas as pd



def parse_agrocenso_json(data: dict, id_produto: str, id_tipo_agricultura: str) -> pd.DataFrame:
    """
    Analisa os dados JSON da API do Agrocenso e retorna um DataFrame estruturado.
    
    Parameters:
    data (dict): Os dados JSON da API do Agrocenso
    id_produto (str): O ID da classificação do produto a ser filtrado
    id_tipo_agricultura (str): O ID da classificação do tipo de agricultura a ser filtrado
    
    Returns:
    pd.DataFrame: Um DataFrame contendo dados de extração estruturados com ID do município e unidade de medida
    """
    extraction_data = []
    
    # Verifica se os dados estão no formato esperado
    if not isinstance(data, list):
        # Se data for um dicionário com uma chave que contém a lista principal
        if isinstance(data, dict) and any(isinstance(data.get(key), list) for key in data):
            for key in data:
                if isinstance(data[key], list):
                    data = data[key]
                    break
        else:
            raise ValueError("Formato de dados inválido. Esperava uma lista ou dicionário com lista.")
    
    for variable in data:
        var_id = variable.get('id', '')
        var_name = variable.get('variavel', '')
        var_unit = variable.get('unidade', '')
        
        for result in variable.get('resultados', []):
            # Inicializa variáveis para esta iteração
            produto_info = None
            tipo_agricultura_info = None
            
            # Busca as classificações específicas
            for classificacao in result.get('classificacoes', []):
                class_id = classificacao.get('id', '')
                
                # Encontra a classificação do produto
                if class_id == id_produto:
                    nome_classificacao = classificacao.get('nome', '')
                    # Obtém o primeiro item do dicionário de categoria
                    categoria_key = list(classificacao.get('categoria', {}).keys())[0]
                    categoria_value = classificacao.get('categoria', {}).get(categoria_key, '')
                    produto_info = {
                        'id': class_id,
                        'nome': nome_classificacao,
                        'codigo': categoria_key,
                        'valor': categoria_value
                    }
                
                # Encontra a classificação do tipo de agricultura
                if class_id == id_tipo_agricultura:
                    nome_classificacao = classificacao.get('nome', '')
                    # Obtém o primeiro item do dicionário de categoria
                    categoria_key = list(classificacao.get('categoria', {}).keys())[0]
                    categoria_value = classificacao.get('categoria', {}).get(categoria_key, '')
                    tipo_agricultura_info = {
                        'id': class_id,
                        'nome': nome_classificacao,
                        'codigo': categoria_key,
                        'valor': categoria_value
                    }
            
            # Se ambas as classificações foram encontradas, processa os dados da série
            if produto_info and tipo_agricultura_info:
                for series_item in result.get('series', []):
                    locality = series_item.get('localidade', {}).get('nome', '')
                    municipality_id = series_item.get('localidade', {}).get('id', '')
                    
                    for year, value in series_item.get('serie', {}).items():
                        extraction_data.append({
                            'id_variavel': var_id,
                            'nome_variavel': var_name,
                            'unidade_medida': var_unit,
                            'id_tipo_agricultura': tipo_agricultura_info['codigo'],
                            'tipo_agricultura': tipo_agricultura_info['valor'],
                            'id_produto': produto_info['codigo'],
                            'produto': produto_info['valor'],
                            'nome_municipio': locality,
                            'id_municipio': municipality_id,
                            'ano': year,
                            'valor': value
                        })
    
    return pd.DataFrame(extraction_data)