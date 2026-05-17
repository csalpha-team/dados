### Código adaptado de https://github.com/basedosdados/queries-basedosdados/tree/main/models/br_ibge_pam


import numpy as np
import pandas as pd

from dados.utils.agricultural_conversions import currency_fix, products_weight_ratio_fix

__all__ = ["parse_pam_json", "rename_columns", "treat_columns", "currency_fix", "products_weight_ratio_fix"]


def parse_pam_json(data: dict, id_produto: str) -> pd.DataFrame:
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
            
            for series_item in result.get('series', []):
                locality = series_item.get('localidade', {}).get('nome', '')
                municipality_id = series_item.get('localidade', {}).get('id', '')
                
                for year, value in series_item.get('serie', {}).items():
                    extraction_data.append({
                        'id_variavel': var_id,
                        'nome_variavel': var_name,
                        'unidade_medida': var_unit,
                        'id_produto': produto_info['codigo'],
                        'produto': produto_info['valor'],
                        'nome_municipio': locality,
                        'id_municipio': municipality_id,
                        'ano': year,
                        'valor': value
                    })
    
    return pd.DataFrame(extraction_data)

def rename_columns(dataframe):
    renamed_dataframe = dataframe.rename(columns={
            "Área plantada": "area_plantada",
            "Área colhida": "area_colhida",
            "Quantidade produzida": "quantidade_produzida",
            "Rendimento médio da produção": "rendimento_medio_producao",
            "Valor da produção": "valor_producao"
        })
    return renamed_dataframe

def treat_columns(dataframe):
    dataframe = dataframe[["ano", "sigla_uf", "id_municipio", "produto",
                           "area_plantada", "area_colhida",
                           "quantidade_produzida", "rendimento_medio_producao",
                           "valor_producao"]]
    COLUNAS_PARA_TRATAR = ["ano", "area_plantada", "area_colhida",
                           "quantidade_produzida", "rendimento_medio_producao",
                           "valor_producao"]

    for coluna in COLUNAS_PARA_TRATAR:
        dataframe[coluna] = dataframe[coluna].apply(lambda x: np.nan if x in ("-", "..", "...", "X") else x)
        dataframe[coluna] = dataframe[coluna].astype("Int64")
    return dataframe

