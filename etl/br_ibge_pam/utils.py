### Código adaptado de https://github.com/basedosdados/queries-basedosdados/tree/main/models/br_ibge_pam


import numpy as np
import pandas as pd


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
    
    # # Verifica se os dados estão no formato esperado
    # if not isinstance(data, list):
    #     # Se data for um dicionário com uma chave que contém a lista principal
    #     if isinstance(data, dict) and any(isinstance(data.get(key), list) for key in data):
    #         for key in data:
    #             if isinstance(data[key], list):
    #                 data = data[key]
    #                 break
    #     else:
    #         raise ValueError("Formato de dados inválido. Esperava uma lista ou dicionário com lista.")
    
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

def products_weight_ratio_fix(row):
    """
    2 - A partir do ano de 2001 as quantidades produzidas dos produtos abacate, banana,
    caqui, figo, goiaba, laranja, limão, maçã, mamão, manga, maracujá, marmelo, pera,
    pêssego e tangerina passam a ser expressas em toneladas. Nos anos anteriores eram
    expressas em mil frutos, com exceção da banana, que era expressa em mil cachos. O
    rendimento médio passa a ser expresso em Kg/ha. Nos anos anteriores era expresso
    em frutos/ha, com exceção da banana, que era expressa em cachos/ha.
    3 - Veja em o documento
    https://sidra.ibge.gov.br/content/documentos/pam/AlteracoesUnidadesMedidaFrutas.pdf
    com as alterações de unidades de medida das frutíferas ocorridas em 2001 e a tabela
    de conversão fruto x quilograma.
    """

    DICIONARIO_DE_PROPORCOES = {
        "Abacate": 0.38,
        "Banana (cacho)": 10.20,
        "Caqui": 0.18,
        "Figo": 0.09,
        "Goiaba": 0.16,
        "Larajna": 0.16,
        "Limão": 0.10,
        "Maçã": 0.15,
        "Mamão": 0.80,
        "Manga": 0.31,
        "Maracujá": 0.15,
        "Marmelo": 0.19,
        "Pera": 0.17,
        "Pêra": 0.17, # Para garantir, pois nos dados parece que só há Pera, sem acento
        "Pêssego": 0.13,
        "Tangerina": 0.15,
        "Melancia": 6.08,
        "Melão": 1.39
    }

    if row["ano"] >= 2001:
        return row

    if (pd.isna(row["quantidade_produzida"]) or pd.isna(row["area_colhida"])
        or row["quantidade_produzida"] == 0 or row["area_colhida"] == 0):
        return row

    if row["produto"] not in DICIONARIO_DE_PROPORCOES.keys():
        return row

    quantidade_produzida = row["quantidade_produzida"] * DICIONARIO_DE_PROPORCOES[row["produto"]]

    rendimento_medio_producao = quantidade_produzida / row["area_colhida"] * 1000 # kg / ha

    row["quantidade_produzida"] = quantidade_produzida
    row["rendimento_medio_producao"] = rendimento_medio_producao

    return row

def currency_fix(row):
    """
    Valor da produção (Mil Cruzeiros [1974 a 1985, 1990 a 1992], Mil Cruzados [1986 a 1988],
    Mil Cruzados Novos [1989], Mil Cruzeiros Reais [1993], Mil Reais [1994 a 2022])
    Verificado em http://www.igf.com.br/calculadoras/conversor/conversor.htm
    """

    if 1974 <= row["ano"] <= 1985:
        return row["valor_producao"] / (1000**4 * 2.75)
    elif 1986 <= row["ano"] <= 1988:
        return row["valor_producao"] / (1000**3 * 2.75)
    elif row["ano"] == 1989:
        return row["valor_producao"] / (1000**2 * 2.75)
    elif 1990 <= row["ano"] <= 1992:
        return row["valor_producao"] / (1000**2 * 2.75)
    elif row["ano"] == 1993:
        return row["valor_producao"] / (1000 * 2.75)
    else:
        return row["valor_producao"]



