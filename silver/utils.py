import pandas as pd
from typing import List
from pandas import DataFrame

def fix_ibge_digits(columns: List[str], df = pd.DataFrame) -> pd.DataFrame:
    
    
    for coluna in columns:
        print(f"Corrigindo coluna {coluna}")
        df[coluna] = df[coluna].apply(lambda x: 0 if x in ("..", "...", "-", "X") else x)
        df[coluna] = pd.to_numeric(df[coluna], errors='coerce').fillna(0).astype("Int64")
        
    return df
        

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