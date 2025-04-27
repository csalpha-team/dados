import json
from collections import OrderedDict
import pandas as pd
from pathlib import Path


def currency_fix(row):
    if row['unidade'] == 'Mil Cruzados':
        return row['valor'] / (1000**2 * 2750)
    elif row['unidade'] == 'Mil Cruzados Novos':
        return row['valor'] / (1000 * 2750)
    elif row['unidade'] == 'Mil Cruzeiros':
        return row['valor'] / (1000 * 2750)
    elif row['unidade'] == 'Mil Cruzeiros Reais':
        return row['valor'] / 2750
    elif row['unidade'] == 'Mil Reais':
        return row['valor']
   
def transform_df(df):
    df_quantidade, df_valor = split_df(df, "variavel", ["144", "145"])
    del(df)
    
    df_quantidade.rename(columns={"valor": "quantidade"}, inplace=True)
    df_quantidade["quantidade"] = df_quantidade["quantidade"].apply(lambda x: x if x not in ("..", "...", "-") else None)
    df_quantidade["quantidade"] = df_quantidade["quantidade"].astype("Int64")
    
    df_valor["valor"] = df_valor["valor"].apply(lambda x: x if x not in ("..", "...", "-") else None)
    df_valor["valor"] = df_valor["valor"].astype("Float64")
    df_valor["valor"] = df_valor.apply(currency_fix, axis=1)
    df_valor["valor"] = df_valor["valor"].astype("Float64")
    df_valor.drop(columns=["unidade"], inplace=True)
    
    temp_df = df_quantidade.merge(df_valor, left_on=["id_municipio", "tipo_produto", "produto"], right_on=["id_municipio", "tipo_produto", "produto"])
    del(df_quantidade)
    del(df_valor)
    
    temp_df = temp_df[["id_municipio", "tipo_produto", "produto", "unidade", "quantidade", "valor"]]
    return temp_df