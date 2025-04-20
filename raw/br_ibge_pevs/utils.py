import json
from collections import OrderedDict
import pandas as pd
from pathlib import Path

def parse_file(file_path):
    dict_list = []

    with open(file_path) as f:
        json_list = json.load(f)

    for json_obj in json_list:
        for id_json in json_obj:
            temp_od = OrderedDict()
            temp_variavel = id_json['id']
            temp_unidade = id_json['unidade']
            
            for resultado in id_json['resultados']:
                temp_id_categoria = list(resultado['classificacoes'][0]['categoria'].keys())[0]
                
                for serie in resultado['series']:
                    temp_id_municipio = serie['localidade']['id']
                    temp_valor = list(serie['serie'].values())[0]
                                
                    temp_od['id_municipio'] = temp_id_municipio    
                    temp_od['tipo_produto'] = df_metadados_enriquecidos.loc[df_metadados_enriquecidos["id"] == temp_id_categoria, "tipo_produto"].values[0]
                    temp_od['produto'] = df_metadados_enriquecidos.loc[df_metadados_enriquecidos["id"] == temp_id_categoria, "produto"].values[0]
                    temp_od['variavel'] = temp_variavel
                    temp_od['unidade'] = temp_unidade
                    temp_od['valor'] = temp_valor
                    
                    dict_list.append(dict(temp_od))
                    temp_od.clear()
    return dict_list

def split_df(df, column, filters):
    df_list = []
    for filter in filters:
        temp_df = df[df[column] == filter].copy()
        temp_df.drop(columns=[column], inplace=True)
        df_list.append(temp_df)
    return df_list
    
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