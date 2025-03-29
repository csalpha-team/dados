
import json
import pandas as pd




def parse_agrocenso_json(data: json, tipo_classificacao: str) -> pd.DataFrame:
    """
    Parses the JSON data from the Agrocenso API and returns a DataFrame.
    
    Parameters:
    data (json): The JSON data from the Agrocenso API
    tipo_classificacao (str): The type of classification to be used for product classification
    
    Returns:
    pd.DataFrame: A DataFrame containing structured extraction data with municipality ID and unit of measurement
    """
    extraction_data = []

    for variable in data:
        var_id = variable['id']
        var_name = variable['variavel']
        var_unit = variable['unidade'] 
        for result in variable['resultados']:
            product_name = None
            product_category = None
            
            #NOTE: ao selecionar tabelas com outras classificações seria preciso modificar aqui
            for classification in result['classificacoes']:
                if classification['nome'] == tipo_classificacao:
                    product_category = list(classification['categoria'].keys())[0]
                    product_name = classification['categoria'][product_category]
                    break
            
            for series_item in result['series']:
                locality = series_item['localidade']['nome']
                municipality_id = series_item['localidade']['id']
                
                for year, value in series_item['serie'].items():
                    extraction_data.append({
                        'id_variavel': var_id,
                        'nome_variavel': var_name,
                        'unidade_medida': var_unit, 
                        'id_produto': product_category,
                        'nome_produto': product_name,
                        'nome_municipio': locality,
                        'id_municipio': municipality_id,
                        'ano': year,
                        'valor': value
                    })
        
    return pd.DataFrame(extraction_data)