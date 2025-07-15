
from typing import List, Dict, Any
import pandas as pd


def parse_json_pof(data: List[Dict[str, Any]]):
    
    registros = []
    
    for item in data:
        variavel = item.get("variavel")
        unidade = item.get("unidade")
        
        for resultado in item['resultados']:
            classificacoes = resultado.get('classificacoes', [])
            tipo_despesa = None
            situacao_domicilio = None
            
            for classificacao in classificacoes:
                nome = classificacao.get("nome", "")
                categorias = classificacao.get("categoria", {})
                if nome == "Tipos de despesa":
                    tipo_despesa = list(categorias.values())[0]
                elif nome == "Situação do domicílio":
                    situacao_domicilio = list(categorias.values())[0]

            for serie in resultado['series']:
                localidade = serie['localidade']['nome']
                for ano, valor in serie['serie'].items():
                    registros.append({
                        'ano': int(ano),
                        'valor': float(valor),
                        'tipo_despesa': tipo_despesa,
                        'situacao_domicilio': situacao_domicilio,
                        'localidade': localidade,
                        'variavel': variavel,
                        'unidade': unidade
                    })
    
    return pd.DataFrame(registros)
