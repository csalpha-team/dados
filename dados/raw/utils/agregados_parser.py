"""Parser comum para respostas da API de Agregados do IBGE (v3).

Cobre o formato usado pelas *séries novas* (ano de referência 2024 em diante) da
PAC, da PAS e da PIA-Empresa, em que a geografia volta em ``localidades``
(níveis N1/N2/N3) e sobra uma única classificação por resultado — diferente das
séries antigas da PAC/PAS, que carregavam a geografia como classificação.

Uma linha por ``(variável, categoria, localidade, ano)``.
"""

from __future__ import annotations

from typing import Any, Dict, List

import pandas as pd


COLUNAS = [
    "id_variavel",
    "nome_variavel",
    "unidade_medida",
    "id_classificacao",
    "nome_classificacao",
    "id_categoria",
    "nome_categoria",
    "id_localidade",
    "nome_localidade",
    "id_nivel",
    "nome_nivel",
    "ano",
    "valor",
]


def parse_agregado_json_to_table(json_data: List[Dict[str, Any]]) -> pd.DataFrame:
    """Converte o JSON de um agregado (uma classificação) em DataFrame longo."""

    registros: List[Dict[str, Any]] = []

    for variavel in json_data:
        id_variavel = variavel["id"]
        nome_variavel = variavel["variavel"]
        unidade_medida = variavel["unidade"]

        for resultado in variavel["resultados"]:
            classificacoes = resultado.get("classificacoes") or [{}]
            classificacao = classificacoes[0]
            id_classificacao = classificacao.get("id")
            nome_classificacao = classificacao.get("nome")
            categorias = list((classificacao.get("categoria") or {}).items())
            if not categorias:
                categorias = [(None, None)]
            id_categoria, nome_categoria = categorias[0]

            for serie in resultado["series"]:
                localidade = serie["localidade"]
                for ano, valor in serie["serie"].items():
                    registros.append(
                        {
                            "id_variavel": id_variavel,
                            "nome_variavel": nome_variavel,
                            "unidade_medida": unidade_medida,
                            "id_classificacao": id_classificacao,
                            "nome_classificacao": nome_classificacao,
                            "id_categoria": id_categoria,
                            "nome_categoria": nome_categoria,
                            "id_localidade": localidade["id"],
                            "nome_localidade": localidade["nome"],
                            "id_nivel": localidade["nivel"]["id"],
                            "nome_nivel": localidade["nivel"]["nome"],
                            "ano": int(ano),
                            "valor": valor,
                        }
                    )

    return pd.DataFrame(registros, columns=COLUNAS)
