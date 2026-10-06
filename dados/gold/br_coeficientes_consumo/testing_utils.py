from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

from dados.gold.br_coeficientes_consumo.utils import (
    REAIS_TO_THOUSAND_REAIS,
    construir_valores_consumo,
)


DEFAULT_EQUIVALENCE_PATH = Path(__file__).with_name("equivalencia_despesas.json")
DEFAULT_PARAMETERS = {
    "coluna_chave_mip": "TipoDespesaDestinoProvável",
    "coluna_tipo_despesa_mip": "TiposDeDespesa",
    "variavel_alvo": "Despesa monetária e não monetária média mensal familiar",
    "ano_alvo": 2018,
    "rotulo_urbano": "Urbana",
    "rotulo_rural": "Rural",
    "padrao_estado": "Estad|Estadual",
}


def construir_cenario_valores_consumo(
    equivalence_path: Path = DEFAULT_EQUIVALENCE_PATH,
    parametros: dict | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, dict, pd.Series, list[str]]:
    params = dict(DEFAULT_PARAMETERS if parametros is None else parametros)

    mapping = pd.read_json(equivalence_path)
    required_columns = [params["coluna_chave_mip"], params["coluna_tipo_despesa_mip"]]
    mapping = mapping[required_columns].dropna(subset=required_columns).copy()
    mapping = mapping.drop_duplicates(subset=[params["coluna_chave_mip"]], keep="first")
    mapping[params["coluna_chave_mip"]] = (
        mapping[params["coluna_chave_mip"]].astype(str).str.strip()
    )
    mapping[params["coluna_tipo_despesa_mip"]] = (
        mapping[params["coluna_tipo_despesa_mip"]].astype(str).str.strip()
    )

    rows = []
    expected_values = pd.Series(
        0.0,
        index=sorted(mapping[params["coluna_chave_mip"]].unique().tolist()),
        dtype="float64",
    )
    for idx, (_, row) in enumerate(mapping.iterrows(), start=1):
        coeff_key = row[params["coluna_chave_mip"]]
        housing_label = (
            params["rotulo_urbano"]
            if re.search(params["padrao_estado"], coeff_key)
            else params["rotulo_rural"]
        )
        value = float(idx * 25)
        expected_values.loc[coeff_key] = value / REAIS_TO_THOUSAND_REAIS

        rows.append(
            {
                "ano": params["ano_alvo"],
                "variavel": params["variavel_alvo"],
                "situacao_domicilio": housing_label,
                "tipo_despesa": row[params["coluna_tipo_despesa_mip"]],
                "valor": value,
                "unidade_medida": "Reais",
            }
        )
        rows.append(
            {
                "ano": params["ano_alvo"],
                "variavel": "Distribuição da despesa monetária e não monetária média mensal familiar",
                "situacao_domicilio": housing_label,
                "tipo_despesa": row[params["coluna_tipo_despesa_mip"]],
                "valor": 100.0,
                "unidade_medida": "%",
            }
        )

    pof_df = pd.DataFrame(rows)
    expected_keys = sorted(expected_values.index.tolist())
    return pof_df, mapping, params, expected_values.sort_index(), expected_keys


def validar_cenario_valores_consumo(
    equivalence_path: Path = DEFAULT_EQUIVALENCE_PATH,
    parametros: dict | None = None,
) -> tuple[pd.DataFrame, pd.Series, list[str]]:
    pof_df, mapping, params, expected_values, expected_keys = (
        construir_cenario_valores_consumo(
            equivalence_path=equivalence_path,
            parametros=parametros,
        )
    )
    values = construir_valores_consumo(pof_df, mapping, params)
    return values, expected_values, expected_keys
