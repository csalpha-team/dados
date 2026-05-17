from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

from dados.gold.br_coeficientes_consumo.utils import construir_coeficientes_consumo


DEFAULT_EQUIVALENCE_PATH = Path(__file__).with_name("equivalencia_despesas.json")
DEFAULT_PARAMETERS = {
    "coluna_chave_mip": "TipoDespesaDestinoProvável",
    "coluna_tipo_despesa_mip": "TiposDeDespesa",
    "variavel_alvo": "Distribuição da despesa monetária e não monetária média mensal familiar",
    "ano_alvo": 2018,
    "rotulo_urbano": "Urbana",
    "rotulo_rural": "Rural",
    "padrao_estado": "Estad|Estadual",
}


def construir_cenario_coeficientes_unitarios(
    equivalence_path: Path = DEFAULT_EQUIVALENCE_PATH,
    parametros: dict | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, dict, list[str]]:
    params = dict(DEFAULT_PARAMETERS if parametros is None else parametros)

    mapping = pd.read_json(equivalence_path)
    required_columns = [params["coluna_chave_mip"], params["coluna_tipo_despesa_mip"]]
    mapping = mapping[required_columns].dropna(subset=required_columns).copy()
    mapping = mapping.drop_duplicates(subset=[params["coluna_chave_mip"]], keep="first")

    rows = []
    for _, row in mapping.iterrows():
        coeff_key = str(row[params["coluna_chave_mip"]]).strip()
        expense_type = str(row[params["coluna_tipo_despesa_mip"]]).strip()
        housing_label = (
            params["rotulo_urbano"]
            if re.search(params["padrao_estado"], coeff_key)
            else params["rotulo_rural"]
        )

        rows.append(
            {
                "ano": params["ano_alvo"],
                "variavel": params["variavel_alvo"],
                "situacao_domicilio": housing_label,
                "tipo_despesa": expense_type,
                "valor": 100.0,
                "unidade_medida": "%",
            }
        )

    pof_df = pd.DataFrame(rows)
    expected_keys = sorted(
        mapping[params["coluna_chave_mip"]].astype(str).str.strip().unique()
    )
    return pof_df, mapping, params, expected_keys


def validar_cenario_coeficientes_unitarios(
    equivalence_path: Path = DEFAULT_EQUIVALENCE_PATH,
    parametros: dict | None = None,
) -> tuple[pd.DataFrame, list[str]]:
    pof_df, mapping, params, expected_keys = construir_cenario_coeficientes_unitarios(
        equivalence_path=equivalence_path,
        parametros=parametros,
    )
    coefficients = construir_coeficientes_consumo(pof_df, mapping, params)
    return coefficients, expected_keys


def construir_cenario_coeficientes_normalizados(
    equivalence_path: Path = DEFAULT_EQUIVALENCE_PATH,
    parametros: dict | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, dict, list[str]]:
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

    normalized_values = pd.Series(1.0, index=mapping.index, dtype="float64")
    normalized_values = normalized_values / normalized_values.sum() * 100.0
    normalized_values.iloc[-1] = 100.0 - normalized_values.iloc[:-1].sum()

    rows = []
    for (_, row), value in zip(mapping.iterrows(), normalized_values.tolist()):
        coeff_key = row[params["coluna_chave_mip"]]
        housing_label = (
            params["rotulo_urbano"]
            if re.search(params["padrao_estado"], coeff_key)
            else params["rotulo_rural"]
        )

        rows.append(
            {
                "ano": params["ano_alvo"],
                "variavel": params["variavel_alvo"],
                "situacao_domicilio": housing_label,
                "tipo_despesa": row[params["coluna_tipo_despesa_mip"]],
                "valor": float(value),
                "unidade_medida": "%",
            }
        )

    pof_df = pd.DataFrame(rows)
    expected_keys = sorted(mapping[params["coluna_chave_mip"]].unique().tolist())
    return pof_df, mapping, params, expected_keys


def validar_cenario_soma_coeficientes_unitaria(
    equivalence_path: Path = DEFAULT_EQUIVALENCE_PATH,
    parametros: dict | None = None,
) -> tuple[pd.DataFrame, list[str]]:
    pof_df, mapping, params, expected_keys = (
        construir_cenario_coeficientes_normalizados(
            equivalence_path=equivalence_path,
            parametros=parametros,
        )
    )
    coefficients = construir_coeficientes_consumo(pof_df, mapping, params)
    return coefficients, expected_keys
