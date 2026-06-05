from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from dados.gold.pa_coeficientes_custo.utils import (
    agregar_coeficientes_regional_mais_recente,
    calcular_coeficientes_municipais,
    carregar_parametros_custo,
    clean_region_name,
    expandir_coeficientes,
)


DEFAULT_CONFIG_PATH = Path(__file__).with_name("parametros_coeficientes_custo.json")


def construir_cenario_coeficientes_unitarios(
    config_path: Path = DEFAULT_CONFIG_PATH,
) -> tuple[pd.DataFrame, list[str]]:
    with config_path.open("r", encoding="utf-8") as file:
        config = json.load(file)

    parameter_groups = config.get(
        "parameter_groups", config.get("grupos_parametros", {})
    )
    total_expense_label = config.get(
        "total_expense_label", config.get("rotulo_despesa_total", "Total")
    )

    expense_types: list[str] = []
    coeff_keys: list[str] = []
    for mappings in parameter_groups.values():
        for mapping in mappings:
            expense_types.extend(
                str(value).strip() for value in mapping.get("expense_types", [])
            )
            coeff_keys.extend(
                str(value).strip() for value in mapping.get("coeff_keys", [])
            )

    rows = [
        {
            "ano": 2023,
            "id_municipio": 1501402,
            "nome": "Belem",
            "nome_regiao_integracao": "Rio Capim",
            "sigla_uf": "PA",
            "tipo_despesa": total_expense_label,
            "quantidade_estabelecimentos_fizeram_despesa": 1,
            "valor_despesa": 100.0,
        }
    ]

    for expense_type in dict.fromkeys(expense_types):
        rows.append(
            {
                "ano": 2023,
                "id_municipio": 1501402,
                "nome": "Belem",
                "nome_regiao_integracao": "Rio Capim",
                "sigla_uf": "PA",
                "tipo_despesa": expense_type,
                "quantidade_estabelecimentos_fizeram_despesa": 1,
                "valor_despesa": 100.0,
            }
        )

    data = pd.DataFrame(rows)
    data["nome_regiao_integracao"] = data["nome_regiao_integracao"].apply(
        clean_region_name
    )
    expected_keys = sorted(dict.fromkeys(coeff_keys))
    return data, expected_keys


def validar_cenario_coeficientes_unitarios(
    config_path: Path = DEFAULT_CONFIG_PATH,
) -> tuple[pd.DataFrame, list[str]]:
    cost_df, expected_keys = construir_cenario_coeficientes_unitarios(
        config_path=config_path
    )
    value_to_key_map, rename_map, total_expense_label = carregar_parametros_custo(
        config_path
    )

    grouped = calcular_coeficientes_municipais(
        cost_df,
        total_expense_label=total_expense_label,
    )
    grouped["nome_regiao_integracao"] = grouped["nome_regiao_integracao"].replace(
        rename_map
    )

    expanded = expandir_coeficientes(grouped, value_to_key_map)
    coefficients = agregar_coeficientes_regional_mais_recente(expanded)
    return coefficients, expected_keys


def construir_cenario_coeficientes_normalizados(
    config_path: Path = DEFAULT_CONFIG_PATH,
) -> tuple[pd.DataFrame, list[str], list[str], str]:
    with config_path.open("r", encoding="utf-8") as file:
        config = json.load(file)

    parameter_groups = config.get(
        "parameter_groups", config.get("grupos_parametros", {})
    )
    total_expense_label = config.get(
        "total_expense_label",
        config.get("rotulo_despesa_total", "Total"),
    )

    expense_types: list[str] = []
    coeff_keys: list[str] = []
    for mappings in parameter_groups.values():
        for mapping in mappings:
            expense_types.extend(
                str(value).strip() for value in mapping.get("expense_types", [])
            )
            coeff_keys.extend(
                str(value).strip() for value in mapping.get("coeff_keys", [])
            )

    unique_expense_types = list(dict.fromkeys(expense_types))
    base_expense_value = 100.0
    total_expense_value = base_expense_value * len(unique_expense_types)

    rows = [
        {
            "ano": 2023,
            "id_municipio": 1501402,
            "nome": "Belem",
            "nome_regiao_integracao": "Rio Capim",
            "sigla_uf": "PA",
            "tipo_despesa": total_expense_label,
            "quantidade_estabelecimentos_fizeram_despesa": 1,
            "valor_despesa": total_expense_value,
        }
    ]

    for expense_type in unique_expense_types:
        rows.append(
            {
                "ano": 2023,
                "id_municipio": 1501402,
                "nome": "Belem",
                "nome_regiao_integracao": "Rio Capim",
                "sigla_uf": "PA",
                "tipo_despesa": expense_type,
                "quantidade_estabelecimentos_fizeram_despesa": 1,
                "valor_despesa": base_expense_value,
            }
        )

    data = pd.DataFrame(rows)
    data["nome_regiao_integracao"] = data["nome_regiao_integracao"].apply(
        clean_region_name
    )

    expected_expense_types = sorted(unique_expense_types)
    expected_coeff_keys = sorted(dict.fromkeys(coeff_keys))
    return data, expected_expense_types, expected_coeff_keys, total_expense_label


def validar_cenario_soma_coeficientes_unitaria(
    config_path: Path = DEFAULT_CONFIG_PATH,
) -> tuple[pd.DataFrame, pd.DataFrame, list[str], list[str]]:
    (
        cost_df,
        expected_expense_types,
        expected_coeff_keys,
        total_expense_label,
    ) = construir_cenario_coeficientes_normalizados(config_path=config_path)
    value_to_key_map, rename_map, _ = carregar_parametros_custo(config_path)

    grouped = calcular_coeficientes_municipais(
        cost_df,
        total_expense_label=total_expense_label,
    )
    grouped["nome_regiao_integracao"] = grouped["nome_regiao_integracao"].replace(
        rename_map
    )

    expanded = expandir_coeficientes(grouped, value_to_key_map)
    coefficients = agregar_coeficientes_regional_mais_recente(expanded)
    return grouped, coefficients, expected_expense_types, expected_coeff_keys
