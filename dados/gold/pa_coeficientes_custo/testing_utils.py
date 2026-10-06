from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from dados.gold.pa_coeficientes_custo.utils import (
    agregar_valores_regionais,
    calcular_valores_municipais,
    carregar_parametros_custo,
    clean_region_name,
    expandir_coeficientes,
)


DEFAULT_CONFIG_PATH = Path(__file__).with_name("parametros_coeficientes_custo.json")


def _config_mappings(
    config_path: Path,
) -> tuple[dict[str, list[str]], list[str], str]:
    with config_path.open("r", encoding="utf-8") as file:
        config = json.load(file)

    parameter_groups = config.get(
        "parameter_groups", config.get("grupos_parametros", {})
    )
    total_expense_label = config.get(
        "total_expense_label", config.get("rotulo_despesa_total", "Total")
    )

    expense_to_keys: dict[str, list[str]] = {}
    coeff_keys: list[str] = []
    for mappings in parameter_groups.values():
        for mapping in mappings:
            keys = [str(value).strip() for value in mapping.get("coeff_keys", [])]
            coeff_keys.extend(keys)
            for expense_type in mapping.get("expense_types", []):
                expense_to_keys.setdefault(str(expense_type).strip(), []).extend(keys)

    return expense_to_keys, sorted(dict.fromkeys(coeff_keys)), total_expense_label


def construir_cenario_valores_custo(
    config_path: Path = DEFAULT_CONFIG_PATH,
) -> tuple[pd.DataFrame, pd.Series, list[str]]:
    expense_to_keys, expected_keys, total_expense_label = _config_mappings(config_path)

    rows = [
        {
            "ano": 2023,
            "id_municipio": 1501402,
            "nome": "Belem",
            "nome_regiao_integracao": "Rio Capim",
            "sigla_uf": "PA",
            "tipo_despesa": total_expense_label,
            "quantidade_estabelecimentos_fizeram_despesa": 1,
            "valor_despesa": 999999.0,
        }
    ]

    expected_values = pd.Series(0.0, index=expected_keys, dtype="float64")
    for idx, (expense_type, keys) in enumerate(expense_to_keys.items(), start=1):
        value = float(idx * 10)
        rows.append(
            {
                "ano": 2023,
                "id_municipio": 1501402,
                "nome": "Belem",
                "nome_regiao_integracao": "Rio Capim",
                "sigla_uf": "PA",
                "tipo_despesa": expense_type,
                "quantidade_estabelecimentos_fizeram_despesa": 1,
                "valor_despesa": value,
            }
        )
        for key in keys:
            expected_values.loc[key] += value

    data = pd.DataFrame(rows)
    data["nome_regiao_integracao"] = data["nome_regiao_integracao"].apply(
        clean_region_name
    )
    return data, expected_values.sort_index(), expected_keys


def validar_cenario_valores_custo(
    config_path: Path = DEFAULT_CONFIG_PATH,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.Series, list[str]]:
    cost_df, expected_values, expected_keys = construir_cenario_valores_custo(
        config_path=config_path
    )
    value_to_key_map, rename_map, total_expense_label = carregar_parametros_custo(
        config_path
    )

    grouped = calcular_valores_municipais(
        cost_df,
        total_expense_label=total_expense_label,
    )
    grouped["nome_regiao_integracao"] = grouped["nome_regiao_integracao"].replace(
        rename_map
    )

    expanded = expandir_coeficientes(grouped, value_to_key_map)
    values = agregar_valores_regionais(expanded)
    return grouped, values, expected_values, expected_keys
