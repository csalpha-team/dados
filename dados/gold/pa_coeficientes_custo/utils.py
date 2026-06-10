import json
import unicodedata
from pathlib import Path
import pandas as pd


MUNICIPALITY_GROUP_COLUMNS = [
    "ano",
    "id_municipio",
    "nome",
    "nome_regiao_integracao",
    "sigla_uf",
    "tipo_despesa",
]

REGIONAL_GROUP_COLUMNS = ["ano", "nome_regiao_integracao", "tipo_coeff"]
VALUE_COLUMNS = ["ano", "nome_regiao_integracao", "tipo_coeff", "valor"]
COEFFICIENT_COLUMNS = ["ano", "nome_regiao_integracao", "tipo_coeff", "coeff"]
FINAL_COLUMNS = VALUE_COLUMNS


def clean_region_name(name: str) -> str:
    """Remove acentos, remove espaços e converte para PascalCase."""
    if not isinstance(name, str):
        return name

    name = name.title().replace(" ", "")
    normalized_name = unicodedata.normalize("NFKD", name)
    return "".join(char for char in normalized_name if not unicodedata.combining(char))


def carregar_parametros_custo(
    config_path: Path,
) -> tuple[list[tuple[str, tuple[str, ...]]], dict[str, str], str]:
    with config_path.open("r", encoding="utf-8") as file:
        config = json.load(file)

    parameter_groups = config.get(
        "parameter_groups", config.get("grupos_parametros", {})
    )
    region_rename_map = config.get(
        "region_rename_map", config.get("mapa_renomeacao_regiao", {})
    )
    total_expense_label = config.get(
        "total_expense_label", config.get("rotulo_despesa_total", "Total")
    )

    if not isinstance(parameter_groups, dict):
        raise ValueError(
            "parameter_groups deve ser um dicionário no arquivo de configuração."
        )

    if not isinstance(region_rename_map, dict):
        raise ValueError(
            "region_rename_map deve ser um dicionário no arquivo de configuração."
        )

    if not isinstance(total_expense_label, str):
        raise ValueError(
            "total_expense_label deve ser uma string no arquivo de configuração."
        )

    value_to_key_map = []
    for mappings in parameter_groups.values():
        for mapping in mappings:
            coeff_keys = tuple(mapping.get("coeff_keys", []))
            expense_types = mapping.get("expense_types", [])

            for expense_type in expense_types:
                value_to_key_map.append((expense_type, coeff_keys))

    return value_to_key_map, region_rename_map, total_expense_label


def calcular_valores_municipais(
    data: pd.DataFrame,
    total_expense_label: str = "Total",
) -> pd.DataFrame:
    grouped = data.groupby(MUNICIPALITY_GROUP_COLUMNS, as_index=False).agg(
        {
            "quantidade_estabelecimentos_fizeram_despesa": "sum",
            "valor_despesa": "sum",
        }
    )

    grouped["valor_despesa"] = pd.to_numeric(grouped["valor_despesa"], errors="coerce")
    grouped = grouped[grouped["tipo_despesa"] != total_expense_label].copy()
    grouped = grouped.rename(columns={"valor_despesa": "valor"})
    return grouped


def calcular_coeficientes_municipais(
    data: pd.DataFrame,
    total_expense_label: str = "Total",
) -> pd.DataFrame:
    grouped = data.groupby(MUNICIPALITY_GROUP_COLUMNS, as_index=False).agg(
        {
            "quantidade_estabelecimentos_fizeram_despesa": "sum",
            "valor_despesa": "sum",
        }
    )

    total_df = grouped[grouped["tipo_despesa"] == total_expense_label][
        [
            "ano",
            "id_municipio",
            "nome",
            "nome_regiao_integracao",
            "sigla_uf",
            "valor_despesa",
        ]
    ].rename(columns={"valor_despesa": "total_despesa"})

    grouped = grouped.merge(
        total_df,
        on=["ano", "id_municipio", "nome", "nome_regiao_integracao", "sigla_uf"],
        how="left",
    )

    grouped["valor_despesa"] = pd.to_numeric(grouped["valor_despesa"], errors="coerce")
    grouped["total_despesa"] = pd.to_numeric(grouped["total_despesa"], errors="coerce")

    valid_denominator = grouped["total_despesa"].notna() & (
        grouped["total_despesa"] != 0
    )
    grouped["coeff"] = pd.NA
    grouped.loc[valid_denominator, "coeff"] = (
        grouped.loc[valid_denominator, "valor_despesa"]
        / grouped.loc[valid_denominator, "total_despesa"]
    )
    return grouped


def expandir_coeficientes(
    grouped_coefficients: pd.DataFrame,
    value_to_key_map: list[tuple[str, tuple[str, ...]]],
) -> pd.DataFrame:
    rows = []
    for expense_type, coeff_keys in value_to_key_map:
        temp = grouped_coefficients[
            grouped_coefficients["tipo_despesa"] == expense_type
        ].copy()
        for coeff_key in coeff_keys:
            temp_copy = temp.copy()
            temp_copy["tipo_coeff"] = coeff_key
            rows.append(temp_copy)

    if not rows:
        return pd.DataFrame(columns=FINAL_COLUMNS)

    return pd.concat(rows, ignore_index=True)


def agregar_valores_regionais(value_df: pd.DataFrame) -> pd.DataFrame:
    if value_df.empty:
        return pd.DataFrame(columns=VALUE_COLUMNS)

    regional_values = value_df.groupby(
        REGIONAL_GROUP_COLUMNS, as_index=False
    ).agg({"valor": "sum"})

    regional_values = regional_values.sort_values(
        ["ano", "nome_regiao_integracao", "tipo_coeff"]
    ).reset_index(drop=True)

    return regional_values[VALUE_COLUMNS]


def agregar_coeficientes_regional_mais_recente(coeff_df: pd.DataFrame) -> pd.DataFrame:
    if coeff_df.empty:
        return pd.DataFrame(columns=COEFFICIENT_COLUMNS)

    regional_coefficients = coeff_df.groupby(
        REGIONAL_GROUP_COLUMNS, as_index=False
    ).agg({"coeff": "mean"})

    ordered_coefficients = regional_coefficients.sort_values(by="ano", ascending=False)
    latest_coefficients = ordered_coefficients.drop_duplicates(
        subset=["nome_regiao_integracao", "tipo_coeff"], keep="first"
    ).reset_index(drop=True)

    return latest_coefficients[COEFFICIENT_COLUMNS]
