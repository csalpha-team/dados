import json
from pathlib import Path
from typing import Any

import pandas as pd

from dados.gold.br_coeficientes_renda.previsao_renda import ForecastConfig, IncomeForecaster


PIA_REQUIRED_COLUMNS = [
    "ano",
    "divisao_grupo_cnae_2",
    "pessoal_ocupado_31_12",
    "valor_bruto_producao_industrial",
    "valor_salarios_remuneracoes",
]

PAC_REQUIRED_COLUMNS = [
    "ano",
    "divisao_grupo_cnae_2",
    "valor_receita_bruta_revenda",
    "pessoal_ocupado_31_12",
    "margem_comercializacao",
    "valor_gastos_salarios_remuneracoes",
]

PIA_VALUE_COLUMNS = [
    "pessoal_ocupado_31_12",
    "valor_bruto_producao_industrial",
    "valor_salarios_remuneracoes",
]

PAC_VALUE_COLUMNS = [
    "valor_receita_bruta_revenda",
    "pessoal_ocupado_31_12",
    "margem_comercializacao",
    "valor_gastos_salarios_remuneracoes",
]

FINAL_COLUMNS = ["ano", "conta_alfa", "tipo_coeff", "coeff"]


def _construir_anos(config_value: Any) -> list[int]:
    if isinstance(config_value, dict):
        start = int(config_value.get("start", config_value.get("inicio", 1995)))
        end = int(config_value.get("end", config_value.get("fim", 2023)))
        if end < start:
            raise ValueError("target_years.end deve ser maior ou igual a target_years.start")
        return list(range(start, end + 1))

    if isinstance(config_value, list):
        years = sorted({int(year) for year in config_value})
        if not years:
            raise ValueError("target_years nao pode ser vazio")
        return years

    return list(range(1995, 2024))


def carregar_parametros_renda(
    config_path: Path,
) -> tuple[dict[str, dict[str, list[str]]], list[int], dict[str, float], ForecastConfig]:
    with config_path.open("r", encoding="utf-8") as file:
        config = json.load(file)

    sector_mappings = config.get("sector_mappings", config.get("mapa_setores", {}))
    if not isinstance(sector_mappings, dict):
        raise ValueError("sector_mappings deve ser um dicionario no arquivo de configuracao")

    normalized_sector_mappings: dict[str, dict[str, list[str]]] = {}
    for dataset_name, account_map in sector_mappings.items():
        if not isinstance(account_map, dict):
            raise ValueError("Cada item de sector_mappings deve ser um dicionario")

        normalized_sector_mappings[str(dataset_name)] = {}
        for account_name, prefixes in account_map.items():
            if not isinstance(prefixes, list):
                raise ValueError("Cada conta em sector_mappings deve apontar para uma lista")

            normalized_sector_mappings[str(dataset_name)][str(account_name)] = [
                str(prefix).strip() for prefix in prefixes if str(prefix).strip()
            ]

    aa_production_values = config.get(
        "aa_production_values", config.get("valores_producao_aa", {})
    )
    if not isinstance(aa_production_values, dict):
        raise ValueError("aa_production_values deve ser um dicionario")

    normalized_aa_values = {
        "prod_mon_trab": float(aa_production_values.get("prod_mon_trab", 0.0)),
        "salario_medio": float(aa_production_values.get("salario_medio", 0.0)),
    }

    forecast_config_raw = config.get("forecast_config", config.get("config_previsao", {}))
    if not isinstance(forecast_config_raw, dict):
        raise ValueError("forecast_config deve ser um dicionario")

    forecast_config = ForecastConfig(
        method=str(forecast_config_raw.get("method", "linear")),
        min_history=int(forecast_config_raw.get("min_history", 2)),
        clamp_non_negative=bool(forecast_config_raw.get("clamp_non_negative", True)),
        rolling_window=int(forecast_config_raw.get("rolling_window", 3)),
    )

    years = _construir_anos(config.get("target_years", config.get("anos_alvo")))

    return normalized_sector_mappings, years, normalized_aa_values, forecast_config


def validar_colunas_entrada(data: pd.DataFrame, required_columns: list[str], label: str) -> None:
    missing_columns = [column for column in required_columns if column not in data.columns]
    if missing_columns:
        raise ValueError(
            f"Colunas obrigatorias ausentes em {label}: {', '.join(missing_columns)}"
        )


def _forcar_colunas_numericas(data: pd.DataFrame, numeric_columns: list[str]) -> pd.DataFrame:
    cleaned = data.copy()
    cleaned["ano"] = pd.to_numeric(cleaned["ano"], errors="coerce")
    cleaned["divisao_grupo_cnae_2"] = cleaned["divisao_grupo_cnae_2"].astype("string").str.strip()

    for column in numeric_columns:
        cleaned[column] = pd.to_numeric(cleaned[column], errors="coerce")

    cleaned = cleaned.dropna(subset=["ano", "divisao_grupo_cnae_2"]).copy()
    cleaned["ano"] = cleaned["ano"].astype(int)
    return cleaned


def _combinar_contas(codigo: str, mapping: dict[str, list[str]]) -> list[str]:
    matched_accounts = []
    codigo_texto = str(codigo)

    for account_name, prefixes in mapping.items():
        if any(codigo_texto.startswith(prefix) for prefix in prefixes):
            matched_accounts.append(account_name)

    return matched_accounts


def _agrupar_contas_por_ano(
    forecast_df: pd.DataFrame,
    year: int,
    mapping: dict[str, list[str]],
    numeric_columns: list[str],
) -> pd.DataFrame:
    year_data = forecast_df.loc[forecast_df["ano"] == year].copy()
    if year_data.empty:
        return pd.DataFrame(columns=["conta_alfa", *numeric_columns])

    year_data["conta_alfa"] = year_data["divisao_grupo_cnae_2"].apply(
        lambda codigo: _combinar_contas(codigo, mapping)
    )
    year_data = year_data[year_data["conta_alfa"].map(bool)].copy()
    if year_data.empty:
        return pd.DataFrame(columns=["conta_alfa", *numeric_columns])

    exploded = year_data.explode("conta_alfa")
    aggregated = exploded.groupby("conta_alfa", as_index=False)[numeric_columns].sum()
    return aggregated


def _divisao_segura(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    result = pd.Series(0.0, index=numerator.index, dtype=float)
    valid = denominator.notna() & (denominator != 0)
    result.loc[valid] = numerator.loc[valid] / denominator.loc[valid]
    return result


def _calcular_coeficientes_por_ano(data: pd.DataFrame, numerator_column: str, salary_column: str) -> pd.DataFrame:
    if data.empty:
        return pd.DataFrame(columns=["conta_alfa", "prod_mon_trab", "salario_medio"])

    result = data.copy()
    result["prod_mon_trab"] = _divisao_segura(result[numerator_column], result["pessoal_ocupado_31_12"])
    result["salario_medio"] = _divisao_segura(result[salary_column], result["pessoal_ocupado_31_12"])
    return result[["conta_alfa", "prod_mon_trab", "salario_medio"]]


def preparar_dados_coeficientes_renda(
    pia_df: pd.DataFrame,
    pac_df: pd.DataFrame,
    sector_mappings: dict[str, dict[str, list[str]]],
    years: list[int],
    aa_production_values: dict[str, float],
    forecast_config: ForecastConfig,
) -> pd.DataFrame:
    validar_colunas_entrada(pia_df, PIA_REQUIRED_COLUMNS, "PIA")
    validar_colunas_entrada(pac_df, PAC_REQUIRED_COLUMNS, "PAC")

    pia_cleaned = _forcar_colunas_numericas(pia_df, PIA_VALUE_COLUMNS)
    pac_cleaned = _forcar_colunas_numericas(pac_df, PAC_VALUE_COLUMNS)

    pia_forecaster = IncomeForecaster(
        year_col="ano",
        label_cols="divisao_grupo_cnae_2",
        value_cols=PIA_VALUE_COLUMNS,
        config=forecast_config,
    )
    pac_forecaster = IncomeForecaster(
        year_col="ano",
        label_cols="divisao_grupo_cnae_2",
        value_cols=PAC_VALUE_COLUMNS,
        config=forecast_config,
    )

    pia_forecast = pia_forecaster.forecast(
        pia_cleaned,
        forecast_years=years,
        include_history=True,
    )
    pac_forecast = pac_forecaster.forecast(
        pac_cleaned,
        forecast_years=years,
        include_history=True,
    )

    rows = []
    pac_mapping = sector_mappings.get("PAC_COMERCIO", {})
    pia_mapping = sector_mappings.get("PIA_INDUSTRIA", {})

    for year in years:
        pia_aggregated = _agrupar_contas_por_ano(
            pia_forecast,
            year,
            pia_mapping,
            PIA_VALUE_COLUMNS,
        )
        pac_aggregated = _agrupar_contas_por_ano(
            pac_forecast,
            year,
            pac_mapping,
            PAC_VALUE_COLUMNS,
        )

        pia_coefficients = _calcular_coeficientes_por_ano(
            pia_aggregated,
            "valor_bruto_producao_industrial",
            "valor_salarios_remuneracoes",
        )
        pac_coefficients = _calcular_coeficientes_por_ano(
            pac_aggregated,
            "valor_receita_bruta_revenda",
            "valor_gastos_salarios_remuneracoes",
        )

        combined = pd.concat([pia_coefficients, pac_coefficients], ignore_index=True)

        aa_row = pd.DataFrame(
            [
                {
                    "conta_alfa": "AAProdução",
                    "prod_mon_trab": float(aa_production_values["prod_mon_trab"]),
                    "salario_medio": float(aa_production_values["salario_medio"]),
                }
            ]
        )

        combined = pd.concat([combined, aa_row], ignore_index=True)
        combined = combined.sort_values("conta_alfa").reset_index(drop=True)

        melted = combined.melt(
            id_vars=["conta_alfa"],
            value_vars=["prod_mon_trab", "salario_medio"],
            var_name="tipo_coeff",
            value_name="coeff",
        )
        melted["ano"] = year
        rows.append(melted[FINAL_COLUMNS])

    if not rows:
        return pd.DataFrame(columns=FINAL_COLUMNS)

    final = pd.concat(rows, ignore_index=True)
    final["coeff"] = pd.to_numeric(final["coeff"], errors="coerce")
    final = final.sort_values(["ano", "conta_alfa", "tipo_coeff"]).reset_index(drop=True)
    return final[FINAL_COLUMNS]


def construir_tabela_saida_renda(coefficients_df: pd.DataFrame, tipo_coeff: str) -> pd.DataFrame:
    filtered = coefficients_df.loc[coefficients_df["tipo_coeff"] == tipo_coeff].copy()
    if filtered.empty:
        return pd.DataFrame(columns=["ano", "conta_alfa", "coeff"])

    filtered = filtered[["ano", "conta_alfa", "coeff"]].copy()
    filtered["ano"] = pd.to_numeric(filtered["ano"], errors="coerce").astype("Int64")
    filtered["coeff"] = pd.to_numeric(filtered["coeff"], errors="coerce")
    filtered = filtered.sort_values(["ano", "conta_alfa"]).reset_index(drop=True)
    return filtered
