import json
from pathlib import Path
from typing import Any

import pandas as pd

from dados.gold.br_coeficientes_renda.previsao_renda import (
    ForecastConfig,
    IncomeForecaster,
)


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
            raise ValueError(
                "target_years.end deve ser maior ou igual a target_years.start"
            )
        return list(range(start, end + 1))

    if isinstance(config_value, list):
        years = sorted({int(year) for year in config_value})
        if not years:
            raise ValueError("target_years nao pode ser vazio")
        return years

    return list(range(1995, 2024))


def carregar_parametros_renda(
    config_path: Path,
) -> tuple[
    dict[str, dict[str, list[str]]], list[int], dict[str, float], ForecastConfig
]:
    with config_path.open("r", encoding="utf-8") as file:
        config = json.load(file)

    sector_mappings = config.get("sector_mappings", config.get("mapa_setores", {}))
    if not isinstance(sector_mappings, dict):
        raise ValueError(
            "sector_mappings deve ser um dicionario no arquivo de configuracao"
        )

    normalized_sector_mappings: dict[str, dict[str, list[str]]] = {}
    for dataset_name, account_map in sector_mappings.items():
        if not isinstance(account_map, dict):
            raise ValueError("Cada item de sector_mappings deve ser um dicionario")

        normalized_sector_mappings[str(dataset_name)] = {}
        for account_name, prefixes in account_map.items():
            if not isinstance(prefixes, list):
                raise ValueError(
                    "Cada conta em sector_mappings deve apontar para uma lista"
                )

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

    forecast_config_raw = config.get(
        "forecast_config", config.get("config_previsao", {})
    )
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


def validar_colunas_entrada(
    data: pd.DataFrame, required_columns: list[str], label: str
) -> None:
    missing_columns = [
        column for column in required_columns if column not in data.columns
    ]
    if missing_columns:
        raise ValueError(
            f"Colunas obrigatorias ausentes em {label}: {', '.join(missing_columns)}"
        )


def _forcar_colunas_numericas(
    data: pd.DataFrame, numeric_columns: list[str]
) -> pd.DataFrame:
    cleaned = data.copy()
    cleaned["ano"] = pd.to_numeric(cleaned["ano"], errors="coerce")
    cleaned["divisao_grupo_cnae_2"] = (
        cleaned["divisao_grupo_cnae_2"].astype("string").str.strip()
    )

    for column in numeric_columns:
        cleaned[column] = pd.to_numeric(cleaned[column], errors="coerce")

    cleaned = cleaned.dropna(subset=["ano", "divisao_grupo_cnae_2"]).copy()
    cleaned["ano"] = cleaned["ano"].astype(int)
    return cleaned


def _agrupar_variaveis_brutas_para_forecast(
    data: pd.DataFrame,
    numeric_columns: list[str],
) -> pd.DataFrame:
    if data.empty:
        return pd.DataFrame(columns=["ano", "divisao_grupo_cnae_2", *numeric_columns])

    base = data[["ano", "divisao_grupo_cnae_2", *numeric_columns]].copy()
    aggregated = (
        base.groupby(["ano", "divisao_grupo_cnae_2"], as_index=False)[numeric_columns]
        .sum(min_count=1)
        .sort_values(["divisao_grupo_cnae_2", "ano"])
        .reset_index(drop=True)
    )
    return aggregated


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


def _calcular_coeficientes_por_ano(
    data: pd.DataFrame, numerator_column: str, salary_column: str
) -> pd.DataFrame:
    if data.empty:
        return pd.DataFrame(columns=["conta_alfa", "prod_mon_trab", "salario_medio"])

    result = data.copy()
    result["prod_mon_trab"] = _divisao_segura(
        result[numerator_column], result["pessoal_ocupado_31_12"]
    )
    result["salario_medio"] = _divisao_segura(
        result[salary_column], result["pessoal_ocupado_31_12"]
    )
    return result[["conta_alfa", "prod_mon_trab", "salario_medio"]]


def _listar_anos_observados(data: pd.DataFrame) -> list[int]:
    if "ano" not in data.columns:
        return []

    years = (
        pd.to_numeric(data["ano"], errors="coerce")
        .dropna()
        .astype(int)
        .unique()
        .tolist()
    )
    return sorted(int(year) for year in years)


def _derreter_coeficientes_renda(coefficients: pd.DataFrame, year: int) -> pd.DataFrame:
    if coefficients.empty:
        return pd.DataFrame(columns=FINAL_COLUMNS)

    melted = coefficients.melt(
        id_vars=["conta_alfa"],
        value_vars=["prod_mon_trab", "salario_medio"],
        var_name="tipo_coeff",
        value_name="coeff",
    )
    melted["ano"] = int(year)
    return melted[FINAL_COLUMNS]


def _construir_coeficientes_por_anos(
    data: pd.DataFrame,
    years: list[int],
    mapping: dict[str, list[str]],
    numeric_columns: list[str],
    numerator_column: str,
    salary_column: str,
) -> pd.DataFrame:
    rows = []
    for year in years:
        aggregated = _agrupar_contas_por_ano(
            data,
            year,
            mapping,
            numeric_columns,
        )
        coefficients = _calcular_coeficientes_por_ano(
            aggregated,
            numerator_column,
            salary_column,
        )
        if not coefficients.empty:
            rows.append(_derreter_coeficientes_renda(coefficients, year))

    if not rows:
        return pd.DataFrame(columns=FINAL_COLUMNS)

    return pd.concat(rows, ignore_index=True)


def _construir_coeficientes_observados(
    data: pd.DataFrame,
    observed_years: list[int],
    mapping: dict[str, list[str]],
    numeric_columns: list[str],
    numerator_column: str,
    salary_column: str,
) -> pd.DataFrame:
    return _construir_coeficientes_por_anos(
        data,
        observed_years,
        mapping,
        numeric_columns,
        numerator_column,
        salary_column,
    )


def _projetar_variaveis_brutas(
    data: pd.DataFrame,
    *,
    years: list[int],
    value_columns: list[str],
    forecast_config: ForecastConfig,
) -> pd.DataFrame:
    aggregated_data = _agrupar_variaveis_brutas_para_forecast(data, value_columns)
    if aggregated_data.empty:
        return pd.DataFrame(columns=["ano", "divisao_grupo_cnae_2", *value_columns])

    forecaster = IncomeForecaster(
        year_col="ano",
        label_cols="divisao_grupo_cnae_2",
        value_cols=value_columns,
        config=forecast_config,
    )
    return forecaster.forecast(
        aggregated_data,
        forecast_years=years,
        include_history=True,
    )


def _calcular_cagr_serie(observed: pd.Series) -> float:
    positive_observed = observed[observed > 0].dropna()
    if positive_observed.size < 2:
        return 0.0

    first_year = int(positive_observed.index[0])
    last_year = int(positive_observed.index[-1])
    span = last_year - first_year
    if span <= 0:
        return 0.0

    first_value = float(positive_observed.iloc[0])
    last_value = float(positive_observed.iloc[-1])
    if first_value <= 0 or last_value <= 0:
        return 0.0

    return float((last_value / first_value) ** (1.0 / span) - 1.0)


def _completar_coeficientes_finais(
    coefficients_df: pd.DataFrame,
    years: list[int],
    *,
    clamp_non_negative: bool,
) -> pd.DataFrame:
    target_years = sorted({int(year) for year in years})
    if coefficients_df.empty or not target_years:
        return pd.DataFrame(columns=FINAL_COLUMNS)

    completed_rows = []
    grouped = coefficients_df.groupby(["conta_alfa", "tipo_coeff"], dropna=False)

    for (account_name, coeff_type), group in grouped:
        ordered = group.sort_values("ano").copy()
        series = (
            pd.Series(
                pd.to_numeric(ordered["coeff"], errors="coerce").to_numpy(dtype=float),
                index=ordered["ano"].astype(int).tolist(),
                dtype=float,
            )
            .groupby(level=0)
            .last()
            .reindex(target_years)
        )

        observed = series.dropna()
        if observed.empty:
            continue

        first_year = int(observed.index[0])
        last_year = int(observed.index[-1])
        first_value = float(observed.iloc[0])
        last_value = float(observed.iloc[-1])
        cagr = _calcular_cagr_serie(observed)

        for year in series.index[series.index < first_year]:
            if first_value > 0 and cagr > -1.0:
                series.loc[year] = first_value / ((1.0 + cagr) ** (first_year - int(year)))
            else:
                series.loc[year] = first_value

        for year in series.index[series.index > last_year]:
            if last_value > 0 and cagr > -1.0:
                series.loc[year] = last_value * ((1.0 + cagr) ** (int(year) - last_year))
            else:
                series.loc[year] = last_value

        series = series.ffill().bfill()

        if clamp_non_negative:
            series = series.clip(lower=0.0)

        for year, coeff in series.items():
            completed_rows.append(
                {
                    "ano": int(year),
                    "conta_alfa": str(account_name),
                    "tipo_coeff": str(coeff_type),
                    "coeff": float(coeff),
                }
            )

    if not completed_rows:
        return pd.DataFrame(columns=FINAL_COLUMNS)

    completed = pd.DataFrame(completed_rows)
    completed = completed.sort_values(["ano", "conta_alfa", "tipo_coeff"]).reset_index(
        drop=True
    )
    return completed[FINAL_COLUMNS]


def _construir_coeficientes_aa(
    years: list[int],
    aa_production_values: dict[str, float],
) -> pd.DataFrame:
    target_years = sorted({int(year) for year in years})
    rows = []
    for year in target_years:
        rows.extend(
            [
                {
                    "ano": year,
                    "conta_alfa": "AAProdução",
                    "tipo_coeff": "prod_mon_trab",
                    "coeff": float(aa_production_values["prod_mon_trab"]),
                },
                {
                    "ano": year,
                    "conta_alfa": "AAProdução",
                    "tipo_coeff": "salario_medio",
                    "coeff": float(aa_production_values["salario_medio"]),
                },
            ]
        )

    if not rows:
        return pd.DataFrame(columns=FINAL_COLUMNS)

    return pd.DataFrame(rows, columns=FINAL_COLUMNS)


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
    pac_mapping = sector_mappings.get("PAC_COMERCIO", {})
    pia_mapping = sector_mappings.get("PIA_INDUSTRIA", {})
    target_years = sorted({int(year) for year in years})

    pia_observed_years = _listar_anos_observados(pia_cleaned)
    pac_observed_years = _listar_anos_observados(pac_cleaned)

    pia_observed_coefficients = _construir_coeficientes_observados(
        pia_cleaned,
        observed_years=[year for year in pia_observed_years if year in target_years],
        mapping=pia_mapping,
        numeric_columns=PIA_VALUE_COLUMNS,
        numerator_column="valor_bruto_producao_industrial",
        salary_column="valor_salarios_remuneracoes",
    )
    pac_observed_coefficients = _construir_coeficientes_observados(
        pac_cleaned,
        observed_years=[year for year in pac_observed_years if year in target_years],
        mapping=pac_mapping,
        numeric_columns=PAC_VALUE_COLUMNS,
        numerator_column="valor_receita_bruta_revenda",
        salary_column="valor_gastos_salarios_remuneracoes",
    )

    observed_frames = [
        frame
        for frame in [pia_observed_coefficients, pac_observed_coefficients]
        if not frame.empty
    ]
    if observed_frames:
        observed_coefficients = pd.concat(observed_frames, ignore_index=True)
        completed_coefficients = _completar_coeficientes_finais(
            observed_coefficients,
            years=target_years,
            clamp_non_negative=forecast_config.clamp_non_negative,
        )
    else:
        completed_coefficients = pd.DataFrame(columns=FINAL_COLUMNS)
    aa_coefficients = _construir_coeficientes_aa(years, aa_production_values)

    final = pd.concat([completed_coefficients, aa_coefficients], ignore_index=True)
    final["coeff"] = pd.to_numeric(final["coeff"], errors="coerce")
    final = final.sort_values(["ano", "conta_alfa", "tipo_coeff"]).reset_index(
        drop=True
    )
    return final[FINAL_COLUMNS]


def construir_tabela_saida_renda(
    coefficients_df: pd.DataFrame, tipo_coeff: str
) -> pd.DataFrame:
    filtered = coefficients_df.loc[coefficients_df["tipo_coeff"] == tipo_coeff].copy()
    if filtered.empty:
        return pd.DataFrame(columns=["ano", "conta_alfa", "coeff"])

    filtered = filtered[["ano", "conta_alfa", "coeff"]].copy()
    filtered["ano"] = pd.to_numeric(filtered["ano"], errors="coerce").astype("Int64")
    filtered["coeff"] = pd.to_numeric(filtered["coeff"], errors="coerce")
    filtered = filtered.sort_values(["ano", "conta_alfa"]).reset_index(drop=True)
    return filtered
