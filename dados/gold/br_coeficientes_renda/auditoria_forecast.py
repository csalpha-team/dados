from __future__ import annotations

import math
from dataclasses import dataclass
from pathlib import Path

import pandas as pd


@dataclass(frozen=True)
class ForecastAuditConfig:
    year_column: str = "ano"
    account_column: str = "conta_alfa"
    coefficient_type_column: str = "tipo_coeff"
    value_column: str = "coeff"
    sigma_multiplier: float = 3.0
    min_relative_tolerance: float = 0.20
    min_transitions: int = 3


def _audit_columns(config: ForecastAuditConfig) -> list[str]:
    return [
        config.account_column,
        config.coefficient_type_column,
        "ano_anterior",
        config.year_column,
        "valor_anterior",
        "valor_atual",
        "variacao_percentual",
        "variacao_log",
        "cagr_log_esperado",
        "limite_inferior_log",
        "limite_superior_log",
        "discrepancia_log",
        "tipo_discrepancia",
        "possivel_motivo",
    ]


def _validar_colunas_auditoria(data: pd.DataFrame, config: ForecastAuditConfig) -> None:
    required_columns = {
        config.year_column,
        config.account_column,
        config.coefficient_type_column,
        config.value_column,
    }
    missing_columns = sorted(required_columns.difference(data.columns))
    if missing_columns:
        raise ValueError(
            "Colunas obrigatorias ausentes para auditoria de forecast: "
            + ", ".join(missing_columns)
        )


def _calcular_limites_variacao(
    annual_log_returns: pd.Series,
    years: pd.Series,
    values: pd.Series,
    config: ForecastAuditConfig,
) -> tuple[float, float, float]:
    first_year = int(years.iloc[0])
    last_year = int(years.iloc[-1])
    year_span = last_year - first_year

    if year_span <= 0 or values.iloc[0] <= 0 or values.iloc[-1] <= 0:
        expected_cagr_log = 0.0
    else:
        expected_cagr_log = math.log(float(values.iloc[-1] / values.iloc[0])) / year_span

    residuals = annual_log_returns - expected_cagr_log
    residual_center = float(residuals.median())
    mad = float((residuals - residual_center).abs().median())
    robust_sigma = 1.4826 * mad

    tolerance = max(
        config.sigma_multiplier * robust_sigma,
        math.log1p(config.min_relative_tolerance),
    )
    lower_bound = expected_cagr_log + residual_center - tolerance
    upper_bound = expected_cagr_log + residual_center + tolerance
    return expected_cagr_log, lower_bound, upper_bound


def _classificar_possivel_motivo(tipo_discrepancia: str) -> str:
    if tipo_discrepancia == "valor_nao_positivo":
        return (
            "coeficiente final menor ou igual a zero; "
            "verificar denominador, numerador ou imputacao"
        )
    if tipo_discrepancia == "intervalo_temporal_irregular":
        return "serie possui intervalo entre anos diferente de um; verificar completude temporal"
    if tipo_discrepancia == "acima_banda_superior":
        return (
            "crescimento anual acima da banda estatistica; "
            "possivel quebra, outlier ou mudanca metodologica"
        )
    if tipo_discrepancia == "abaixo_banda_inferior":
        return (
            "queda anual abaixo da banda estatistica; "
            "possivel quebra, outlier ou mudanca metodologica"
        )
    return "discrepancia temporal nao classificada"


def auditar_variacao_temporal_forecast(
    coefficients: pd.DataFrame,
    config: ForecastAuditConfig | None = None,
) -> pd.DataFrame:
    audit_config = config or ForecastAuditConfig()
    _validar_colunas_auditoria(coefficients, audit_config)

    prepared = coefficients[
        [
            audit_config.year_column,
            audit_config.account_column,
            audit_config.coefficient_type_column,
            audit_config.value_column,
        ]
    ].copy()
    prepared[audit_config.year_column] = pd.to_numeric(
        prepared[audit_config.year_column], errors="coerce"
    )
    prepared[audit_config.value_column] = pd.to_numeric(
        prepared[audit_config.value_column], errors="coerce"
    )
    prepared = prepared.dropna(
        subset=[
            audit_config.year_column,
            audit_config.account_column,
            audit_config.coefficient_type_column,
        ]
    ).copy()
    prepared[audit_config.year_column] = prepared[audit_config.year_column].astype(int)

    audit_rows: list[dict[str, object]] = []
    group_columns = [
        audit_config.account_column,
        audit_config.coefficient_type_column,
    ]

    for (account_name, coefficient_type), group in prepared.groupby(
        group_columns, dropna=False
    ):
        ordered = group.sort_values(audit_config.year_column).copy()
        ordered = ordered.drop_duplicates(subset=[audit_config.year_column], keep="last")

        non_positive = ordered[
            ordered[audit_config.value_column].isna()
            | (ordered[audit_config.value_column] <= 0)
        ]
        for _, row in non_positive.iterrows():
            audit_rows.append(
                {
                    audit_config.account_column: account_name,
                    audit_config.coefficient_type_column: coefficient_type,
                    "ano_anterior": pd.NA,
                    audit_config.year_column: int(row[audit_config.year_column]),
                    "valor_anterior": pd.NA,
                    "valor_atual": row[audit_config.value_column],
                    "variacao_percentual": pd.NA,
                    "variacao_log": pd.NA,
                    "cagr_log_esperado": pd.NA,
                    "limite_inferior_log": pd.NA,
                    "limite_superior_log": pd.NA,
                    "discrepancia_log": pd.NA,
                    "tipo_discrepancia": "valor_nao_positivo",
                    "possivel_motivo": _classificar_possivel_motivo(
                        "valor_nao_positivo"
                    ),
                }
            )

        positive = ordered[ordered[audit_config.value_column] > 0].copy()
        if positive.shape[0] < 2:
            continue

        years = positive[audit_config.year_column].reset_index(drop=True)
        values = positive[audit_config.value_column].reset_index(drop=True)
        year_gaps = years.diff().iloc[1:].reset_index(drop=True)
        annual_log_returns = (
            (values / values.shift(1)).map(math.log).iloc[1:].reset_index(drop=True)
            / year_gaps
        )

        for index, year_gap in year_gaps.items():
            if int(year_gap) != 1:
                current_position = index + 1
                audit_rows.append(
                    {
                        audit_config.account_column: account_name,
                        audit_config.coefficient_type_column: coefficient_type,
                        "ano_anterior": int(years.iloc[current_position - 1]),
                        audit_config.year_column: int(years.iloc[current_position]),
                        "valor_anterior": float(values.iloc[current_position - 1]),
                        "valor_atual": float(values.iloc[current_position]),
                        "variacao_percentual": float(
                            values.iloc[current_position]
                            / values.iloc[current_position - 1]
                            - 1.0
                        ),
                        "variacao_log": float(annual_log_returns.iloc[index]),
                        "cagr_log_esperado": pd.NA,
                        "limite_inferior_log": pd.NA,
                        "limite_superior_log": pd.NA,
                        "discrepancia_log": pd.NA,
                        "tipo_discrepancia": "intervalo_temporal_irregular",
                        "possivel_motivo": _classificar_possivel_motivo(
                            "intervalo_temporal_irregular"
                        ),
                    }
                )

        if annual_log_returns.shape[0] < audit_config.min_transitions:
            continue

        expected_cagr_log, lower_bound, upper_bound = _calcular_limites_variacao(
            annual_log_returns,
            years,
            values,
            audit_config,
        )

        for index, log_return in annual_log_returns.items():
            discrepancy_type = None
            discrepancy_value = 0.0
            if log_return > upper_bound:
                discrepancy_type = "acima_banda_superior"
                discrepancy_value = float(log_return - upper_bound)
            elif log_return < lower_bound:
                discrepancy_type = "abaixo_banda_inferior"
                discrepancy_value = float(lower_bound - log_return)

            if discrepancy_type is None:
                continue

            current_position = index + 1
            audit_rows.append(
                {
                    audit_config.account_column: account_name,
                    audit_config.coefficient_type_column: coefficient_type,
                    "ano_anterior": int(years.iloc[current_position - 1]),
                    audit_config.year_column: int(years.iloc[current_position]),
                    "valor_anterior": float(values.iloc[current_position - 1]),
                    "valor_atual": float(values.iloc[current_position]),
                    "variacao_percentual": float(
                        values.iloc[current_position]
                        / values.iloc[current_position - 1]
                        - 1.0
                    ),
                    "variacao_log": float(log_return),
                    "cagr_log_esperado": expected_cagr_log,
                    "limite_inferior_log": lower_bound,
                    "limite_superior_log": upper_bound,
                    "discrepancia_log": discrepancy_value,
                    "tipo_discrepancia": discrepancy_type,
                    "possivel_motivo": _classificar_possivel_motivo(discrepancy_type),
                }
            )

    if not audit_rows:
        return pd.DataFrame(columns=_audit_columns(audit_config))

    audit = pd.DataFrame(audit_rows)
    audit = audit.sort_values(
        [
            audit_config.account_column,
            audit_config.coefficient_type_column,
            audit_config.year_column,
        ]
    ).reset_index(drop=True)
    return audit[_audit_columns(audit_config)]


def exportar_auditoria_forecast(
    audit_df: pd.DataFrame,
    output_path: str | Path,
    config: ForecastAuditConfig | None = None,
) -> Path:
    audit_config = config or ForecastAuditConfig()
    destination = Path(output_path)
    destination.parent.mkdir(parents=True, exist_ok=True)

    output_df = audit_df.copy()
    if output_df.empty:
        output_df = pd.DataFrame(columns=_audit_columns(audit_config))

    output_df.to_csv(destination, index=False)
    return destination
