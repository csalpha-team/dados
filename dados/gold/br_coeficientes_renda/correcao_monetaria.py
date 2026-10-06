"""IPCA-based monetary correction for annual income inputs."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Sequence

import numpy as np
import pandas as pd


CORRECTION_ATTR = "br_coeficientes_renda_correcao_monetaria"


@dataclass(frozen=True)
class MonetaryCorrectionConfig:
    """Settings that define the common price basis of monetary inputs."""

    enabled: bool = True
    series_code: int = 433
    anchor_year: int = 2022


def construir_fatores_correcao_ipca(
    ipca_df: pd.DataFrame,
    *,
    years: Iterable[int],
    config: MonetaryCorrectionConfig,
) -> pd.Series:
    """Return ``IPCA(anchor) / IPCA(year)`` for complete requested years."""

    required_columns = {"ano", "indice_ipca"}
    missing_columns = required_columns.difference(ipca_df.columns)
    if missing_columns:
        raise ValueError(
            f"Colunas obrigatorias ausentes no IPCA anual: {sorted(missing_columns)}"
        )
    if ipca_df.empty:
        raise ValueError("A tabela anual do IPCA esta vazia")

    normalized = ipca_df.copy()
    normalized["ano"] = pd.to_numeric(normalized["ano"], errors="coerce")
    normalized["indice_ipca"] = pd.to_numeric(
        normalized["indice_ipca"], errors="coerce"
    )
    if normalized[["ano", "indice_ipca"]].isna().any().any():
        raise ValueError("A tabela anual do IPCA contem ano ou indice invalido")
    normalized["ano"] = normalized["ano"].astype(int)

    if normalized["ano"].duplicated().any():
        duplicated_years = sorted(
            normalized.loc[normalized["ano"].duplicated(keep=False), "ano"]
            .unique()
            .tolist()
        )
        raise ValueError(f"A tabela anual do IPCA contem anos duplicados: {duplicated_years}")
    if (normalized["indice_ipca"] <= 0).any():
        raise ValueError("A tabela anual do IPCA contem indice nao positivo")

    if "serie_sgs" in normalized.columns:
        series_codes = set(
            pd.to_numeric(normalized["serie_sgs"], errors="coerce")
            .dropna()
            .astype(int)
            .tolist()
        )
        if series_codes != {config.series_code}:
            raise ValueError(
                f"Serie SGS inesperada no IPCA anual: {sorted(series_codes)}; "
                f"esperada: {config.series_code}"
            )

    required_years = sorted({int(year) for year in years} | {config.anchor_year})
    available_years = set(normalized["ano"].tolist())
    missing_years = sorted(set(required_years).difference(available_years))
    if missing_years:
        raise ValueError(f"IPCA anual ausente para os anos: {missing_years}")

    selected = normalized.loc[normalized["ano"].isin(required_years)].copy()
    if "ano_completo" in selected.columns:
        complete_values = selected["ano_completo"].map(
            lambda value: value
            if isinstance(value, (bool, np.bool_))
            else str(value).strip().lower() in {"true", "t", "1"}
        )
        incomplete = selected.loc[~complete_values, "ano"].tolist()
        if incomplete:
            raise ValueError(f"IPCA anual incompleto para os anos: {sorted(incomplete)}")
    if "meses_observados" in selected.columns:
        month_counts = pd.to_numeric(selected["meses_observados"], errors="coerce")
        incomplete = selected.loc[month_counts.ne(12), "ano"].tolist()
        if incomplete:
            raise ValueError(f"IPCA anual nao possui 12 meses nos anos: {sorted(incomplete)}")

    index_by_year = selected.set_index("ano")["indice_ipca"].sort_index()
    anchor_index = float(index_by_year.loc[config.anchor_year])
    factors = anchor_index / index_by_year
    factors.name = "fator_correcao_ipca"
    if not np.isclose(float(factors.loc[config.anchor_year]), 1.0):
        raise ValueError("O fator IPCA do ano-ancora deveria ser igual a 1")
    return factors.reindex(required_years)


def corrigir_colunas_monetarias(
    df: pd.DataFrame,
    *,
    monetary_columns: Sequence[str],
    factors: pd.Series,
    config: MonetaryCorrectionConfig,
) -> pd.DataFrame:
    """Convert only named annual monetary columns to anchor-year prices."""

    if CORRECTION_ATTR in df.attrs:
        previous = df.attrs[CORRECTION_ATTR]
        raise ValueError(f"DataFrame ja corrigido monetariamente: {previous}")

    missing_columns = set(monetary_columns).difference(df.columns)
    if missing_columns:
        raise ValueError(
            f"Colunas monetarias ausentes para correcao: {sorted(missing_columns)}"
        )
    if "ano" not in df.columns:
        raise ValueError("Coluna ano ausente para correcao monetaria")

    corrected = df.copy()
    years = pd.to_numeric(corrected["ano"], errors="coerce")
    if years.isna().any():
        raise ValueError("Existem anos invalidos nos dados monetarios")
    row_factors = years.astype(int).map(factors)
    missing_years = sorted(years.loc[row_factors.isna()].astype(int).unique().tolist())
    if missing_years:
        raise ValueError(f"Fator IPCA ausente para os anos: {missing_years}")

    for column in monetary_columns:
        values = pd.to_numeric(corrected[column], errors="coerce")
        corrected[column] = values * row_factors

    corrected.attrs[CORRECTION_ATTR] = {
        "serie_sgs": config.series_code,
        "ano_ancora": config.anchor_year,
    }
    return corrected


__all__ = [
    "CORRECTION_ATTR",
    "MonetaryCorrectionConfig",
    "construir_fatores_correcao_ipca",
    "corrigir_colunas_monetarias",
]
