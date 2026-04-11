"""Simple annual interpolation and forecasting helpers for income panels."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, List, Mapping, MutableMapping, Optional, Sequence, Tuple, Union

import numpy as np
import pandas as pd

ForecastValues = Mapping[int, float]


def _ensure_tuple(value: Union[str, Sequence[str]]) -> Tuple[str, ...]:
    if isinstance(value, str):
        return (value,)
    return tuple(value)


def _clean_forecast_value(value: float, clamp_non_negative: bool) -> float:
    if np.isnan(value):
        return np.nan
    if clamp_non_negative:
        return float(max(value, 0.0))
    return float(value)


@dataclass
class ForecastConfig:
    """Basic settings for the annual forecast."""

    method: str = "linear"
    min_history: int = 2
    clamp_non_negative: bool = True
    rolling_window: int = 3


class IncomeForecaster:
    """Applies simple forecasting/interpolation methods per annual label series."""

    def __init__(
        self,
        *,
        year_col: str,
        label_cols: Union[str, Sequence[str]],
        value_cols: Union[str, Sequence[str]],
        config: Optional[ForecastConfig] = None,
    ) -> None:
        self.year_col = year_col
        self.label_cols: Tuple[str, ...] = _ensure_tuple(label_cols)
        self.value_cols: Tuple[str, ...] = _ensure_tuple(value_cols)
        self.config = config or ForecastConfig()

    def forecast(
        self,
        df: pd.DataFrame,
        *,
        forecast_years: Iterable[int],
        include_history: bool = False,
        drop_duplicates: bool = True,
    ) -> pd.DataFrame:
        if df.empty:
            raise ValueError("Input DataFrame is empty.")

        cleaned = df.copy()
        required_columns = {self.year_col, *self.label_cols, *self.value_cols}
        missing_columns = required_columns.difference(cleaned.columns)
        if missing_columns:
            raise KeyError(f"Missing columns in input DataFrame: {sorted(missing_columns)}")

        cleaned = cleaned[list(required_columns)].dropna(subset=[self.year_col]).copy()
        cleaned[self.year_col] = cleaned[self.year_col].astype(int)
        for column in self.value_cols:
            cleaned[column] = pd.to_numeric(cleaned[column], errors="coerce")

        if drop_duplicates:
            subset_keys = [self.year_col, *self.label_cols]
            cleaned = cleaned.sort_values(subset_keys + list(self.value_cols))
            cleaned = cleaned.drop_duplicates(subset=subset_keys, keep="last")

        target_years = sorted({int(year) for year in forecast_years})
        if not target_years:
            raise ValueError("No year was provided in `forecast_years`.")

        forecast_frames: List[pd.DataFrame] = []

        for labels, group in cleaned.groupby(list(self.label_cols), dropna=False):
            if not isinstance(labels, tuple):
                labels = (labels,)

            group_sorted = group.sort_values(self.year_col)
            predictions_by_column: Dict[str, ForecastValues] = {}

            for column in self.value_cols:
                predictions_by_column[column] = self._predict_series(
                    years=group_sorted[self.year_col].to_numpy(dtype=int),
                    values=group_sorted[column].to_numpy(dtype=float),
                    targets=target_years,
                )

            rows = []
            for year in target_years:
                row: MutableMapping[str, Union[int, float, str]] = {self.year_col: year}
                for index, label_name in enumerate(self.label_cols):
                    row[label_name] = labels[index]
                for column in self.value_cols:
                    value = predictions_by_column[column].get(year, np.nan)
                    row[column] = _clean_forecast_value(value, self.config.clamp_non_negative)
                rows.append(row)

            forecast_frames.append(pd.DataFrame(rows))

        forecast_df = pd.concat(forecast_frames, ignore_index=True)
        sort_keys = [*self.label_cols, self.year_col]

        if include_history:
            history_df = cleaned.copy()
            history_df["_source_priority"] = 0
            forecast_df["_source_priority"] = 1

            combined = pd.concat([history_df, forecast_df], ignore_index=True)
            combined = combined.sort_values(sort_keys + ["_source_priority"])
            combined = combined.drop_duplicates(
                subset=[*self.label_cols, self.year_col],
                keep="first",
            )
            combined = combined.drop(columns="_source_priority")
            return combined.sort_values(sort_keys).reset_index(drop=True)

        return forecast_df.sort_values(sort_keys).reset_index(drop=True)

    def _predict_series(
        self,
        *,
        years: np.ndarray,
        values: np.ndarray,
        targets: Sequence[int],
    ) -> Dict[int, float]:
        valid_mask = ~np.isnan(values)
        years = years[valid_mask]
        values = values[valid_mask]

        if years.size == 0:
            return {year: np.nan for year in targets}

        method = self.config.method.lower()
        if method == "linear":
            return self._predict_linear(years, values, targets)
        if method == "cagr":
            return self._predict_cagr(years, values, targets)
        if method == "rolling_mean":
            return self._predict_rolling_mean(years, values, targets)
        if method == "last":
            return self._predict_last_value(years, values, targets)

        raise ValueError(f"Unknown forecast method: {self.config.method!r}")

    def _predict_linear(
        self,
        years: np.ndarray,
        values: np.ndarray,
        targets: Sequence[int],
    ) -> Dict[int, float]:
        if years.size < max(self.config.min_history, 2):
            return self._predict_last_value(years, values, targets)

        slope, intercept = np.polyfit(years, values, 1)
        return {int(year): float(intercept + slope * year) for year in targets}

    def _predict_cagr(
        self,
        years: np.ndarray,
        values: np.ndarray,
        targets: Sequence[int],
    ) -> Dict[int, float]:
        if years.size < 2 or np.any(values <= 0):
            return self._predict_linear(years, values, targets)

        start_year = int(years[0])
        end_year = int(years[-1])
        start_value = float(values[0])
        end_value = float(values[-1])
        span = end_year - start_year

        if span <= 0:
            return self._predict_last_value(years, values, targets)

        rate = (end_value / start_value) ** (1.0 / span) - 1.0
        predictions: Dict[int, float] = {}
        for year in targets:
            if year >= end_year:
                delta = year - end_year
                base = end_value
            else:
                delta = year - start_year
                base = start_value
            predictions[int(year)] = float(base * ((1.0 + rate) ** delta))
        return predictions

    def _predict_rolling_mean(
        self,
        years: np.ndarray,
        values: np.ndarray,
        targets: Sequence[int],
    ) -> Dict[int, float]:
        window = max(self.config.rolling_window, 1)
        if years.size < 1:
            return {int(year): np.nan for year in targets}

        history = pd.Series(values, index=years)
        rolling_mean = history.rolling(window=window, min_periods=1).mean()
        last_value = float(rolling_mean.iloc[-1])

        predictions: Dict[int, float] = {}
        for year in targets:
            if year in rolling_mean.index:
                predictions[int(year)] = float(rolling_mean.loc[year])
            else:
                predictions[int(year)] = last_value
        return predictions

    @staticmethod
    def _predict_last_value(
        years: np.ndarray,
        values: np.ndarray,
        targets: Sequence[int],
    ) -> Dict[int, float]:
        if values.size == 0:
            return {int(year): np.nan for year in targets}
        last_value = float(values[-1])
        return {int(year): last_value for year in targets}


__all__ = ["ForecastConfig", "IncomeForecaster"]
