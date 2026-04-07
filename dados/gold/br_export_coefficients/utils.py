import json
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, Mapping, Optional, Sequence, Tuple

import pandas as pd


REQUIRED_EXPORT_COLUMNS = [
    "ano",
    "id_ncm",
    "nome_ncm_portugues",
    "sigla_uf_ncm",
    "valor_fob_dolar",
]

FINAL_COLUMNS = ["ano", "produto", "valor_fob_dolar", "valor_fob_real", "coeff"]


def _escape_sql_literal(value: str) -> str:
    return value.replace("'", "''")


def list_table_columns(db: Any, schema: str, table: str) -> set[str]:
    schema_escaped = _escape_sql_literal(schema)
    table_escaped = _escape_sql_literal(table)

    query = f"""
    select column_name
    from information_schema.columns
    where table_schema = '{schema_escaped}'
      and table_name = '{table_escaped}'
    order by ordinal_position
    """

    columns_df = db.download_data(query)
    if columns_df.empty:
        return set()

    return set(columns_df["column_name"].astype(str))


def build_export_query(
    db: Any,
    source_schema: str,
    source_table: str,
    ncm_schema: str,
    ncm_table: str,
) -> str:
    source_columns = list_table_columns(db, source_schema, source_table)
    if not source_columns:
        raise ValueError(
            f"Tabela de origem não encontrada: {source_schema}.{source_table}"
        )

    required_source_columns = {"ano", "id_ncm", "sigla_uf_ncm", "valor_fob_dolar"}
    missing_source_columns = sorted(required_source_columns - source_columns)

    if missing_source_columns:
        raise ValueError(
            "Colunas obrigatórias ausentes na tabela de origem "
            f"{source_schema}.{source_table}: {', '.join(missing_source_columns)}"
        )

    if "nome_ncm_portugues" in source_columns:
        name_expression = "nullif(trim(c.nome_ncm_portugues), '')"
        join_clause = ""
    else:
        ncm_columns = list_table_columns(db, ncm_schema, ncm_table)
        if "nome_ncm_portugues" not in ncm_columns:
            raise ValueError(
                "Não foi possível obter 'nome_ncm_portugues'. "
                f"A coluna não existe em {source_schema}.{source_table} e nem em "
                f"{ncm_schema}.{ncm_table}."
            )

        join_clause = (
            f"left join {ncm_schema}.{ncm_table} as n "
            "on c.id_ncm = n.id_ncm"
        )
        name_expression = "nullif(trim(n.nome_ncm_portugues), '')"

    return f"""
    select
        c.ano,
        c.id_ncm,
        c.sigla_uf_ncm,
        {name_expression} as nome_ncm_portugues,
        c.valor_fob_dolar
    from {source_schema}.{source_table} as c
    {join_clause}
    where c.ano is not null
      and c.id_ncm is not null
      and c.valor_fob_dolar is not null
    """


def _build_forecast_years(config_value: Any) -> list[int]:
    if isinstance(config_value, dict):
        start = int(config_value.get("start", 1995))
        end = int(config_value.get("end", 2023))
        if end < start:
            raise ValueError("forecast_years.end deve ser maior ou igual a forecast_years.start")
        return list(range(start, end + 1))

    if isinstance(config_value, list):
        years = sorted({int(year) for year in config_value})
        if not years:
            raise ValueError("forecast_years não pode ser vazio")
        return years

    return list(range(1995, 2024))


def load_export_parameters(
    config_path: Path,
) -> tuple[dict[str, list[str]], dict[tuple[str, str], float], list[int], float, str]:
    with config_path.open("r", encoding="utf-8") as file:
        config = json.load(file)

    products_preparations = config.get("products_preparations", {})
    if not isinstance(products_preparations, dict):
        raise ValueError("products_preparations deve ser um dicionário no arquivo de configuração")

    normalized_products_preparations: dict[str, list[str]] = {}
    for product, ncm_names in products_preparations.items():
        normalized_product = str(product).strip()
        if not isinstance(ncm_names, list):
            raise ValueError(
                "Cada valor de products_preparations deve ser uma lista de nomes NCM"
            )

        normalized_products_preparations[normalized_product] = [
            str(ncm_name).strip() for ncm_name in ncm_names if str(ncm_name).strip()
        ]

    specific_shares_raw = config.get("specific_shares", [])
    if not isinstance(specific_shares_raw, list):
        raise ValueError("specific_shares deve ser uma lista no arquivo de configuração")

    specific_shares: dict[tuple[str, str], float] = {}
    for item in specific_shares_raw:
        if not isinstance(item, dict):
            raise ValueError("Cada item de specific_shares deve ser um objeto")

        product = str(item.get("product", "")).strip()
        ncm_name = str(item.get("ncm_name", "")).strip()

        if not product or not ncm_name:
            raise ValueError(
                "Cada item de specific_shares precisa de 'product' e 'ncm_name' válidos"
            )

        share = float(item.get("share", 0.0))
        if share < 0 or share > 1:
            raise ValueError("Os valores de share devem estar no intervalo [0, 1]")

        specific_shares[(product, ncm_name)] = share

    years = _build_forecast_years(config.get("forecast_years"))

    exchange_rate_brl_per_usd = float(config.get("exchange_rate_brl_per_usd", 1.0))
    if exchange_rate_brl_per_usd < 0:
        raise ValueError("exchange_rate_brl_per_usd deve ser não-negativo")

    target_state = str(config.get("target_state", "PA")).strip().upper()

    return (
        normalized_products_preparations,
        specific_shares,
        years,
        exchange_rate_brl_per_usd,
        target_state,
    )


def _linear_regression_predict(
    observed_years: list[int],
    observed_values: list[float],
    target_years: list[int],
    *,
    clamp_non_negative: bool,
) -> dict[int, float]:
    if not observed_years:
        return {year: 0.0 for year in target_years}

    if len(observed_years) == 1:
        prediction = {year: observed_values[0] for year in target_years}
    else:
        n_points = len(observed_years)
        mean_x = sum(observed_years) / n_points
        mean_y = sum(observed_values) / n_points

        denominator = sum((x - mean_x) ** 2 for x in observed_years)
        if denominator == 0:
            slope = 0.0
        else:
            numerator = sum(
                (x - mean_x) * (y - mean_y)
                for x, y in zip(observed_years, observed_values)
            )
            slope = numerator / denominator

        intercept = mean_y - slope * mean_x
        prediction = {year: slope * year + intercept for year in target_years}

    if clamp_non_negative:
        prediction = {year: max(0.0, value) for year, value in prediction.items()}

    return prediction


def forecast_exports_linear(
    export_df: pd.DataFrame,
    years: Sequence[int],
    *,
    year_col: str = "ano",
    label_cols: Sequence[str] = ("id_ncm", "nome_ncm_portugues"),
    value_col: str = "valor_fob_dolar",
    include_history: bool = True,
    clamp_non_negative: bool = True,
) -> pd.DataFrame:
    forecast_years = sorted({int(year) for year in years})
    if not forecast_years:
        raise ValueError("A lista de anos para forecast não pode ser vazia")

    needed_columns = [year_col, value_col, *label_cols]
    missing_columns = [column for column in needed_columns if column not in export_df.columns]
    if missing_columns:
        raise ValueError(
            "Colunas obrigatórias ausentes para forecast: "
            f"{', '.join(missing_columns)}"
        )

    data = export_df[needed_columns].copy()
    data[year_col] = pd.to_numeric(data[year_col], errors="coerce")
    data[value_col] = pd.to_numeric(data[value_col], errors="coerce")

    for column in label_cols:
        data[column] = data[column].astype("string").str.strip()

    data = data.dropna(subset=[year_col, value_col, *label_cols]).copy()
    if data.empty:
        return pd.DataFrame(columns=needed_columns)

    data[year_col] = data[year_col].astype(int)

    grouped = data.groupby([year_col, *label_cols], as_index=False)[value_col].sum()

    forecast_rows: list[dict[str, Any]] = []
    for labels, group in grouped.groupby(list(label_cols), dropna=False):
        ordered = group.sort_values(year_col)
        observed_years = ordered[year_col].astype(int).tolist()
        observed_values = ordered[value_col].astype(float).tolist()
        observed_map = dict(zip(observed_years, observed_values))

        predicted_map = _linear_regression_predict(
            observed_years,
            observed_values,
            forecast_years,
            clamp_non_negative=clamp_non_negative,
        )

        if not isinstance(labels, tuple):
            labels = (labels,)

        label_data = {column: value for column, value in zip(label_cols, labels)}

        for year in forecast_years:
            value = predicted_map[year]
            if include_history and year in observed_map:
                value = observed_map[year]

            forecast_rows.append(
                {
                    year_col: year,
                    value_col: float(value),
                    **label_data,
                }
            )

    return pd.DataFrame(forecast_rows)


def distribute_exports_by_product(
    exports_by_ncm: pd.DataFrame,
    products_preparations: Mapping[str, Sequence[str]],
    *,
    ncm_col: str = "nome_ncm_portugues",
    id_ncm_col: str = "id_ncm",
    fill_value: float = 0.0,
    specific_shares: Optional[Mapping[Tuple[str, str], float]] = None,
) -> pd.DataFrame:
    if ncm_col not in exports_by_ncm.columns:
        raise KeyError(f"Coluna '{ncm_col}' não encontrada no dataframe de exportações")

    metric_columns = [
        column for column in exports_by_ncm.columns if column not in {ncm_col, id_ncm_col}
    ]

    numeric_data = exports_by_ncm.copy()
    for column in metric_columns:
        numeric_data[column] = pd.to_numeric(numeric_data[column], errors="coerce")
    numeric_data = numeric_data.fillna({column: fill_value for column in metric_columns})

    ncm_to_products: Dict[str, set[str]] = defaultdict(set)
    for product, ncm_names in products_preparations.items():
        for ncm_name in ncm_names:
            ncm_to_products[str(ncm_name)].add(str(product))

    normalized_specific_shares = {
        (str(product), str(ncm_name)): share
        for (product, ncm_name), share in (specific_shares or {}).items()
    }

    distributed_rows: list[dict[str, Any]] = []
    for _, row in numeric_data.iterrows():
        ncm_name = str(row[ncm_col])
        related_products = sorted(ncm_to_products.get(ncm_name, []))
        if not related_products:
            continue

        values = row[metric_columns]
        defined_shares = {
            product: normalized_specific_shares[(product, ncm_name)]
            for product in related_products
            if (product, ncm_name) in normalized_specific_shares
        }

        total_defined_share = sum(defined_shares.values())
        if total_defined_share > 1.0 + 1e-12:
            raise ValueError(
                "A soma das participações específicas excede 1.0 para "
                f"'{ncm_name}' ({total_defined_share:.4f})"
            )

        if defined_shares:
            for product, share in defined_shares.items():
                distributed_rows.append(
                    {"produto": product, **(values * share).to_dict()}
                )

            remaining_products = [
                product for product in related_products if product not in defined_shares
            ]
            remaining_share_total = max(0.0, 1.0 - total_defined_share)
            if remaining_products and remaining_share_total > 0:
                remaining_share = remaining_share_total / len(remaining_products)
                for product in remaining_products:
                    distributed_rows.append(
                        {"produto": product, **(values * remaining_share).to_dict()}
                    )
        else:
            equal_share = 1.0 / len(related_products)
            values_distributed = values * equal_share
            for product in related_products:
                distributed_rows.append(
                    {"produto": product, **values_distributed.to_dict()}
                )

    if not distributed_rows:
        return pd.DataFrame(columns=metric_columns, index=pd.Index([], name="produto"))

    return (
        pd.DataFrame(distributed_rows)
        .groupby("produto", as_index=True)[metric_columns]
        .sum(min_count=1)
        .fillna(fill_value)
        .sort_index()
    )


def prepare_export_coefficients_data(
    export_df: pd.DataFrame,
    products_preparations: Mapping[str, Sequence[str]],
    specific_shares: Mapping[Tuple[str, str], float],
    years: Sequence[int],
    exchange_rate_brl_per_usd: float,
    target_state: str,
) -> pd.DataFrame:
    missing_columns = [
        column for column in REQUIRED_EXPORT_COLUMNS if column not in export_df.columns
    ]
    if missing_columns:
        raise ValueError(
            "Colunas obrigatórias ausentes no dataframe de exportação: "
            f"{', '.join(missing_columns)}"
        )

    data = export_df[REQUIRED_EXPORT_COLUMNS].copy()

    data["ano"] = pd.to_numeric(data["ano"], errors="coerce")
    data["valor_fob_dolar"] = pd.to_numeric(data["valor_fob_dolar"], errors="coerce")
    data["nome_ncm_portugues"] = data["nome_ncm_portugues"].astype("string").str.strip()
    data["id_ncm"] = data["id_ncm"].astype("string").str.strip()
    data["sigla_uf_ncm"] = data["sigla_uf_ncm"].astype("string").str.strip().str.upper()

    data = data.dropna(subset=["ano", "valor_fob_dolar", "nome_ncm_portugues", "id_ncm"])

    if target_state:
        data = data[data["sigla_uf_ncm"] == target_state]

    if data.empty:
        return pd.DataFrame(columns=FINAL_COLUMNS)

    data["ano"] = data["ano"].astype(int)

    grouped = data.groupby(
        ["ano", "id_ncm", "nome_ncm_portugues"], as_index=False
    )["valor_fob_dolar"].sum()

    forecast_df = forecast_exports_linear(
        grouped,
        years,
        year_col="ano",
        label_cols=("id_ncm", "nome_ncm_portugues"),
        value_col="valor_fob_dolar",
        include_history=True,
        clamp_non_negative=True,
    )

    distributed_per_year: list[pd.DataFrame] = []
    for year in sorted({int(value) for value in years}):
        year_data = forecast_df.loc[forecast_df["ano"] == year]
        if year_data.empty:
            continue

        grouped_year_data = (
            year_data.groupby("nome_ncm_portugues", as_index=False)
            .agg({"id_ncm": "first", "valor_fob_dolar": "sum"})
            .loc[:, ["nome_ncm_portugues", "id_ncm", "valor_fob_dolar"]]
        )

        distributed = distribute_exports_by_product(
            grouped_year_data,
            products_preparations,
            specific_shares=specific_shares,
        )

        if distributed.empty:
            continue

        distributed = distributed.reset_index()
        distributed["ano"] = year
        distributed_per_year.append(distributed)

    if not distributed_per_year:
        return pd.DataFrame(columns=FINAL_COLUMNS)

    final = pd.concat(distributed_per_year, ignore_index=True)

    final["valor_fob_real"] = (
        pd.to_numeric(final["valor_fob_dolar"], errors="coerce")
        * exchange_rate_brl_per_usd
        / 1000.0
    )

    year_totals = final.groupby("ano")["valor_fob_real"].transform("sum")
    final["coeff"] = 0.0

    non_zero_totals = year_totals != 0
    final.loc[non_zero_totals, "coeff"] = (
        final.loc[non_zero_totals, "valor_fob_real"]
        / year_totals[non_zero_totals]
    )

    final = final[["ano", "produto", "valor_fob_dolar", "valor_fob_real", "coeff"]]
    final = final.sort_values(["ano", "produto"]).reset_index(drop=True)

    return final[FINAL_COLUMNS]


def build_export_coefficients_json(export_coefficients: pd.DataFrame) -> dict[str, list[dict[str, Any]]]:
    if export_coefficients.empty:
        return {}

    payload: dict[str, list[dict[str, Any]]] = {}
    for year, year_data in export_coefficients.groupby("ano", as_index=False):
        payload[str(int(year))] = year_data.drop(columns=["ano"]).to_dict(orient="records")

    return payload


def save_export_coefficients_json(
    export_coefficients: pd.DataFrame,
    output_path: Path,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    payload = build_export_coefficients_json(export_coefficients)

    with output_path.open("w", encoding="utf-8") as file:
        json.dump(payload, file, ensure_ascii=False, indent=2)
