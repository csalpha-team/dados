from pathlib import Path
import pandas as pd


REQUIRED_POF_COLUMNS = [
    "ano",
    "variavel",
    "situacao_domicilio",
    "tipo_despesa",
    "valor",
]

FINAL_COLUMNS = ["ano", "coeff_key", "coeff"]


def load_mip_mapping(mip_path: Path, sheet_name: str) -> pd.DataFrame:
    if not mip_path.exists():
        raise FileNotFoundError(f"Arquivo MIP não encontrado: {mip_path}")

    return pd.read_excel(mip_path, sheet_name=sheet_name)


def validate_pof_columns(pof_df: pd.DataFrame) -> None:
    missing_columns = [column for column in REQUIRED_POF_COLUMNS if column not in pof_df.columns]
    if missing_columns:
        raise ValueError(
            f"Colunas obrigatórias ausentes na POF: {', '.join(missing_columns)}"
        )


def build_consumption_coefficients(
    pof_df: pd.DataFrame,
    mip_mapping_df: pd.DataFrame,
    parameters: dict,
) -> pd.DataFrame:
    validate_pof_columns(pof_df)

    mip_coeff_key_column = parameters["mip_coeff_key_column"]
    mip_expense_type_column = parameters["mip_expense_type_column"]

    if mip_coeff_key_column not in mip_mapping_df.columns:
        raise ValueError(f"Coluna '{mip_coeff_key_column}' ausente no de-para do MIP.")
    if mip_expense_type_column not in mip_mapping_df.columns:
        raise ValueError(f"Coluna '{mip_expense_type_column}' ausente no de-para do MIP.")

    pof_merged = pof_df.merge(
        mip_mapping_df[[mip_coeff_key_column, mip_expense_type_column]],
        left_on="tipo_despesa",
        right_on=mip_expense_type_column,
        how="inner",
    ).drop(columns=[mip_expense_type_column, "unidade_medida", "Unnamed: 0"], errors="ignore")

    pof_filtered = pof_merged[
        pof_merged["variavel"] == parameters["target_variable"]
    ].copy()

    pof_filtered = pof_filtered[pof_filtered["ano"] == parameters["target_year"]].copy()

    pof_filtered["valor"] = pd.to_numeric(pof_filtered["valor"], errors="coerce")
    pof_filtered["valor"] = pof_filtered["valor"] / 100

    pof_filtered = pof_filtered.rename(columns={mip_coeff_key_column: "coeff_key"})

    state_pattern = parameters["state_pattern"]
    urban_mask = (
        (pof_filtered["situacao_domicilio"] == parameters["urban_label"])
        & (pof_filtered["coeff_key"].str.contains(state_pattern, na=False))
    )
    rural_mask = (
        (pof_filtered["situacao_domicilio"] == parameters["rural_label"])
        & (~pof_filtered["coeff_key"].str.contains(state_pattern, na=False))
    )

    filtered = pof_filtered[urban_mask | rural_mask].copy().reset_index(drop=True)

    filtered = filtered.dropna(subset=["coeff_key", "valor"])
    filtered = filtered.drop_duplicates(subset=["coeff_key"], keep="last")

    coefficients = filtered[["ano", "coeff_key", "valor"]].rename(columns={"valor": "coeff"})
    return coefficients[FINAL_COLUMNS]
