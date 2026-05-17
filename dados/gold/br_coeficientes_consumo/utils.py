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


def carregar_mapeamento_mip(mip_path: Path, sheet_name: str) -> pd.DataFrame:
    if not mip_path.exists():
        raise FileNotFoundError(f"Arquivo MIP não encontrado: {mip_path}")

    return pd.read_excel(mip_path, sheet_name=sheet_name)


def validar_colunas_pof(pof_df: pd.DataFrame) -> None:
    missing_columns = [
        column for column in REQUIRED_POF_COLUMNS if column not in pof_df.columns
    ]
    if missing_columns:
        raise ValueError(
            f"Colunas obrigatórias ausentes na POF: {', '.join(missing_columns)}"
        )


def construir_coeficientes_consumo(
    pof_df: pd.DataFrame,
    mip_mapping_df: pd.DataFrame,
    parametros: dict,
) -> pd.DataFrame:
    validar_colunas_pof(pof_df)

    coluna_chave_mip = parametros["coluna_chave_mip"]
    coluna_tipo_despesa_mip = parametros["coluna_tipo_despesa_mip"]

    if coluna_chave_mip not in mip_mapping_df.columns:
        raise ValueError(f"Coluna '{coluna_chave_mip}' ausente no de-para do MIP.")
    if coluna_tipo_despesa_mip not in mip_mapping_df.columns:
        raise ValueError(
            f"Coluna '{coluna_tipo_despesa_mip}' ausente no de-para do MIP."
        )

    pof_merged = pof_df.merge(
        mip_mapping_df[[coluna_chave_mip, coluna_tipo_despesa_mip]],
        left_on="tipo_despesa",
        right_on=coluna_tipo_despesa_mip,
        how="inner",
    ).drop(
        columns=[coluna_tipo_despesa_mip, "unidade_medida", "Unnamed: 0"],
        errors="ignore",
    )

    pof_filtrada = pof_merged[
        pof_merged["variavel"] == parametros["variavel_alvo"]
    ].copy()

    pof_filtrada = pof_filtrada[pof_filtrada["ano"] == parametros["ano_alvo"]].copy()

    pof_filtrada["valor"] = pd.to_numeric(pof_filtrada["valor"], errors="coerce")
    pof_filtrada["valor"] = pof_filtrada["valor"] / 100

    pof_filtrada = pof_filtrada.rename(columns={coluna_chave_mip: "coeff_key"})

    padrao_estado = parametros["padrao_estado"]
    mascara_urbana = (
        pof_filtrada["situacao_domicilio"] == parametros["rotulo_urbano"]
    ) & (pof_filtrada["coeff_key"].str.contains(padrao_estado, na=False))
    mascara_rural = (
        pof_filtrada["situacao_domicilio"] == parametros["rotulo_rural"]
    ) & (~pof_filtrada["coeff_key"].str.contains(padrao_estado, na=False))

    filtrado = (
        pof_filtrada[mascara_urbana | mascara_rural].copy().reset_index(drop=True)
    )

    filtrado = filtrado.dropna(subset=["coeff_key", "valor"])
    filtrado = filtrado.drop_duplicates(subset=["coeff_key"], keep="last")

    coefficients = filtrado[["ano", "coeff_key", "valor"]].rename(
        columns={"valor": "coeff"}
    )
    return coefficients[FINAL_COLUMNS]
