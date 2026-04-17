import json
from pathlib import Path

import pandas as pd


FINAL_COLUMNS = ["coeff_key", "coeff"]


def carregar_coeficientes_investimento(json_path: Path) -> pd.DataFrame:
    """Converte o JSON versionado de investimento para o formato tabular final.

    O arquivo deve conter apenas pares `coeff_key -> coeff` porque os valores
    ja chegam consolidados de uma pesquisa anterior, e nao de um calculo
    reproduzido neste repositorio. A funcao so garante que o artefato esteja em
    um formato simples, consistente e pronto para carga na zona gold.
    """
    if not json_path.exists():
        raise FileNotFoundError(f"Arquivo de coeficientes não encontrado: {json_path}")

    with json_path.open("r", encoding="utf-8") as file:
        raw_data = json.load(file)

    if not isinstance(raw_data, dict):
        raise ValueError("coeficientes_investimento.json deve conter um objeto chave-valor")

    coefficients = pd.DataFrame(
        [
            {"coeff_key": str(coeff_key), "coeff": value}
            for coeff_key, value in raw_data.items()
        ]
    )

    coefficients["coeff"] = pd.to_numeric(coefficients["coeff"], errors="coerce")
    coefficients = coefficients.dropna(subset=["coeff_key", "coeff"]).copy()
    coefficients = coefficients.sort_values("coeff_key").reset_index(drop=True)

    return coefficients[FINAL_COLUMNS]
