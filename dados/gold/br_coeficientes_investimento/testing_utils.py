from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pandas as pd

from dados.gold.br_coeficientes_investimento.utils import carregar_coeficientes_investimento


DEFAULT_JSON_PATH = Path(__file__).with_name("coeficientes_investimento.json")


def validar_cenario_coeficientes_unitarios(
    json_path: Path = DEFAULT_JSON_PATH,
) -> tuple[pd.DataFrame, list[str]]:
    with json_path.open("r", encoding="utf-8") as file:
        original_coefficients = json.load(file)

    unit_coefficients = {str(coeff_key): 1.0 for coeff_key in original_coefficients}
    expected_keys = sorted(unit_coefficients)

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir) / "coeficientes_investimento_teste.json"
        temp_path.write_text(
            json.dumps(unit_coefficients, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        coefficients = carregar_coeficientes_investimento(temp_path)

    return coefficients, expected_keys
