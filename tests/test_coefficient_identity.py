from __future__ import annotations

import unittest

import pandas as pd

from dados.gold.br_coeficientes_consumo.testing_utils import (
    validar_cenario_coeficientes_unitarios as validar_consumo_unitario,
)
from dados.gold.pa_coeficientes_custo.testing_utils import (
    validar_cenario_coeficientes_unitarios as validar_custo_unitario,
)


class CoefficientIdentityTests(unittest.TestCase):
    maxDiff = None

    def assert_all_coefficients_are_one(
        self,
        data: pd.DataFrame,
        key_column: str,
        expected_keys: list[str],
    ) -> None:
        self.assertFalse(data.empty, "O DataFrame de coeficientes nao deveria estar vazio.")

        actual_keys = sorted(data[key_column].dropna().astype(str).unique().tolist())
        self.assertEqual(actual_keys, sorted(expected_keys))

        non_unitary = data.loc[~data["coeff"].astype(float).sub(1.0).abs().lt(1e-9)]
        self.assertTrue(
            non_unitary.empty,
            f"Foram encontrados coeficientes diferentes de 1:\n{non_unitary.to_string(index=False)}",
        )

    def test_consumption_coefficients_can_be_forced_to_one(self) -> None:
        coefficients, expected_keys = validar_consumo_unitario()
        self.assert_all_coefficients_are_one(coefficients, "coeff_key", expected_keys)

    def test_cost_coefficients_can_be_forced_to_one(self) -> None:
        coefficients, expected_keys = validar_custo_unitario()
        self.assert_all_coefficients_are_one(coefficients, "tipo_coeff", expected_keys)


if __name__ == "__main__":
    unittest.main()
