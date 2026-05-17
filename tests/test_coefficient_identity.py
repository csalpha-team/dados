from __future__ import annotations

import unittest

import pandas as pd

from dados.gold.br_coeficientes_consumo.testing_utils import (
    validar_cenario_coeficientes_unitarios as validar_consumo_unitario,
    validar_cenario_soma_coeficientes_unitaria as validar_soma_consumo,
)
from dados.gold.pa_coeficientes_custo.testing_utils import (
    validar_cenario_coeficientes_unitarios as validar_custo_unitario,
    validar_cenario_soma_coeficientes_unitaria as validar_soma_custo,
)


class CoefficientIdentityTests(unittest.TestCase):
    maxDiff = None

    def assert_expected_keys(
        self,
        data: pd.DataFrame,
        key_column: str,
        expected_keys: list[str],
    ) -> None:
        self.assertFalse(
            data.empty, "O DataFrame de coeficientes nao deveria estar vazio."
        )

        actual_keys = sorted(data[key_column].dropna().astype(str).unique().tolist())
        self.assertEqual(actual_keys, sorted(expected_keys))

    def assert_all_coefficients_are_one(
        self,
        data: pd.DataFrame,
        key_column: str,
        expected_keys: list[str],
    ) -> None:
        self.assert_expected_keys(data, key_column, expected_keys)

        non_unitary = data.loc[~data["coeff"].astype(float).sub(1.0).abs().lt(1e-9)]
        self.assertTrue(
            non_unitary.empty,
            f"Foram encontrados coeficientes diferentes de 1:\n{non_unitary.to_string(index=False)}",
        )

    def assert_coefficients_sum_to_one(
        self,
        data: pd.DataFrame,
        *,
        delta: float = 1e-9,
    ) -> None:
        self.assertFalse(
            data.empty, "O DataFrame de coeficientes nao deveria estar vazio."
        )

        coefficients = pd.to_numeric(data["coeff"], errors="raise")
        self.assertTrue(
            coefficients.ge(0).all(),
            f"Foram encontrados coeficientes negativos:\n{data.loc[coefficients.lt(0)].to_string(index=False)}",
        )
        self.assertAlmostEqual(float(coefficients.sum()), 1.0, delta=delta)

    def test_consumption_coefficients_can_be_forced_to_one(self) -> None:
        coefficients, expected_keys = validar_consumo_unitario()
        self.assert_all_coefficients_are_one(coefficients, "coeff_key", expected_keys)

    def test_cost_coefficients_can_be_forced_to_one(self) -> None:
        coefficients, expected_keys = validar_custo_unitario()
        self.assert_all_coefficients_are_one(coefficients, "tipo_coeff", expected_keys)

    def test_consumption_coefficients_sum_to_one_in_balanced_scenario(self) -> None:
        coefficients, expected_keys = validar_soma_consumo()
        self.assert_expected_keys(coefficients, "coeff_key", expected_keys)
        self.assert_coefficients_sum_to_one(coefficients)

    def test_cost_base_coefficients_sum_to_one_in_balanced_scenario(self) -> None:
        (
            grouped_coefficients,
            final_coefficients,
            expected_expense_types,
            expected_keys,
        ) = validar_soma_custo()
        mapped_coefficients = grouped_coefficients[
            grouped_coefficients["tipo_despesa"].isin(expected_expense_types)
        ].copy()

        self.assert_expected_keys(
            mapped_coefficients, "tipo_despesa", expected_expense_types
        )
        self.assert_coefficients_sum_to_one(mapped_coefficients)
        self.assert_expected_keys(final_coefficients, "tipo_coeff", expected_keys)


if __name__ == "__main__":
    unittest.main()
