from __future__ import annotations

import unittest

import pandas as pd

from dados.gold.br_coeficientes_consumo.testing_utils import (
    validar_cenario_coeficientes_unitarios as validar_consumo_unitario,
    validar_cenario_soma_coeficientes_unitaria as validar_consumo_normalizado,
)
from dados.gold.pa_coeficientes_custo.testing_utils import (
    validar_cenario_coeficientes_unitarios as validar_custo_unitario,
    validar_cenario_soma_coeficientes_unitaria as validar_custo_normalizado,
)


class CostConsumptionCoefficientTests(unittest.TestCase):
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

    def assert_coefficients_are_unitary(self, data: pd.DataFrame) -> None:
        coefficients = pd.to_numeric(data["coeff"], errors="raise")
        self.assertTrue(
            coefficients.eq(1.0).all(),
            f"Foram encontrados coeficientes diferentes de 1:\n{data.loc[~coefficients.eq(1.0)].to_string(index=False)}",
        )

    def assert_coefficients_are_non_negative(self, data: pd.DataFrame) -> None:
        coefficients = pd.to_numeric(data["coeff"], errors="raise")
        self.assertTrue(
            coefficients.ge(0).all(),
            f"Foram encontrados coeficientes negativos:\n{data.loc[coefficients.lt(0)].to_string(index=False)}",
        )

    def test_consumption_coefficients_follow_contract(self) -> None:
        coefficients, expected_keys = validar_consumo_unitario()

        self.assertEqual(coefficients.columns.tolist(), ["ano", "coeff_key", "coeff"])
        self.assert_expected_keys(coefficients, "coeff_key", expected_keys)
        self.assert_coefficients_are_unitary(coefficients)

    def test_consumption_coefficients_can_sum_to_one(self) -> None:
        coefficients, expected_keys = validar_consumo_normalizado()

        self.assert_expected_keys(coefficients, "coeff_key", expected_keys)
        self.assert_coefficients_are_non_negative(coefficients)
        self.assertAlmostEqual(
            pd.to_numeric(coefficients["coeff"], errors="raise").sum(),
            1.0,
            places=12,
        )

    def test_cost_coefficients_follow_contract(self) -> None:
        coefficients, expected_keys = validar_custo_unitario()

        self.assertEqual(
            coefficients.columns.tolist(),
            ["ano", "nome_regiao_integracao", "tipo_coeff", "coeff"],
        )
        self.assert_expected_keys(coefficients, "tipo_coeff", expected_keys)
        self.assert_coefficients_are_unitary(coefficients)

    def test_cost_coefficients_can_sum_to_one_by_expense_item(self) -> None:
        grouped, coefficients, expected_expenses, expected_keys = (
            validar_custo_normalizado()
        )

        grouped_expenses = sorted(
            grouped.loc[
                grouped["tipo_despesa"].ne("Total"),
                "tipo_despesa",
            ]
            .astype(str)
            .unique()
            .tolist()
        )
        self.assertEqual(grouped_expenses, expected_expenses)
        self.assert_expected_keys(coefficients, "tipo_coeff", expected_keys)
        self.assert_coefficients_are_non_negative(coefficients)

        grouped_coefficients = grouped.loc[grouped["tipo_despesa"].ne("Total")].copy()
        self.assertAlmostEqual(
            pd.to_numeric(grouped_coefficients["coeff"], errors="raise").sum(),
            1.0,
            places=12,
        )


if __name__ == "__main__":
    unittest.main()
