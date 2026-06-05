from __future__ import annotations

import unittest

import pandas as pd

from dados.gold.br_coeficientes_consumo.testing_utils import (
    validar_cenario_valores_consumo,
)
from dados.gold.pa_coeficientes_custo.testing_utils import (
    validar_cenario_valores_custo,
)


class CostConsumptionValueTests(unittest.TestCase):
    maxDiff = None

    def assert_expected_keys(
        self,
        data: pd.DataFrame,
        key_column: str,
        expected_keys: list[str],
    ) -> None:
        self.assertFalse(data.empty, "O DataFrame de valores nao deveria estar vazio.")

        actual_keys = sorted(data[key_column].dropna().astype(str).unique().tolist())
        self.assertEqual(actual_keys, sorted(expected_keys))

    def assert_values_match(
        self,
        data: pd.DataFrame,
        key_column: str,
        expected_values: pd.Series,
    ) -> None:
        values = (
            data.assign(valor=pd.to_numeric(data["valor"], errors="raise"))
            .groupby(key_column)["valor"]
            .sum()
            .sort_index()
        )
        pd.testing.assert_series_equal(
            values,
            expected_values.sort_index(),
            check_names=False,
            check_dtype=False,
        )

    def assert_values_are_non_negative(self, data: pd.DataFrame) -> None:
        values = pd.to_numeric(data["valor"], errors="raise")
        self.assertTrue(
            values.ge(0).all(),
            f"Foram encontrados valores negativos:\n{data.loc[values.lt(0)].to_string(index=False)}",
        )

    def test_consumption_preserves_observed_monetary_values(self) -> None:
        values, expected_values, expected_keys = validar_cenario_valores_consumo()

        self.assert_expected_keys(values, "coeff_key", expected_keys)
        self.assert_values_are_non_negative(values)
        self.assert_values_match(values, "coeff_key", expected_values)

    def test_cost_preserves_observed_monetary_values_by_mapped_item(self) -> None:
        grouped_values, final_values, expected_values, expected_keys = (
            validar_cenario_valores_custo()
        )

        self.assertFalse(
            grouped_values["tipo_despesa"].eq("Total").any(),
            "A despesa Total nao deve ser exportada como item de custo.",
        )
        self.assert_expected_keys(final_values, "tipo_coeff", expected_keys)
        self.assert_values_are_non_negative(final_values)
        self.assert_values_match(final_values, "tipo_coeff", expected_values)


if __name__ == "__main__":
    unittest.main()
