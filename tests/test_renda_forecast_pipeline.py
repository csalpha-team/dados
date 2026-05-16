from __future__ import annotations

import unittest

import pandas as pd

from dados.gold.br_coeficientes_renda.previsao_renda import ForecastConfig, IncomeForecaster
from dados.gold.br_coeficientes_renda.utils import preparar_dados_coeficientes_renda


class IncomeForecasterTests(unittest.TestCase):
    def test_linear_backcast_repeats_last_positive_projection_after_crossing_zero(self) -> None:
        data = pd.DataFrame(
            {
                "ano": [2007, 2008, 2009],
                "serie": ["A", "A", "A"],
                "valor": [100.0, 150.0, 200.0],
            }
        )

        forecaster = IncomeForecaster(
            year_col="ano",
            label_cols="serie",
            value_cols="valor",
            config=ForecastConfig(method="linear", clamp_non_negative=True),
        )

        result = forecaster.forecast(
            data,
            forecast_years=[2005, 2006, 2007, 2008, 2009],
            include_history=True,
        )
        result = result.set_index("ano")["valor"].to_dict()

        self.assertAlmostEqual(result[2006], 50.0)
        self.assertAlmostEqual(result[2005], 50.0)
        self.assertAlmostEqual(result[2007], 100.0)
        self.assertAlmostEqual(result[2008], 150.0)
        self.assertAlmostEqual(result[2009], 200.0)


class RendaPreparationTests(unittest.TestCase):
    def test_preparacao_usa_forecaster_nas_variaveis_brutas(self) -> None:
        pia_df = pd.DataFrame(
            columns=[
                "ano",
                "divisao_grupo_cnae_2",
                "pessoal_ocupado_31_12",
                "valor_bruto_producao_industrial",
                "valor_salarios_remuneracoes",
            ]
        )

        pac_df = pd.DataFrame(
            {
                "ano": [2007, 2008, 2009],
                "divisao_grupo_cnae_2": ["3. Comércio por atacado"] * 3,
                "valor_receita_bruta_revenda": [1000.0, 1500.0, 2000.0],
                "pessoal_ocupado_31_12": [10.0, 10.0, 10.0],
                "margem_comercializacao": [100.0, 120.0, 140.0],
                "valor_gastos_salarios_remuneracoes": [100.0, 150.0, 200.0],
            }
        )

        coefficients = preparar_dados_coeficientes_renda(
            pia_df=pia_df,
            pac_df=pac_df,
            sector_mappings={
                "PIA_INDUSTRIA": {},
                "PAC_COMERCIO": {"ContaTeste": ["3"]},
            },
            years=[2005, 2006, 2007, 2008, 2009],
            aa_production_values={"prod_mon_trab": 0.0, "salario_medio": 0.0},
            forecast_config=ForecastConfig(method="linear", clamp_non_negative=True),
        )

        conta_teste = coefficients.loc[coefficients["conta_alfa"] == "ContaTeste"].copy()
        productivity = (
            conta_teste.loc[conta_teste["tipo_coeff"] == "prod_mon_trab", ["ano", "coeff"]]
            .set_index("ano")["coeff"]
            .to_dict()
        )
        salary = (
            conta_teste.loc[conta_teste["tipo_coeff"] == "salario_medio", ["ano", "coeff"]]
            .set_index("ano")["coeff"]
            .to_dict()
        )

        self.assertAlmostEqual(productivity[2005], 50.0)
        self.assertAlmostEqual(productivity[2006], 50.0)
        self.assertAlmostEqual(productivity[2007], 100.0)
        self.assertAlmostEqual(productivity[2008], 150.0)
        self.assertAlmostEqual(productivity[2009], 200.0)

        self.assertAlmostEqual(salary[2005], 5.0)
        self.assertAlmostEqual(salary[2006], 5.0)
        self.assertAlmostEqual(salary[2007], 10.0)
        self.assertAlmostEqual(salary[2008], 15.0)
        self.assertAlmostEqual(salary[2009], 20.0)

    def test_preparacao_preserva_anos_observados_com_multiplas_ufs(self) -> None:
        pia_df = pd.DataFrame(
            columns=[
                "ano",
                "divisao_grupo_cnae_2",
                "pessoal_ocupado_31_12",
                "valor_bruto_producao_industrial",
                "valor_salarios_remuneracoes",
            ]
        )

        pac_df = pd.DataFrame(
            {
                "ano": [2007, 2007, 2008, 2008],
                "unidade_geografica": ["Acre", "Bahia", "Acre", "Bahia"],
                "divisao_grupo_cnae_2": ["4. Comércio varejista"] * 4,
                "valor_receita_bruta_revenda": [100.0, 300.0, 120.0, 330.0],
                "pessoal_ocupado_31_12": [10.0, 30.0, 12.0, 33.0],
                "margem_comercializacao": [10.0, 30.0, 12.0, 33.0],
                "valor_gastos_salarios_remuneracoes": [50.0, 150.0, 72.0, 198.0],
            }
        )

        coefficients = preparar_dados_coeficientes_renda(
            pia_df=pia_df,
            pac_df=pac_df,
            sector_mappings={
                "PIA_INDUSTRIA": {},
                "PAC_COMERCIO": {"ContaTeste": ["4"]},
            },
            years=[2007, 2008],
            aa_production_values={"prod_mon_trab": 0.0, "salario_medio": 0.0},
            forecast_config=ForecastConfig(method="linear", clamp_non_negative=True),
        )

        conta_teste = coefficients.loc[coefficients["conta_alfa"] == "ContaTeste"].copy()
        productivity = (
            conta_teste.loc[conta_teste["tipo_coeff"] == "prod_mon_trab", ["ano", "coeff"]]
            .set_index("ano")["coeff"]
            .to_dict()
        )
        salary = (
            conta_teste.loc[conta_teste["tipo_coeff"] == "salario_medio", ["ano", "coeff"]]
            .set_index("ano")["coeff"]
            .to_dict()
        )

        self.assertAlmostEqual(productivity[2007], 10.0)
        self.assertAlmostEqual(productivity[2008], 10.0)
        self.assertAlmostEqual(salary[2007], 5.0)
        self.assertAlmostEqual(salary[2008], 6.0)


if __name__ == "__main__":
    unittest.main()
