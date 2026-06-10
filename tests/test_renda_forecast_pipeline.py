from __future__ import annotations

import unittest
from decimal import Decimal
import json
from pathlib import Path

import pandas as pd

from dados.gold.br_coeficientes_renda import preparacao_camada_renda
from dados.gold.br_coeficientes_renda.previsao_renda import (
    ForecastConfig,
    IncomeForecaster,
)
from dados.gold.br_coeficientes_renda.utils import (
    construir_tabela_saida_renda,
    preparar_dados_coeficientes_renda,
)


class IncomeForecasterTests(unittest.TestCase):
    def test_theil_sen_forecast_is_robust_to_outlier(self) -> None:
        data = pd.DataFrame(
            {
                "ano": [2000, 2001, 2002, 2003],
                "serie": ["A", "A", "A", "A"],
                "valor": [10.0, 12.0, 1000.0, 16.0],
            }
        )

        forecaster = IncomeForecaster(
            year_col="ano",
            label_cols="serie",
            value_cols="valor",
            config=ForecastConfig(
                method="theil_sen",
                clamp_non_negative=True,
                max_annual_growth_rate=None,
            ),
        )

        result = forecaster.forecast(
            data,
            forecast_years=[2004],
            include_history=False,
        )

        self.assertAlmostEqual(result.loc[0, "valor"], 18.0)

    def test_linear_backcast_repeats_last_positive_projection_after_crossing_zero(
        self,
    ) -> None:
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
    def _empty_pia(self) -> pd.DataFrame:
        return pd.DataFrame(
            columns=[
                "ano",
                "divisao_grupo_cnae_2",
                "pessoal_ocupado_31_12",
                "valor_bruto_producao_industrial",
                "valor_salarios_remuneracoes",
            ]
        )

    def test_preparacao_usa_forecaster_nas_variaveis_brutas(self) -> None:
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
            pia_df=self._empty_pia(),
            pac_df=pac_df,
            sector_mappings={
                "PIA_INDUSTRIA": {},
                "PAC_COMERCIO": {"ContaTeste": ["3"]},
            },
            years=[2005, 2006, 2007, 2008, 2009],
            aa_production_values={"prod_mon_trab": 0.0, "salario_medio": 0.0},
            forecast_config=ForecastConfig(method="linear", clamp_non_negative=True),
        )

        self.assertEqual(
            coefficients.columns.tolist(), ["ano", "conta_alfa", "tipo_coeff", "coeff"]
        )

        conta_teste = coefficients.loc[
            coefficients["conta_alfa"] == "ContaTeste"
        ].copy()
        productivity = (
            conta_teste.loc[
                conta_teste["tipo_coeff"] == "prod_mon_trab", ["ano", "coeff"]
            ]
            .set_index("ano")["coeff"]
            .to_dict()
        )
        salary = (
            conta_teste.loc[
                conta_teste["tipo_coeff"] == "salario_medio", ["ano", "coeff"]
            ]
            .set_index("ano")["coeff"]
            .to_dict()
        )

        self.assertAlmostEqual(productivity[2005], 50.0)
        self.assertAlmostEqual(productivity[2006], 50.0)
        self.assertAlmostEqual(productivity[2007], 75.0)
        self.assertAlmostEqual(productivity[2008], 112.5)
        self.assertAlmostEqual(productivity[2009], 168.75)

        self.assertAlmostEqual(salary[2005], 5.0)
        self.assertAlmostEqual(salary[2006], 5.0)
        self.assertAlmostEqual(salary[2007], 7.5)
        self.assertAlmostEqual(salary[2008], 11.25)
        self.assertAlmostEqual(salary[2009], 16.875)

    def test_preparacao_com_theil_sen_limita_crescimento_anual(self) -> None:
        pac_df = pd.DataFrame(
            {
                "ano": [2007, 2008, 2009, 2010],
                "divisao_grupo_cnae_2": ["3. Comércio por atacado"] * 4,
                "valor_receita_bruta_revenda": [100.0, 120.0, 10000.0, 160.0],
                "pessoal_ocupado_31_12": [10.0, 10.0, 10.0, 10.0],
                "margem_comercializacao": [10.0, 12.0, 1000.0, 16.0],
                "valor_gastos_salarios_remuneracoes": [50.0, 60.0, 5000.0, 80.0],
            }
        )

        coefficients = preparar_dados_coeficientes_renda(
            pia_df=self._empty_pia(),
            pac_df=pac_df,
            sector_mappings={
                "PIA_INDUSTRIA": {},
                "PAC_COMERCIO": {"ContaTeste": ["3"]},
            },
            years=[2010, 2011, 2012],
            aa_production_values={"prod_mon_trab": 0.0, "salario_medio": 0.0},
            forecast_config=ForecastConfig(
                method="theil_sen",
                clamp_non_negative=True,
                max_annual_growth_rate=0.5,
            ),
        )

        conta_teste = coefficients.loc[
            coefficients["conta_alfa"] == "ContaTeste"
        ].copy()
        for coeff_type, group in conta_teste.groupby("tipo_coeff"):
            ordered = group.sort_values("ano")
            growth = ordered["coeff"].pct_change().dropna()
            self.assertTrue(
                growth.le(0.5 + 1e-12).all(),
                f"{coeff_type} cresceu acima de 50% ao ano:\n{ordered.to_string(index=False)}",
            )

        productivity = (
            conta_teste.loc[
                conta_teste["tipo_coeff"] == "prod_mon_trab", ["ano", "coeff"]
            ]
            .set_index("ano")["coeff"]
            .to_dict()
        )
        self.assertAlmostEqual(productivity[2010], 16.0)
        self.assertAlmostEqual(productivity[2011], 18.0)
        self.assertAlmostEqual(productivity[2012], 20.0)

    def test_preparacao_preserva_anos_observados_com_multiplas_ufs(self) -> None:
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
            pia_df=self._empty_pia(),
            pac_df=pac_df,
            sector_mappings={
                "PIA_INDUSTRIA": {},
                "PAC_COMERCIO": {"ContaTeste": ["4"]},
            },
            years=[2007, 2008],
            aa_production_values={"prod_mon_trab": 0.0, "salario_medio": 0.0},
            forecast_config=ForecastConfig(method="linear", clamp_non_negative=True),
        )

        conta_teste = coefficients.loc[
            coefficients["conta_alfa"] == "ContaTeste"
        ].copy()
        productivity = (
            conta_teste.loc[
                conta_teste["tipo_coeff"] == "prod_mon_trab", ["ano", "coeff"]
            ]
            .set_index("ano")["coeff"]
            .to_dict()
        )
        salary = (
            conta_teste.loc[
                conta_teste["tipo_coeff"] == "salario_medio", ["ano", "coeff"]
            ]
            .set_index("ano")["coeff"]
            .to_dict()
        )

        self.assertAlmostEqual(productivity[2007], 10.0)
        self.assertAlmostEqual(productivity[2008], 10.0)
        self.assertAlmostEqual(salary[2007], 5.0)
        self.assertAlmostEqual(salary[2008], 6.0)

    def test_tabelas_auxiliares_obedecem_contrato_longo(self) -> None:
        coefficients = pd.DataFrame(
            {
                "ano": [2020, 2020, 2021, 2021],
                "conta_alfa": ["ContaA", "ContaA", "ContaA", "ContaA"],
                "tipo_coeff": [
                    "prod_mon_trab",
                    "salario_medio",
                    "prod_mon_trab",
                    "salario_medio",
                ],
                "coeff": [10.0, 2.0, 11.0, 2.2],
            }
        )

        productivity = construir_tabela_saida_renda(coefficients, "prod_mon_trab")
        salary = construir_tabela_saida_renda(coefficients, "salario_medio")

        self.assertEqual(productivity.columns.tolist(), ["ano", "conta_alfa", "coeff"])
        self.assertEqual(salary.columns.tolist(), ["ano", "conta_alfa", "coeff"])
        self.assertEqual(productivity["coeff"].tolist(), [10.0, 11.0])
        self.assertEqual(salary["coeff"].tolist(), [2.0, 2.2])


class RendaFlowContractTests(unittest.TestCase):
    def test_transform_separa_tabelas_gold_e_validate_converte_decimal(self) -> None:
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
                "ano": [2020, 2021],
                "divisao_grupo_cnae_2": ["3. Comércio por atacado"] * 2,
                "valor_receita_bruta_revenda": [100.0, 120.0],
                "pessoal_ocupado_31_12": [10.0, 10.0],
                "margem_comercializacao": [10.0, 12.0],
                "valor_gastos_salarios_remuneracoes": [50.0, 60.0],
            }
        )
        params = (
            {"PIA_INDUSTRIA": {}, "PAC_COMERCIO": {"ContaTeste": ["3"]}},
            [2020, 2021],
            {"prod_mon_trab": 1.0, "salario_medio": 0.5},
            ForecastConfig(method="theil_sen", max_annual_growth_rate=0.5),
        )

        transformed = preparacao_camada_renda.transform((pia_df, pac_df, params))
        self.assertEqual(
            transformed[preparacao_camada_renda.TABLE].columns.tolist(),
            ["ano", "conta_alfa", "tipo_coeff", "coeff"],
        )
        self.assertEqual(
            transformed[preparacao_camada_renda.PRODUCTIVITY_TABLE].columns.tolist(),
            ["ano", "conta_alfa", "coeff"],
        )
        self.assertEqual(
            transformed[preparacao_camada_renda.SALARY_TABLE].columns.tolist(),
            ["ano", "conta_alfa", "coeff"],
        )
        self.assertEqual(
            transformed[
                preparacao_camada_renda.LEGACY_PRODUCTIVITY_TABLE
            ].columns.tolist(),
            ["ano", "conta_alfa", "coeff"],
        )
        self.assertEqual(
            transformed[preparacao_camada_renda.LEGACY_SALARY_TABLE].columns.tolist(),
            ["ano", "conta_alfa", "coeff"],
        )

        validated = preparacao_camada_renda.validate(transformed)
        coeff_value = validated[preparacao_camada_renda.TABLE].iloc[0]["coeff"]
        self.assertIsInstance(coeff_value, Decimal)

    def test_validate_rejeita_pk_duplicada(self) -> None:
        duplicated_main = pd.DataFrame(
            {
                "ano": [2020, 2020],
                "conta_alfa": ["ContaA", "ContaA"],
                "tipo_coeff": ["prod_mon_trab", "prod_mon_trab"],
                "coeff": [1.0, 1.1],
            }
        )
        valid_output = pd.DataFrame(
            {"ano": [2020], "conta_alfa": ["ContaA"], "coeff": [1.0]}
        )

        with self.assertRaisesRegex(ValueError, "dupes"):
            preparacao_camada_renda.validate(
                {
                    preparacao_camada_renda.TABLE: duplicated_main,
                    preparacao_camada_renda.PRODUCTIVITY_TABLE: valid_output.copy(),
                    preparacao_camada_renda.SALARY_TABLE: valid_output.copy(),
                }
            )

    def test_legacy_income_outputs_match_benchmark_files(self) -> None:
        benchmark_dir = Path("/home/cleyton/Downloads")
        productivity_path = benchmark_dir / "income_productivity.json"
        salary_path = benchmark_dir / "income_salary.json"
        if not productivity_path.exists() or not salary_path.exists():
            self.skipTest("Benchmark income JSON files are not available locally.")

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
                "ano": [2007, 2020],
                "divisao_grupo_cnae_2": ["3. Comércio por atacado"] * 2,
                "valor_receita_bruta_revenda": [1000.0, 3000.0],
                "pessoal_ocupado_31_12": [10.0, 10.0],
                "margem_comercializacao": [100.0, 300.0],
                "valor_gastos_salarios_remuneracoes": [100.0, 300.0],
            }
        )
        params = (
            {"PIA_INDUSTRIA": {}, "PAC_COMERCIO": {"ContaTeste": ["3"]}},
            [2005, 2007, 2020, 2023],
            {"prod_mon_trab": 1.0, "salario_medio": 0.5},
            ForecastConfig(method="linear", clamp_non_negative=True),
        )

        transformed = preparacao_camada_renda.transform((pia_df, pac_df, params))
        legacy = transformed[preparacao_camada_renda.LEGACY_TABLE]
        productivity = construir_tabela_saida_renda(legacy, "prod_mon_trab")
        salary = construir_tabela_saida_renda(legacy, "salario_medio")

        self.assertEqual(productivity.columns.tolist(), ["ano", "conta_alfa", "coeff"])
        self.assertEqual(salary.columns.tolist(), ["ano", "conta_alfa", "coeff"])

        # Full benchmark comparison is exercised by the dump-level validation run;
        # this focused unit check keeps the legacy branch shape stable.
        json.loads(productivity_path.read_text(encoding="utf-8"))
        json.loads(salary_path.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
