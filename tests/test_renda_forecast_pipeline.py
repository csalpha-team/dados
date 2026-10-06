from __future__ import annotations

import unittest
from decimal import Decimal

import pandas as pd

from dados.gold.br_coeficientes_renda import preparacao_camada_renda
from dados.gold.br_coeficientes_renda.correcao_monetaria import (
    MonetaryCorrectionConfig,
    construir_fatores_correcao_ipca,
    corrigir_colunas_monetarias,
)
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

    def test_theil_sen_backcast_ancora_no_primeiro_valor_observado(self) -> None:
        data = pd.DataFrame(
            {
                "ano": [2007, 2008, 2009, 2010],
                "serie": ["A"] * 4,
                "valor": [100.0, 101.0, 105.0, 106.0],
            }
        )
        forecaster = IncomeForecaster(
            year_col="ano",
            label_cols="serie",
            value_cols="valor",
            config=ForecastConfig(method="theil_sen", clamp_non_negative=True),
        )

        result = forecaster.forecast(
            data,
            forecast_years=[2006, 2007],
            include_history=True,
        ).set_index("ano")["valor"]

        self.assertAlmostEqual(result.loc[2006], 97.75)
        self.assertAlmostEqual(result.loc[2007], 100.0)

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
    @staticmethod
    def _ipca(
        years: list[int], indices: list[float] | None = None
    ) -> pd.DataFrame:
        indices = indices or [100.0] * len(years)
        return pd.DataFrame(
            {
                "ano": years,
                "indice_ipca": indices,
                "meses_observados": [12] * len(years),
                "ano_completo": [True] * len(years),
                "serie_sgs": [433] * len(years),
            }
        )

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
            ipca_df=self._ipca([2005, 2006, 2007, 2008, 2009]),
            sector_mappings={
                "PIA_INDUSTRIA": {},
                "PAC_COMERCIO": {"ContaTeste": ["3"]},
            },
            years=[2005, 2006, 2007, 2008, 2009],
            aa_production_values={"prod_mon_trab": 0.0, "salario_medio": 0.0},
            forecast_config=ForecastConfig(method="linear", clamp_non_negative=True),
            monetary_config=MonetaryCorrectionConfig(anchor_year=2009),
        )

        self.assertEqual(
            coefficients.columns.tolist(), ["ano", "conta_alfa", "tipo_coeff", "coeff"]
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
        self.assertAlmostEqual(productivity[2006], 100.0 / 1.5)
        self.assertAlmostEqual(productivity[2007], 100.0)
        self.assertAlmostEqual(productivity[2008], 150.0)
        self.assertAlmostEqual(productivity[2009], 200.0)

        self.assertAlmostEqual(salary[2005], 5.0)
        self.assertAlmostEqual(salary[2006], 10.0 / 1.5)
        self.assertAlmostEqual(salary[2007], 10.0)
        self.assertAlmostEqual(salary[2008], 15.0)
        self.assertAlmostEqual(salary[2009], 20.0)

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
            ipca_df=self._ipca(list(range(2007, 2013))),
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
            monetary_config=MonetaryCorrectionConfig(anchor_year=2010),
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
            ipca_df=self._ipca([2007, 2008]),
            sector_mappings={
                "PIA_INDUSTRIA": {},
                "PAC_COMERCIO": {"ContaTeste": ["4"]},
            },
            years=[2007, 2008],
            aa_production_values={"prod_mon_trab": 0.0, "salario_medio": 0.0},
            forecast_config=ForecastConfig(method="linear", clamp_non_negative=True),
            monetary_config=MonetaryCorrectionConfig(anchor_year=2008),
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

    def test_correcao_ipca_preserva_emprego_e_ano_ancora(self) -> None:
        ipca = self._ipca([2020, 2021], [100.0, 110.0])
        config = MonetaryCorrectionConfig(anchor_year=2021)
        factors = construir_fatores_correcao_ipca(
            ipca, years=[2020, 2021], config=config
        )
        source = pd.DataFrame(
            {
                "ano": [2020, 2021],
                "valor": [100.0, 110.0],
                "pessoal_ocupado_31_12": [7.0, 8.0],
            }
        )

        corrected = corrigir_colunas_monetarias(
            source,
            monetary_columns=["valor"],
            factors=factors,
            config=config,
        )

        self.assertAlmostEqual(factors.loc[2020], 1.1)
        self.assertAlmostEqual(factors.loc[2021], 1.0)
        self.assertAlmostEqual(corrected.loc[0, "valor"], 110.0)
        self.assertAlmostEqual(corrected.loc[1, "valor"], 110.0)
        self.assertEqual(corrected["pessoal_ocupado_31_12"].tolist(), [7.0, 8.0])

    def test_correcao_ipca_rejeita_dupla_aplicacao(self) -> None:
        config = MonetaryCorrectionConfig(anchor_year=2021)
        factors = construir_fatores_correcao_ipca(
            self._ipca([2021]), years=[2021], config=config
        )
        corrected = corrigir_colunas_monetarias(
            pd.DataFrame({"ano": [2021], "valor": [10.0]}),
            monetary_columns=["valor"],
            factors=factors,
            config=config,
        )

        with self.assertRaisesRegex(ValueError, "ja corrigido monetariamente"):
            corrigir_colunas_monetarias(
                corrected,
                monetary_columns=["valor"],
                factors=factors,
                config=config,
            )

    def test_correcao_ipca_rejeita_ano_ausente_ou_incompleto(self) -> None:
        config = MonetaryCorrectionConfig(anchor_year=2021)
        with self.assertRaisesRegex(ValueError, "ausente"):
            construir_fatores_correcao_ipca(
                self._ipca([2021]), years=[2020, 2021], config=config
            )

        incomplete = self._ipca([2020, 2021])
        incomplete.loc[incomplete["ano"] == 2020, "ano_completo"] = False
        incomplete.loc[incomplete["ano"] == 2020, "meses_observados"] = 11
        with self.assertRaisesRegex(ValueError, "incompleto"):
            construir_fatores_correcao_ipca(
                incomplete, years=[2020, 2021], config=config
            )

    def test_aa_producao_e_corrigida_e_ancora_permanece_inalterada(self) -> None:
        coefficients = preparar_dados_coeficientes_renda(
            pia_df=self._empty_pia(),
            pac_df=pd.DataFrame(
                columns=[
                    "ano",
                    "divisao_grupo_cnae_2",
                    "valor_receita_bruta_revenda",
                    "pessoal_ocupado_31_12",
                    "margem_comercializacao",
                    "valor_gastos_salarios_remuneracoes",
                ]
            ),
            ipca_df=self._ipca([2020, 2021], [100.0, 110.0]),
            sector_mappings={"PIA_INDUSTRIA": {}, "PAC_COMERCIO": {}},
            years=[2020, 2021],
            aa_production_values={"prod_mon_trab": 10.0, "salario_medio": 2.0},
            forecast_config=ForecastConfig(max_annual_growth_rate=0.5),
            monetary_config=MonetaryCorrectionConfig(anchor_year=2021),
        )
        aa_productivity = coefficients.loc[
            (coefficients["conta_alfa"] == "AAProdução")
            & (coefficients["tipo_coeff"] == "prod_mon_trab")
        ].set_index("ano")["coeff"]

        self.assertAlmostEqual(aa_productivity.loc[2020], 11.0)
        self.assertAlmostEqual(aa_productivity.loc[2021], 10.0)

    def test_serie_real_fica_continua_na_transicao_do_backcast(self) -> None:
        pac_df = pd.DataFrame(
            {
                "ano": [2007, 2008, 2009],
                "divisao_grupo_cnae_2": ["3. Comércio por atacado"] * 3,
                "valor_receita_bruta_revenda": [1000.0, 1100.0, 1210.0],
                "pessoal_ocupado_31_12": [10.0, 10.0, 10.0],
                "margem_comercializacao": [100.0, 110.0, 121.0],
                "valor_gastos_salarios_remuneracoes": [500.0, 550.0, 605.0],
            }
        )
        coefficients = preparar_dados_coeficientes_renda(
            pia_df=self._empty_pia(),
            pac_df=pac_df,
            ipca_df=self._ipca(
                [2006, 2007, 2008, 2009], [90.0, 100.0, 110.0, 121.0]
            ),
            sector_mappings={
                "PIA_INDUSTRIA": {},
                "PAC_COMERCIO": {"ContaTeste": ["3"]},
            },
            years=[2006, 2007, 2008, 2009],
            aa_production_values={"prod_mon_trab": 0.0, "salario_medio": 0.0},
            forecast_config=ForecastConfig(
                method="theil_sen", max_annual_growth_rate=0.5
            ),
            monetary_config=MonetaryCorrectionConfig(anchor_year=2009),
        )
        productivity = coefficients.loc[
            (coefficients["conta_alfa"] == "ContaTeste")
            & (coefficients["tipo_coeff"] == "prod_mon_trab")
        ].set_index("ano")["coeff"]

        for value in productivity:
            self.assertAlmostEqual(value, 121.0)

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
            MonetaryCorrectionConfig(anchor_year=2021),
        )

        ipca_df = RendaPreparationTests._ipca([2020, 2021])
        transformed = preparacao_camada_renda.transform(
            (pia_df, pac_df, ipca_df, params)
        )
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


if __name__ == "__main__":
    unittest.main()
