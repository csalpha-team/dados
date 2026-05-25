from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import pandas as pd

from dados.gold.br_coeficientes_renda.auditoria_forecast import (
    ForecastAuditConfig,
    auditar_variacao_temporal_forecast,
    exportar_auditoria_forecast,
)
from dados.gold.br_coeficientes_renda.previsao_renda import (
    ForecastConfig,
    IncomeForecaster,
)
from dados.gold.br_coeficientes_renda.utils import preparar_dados_coeficientes_renda


class IncomeForecasterTests(unittest.TestCase):
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
    def test_preparacao_retroprojeta_coeficientes_com_cagr_ancorado(self) -> None:
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
        self.assertAlmostEqual(productivity[2006], 70.71067811865476)
        self.assertAlmostEqual(productivity[2007], 100.0)
        self.assertAlmostEqual(productivity[2008], 150.0)
        self.assertAlmostEqual(productivity[2009], 200.0)

        self.assertAlmostEqual(salary[2005], 5.0)
        self.assertAlmostEqual(salary[2006], 7.0710678118654755)
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


class RendaForecastAuditTests(unittest.TestCase):
    def test_auditoria_aprova_serie_cagr_sem_discrepancia(self) -> None:
        coefficients = pd.DataFrame(
            {
                "ano": [2005, 2006, 2007, 2008, 2009],
                "conta_alfa": ["ContaTeste"] * 5,
                "tipo_coeff": ["prod_mon_trab"] * 5,
                "coeff": [100.0, 110.0, 121.0, 133.1, 146.41],
            }
        )

        audit = auditar_variacao_temporal_forecast(coefficients)

        self.assertTrue(
            audit.empty,
            "Nao eram esperadas discrepancias para CAGR constante:\n"
            f"{audit.to_string(index=False)}",
        )

    def test_auditoria_exporta_csv_para_variacao_abrupta(self) -> None:
        coefficients = pd.DataFrame(
            {
                "ano": [2005, 2006, 2007, 2008, 2009],
                "conta_alfa": ["ContaTeste"] * 5,
                "tipo_coeff": ["prod_mon_trab"] * 5,
                "coeff": [100.0, 105.0, 110.0, 500.0, 520.0],
            }
        )
        config = ForecastAuditConfig(min_relative_tolerance=0.15)

        audit = auditar_variacao_temporal_forecast(coefficients, config=config)

        self.assertFalse(audit.empty)
        discrepancia = audit.iloc[0]
        self.assertEqual(discrepancia["conta_alfa"], "ContaTeste")
        self.assertEqual(discrepancia["tipo_coeff"], "prod_mon_trab")
        self.assertEqual(discrepancia["ano"], 2008)
        self.assertEqual(discrepancia["tipo_discrepancia"], "acima_banda_superior")
        self.assertGreater(float(discrepancia["discrepancia_log"]), 0.0)
        self.assertIn("possivel_motivo", audit.columns)

        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "auditoria_forecast_renda.csv"
            exported_path = exportar_auditoria_forecast(audit, output_path, config=config)
            exported = pd.read_csv(exported_path)

        self.assertEqual(exported_path, output_path)
        self.assertEqual(exported.loc[0, "conta_alfa"], "ContaTeste")
        self.assertEqual(exported.loc[0, "ano"], 2008)
        self.assertEqual(exported.loc[0, "tipo_discrepancia"], "acima_banda_superior")

    def test_auditoria_reprova_coeficientes_zero_ou_negativos(self) -> None:
        coefficients = pd.DataFrame(
            {
                "ano": [2007, 2008, 2009],
                "conta_alfa": ["ContaTeste"] * 3,
                "tipo_coeff": ["salario_medio"] * 3,
                "coeff": [10.0, 0.0, -1.0],
            }
        )

        audit = auditar_variacao_temporal_forecast(coefficients)

        self.assertEqual(audit.shape[0], 2)
        self.assertTrue((audit["tipo_discrepancia"] == "valor_nao_positivo").all())
        self.assertEqual(audit["ano"].tolist(), [2008, 2009])


if __name__ == "__main__":
    unittest.main()
