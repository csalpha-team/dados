from __future__ import annotations

from pathlib import Path

import pandas as pd

from dados.gold.br_coeficientes_renda.utils import (
    carregar_parametros_renda,
    construir_tabela_saida_renda,
    preparar_dados_coeficientes_renda,
)


DEFAULT_CONFIG_PATH = Path(__file__).with_name("parametros_coeficientes_renda.json")
AA_ACCOUNT = "AAProdu\u00e7\u00e3o"


def _construir_pia_sintetica(mapping: dict[str, list[str]]) -> pd.DataFrame:
    prefixes = sorted({str(prefix).strip() for values in mapping.values() for prefix in values})
    return pd.DataFrame(
        [
            {
                "ano": 2022,
                "divisao_grupo_cnae_2": prefix,
                "pessoal_ocupado_31_12": 1.0,
                "valor_bruto_producao_industrial": 1.0,
                "valor_salarios_remuneracoes": 1.0,
            }
            for prefix in prefixes
        ]
    )


def _construir_pac_sintetica(mapping: dict[str, list[str]]) -> pd.DataFrame:
    prefixes = sorted({str(prefix).strip() for values in mapping.values() for prefix in values})
    return pd.DataFrame(
        [
            {
                "ano": 2022,
                "divisao_grupo_cnae_2": prefix,
                "valor_receita_bruta_revenda": 1.0,
                "pessoal_ocupado_31_12": 1.0,
                "margem_comercializacao": 1.0,
                "valor_gastos_salarios_remuneracoes": 1.0,
            }
            for prefix in prefixes
        ]
    )


def validar_cenario_coeficientes_unitarios(
    config_path: Path = DEFAULT_CONFIG_PATH,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, list[str]]:
    sector_mappings, _years, _aa_values, forecast_config = carregar_parametros_renda(config_path)

    pia_mapping = sector_mappings.get("PIA_INDUSTRIA", {})
    pac_mapping = sector_mappings.get("PAC_COMERCIO", {})
    pia_df = _construir_pia_sintetica(pia_mapping)
    pac_df = _construir_pac_sintetica(pac_mapping)

    coefficients = preparar_dados_coeficientes_renda(
        pia_df,
        pac_df,
        sector_mappings=sector_mappings,
        years=[2022],
        aa_production_values={
            "prod_mon_trab": 1.0,
            "salario_medio": 1.0,
        },
        forecast_config=forecast_config,
    )

    productivity_output = construir_tabela_saida_renda(coefficients, "prod_mon_trab")
    salary_output = construir_tabela_saida_renda(coefficients, "salario_medio")

    expected_accounts = sorted(
        set(pia_mapping)
        | set(pac_mapping)
        | {AA_ACCOUNT}
    )
    return coefficients, productivity_output, salary_output, expected_accounts
