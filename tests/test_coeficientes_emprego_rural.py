from decimal import Decimal

import pandas as pd

from dados.gold.pa_coeficientes_emprego_rural.censo_2017_grupo_atividade import (
    transform,
)


def test_transform_calculates_coefficients_without_merging_crop_groups() -> None:
    key_fields = {
        "ano": [2017, 2017],
        "id_municipio": ["1500107", "1500107"],
        "grupo_atividade_economica": [
            "Produção de lavouras temporárias",
            "Produção de lavouras permanentes",
        ],
    }
    pessoal_ocupado = pd.DataFrame(
        {
            **key_fields,
            "nome": ["Abaetetuba", "Abaetetuba"],
            "nome_regiao_integracao": ["Tocantins", "Tocantins"],
            "sigla_uf": ["PA", "PA"],
            "pessoal_ocupado": [Decimal("20"), Decimal("0")],
            "quantidade_estabelecimentos": [Decimal("8"), Decimal("2")],
        }
    )
    valor_producao = pd.DataFrame(
        {
            **key_fields,
            "quantidade_estabelecimentos_produtivos": [Decimal("7"), Decimal("1")],
            "valor_producao": [Decimal("1000"), Decimal("500")],
        }
    )

    result = transform(pessoal_ocupado, valor_producao)

    coefficients = result.set_index("grupo_atividade_economica")["coeficiente_tecnico"]
    assert coefficients["Produção de lavouras temporárias"] == Decimal("50")
    assert coefficients["Produção de lavouras permanentes"] is None
