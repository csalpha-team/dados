"""Silver schemas for IBGE PAS (Pesquisa Anual de Serviços)."""

from __future__ import annotations

from decimal import Decimal

from pydantic import BaseModel, Field


class BrIbgePasTbl2715(BaseModel):
    ano: int = Field(description="Reference year", json_schema_extra={"unit": "YYYY"})
    unidade_geografica: str = Field(
        description="Geographic unit (Brazil / Major Region / UF) reported by IBGE",
        json_schema_extra={"unit": "code"},
    )
    divisao_grupo_cnae_2: str = Field(
        description="CNAE 2.0 division/group breakdown for services activity",
        json_schema_extra={"unit": "code"},
    )
    quantidade_empresas: Decimal | None = Field(
        description="Number of service-sector firms",
        json_schema_extra={"unit": "head_count"},
    )
    valor_gastos_salarios_remuneracoes: Decimal | None = Field(
        description="Total spent on salaries, withdrawals and other remunerations",
        json_schema_extra={"unit": "BRL"},
    )
    pessoal_ocupado_31_12: Decimal | None = Field(
        description="Persons employed in service-sector firms as of Dec 31",
        json_schema_extra={"unit": "head_count"},
    )
    valor_receita_bruta_servicos: Decimal | None = Field(
        description="Gross revenue from services",
        json_schema_extra={"unit": "BRL"},
    )


class BrIbgePasTbl10762(BaseModel):
    """PAS série nova (ano de referência 2024 em diante), agregado 10762.

    Mesmos indicadores de ``BrIbgePasTbl2715``, com a geografia vinda de
    ``localidades`` (N1/N2/N3) em vez da classificação 12354 e os salários na
    variável 857. A coluna ``serie`` marca a ruptura da Nota Técnica PAS
    01/2026: os valores **não são comparáveis** com os da tabela 2715.
    """

    ano: int = Field(description="Reference year", json_schema_extra={"unit": "YYYY"})
    unidade_geografica: str = Field(
        description="Geographic unit (Brazil / Major Region / UF) reported by IBGE",
        json_schema_extra={"unit": "code"},
    )
    divisao_grupo_cnae_2: str = Field(
        description="CNAE 2.0 division/group breakdown for services activity",
        json_schema_extra={"unit": "code"},
    )
    serie: str = Field(
        description="IBGE series the row belongs to ('nova' from 2024 onwards)",
        json_schema_extra={"unit": "code"},
    )
    quantidade_empresas: Decimal | None = Field(
        description="Number of service-sector firms",
        json_schema_extra={"unit": "head_count"},
    )
    valor_gastos_salarios_remuneracoes: Decimal | None = Field(
        description=(
            "Total spent on salaries, withdrawals and other remunerations "
            "(IBGE variable 857, source unit 'Mil Reais')"
        ),
        json_schema_extra={"unit": "thousand_BRL"},
    )
    pessoal_ocupado_31_12: Decimal | None = Field(
        description="Persons employed in service-sector firms as of Dec 31",
        json_schema_extra={"unit": "head_count"},
    )
    valor_receita_bruta_servicos: Decimal | None = Field(
        description="Gross revenue from services (source unit 'Mil Reais')",
        json_schema_extra={"unit": "thousand_BRL"},
    )
