"""Silver schemas for IBGE PIA (Pesquisa Industrial Anual)."""

from __future__ import annotations

from decimal import Decimal

from pydantic import BaseModel, Field


class BrIbgePiaTbl1849(BaseModel):
    ano: int = Field(description="Reference year", json_schema_extra={"unit": "YYYY"})
    nome_localidade: str = Field(
        description="State (UF) name reported by IBGE",
        json_schema_extra={"unit": "code"},
    )
    divisao_grupo_cnae_2: str = Field(
        description="CNAE 2.0 division/group breakdown for industrial activity",
        json_schema_extra={"unit": "code"},
    )
    custos_materias_primas: Decimal | None = Field(
        description="Costs of raw materials, auxiliary materials, and components",
        json_schema_extra={"unit": "thousand_BRL"},
    )
    encargos_sociais_trabalhistas: Decimal | None = Field(
        description="Social and labor charges, indemnities, and benefits",
        json_schema_extra={"unit": "thousand_BRL"},
    )
    quantidade_unidades_locais: Decimal | None = Field(
        description="Number of local units",
        json_schema_extra={"unit": "head_count"},
    )
    pessoal_ocupado_31_12: Decimal | None = Field(
        description="Persons employed as of Dec 31",
        json_schema_extra={"unit": "head_count"},
    )
    receita_liquida_vendas_industriais: Decimal | None = Field(
        description="Net sales revenue from industrial activities",
        json_schema_extra={"unit": "thousand_BRL"},
    )
    receita_liquida_vendas_nao_industriais: Decimal | None = Field(
        description="Net sales revenue from non-industrial activities",
        json_schema_extra={"unit": "thousand_BRL"},
    )
    valor_salarios_remuneracoes: Decimal | None = Field(
        description="Salaries, withdrawals and other remunerations",
        json_schema_extra={"unit": "thousand_BRL"},
    )
    valor_custos_operacoes_industriais: Decimal | None = Field(
        description="Total costs of industrial operations",
        json_schema_extra={"unit": "thousand_BRL"},
    )
    valor_custos_despesas: Decimal | None = Field(
        description="Total costs and expenses",
        json_schema_extra={"unit": "thousand_BRL"},
    )
    valor_receitas_liquidas_vendas: Decimal | None = Field(
        description="Total net sales revenue",
        json_schema_extra={"unit": "thousand_BRL"},
    )
    valor_bruto_producao_industrial: Decimal | None = Field(
        description="Gross value of industrial production",
        json_schema_extra={"unit": "thousand_BRL"},
    )
    valor_transformacao_industrial: Decimal | None = Field(
        description="Value of industrial transformation",
        json_schema_extra={"unit": "thousand_BRL"},
    )


class BrIbgePiaTbl10457(BaseModel):
    """PIA-Empresa série nova (ano de referência 2024 em diante), agregado 10457.

    Mesmos indicadores de ``BrIbgePiaTbl1849``, com duas mudanças de variável:
    706 ("Número de unidades locais") vira 13816 e entra 2086 ("Número de
    empresas com 5 ou mais pessoas ocupadas"). A coluna ``serie`` marca a
    ruptura da Nota Técnica PIA-Empresa 01/2026: os valores **não são
    comparáveis** com os da tabela 1849.
    """

    ano: int = Field(description="Reference year", json_schema_extra={"unit": "YYYY"})
    nome_localidade: str = Field(
        description="State (UF) name reported by IBGE",
        json_schema_extra={"unit": "code"},
    )
    divisao_grupo_cnae_2: str = Field(
        description="CNAE 2.0 division/group breakdown for industrial activity",
        json_schema_extra={"unit": "code"},
    )
    custos_materias_primas: Decimal | None = Field(
        description="Costs of raw materials, auxiliary materials, and components",
        json_schema_extra={"unit": "thousand_BRL"},
    )
    encargos_sociais_trabalhistas: Decimal | None = Field(
        description="Social and labor charges, indemnities, and benefits",
        json_schema_extra={"unit": "thousand_BRL"},
    )
    serie: str = Field(
        description="IBGE series the row belongs to ('nova' from 2024 onwards)",
        json_schema_extra={"unit": "code"},
    )
    quantidade_empresas: Decimal | None = Field(
        description="Number of firms with 5 or more employed persons (IBGE variable 2086)",
        json_schema_extra={"unit": "head_count"},
    )
    quantidade_unidades_locais: Decimal | None = Field(
        description=(
            "Number of local units of firms with 5 or more employed persons "
            "(IBGE variable 13816; replaces variable 706 of the old series)"
        ),
        json_schema_extra={"unit": "head_count"},
    )
    pessoal_ocupado_31_12: Decimal | None = Field(
        description="Persons employed as of Dec 31",
        json_schema_extra={"unit": "head_count"},
    )
    receita_liquida_vendas_industriais: Decimal | None = Field(
        description="Net sales revenue from industrial activities",
        json_schema_extra={"unit": "thousand_BRL"},
    )
    receita_liquida_vendas_nao_industriais: Decimal | None = Field(
        description="Net sales revenue from non-industrial activities",
        json_schema_extra={"unit": "thousand_BRL"},
    )
    valor_salarios_remuneracoes: Decimal | None = Field(
        description="Salaries, withdrawals and other remunerations",
        json_schema_extra={"unit": "thousand_BRL"},
    )
    valor_custos_operacoes_industriais: Decimal | None = Field(
        description="Total costs of industrial operations",
        json_schema_extra={"unit": "thousand_BRL"},
    )
    valor_custos_despesas: Decimal | None = Field(
        description="Total costs and expenses",
        json_schema_extra={"unit": "thousand_BRL"},
    )
    valor_receitas_liquidas_vendas: Decimal | None = Field(
        description="Total net sales revenue",
        json_schema_extra={"unit": "thousand_BRL"},
    )
    valor_bruto_producao_industrial: Decimal | None = Field(
        description="Gross value of industrial production",
        json_schema_extra={"unit": "thousand_BRL"},
    )
    valor_transformacao_industrial: Decimal | None = Field(
        description="Value of industrial transformation",
        json_schema_extra={"unit": "thousand_BRL"},
    )
