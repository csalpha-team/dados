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
        json_schema_extra={"unit": "BRL"},
    )
    encargos_sociais_trabalhistas: Decimal | None = Field(
        description="Social and labor charges, indemnities, and benefits",
        json_schema_extra={"unit": "BRL"},
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
        json_schema_extra={"unit": "BRL"},
    )
    receita_liquida_vendas_nao_industriais: Decimal | None = Field(
        description="Net sales revenue from non-industrial activities",
        json_schema_extra={"unit": "BRL"},
    )
    valor_salarios_remuneracoes: Decimal | None = Field(
        description="Salaries, withdrawals and other remunerations",
        json_schema_extra={"unit": "BRL"},
    )
    valor_custos_operacoes_industriais: Decimal | None = Field(
        description="Total costs of industrial operations",
        json_schema_extra={"unit": "BRL"},
    )
    valor_custos_despesas: Decimal | None = Field(
        description="Total costs and expenses",
        json_schema_extra={"unit": "BRL"},
    )
    valor_receitas_liquidas_vendas: Decimal | None = Field(
        description="Total net sales revenue",
        json_schema_extra={"unit": "BRL"},
    )
    valor_bruto_producao_industrial: Decimal | None = Field(
        description="Gross value of industrial production",
        json_schema_extra={"unit": "BRL"},
    )
    valor_transformacao_industrial: Decimal | None = Field(
        description="Value of industrial transformation",
        json_schema_extra={"unit": "BRL"},
    )
