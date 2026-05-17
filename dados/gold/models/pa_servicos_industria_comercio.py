"""Gold schemas for pa_servicos_industria_comercio (Pará regional cuts)."""
from __future__ import annotations

from decimal import Decimal

from pydantic import BaseModel, Field


class PaServicosIndustriaComercioPasServicos(BaseModel):
    ano: int = Field(description="Reference year", json_schema_extra={"unit": "YYYY"})
    unidade_geografica: str = Field(
        description="Geographic unit (state) reported by IBGE PAS — fixed to 'Pará'",
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


class PaServicosIndustriaComercioPacComercio(BaseModel):
    ano: int = Field(description="Reference year", json_schema_extra={"unit": "YYYY"})
    unidade_geografica: str = Field(
        description="Geographic unit (state) reported by IBGE PAC — fixed to 'Pará'",
        json_schema_extra={"unit": "code"},
    )
    divisao_grupo_cnae_2: str = Field(
        description="CNAE 2.0 division/group breakdown for commerce activity",
        json_schema_extra={"unit": "code"},
    )
    valor_gastos_salarios_remuneracoes: Decimal | None = Field(
        description="Total spent on salaries, withdrawals and other remunerations in commercial firms",
        json_schema_extra={"unit": "BRL"},
    )
    margem_comercializacao: Decimal | None = Field(
        description="Commercialization margin in commercial firms",
        json_schema_extra={"unit": "BRL"},
    )
    quantidade_unidades_empresas_receita_revenda: Decimal | None = Field(
        description="Number of local units with resale revenue",
        json_schema_extra={"unit": "head_count"},
    )
    pessoal_ocupado_31_12: Decimal | None = Field(
        description="Persons employed in commercial firms as of Dec 31",
        json_schema_extra={"unit": "head_count"},
    )
    valor_receita_bruta_revenda: Decimal | None = Field(
        description="Gross revenue from resale of goods",
        json_schema_extra={"unit": "BRL"},
    )


class PaServicosIndustriaComercioPiaIndustrias(BaseModel):
    ano: int = Field(description="Reference year", json_schema_extra={"unit": "YYYY"})
    nome_localidade: str = Field(
        description="State (UF) name reported by IBGE PIA — fixed to 'Pará'",
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
