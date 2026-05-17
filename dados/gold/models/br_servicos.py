"""Gold schemas for br_servicos."""
from __future__ import annotations

from decimal import Decimal

from pydantic import BaseModel, Field


class BrServicosPasServicos(BaseModel):
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
