"""Silver schemas for IBGE PAC (Pesquisa Anual de Comércio)."""
from __future__ import annotations

from decimal import Decimal

from pydantic import BaseModel, Field


class BrIbgePacTbl1407(BaseModel):
    ano: int = Field(description="Reference year", json_schema_extra={"unit": "YYYY"})
    unidade_geografica: str = Field(
        description="Geographic unit (Brazil / Major Region / UF) reported by IBGE",
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
