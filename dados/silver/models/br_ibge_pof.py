"""Silver schemas for IBGE POF (Pesquisa de Orçamentos Familiares)."""
from __future__ import annotations

from decimal import Decimal

from pydantic import BaseModel, Field


class BrIbgePofTbl2393(BaseModel):
    ano: int = Field(description="Reference year", json_schema_extra={"unit": "YYYY"})
    uf: str = Field(
        description="State (UF) name reported by IBGE POF",
        json_schema_extra={"unit": "code"},
    )
    categoria_alimento: str = Field(
        description="Food category (IBGE POF classification)",
        json_schema_extra={"unit": "code"},
    )
    quantidade_aquisicao_alimentar_per_capta: Decimal | None = Field(
        description="Annual per-capita household food acquisition",
        json_schema_extra={"unit": "kg"},
    )
    unidade_medida: str | None = Field(
        description="Measurement unit reported for the acquisition quantity (kg, L, etc.)",
        json_schema_extra={"unit": "code"},
    )


class BrIbgePofTbl6970(BaseModel):
    ano: int = Field(description="Reference year", json_schema_extra={"unit": "YYYY"})
    localidade: str = Field(
        description="Geographic locality (Brazil / Region / UF) reported by POF",
        json_schema_extra={"unit": "code"},
    )
    variavel: str = Field(
        description="POF variable label (e.g. monthly average expenditure)",
        json_schema_extra={"unit": "code"},
    )
    situacao_domicilio: str = Field(
        description="Household setting (urban/rural) breakdown",
        json_schema_extra={"unit": "code"},
    )
    tipo_despesa: str = Field(
        description="Type of expenditure (POF classification)",
        json_schema_extra={"unit": "code"},
    )
    valor: Decimal | None = Field(
        description="Reported value for the (variavel, situacao_domicilio, tipo_despesa) cell",
        json_schema_extra={"unit": "BRL"},
    )
    unidade_medida: str | None = Field(
        description="Measurement unit reported for the value",
        json_schema_extra={"unit": "code"},
    )
