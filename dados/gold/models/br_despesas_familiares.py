"""Gold schemas for br_despesas_familiares."""
from __future__ import annotations

from decimal import Decimal

from pydantic import BaseModel, Field


class BrDespesasFamiliaresPof2018DespesasFamiliaresSituacaoDomicilio(BaseModel):
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
