"""Gold schemas for br_coeficientes_renda."""
from __future__ import annotations

from decimal import Decimal

from pydantic import BaseModel, Field


class BrCoeficientesRendaPreparacaoCamadaRenda(BaseModel):
    ano: int = Field(description="Reference year", json_schema_extra={"unit": "YYYY"})
    conta_alfa: str = Field(
        description="Conta Alfa sector label",
        json_schema_extra={"unit": "code"},
    )
    tipo_coeff: str = Field(
        description="Coefficient type (e.g. prod_mon_trab, salario_medio)",
        json_schema_extra={"unit": "code"},
    )
    coeff: Decimal | None = Field(
        description="Coefficient value for the (year, sector, type) cell",
        json_schema_extra={"unit": "BRL"},
    )


class BrCoeficientesRendaRendaProdutividade(BaseModel):
    ano: int = Field(description="Reference year", json_schema_extra={"unit": "YYYY"})
    conta_alfa: str = Field(
        description="Conta Alfa sector label",
        json_schema_extra={"unit": "code"},
    )
    coeff: Decimal | None = Field(
        description="Monthly worker productivity coefficient",
        json_schema_extra={"unit": "BRL"},
    )


class BrCoeficientesRendaRendaSalario(BaseModel):
    ano: int = Field(description="Reference year", json_schema_extra={"unit": "YYYY"})
    conta_alfa: str = Field(
        description="Conta Alfa sector label",
        json_schema_extra={"unit": "code"},
    )
    coeff: Decimal | None = Field(
        description="Average salary coefficient",
        json_schema_extra={"unit": "BRL"},
    )
