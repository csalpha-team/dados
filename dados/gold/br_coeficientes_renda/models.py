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
        description="Annual income coefficient in constant 2022 prices per worker",
        json_schema_extra={"unit": "thousand_BRL_2022_per_worker_year"},
    )


class BrCoeficientesRendaRendaProdutividade(BaseModel):
    ano: int = Field(description="Reference year", json_schema_extra={"unit": "YYYY"})
    conta_alfa: str = Field(
        description="Conta Alfa sector label",
        json_schema_extra={"unit": "code"},
    )
    coeff: Decimal | None = Field(
        description="Annual worker productivity in constant 2022 prices",
        json_schema_extra={"unit": "thousand_BRL_2022_per_worker_year"},
    )


class BrCoeficientesRendaRendaSalario(BaseModel):
    ano: int = Field(description="Reference year", json_schema_extra={"unit": "YYYY"})
    conta_alfa: str = Field(
        description="Conta Alfa sector label",
        json_schema_extra={"unit": "code"},
    )
    coeff: Decimal | None = Field(
        description="Annual average remuneration in constant 2022 prices",
        json_schema_extra={"unit": "thousand_BRL_2022_per_worker_year"},
    )
