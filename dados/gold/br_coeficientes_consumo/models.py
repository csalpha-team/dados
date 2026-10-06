"""Gold schemas for br_coeficientes_consumo."""

from __future__ import annotations

from decimal import Decimal

from pydantic import BaseModel, Field


class BrCoeficientesConsumoPreparacaoCamadaConsumo(BaseModel):
    ano: int = Field(description="Reference year", json_schema_extra={"unit": "YYYY"})
    coeff_key: str = Field(
        description="MIP destination expense category (chave_mip)",
        json_schema_extra={"unit": "code"},
    )
    valor: Decimal | None = Field(
        description="Observed consumption expense value converted to thousands of BRL",
        json_schema_extra={"unit": "1000xBRL"},
    )
