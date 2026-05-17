"""Gold schemas for br_coeficientes_investimento."""

from __future__ import annotations

from decimal import Decimal

from pydantic import BaseModel, Field


class BrCoeficientesInvestimentoCoeficientesInvestimento(BaseModel):
    coeff_key: str = Field(
        description="Investment coefficient identifier (matches keys in coeficientes_investimento.json)",
        json_schema_extra={"unit": "code"},
    )
    coeff: Decimal = Field(
        description="Exogenous investment coefficient — cost fraction per micro-production unit",
        json_schema_extra={"unit": "ratio"},
    )
