"""Gold schemas for br_coeficientes_exportacao."""
from __future__ import annotations

from decimal import Decimal

from pydantic import BaseModel, Field


class BrCoeficientesExportacaoPreparacaoCamadaExportacao(BaseModel):
    ano: int = Field(description="Reference year", json_schema_extra={"unit": "YYYY"})
    produto: str = Field(
        description="Aggregated product label (mapped from NCM)",
        json_schema_extra={"unit": "code"},
    )
    valor_fob_dolar: Decimal | None = Field(
        description="Total FOB export value in US dollars",
        json_schema_extra={"unit": "USD"},
    )
    valor_fob_real: Decimal | None = Field(
        description="Total FOB export value converted to Brazilian reais",
        json_schema_extra={"unit": "BRL"},
    )
    coeff: Decimal | None = Field(
        description="Export coefficient for the product/year cell",
        json_schema_extra={"unit": "ratio"},
    )
