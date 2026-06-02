"""Gold schemas for pa_coeficientes_custo."""

from __future__ import annotations

from decimal import Decimal

from pydantic import BaseModel, Field


class PaCoeficientesCustoPreparacaoCamadaCusto(BaseModel):
    ano: int = Field(description="Reference year", json_schema_extra={"unit": "YYYY"})
    nome_regiao_integracao: str = Field(
        description="Pará integration region (RI)",
        json_schema_extra={"unit": "code"},
    )
    tipo_coeff: str = Field(
        description="Cost value type label used by the downstream model",
        json_schema_extra={"unit": "code"},
    )
    valor: Decimal | None = Field(
        description="Observed gross cost value by region, year, and mapped cost item",
        json_schema_extra={"unit": "BRL"},
    )
