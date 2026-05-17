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
        description="Cost coefficient type label",
        json_schema_extra={"unit": "code"},
    )
    coeff: Decimal | None = Field(
        description="Cost coefficient (share of total establishment expenses)",
        json_schema_extra={"unit": "ratio"},
    )
