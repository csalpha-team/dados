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
    valor: Decimal | None = Field(
        description="Observed cost expense value in thousands of BRL",
        json_schema_extra={"unit": "1000xBRL"},
    )
    coeff: Decimal | None = Field(
        description="Legacy cost coefficient (share of total establishment expenses)",
        json_schema_extra={"unit": "ratio"},
    )
