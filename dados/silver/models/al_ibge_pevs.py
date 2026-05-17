"""Silver schemas for IBGE PEVS (Produção da Extração Vegetal e Silvicultura) — Amazônia Legal."""
from __future__ import annotations

from decimal import Decimal

from pydantic import BaseModel, Field


class AlIbgePevsExtracaoVegetal(BaseModel):
    ano: int = Field(description="Reference year", json_schema_extra={"unit": "YYYY"})
    id_municipio: str = Field(
        description="IBGE 7-digit municipality code",
        json_schema_extra={"unit": "code"},
    )
    produto: str = Field(
        description="Standardized vegetal-extraction product name",
        json_schema_extra={"unit": "code"},
    )
    quantidade_produzida: Decimal | None = Field(
        description="Quantity produced in vegetal extraction (unit depends on product)",
        json_schema_extra={"unit": "ton"},
    )
    valor_producao: Decimal | None = Field(
        description="Value of vegetal-extraction production (deflated by currency_fix)",
        json_schema_extra={"unit": "BRL"},
    )
