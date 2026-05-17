"""Silver schemas for IBGE PAM (Produção Agrícola Municipal) — Amazônia Legal."""

from __future__ import annotations

from decimal import Decimal

from pydantic import BaseModel, Field


class _PamBase(BaseModel):
    ano: int = Field(description="Reference year", json_schema_extra={"unit": "YYYY"})
    id_municipio: str = Field(
        description="IBGE 7-digit municipality code",
        json_schema_extra={"unit": "code"},
    )
    produto: str = Field(
        description="Standardized agricultural product name",
        json_schema_extra={"unit": "code"},
    )
    quantidade_produzida: Decimal | None = Field(
        description="Quantity produced (unit depends on product)",
        json_schema_extra={"unit": "ton"},
    )
    valor_producao: Decimal | None = Field(
        description="Value of production (deflated by currency_fix)",
        json_schema_extra={"unit": "BRL"},
    )
    area_colhida: Decimal | None = Field(
        description="Harvested area",
        json_schema_extra={"unit": "hectare"},
    )
    rendimento_medio_producao: Decimal | None = Field(
        description="Average yield of production",
        json_schema_extra={"unit": "kg"},
    )


class AlIbgePamLavouraPermanente(_PamBase):
    area_destinada_colheita: Decimal | None = Field(
        description="Area destined for harvest (permanent crops)",
        json_schema_extra={"unit": "hectare"},
    )


class AlIbgePamLavouraTemporaria(_PamBase):
    area_plantada: Decimal | None = Field(
        description="Planted area (temporary crops)",
        json_schema_extra={"unit": "hectare"},
    )
