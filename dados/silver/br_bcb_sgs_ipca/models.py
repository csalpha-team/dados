"""Silver schemas for the BCB SGS IPCA series."""

from __future__ import annotations

from decimal import Decimal

from pydantic import BaseModel, Field


class BrBcbSgsIpcaAnual(BaseModel):
    ano: int = Field(description="Reference year", json_schema_extra={"unit": "YYYY"})
    indice_ipca: Decimal = Field(
        description="Annual average IPCA price-level index chained from SGS 433",
        json_schema_extra={"unit": "index"},
    )
    variacao_acumulada_ano_pct: Decimal = Field(
        description="IPCA percentage change compounded over the available months",
        json_schema_extra={"unit": "%"},
    )
    meses_observados: int = Field(
        description="Number of monthly observations used in the year",
        json_schema_extra={"unit": "month_count"},
    )
    ano_completo: bool = Field(
        description="Whether all twelve monthly observations are available",
        json_schema_extra={"unit": "boolean"},
    )
    serie_sgs: int = Field(
        description="BCB SGS series identifier",
        json_schema_extra={"unit": "code"},
    )
    fonte: str = Field(
        description="Official data source",
        json_schema_extra={"unit": "text"},
    )
