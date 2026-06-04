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
        description=(
            "Quantidade produzida, em toneladas. As frutíferas informadas pelo IBGE em "
            "mil frutos (banana em mil cachos) antes de 2001 são convertidas para "
            "toneladas em silver via products_weight_ratio_fix; de 2001 em diante já são "
            "toneladas na origem."
        ),
        json_schema_extra={"unit": "ton"},
    )
    valor_producao: Decimal | None = Field(
        description=(
            "Value of production. Historical currencies normalized by currency_fix to "
            "the Mil Reais base; kept in thousands of BRL (IBGE source unit 'Mil Reais')."
        ),
        json_schema_extra={"unit": "1000xBRL"},
    )
    area_colhida: Decimal | None = Field(
        description="Harvested area",
        json_schema_extra={"unit": "hectare"},
    )
    rendimento_medio_producao: Decimal | None = Field(
        description=(
            "Rendimento médio da produção, em kg/ha. Para as frutíferas pré-2001 "
            "(originalmente em frutos/ha; banana em cachos/ha) é recalculado a partir da "
            "quantidade convertida para toneladas (quantidade_t / area_colhida * 1000)."
        ),
        json_schema_extra={"unit": "kg/hectare"},
    )
    unidade_medida: str | None = Field(
        description=(
            "Unidade de medida efetiva da quantidade produzida (Toneladas). Origem: "
            "unidade da variável 'Quantidade produzida' nos metadados do IBGE — na PAM a "
            "unidade é definida por variável, não por produto (categorias sem unidade). "
            "Frutas pré-2001, informadas em mil frutos/mil cachos, são convertidas para "
            "toneladas em silver, de modo que o valor armazenado fica sempre em toneladas."
        ),
        json_schema_extra={"unit": "code"},
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
