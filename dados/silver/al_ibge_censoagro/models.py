"""Silver schemas for IBGE Censo Agropecuário (2006 + 2017) — Amazônia Legal.

Each model corresponds to one materialized silver table under
``$DB_SILVER_ZONE.al_ibge_censoagro``.
"""

from __future__ import annotations

from decimal import Decimal

from pydantic import BaseModel, Field

# ---------------------------------------------------------------------------
# 2006
# ---------------------------------------------------------------------------


class AlIbgeCensoagroTbl19092006(BaseModel):
    ano: int = Field(description="Reference year", json_schema_extra={"unit": "YYYY"})
    id_municipio: str = Field(
        description="IBGE 7-digit municipality code", json_schema_extra={"unit": "code"}
    )
    tipo_agricultura: str = Field(
        description="Standardized agriculture type (agricultura familiar / agricultura não familiar)",
        json_schema_extra={"unit": "code"},
    )
    tipo_despesa: str = Field(
        description="Expenditure category", json_schema_extra={"unit": "code"}
    )
    quantidade_estabelecimentos_fizeram_despesa: Decimal | None = Field(
        description="Number of farms that reported the given expenditure",
        json_schema_extra={"unit": "head_count"},
    )
    valor_despesa: Decimal | None = Field(
        description="Reported expenditure amount", json_schema_extra={"unit": "BRL"}
    )


class AlIbgeCensoagroTbl19312006(BaseModel):
    ano: int = Field(description="Reference year", json_schema_extra={"unit": "YYYY"})
    id_municipio: str = Field(
        description="IBGE 7-digit municipality code", json_schema_extra={"unit": "code"}
    )
    tipo_agricultura: str = Field(
        description="Standardized agriculture type", json_schema_extra={"unit": "code"}
    )
    tipo_producao: str = Field(
        description="Production type (livestock/crop class)",
        json_schema_extra={"unit": "code"},
    )
    quantidade_estabelecimentos_produtivos: Decimal | None = Field(
        description="Number of producing farms",
        json_schema_extra={"unit": "head_count"},
    )
    valor_producao: Decimal | None = Field(
        description="Value of production", json_schema_extra={"unit": "BRL"}
    )


class _Censo2233Like(BaseModel):
    """Shared schema for tbl_2233_2006 / tbl_2518_2006 — full autoconsumo + comércio."""

    ano: int = Field(description="Reference year", json_schema_extra={"unit": "YYYY"})
    id_municipio: str = Field(
        description="IBGE 7-digit municipality code", json_schema_extra={"unit": "code"}
    )
    produto: str = Field(
        description="Standardized agricultural product name",
        json_schema_extra={"unit": "code"},
    )
    tipo_agricultura: str = Field(
        description="Standardized agriculture type", json_schema_extra={"unit": "code"}
    )

    quantidade_estabelecimentos: Decimal | None = Field(
        description="Number of farms", json_schema_extra={"unit": "head_count"}
    )
    quantidade_produzida: Decimal | None = Field(
        description="Quantity produced", json_schema_extra={"unit": "ton"}
    )
    quantidade_vendida: Decimal | None = Field(
        description="Quantity sold", json_schema_extra={"unit": "ton"}
    )
    valor_producao: Decimal | None = Field(
        description="Value of production", json_schema_extra={"unit": "BRL"}
    )
    valor_venda: Decimal | None = Field(
        description="Value of sales", json_schema_extra={"unit": "BRL"}
    )

    autoconsumo_quantidade_estabelecimentos: Decimal | None = Field(
        description="Farms reporting on-farm consumption",
        json_schema_extra={"unit": "head_count"},
    )
    autoconsumo_quantidade_produzida: Decimal | None = Field(
        description="Quantity produced consumed on-farm",
        json_schema_extra={"unit": "ton"},
    )
    autoconsumo_quantidade_vendida: Decimal | None = Field(
        description="Quantity sold from on-farm consumption portion",
        json_schema_extra={"unit": "ton"},
    )
    autoconsumo_valor_producao: Decimal | None = Field(
        description="Value of production consumed on-farm",
        json_schema_extra={"unit": "BRL"},
    )
    autoconsumo_valor_venda: Decimal | None = Field(
        description="Value of sales from on-farm consumption portion",
        json_schema_extra={"unit": "BRL"},
    )

    comercio_quantidade_estabelecimentos: Decimal | None = Field(
        description="Farms estimated to commercialize (total - autoconsumo)",
        json_schema_extra={"unit": "head_count"},
    )
    comercio_quantidade_produzida: Decimal | None = Field(
        description="Quantity produced commercialized",
        json_schema_extra={"unit": "ton"},
    )
    comercio_quantidade_vendida: Decimal | None = Field(
        description="Quantity sold commercialized", json_schema_extra={"unit": "ton"}
    )
    comercio_valor_producao: Decimal | None = Field(
        description="Value of production commercialized",
        json_schema_extra={"unit": "BRL"},
    )
    comercio_valor_venda: Decimal | None = Field(
        description="Value of sales commercialized", json_schema_extra={"unit": "BRL"}
    )


class AlIbgeCensoagroTbl22332006(_Censo2233Like):
    pass


class AlIbgeCensoagroTbl25182006(_Censo2233Like):
    pass


class AlIbgeCensoagroTbl22842006(BaseModel):
    """Same shape as 2233 minus the ``valor_venda`` series."""

    ano: int = Field(description="Reference year", json_schema_extra={"unit": "YYYY"})
    id_municipio: str = Field(
        description="IBGE 7-digit municipality code", json_schema_extra={"unit": "code"}
    )
    produto: str = Field(
        description="Standardized agricultural product name",
        json_schema_extra={"unit": "code"},
    )
    tipo_agricultura: str = Field(
        description="Standardized agriculture type", json_schema_extra={"unit": "code"}
    )

    quantidade_estabelecimentos: Decimal | None = Field(
        description="Number of farms", json_schema_extra={"unit": "head_count"}
    )
    quantidade_produzida: Decimal | None = Field(
        description="Quantity produced", json_schema_extra={"unit": "ton"}
    )
    quantidade_vendida: Decimal | None = Field(
        description="Quantity sold", json_schema_extra={"unit": "ton"}
    )
    valor_producao: Decimal | None = Field(
        description="Value of production", json_schema_extra={"unit": "BRL"}
    )

    autoconsumo_quantidade_estabelecimentos: Decimal | None = Field(
        description="Farms reporting on-farm consumption",
        json_schema_extra={"unit": "head_count"},
    )
    autoconsumo_quantidade_produzida: Decimal | None = Field(
        description="Quantity produced consumed on-farm",
        json_schema_extra={"unit": "ton"},
    )
    autoconsumo_quantidade_vendida: Decimal | None = Field(
        description="Quantity sold from on-farm consumption portion",
        json_schema_extra={"unit": "ton"},
    )
    autoconsumo_valor_producao: Decimal | None = Field(
        description="Value of production consumed on-farm",
        json_schema_extra={"unit": "BRL"},
    )

    comercio_quantidade_estabelecimentos: Decimal | None = Field(
        description="Farms estimated to commercialize (total - autoconsumo)",
        json_schema_extra={"unit": "head_count"},
    )
    comercio_quantidade_produzida: Decimal | None = Field(
        description="Quantity produced commercialized",
        json_schema_extra={"unit": "ton"},
    )
    comercio_quantidade_vendida: Decimal | None = Field(
        description="Quantity sold commercialized", json_schema_extra={"unit": "ton"}
    )
    comercio_valor_producao: Decimal | None = Field(
        description="Value of production commercialized",
        json_schema_extra={"unit": "BRL"},
    )


class AlIbgeCensoagroTbl23372006(BaseModel):
    ano: int = Field(description="Reference year", json_schema_extra={"unit": "YYYY"})
    id_municipio: str = Field(
        description="IBGE 7-digit municipality code", json_schema_extra={"unit": "code"}
    )
    produto: str = Field(
        description="Standardized agricultural product name",
        json_schema_extra={"unit": "code"},
    )
    tipo_agricultura: str = Field(
        description="Standardized agriculture type", json_schema_extra={"unit": "code"}
    )
    quantidade_estabelecimentos: Decimal | None = Field(
        description="Number of farms", json_schema_extra={"unit": "head_count"}
    )
    quantidade_produzida: Decimal | None = Field(
        description="Quantity produced", json_schema_extra={"unit": "ton"}
    )
    quantidade_vendida: Decimal | None = Field(
        description="Quantity sold", json_schema_extra={"unit": "ton"}
    )
    valor_producao: Decimal | None = Field(
        description="Value of production", json_schema_extra={"unit": "BRL"}
    )
    area_colhida: Decimal | None = Field(
        description="Harvested area", json_schema_extra={"unit": "hectare"}
    )


class AlIbgeCensoagroTbl27822006(BaseModel):
    ano: int = Field(description="Reference year", json_schema_extra={"unit": "YYYY"})
    id_municipio: str = Field(
        description="IBGE 7-digit municipality code", json_schema_extra={"unit": "code"}
    )
    tipo_agricultura: str = Field(
        description="Standardized agriculture type", json_schema_extra={"unit": "code"}
    )
    pessoal_ocupado_mais_14_anos_familia: Decimal | None = Field(
        description="Family members aged 14+ employed on the farm",
        json_schema_extra={"unit": "head_count"},
    )
    pessoal_total_ocupado_familia: Decimal | None = Field(
        description="Total family members employed on the farm",
        json_schema_extra={"unit": "head_count"},
    )


# ---------------------------------------------------------------------------
# 2017
# ---------------------------------------------------------------------------


class AlIbgeCensoagroTbl68852017(BaseModel):
    ano: int = Field(description="Reference year", json_schema_extra={"unit": "YYYY"})
    id_municipio: str = Field(
        description="IBGE 7-digit municipality code", json_schema_extra={"unit": "code"}
    )
    tipo_agricultura: str = Field(
        description="Standardized agriculture type", json_schema_extra={"unit": "code"}
    )
    pessoal_total_ocupado: Decimal | None = Field(
        description="Total persons employed on farms",
        json_schema_extra={"unit": "head_count"},
    )
    quantidade_total_estabecimentos: Decimal | None = Field(
        description="Total number of farms with persons employed",
        json_schema_extra={"unit": "head_count"},
    )
    pessoal_ocupado_familia: Decimal | None = Field(
        description="Persons employed with family ties to the producer",
        json_schema_extra={"unit": "head_count"},
    )
    quantidade_estabecimentos_pessoal_ocupado_familia: Decimal | None = Field(
        description="Number of farms with family-tied employees",
        json_schema_extra={"unit": "head_count"},
    )
    pessoal_ocupado_fora_familia: Decimal | None = Field(
        description="Persons employed without family ties",
        json_schema_extra={"unit": "head_count"},
    )
    quantidade_estabecimentos_pessoal_ocupado_fora_familia: Decimal | None = Field(
        description="Number of farms with non-family employees",
        json_schema_extra={"unit": "head_count"},
    )


class AlIbgeCensoagroTbl68982017(BaseModel):
    ano: int = Field(description="Reference year", json_schema_extra={"unit": "YYYY"})
    id_municipio: str = Field(
        description="IBGE 7-digit municipality code", json_schema_extra={"unit": "code"}
    )
    tipo_agricultura: str = Field(
        description="Standardized agriculture type", json_schema_extra={"unit": "code"}
    )
    tipo_producao: str = Field(
        description="Production type", json_schema_extra={"unit": "code"}
    )
    quantidade_estabelecimentos_produtivos: Decimal | None = Field(
        description="Number of producing farms",
        json_schema_extra={"unit": "head_count"},
    )
    valor_producao: Decimal | None = Field(
        description="Value of production", json_schema_extra={"unit": "BRL"}
    )


class AlIbgeCensoagroTbl68992017(AlIbgeCensoagroTbl19092006):
    """2017 expenditure table — same columns as 1909/2006."""


class _Censo2017ProdutoLike(BaseModel):
    ano: int = Field(description="Reference year", json_schema_extra={"unit": "YYYY"})
    id_municipio: str = Field(
        description="IBGE 7-digit municipality code", json_schema_extra={"unit": "code"}
    )
    produto: str = Field(
        description="Standardized agricultural product name",
        json_schema_extra={"unit": "code"},
    )
    tipo_agricultura: str = Field(
        description="Standardized agriculture type", json_schema_extra={"unit": "code"}
    )
    quantidade_estabelecimentos: Decimal | None = Field(
        description="Number of farms", json_schema_extra={"unit": "head_count"}
    )
    quantidade_produzida: Decimal | None = Field(
        description="Quantity produced", json_schema_extra={"unit": "ton"}
    )
    quantidade_vendida: Decimal | None = Field(
        description="Quantity sold", json_schema_extra={"unit": "ton"}
    )
    valor_producao: Decimal | None = Field(
        description="Value of production", json_schema_extra={"unit": "BRL"}
    )
    valor_venda: Decimal | None = Field(
        description="Value of sales", json_schema_extra={"unit": "BRL"}
    )


class AlIbgeCensoagroTbl69492017(_Censo2017ProdutoLike):
    """Extração vegetal — 2017."""


class AlIbgeCensoagroTbl69552017(_Censo2017ProdutoLike):
    """Lavoura permanente (50 pés+) — 2017."""

    area_colhida: Decimal | None = Field(
        description="Harvested area", json_schema_extra={"unit": "hectare"}
    )
    area_plantada: Decimal | None = Field(
        description="Planted area", json_schema_extra={"unit": "hectare"}
    )


class AlIbgeCensoagroTbl69572017(_Censo2017ProdutoLike):
    """Lavoura temporária — 2017."""

    area_colhida: Decimal | None = Field(
        description="Harvested area", json_schema_extra={"unit": "hectare"}
    )
