"""Gold schemas for pa_indexadores_producao_rural."""
from __future__ import annotations

from decimal import Decimal

from pydantic import BaseModel, Field

_ANO = Field(description="Reference year", json_schema_extra={"unit": "YYYY"})
_ID_MUN = Field(
    description="IBGE 7-digit municipality code", json_schema_extra={"unit": "code"}
)
_NOME = Field(
    description="Municipality name from IBGE directory",
    json_schema_extra={"unit": "code"},
)
_REGIAO = Field(
    description="Pará integration region (RI) the municipality belongs to",
    json_schema_extra={"unit": "code"},
)
_UF = Field(
    description="State (UF) two-letter code",
    json_schema_extra={"unit": "code"},
)
_PRODUTO = Field(
    description="Product reported by IBGE (free text)",
    json_schema_extra={"unit": "code"},
)
_TIPO_AGRI = Field(
    description="Agriculture type (familiar vs non-familiar) reported by Censo",
    json_schema_extra={"unit": "code"},
)


class _BaseRow(BaseModel):
    ano: int = _ANO
    id_municipio: str = _ID_MUN
    nome: str | None = _NOME
    nome_regiao_integracao: str | None = _REGIAO
    sigla_uf: str | None = _UF


class PaIndexadoresLavouraTemporariaCenso2006(_BaseRow):
    tipo_agricultura: str | None = _TIPO_AGRI
    produto: str | None = _PRODUTO
    quantidade_estabelecimentos: Decimal | None = Field(
        description="Number of establishments reporting the product",
        json_schema_extra={"unit": "head_count"},
    )
    quantidade_produzida: Decimal | None = Field(
        description="Total quantity produced",
        json_schema_extra={"unit": "ton"},
    )
    quantidade_vendida: Decimal | None = Field(
        description="Total quantity sold",
        json_schema_extra={"unit": "ton"},
    )
    valor_producao: Decimal | None = Field(
        description="Total production value",
        json_schema_extra={"unit": "BRL"},
    )


class PaIndexadoresLavouraTemporariaCenso2006_2284(_BaseRow):
    tipo_agricultura: str | None = _TIPO_AGRI
    produto: str | None = _PRODUTO
    quantidade_estabelecimentos: Decimal | None = Field(
        description="Total number of establishments", json_schema_extra={"unit": "head_count"}
    )
    quantidade_produzida: Decimal | None = Field(
        description="Total quantity produced", json_schema_extra={"unit": "ton"}
    )
    quantidade_vendida: Decimal | None = Field(
        description="Total quantity sold", json_schema_extra={"unit": "ton"}
    )
    valor_producao: Decimal | None = Field(
        description="Total production value", json_schema_extra={"unit": "BRL"}
    )
    autoconsumo_quantidade_estabelecimentos: Decimal | None = Field(
        description="Establishments reporting self-consumption",
        json_schema_extra={"unit": "head_count"},
    )
    autoconsumo_quantidade_produzida: Decimal | None = Field(
        description="Quantity produced for self-consumption",
        json_schema_extra={"unit": "ton"},
    )
    autoconsumo_quantidade_vendida: Decimal | None = Field(
        description="Quantity sold from self-consumption series",
        json_schema_extra={"unit": "ton"},
    )
    autoconsumo_valor_producao: Decimal | None = Field(
        description="Production value attributed to self-consumption",
        json_schema_extra={"unit": "BRL"},
    )
    comercio_quantidade_estabelecimentos: Decimal | None = Field(
        description="Establishments reporting commercial production",
        json_schema_extra={"unit": "head_count"},
    )
    comercio_quantidade_produzida: Decimal | None = Field(
        description="Quantity produced for commerce",
        json_schema_extra={"unit": "ton"},
    )
    comercio_quantidade_vendida: Decimal | None = Field(
        description="Quantity sold via commerce",
        json_schema_extra={"unit": "ton"},
    )
    comercio_valor_producao: Decimal | None = Field(
        description="Production value attributed to commerce",
        json_schema_extra={"unit": "BRL"},
    )


class PaIndexadoresLavouraTemporariaCenso2017(_BaseRow):
    tipo_agricultura: str | None = _TIPO_AGRI
    produto: str | None = _PRODUTO
    quantidade_estabelecimentos: Decimal | None = Field(
        description="Number of establishments", json_schema_extra={"unit": "head_count"}
    )
    quantidade_produzida: Decimal | None = Field(
        description="Total quantity produced", json_schema_extra={"unit": "ton"}
    )
    comercio_quantidade_produzida: Decimal | None = Field(
        description="Quantity sold (rebadged as commerce series)",
        json_schema_extra={"unit": "ton"},
    )
    autoconsumo_quantidade_vendida: Decimal | None = Field(
        description="Produced minus sold (treated as self-consumption)",
        json_schema_extra={"unit": "ton"},
    )
    valor_producao: Decimal | None = Field(
        description="Total production value", json_schema_extra={"unit": "BRL"}
    )
    comercio_valor_producao: Decimal | None = Field(
        description="Sales value (treated as commerce)",
        json_schema_extra={"unit": "BRL"},
    )
    autoconsumo_valor_producao: Decimal | None = Field(
        description="Production minus sales value (treated as self-consumption)",
        json_schema_extra={"unit": "BRL"},
    )


class PaIndexadoresLavouraPermanenteCenso2006(_BaseRow):
    tipo_agricultura: str | None = _TIPO_AGRI
    produto: str | None = _PRODUTO
    quantidade_estabelecimentos: Decimal | None = Field(
        description="Number of establishments", json_schema_extra={"unit": "head_count"}
    )
    quantidade_produzida: Decimal | None = Field(
        description="Total quantity produced", json_schema_extra={"unit": "ton"}
    )
    quantidade_vendida: Decimal | None = Field(
        description="Total quantity sold", json_schema_extra={"unit": "ton"}
    )
    valor_producao: Decimal | None = Field(
        description="Total production value", json_schema_extra={"unit": "BRL"}
    )
    valor_venda: Decimal | None = Field(
        description="Total sales value", json_schema_extra={"unit": "BRL"}
    )
    autoconsumo_quantidade_estabelecimentos: Decimal | None = Field(
        description="Establishments reporting self-consumption",
        json_schema_extra={"unit": "head_count"},
    )
    autoconsumo_quantidade_produzida: Decimal | None = Field(
        description="Quantity produced for self-consumption",
        json_schema_extra={"unit": "ton"},
    )
    autoconsumo_quantidade_vendida: Decimal | None = Field(
        description="Quantity sold for self-consumption",
        json_schema_extra={"unit": "ton"},
    )
    autoconsumo_valor_producao: Decimal | None = Field(
        description="Production value for self-consumption",
        json_schema_extra={"unit": "BRL"},
    )
    autoconsumo_valor_venda: Decimal | None = Field(
        description="Sales value for self-consumption",
        json_schema_extra={"unit": "BRL"},
    )
    comercio_quantidade_estabelecimentos: Decimal | None = Field(
        description="Establishments reporting commercial production",
        json_schema_extra={"unit": "head_count"},
    )
    comercio_quantidade_produzida: Decimal | None = Field(
        description="Quantity produced for commerce", json_schema_extra={"unit": "ton"}
    )
    comercio_quantidade_vendida: Decimal | None = Field(
        description="Quantity sold for commerce", json_schema_extra={"unit": "ton"}
    )
    comercio_valor_producao: Decimal | None = Field(
        description="Production value attributed to commerce",
        json_schema_extra={"unit": "BRL"},
    )
    comercio_valor_venda: Decimal | None = Field(
        description="Sales value attributed to commerce",
        json_schema_extra={"unit": "BRL"},
    )


class PaIndexadoresLavouraPermanenteCenso2017(_BaseRow):
    tipo_agricultura: str | None = _TIPO_AGRI
    produto: str | None = _PRODUTO
    quantidade_estabelecimentos: Decimal | None = Field(
        description="Number of establishments", json_schema_extra={"unit": "head_count"}
    )
    quantidade_produzida: Decimal | None = Field(
        description="Total quantity produced", json_schema_extra={"unit": "ton"}
    )
    quantidade_vendida: Decimal | None = Field(
        description="Total quantity sold", json_schema_extra={"unit": "ton"}
    )
    comercio_quantidade_produzida: Decimal | None = Field(
        description="Quantity sold (commerce alias)",
        json_schema_extra={"unit": "ton"},
    )
    autoconsumo_quantidade_vendida: Decimal | None = Field(
        description="Produced minus sold (self-consumption)",
        json_schema_extra={"unit": "ton"},
    )
    valor_producao: Decimal | None = Field(
        description="Total production value", json_schema_extra={"unit": "BRL"}
    )
    valor_venda: Decimal | None = Field(
        description="Total sales value", json_schema_extra={"unit": "BRL"}
    )
    comercio_valor_producao: Decimal | None = Field(
        description="Sales value (commerce alias)", json_schema_extra={"unit": "BRL"}
    )
    autoconsumo_valor_producao: Decimal | None = Field(
        description="Production minus sales (self-consumption)",
        json_schema_extra={"unit": "BRL"},
    )
    area_colhida: Decimal | None = Field(
        description="Harvested area", json_schema_extra={"unit": "hectare"}
    )
    area_plantada: Decimal | None = Field(
        description="Planted area", json_schema_extra={"unit": "hectare"}
    )


class PaIndexadoresExtracaoVegetalCenso2006(_BaseRow):
    tipo_agricultura: str | None = _TIPO_AGRI
    produto: str | None = _PRODUTO
    quantidade_estabelecimentos: Decimal | None = Field(
        description="Number of establishments", json_schema_extra={"unit": "head_count"}
    )
    quantidade_produzida: Decimal | None = Field(
        description="Total quantity produced", json_schema_extra={"unit": "ton"}
    )
    quantidade_vendida: Decimal | None = Field(
        description="Total quantity sold", json_schema_extra={"unit": "ton"}
    )
    valor_producao: Decimal | None = Field(
        description="Total production value", json_schema_extra={"unit": "BRL"}
    )
    valor_venda: Decimal | None = Field(
        description="Total sales value", json_schema_extra={"unit": "BRL"}
    )
    autoconsumo_quantidade_estabelecimentos: Decimal | None = Field(
        description="Establishments reporting self-consumption",
        json_schema_extra={"unit": "head_count"},
    )
    autoconsumo_quantidade_produzida: Decimal | None = Field(
        description="Quantity produced for self-consumption",
        json_schema_extra={"unit": "ton"},
    )
    autoconsumo_quantidade_vendida: Decimal | None = Field(
        description="Quantity sold for self-consumption",
        json_schema_extra={"unit": "ton"},
    )
    autoconsumo_valor_producao: Decimal | None = Field(
        description="Production value for self-consumption",
        json_schema_extra={"unit": "BRL"},
    )
    autoconsumo_valor_venda: Decimal | None = Field(
        description="Sales value for self-consumption",
        json_schema_extra={"unit": "BRL"},
    )
    comercio_quantidade_estabelecimentos: Decimal | None = Field(
        description="Establishments reporting commerce", json_schema_extra={"unit": "head_count"}
    )
    comercio_quantidade_produzida: Decimal | None = Field(
        description="Quantity produced for commerce", json_schema_extra={"unit": "ton"}
    )
    comercio_quantidade_vendida: Decimal | None = Field(
        description="Quantity sold for commerce", json_schema_extra={"unit": "ton"}
    )
    comercio_valor_producao: Decimal | None = Field(
        description="Production value for commerce", json_schema_extra={"unit": "BRL"}
    )
    comercio_valor_venda: Decimal | None = Field(
        description="Sales value for commerce", json_schema_extra={"unit": "BRL"}
    )


class PaIndexadoresExtracaoVegetalCenso2017(_BaseRow):
    tipo_agricultura: str | None = _TIPO_AGRI
    produto: str | None = _PRODUTO
    quantidade_estabelecimentos: Decimal | None = Field(
        description="Number of establishments", json_schema_extra={"unit": "head_count"}
    )
    quantidade_produzida: Decimal | None = Field(
        description="Total quantity produced", json_schema_extra={"unit": "ton"}
    )
    quantidade_vendida: Decimal | None = Field(
        description="Total quantity sold", json_schema_extra={"unit": "ton"}
    )
    comercio_quantidade_produzida: Decimal | None = Field(
        description="Sold quantity (commerce alias)", json_schema_extra={"unit": "ton"}
    )
    autoconsumo_quantidade_vendida: Decimal | None = Field(
        description="Produced minus sold (self-consumption)",
        json_schema_extra={"unit": "ton"},
    )
    valor_producao: Decimal | None = Field(
        description="Total production value", json_schema_extra={"unit": "BRL"}
    )
    valor_venda: Decimal | None = Field(
        description="Total sales value", json_schema_extra={"unit": "BRL"}
    )
    comercio_valor_producao: Decimal | None = Field(
        description="Sales value (commerce alias)", json_schema_extra={"unit": "BRL"}
    )
    autoconsumo_valor_producao: Decimal | None = Field(
        description="Production minus sales (self-consumption)",
        json_schema_extra={"unit": "BRL"},
    )


class PaIndexadoresDespesasCenso2006_2017(_BaseRow):
    tipo_agricultura: str | None = _TIPO_AGRI
    tipo_despesa: str | None = Field(
        description="Expense type reported by Censo Agropecuário",
        json_schema_extra={"unit": "code"},
    )
    quantidade_estabelecimentos_fizeram_despesa: Decimal | None = Field(
        description="Establishments that incurred this expense",
        json_schema_extra={"unit": "head_count"},
    )
    valor_despesa: Decimal | None = Field(
        description="Total expense value", json_schema_extra={"unit": "BRL"}
    )


class PaIndexadoresLavouraPermanentePam(_BaseRow):
    produto: str | None = _PRODUTO
    quantidade_produzida: Decimal | None = Field(
        description="Quantity produced (PAM permanente)", json_schema_extra={"unit": "ton"}
    )
    valor_producao: Decimal | None = Field(
        description="Production value (PAM permanente)", json_schema_extra={"unit": "BRL"}
    )
    area_destinada_colheita: Decimal | None = Field(
        description="Area destined for harvest", json_schema_extra={"unit": "hectare"}
    )
    area_colhida: Decimal | None = Field(
        description="Harvested area", json_schema_extra={"unit": "hectare"}
    )
    rendimento_medio_producao: Decimal | None = Field(
        description="Average production yield", json_schema_extra={"unit": "dimensionless"}
    )


class PaIndexadoresLavouraTemporariaPam(_BaseRow):
    produto: str | None = _PRODUTO
    quantidade_produzida: Decimal | None = Field(
        description="Quantity produced (PAM temporária)", json_schema_extra={"unit": "ton"}
    )
    valor_producao: Decimal | None = Field(
        description="Production value (PAM temporária)", json_schema_extra={"unit": "BRL"}
    )
    area_plantada: Decimal | None = Field(
        description="Planted area", json_schema_extra={"unit": "hectare"}
    )
    area_colhida: Decimal | None = Field(
        description="Harvested area", json_schema_extra={"unit": "hectare"}
    )
    rendimento_medio_producao: Decimal | None = Field(
        description="Average production yield", json_schema_extra={"unit": "dimensionless"}
    )


class PaIndexadoresExtracaoVegetalPevs(_BaseRow):
    produto: str | None = _PRODUTO
    quantidade_produzida: Decimal | None = Field(
        description="Quantity produced (PEVS)", json_schema_extra={"unit": "ton"}
    )
    valor_producao: Decimal | None = Field(
        description="Production value (PEVS)", json_schema_extra={"unit": "BRL"}
    )
