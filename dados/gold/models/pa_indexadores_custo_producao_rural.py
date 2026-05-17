"""Gold schemas for pa_indexadores_custo_producao_rural."""

from __future__ import annotations

from decimal import Decimal

from pydantic import BaseModel, Field


class PaIndexadoresCustoDespesasCenso2006_2017(BaseModel):
    ano: int = Field(description="Reference year", json_schema_extra={"unit": "YYYY"})
    id_municipio: str = Field(
        description="IBGE 7-digit municipality code",
        json_schema_extra={"unit": "code"},
    )
    nome: str | None = Field(
        description="Municipality name", json_schema_extra={"unit": "code"}
    )
    nome_regiao_integracao: str | None = Field(
        description="Pará integration region (RI)",
        json_schema_extra={"unit": "code"},
    )
    sigla_uf: str | None = Field(
        description="State (UF) two-letter code",
        json_schema_extra={"unit": "code"},
    )
    tipo_agricultura: str | None = Field(
        description="Agriculture type from Censo Agropecuário",
        json_schema_extra={"unit": "code"},
    )
    tipo_despesa: str | None = Field(
        description="Expense type reported by Censo Agropecuário",
        json_schema_extra={"unit": "code"},
    )
    quantidade_estabelecimentos_fizeram_despesa: Decimal | None = Field(
        description="Establishments that incurred this expense",
        json_schema_extra={"unit": "head_count"},
    )
    valor_despesa: Decimal | None = Field(
        description="Total expense value",
        json_schema_extra={"unit": "BRL"},
    )
