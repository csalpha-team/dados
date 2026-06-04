"""Gold schemas for pa_indexadores_valor_producao_rural."""

from __future__ import annotations

from decimal import Decimal

from pydantic import BaseModel, Field


class PaIndexadoresValorProducaoCenso2006_2017(BaseModel):
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
    tipo_producao: str | None = Field(
        description="Production type label",
        json_schema_extra={"unit": "code"},
    )
    quantidade_estabelecimentos_produtivos: Decimal | None = Field(
        description="Number of productive establishments",
        json_schema_extra={"unit": "head_count"},
    )
    valor_producao: Decimal | None = Field(
        description="Total production value, in thousands of BRL (IBGE source unit 'Mil Reais')",
        json_schema_extra={"unit": "1000xBRL"},
    )


class PaIndexadoresPessoalOcupadoCenso2017(BaseModel):
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
        description="Agriculture type from Censo Agropecuário 2017",
        json_schema_extra={"unit": "code"},
    )
    pessoal_total_ocupado: Decimal | None = Field(
        description="Total persons employed in rural establishments",
        json_schema_extra={"unit": "head_count"},
    )
    quantidade_total_estabecimentos: Decimal | None = Field(
        description="Total number of establishments",
        json_schema_extra={"unit": "head_count"},
    )
    pessoal_ocupado_familia: Decimal | None = Field(
        description="Persons employed who are part of the producer family",
        json_schema_extra={"unit": "head_count"},
    )
    quantidade_estabecimentos_pessoal_ocupado_familia: Decimal | None = Field(
        description="Establishments with family-member employees",
        json_schema_extra={"unit": "head_count"},
    )
    pessoal_ocupado_fora_familia: Decimal | None = Field(
        description="Persons employed from outside the producer family",
        json_schema_extra={"unit": "head_count"},
    )
    quantidade_estabecimentos_pessoal_ocupado_fora_familia: Decimal | None = Field(
        description="Establishments with non-family employees",
        json_schema_extra={"unit": "head_count"},
    )
