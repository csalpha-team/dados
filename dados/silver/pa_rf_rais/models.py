"""Silver schemas for RAIS (Relação Anual de Informações Sociais) — Pará."""

from __future__ import annotations

from decimal import Decimal

from pydantic import BaseModel, Field


class PaRfRaisUpRais(BaseModel):
    ano: int = Field(description="Reference year", json_schema_extra={"unit": "YYYY"})
    regiao_integracao: str = Field(
        description=(
            "Região de integração do Pará, derivada do código do município no raw "
            "('Não classificado' para municípios fora do mapeamento)."
        ),
        json_schema_extra={"unit": "code"},
    )
    cnae_2_subclasse: str = Field(
        description="Subclasse CNAE 2.0 de 7 dígitos do vínculo",
        json_schema_extra={"unit": "code"},
    )
    cnae_2_subclasse_descricao_subclasse: str | None = Field(
        description="Descrição da subclasse CNAE 2.0",
        json_schema_extra={"unit": "code"},
    )
    cnae_2_subclasse_descricao_secao: str | None = Field(
        description="Descrição da seção CNAE 2.0 a que a subclasse pertence",
        json_schema_extra={"unit": "code"},
    )
    remuneracao_media_dezembro: Decimal | None = Field(
        description=(
            "Remuneração média de dezembro dos vínculos ativos em 31/12, em reais "
            "correntes do ano de referência (sem deflacionamento)."
        ),
        json_schema_extra={"unit": "BRL"},
    )
    quantidade_vinculos_ativos: int = Field(
        description="Número de vínculos ativos em 31/12 no recorte",
        json_schema_extra={"unit": "head_count"},
    )
    massa_salarial: Decimal | None = Field(
        description=(
            "Massa salarial de dezembro (remuneração média × vínculos ativos), em "
            "reais correntes do ano de referência."
        ),
        json_schema_extra={"unit": "BRL"},
    )
