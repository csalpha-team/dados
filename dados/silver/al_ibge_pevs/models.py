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
        description=(
            "Quantidade produzida na extração vegetal, em toneladas. Produtos madeireiros "
            "(lenha, madeira em tora, nó-de-pinho), informados pelo IBGE em m³, são "
            "convertidos para toneladas via densidade de 0,5 t/m³. 'árvores abatidas' "
            "permanece em 'Mil árvores' (contagem; zero na Amazônia Legal)."
        ),
        json_schema_extra={"unit": "ton"},
    )
    valor_producao: Decimal | None = Field(
        description="Value of vegetal-extraction production (deflated by currency_fix)",
        json_schema_extra={"unit": "1000xBRL"},
    )
    unidade_medida: str | None = Field(
        description=(
            "Unidade de medida efetiva da quantidade produzida (Toneladas; 'Mil árvores' "
            "para árvores abatidas). Origem: metadados IBGE agregado 289, classificação 193."
        ),
        json_schema_extra={"unit": "code"},
    )
