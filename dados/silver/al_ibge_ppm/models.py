"""Silver schemas for IBGE PPM (Pesquisa da Pecuária Municipal) — Amazônia Legal."""

from __future__ import annotations

from decimal import Decimal

from pydantic import BaseModel, Field


class AlIbgePpmEfetivoRebanhos(BaseModel):
    ano: int = Field(description="Reference year", json_schema_extra={"unit": "YYYY"})
    id_municipio: str = Field(
        description="IBGE 7-digit municipality code",
        json_schema_extra={"unit": "code"},
    )
    produto: str = Field(
        description=(
            "Espécie do rebanho conforme o IBGE (agregado 3939, classificação 79): "
            "Bovino, Bubalino, Caprino, Codornas, Equino, Galináceos (total e galinhas), "
            "Ovino e Suíno (total e matrizes)."
        ),
        json_schema_extra={"unit": "code"},
    )
    efetivo_rebanhos: Decimal | None = Field(
        description=(
            "Efetivo dos rebanhos em 31/12, em cabeças. Sentinelas não numéricas do "
            "IBGE ('-', '..', '...', 'X') são tratadas por fix_ibge_digits."
        ),
        json_schema_extra={"unit": "head_count"},
    )
    unidade_medida: str | None = Field(
        description="Unidade de medida de origem do efetivo (Cabeças).",
        json_schema_extra={"unit": "code"},
    )


class AlIbgePpmProducaoOrigemAnimal(BaseModel):
    ano: int = Field(description="Reference year", json_schema_extra={"unit": "YYYY"})
    id_municipio: str = Field(
        description="IBGE 7-digit municipality code",
        json_schema_extra={"unit": "code"},
    )
    produto: str = Field(
        description=(
            "Produto de origem animal conforme o IBGE (agregado 74, classificação 80): "
            "Leite, Ovos de galinha, Ovos de codorna, Mel de abelha, Casulos do "
            "bicho-da-seda e Lã."
        ),
        json_schema_extra={"unit": "code"},
    )
    producao: Decimal | None = Field(
        description=(
            "Quantidade produzida. A unidade varia por produto e está em "
            "``unidade_medida`` (Mil litros para leite, Mil dúzias para ovos, "
            "Quilogramas para mel, casulos e lã)."
        ),
        json_schema_extra={"unit": "code"},
    )
    valor_producao: Decimal | None = Field(
        description=(
            "Valor da produção. Moedas históricas (Cruzeiros/Cruzados/Cruzados "
            "Novos/Cruzeiros Reais) normalizadas por currency_fix para a base "
            "Mil Reais."
        ),
        json_schema_extra={"unit": "1000xBRL"},
    )
    unidade_medida: str | None = Field(
        description=(
            "Unidade de medida efetiva da quantidade produzida, vinda dos metadados "
            "do IBGE (agregado 74, classificação 80)."
        ),
        json_schema_extra={"unit": "code"},
    )


class AlIbgePpmProducaoAquicultura(BaseModel):
    ano: int = Field(description="Reference year", json_schema_extra={"unit": "YYYY"})
    id_municipio: str = Field(
        description="IBGE 7-digit municipality code",
        json_schema_extra={"unit": "code"},
    )
    produto: str = Field(
        description=(
            "Produto da aquicultura conforme o IBGE (agregado 3940, classificação 654)."
        ),
        json_schema_extra={"unit": "code"},
    )
    producao: Decimal | None = Field(
        description=(
            "Quantidade produzida na aquicultura. A unidade varia por produto e "
            "está em ``unidade_medida`` (em geral Quilogramas; Milheiros para "
            "alevinos e formas jovens)."
        ),
        json_schema_extra={"unit": "code"},
    )
    valor_producao: Decimal | None = Field(
        description=(
            "Valor da produção da aquicultura. A série começa em 2013, já na base "
            "Mil Reais; currency_fix é aplicado por consistência e é identidade "
            "a partir de 1994."
        ),
        json_schema_extra={"unit": "1000xBRL"},
    )
    unidade_medida: str | None = Field(
        description=(
            "Unidade de medida efetiva da quantidade produzida, vinda dos metadados "
            "do IBGE (agregado 3940, classificação 654)."
        ),
        json_schema_extra={"unit": "code"},
    )
