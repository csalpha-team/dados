"""Pydantic schemas for the L1 export artefacts (`dados.export.dump_gold_l1`).

Two output patterns feed the downstream Layer 1 algorithm:

- **Flat série** — one metric per file, shape
  ``nome_regiao_integracao -> produto -> ano(str) -> float``. Used by the 12
  ``{dataset}_{quantidade|valor}.json`` files. Monetary values are in
  ``1000xBRL`` (Mil Reais); quantities carry the product-specific unit reported
  by IBGE (ton, mil cachos, m³, ...), so a single label cannot describe them.

- **Vetores de autoconsumo** — the hierarchical census-only artefact
  (``censo_autoconsumo.json``) carrying the commerce split per leaf.
"""

from __future__ import annotations

from typing import Dict

from pydantic import BaseModel, Field, RootModel

# nome_regiao_integracao -> produto -> ano (str) -> valor
SerieRegionalMap = Dict[str, Dict[str, Dict[str, float]]]


class SerieRegional(RootModel[SerieRegionalMap]):
    """Flat metric série: ``regiao -> produto -> ano(str) -> valor``.

    Holds a single metric (``quantidade_produzida`` *or* ``valor_producao``).
    Valor is expressed in ``1000xBRL``; quantidade in the product-specific unit.
    """


class VetorAutoconsumo(BaseModel):
    """Leaf of ``censo_autoconsumo.json`` — total vs commerce-destined output."""

    quantidade_produzida: float = Field(
        description="Quantidade total produzida na unidade de medida do produto",
        json_schema_extra={"unit": "product-specific"},
    )
    valor_producao: float = Field(
        description="Valor financeiro da produção",
        json_schema_extra={"unit": "1000xBRL"},
    )
    comercio_quantidade_produzida: float = Field(
        description="Quantidade produzida destinada ao comércio",
        json_schema_extra={"unit": "product-specific"},
    )
    comercio_valor_producao: float = Field(
        description="Valor financeiro da produção destinada ao comércio",
        json_schema_extra={"unit": "1000xBRL"},
    )


# nome_pesquisa -> tipo_pesquisa -> regiao -> produto -> ano (str) -> leaf
VetoresProducaoRuralMap = Dict[
    str, Dict[str, Dict[str, Dict[str, Dict[str, VetorAutoconsumo]]]]
]


class VetoresProducaoRural(BaseModel):
    """Hierarchical census artefact, top-level key ``vetores_producao_rural``."""

    vetores_producao_rural: VetoresProducaoRuralMap
