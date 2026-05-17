# -*- coding: utf-8 -*-
"""Nomes canônicos de colunas usadas nos pivots SQL dos scripts silver.

Os mapeamentos `source → canônico` continuam locais em cada script silver,
pois variam por pesquisa SIDRA. Esta constante apenas fixa o nome final."""

from typing import Final

AREA_PLANTADA: Final = "area_plantada"
AREA_COLHIDA: Final = "area_colhida"
QUANTIDADE_PRODUZIDA: Final = "quantidade_produzida"
VALOR_PRODUCAO: Final = "valor_producao"
