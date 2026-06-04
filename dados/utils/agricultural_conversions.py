"""Cross-zone pure-math helpers for IBGE agricultural data.

Pure functions with no I/O — safe to import from any zone.
"""

import pandas as pd


def currency_fix(row):
    """
    Valor da produção (Mil Cruzeiros [1974 a 1985, 1990 a 1992], Mil Cruzados [1986 a 1988],
    Mil Cruzados Novos [1989], Mil Cruzeiros Reais [1993], Mil Reais [1994 a 2022])
    Verificado em http://www.igf.com.br/calculadoras/conversor/conversor.htm
    """

    if 1974 <= row["ano"] <= 1985:
        return row["valor_producao"] / (1000**4 * 2.75)
    elif 1986 <= row["ano"] <= 1988:
        return row["valor_producao"] / (1000**3 * 2.75)
    elif row["ano"] == 1989:
        return row["valor_producao"] / (1000**2 * 2.75)
    elif 1990 <= row["ano"] <= 1992:
        return row["valor_producao"] / (1000**2 * 2.75)
    elif row["ano"] == 1993:
        return row["valor_producao"] / (1000 * 2.75)
    else:
        return row["valor_producao"]


def products_weight_ratio_fix(row):
    """
    2 - A partir do ano de 2001 as quantidades produzidas dos produtos abacate, banana,
    caqui, figo, goiaba, laranja, limão, maçã, mamão, manga, maracujá, marmelo, pera,
    pêssego e tangerina passam a ser expressas em toneladas. Nos anos anteriores eram
    expressas em mil frutos, com exceção da banana, que era expressa em mil cachos. O
    rendimento médio passa a ser expresso em Kg/ha. Nos anos anteriores era expresso
    em frutos/ha, com exceção da banana, que era expressa em cachos/ha.
    3 - Veja em o documento
    https://sidra.ibge.gov.br/content/documentos/pam/AlteracoesUnidadesMedidaFrutas.pdf
    com as alterações de unidades de medida das frutíferas ocorridas em 2001 e a tabela
    de conversão fruto x quilograma.
    """

    DICIONARIO_DE_PROPORCOES = {
        "Abacate": 0.38,
        "Banana (cacho)": 10.20,
        "Caqui": 0.18,
        "Figo": 0.09,
        "Goiaba": 0.16,
        "Larajna": 0.16,
        "Limão": 0.10,
        "Maçã": 0.15,
        "Mamão": 0.80,
        "Manga": 0.31,
        "Maracujá": 0.15,
        "Marmelo": 0.19,
        "Pera": 0.17,
        "Pêra": 0.17,  # Para garantir, pois nos dados parece que só há Pera, sem acento
        "Pêssego": 0.13,
        "Tangerina": 0.15,
        "Melancia": 6.08,
        "Melão": 1.39,
    }

    if row["ano"] >= 2001:
        return row

    if (
        pd.isna(row["quantidade_produzida"])
        or pd.isna(row["area_colhida"])
        or row["quantidade_produzida"] == 0
        or row["area_colhida"] == 0
    ):
        return row

    if row["produto"] not in DICIONARIO_DE_PROPORCOES.keys():
        return row

    quantidade_produzida = (
        row["quantidade_produzida"] * DICIONARIO_DE_PROPORCOES[row["produto"]]
    )

    rendimento_medio_producao = (
        quantidade_produzida / row["area_colhida"] * 1000
    )  # kg / ha

    row["quantidade_produzida"] = quantidade_produzida
    row["rendimento_medio_producao"] = rendimento_medio_producao

    return row


def censo_quantity_to_weight(
    df: pd.DataFrame,
    qty_cols: list[str],
    fator_mil_m3: float,
    kg_por_fruto: dict[str, float],
    unidade_col: str = "unidade_medida",
    produto_col: str = "produto",
) -> pd.DataFrame:
    """Homogeneíza as quantidades do Censo Agropecuário para toneladas.

    Dirigido pela coluna ``unidade_medida`` ingerida dos metadados do IBGE (por
    produto). Trata as duas unidades não-tonelada com massa equivalente:

    - ``'Mil metros cúbicos'`` (lenha, madeira em toras): multiplica por
      ``fator_mil_m3`` (= 1000 m³ × densidade t/m³).
    - ``'Mil frutos'`` (coco-da-baía, graviola, jaca, abacaxi): multiplica pelo
      peso médio do fruto em kg (``kg_por_fruto[produto]``); como o valor está em
      milhares de frutos, ``toneladas = quantidade × kg_por_fruto``.

    As colunas em ``qty_cols`` (produzida/vendida) são convertidas em conjunto, e a
    ``unidade_medida`` efetiva das linhas convertidas passa a ``'Toneladas'``.
    Contagens (``'Mil unidades'`` — mudas), ``'Toneladas'`` e ``None`` (Total)
    ficam intactos. Frutos em ``'Mil frutos'`` sem fator mapeado não são
    convertidos (preservam a unidade nativa).
    """
    df = df.copy()

    m3_mask = df[unidade_col] == "Mil metros cúbicos"
    for col in qty_cols:
        df.loc[m3_mask, col] = df.loc[m3_mask, col].astype(float) * fator_mil_m3
    df.loc[m3_mask, unidade_col] = "Toneladas"

    fator_fruto = df[produto_col].map(kg_por_fruto)
    fruto_mask = (df[unidade_col] == "Mil frutos") & fator_fruto.notna()
    for col in qty_cols:
        df.loc[fruto_mask, col] = (
            df.loc[fruto_mask, col].astype(float) * fator_fruto[fruto_mask]
        )
    df.loc[fruto_mask, unidade_col] = "Toneladas"

    return df


def pevs_volume_to_weight(
    df: pd.DataFrame,
    fator_ton_m3: float,
    unidade_col: str = "unidade_medida",
    qty_col: str = "quantidade_produzida",
) -> pd.DataFrame:
    """Converte a quantidade de m³ para toneladas onde a unidade nativa é 'Metros cúbicos'.

    A PEVS informa lenha, madeira em tora e nó-de-pinho em metros cúbicos e os demais
    produtos em massa. Dirigido pela coluna ``unidade_medida`` ingerida dos metadados do
    IBGE: multiplica as linhas em m³ pela densidade (t/m³) e marca a unidade efetiva como
    'Toneladas'. 'Mil árvores' (árvores abatidas) e 'Toneladas' permanecem intactos.
    """
    df = df.copy()
    mask = df[unidade_col] == "Metros cúbicos"
    df.loc[mask, qty_col] = df.loc[mask, qty_col].astype(float) * fator_ton_m3
    df.loc[mask, unidade_col] = "Toneladas"
    return df
