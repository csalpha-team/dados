import json
from collections import defaultdict
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Dict, Optional, Sequence, Tuple

import pandas as pd


COLUNAS_OBRIGATORIAS_EXPORTACAO = [
    "ano",
    "id_ncm",
    "nome_ncm_portugues",
    "sigla_uf_ncm",
    "valor_fob_dolar",
]

COLUNAS_FINAIS = ["ano", "produto", "valor_fob_dolar", "valor_fob_real", "coeff"]
TaxaCambio = float | Mapping[int, float]

# Aliases para compatibilidade com a interface anterior em ingles.
REQUIRED_EXPORT_COLUMNS = COLUNAS_OBRIGATORIAS_EXPORTACAO
FINAL_COLUMNS = COLUNAS_FINAIS


def _escapar_literal_sql(valor: str) -> str:
    return valor.replace("'", "''")


def listar_colunas_tabela(db: Any, esquema: str, tabela: str) -> set[str]:
    esquema_escapado = _escapar_literal_sql(esquema)
    tabela_escapada = _escapar_literal_sql(tabela)

    consulta = f"""
    select column_name
    from information_schema.columns
    where table_schema = '{esquema_escapado}'
      and table_name = '{tabela_escapada}'
    order by ordinal_position
    """

    colunas_df = db.download_data(consulta)
    if colunas_df.empty:
        return set()

    return set(colunas_df["column_name"].astype(str))


def construir_consulta_exportacao(
    db: Any,
    esquema_origem: str,
    tabela_origem: str,
    esquema_ncm: str,
    tabela_ncm: str,
) -> str:
    colunas_origem = listar_colunas_tabela(db, esquema_origem, tabela_origem)
    if not colunas_origem:
        raise ValueError(
            f"Tabela de origem nao encontrada: {esquema_origem}.{tabela_origem}"
        )

    colunas_obrigatorias_origem = {"ano", "id_ncm", "sigla_uf_ncm", "valor_fob_dolar"}
    colunas_faltantes_origem = sorted(colunas_obrigatorias_origem - colunas_origem)

    if colunas_faltantes_origem:
        raise ValueError(
            "Colunas obrigatorias ausentes na tabela de origem "
            f"{esquema_origem}.{tabela_origem}: {', '.join(colunas_faltantes_origem)}"
        )

    if "nome_ncm_portugues" in colunas_origem:
        expressao_nome = "nullif(trim(c.nome_ncm_portugues), '')"
        clausula_join = ""
    else:
        colunas_ncm = listar_colunas_tabela(db, esquema_ncm, tabela_ncm)
        if "nome_ncm_portugues" not in colunas_ncm:
            raise ValueError(
                "Nao foi possivel obter 'nome_ncm_portugues'. "
                f"A coluna nao existe em {esquema_origem}.{tabela_origem} e nem em "
                f"{esquema_ncm}.{tabela_ncm}."
            )

        clausula_join = (
            f"left join {esquema_ncm}.{tabela_ncm} as n on c.id_ncm = n.id_ncm"
        )
        expressao_nome = "nullif(trim(n.nome_ncm_portugues), '')"

    return f"""
    select
        c.ano,
        c.id_ncm,
        c.sigla_uf_ncm,
        {expressao_nome} as nome_ncm_portugues,
        c.valor_fob_dolar
    from {esquema_origem}.{tabela_origem} as c
    {clausula_join}
    where c.ano is not null
      and c.id_ncm is not null
      and c.valor_fob_dolar is not null
    """


def _construir_anos_previsao(valor_configuracao: Any) -> list[int]:
    if isinstance(valor_configuracao, dict):
        inicio = int(
            valor_configuracao.get("start", valor_configuracao.get("inicio", 1995))
        )
        fim = int(valor_configuracao.get("end", valor_configuracao.get("fim", 2023)))
        if fim < inicio:
            raise ValueError(
                "anos_previsao.fim deve ser maior ou igual a anos_previsao.inicio"
            )
        return list(range(inicio, fim + 1))

    if isinstance(valor_configuracao, list):
        anos = sorted({int(ano) for ano in valor_configuracao})
        if not anos:
            raise ValueError("anos_previsao nao pode ser vazio")
        return anos

    return list(range(1995, 2024))


def _resolver_caminho_configuracao(
    caminho_configuracao: Path,
    caminho_referenciado: str,
) -> Path:
    caminho = Path(caminho_referenciado).expanduser()
    if caminho.is_absolute():
        return caminho
    return (caminho_configuracao.parent / caminho).resolve()


def _extrair_nome_ncm(item: Any) -> str:
    if isinstance(item, dict):
        return str(
            item.get(
                "nome_ncm",
                item.get("nome_ncm_portugues", item.get("NO_NCM_POR", "")),
            )
        ).strip()

    return str(item).strip()


def _carregar_taxas_cambio_csv(
    caminho_configuracao: Path,
    configuracao_taxa: Mapping[str, Any],
) -> dict[int, float]:
    caminho_bruto = configuracao_taxa.get(
        "arquivo",
        configuracao_taxa.get("path", configuracao_taxa.get("caminho")),
    )
    if not caminho_bruto:
        raise ValueError(
            "Configuracao de taxa de cambio em CSV precisa de 'arquivo' ou 'path'"
        )

    caminho_csv = _resolver_caminho_configuracao(
        caminho_configuracao, str(caminho_bruto)
    )
    coluna_ano = str(configuracao_taxa.get("coluna_ano", "ano"))
    coluna_taxa = str(configuracao_taxa.get("coluna_taxa", "taxa_cambio"))

    taxas_df = pd.read_csv(caminho_csv)
    colunas_faltantes = [
        coluna for coluna in [coluna_ano, coluna_taxa] if coluna not in taxas_df.columns
    ]
    if colunas_faltantes:
        raise ValueError(
            "Colunas obrigatorias ausentes no CSV de taxa de cambio "
            f"{caminho_csv}: {', '.join(colunas_faltantes)}"
        )

    dados = taxas_df[[coluna_ano, coluna_taxa]].copy()
    dados[coluna_ano] = pd.to_numeric(dados[coluna_ano], errors="coerce")
    dados[coluna_taxa] = pd.to_numeric(dados[coluna_taxa], errors="coerce")
    dados = dados.dropna(subset=[coluna_ano, coluna_taxa])

    taxas = {
        int(linha[coluna_ano]): float(linha[coluna_taxa])
        for _, linha in dados.iterrows()
    }
    if not taxas:
        raise ValueError(f"Nenhuma taxa de cambio valida encontrada em {caminho_csv}")

    taxas_invalidas = {ano: taxa for ano, taxa in taxas.items() if taxa < 0}
    if taxas_invalidas:
        anos_invalidos = ", ".join(str(ano) for ano in sorted(taxas_invalidas))
        raise ValueError(
            "taxa_cambio_brl_por_usd deve ser nao-negativa. "
            f"Anos invalidos: {anos_invalidos}"
        )

    return taxas


def _carregar_taxas_cambio(
    caminho_configuracao: Path,
    valor_configuracao: Any,
) -> TaxaCambio:
    if isinstance(valor_configuracao, dict):
        if any(chave in valor_configuracao for chave in ["arquivo", "path", "caminho"]):
            return _carregar_taxas_cambio_csv(caminho_configuracao, valor_configuracao)

        taxas = {int(ano): float(taxa) for ano, taxa in valor_configuracao.items()}
        taxas_invalidas = {ano: taxa for ano, taxa in taxas.items() if taxa < 0}
        if taxas_invalidas:
            anos_invalidos = ", ".join(str(ano) for ano in sorted(taxas_invalidas))
            raise ValueError(
                "taxa_cambio_brl_por_usd deve ser nao-negativa. "
                f"Anos invalidos: {anos_invalidos}"
            )
        return taxas

    taxa_cambio_brl_por_usd = float(valor_configuracao)
    if taxa_cambio_brl_por_usd < 0:
        raise ValueError("taxa_cambio_brl_por_usd deve ser nao-negativa")
    return taxa_cambio_brl_por_usd


def _obter_taxa_cambio_ano(taxa_cambio_brl_por_usd: TaxaCambio, ano: int) -> float:
    if isinstance(taxa_cambio_brl_por_usd, Mapping):
        if ano not in taxa_cambio_brl_por_usd:
            raise ValueError(
                "Taxa de cambio ausente para o ano "
                f"{ano}. Ajuste anos_previsao ou a fonte de taxa_cambio."
            )
        return float(taxa_cambio_brl_por_usd[ano])

    return float(taxa_cambio_brl_por_usd)


def carregar_parametros_exportacao(
    caminho_configuracao: Path,
) -> tuple[
    dict[str, list[str]],
    dict[tuple[str, str], float],
    list[int],
    TaxaCambio,
    str,
]:
    with caminho_configuracao.open("r", encoding="utf-8") as file:
        config = json.load(file)

    preparacoes_produtos_raw = config.get(
        "composicao_produtos",
        config.get(
            "preparacoes_produtos",
            config.get("products_preparations", {}),
        ),
    )
    if not isinstance(preparacoes_produtos_raw, dict):
        raise ValueError(
            "preparacoes_produtos deve ser um dicionario no arquivo de configuracao"
        )

    preparacoes_produtos: dict[str, list[str]] = {}
    for produto, itens_ncm in preparacoes_produtos_raw.items():
        produto_normalizado = str(produto).strip()
        if not isinstance(itens_ncm, list):
            raise ValueError(
                "Cada valor de composicao_produtos/preparacoes_produtos deve ser "
                "uma lista de nomes NCM ou objetos com nome_ncm"
            )

        preparacoes_produtos[produto_normalizado] = [
            nome_ncm
            for nome_ncm in (_extrair_nome_ncm(item) for item in itens_ncm)
            if nome_ncm
        ]

    participacoes_especificas_raw = config.get(
        "participacoes_especificas",
        config.get("specific_shares", []),
    )
    if not isinstance(participacoes_especificas_raw, list):
        raise ValueError(
            "participacoes_especificas deve ser uma lista no arquivo de configuracao"
        )

    participacoes_especificas: dict[tuple[str, str], float] = {}
    for item in participacoes_especificas_raw:
        if not isinstance(item, dict):
            raise ValueError(
                "Cada item de participacoes_especificas deve ser um objeto"
            )

        produto = str(item.get("produto", item.get("product", ""))).strip()
        nome_ncm = str(item.get("nome_ncm", item.get("ncm_name", ""))).strip()

        if not produto or not nome_ncm:
            raise ValueError(
                "Cada item de participacoes_especificas precisa de 'produto' e "
                "'nome_ncm' validos"
            )

        participacao = float(item.get("participacao", item.get("share", 0.0)))
        if participacao < 0 or participacao > 1:
            raise ValueError(
                "Os valores de participacao devem estar no intervalo [0, 1]"
            )

        participacoes_especificas[(produto, nome_ncm)] = participacao

    anos = _construir_anos_previsao(
        config.get("anos_previsao", config.get("forecast_years"))
    )

    taxa_cambio_brl_por_usd = _carregar_taxas_cambio(
        caminho_configuracao,
        config.get(
            "taxa_cambio_brl_por_usd",
            config.get("exchange_rate_brl_per_usd", 1.0),
        ),
    )

    uf_alvo = (
        str(config.get("uf_alvo", config.get("target_state", "PA"))).strip().upper()
    )

    return (
        preparacoes_produtos,
        participacoes_especificas,
        anos,
        taxa_cambio_brl_por_usd,
        uf_alvo,
    )


def _prever_regressao_linear(
    anos_observados: list[int],
    valores_observados: list[float],
    anos_alvo: list[int],
    *,
    limitar_nao_negativo: bool,
) -> dict[int, float]:
    if not anos_observados:
        return {ano: 0.0 for ano in anos_alvo}

    if len(anos_observados) == 1:
        previsao = {ano: valores_observados[0] for ano in anos_alvo}
    else:
        quantidade_pontos = len(anos_observados)
        media_x = sum(anos_observados) / quantidade_pontos
        media_y = sum(valores_observados) / quantidade_pontos

        denominador = sum((x - media_x) ** 2 for x in anos_observados)
        if denominador == 0:
            inclinacao = 0.0
        else:
            numerador = sum(
                (x - media_x) * (y - media_y)
                for x, y in zip(anos_observados, valores_observados)
            )
            inclinacao = numerador / denominador

        intercepto = media_y - inclinacao * media_x
        previsao = {ano: inclinacao * ano + intercepto for ano in anos_alvo}

    if limitar_nao_negativo:
        previsao = {ano: max(0.0, valor) for ano, valor in previsao.items()}

    return previsao


def prever_exportacoes_linear(
    exportacoes_df: pd.DataFrame,
    anos: Sequence[int],
    *,
    coluna_ano: str = "ano",
    colunas_rotulo: Sequence[str] = ("id_ncm", "nome_ncm_portugues"),
    coluna_valor: str = "valor_fob_dolar",
    incluir_historico: bool = True,
    limitar_nao_negativo: bool = True,
) -> pd.DataFrame:
    anos_previsao = sorted({int(ano) for ano in anos})
    if not anos_previsao:
        raise ValueError("A lista de anos para previsao nao pode ser vazia")

    colunas_necessarias = [coluna_ano, coluna_valor, *colunas_rotulo]
    colunas_faltantes = [
        coluna for coluna in colunas_necessarias if coluna not in exportacoes_df.columns
    ]
    if colunas_faltantes:
        raise ValueError(
            "Colunas obrigatorias ausentes para previsao: "
            f"{', '.join(colunas_faltantes)}"
        )

    dados = exportacoes_df[colunas_necessarias].copy()
    dados[coluna_ano] = pd.to_numeric(dados[coluna_ano], errors="coerce")
    dados[coluna_valor] = pd.to_numeric(dados[coluna_valor], errors="coerce")

    for coluna in colunas_rotulo:
        dados[coluna] = dados[coluna].astype("string").str.strip()

    dados = dados.dropna(subset=[coluna_ano, coluna_valor, *colunas_rotulo]).copy()
    if dados.empty:
        return pd.DataFrame(columns=colunas_necessarias)

    dados[coluna_ano] = dados[coluna_ano].astype(int)
    agrupado = dados.groupby([coluna_ano, *colunas_rotulo], as_index=False)[
        coluna_valor
    ].sum()

    linhas_previsao: list[dict[str, Any]] = []
    for rotulos, grupo in agrupado.groupby(list(colunas_rotulo), dropna=False):
        ordenado = grupo.sort_values(coluna_ano)
        anos_observados = ordenado[coluna_ano].astype(int).tolist()
        valores_observados = ordenado[coluna_valor].astype(float).tolist()
        mapa_observado = dict(zip(anos_observados, valores_observados))

        mapa_previsto = _prever_regressao_linear(
            anos_observados,
            valores_observados,
            anos_previsao,
            limitar_nao_negativo=limitar_nao_negativo,
        )

        if not isinstance(rotulos, tuple):
            rotulos = (rotulos,)

        dados_rotulo = {coluna: valor for coluna, valor in zip(colunas_rotulo, rotulos)}

        for ano in anos_previsao:
            valor = mapa_previsto[ano]
            if incluir_historico and ano in mapa_observado:
                valor = mapa_observado[ano]

            linhas_previsao.append(
                {
                    coluna_ano: ano,
                    coluna_valor: float(valor),
                    **dados_rotulo,
                }
            )

    return pd.DataFrame(linhas_previsao)


def distribuir_exportacoes_por_produto(
    exportacoes_por_ncm: pd.DataFrame,
    preparacoes_produtos: Mapping[str, Sequence[str]],
    *,
    coluna_ncm: str = "nome_ncm_portugues",
    coluna_id_ncm: str = "id_ncm",
    valor_preenchimento: float = 0.0,
    participacoes_especificas: Optional[Mapping[Tuple[str, str], float]] = None,
) -> pd.DataFrame:
    if coluna_ncm not in exportacoes_por_ncm.columns:
        raise KeyError(
            f"Coluna '{coluna_ncm}' nao encontrada no dataframe de exportacoes"
        )

    colunas_metricas = [
        coluna
        for coluna in exportacoes_por_ncm.columns
        if coluna not in {coluna_ncm, coluna_id_ncm}
    ]

    dados_numericos = exportacoes_por_ncm.copy()
    for coluna in colunas_metricas:
        dados_numericos[coluna] = pd.to_numeric(
            dados_numericos[coluna], errors="coerce"
        )
    dados_numericos = dados_numericos.fillna(
        {coluna: valor_preenchimento for coluna in colunas_metricas}
    )

    ncm_para_produtos: Dict[str, set[str]] = defaultdict(set)
    for produto, nomes_ncm in preparacoes_produtos.items():
        for nome_ncm in nomes_ncm:
            ncm_para_produtos[str(nome_ncm)].add(str(produto))

    participacoes_normalizadas = {
        (str(produto), str(nome_ncm)): participacao
        for (produto, nome_ncm), participacao in (
            participacoes_especificas or {}
        ).items()
    }

    linhas_distribuidas: list[dict[str, Any]] = []
    for _, linha in dados_numericos.iterrows():
        nome_ncm = str(linha[coluna_ncm])
        produtos_relacionados = sorted(ncm_para_produtos.get(nome_ncm, []))
        if not produtos_relacionados:
            continue

        valores = linha[colunas_metricas]
        participacoes_definidas = {
            produto: participacoes_normalizadas[(produto, nome_ncm)]
            for produto in produtos_relacionados
            if (produto, nome_ncm) in participacoes_normalizadas
        }

        total_participacoes_definidas = sum(participacoes_definidas.values())
        if total_participacoes_definidas > 1.0 + 1e-12:
            raise ValueError(
                "A soma das participacoes especificas excede 1.0 para "
                f"'{nome_ncm}' ({total_participacoes_definidas:.4f})"
            )

        if participacoes_definidas:
            for produto, participacao in participacoes_definidas.items():
                linhas_distribuidas.append(
                    {"produto": produto, **(valores * participacao).to_dict()}
                )

            produtos_restantes = [
                produto
                for produto in produtos_relacionados
                if produto not in participacoes_definidas
            ]
            participacao_restante_total = max(0.0, 1.0 - total_participacoes_definidas)
            if produtos_restantes and participacao_restante_total > 0:
                participacao_restante = participacao_restante_total / len(
                    produtos_restantes
                )
                for produto in produtos_restantes:
                    linhas_distribuidas.append(
                        {
                            "produto": produto,
                            **(valores * participacao_restante).to_dict(),
                        }
                    )
        else:
            participacao_igual = 1.0 / len(produtos_relacionados)
            valores_distribuidos = valores * participacao_igual
            for produto in produtos_relacionados:
                linhas_distribuidas.append(
                    {"produto": produto, **valores_distribuidos.to_dict()}
                )

    if not linhas_distribuidas:
        return pd.DataFrame(
            columns=colunas_metricas, index=pd.Index([], name="produto")
        )

    return (
        pd.DataFrame(linhas_distribuidas)
        .groupby("produto", as_index=True)[colunas_metricas]
        .sum(min_count=1)
        .fillna(valor_preenchimento)
        .sort_index()
    )


def preparar_dados_coeficientes_exportacao(
    exportacoes_df: pd.DataFrame,
    preparacoes_produtos: Mapping[str, Sequence[str]],
    participacoes_especificas: Mapping[Tuple[str, str], float],
    anos: Sequence[int],
    taxa_cambio_brl_por_usd: TaxaCambio,
    uf_alvo: str,
) -> pd.DataFrame:
    colunas_faltantes = [
        coluna
        for coluna in COLUNAS_OBRIGATORIAS_EXPORTACAO
        if coluna not in exportacoes_df.columns
    ]
    if colunas_faltantes:
        raise ValueError(
            "Colunas obrigatorias ausentes no dataframe de exportacao: "
            f"{', '.join(colunas_faltantes)}"
        )

    dados = exportacoes_df[COLUNAS_OBRIGATORIAS_EXPORTACAO].copy()

    dados["ano"] = pd.to_numeric(dados["ano"], errors="coerce")
    dados["valor_fob_dolar"] = pd.to_numeric(dados["valor_fob_dolar"], errors="coerce")
    dados["nome_ncm_portugues"] = (
        dados["nome_ncm_portugues"].astype("string").str.strip()
    )
    dados["id_ncm"] = dados["id_ncm"].astype("string").str.strip()
    dados["sigla_uf_ncm"] = (
        dados["sigla_uf_ncm"].astype("string").str.strip().str.upper()
    )

    dados = dados.dropna(
        subset=["ano", "valor_fob_dolar", "nome_ncm_portugues", "id_ncm"]
    )

    if uf_alvo:
        dados = dados[dados["sigla_uf_ncm"] == uf_alvo]

    if dados.empty:
        return pd.DataFrame(columns=COLUNAS_FINAIS)

    dados["ano"] = dados["ano"].astype(int)
    agrupado = dados.groupby(["ano", "id_ncm", "nome_ncm_portugues"], as_index=False)[
        "valor_fob_dolar"
    ].sum()

    previsao_df = prever_exportacoes_linear(
        agrupado,
        anos,
        coluna_ano="ano",
        colunas_rotulo=("id_ncm", "nome_ncm_portugues"),
        coluna_valor="valor_fob_dolar",
        incluir_historico=True,
        limitar_nao_negativo=True,
    )

    distribuido_por_ano: list[pd.DataFrame] = []
    for ano in sorted({int(valor) for valor in anos}):
        dados_ano = previsao_df.loc[previsao_df["ano"] == ano]
        if dados_ano.empty:
            continue

        dados_ano_agrupados = (
            dados_ano.groupby("nome_ncm_portugues", as_index=False)
            .agg({"id_ncm": "first", "valor_fob_dolar": "sum"})
            .loc[:, ["nome_ncm_portugues", "id_ncm", "valor_fob_dolar"]]
        )

        distribuido = distribuir_exportacoes_por_produto(
            dados_ano_agrupados,
            preparacoes_produtos,
            participacoes_especificas=participacoes_especificas,
        )

        if distribuido.empty:
            continue

        distribuido = distribuido.reset_index()
        distribuido["ano"] = ano
        distribuido_por_ano.append(distribuido)

    if not distribuido_por_ano:
        return pd.DataFrame(columns=COLUNAS_FINAIS)

    final = pd.concat(distribuido_por_ano, ignore_index=True)
    final["taxa_cambio_brl_por_usd"] = final["ano"].map(
        lambda ano: _obter_taxa_cambio_ano(taxa_cambio_brl_por_usd, int(ano))
    )
    final["valor_fob_real"] = (
        pd.to_numeric(final["valor_fob_dolar"], errors="coerce")
        * final["taxa_cambio_brl_por_usd"]
        / 1000.0
    )

    totais_ano = final.groupby("ano")["valor_fob_real"].transform("sum")
    final["coeff"] = 0.0

    totais_nao_zero = totais_ano != 0
    final.loc[totais_nao_zero, "coeff"] = (
        final.loc[totais_nao_zero, "valor_fob_real"] / totais_ano[totais_nao_zero]
    )

    final = final[["ano", "produto", "valor_fob_dolar", "valor_fob_real", "coeff"]]
    final = final.sort_values(["ano", "produto"]).reset_index(drop=True)

    return final[COLUNAS_FINAIS]


def construir_json_coeficientes_exportacao(
    coeficientes_exportacao: pd.DataFrame,
) -> dict[str, list[dict[str, Any]]]:
    if coeficientes_exportacao.empty:
        return {}

    payload: dict[str, list[dict[str, Any]]] = {}
    for ano, dados_ano in coeficientes_exportacao.groupby("ano", as_index=False):
        payload[str(int(ano))] = dados_ano.drop(columns=["ano"]).to_dict(
            orient="records"
        )

    return payload


def salvar_json_coeficientes_exportacao(
    coeficientes_exportacao: pd.DataFrame,
    caminho_saida: Path,
) -> None:
    caminho_saida.parent.mkdir(parents=True, exist_ok=True)

    payload = construir_json_coeficientes_exportacao(coeficientes_exportacao)
    with caminho_saida.open("w", encoding="utf-8") as file:
        json.dump(payload, file, ensure_ascii=False, indent=2)


def list_table_columns(db: Any, schema: str, table: str) -> set[str]:
    return listar_colunas_tabela(db, schema, table)


def build_export_query(
    db: Any,
    source_schema: str,
    source_table: str,
    ncm_schema: str,
    ncm_table: str,
) -> str:
    return construir_consulta_exportacao(
        db,
        esquema_origem=source_schema,
        tabela_origem=source_table,
        esquema_ncm=ncm_schema,
        tabela_ncm=ncm_table,
    )


def load_export_parameters(
    config_path: Path,
) -> tuple[
    dict[str, list[str]],
    dict[tuple[str, str], float],
    list[int],
    TaxaCambio,
    str,
]:
    return carregar_parametros_exportacao(config_path)


def forecast_exports_linear(
    export_df: pd.DataFrame,
    years: Sequence[int],
    *,
    year_col: str = "ano",
    label_cols: Sequence[str] = ("id_ncm", "nome_ncm_portugues"),
    value_col: str = "valor_fob_dolar",
    include_history: bool = True,
    clamp_non_negative: bool = True,
) -> pd.DataFrame:
    return prever_exportacoes_linear(
        export_df,
        years,
        coluna_ano=year_col,
        colunas_rotulo=label_cols,
        coluna_valor=value_col,
        incluir_historico=include_history,
        limitar_nao_negativo=clamp_non_negative,
    )


def distribute_exports_by_product(
    exports_by_ncm: pd.DataFrame,
    products_preparations: Mapping[str, Sequence[str]],
    *,
    ncm_col: str = "nome_ncm_portugues",
    id_ncm_col: str = "id_ncm",
    fill_value: float = 0.0,
    specific_shares: Optional[Mapping[Tuple[str, str], float]] = None,
) -> pd.DataFrame:
    return distribuir_exportacoes_por_produto(
        exports_by_ncm,
        products_preparations,
        coluna_ncm=ncm_col,
        coluna_id_ncm=id_ncm_col,
        valor_preenchimento=fill_value,
        participacoes_especificas=specific_shares,
    )


def prepare_export_coefficients_data(
    export_df: pd.DataFrame,
    products_preparations: Mapping[str, Sequence[str]],
    specific_shares: Mapping[Tuple[str, str], float],
    years: Sequence[int],
    exchange_rate_brl_per_usd: TaxaCambio,
    target_state: str,
) -> pd.DataFrame:
    return preparar_dados_coeficientes_exportacao(
        export_df,
        preparacoes_produtos=products_preparations,
        participacoes_especificas=specific_shares,
        anos=years,
        taxa_cambio_brl_por_usd=exchange_rate_brl_per_usd,
        uf_alvo=target_state,
    )


def build_export_coefficients_json(
    export_coefficients: pd.DataFrame,
) -> dict[str, list[dict[str, Any]]]:
    return construir_json_coeficientes_exportacao(export_coefficients)


def save_export_coefficients_json(
    export_coefficients: pd.DataFrame,
    output_path: Path,
) -> None:
    salvar_json_coeficientes_exportacao(export_coefficients, output_path)
