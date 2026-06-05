"""Audita matches textuais entre produtos unicos e catalogo NCM."""

from __future__ import annotations

import json
import re
import unicodedata
from difflib import SequenceMatcher
from pathlib import Path
from typing import Iterable

import pandas as pd

from dados.utils.logging import get_logger

DATASET_DIR = Path(__file__).parent
PARAMETROS_PATH = DATASET_DIR / "parametros_coeficientes_exportacao.json"
RAW_COMEX_PATH = DATASET_DIR.parents[1] / "raw" / "pa_me_comex_stat"
RESULTADOS_DIR = DATASET_DIR / "resultados"

COLUNAS_NCM = ["CO_NCM", "NO_NCM_POR", "NO_NCM_ESP", "NO_NCM_ING"]
log = get_logger(dataset_id="br_coeficientes_exportacao", zone="gold")
STOPWORDS = {
    "acu",
    "de",
    "da",
    "das",
    "do",
    "dos",
    "e",
    "em",
    "fruto",
    "in",
    "insumo",
    "amendoa",
    "natura",
    "oleo",
    "para",
}

ALIASES_PRODUTO = {
    "AcaiCaroco": ["acai", "caroco"],
    "AcaiFruto": ["acai"],
    "AcaiInsumo": ["acai"],
    "CacauAmendoa": ["cacau", "cocoa"],
    "CajuAcuFruto": ["caju", "cajuacu", "caju acu"],
    "CascaBarbaTimao": ["barbatimao", "barba timao", "casca"],
    "CascaCajuacu": ["cajuacu", "caju acu", "casca"],
    "CastanhaDoPara": [
        "castanha do para",
        "castanha do brasil",
        "castanha da amazonia",
        "brazil nuts",
        "nuez de brasil",
    ],
    "CestoBuriti": ["buriti", "cesto"],
    "CopaibaOleo": ["copaiba"],
    "Limao": ["limao", "limon", "lemon"],
    "PimentaDoReino": ["pimenta do reino", "piper", "pepper"],
}


def normalizar_texto(valor: str) -> str:
    sem_acento = (
        unicodedata.normalize("NFKD", str(valor))
        .encode("ascii", "ignore")
        .decode("ascii")
        .lower()
    )
    return re.sub(r"[^a-z0-9]+", " ", sem_acento).strip()


def tokens_produto(produto: str) -> list[str]:
    separado = re.sub(r"(?<!^)(?=[A-Z])", " ", produto)
    tokens = normalizar_texto(separado).split()
    return sorted({token for token in tokens if token not in STOPWORDS})


def aliases_produto(produto: str) -> list[str]:
    return [normalizar_texto(alias) for alias in ALIASES_PRODUTO.get(produto, [])]


def carregar_parametros() -> dict:
    return json.loads(PARAMETROS_PATH.read_text(encoding="utf-8"))


def carregar_catalogo_ncm() -> pd.DataFrame:
    return pd.read_csv(
        RAW_COMEX_PATH / "NCM.csv",
        sep=";",
        encoding="cp1252",
        dtype=str,
        usecols=COLUNAS_NCM,
    ).fillna("")


def carregar_produtos_unicos() -> list[str]:
    return json.loads(
        (RAW_COMEX_PATH / "produtos_unicos_matrizes.json").read_text(encoding="utf-8")
    )


def _score_tokens(tokens: Iterable[str], texto_normalizado: str) -> float:
    tokens_validos = list(tokens)
    if not tokens_validos:
        return 0.0
    palavras = set(texto_normalizado.split())
    encontrados = sum(1 for token in tokens_validos if token in palavras)
    return encontrados / len(tokens_validos)


def _score_similaridade(consulta: str, texto_normalizado: str) -> float:
    if not consulta or not texto_normalizado:
        return 0.0
    return SequenceMatcher(None, consulta, texto_normalizado).ratio()


def _score_aliases(aliases: Iterable[str], texto_normalizado: str) -> float:
    aliases_validos = [alias for alias in aliases if alias]
    if not aliases_validos:
        return 0.0

    texto_com_bordas = f" {texto_normalizado} "
    return (
        1.0
        if any(f" {alias} " in texto_com_bordas for alias in aliases_validos)
        else 0.0
    )


def auditar_parametros_vs_ncm(
    parametros: dict,
    catalogo_ncm: pd.DataFrame,
) -> pd.DataFrame:
    catalogo_por_codigo = {
        str(linha.CO_NCM): linha
        for linha in catalogo_ncm.itertuples()
    }
    linhas = []
    for produto, composicao in parametros["composicao_produtos"].items():
        for item in composicao:
            codigo = str(item["id_ncm"])
            ncm = catalogo_por_codigo.get(codigo)
            nome_parametro = str(item["nome_ncm"])
            nome_catalogo = "" if ncm is None else str(ncm.NO_NCM_POR)
            linhas.append(
                {
                    "produto": produto,
                    "id_ncm": codigo,
                    "nome_ncm_parametro": nome_parametro,
                    "nome_ncm_catalogo_por": nome_catalogo,
                    "codigo_existe_no_catalogo": ncm is not None,
                    "nome_por_match_normalizado": (
                        normalizar_texto(nome_parametro)
                        == normalizar_texto(nome_catalogo)
                    ),
                }
            )
    return pd.DataFrame(linhas)


def gerar_candidatos_produto(
    produtos_unicos: list[str],
    parametros: dict,
    catalogo_ncm: pd.DataFrame,
    *,
    limite_por_produto: int = 25,
) -> pd.DataFrame:
    codigos_parametrizados = {
        (produto, str(item["id_ncm"]))
        for produto, composicao in parametros["composicao_produtos"].items()
        for item in composicao
    }

    catalogo = catalogo_ncm.copy()
    for coluna in ["NO_NCM_POR", "NO_NCM_ESP", "NO_NCM_ING"]:
        catalogo[f"{coluna}_normalizado"] = catalogo[coluna].map(normalizar_texto)

    linhas = []
    for produto in produtos_unicos:
        tokens = tokens_produto(produto)
        aliases = aliases_produto(produto)
        consulta = " ".join(tokens)
        candidatos_produto = []

        for ncm in catalogo.itertuples():
            token_scores = {
                "por": _score_tokens(tokens, ncm.NO_NCM_POR_normalizado),
                "esp": _score_tokens(tokens, ncm.NO_NCM_ESP_normalizado),
                "ing": _score_tokens(tokens, ncm.NO_NCM_ING_normalizado),
            }
            alias_scores = {
                "por": _score_aliases(aliases, ncm.NO_NCM_POR_normalizado),
                "esp": _score_aliases(aliases, ncm.NO_NCM_ESP_normalizado),
                "ing": _score_aliases(aliases, ncm.NO_NCM_ING_normalizado),
            }
            codigo = str(ncm.CO_NCM)
            parametrizado = (produto, codigo) in codigos_parametrizados
            tem_token = max(token_scores.values()) > 0 or max(alias_scores.values()) > 0
            if not tem_token and not parametrizado:
                continue

            similaridade_scores = {
                "por": _score_similaridade(consulta, ncm.NO_NCM_POR_normalizado),
                "esp": _score_similaridade(consulta, ncm.NO_NCM_ESP_normalizado),
                "ing": _score_similaridade(consulta, ncm.NO_NCM_ING_normalizado),
            }
            scores_linguagem = {
                linguagem: max(
                    token_scores[linguagem],
                    alias_scores[linguagem],
                    similaridade * 0.25,
                )
                for linguagem, similaridade in similaridade_scores.items()
            }
            melhor_linguagem = max(scores_linguagem, key=scores_linguagem.get)
            melhor_score = scores_linguagem[melhor_linguagem]

            candidatos_produto.append(
                {
                    "produto": produto,
                    "tokens_busca": " ".join(tokens),
                    "aliases_busca": "; ".join(aliases),
                    "id_ncm": codigo,
                    "nome_ncm_por": ncm.NO_NCM_POR,
                    "nome_ncm_esp": ncm.NO_NCM_ESP,
                    "nome_ncm_ing": ncm.NO_NCM_ING,
                    "score_por": round(scores_linguagem["por"], 4),
                    "score_esp": round(scores_linguagem["esp"], 4),
                    "score_ing": round(scores_linguagem["ing"], 4),
                    "token_score_por": round(token_scores["por"], 4),
                    "token_score_esp": round(token_scores["esp"], 4),
                    "token_score_ing": round(token_scores["ing"], 4),
                    "alias_score_por": round(alias_scores["por"], 4),
                    "alias_score_esp": round(alias_scores["esp"], 4),
                    "alias_score_ing": round(alias_scores["ing"], 4),
                    "melhor_linguagem": melhor_linguagem,
                    "score_match": round(melhor_score, 4),
                    "ja_parametrizado": parametrizado,
                }
            )

        candidatos_produto = sorted(
            candidatos_produto,
            key=lambda item: (item["ja_parametrizado"], item["score_match"]),
            reverse=True,
        )[:limite_por_produto]
        linhas.extend(candidatos_produto)

    return pd.DataFrame(linhas)


def gerar_relatorio_matches() -> tuple[Path, Path]:
    parametros = carregar_parametros()
    produtos_unicos = carregar_produtos_unicos()
    catalogo_ncm = carregar_catalogo_ncm()

    parametros_vs_ncm = auditar_parametros_vs_ncm(parametros, catalogo_ncm)
    candidatos = gerar_candidatos_produto(produtos_unicos, parametros, catalogo_ncm)
    sem_composicao = pd.DataFrame(
        {
            "produto": [
                produto
                for produto, composicao in parametros["composicao_produtos"].items()
                if not composicao
            ]
        }
    )

    RESULTADOS_DIR.mkdir(parents=True, exist_ok=True)
    caminho_csv = RESULTADOS_DIR / "auditoria_matches_ncm_candidatos.csv"
    caminho_xlsx = RESULTADOS_DIR / "auditoria_matches_ncm.xlsx"

    candidatos.to_csv(caminho_csv, index=False, encoding="utf-8")
    with pd.ExcelWriter(caminho_xlsx) as writer:
        parametros_vs_ncm.to_excel(writer, sheet_name="parametros_vs_ncm", index=False)
        candidatos.to_excel(writer, sheet_name="candidatos_produto", index=False)
        sem_composicao.to_excel(writer, sheet_name="produtos_sem_composicao", index=False)

    return caminho_csv, caminho_xlsx


def main() -> None:
    caminho_csv, caminho_xlsx = gerar_relatorio_matches()
    log.info(
        "auditoria_matches_ncm.done",
        csv=str(caminho_csv),
        xlsx=str(caminho_xlsx),
    )


if __name__ == "__main__":
    main()
