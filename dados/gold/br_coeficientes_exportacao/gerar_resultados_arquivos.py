"""Gera arquivos CSV/XLSX dos coeficientes de exportacao sem depender do DB local.

Este utilitario usa a API publica do ComexStat como fonte operacional, aplica a
mesma transformacao da gold e salva artefatos auditaveis no repositorio.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from dotenv import load_dotenv

from dados.gold.br_coeficientes_exportacao.auditoria_series_coeficientes import (
    gerar_grafico_series,
)
from dados.gold.br_coeficientes_exportacao.utils import (
    carregar_parametros_exportacao,
    preparar_dados_coeficientes_exportacao,
)
from dados.utils.logging import get_logger

load_dotenv()

DATASET_ID = "br_coeficientes_exportacao"
CONFIG_PATH = Path(__file__).with_name("parametros_coeficientes_exportacao.json")
RAW_COMEX_PATH = Path(__file__).parents[2] / "raw" / "pa_me_comex_stat"
RESULTADOS_DIR = Path(__file__).with_name("resultados")
CACHE_DIR = RESULTADOS_DIR / "_cache_comexstat"

COMEXSTAT_API_URL = "https://api-comexstat.mdic.gov.br/general?language=pt"
CODIGO_UF_PA = 15

log = get_logger(dataset_id=DATASET_ID, zone="gold")


def _carregar_catalogo_ncm() -> pd.DataFrame:
    catalogo = pd.read_csv(
        RAW_COMEX_PATH / "NCM.csv",
        sep=";",
        encoding="cp1252",
        dtype=str,
        usecols=["CO_NCM", "NO_NCM_POR"],
    )
    return catalogo.rename(
        columns={"CO_NCM": "id_ncm", "NO_NCM_POR": "nome_ncm_portugues"}
    )


def _consultar_exportacoes_pa_ano(ano: int) -> pd.DataFrame:
    caminho_cache = CACHE_DIR / f"exportacoes_pa_ncm_{ano}.csv"
    if caminho_cache.exists():
        return pd.read_csv(caminho_cache, dtype={"id_ncm": str})

    payload: dict[str, Any] = {
        "flow": "export",
        "monthDetail": False,
        "period": {"from": f"{ano}-01", "to": f"{ano}-12"},
        "filters": [{"filter": "state", "values": [CODIGO_UF_PA]}],
        "details": ["state", "ncm"],
        "metrics": ["metricFOB"],
    }
    response = None
    for tentativa in range(1, 6):
        response = requests.post(COMEXSTAT_API_URL, json=payload, timeout=90)
        if response.status_code != 429:
            break
        espera = 10 * tentativa
        log.info(
            "comexstat.rate_limit",
            ano=ano,
            tentativa=tentativa,
            espera_segundos=espera,
        )
        time.sleep(espera)

    if response is None:
        raise RuntimeError("Falha inesperada ao consultar ComexStat")

    response.raise_for_status()
    dados = response.json()
    if not dados.get("success"):
        raise ValueError(f"ComexStat retornou falha para {ano}: {dados.get('message')}")

    linhas = (dados.get("data") or {}).get("list") or []
    if not linhas:
        vazio = pd.DataFrame(
            columns=[
                "ano",
                "id_ncm",
                "nome_ncm_api",
                "sigla_uf_ncm",
                "valor_fob_dolar",
            ]
        )
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        vazio.to_csv(caminho_cache, index=False)
        return vazio

    df = pd.DataFrame(linhas)
    resultado = pd.DataFrame(
        {
            "ano": pd.to_numeric(df["year"], errors="coerce").astype("Int64"),
            "id_ncm": df["coNcm"].astype(str).str.strip(),
            "nome_ncm_api": df["ncm"].astype(str).str.strip(),
            "sigla_uf_ncm": "PA",
            "valor_fob_dolar": pd.to_numeric(df["metricFOB"], errors="coerce"),
        }
    )
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    resultado.to_csv(caminho_cache, index=False)
    return resultado


def baixar_exportacoes_comexstat(anos: list[int]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for ano in sorted({int(valor) for valor in anos if int(valor) >= 1997}):
        log.info("comexstat.extract.year.start", ano=ano)
        dados_ano = _consultar_exportacoes_pa_ano(ano)
        log.info("comexstat.extract.year.done", ano=ano, rows=len(dados_ano))
        frames.append(dados_ano)

    if not frames:
        return pd.DataFrame(
            columns=[
                "ano",
                "id_ncm",
                "nome_ncm_portugues",
                "sigla_uf_ncm",
                "valor_fob_dolar",
            ]
        )

    exportacoes = pd.concat(frames, ignore_index=True)
    catalogo = _carregar_catalogo_ncm()
    exportacoes = exportacoes.merge(catalogo, on="id_ncm", how="left")
    exportacoes["nome_ncm_portugues"] = exportacoes["nome_ncm_portugues"].fillna(
        exportacoes["nome_ncm_api"]
    )
    return exportacoes[
        [
            "ano",
            "id_ncm",
            "nome_ncm_portugues",
            "sigla_uf_ncm",
            "valor_fob_dolar",
        ]
    ]


def construir_resumo(coeficientes: pd.DataFrame) -> pd.DataFrame:
    return (
        coeficientes.groupby("produto", as_index=False)
        .agg(
            coeff_medio=("coeff", "mean"),
            coeff_maximo=("coeff", "max"),
            valor_fob_dolar_total=("valor_fob_dolar", "sum"),
            valor_fob_real_total=("valor_fob_real", "sum"),
            anos=("ano", "nunique"),
        )
        .sort_values("coeff_medio", ascending=False)
    )


def construir_consistencia_anual(coeficientes: pd.DataFrame) -> pd.DataFrame:
    return (
        coeficientes.groupby("ano", as_index=False)
        .agg(
            produtos=("produto", "nunique"),
            valor_fob_dolar_total=("valor_fob_dolar", "sum"),
            valor_fob_real_total=("valor_fob_real", "sum"),
            soma_coeff=("coeff", "sum"),
        )
        .sort_values("ano")
    )


def salvar_resultados(coeficientes: pd.DataFrame, exportacoes: pd.DataFrame) -> None:
    RESULTADOS_DIR.mkdir(parents=True, exist_ok=True)

    resumo = construir_resumo(coeficientes)
    consistencia = construir_consistencia_anual(coeficientes)
    metadados = pd.DataFrame(
        [
            {
                "fonte_exportacoes": COMEXSTAT_API_URL,
                "uf": "PA",
                "codigo_uf": CODIGO_UF_PA,
                "linhas_exportacoes_ncm": len(exportacoes),
                "linhas_coeficientes": len(coeficientes),
                "observacao": (
                    "ComexStat geral retorna dados a partir de 1997; anos anteriores "
                    "em anos_previsao sao estimados pela regressao linear do pipeline."
                ),
            }
        ]
    )

    coeficientes.to_csv(
        RESULTADOS_DIR / "coeficientes_exportacao.csv",
        index=False,
        encoding="utf-8",
    )
    resumo.to_csv(
        RESULTADOS_DIR / "resumo_coeficientes_exportacao.csv",
        index=False,
        encoding="utf-8",
    )
    consistencia.to_csv(
        RESULTADOS_DIR / "consistencia_anual_coeficientes_exportacao.csv",
        index=False,
        encoding="utf-8",
    )

    with pd.ExcelWriter(RESULTADOS_DIR / "coeficientes_exportacao.xlsx") as writer:
        coeficientes.to_excel(writer, sheet_name="coeficientes", index=False)
        resumo.to_excel(writer, sheet_name="resumo_produto", index=False)
        consistencia.to_excel(writer, sheet_name="consistencia_anual", index=False)
        metadados.to_excel(writer, sheet_name="metadados", index=False)

    gerar_grafico_series(
        coeficientes,
        RESULTADOS_DIR / "series_coeficientes_exportacao.png",
    )

    with (RESULTADOS_DIR / "metadados_resultados.json").open(
        "w", encoding="utf-8"
    ) as file:
        json.dump(
            metadados.to_dict(orient="records")[0], file, ensure_ascii=False, indent=2
        )


def main() -> None:
    (
        preparacoes_produtos,
        participacoes_especificas,
        anos,
        taxa_cambio_brl_por_usd,
        uf_alvo,
    ) = carregar_parametros_exportacao(CONFIG_PATH)

    exportacoes = baixar_exportacoes_comexstat(anos)
    coeficientes = preparar_dados_coeficientes_exportacao(
        exportacoes,
        preparacoes_produtos=preparacoes_produtos,
        participacoes_especificas=participacoes_especificas,
        anos=anos,
        taxa_cambio_brl_por_usd=taxa_cambio_brl_por_usd,
        uf_alvo=uf_alvo,
    )
    salvar_resultados(coeficientes, exportacoes)
    log.info(
        "resultados_arquivos.done",
        output_dir=str(RESULTADOS_DIR),
        rows=len(coeficientes),
    )


if __name__ == "__main__":
    main()
