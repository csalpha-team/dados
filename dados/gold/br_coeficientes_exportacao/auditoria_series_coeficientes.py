"""Auditoria visual das series gold de coeficientes de exportacao."""

from __future__ import annotations

import os
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
from dotenv import load_dotenv

from dados.raw.utils.postgres_interactions import PostgresETL
from dados.utils.logging import get_logger
from dados.utils.paths import tmp_dir

load_dotenv()

DATASET_ID = "br_coeficientes_exportacao"
TABLE = "preparacao_camada_exportacao"
log = get_logger(dataset_id=DATASET_ID, zone="gold")


def _database_gold() -> str:
    database = os.getenv("DB_GOLD_ZONE") or os.getenv("DB_AGREGATED_ZONE")
    if not database:
        raise ValueError(
            "Banco gold nao configurado. Defina DB_GOLD_ZONE ou DB_AGREGATED_ZONE."
        )
    return database


def carregar_coeficientes() -> pd.DataFrame:
    with PostgresETL(
        host="localhost",
        database=_database_gold(),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        schema=DATASET_ID,
    ) as db:
        return db.download_table(TABLE)


def gerar_grafico_series(
    coeficientes: pd.DataFrame,
    caminho_saida: Path | None = None,
    *,
    max_produtos: int = 12,
) -> Path:
    if coeficientes.empty:
        raise ValueError("Tabela de coeficientes de exportacao esta vazia")

    dados = coeficientes.copy()
    dados["ano"] = pd.to_numeric(dados["ano"], errors="coerce")
    dados["coeff"] = pd.to_numeric(dados["coeff"], errors="coerce")
    dados = dados.dropna(subset=["ano", "produto", "coeff"])
    dados["ano"] = dados["ano"].astype(int)

    produtos_principais = (
        dados.groupby("produto")["coeff"]
        .mean()
        .sort_values(ascending=False)
        .head(max_produtos)
        .index
    )
    serie = dados[dados["produto"].isin(produtos_principais)].pivot_table(
        index="ano",
        columns="produto",
        values="coeff",
        aggfunc="sum",
        fill_value=0.0,
    )

    if caminho_saida is None:
        caminho_saida = (
            tmp_dir(DATASET_ID, "output") / "series_coeficientes_exportacao.png"
        )

    ax = serie.plot(figsize=(14, 8), linewidth=2)
    ax.set_title("Coeficientes de exportacao por produto")
    ax.set_xlabel("Ano")
    ax.set_ylabel("Coeficiente")
    ax.grid(True, alpha=0.3)
    ax.legend(title="Produto", bbox_to_anchor=(1.02, 1), loc="upper left")

    caminho_saida.parent.mkdir(parents=True, exist_ok=True)
    plt.tight_layout()
    plt.savefig(caminho_saida, dpi=160)
    plt.close()
    return caminho_saida


def salvar_resumo(
    coeficientes: pd.DataFrame, caminho_saida: Path | None = None
) -> Path:
    if caminho_saida is None:
        caminho_saida = tmp_dir(DATASET_ID, "output") / "resumo_coeficientes.csv"

    resumo = (
        coeficientes.assign(coeff=pd.to_numeric(coeficientes["coeff"], errors="coerce"))
        .groupby("produto", as_index=False)
        .agg(
            coeff_medio=("coeff", "mean"),
            coeff_maximo=("coeff", "max"),
            anos=("ano", "nunique"),
        )
        .sort_values("coeff_medio", ascending=False)
    )
    caminho_saida.parent.mkdir(parents=True, exist_ok=True)
    resumo.to_csv(caminho_saida, index=False)
    return caminho_saida


def main() -> None:
    coeficientes = carregar_coeficientes()
    caminho_resumo = salvar_resumo(coeficientes)
    caminho_grafico = gerar_grafico_series(coeficientes)
    log.info(
        "auditoria_series.done",
        resumo=str(caminho_resumo),
        grafico=str(caminho_grafico),
    )


if __name__ == "__main__":
    main()
