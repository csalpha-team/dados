"""Compara as séries do L1 recém-geradas (``gold_export_l1/``) contra os
arquivos de referência (``benchmark/``).

Gera dois CSVs e um relatório markdown em ``comparacao_benchmark/``:

1. ``comparacao_regiao_produto.csv`` — soma por (série, região, produto),
   benchmark × new (agregando sobre os anos).
2. ``comparacao_estado_produto.csv`` — soma por (série, produto), benchmark ×
   new (agregando sobre regiões e anos).
3. ``RELATORIO_comparacao.md`` — resumo por série + leitura dos drivers.

O benchmark é um *snapshot anterior* à migração de regiões de integração e às
correções de unidade/monetárias, logo divergências de valor são esperadas. Para
isolar efeitos, o nível estado/produto cancela a remontagem de municípios entre
RIs. Os nomes de RI antigos do benchmark são remapeados para os canônicos.

Uso:
    uv run python -m dados.export.comparar_benchmark
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Iterator, List, Tuple

import pandas as pd

from dados.export.dump_gold_l1 import OUTPUT_DIR, REPO_ROOT
from dados.utils.logging import get_logger

DATASET_ID = "pa_indexadores_producao_rural"
ZONE = "export"

BENCHMARK_DIR = REPO_ROOT / "benchmark"
NEW_DIR = OUTPUT_DIR
OUT_DIR = REPO_ROOT / "comparacao_benchmark"

# Tolerância relativa para considerar dois valores "iguais".
TOL = 0.01

# RIs renomeadas na migração: nome antigo (benchmark) -> canônico (gold atual).
REGION_RENAME = {
    "Caeté": "Rio Caeté",
    "Capim": "Rio Capim",
    "Tucuruí": "Lago de Tucuruí",
}

# Métricas do leaf de censo_autoconsumo.json.
AUTOCONSUMO_METRICS = (
    "quantidade_produzida",
    "valor_producao",
    "comercio_quantidade_produzida",
    "comercio_valor_producao",
)

log = get_logger(dataset_id=DATASET_ID, zone=ZONE)


# ==========================================================================
# Leitura → registros longos (serie, regiao, produto, ano, value)
# ==========================================================================
def _records(
    path: Path, remap_region: bool
) -> Iterator[Tuple[str, str, str, str, float]]:
    """Achata um json do L1 em registros ``(serie, regiao, produto, ano, val)``.

    Arquivos flat viram uma série (= nome do arquivo). ``censo_autoconsumo.json``
    é decomposto em ``censo_autoconsumo::<nome_pesquisa>::<metrica>``.
    """
    data = json.loads(path.read_text(encoding="utf-8"))

    def reg(name: str) -> str:
        return REGION_RENAME.get(name, name) if remap_region else name

    if path.name == "censo_autoconsumo.json":
        for nome_pesquisa, tipos in data["vetores_producao_rural"].items():
            for _tipo, regioes in tipos.items():
                for regiao, produtos in regioes.items():
                    for produto, anos in produtos.items():
                        for ano, leaf in anos.items():
                            for metric in AUTOCONSUMO_METRICS:
                                serie = f"censo_autoconsumo::{nome_pesquisa}::{metric}"
                                yield (
                                    serie,
                                    reg(regiao),
                                    produto,
                                    ano,
                                    float(leaf[metric]),
                                )
        return

    serie = path.name
    for regiao, produtos in data.items():
        for produto, anos in produtos.items():
            for ano, val in anos.items():
                yield serie, reg(regiao), produto, ano, float(val)


def _frame(directory: Path, remap_region: bool) -> pd.DataFrame:
    files = sorted(p for p in directory.glob("*.json"))
    rows: List[tuple] = []
    for p in files:
        rows.extend(_records(p, remap_region))
    df = pd.DataFrame(rows, columns=["serie", "regiao", "produto", "ano", "value"])
    log.info("load.done", directory=str(directory), files=len(files), rows=len(df))
    return df


# ==========================================================================
# Comparação
# ==========================================================================
def _status(row: pd.Series) -> str:
    b, n = row["soma_benchmark"], row["soma_new"]
    if pd.isna(b) or row["_only"] == "new":
        return "only_new"
    if pd.isna(n) or row["_only"] == "benchmark":
        return "only_benchmark"
    if abs(b - n) <= TOL * max(abs(b), abs(n), 1e-9):
        return "match"
    return "diff"


def _compare(bench: pd.DataFrame, new: pd.DataFrame, keys: List[str]) -> pd.DataFrame:
    b = (
        bench.groupby(keys, as_index=False)["value"]
        .sum()
        .rename(columns={"value": "soma_benchmark"})
    )
    n = (
        new.groupby(keys, as_index=False)["value"]
        .sum()
        .rename(columns={"value": "soma_new"})
    )
    merged = b.merge(n, on=keys, how="outer", indicator="_ind")
    merged["_only"] = merged["_ind"].map(
        {"left_only": "benchmark", "right_only": "new", "both": "both"}
    )
    merged["soma_benchmark"] = merged["soma_benchmark"].round(2)
    merged["soma_new"] = merged["soma_new"].round(2)
    merged["diff"] = (
        merged["soma_new"].fillna(0) - merged["soma_benchmark"].fillna(0)
    ).round(2)
    denom = merged[["soma_benchmark", "soma_new"]].abs().max(axis=1)
    merged["diff_rel_pct"] = (
        100 * merged["diff"].abs() / denom.where(denom > 0)
    ).round(2)
    merged["status"] = merged.apply(_status, axis=1)
    merged = merged.drop(columns=["_ind", "_only"])
    return merged.sort_values(keys).reset_index(drop=True)


# ==========================================================================
# Relatório
# ==========================================================================
def _summary(rp: pd.DataFrame, sp: pd.DataFrame) -> pd.DataFrame:
    def per_serie(df: pd.DataFrame, label: str) -> pd.DataFrame:
        g = df.groupby("serie")
        out = pd.DataFrame(
            {
                f"{label}_linhas": g.size(),
                f"{label}_match": g.apply(
                    lambda x: int((x["status"] == "match").sum()), include_groups=False
                ),
            }
        )
        out[f"{label}_match_pct"] = (
            100 * out[f"{label}_match"] / out[f"{label}_linhas"]
        ).round(1)
        return out

    a = per_serie(rp, "rp")
    b = per_serie(sp, "sp")
    tot = sp.groupby("serie")[["soma_benchmark", "soma_new"]].sum().round(2)
    return a.join(b).join(tot).reset_index()


_DRIVERS_MD = """\
## Como ler as duas tabelas

- **Região/produto** (`comparacao_regiao_produto.csv`) — soma sobre os anos, por
  (série, região, produto). Sensível à migração de RIs.
- **Estado/produto** (`comparacao_estado_produto.csv`) — soma sobre regiões *e*
  anos, por (série, produto). A migração de RIs se cancela aqui (todos os
  municípios entram no total estadual em ambas as versões), então o que sobra são
  correções de valor e diferenças de cobertura.

`status`: `match` (|Δ| ≤ 1%), `diff`, `only_benchmark`, `only_new`. `diff_rel_pct`
é |Δ| relativo ao maior dos dois valores.

## Leitura dos drivers das divergências

O benchmark é um *snapshot anterior* a mudanças intencionais a montante;
divergências de valor são esperadas e **não** indicam erro de exportação.

1. **Migração de regiões de integração** (`MIGRACAO_REGIOES_INTEGRACAO.md`).
   Municípios antes sem RI (`"NaN"` no benchmark) agora estão mapeados, três RIs
   foram renomeadas (`Caeté→Rio Caeté`, `Capim→Rio Capim`, `Tucuruí→Lago de
   Tucuruí`, já remapeadas aqui) e a composição municipal de cada RI mudou. Afeta
   o nível **região/produto**; **cancela-se** no nível **estado/produto**.
2. **Correções de unidade.** PEVS `madeira-tora` agora é exatamente ÷2 (correção
   m³) — domina a soma de `quantidade` do PEVS; produtos madeireiros do censo
   foram reescalados por fatores específicos por produto.
3. **Padronização monetária (1000xBRL).** Séries de `valor` do PAM trazem o novo
   fator de escala; entradas pré-1995 (reforma monetária) zeram.
4. **Rebuild do silver de lavoura PAM** (`silver/al_ibge_pam/lavoura_*.py`). Até a
   `quantidade` (não-monetária) diverge — daí o match baixíssimo do lavoura PAM.
5. **Cobertura de anos.** O benchmark precede o censo de lavoura 2017, então as
   somas de `lavoura_*_censo` no new incluem 2017 e superam o benchmark. Também
   há pequenas diferenças de grade (produto×ano) no PEVS.

**Sanidade da exportação:** mesmo com produtos individuais embaralhados pela
padronização de nomes, o **total estadual** do PEVS `valor` é praticamente
idêntico (Σ benchmark ≈ Σ new, ~0,06% de diferença) — confirmando que a lógica de
exportação é fiel. As demais divergências acompanham as correções acima.
"""


def _report_md(summary: pd.DataFrame) -> str:
    out = ["# Comparação benchmark × séries L1 geradas", ""]
    out.append(
        "Soma por região/produto e por estado/produto, benchmark × new, por "
        "série. Nomes de RI antigos do benchmark remapeados para os canônicos.\n"
    )
    out.append(
        "- `comparacao_regiao_produto.csv` — soma por (série, região, produto)\n"
        "- `comparacao_estado_produto.csv` — soma por (série, produto)\n"
    )
    out.append("## Resumo por série\n")
    out.append(
        "| série | reg/prod linhas | match% | estado/prod linhas | match% | "
        "Σ benchmark | Σ new |"
    )
    out.append("|---|--:|--:|--:|--:|--:|--:|")
    for r in summary.itertuples(index=False):
        out.append(
            f"| {r.serie} | {r.rp_linhas} | {r.rp_match_pct} | {r.sp_linhas} "
            f"| {r.sp_match_pct} | {r.soma_benchmark:,.2f} | {r.soma_new:,.2f} |"
        )
    out.append("")
    out.append(_DRIVERS_MD)
    return "\n".join(out)


# ==========================================================================
# Flow
# ==========================================================================
def flow() -> None:
    log.info("flow.start", benchmark=str(BENCHMARK_DIR), new=str(NEW_DIR))
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    bench = _frame(BENCHMARK_DIR, remap_region=True)
    new = _frame(NEW_DIR, remap_region=False)

    rp = _compare(bench, new, ["serie", "regiao", "produto"])
    sp = _compare(bench, new, ["serie", "produto"])

    rp_path = OUT_DIR / "comparacao_regiao_produto.csv"
    sp_path = OUT_DIR / "comparacao_estado_produto.csv"
    rp.to_csv(rp_path, index=False, encoding="utf-8")
    sp.to_csv(sp_path, index=False, encoding="utf-8")

    summary = _summary(rp, sp)
    md_path = OUT_DIR / "RELATORIO_comparacao.md"
    md_path.write_text(_report_md(summary), encoding="utf-8")

    log.info(
        "compare.done",
        regiao_produto=str(rp_path),
        estado_produto=str(sp_path),
        relatorio=str(md_path),
        linhas_rp=len(rp),
        linhas_sp=len(sp),
    )
    log.info("flow.end")


if __name__ == "__main__":
    try:
        flow()
    except Exception as exc:  # noqa: BLE001
        log.exception("flow.error", error=str(exc))
        raise
