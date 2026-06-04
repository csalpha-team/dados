"""Validador efêmero de séries — `pa_indexadores_producao_rural`.

Lê os arquivos flat produzidos por :mod:`dados.export.dump_gold_l1`
(``gold_export_l1/{dataset}_{quantidade,valor}.json``) e emite um relatório de
qualidade. **Não** persiste nenhuma tabela (respeita a regra de camadas:
nenhuma tabela gold derivada de gold).

Spec: ``ESTRATEGIA_validacao_series.md``. Como o insumo é o JSON do L1, a
análise roda na granularidade **região de integração (RI)** — o JSON não traz
``id_municipio`` nem ``area_colhida``/``rendimento_medio_producao``, logo o grão
município e a identidade rendimento×área (A4) ficam fora de escopo.

Duas partes:

- **Parte A** — anomalias temporais nas séries amostrais (PAM/PEVS): z-score
  modificado robusto sobre log-retornos (A1), classificador pico-vs-degrau (A2),
  valor unitário implícito / preço (A3) e score de severidade (A5).
- **Parte B** — validação cruzada Censo 2006/2017 ↔ pesquisa: razão nas âncoras
  (B1), envelope (B2), consistência de inclinação (B3), coerência de preço (B4)
  e correlação de postos (B5).

Saídas em ``OUTPUT_DIR`` (ou ``--outdir``):
``relatorio_series_pam_pevs.csv``, ``relatorio_censo_vs_series.csv``,
``resumo_validacao.md``.

Uso:
    uv run python -m dados.gold.pa_indexadores_producao_rural.validacao_series
    uv run python -m dados.gold.pa_indexadores_producao_rural.validacao_series \
        --indir gold_export_l1 --outdir /tmp/val
"""

from __future__ import annotations

import argparse
import math
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import numpy as np
import pandas as pd

from dados.export.dump_gold_l1 import AMOSTRAL, CENSUS_FAMILIES, OUTPUT_DIR
from dados.export.models import SerieRegional
from dados.utils.logging import get_logger

DATASET_ID = "pa_indexadores_producao_rural"
ZONE = "gold"

log = get_logger(dataset_id=DATASET_ID, zone=ZONE)

# --- Limiares e pesos (calibráveis no spot-check) -------------------------
Z_THRESHOLD = 3.5  # Iglewicz–Hoaglin
EPS = 1e-9
# Teto por componente no score de severidade: impede que um único artefato
# (ex.: MAD≈0 colapsando o z transversal) domine o ranking de triagem. Os
# componentes brutos (não-capados) seguem nos CSVs para explicabilidade.
SEVERITY_CAP = 20.0
# Piso de dispersão em espaço log: abaixo disso a variação é ruído de
# arredondamento, não sinal (evita z explodindo por MAD ≈ 0).
_DISP_FLOOR = 1e-6
# Quando a maioria dos valores é idêntica (ex.: preço deflacionado imputado
# uniforme), a mediana cai sobre um "pico" e o MAD colapsa para perto de zero,
# inflando o z. Se MAD < este fração do desvio absoluto médio, o MAD é tido
# como degenerado e usa-se o meanAD (não-robusto, porém estável) como escala.
_MAD_MEANAD_RATIO = 0.1
# Mínimo de RIs para o z transversal de preço fazer sentido (poucos pontos →
# MAD instável → z espúrio). Abaixo disso, o eixo transversal é ignorado.
MIN_RIS_TRANSVERSAL = 5
# Pesos do score de severidade A5 (sem o termo de quebra de identidade A4,
# ausente no JSON). Iniciados em ~1; ajustar contra casos conhecidos.
W_MQTD, W_MVAL, W_REVERSAO, W_PRECO = 1.0, 1.0, 1.0, 1.0
ANCHORS = (2006, 2017)

# --- Crosswalk de produtos pesquisa(PAM/PEVS) → Censo (Parte B) ------------
# Os nomes padronizados divergem entre fontes (o Censo é a referência). Match
# exato cobre a maioria; este bridge cobre as divergências conhecidas. Derivado
# de dados/silver/constants/produtos.py — best-effort, revisar no spot-check.
# Produtos sem par viram linha de achado (status sem_par_*), não são silenciados.
BRIDGE_PRODUTOS: Dict[str, str] = {
    # lavoura permanente
    "banana-cacho": "banana",
    "erva mate": "erva-mate",
    "pimenta do reino": "pimenta-do-reino",
    # lavoura temporária
    "girassol-grão": "girassol-semente",
    "mamona-baga": "mamona",
    "cevada-grão": "cevada-casca",
    # extração vegetal
    "jaborandi": "jaborandi-folha",
    "oiticica": "oiticica-semente",
    "cumaru-amêndoa": "cumaru-semente",
    "caucho": "caucho-goma elástica",
}

# Produtos-âncora para a checagem de calibração no resumo (devem sair limpos
# nos dados pós-correção).
CALIBRACAO_PRODUTOS = ("banana-cacho", "madeira-tora", "lenha")


# ==========================================================================
# Carga + achatamento do JSON
# ==========================================================================
def _read_serie(path: Path) -> Dict:
    """Valida um arquivo flat do L1 (``regiao→produto→ano→valor``)."""
    return SerieRegional.model_validate_json(path.read_text(encoding="utf-8")).root


def load_long(indir: Path) -> pd.DataFrame:
    """Lê os arquivos flat do L1 (quantidade+valor) e os achata num DF longo.

    Cada fonte casa o par ``{base}_quantidade.json`` / ``{base}_valor.json`` por
    ``(regiao, produto, ano)``; métrica ausente vira 0.0.
    """
    sources = [(name, "amostral") for name in AMOSTRAL]
    sources += [(family, "censitaria") for family in CENSUS_FAMILIES]

    rows: List[dict] = []
    for base, tipo_pesquisa in sources:
        q_path = indir / f"{base}_quantidade.json"
        v_path = indir / f"{base}_valor.json"
        if not q_path.exists() or not v_path.exists():
            raise FileNotFoundError(
                f"Arquivos flat do L1 não encontrados para '{base}' em {indir}. "
                "Rode antes: uv run python -m dados.export.dump_gold_l1"
            )
        qser, vser = _read_serie(q_path), _read_serie(v_path)
        for regiao in set(qser) | set(vser):
            qprod, vprod = qser.get(regiao, {}), vser.get(regiao, {})
            for produto in set(qprod) | set(vprod):
                qanos, vanos = qprod.get(produto, {}), vprod.get(produto, {})
                for ano in set(qanos) | set(vanos):
                    rows.append(
                        {
                            "nome_pesquisa": base,
                            "tipo_pesquisa": tipo_pesquisa,
                            "nome_regiao_integracao": regiao,
                            "produto": produto,
                            "ano": int(ano),
                            "quantidade_produzida": float(qanos.get(ano, 0.0)),
                            "valor_producao": float(vanos.get(ano, 0.0)),
                        }
                    )
    df = pd.DataFrame(rows)
    log.info(
        "load.done",
        rows=len(df),
        pesquisas=df["nome_pesquisa"].nunique() if not df.empty else 0,
    )
    return df


def family_of(nome_pesquisa: str) -> str:
    """Família de pesquisa, ignorando fonte/ano (para parear pesquisa ↔ censo)."""
    if "_censo" in nome_pesquisa:
        return nome_pesquisa.split("_censo")[0]
    for suffix in ("_pevs", "_pam"):
        if nome_pesquisa.endswith(suffix):
            return nome_pesquisa[: -len(suffix)]
    return nome_pesquisa


# ==========================================================================
# Helpers numéricos
# ==========================================================================
def _modified_z(values: Dict, min_n: int = 2) -> Dict:
    """Z-score modificado robusto (mediana/MAD) sobre o *conjunto* de valores.

    Quando o MAD colapsa (maioria dos valores idêntica → mediana sobre um pico,
    MAD ≈ 0 ou << meanAD), recai sobre o desvio absoluto médio (fallback padrão
    de Iglewicz–Hoaglin) em vez de explodir. Retorna 0 com < ``min_n`` pontos ou
    quando não há dispersão alguma.
    """
    if len(values) < min_n:
        return {k: 0.0 for k in values}
    keys = list(values)
    arr = np.array([values[k] for k in keys], dtype=float)
    med = np.median(arr)
    abs_dev = np.abs(arr - med)
    mad = np.median(abs_dev)
    mean_ad = float(abs_dev.mean())
    # MAD útil só se acima do piso de ruído E não colapsado contra o meanAD
    # (maioria idêntica → mediana sobre um pico). Caso contrário, meanAD.
    if mad > _DISP_FLOOR and mad >= _MAD_MEANAD_RATIO * mean_ad:
        z = 0.6745 * (arr - med) / mad
    elif mean_ad > _DISP_FLOOR:
        z = (arr - med) / (1.253314 * mean_ad)
    else:
        return {k: 0.0 for k in keys}
    return {k: float(v) for k, v in zip(keys, z)}


def _log_returns(serie: Dict[int, float]) -> Dict[int, float]:
    """r_t = ln(x_t) − ln(x_{t−1}) apenas entre anos calendário consecutivos."""
    returns: Dict[int, float] = {}
    for ano in sorted(serie):
        if (ano - 1) in serie:
            returns[ano] = math.log(serie[ano]) - math.log(serie[ano - 1])
    return returns


def _reversal(returns: Dict[int, float], ano: int) -> float:
    """ρ_t = −(r_t·r_{t+1})/(|r_t|·|r_{t+1}|+ε) ∈ [−1, 1]. NaN se incompleto."""
    if ano in returns and (ano + 1) in returns:
        a, b = returns[ano], returns[ano + 1]
        return -(a * b) / (abs(a) * abs(b) + EPS)
    return float("nan")


def _abs_or_nan(x: float) -> float:
    return abs(x) if not math.isnan(x) else float("nan")


def _safe_max_abs(*xs: float) -> float:
    vals = [abs(x) for x in xs if not math.isnan(x)]
    return max(vals) if vals else float("nan")


def _spearman(pairs: List[Tuple[float, float]]) -> float:
    """Correlação de Spearman (rank → Pearson). NaN se < 3 pares ou sem variância."""
    if len(pairs) < 3:
        return float("nan")
    rc = pd.Series([p[0] for p in pairs]).rank()
    rp = pd.Series([p[1] for p in pairs]).rank()
    if rc.std(ddof=0) == 0 or rp.std(ddof=0) == 0:
        return float("nan")
    return float(np.corrcoef(rc, rp)[0, 1])


def _to_serie(group: pd.DataFrame, col: str) -> Dict[int, float]:
    """ano → valor, descartando 0/NaN (tratados como lacuna)."""
    out: Dict[int, float] = {}
    for ano, val in zip(group["ano"], group[col]):
        if pd.notna(val) and val > 0:
            out[int(ano)] = float(val)
    return out


# ==========================================================================
# Parte A — anomalias temporais (séries amostrais)
# ==========================================================================
def _cross_sectional_price_z(survey: pd.DataFrame) -> Dict[Tuple, float]:
    """z modificado de ln(preço) entre RIs, por (nome_pesquisa, produto, ano)."""
    out: Dict[Tuple, float] = {}
    mask = (survey["quantidade_produzida"] > 0) & (survey["valor_producao"] > 0)
    work = survey.loc[mask].copy()
    work["ln_p"] = np.log(work["valor_producao"] / work["quantidade_produzida"])
    for (pesquisa, produto, ano), grp in work.groupby(
        ["nome_pesquisa", "produto", "ano"]
    ):
        vals = {
            regiao: lp for regiao, lp in zip(grp["nome_regiao_integracao"], grp["ln_p"])
        }
        z = _modified_z(vals, min_n=MIN_RIS_TRANSVERSAL)
        for regiao, zval in z.items():
            out[(pesquisa, produto, int(ano), regiao)] = zval
    return out


def parte_a(survey: pd.DataFrame) -> pd.DataFrame:
    """Uma linha por (RI, produto, ano) sinalizado em alguma das métricas A."""
    z_transversal = _cross_sectional_price_z(survey)
    records: List[dict] = []

    for (pesquisa, regiao, produto), grp in survey.groupby(
        ["nome_pesquisa", "nome_regiao_integracao", "produto"]
    ):
        qtd = _to_serie(grp, "quantidade_produzida")
        val = _to_serie(grp, "valor_producao")
        if len(qtd) < 2 and len(val) < 2:
            continue  # série de ponto único → sem estatística temporal

        r_qtd, r_val = _log_returns(qtd), _log_returns(val)
        m_qtd, m_val = _modified_z(r_qtd), _modified_z(r_val)
        ln_preco = {a: math.log(val[a] / qtd[a]) for a in qtd if a in val}
        z_preco_temp = _modified_z(ln_preco)

        anos = set(m_qtd) | set(m_val) | set(z_preco_temp)
        for ano in sorted(anos):
            mq = m_qtd.get(ano, float("nan"))
            mv = m_val.get(ano, float("nan"))
            zt = z_preco_temp.get(ano, float("nan"))
            ztr = z_transversal.get((pesquisa, produto, ano, regiao), float("nan"))
            z_preco = _safe_max_abs(zt, ztr)

            flag_qtd = _abs_or_nan(mq) > Z_THRESHOLD
            flag_val = _abs_or_nan(mv) > Z_THRESHOLD
            flag_preco = (not math.isnan(z_preco)) and z_preco > Z_THRESHOLD
            if not (flag_qtd or flag_val or flag_preco):
                continue

            # ρ na métrica dominante (maior |M|; NaN trata como -1)
            amq = abs(mq) if not math.isnan(mq) else -1.0
            amv = abs(mv) if not math.isnan(mv) else -1.0
            rho = _reversal(r_qtd if amq >= amv else r_val, ano)

            # Triangulação: qual sinal disparou → causa-raiz provável
            gatilhos = []
            if flag_qtd:
                gatilhos.append("qtd")
            if flag_val:
                gatilhos.append("valor")
            if flag_preco:
                gatilhos.append("preco")
            gatilho = "+".join(gatilhos)
            if flag_qtd and flag_val and not flag_preco:
                causa = "choque_real?"  # ambos movem, preço estável
            elif flag_qtd and not flag_val:
                causa = "unidade_quantidade"
            elif flag_val and not flag_qtd:
                causa = "unidade_valor"
            else:
                causa = "preco"

            # Severidade com cada componente capado em SEVERITY_CAP: triagem
            # robusta a artefatos. Componentes brutos seguem nas colunas do CSV.
            def cap(x: float) -> float:
                return 0.0 if math.isnan(x) else min(abs(x), SEVERITY_CAP)

            rho_pos = max(rho, 0.0) if not math.isnan(rho) else 0.0
            severity = (
                W_MQTD * cap(mq)
                + W_MVAL * cap(mv)
                + W_REVERSAO * rho_pos * cap(_safe_max_abs(mq, mv))
                + W_PRECO * cap(z_preco)
            )

            records.append(
                {
                    "nome_pesquisa": pesquisa,
                    "nome_regiao_integracao": regiao,
                    "produto": produto,
                    "ano": ano,
                    "M_qtd": round(mq, 3) if not math.isnan(mq) else None,
                    "M_val": round(mv, 3) if not math.isnan(mv) else None,
                    "rho_reversao": round(rho, 3) if not math.isnan(rho) else None,
                    "z_preco_temporal": round(zt, 3) if not math.isnan(zt) else None,
                    "z_preco_transversal": (
                        round(ztr, 3) if not math.isnan(ztr) else None
                    ),
                    "gatilho": gatilho,
                    "causa_provavel": causa,
                    "severity": round(severity, 3),
                }
            )

    df = pd.DataFrame(records)
    if not df.empty:
        df = df.sort_values("severity", ascending=False).reset_index(drop=True)
    log.info("parte_a.done", linhas_sinalizadas=len(df))
    return df


# ==========================================================================
# Parte B — Censo 2006/2017 vs pesquisa
# ==========================================================================
def _agg_by_key(df: pd.DataFrame) -> Dict[Tuple[str, str, int], Tuple[float, float]]:
    """(regiao, produto_canon, ano) → (quantidade, valor) somados."""
    agg = df.groupby(
        ["nome_regiao_integracao", "produto_canon", "ano"], as_index=False
    )[["quantidade_produzida", "valor_producao"]].sum()
    return {
        (r.nome_regiao_integracao, r.produto_canon, int(r.ano)): (
            float(r.quantidade_produzida),
            float(r.valor_producao),
        )
        for r in agg.itertuples(index=False)
    }


def _window_median(
    sval: Dict[Tuple[str, str, int], Tuple[float, float]],
    regiao: str,
    produto: str,
    years: Iterable[int],
) -> float:
    vals = [
        sval[(regiao, produto, y)][0]
        for y in years
        if (regiao, produto, y) in sval and sval[(regiao, produto, y)][0] > 0
    ]
    return float(np.median(vals)) if vals else float("nan")


def _ln_ratio(num: float, den: float) -> float:
    if num and den and num > 0 and den > 0:
        return math.log(num / den)
    return float("nan")


def parte_b(survey: pd.DataFrame, census: pd.DataFrame) -> pd.DataFrame:
    survey = survey.copy()
    census = census.copy()
    survey["family"] = survey["nome_pesquisa"].map(family_of)
    census["family"] = census["nome_pesquisa"].map(family_of)
    survey["produto_canon"] = survey["produto"].map(lambda p: BRIDGE_PRODUTOS.get(p, p))
    census["produto_canon"] = census["produto"]  # Censo é a referência

    records: List[dict] = []
    for family in sorted(set(survey["family"]) & set(census["family"])):
        s = survey[survey["family"] == family]
        c = census[census["family"] == family]
        sval = _agg_by_key(s)
        cval = _agg_by_key(c)

        keys_s = {(r, p) for (r, p, a) in sval if a in ANCHORS}
        keys_c = {(r, p) for (r, p, a) in cval if a in ANCHORS}

        # Pares matched + lacunas (cada lacuna é um achado)
        for regiao, produto in sorted(keys_s | keys_c):
            in_s, in_c = (regiao, produto) in keys_s, (regiao, produto) in keys_c
            if in_c and not in_s:
                status = "sem_par_pam"
            elif in_s and not in_c:
                status = "sem_par_censo"
            else:
                status = "ok"

            def g(store, ano, idx):  # noqa: ANN001 - helper local
                v = store.get((regiao, produto, ano))
                return v[idx] if v else float("nan")

            c06q, c17q = g(cval, 2006, 0), g(cval, 2017, 0)
            p06q, p17q = g(sval, 2006, 0), g(sval, 2017, 0)
            c06v, c17v = g(cval, 2006, 1), g(cval, 2017, 1)
            p06v, p17v = g(sval, 2006, 1), g(sval, 2017, 1)

            # B1 razões nas âncoras
            razao_2006 = (
                (c06q / p06q) if (p06q and p06q > 0 and c06q > 0) else float("nan")
            )
            razao_2017 = (
                (c17q / p17q) if (p17q and p17q > 0 and c17q > 0) else float("nan")
            )
            divergencia = (
                abs(math.log(razao_2017) - math.log(razao_2006))
                if not (math.isnan(razao_2006) or math.isnan(razao_2017))
                else float("nan")
            )
            # B3 inclinação
            g_censo = _ln_ratio(c17q, c06q)
            g_pam = _ln_ratio(p17q, p06q)
            D = (
                g_censo - g_pam
                if not (math.isnan(g_censo) or math.isnan(g_pam))
                else float("nan")
            )
            # B4 preço por fonte
            pc06 = (c06v / c06q) if (c06q and c06q > 0) else float("nan")
            pp06 = (p06v / p06q) if (p06q and p06q > 0) else float("nan")
            pc17 = (c17v / c17q) if (c17q and c17q > 0) else float("nan")
            pp17 = (p17v / p17q) if (p17q and p17q > 0) else float("nan")
            coer = _safe_max_abs(_ln_ratio(pc06, pp06), _ln_ratio(pc17, pp17))
            # B2 envelope
            env_2006 = _ln_ratio(
                c06q, _window_median(sval, regiao, produto, (2005, 2006, 2007))
            )
            env_2017 = _ln_ratio(
                c17q, _window_median(sval, regiao, produto, (2016, 2017, 2018))
            )

            records.append(
                {
                    "family": family,
                    "nome_regiao_integracao": regiao,
                    "produto": produto,
                    "status_crosswalk": status,
                    "razao_2006": round(razao_2006, 3)
                    if not math.isnan(razao_2006)
                    else None,
                    "razao_2017": round(razao_2017, 3)
                    if not math.isnan(razao_2017)
                    else None,
                    "divergencia_ancoras": round(divergencia, 3)
                    if not math.isnan(divergencia)
                    else None,
                    "D_inclinacao": round(D, 3) if not math.isnan(D) else None,
                    "envelope_2006": round(env_2006, 3)
                    if not math.isnan(env_2006)
                    else None,
                    "envelope_2017": round(env_2017, 3)
                    if not math.isnan(env_2017)
                    else None,
                    "p_censo_2006": round(pc06, 2) if not math.isnan(pc06) else None,
                    "p_pam_2006": round(pp06, 2) if not math.isnan(pp06) else None,
                    "p_censo_2017": round(pc17, 2) if not math.isnan(pc17) else None,
                    "p_pam_2017": round(pp17, 2) if not math.isnan(pp17) else None,
                    "coerencia_preco": round(coer, 3) if not math.isnan(coer) else None,
                }
            )

        # B5 Spearman por (family, regiao, ano) — atribuído às linhas da RI
        spearman_por_regiao: Dict[Tuple[str, int], float] = {}
        regioes = {r for (r, _p) in (keys_s | keys_c)}
        for regiao in regioes:
            for ano in ANCHORS:
                pares = [
                    (cval[(regiao, p, ano)][0], sval[(regiao, p, ano)][0])
                    for (rr, p) in keys_s & keys_c
                    if rr == regiao
                    and (regiao, p, ano) in cval
                    and (regiao, p, ano) in sval
                    and cval[(regiao, p, ano)][0] > 0
                    and sval[(regiao, p, ano)][0] > 0
                ]
                spearman_por_regiao[(regiao, ano)] = _spearman(pares)
        for rec in records:
            if rec["family"] != family:
                continue
            r = rec["nome_regiao_integracao"]
            for ano in ANCHORS:
                sp = spearman_por_regiao.get((r, ano), float("nan"))
                rec[f"spearman_{ano}"] = round(sp, 3) if not math.isnan(sp) else None

    df = pd.DataFrame(records)
    if not df.empty:
        # z-MAD de ln(razao) entre produtos, por (family, ano)
        for ano in ANCHORS:
            col = f"razao_{ano}"
            zcol = f"z_razao_{ano}"
            df[zcol] = None
            for family, grp in df.groupby("family"):
                vals = {
                    idx: math.log(v)
                    for idx, v in grp[col].items()
                    if v is not None and v > 0
                }
                z = _modified_z(vals)
                for idx, zval in z.items():
                    df.at[idx, zcol] = round(zval, 3)
        df = df.sort_values(
            "divergencia_ancoras", ascending=False, na_position="last"
        ).reset_index(drop=True)
    log.info(
        "parte_b.done",
        linhas=len(df),
        sem_par=int((df["status_crosswalk"] != "ok").sum()) if not df.empty else 0,
    )
    return df


# ==========================================================================
# Resumo markdown
# ==========================================================================
def _resumo_md(df_a: pd.DataFrame, df_b: pd.DataFrame, top_n: int = 15) -> str:
    out = ["# Resumo da validação de séries — `pa_indexadores_producao_rural`", ""]
    out.append(
        "> Insumo: arquivos flat do L1 `{dataset}_{quantidade,valor}.json` "
        "(granularidade RI, pós-correção de unidades).\n"
    )

    out.append("## Parte A — variações temporais bruscas (PAM/PEVS)\n")
    if df_a.empty:
        out.append("_Nenhuma série sinalizada._\n")
    else:
        out.append(
            f"{len(df_a)} ano-série sinalizado(s). Top {top_n} por severidade:\n"
        )
        out.append(
            "| pesquisa | RI | produto | ano | M_qtd | M_val | ρ | causa | sev |"
        )
        out.append("|---|---|---|---|---|---|---|---|---|")
        for r in df_a.head(top_n).itertuples(index=False):
            out.append(
                f"| {r.nome_pesquisa} | {r.nome_regiao_integracao} | {r.produto} "
                f"| {r.ano} | {r.M_qtd} | {r.M_val} | {r.rho_reversao} "
                f"| {r.causa_provavel} | {r.severity} |"
            )
        out.append("")

    out.append("## Parte B — Censo 2006/2017 vs pesquisa\n")
    if df_b.empty:
        out.append("_Sem pares comparáveis._\n")
    else:
        ok = df_b[df_b["status_crosswalk"] == "ok"]
        gaps = df_b[df_b["status_crosswalk"] != "ok"]
        out.append(
            f"{len(ok)} par(es) comparável(is); {len(gaps)} lacuna(s) de "
            f"crosswalk (sem par).\n"
        )
        top_d = ok.dropna(subset=["D_inclinacao"]).reindex(
            ok["D_inclinacao"].abs().sort_values(ascending=False).index
        )
        if not top_d.empty:
            out.append(f"Top {top_n} por |D| (discrepância de inclinação 2006→2017):\n")
            out.append(
                "| family | RI | produto | razão_06 | razão_17 | D | coer_preço |"
            )
            out.append("|---|---|---|---|---|---|---|")
            for r in top_d.head(top_n).itertuples(index=False):
                out.append(
                    f"| {r.family} | {r.nome_regiao_integracao} | {r.produto} "
                    f"| {r.razao_2006} | {r.razao_2017} | {r.D_inclinacao} "
                    f"| {r.coerencia_preco} |"
                )
            out.append("")

    out.append("## Calibração (devem sair limpos nos dados pós-correção)\n")
    p99 = float(df_a["severity"].quantile(0.99)) if not df_a.empty else float("nan")
    out.append(
        f"Critério: limpo = nenhum flag acima do p99 global de severidade "
        f"(p99 = {p99:.1f}). Volatilidade interanual normal pós-correção é "
        f"esperada e não conta como erro de unidade.\n"
    )
    for prod in CALIBRACAO_PRODUTOS:
        hits = pd.DataFrame() if df_a.empty else df_a[df_a["produto"] == prod]
        if hits.empty:
            out.append(f"- `{prod}`: ✅ limpo (sem flag).")
            continue
        sev = float(hits["severity"].max())
        if sev <= p99:
            out.append(
                f"- `{prod}`: ✅ {len(hits)} flag(s), severidade máx {sev:.1f} "
                f"≤ p99 — dentro do ruído normal."
            )
        else:
            out.append(
                f"- `{prod}`: ⚠️ {len(hits)} flag(s), severidade máx {sev:.1f} "
                f"> p99 — inspecionar (pode restar resíduo de unidade)."
            )
    out.append("")
    return "\n".join(out)


# ==========================================================================
# Flow
# ==========================================================================
def run(indir: Path, outdir: Path) -> None:
    log.info("flow.start", indir=str(indir), outdir=str(outdir))
    if not indir.is_dir():
        raise FileNotFoundError(
            f"Diretório do L1 não encontrado: {indir}. "
            "Rode antes: uv run python -m dados.export.dump_gold_l1"
        )
    outdir.mkdir(parents=True, exist_ok=True)

    df = load_long(indir)
    survey = df[df["tipo_pesquisa"] == "amostral"]
    census = df[df["tipo_pesquisa"] == "censitaria"]

    df_a = parte_a(survey)
    df_b = parte_b(survey, census)

    a_path = outdir / "relatorio_series_pam_pevs.csv"
    b_path = outdir / "relatorio_censo_vs_series.csv"
    md_path = outdir / "resumo_validacao.md"
    df_a.to_csv(a_path, index=False)
    df_b.to_csv(b_path, index=False)
    md_path.write_text(_resumo_md(df_a, df_b), encoding="utf-8")

    log.info(
        "validate.done",
        parte_a=str(a_path),
        parte_b=str(b_path),
        resumo=str(md_path),
        linhas_a=len(df_a),
        linhas_b=len(df_b),
    )
    log.info("flow.end")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--indir",
        type=Path,
        default=OUTPUT_DIR,
        help="Diretório com os arquivos flat do L1 (default: gold_export_l1/)",
    )
    parser.add_argument(
        "--outdir",
        type=Path,
        default=OUTPUT_DIR,
        help="Diretório de saída dos relatórios (default: gold_export_l1/)",
    )
    args = parser.parse_args()
    try:
        run(args.indir, args.outdir)
    except Exception as exc:  # noqa: BLE001
        log.exception("flow.error", error=str(exc))
        raise


if __name__ == "__main__":
    main()
