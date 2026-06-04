"""Export the gold tables that feed the downstream Layer 1 algorithm.

Reads the gold zone (`pa_indexadores_producao_rural`) via :class:`PostgresETL`
and materialises the artefact set consumed by the L1 algorithm, mirroring the
shapes captured in ``benchmark/``:

- **12 flat files** — ``{dataset}_{quantidade|valor}.json``, one metric per
  file, shape ``nome_regiao_integracao -> produto -> ano(str) -> float``:

    extracao_vegetal_pevs / lavoura_permanente_pam / lavoura_temporaria_pam
    (amostral, one gold table each), and
    extracao_vegetal_censo / lavoura_permanente_censo / lavoura_temporaria_censo
    (censitária, the 2006 + 2017 gold tables merged per family).

- **1 hierarchical file** — ``censo_autoconsumo.json``, census tables only,
  top-level key ``vetores_producao_rural`` with the commerce split per leaf
  (``quantidade_produzida``, ``valor_producao``, ``comercio_quantidade_produzida``,
  ``comercio_valor_producao``).

Schemas live in :mod:`dados.export.models`.
"""

from __future__ import annotations

import os
import zipfile
from contextlib import contextmanager
from pathlib import Path
from typing import Dict, Iterator

import pandas as pd
from dotenv import load_dotenv
from pydantic import BaseModel

from dados.export.models import (
    SerieRegional,
    VetorAutoconsumo,
    VetoresProducaoRural,
)
from dados.raw.utils.postgres_interactions import PostgresETL
from dados.utils.logging import get_logger

load_dotenv()

DATASET_ID = "pa_indexadores_producao_rural"
ZONE = "export"
SCHEMA = "pa_indexadores_producao_rural"

REPO_ROOT = Path(__file__).resolve().parents[2]
OUTPUT_DIR = REPO_ROOT / "gold_export_l1"
ZIP_PATH = REPO_ROOT / "gold_export_l1.zip"

log = get_logger(dataset_id=DATASET_ID, zone=ZONE)

# Amostral surveys: one gold table → one flat file pair (quantidade + valor).
AMOSTRAL = (
    "extracao_vegetal_pevs",
    "lavoura_permanente_pam",
    "lavoura_temporaria_pam",
)

# Census families: the per-year tables are merged into a single flat file pair.
# Lavoura temporária 2006 uses the `_2284` table — the only 2006 temporária
# table carrying the commerce split (the plain table has no `comercio_*`).
CENSUS_FAMILIES: Dict[str, tuple] = {
    "extracao_vegetal_censo": (
        "extracao_vegetal_censo_2006",
        "extracao_vegetal_censo_2017",
    ),
    "lavoura_permanente_censo": (
        "lavoura_permanente_censo_2006",
        "lavoura_permanente_censo_2017",
    ),
    "lavoura_temporaria_censo": (
        "lavoura_temporaria_censo_2006_2284",
        "lavoura_temporaria_censo_2017",
    ),
}

# Hierarchical autoconsumo artefact: nome_pesquisa key → source gold table.
CENSUS_AUTOCONSUMO: Dict[str, str] = {
    "extracao_vegetal_censo_2006": "extracao_vegetal_censo_2006",
    "extracao_vegetal_censo_2017": "extracao_vegetal_censo_2017",
    "lavoura_permanente_censo_2006": "lavoura_permanente_censo_2006",
    "lavoura_permanente_censo_2017": "lavoura_permanente_censo_2017",
    "lavoura_temporaria_censo_2006": "lavoura_temporaria_censo_2006_2284",
    "lavoura_temporaria_censo_2017": "lavoura_temporaria_censo_2017",
}

METRICS = {
    "quantidade": "quantidade_produzida",
    "valor": "valor_producao",
}

_NA_REGIAO = "NaN"  # unmapped municipalities (no integration region)


@contextmanager
def _gold_db(schema: str) -> Iterator[PostgresETL]:
    with PostgresETL(
        host="localhost",
        database=os.getenv("DB_GOLD_ZONE") or os.getenv("DB_AGREGATED_ZONE"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        schema=schema,
    ) as db:
        yield db


def _read(schema: str, query: str) -> pd.DataFrame:
    with _gold_db(schema) as db:
        return db.download_data(query)


def _write_json(path: Path, model: BaseModel) -> None:
    with path.open("w", encoding="utf-8") as fh:
        fh.write(model.model_dump_json(indent=2))


def _flat_query(tables: tuple) -> str:
    """Aggregate quantidade + valor by (regiao, produto, ano).

    Multiple tables are ``UNION ALL``-ed before grouping so a census family's
    2006 and 2017 tables merge into one série.
    """
    select_cols = (
        "SELECT nome_regiao_integracao, produto, ano, "
        "quantidade_produzida, valor_producao "
    )
    union = " UNION ALL ".join(f"{select_cols}FROM {SCHEMA}.{t}" for t in tables)
    return (
        "SELECT nome_regiao_integracao, produto, ano, "
        "ROUND(SUM(quantidade_produzida::numeric), 2) AS quantidade_produzida, "
        "ROUND(SUM(valor_producao::numeric), 2) AS valor_producao "
        f"FROM ({union}) AS u "
        "GROUP BY 1, 2, 3"
    )


def _build_serie(df: pd.DataFrame, metric_col: str) -> SerieRegional:
    """Nest a long DataFrame into ``regiao -> produto -> ano(str) -> float``."""
    serie: Dict = {}
    for row in df.itertuples(index=False):
        regiao = getattr(row, "nome_regiao_integracao")
        regiao = _NA_REGIAO if pd.isna(regiao) else str(regiao)
        produto = getattr(row, "produto")
        ano = str(int(getattr(row, "ano")))
        value = getattr(row, metric_col)
        leaf = serie.setdefault(regiao, {}).setdefault(produto, {})
        leaf[ano] = 0.0 if pd.isna(value) else round(float(value), 2)
    return SerieRegional(serie)


def export_flat() -> list[Path]:
    """Write the 12 flat metric files (amostral tables + census families)."""
    sources: Dict[str, tuple] = {name: (name,) for name in AMOSTRAL}
    sources.update(CENSUS_FAMILIES)

    written: list[Path] = []
    for name, tables in sources.items():
        df = _read(SCHEMA, _flat_query(tables))
        for metric, col in METRICS.items():
            out = OUTPUT_DIR / f"{name}_{metric}.json"
            _write_json(out, _build_serie(df, col))
            written.append(out)
        log.info("export.flat", dataset=name, rows=len(df), tables=list(tables))
    return written


def _autoconsumo_query(table: str) -> str:
    return (
        "SELECT ano, nome_regiao_integracao, produto, "
        "ROUND(SUM(quantidade_produzida::numeric), 2) AS quantidade_produzida, "
        "ROUND(SUM(valor_producao::numeric), 2) AS valor_producao, "
        "ROUND(SUM(comercio_quantidade_produzida::numeric), 2) AS comercio_quantidade_produzida, "
        "ROUND(SUM(comercio_valor_producao::numeric), 2) AS comercio_valor_producao "
        f"FROM {SCHEMA}.{table} "
        "GROUP BY 1, 2, 3"
    )


def _accumulate_autoconsumo(
    accumulator: Dict, df: pd.DataFrame, nome_pesquisa: str
) -> None:
    if df.empty:
        log.warning("export.empty_table", nome_pesquisa=nome_pesquisa)
        return
    df = df.fillna(0)
    for row in df.itertuples(index=False):
        regiao = getattr(row, "nome_regiao_integracao")
        regiao = _NA_REGIAO if pd.isna(regiao) else str(regiao)
        produto = getattr(row, "produto")
        ano = str(int(getattr(row, "ano")))
        leaf = (
            accumulator.setdefault(nome_pesquisa, {})
            .setdefault("censitaria", {})
            .setdefault(regiao, {})
            .setdefault(produto, {})
        )
        leaf[ano] = VetorAutoconsumo(
            quantidade_produzida=float(getattr(row, "quantidade_produzida")),
            valor_producao=float(getattr(row, "valor_producao")),
            comercio_quantidade_produzida=float(
                getattr(row, "comercio_quantidade_produzida")
            ),
            comercio_valor_producao=float(getattr(row, "comercio_valor_producao")),
        )


def export_censo_autoconsumo() -> Path:
    """Write the hierarchical census artefact with the commerce split."""
    accumulator: Dict = {}
    for nome_pesquisa, table in CENSUS_AUTOCONSUMO.items():
        df = _read(SCHEMA, _autoconsumo_query(table))
        _accumulate_autoconsumo(accumulator, df, nome_pesquisa=nome_pesquisa)
        log.info("export.autoconsumo", nome_pesquisa=nome_pesquisa, rows=len(df))

    model = VetoresProducaoRural(vetores_producao_rural=accumulator)
    out = OUTPUT_DIR / "censo_autoconsumo.json"
    _write_json(out, model)
    log.info("export.censo_autoconsumo", path=str(out))
    return out


GENERATORS = (export_flat, export_censo_autoconsumo)


def bundle_zip(files: list[Path]) -> Path:
    if ZIP_PATH.exists():
        ZIP_PATH.unlink()
    with zipfile.ZipFile(ZIP_PATH, "w", zipfile.ZIP_DEFLATED) as zf:
        for f in sorted(files):
            zf.write(f, arcname=f"gold_export_l1/{f.name}")
    log.info("export.zip", files=len(files), path=str(ZIP_PATH))
    return ZIP_PATH


def flow() -> None:
    log.info("flow.start", output_dir=str(OUTPUT_DIR))
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    try:
        produced: list[Path] = []
        for gen in GENERATORS:
            result = gen()
            produced.extend(result if isinstance(result, list) else [result])
        bundle_zip(produced)
    except Exception as exc:
        log.exception("flow.error", error=str(exc))
        raise
    log.info("flow.end", zip=str(ZIP_PATH))


if __name__ == "__main__":
    flow()
