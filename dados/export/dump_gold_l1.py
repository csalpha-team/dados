"""Export the gold tables that feed the downstream Layer 1 algorithm.

Reads the gold zone (`pa_indexadores_producao_rural`) via :class:`PostgresETL`
and materialises a single consolidated artefact:

- ``vetores_producao_rural.json``

The payload is a Pydantic-validated hierarchical map:

    vetores_producao_rural
      └── nome_pesquisa        (table name: extracao_vegetal_pevs, ...)
            └── tipo_pesquisa  ("amostral" | "censitaria")
                  └── nome_regiao_integracao
                        └── produto
                              └── ano (str) → {quantidade_produzida,
                                               valor_producao,
                                               comercio_quantidade_produzida,
                                               comercio_valor_producao}

PEVS/PAM tables don't publish comercio_* columns; those leaves are 0.0.
"""

from __future__ import annotations

import os
import zipfile
from contextlib import contextmanager
from pathlib import Path
from typing import Dict, Iterator

import pandas as pd
from dotenv import load_dotenv
from pydantic import BaseModel, Field

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

AMOSTRAL_TABLES = (
    "extracao_vegetal_pevs",
    "lavoura_permanente_pam",
    "lavoura_temporaria_pam",
)
CENSITARIA_TABLES = (
    "extracao_vegetal_censo_2006",
    "lavoura_permanente_censo_2006",
    "lavoura_temporaria_censo_2006_2284",
    "extracao_vegetal_censo_2017",
    "lavoura_permanente_censo_2017",
    "lavoura_temporaria_censo_2017",
)
HAS_COMERCIO = set(CENSITARIA_TABLES)


class VetoresProducaoRuralBase(BaseModel):
    quantidade_produzida: float = Field(
        ..., description="Quantidade total produzida na unidade de medida"
    )
    valor_producao: float = Field(..., description="Valor financeiro da produção")
    comercio_quantidade_produzida: float = Field(
        ..., description="Quantidade produzida destinada ao comércio"
    )
    comercio_valor_producao: float = Field(
        ..., description="Valor financeiro da produção destinada ao comércio"
    )


AnoMap = Dict[str, VetoresProducaoRuralBase]
ProdutoMap = Dict[str, AnoMap]
RegiaoMap = Dict[str, ProdutoMap]
TipoPesquisaMap = Dict[str, RegiaoMap]
NomePesquisaMap = Dict[str, TipoPesquisaMap]


class VetoresProducaoRural(BaseModel):
    vetores_producao_rural: NomePesquisaMap


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


def _query(table_name: str, has_comercio: bool) -> str:
    comercio = (
        "ROUND(SUM(comercio_quantidade_produzida), 2) AS comercio_quantidade_produzida, "
        "ROUND(SUM(comercio_valor_producao), 2) AS comercio_valor_producao"
        if has_comercio
        else "0::numeric AS comercio_quantidade_produzida, "
        "0::numeric AS comercio_valor_producao"
    )
    return (
        "SELECT ano, nome_regiao_integracao, produto, "
        "ROUND(SUM(quantidade_produzida), 2) AS quantidade_produzida, "
        "ROUND(SUM(valor_producao), 2) AS valor_producao, "
        f"{comercio} "
        f"FROM {SCHEMA}.{table_name} "
        "GROUP BY 1, 2, 3"
    )


def _accumulate(
    accumulator: Dict,
    df: pd.DataFrame,
    tipo_pesquisa: str,
    nome_pesquisa: str,
) -> None:
    if df.empty:
        log.warning("export.empty_table", nome_pesquisa=nome_pesquisa)
        return

    df = df.fillna(0)
    for row in df.itertuples(index=False):
        regiao = getattr(row, "nome_regiao_integracao")
        produto = getattr(row, "produto")
        ano = str(int(getattr(row, "ano")))

        leaf = (
            accumulator.setdefault(nome_pesquisa, {})
            .setdefault(tipo_pesquisa, {})
            .setdefault(regiao, {})
            .setdefault(produto, {})
        )
        leaf[ano] = VetoresProducaoRuralBase(
            quantidade_produzida=float(getattr(row, "quantidade_produzida")),
            valor_producao=float(getattr(row, "valor_producao")),
            comercio_quantidade_produzida=float(
                getattr(row, "comercio_quantidade_produzida")
            ),
            comercio_valor_producao=float(getattr(row, "comercio_valor_producao")),
        )


def export_vetores_producao_rural() -> Path:
    accumulator: Dict = {}

    for table in AMOSTRAL_TABLES:
        df = _read(SCHEMA, _query(table, has_comercio=False))
        _accumulate(accumulator, df, tipo_pesquisa="amostral", nome_pesquisa=table)
        log.info("export.table", nome_pesquisa=table, tipo="amostral", rows=len(df))

    for table in CENSITARIA_TABLES:
        df = _read(SCHEMA, _query(table, has_comercio=True))
        _accumulate(accumulator, df, tipo_pesquisa="censitaria", nome_pesquisa=table)
        log.info("export.table", nome_pesquisa=table, tipo="censitaria", rows=len(df))

    model = VetoresProducaoRural(vetores_producao_rural=accumulator)
    out = OUTPUT_DIR / "vetores_producao_rural.json"
    _write_json(out, model)
    log.info("export.vetores_producao_rural", path=str(out))
    return out


GENERATORS = (export_vetores_producao_rural,)


def bundle_zip() -> Path:
    if ZIP_PATH.exists():
        ZIP_PATH.unlink()
    files = sorted(p for p in OUTPUT_DIR.iterdir() if p.is_file())
    with zipfile.ZipFile(ZIP_PATH, "w", zipfile.ZIP_DEFLATED) as zf:
        for f in files:
            zf.write(f, arcname=f"gold_export_l1/{f.name}")
    log.info("export.zip", files=len(files), path=str(ZIP_PATH))
    return ZIP_PATH


def flow() -> None:
    log.info("flow.start", output_dir=str(OUTPUT_DIR))
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    try:
        for gen in GENERATORS:
            gen()
        bundle_zip()
    except Exception as exc:
        log.exception("flow.error", error=str(exc))
        raise
    log.info("flow.end", zip=str(ZIP_PATH))


if __name__ == "__main__":
    flow()
