"""Export the gold tables that feed the downstream Layer 2 algorithm.

Reads the gold zone via :class:`PostgresETL` and materialises the Layer 2
package under ``gold_export/layer2_new_values/``:

- ``cost_values.csv``               (pa_coeficientes_custo.preparacao_camada_custo)
- ``consumption_values.csv``        (br_coeficientes_consumo.preparacao_camada_consumo)
- ``investment_coefficients.json``  (br_coeficientes_investimento.coeficientes_investimento)
- ``export_coefficients.json``      (br_coeficientes_exportacao.preparacao_camada_exportacao)
- ``income_productivity.json``      (br_coeficientes_renda.renda_produtividade)
- ``income_salary.json``            (br_coeficientes_renda.renda_salario)

The three ``*_incidence.json`` files and ``l2_input_schemas_examples.md``
already living in the output directory are exogenous configuration — this flow
leaves them in place and packages them as-is into the final zip.
"""

from __future__ import annotations

import json
import os
import zipfile
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

import pandas as pd
from dotenv import load_dotenv

from dados.raw.utils.postgres_interactions import PostgresETL
from dados.utils.logging import get_logger

load_dotenv()

DATASET_ID = "gold_export"
ZONE = "export"

REPO_ROOT = Path(__file__).resolve().parents[2]
OUTPUT_ROOT = REPO_ROOT / "gold_export"
OUTPUT_DIR = OUTPUT_ROOT / "layer2_new_values"
ZIP_PATH = OUTPUT_ROOT / "layer2_new_values.zip"

log = get_logger(dataset_id=DATASET_ID, zone=ZONE)

GENERATED_FILES = {
    "cost_values.csv",
    "consumption_values.csv",
    "investment_coefficients.json",
    "export_coefficients.json",
    "income_productivity.json",
    "income_salary.json",
}


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


def _write_json(path: Path, payload) -> None:
    with path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2, default=float)


def export_cost_values(output_dir: Path | None = None) -> Path:
    output_dir = OUTPUT_DIR if output_dir is None else output_dir
    df = _read(
        "pa_coeficientes_custo",
        "SELECT ano, nome_regiao_integracao, tipo_coeff, valor "
        "FROM pa_coeficientes_custo.preparacao_camada_custo",
    )
    out = output_dir / "cost_values.csv"
    df.to_csv(out, index=False, encoding="utf-8")
    log.info("export.cost_values", rows=len(df), path=str(out))
    return out


def export_consumption_values(output_dir: Path | None = None) -> Path:
    output_dir = OUTPUT_DIR if output_dir is None else output_dir
    df = _read(
        "br_coeficientes_consumo",
        "SELECT ano, coeff_key, valor "
        "FROM br_coeficientes_consumo.preparacao_camada_consumo",
    )
    if df["ano"].nunique() > 1:
        latest = int(df["ano"].max())
        log.info("export.consumption_values.pick_year", year=latest)
        df = df[df["ano"] == latest]

    out = output_dir / "consumption_values.csv"
    df.to_csv(out, index=False, encoding="utf-8")
    log.info("export.consumption_values", rows=len(df), path=str(out))
    return out


def export_investment_coefficients(output_dir: Path | None = None) -> Path:
    output_dir = OUTPUT_DIR if output_dir is None else output_dir
    df = _read(
        "br_coeficientes_investimento",
        "SELECT coeff_key, coeff "
        "FROM br_coeficientes_investimento.coeficientes_investimento",
    )
    payload = {row["coeff_key"]: float(row["coeff"]) for _, row in df.iterrows()}
    out = output_dir / "investment_coefficients.json"
    _write_json(out, payload)
    log.info("export.investment_coefficients", keys=len(payload), path=str(out))
    return out


def export_export_coefficients(output_dir: Path | None = None) -> Path:
    output_dir = OUTPUT_DIR if output_dir is None else output_dir
    df = _read(
        "br_coeficientes_exportacao",
        "SELECT ano, produto, valor_fob_dolar, valor_fob_real "
        "FROM br_coeficientes_exportacao.preparacao_camada_exportacao",
    )
    payload: dict[str, list[dict]] = {}
    for ano, grp in df.groupby("ano", sort=True):
        payload[str(int(ano))] = [
            {
                "produto": r["produto"],
                "valor_fob_dolar": float(r["valor_fob_dolar"])
                if r["valor_fob_dolar"] is not None
                else None,
                "valor_fob_real": float(r["valor_fob_real"])
                if r["valor_fob_real"] is not None
                else None,
            }
            for _, r in grp.iterrows()
        ]
    out = output_dir / "export_coefficients.json"
    _write_json(out, payload)
    log.info("export.export_coefficients", years=len(payload), path=str(out))
    return out


def _yearly_series(table: str) -> dict[str, dict[str, float]]:
    df = _read(
        "br_coeficientes_renda",
        f"SELECT ano, conta_alfa, coeff FROM br_coeficientes_renda.{table}",
    )
    payload: dict[str, dict[str, float]] = {}
    for ano, grp in df.groupby("ano", sort=True):
        payload[str(int(ano))] = {
            r["conta_alfa"]: float(r["coeff"]) if r["coeff"] is not None else None
            for _, r in grp.iterrows()
        }
    return payload


def export_income_productivity(output_dir: Path | None = None) -> Path:
    output_dir = OUTPUT_DIR if output_dir is None else output_dir
    payload = _yearly_series("renda_produtividade")
    out = output_dir / "income_productivity.json"
    _write_json(out, payload)
    log.info("export.income_productivity", years=len(payload), path=str(out))
    return out


def export_income_salary(output_dir: Path | None = None) -> Path:
    output_dir = OUTPUT_DIR if output_dir is None else output_dir
    payload = _yearly_series("renda_salario")
    out = output_dir / "income_salary.json"
    _write_json(out, payload)
    log.info("export.income_salary", years=len(payload), path=str(out))
    return out


GENERATORS = (
    export_cost_values,
    export_consumption_values,
    export_investment_coefficients,
    export_export_coefficients,
    export_income_productivity,
    export_income_salary,
)


def bundle_zip(output_dir: Path | None = None, zip_path: Path | None = None) -> Path:
    output_dir = OUTPUT_DIR if output_dir is None else output_dir
    zip_path = ZIP_PATH if zip_path is None else zip_path
    if zip_path.exists():
        zip_path.unlink()
    files = sorted(
        p
        for p in output_dir.iterdir()
        if p.is_file() and not p.name.startswith(".~lock.")
    )
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for f in files:
            zf.write(f, arcname=f"gold_export/{output_dir.name}/{f.name}")
    log.info("export.zip", files=len(files), path=str(zip_path))
    return zip_path


def _clean_output_dir(output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    for path in output_dir.glob(".~lock.*"):
        path.unlink()
    for filename in GENERATED_FILES:
        path = output_dir / filename
        if path.exists():
            path.unlink()


def _run_package(
    output_dir: Path,
    zip_path: Path,
    generators: tuple,
) -> Path:
    log.info("package.start", output_dir=str(output_dir))
    _clean_output_dir(output_dir)
    for gen in generators:
        gen(output_dir)
    out = bundle_zip(output_dir, zip_path)
    log.info("package.end", output_dir=str(output_dir), zip=str(out))
    return out


def flow() -> None:
    log.info("flow.start", output_root=str(OUTPUT_ROOT))
    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
    try:
        _run_package(OUTPUT_DIR, ZIP_PATH, GENERATORS)
    except Exception as exc:
        log.exception("flow.error", error=str(exc))
        raise
    log.info("flow.end", zip=str(ZIP_PATH))


if __name__ == "__main__":
    flow()
