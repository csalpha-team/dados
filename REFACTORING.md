# Refactoring Guidelines

This is the execution spec for refactoring (and writing new) dataset pipelines in this repo. Rules are imperative — apply them verbatim. Read top-to-bottom; the most load-bearing rules are at the top.

## 1. TL;DR rules

- Every dataset script exposes a single `flow()` entry point composed of `extract / validate / transform / load` functions.
- Every **silver** and **gold** materialized table has a pydantic schema at `dados/<zone>/models/<dataset_id>.py`; every field declares `description`, Python type, and `unit`. Raw is exempt.
- Every dataframe crossing a zone boundary (writing into silver or gold) is validated against that pydantic model before write.
- Postgres I/O only via `PostgresETL` from `dados/raw/utils/postgres_interactions.py` — no raw `psycopg2`/`sqlalchemy` in dataset scripts.
- All logs go through `get_logger(...)` from `dados/utils/logging.py` — no `print`, no stdlib `logging`.
- All temporary files go under `tmp_data/<dataset_id>/{input,output}/` at the repo root — never `../tmp`, never ad-hoc paths. Use `tmp_dir(...)` from `dados/utils/paths.py`.
- Zone dependencies: raw → silver → gold. No gold → gold, no gold → raw, no silver → gold (silver doesn't read gold).
- Multi-table joins / aggregations / enrichment belong in **dbt models**, not Python. Python only does ingestion + per-row cleaning.
- Only the `raw` dbt layer declares `sources:`. Silver and gold are dbt models with `.sql` + `schema.yml`.

## 2. Zone contracts & readiness

Each zone has a fixed input contract, output contract, and readiness checklist. A flow is not "done" until every item ticks.

### 2.1 Raw

- **Input**: external source (IBGE/Comex API, BigQuery, file drop, etc.).
- **Output**: one landing table per dataset, schema `<dataset_id>`, in database `$DB_RAW_ZONE`. Columns may be loosely typed (`VARCHAR(255)`).
- **Pydantic**: optional.
- **Readiness checklist**:
  - [ ] `flow()` entry point exists.
  - [ ] All logging goes through `get_logger`.
  - [ ] `PostgresETL` is the only Postgres caller.
  - [ ] `flow.end` event logged with non-zero `rows`.
- **Allowed consumers**: silver only.

### 2.2 Silver

- **Input**: raw tables (via dbt `{{ source(...) }}` or `PostgresETL` reads).
- **Output**: typed, deduplicated, standardized table at `$DB_SILVER_ZONE.<dataset_id>.<table>`.
- **Pydantic**: **required**. One `BaseModel` per materialized table at `dados/silver/models/<dataset_id>.py`. Every field MUST declare `description`, Python type, and `unit` (see §4).
- **Readiness checklist** (raw checklist plus):
  - [ ] Pydantic schema exists for the output table.
  - [ ] Every column in the schema has `description` and `unit`.
  - [ ] Dataframe validated row-by-row against the schema before write.
  - [ ] Primary key columns are unique (assert in `validate`).
  - [ ] dbt model + `schema.yml` entry exists for the table (when dbt is in place).
- **Allowed consumers**: silver, gold.

### 2.3 Gold

- **Input**: silver only. Never raw, never another gold.
- **Output**: analytic / matrix-ready table at `$DB_AGREGATED_ZONE.<dataset_id>.<table>`.
- **Pydantic**: **required**. Same rules as silver, under `dados/gold/models/<dataset_id>.py`.
- **Readiness checklist**: same as silver, plus:
  - [ ] No `{{ source(...) }}` or raw-table reads in this dataset's SQL/Python — only `{{ ref('silver_*') }}` / silver reads.
- **Allowed consumers**: output zone, downstream apps.

## 3. Dataset flow template

Copy this skeleton into `dados/<zone>/<dataset_id>/<table>.py` (or `flow.py` for single-table datasets) and fill in.

```python
from __future__ import annotations
import os
import pandas as pd
from dotenv import load_dotenv

from dados.utils.logging import get_logger
from dados.raw.utils.postgres_interactions import PostgresETL
# For silver/gold:
# from dados.<zone>.models.<dataset_id> import <ModelClass>

load_dotenv()

DATASET_ID = "<dataset_id>"        # = directory name
ZONE = "<raw|silver|gold>"
TABLE = "<table_name>"

log = get_logger(dataset_id=DATASET_ID, zone=ZONE)


def extract() -> pd.DataFrame:
    """Pull from the upstream source. No transforms."""
    ...


def validate(df: pd.DataFrame) -> pd.DataFrame:
    """Schema check. Raw: row-count + required-cols. Silver/gold: pydantic per row."""
    ...
    return df


def transform(df: pd.DataFrame) -> pd.DataFrame:
    """Per-row cleaning only. Joins/aggregations go in dbt."""
    return df


def load(df: pd.DataFrame) -> None:
    """Write to Postgres via PostgresETL."""
    ...


def flow() -> None:
    log.info("flow.start")
    df = extract();    log.info("extract.done", rows=len(df))
    df = validate(df); log.info("validate.done", rows=len(df))
    df = transform(df);log.info("transform.done", rows=len(df))
    load(df);          log.info("load.done", rows=len(df))
    log.info("flow.end", rows=len(df))


if __name__ == "__main__":
    flow()
```

**Naming**:

- `dataset_id` = the directory name under `dados/<zone>/` (e.g. `al_ibge_ppm`).
- Table name = the script's filename without `.py` (e.g. `efetivo_rebanhos`).
- Pydantic model class = PascalCase of `<dataset_id>_<table>` (e.g. `AlIbgePpmEfetivoRebanhos`).

## 4. Pydantic schema conventions

Location: `dados/<zone>/models/<dataset_id>.py`. One `BaseModel` per materialized table. **Required for silver and gold.**

Every field uses `Field(...)` with three pieces of metadata:

1. **Python type** — `int | None`, `Decimal`, `date`, `str`, etc.
2. **`description=`** — human-readable column description.
3. **`json_schema_extra={"unit": "..."}`** — measurement unit. Allowed values (extend as needed; document additions here):
   - `"BRL"`, `"USD"` (monetary)
   - `"kg"`, `"ton"`, `"L"` (mass/volume)
   - `"head_count"` (livestock, persons)
   - `"hectare"`, `"m2"` (area)
   - `"ratio"`, `"percent"`, `"dimensionless"`
   - `"YYYY"`, `"YYYY-MM"`, `"date"` (temporal)
   - `"code"` (identifiers, classification codes)

Skeleton:

```python
from decimal import Decimal
from pydantic import BaseModel, Field


class AlIbgePpmEfetivoRebanhos(BaseModel):
    ano: int = Field(description="Reference year", json_schema_extra={"unit": "YYYY"})
    id_municipio: str = Field(description="IBGE 7-digit municipality code", json_schema_extra={"unit": "code"})
    id_produto: str = Field(description="IBGE livestock product code", json_schema_extra={"unit": "code"})
    valor: Decimal | None = Field(description="Reported livestock head count", json_schema_extra={"unit": "head_count"})
```

**Validation pattern at the boundary** (canonical — do not mix with pandera):

```python
def validate(df: pd.DataFrame) -> pd.DataFrame:
    rows = df.to_dict("records")
    [AlIbgePpmEfetivoRebanhos(**r) for r in rows]  # raises on any bad row
    return df
```

**Postgres DDL** is derived from the pydantic type — do not maintain a parallel `{'ano': 'VARCHAR(255)'}` dict. Use a small helper `pydantic_to_postgres_columns(Model) -> dict` (to be added to `dados/utils/`) the first time you need it.

A linter test at `tests/test_pydantic_metadata.py` walks every silver/gold model and asserts each field has `description` + `unit`.

## 4.1 Temporary data layout

Every pipeline that needs scratch disk space writes under a fixed layout at the **repo root**:

```text
tmp_data/
  <dataset_id>/
    input/    # raw downloads, API dumps, source files staged before parsing
    output/   # post-processed artefacts staged before load (optional)
```

Rules:

- One `dataset_id` directory per pipeline. Never share scratch space across datasets.
- `input/` and `output/` are both optional — create only what you use. Most raw flows need `input/` only.
- Path resolution: use `tmp_dir(dataset_id, kind)` from `dados/utils/paths.py`. Do not hardcode `../tmp/...` or relative paths.
- Override the root via `TMP_DATA_DIR` env var (default: `tmp_data` relative to CWD/repo root).
- `tmp_data/` is git-ignored. Treat its contents as disposable between runs.

Helper signature:

```python
from dados.utils.paths import tmp_dir

input_dir  = tmp_dir("al_ibge_ppm", "input")   # tmp_data/al_ibge_ppm/input/
output_dir = tmp_dir("al_ibge_ppm", "output")  # tmp_data/al_ibge_ppm/output/
```

## 5. Logging conventions (loguru)

One factory: `dados/utils/logging.py` exposing `get_logger(dataset_id: str, zone: str) -> Logger`.

Standard event names (use verbatim):

- `flow.start`, `flow.end`
- `extract.done`, `validate.done`, `transform.done`, `load.done`
- `*.error` on exceptions
- Add new events only by documenting them here first.

Standard fields per event: `dataset_id`, `zone` (bound once), plus `rows`, `duration_ms`, `step` where relevant.

Configuration is done once at import: format, level via `LOG_LEVEL` env (default `INFO`), sinks to stdout and rotating file `logs/<zone>/<dataset_id>.log`.

**Hard rules**:

- No `print()`.
- No stdlib `logging` in dataset scripts.
- No ad-hoc `logger = logging.getLogger(...)`.

## 6. Shared utilities catalog

If you're about to write a helper, grep this table first.

| What | Where | When |
|------|-------|------|
| `PostgresETL` | `dados/raw/utils/postgres_interactions.py` | All Postgres I/O |
| `get_logger` | `dados/utils/logging.py` | Every flow logger |
| `tmp_dir` | `dados/utils/paths.py` | Resolve `tmp_data/<dataset_id>/{input,output}/` paths |
| `async_crawler_ibge_municipio` | `dados/raw/utils/ibge_api_crawler.py` | IBGE Agregados API fetches |
| `fix_ibge_digits`, `fix_ibge_x_digit` | `dados/silver/utils.py` | Cleaning IBGE special codes (`.`, `X`, `-`) in silver |
| `parse_pam_json` | `dados/raw/br_ibge_pam/utils.py` | Parsing IBGE PAM/PPM JSON payloads |

## 7. Migration playbook

Apply this order when refactoring an existing script:

1. Identify `dataset_id` (directory) and `<table>` (filename).
2. (silver/gold only) Create `dados/<zone>/models/<dataset_id>.py` with the pydantic schema — every field carries `description` + `unit`.
3. Replace `print` / stdlib logging with `get_logger(...)` and the standard event names from §5.
4. Split procedural code into `extract / validate / transform / load`; add a `flow()` and `if __name__ == "__main__": flow()`.
5. Replace inline `psycopg2` / `sqlalchemy` with `PostgresETL`.
6. Move multi-table joins / aggregations into a dbt model (silver or gold). The Python script keeps only ingestion + per-row work.
7. Run verification (§8).

## 8. Verification & checks

Run after every refactor:

```bash
uv run python -m dados.<zone>.<dataset_id>.<table>          # flow executes
uv run pytest tests/<zone>/test_<dataset_id>.py             # if tests exist
uv run pytest tests/test_pydantic_metadata.py               # description+unit lint
docker compose exec postgres psql -U postgres \
    -d $DB_RAW_ZONE -c "\dt <dataset_id>.*"                 # table landed
tail -n 20 logs/<zone>/<dataset_id>.log                     # flow.start → flow.end present
```

Confirm: `flow.start` and `flow.end` events are present, `flow.end` reports a non-zero `rows`, and no `*.error` events.

## 9. Rationale (brief)

We want one place (dbt) for transformations, one place (pydantic) for column contracts, one place (loguru) for observability, and one shape (`flow()` + four named steps) for every dataset. The current scripts are flat procedural files with ad-hoc column dicts, scattered `print` calls, and cross-zone shortcuts. The pillars above remove that variance so a new dataset is a fill-in-the-blanks exercise and an old one can be audited mechanically.