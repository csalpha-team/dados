# CLAUDE.md

Repository: zone-based data pipeline (raw → silver → gold) on PostgreSQL. Python flows handle ingestion and transformation.

## Hard rules

- **Strict layering: raw → silver → gold.** No gold → gold, no gold → raw. Downstream zones consume only the layer directly above.
- **Silver and gold tables require a pydantic schema** at `dados/<zone>/models/<dataset_id>.py`. Every field carries `description`, Python type, and `unit`.
- **All logs use `get_logger`** from `dados/utils/logging.py`. No `print`, no stdlib `logging`.
- **All Postgres calls use `PostgresETL`** from `dados/raw/utils/postgres_interactions.py`.

## For any refactor or new dataset work

Read [`REFACTORING.md`](./REFACTORING.md) first. It is the execution spec.

## Quick reference

| Zone   | DB env var           | Default schema        | Source directory      |
|--------|----------------------|-----------------------|------------------------|
| raw    | `DB_RAW_ZONE`        | `<dataset_id>`        | `dados/raw/`           |
| silver | `DB_SILVER_ZONE`     | `<dataset_id>`        | `dados/silver/`        |
| gold   | `DB_AGREGATED_ZONE`  | `<dataset_id>`        | `dados/gold/`          |

Run a flow: `uv run python -m dados.<zone>.<dataset_id>.<table>`
