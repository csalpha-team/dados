# AGENTS.md

Repository: zone-based data pipeline for Contas Alfa. Python flows move data through PostgreSQL zones: raw -> silver -> gold -> export.

## Hard Rules

- Keep strict layering: raw reads external sources, silver reads raw, gold reads silver, and export reads gold. Never read downstream zones from upstream zones.
- Silver and gold tables require a pydantic schema in `dados/<zone>/models/<dataset_id>.py`.
- Every pydantic field must include a `description`, a Python type, and a `unit` in `json_schema_extra`.
- Use `get_logger` from `dados/utils/logging.py` for all logs. Do not use `print` or stdlib `logging`.
- Use `PostgresETL` from `dados/raw/utils/postgres_interactions.py` for all PostgreSQL I/O.
- Temporary pipeline files belong under `tmp_data/<dataset_id>/{input,output}/`.

## Development Commands

- Run a flow with `uv run python -m dados.<zone>.<dataset_id>.<table>`.
- Run tests with `uv run pytest`.
- Run focused tests whenever possible before broad test suites.

## Repository Shape

- `dados/raw/`: source mirroring and landing tables.
- `dados/silver/`: typed, validated, standardized data.
- `dados/gold/`: analytical coefficients and indexers consumed by exports and the Layer 2 algorithm.
- `dados/export/`: export routines for downstream consumers.
- `dados/utils/`: shared logging, path, conversion, and pydantic/Postgres helpers.

## Local Codex Skills

This repository keeps local Codex engineering skills under `.codex/skills/`. That directory is intentionally ignored by git because these are local agent instructions, not project source.

Use the local skills when the task matches:

- `tdd`: test-first feature work or bug fixes using red-green-refactor.
- `diagnose`: debugging, failing flows, data quality regressions, or performance regressions.
- `improve-codebase-architecture`: architecture review, refactoring opportunities, testability, and module-depth work.
- `zoom-out`: mapping unfamiliar code to the bigger pipeline context.
- `to-prd`: turning current context into a product/engineering requirement document.
- `to-issues`: breaking a plan or PRD into independent vertical slices.
- `triage`: issue triage and preparing durable implementation briefs.

When these skills mention a domain glossary or ADRs, use this file, `README.md`, `CLAUDE.md`, `manual_estilo.md`, and any future `CONTEXT.md` or `docs/adr/` files as the project context.
