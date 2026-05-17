"""Report last commit timestamp per table, grouped by zone and dataset_id.

Requires `track_commit_timestamp = on` on the server. Rows committed before the
setting was enabled (or whose xmin has been frozen by vacuum) return NULL.

Run: uv run python -m scripts.last_update_report
"""

from __future__ import annotations

import os

import psycopg2
from psycopg2 import sql

ZONE_DBS = {
    "raw": os.environ["DB_RAW_ZONE"],
    "silver": os.environ["DB_SILVER_ZONE"],
    "gold": os.environ["DB_GOLD_ZONE"],
}

CONN_KWARGS = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": os.getenv("POSTGRES_PORT", "5432"),
    "user": os.environ["POSTGRES_USER"],
    "password": os.environ["POSTGRES_PASSWORD"],
}

LIST_TABLES = """
SELECT schemaname, tablename
FROM pg_tables
WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
ORDER BY schemaname, tablename;
"""


def last_commit(cur, schema: str, table: str):
    q = sql.SQL("SELECT max(pg_xact_commit_timestamp(xmin)) FROM {}.{}").format(
        sql.Identifier(schema), sql.Identifier(table)
    )
    cur.execute(q)
    return cur.fetchone()[0]


def report():
    rows = []
    for zone, db in ZONE_DBS.items():
        with psycopg2.connect(dbname=db, **CONN_KWARGS) as conn:
            with conn.cursor() as cur:
                cur.execute(LIST_TABLES)
                tables = cur.fetchall()
                for schema, table in tables:
                    ts = last_commit(cur, schema, table)
                    rows.append((zone, schema, table, ts))

    rows.sort(key=lambda r: (r[3] is None, r[3] or 0), reverse=True)
    print(f"{'zone':<8} {'dataset_id':<32} {'table':<48} last_update")
    print("-" * 110)
    for zone, schema, table, ts in rows:
        print(f"{zone:<8} {schema:<32} {table:<48} {ts}")


if __name__ == "__main__":
    report()
