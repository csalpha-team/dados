#!/usr/bin/env bash
# Report last commit timestamp per table, grouped by zone and dataset_id.
# Runs entirely inside the postgres container via docker exec.
# Requires `track_commit_timestamp = on` (already set in custom-entrypoint.sh).
set -euo pipefail

CONTAINER="${POSTGRES_CONTAINER:-postgres_csalpha}"

SQL=$(cat <<'EOF'
SELECT format(
  'SELECT %L::text, %L::text, max(pg_xact_commit_timestamp(xmin))::text FROM %I.%I',
  n.nspname, c.relname, n.nspname, c.relname
)
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind = 'r'
  AND n.nspname NOT IN ('pg_catalog','information_schema','pg_toast')
ORDER BY n.nspname, c.relname
\gexec
EOF
)

printf "%-8s %-32s %-48s %s\n" zone dataset_id table last_update
printf -- '-%.0s' {1..110}; echo

for zone in raw silver gold; do
  db="${zone}_zone"
  echo "$SQL" | docker exec -e PGPASSWORD=postgres -i "$CONTAINER" \
    psql -U postgres -d "$db" -At -F $'\t' -q -v ON_ERROR_STOP=0 2>/dev/null \
  | while IFS=$'\t' read -r schema table ts; do
      [ -z "$schema" ] && continue
      printf "%-8s %-32s %-48s %s\n" "$zone" "$schema" "$table" "${ts:-NULL}"
    done
done
