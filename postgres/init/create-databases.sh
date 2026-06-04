#!/bin/bash
set -e

# This script creates the zone architecture databases using environment variables
echo "Creating zone architecture databases..."

# Wait for PostgreSQL to be ready
# Check to see if server is up and accepting conections to default postgres database
until pg_isready -U "$POSTGRES_USER" -d "postgres"; do
  echo "Waiting for PostgreSQL to be ready..."
  sleep 2
done

# Process the template SQL file to replace environment variables
echo "Processing SQL template with zone names:"
echo "  - Dados Brutos: $DB_RAW_ZONE"
echo "  - Dados Tratados: $DB_TRUSTED_ZONE"
echo "  - Dados Gold: ${DB_GOLD_ZONE:-$DB_AGREGATED_ZONE}"
echo "  - Dados Agregados: $DB_AGREGATED_ZONE"
echo "  - Dados de Saída do Algoritmo: $DB_OUTPUT_ZONE"

# Create a temporary file with environment variables substituted
cat /docker-entrypoint-initdb.d/init.sql.template | \
  sed "s|\${DB_RAW_ZONE}|$DB_RAW_ZONE|g" | \
  sed "s|\${DB_TRUSTED_ZONE}|$DB_TRUSTED_ZONE|g" | \
  sed "s|\${DB_AGREGATED_ZONE}|$DB_AGREGATED_ZONE|g" | \
  sed "s|\${DB_OUTPUT_ZONE}|$DB_OUTPUT_ZONE|g" | \
  sed "s|\${POSTGRES_USER}|$POSTGRES_USER|g" \
  > /tmp/init_processed.sql

# Run the processed SQL script through psql
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "postgres" -f /tmp/init_processed.sql

if [ -n "$DB_GOLD_ZONE" ] && [ "$DB_GOLD_ZONE" != "$DB_AGREGATED_ZONE" ]; then
  echo "Creating compatibility gold database: $DB_GOLD_ZONE"
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "postgres" <<SQL
SELECT 'CREATE DATABASE ${DB_GOLD_ZONE}'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '${DB_GOLD_ZONE}')\gexec
GRANT ALL PRIVILEGES ON DATABASE ${DB_GOLD_ZONE} TO ${POSTGRES_USER};
SQL
fi

echo "Zone architecture databases created successfully!"
