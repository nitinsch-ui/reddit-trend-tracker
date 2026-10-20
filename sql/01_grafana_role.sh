#!/bin/sh
# Read-only role for Grafana, so the dashboard cannot modify pipeline data.
set -e
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
     -v grafana_password="$GRAFANA_DB_PASSWORD" <<'SQL'
CREATE ROLE grafana_reader LOGIN PASSWORD :'grafana_password';
GRANT CONNECT ON DATABASE :"DBNAME" TO grafana_reader;
GRANT USAGE ON SCHEMA public TO grafana_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO grafana_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO grafana_reader;
SQL
