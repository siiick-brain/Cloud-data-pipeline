#!/bin/sh
# Runs once on first PostgreSQL start. Passwords come from env, never from files in git.
set -eu

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
  -v pipeline_pw="$PIPELINE_DB_PASSWORD" \
  -v mcp_pw="$MCP_DB_PASSWORD" \
  -v grafana_pw="$GRAFANA_DB_PASSWORD" \
  -v airflow_pw="$AIRFLOW_DB_PASSWORD" <<'EOSQL'
CREATE ROLE pipeline LOGIN PASSWORD :'pipeline_pw';
CREATE ROLE mcp_ro LOGIN PASSWORD :'mcp_pw';
CREATE ROLE grafana_ro LOGIN PASSWORD :'grafana_pw';
CREATE ROLE airflow LOGIN PASSWORD :'airflow_pw';
CREATE DATABASE airflow OWNER airflow;
REVOKE ALL ON DATABASE warehouse FROM PUBLIC;
REVOKE ALL ON DATABASE airflow FROM PUBLIC;
GRANT CONNECT ON DATABASE warehouse TO pipeline, mcp_ro, grafana_ro;
GRANT CONNECT ON DATABASE airflow TO mcp_ro;
EOSQL
