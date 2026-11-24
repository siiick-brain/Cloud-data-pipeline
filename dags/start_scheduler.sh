#!/usr/bin/env bash
# Scheduler entrypoint (docs/contracts/env.md): migrate, admin user, mcp_ro grant, then the scheduler.
set -euo pipefail

airflow db migrate

# users create exits 0 and changes nothing when the user already exists
airflow users create --role Admin --username "${AIRFLOW_ADMIN_USER}" --password "${AIRFLOW_ADMIN_PASSWORD}" \
  --firstname Admin --lastname User --email admin@example.com

# dag_run exists only after the migration, so the grant lives here and not in sql/
python - <<'PY'
import os

import sqlalchemy

engine = sqlalchemy.create_engine(os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"])
with engine.begin() as conn:
    conn.execute(sqlalchemy.text("GRANT SELECT ON dag_run TO mcp_ro"))
PY

exec airflow scheduler
