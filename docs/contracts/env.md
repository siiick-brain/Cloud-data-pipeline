# Runtime contract: services, env vars and file locations

`docker-compose.yml` holds every service, port and memory cap.

## Services (compose network name, host port on 127.0.0.1)
| Service | Inside compose | From host | mem_limit |
|---|---|---|---|
| postgres | postgres:5432 | localhost:5432 | 900m |
| kafka | kafka:9092 | 127.0.0.1:9094 (not localhost: Windows tries IPv6 first) | 800m |
| redis | redis:6379 | localhost:6379 | 200m |
| consumer | runs `python -m ingest.consumer` | none | 300m |
| spark | spark:8000 trigger service | none | 1500m |
| airflow-scheduler | health on 8974 | none | 800m |
| airflow-webserver | airflow-webserver:8080 | localhost:8080 | 600m |
| floci | floci:4566 | localhost:4566 | 400m |
| otel-collector | otel-collector:4318 (OTLP HTTP), 8889 (Prometheus) | none | 200m |
| prometheus | prometheus:9090 | localhost:9090 | 400m |
| grafana | grafana:3000 | localhost:3000 | 300m |
| mcp | mcp:8765 | localhost:8765 | 200m |
Total: 6.6 GB.

There is no separate producer service. The producer runs inside the consumer container:
`docker compose exec consumer python -m ingest.producer --events N`.

## Env vars by consumer
| Var | Used by | Value in compose |
|---|---|---|
| WAREHOUSE_URL | ingest, dags, quality, governance | postgresql://pipeline:...@postgres:5432/warehouse |
| KAFKA_BOOTSTRAP | ingest | kafka:9092 (host: 127.0.0.1:9094) |
| REDIS_URL | ingest | redis://:pw@redis:6379/0 |
| OTEL_EXPORTER_OTLP_ENDPOINT | ingest, dags, spark | http://otel-collector:4318 |
| OTEL_SERVICE_NAME | all instrumented code | see metrics.md |
| SPARK_TRIGGER_URL | dags | http://spark:8000 |
| DELTA_ROOT | spark, mcp | /data/delta |
| JDBC_URL, JDBC_USER, JDBC_PASSWORD | spark | jdbc:postgresql://postgres:5432/warehouse, pipeline |
| SPARK_DRIVER_MEMORY | spark | 1g |
| AIRFLOW_VAR_FORCE_FAIL | dags | empty, or 1 to force one failure |
| MCP_TOKEN, MCP_ALLOWED_ORIGINS, MCP_HOST, MCP_AUDIT_LOG | mcp | from .env |
| WAREHOUSE_RO_URL | mcp | postgresql://mcp_ro:...@postgres:5432/warehouse |
| AIRFLOW_DB_URL | mcp | postgresql://mcp_ro:...@postgres:5432/airflow |
| AWS_ENDPOINT_URL, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION | finops | http://localhost:4566, test, test, us-east-1 |
| GRAFANA_DB_PASSWORD | grafana provisioning | from .env (user grafana_ro) |

## Health endpoints a stream must provide
| Service | Check |
|---|---|
| consumer | touches `/tmp/heartbeat` at least every 30 seconds while the poll loop runs |
| spark | `GET /health` on port 8000 returns 200 |
| mcp | `GET /health` on port 8765 returns 200 without auth and returns no data |

## Force fail flag
The ETL DAG reads the flag from env `AIRFLOW_VAR_FORCE_FAIL` or the Airflow Variable `FORCE_FAIL` (Airflow resolves both through `Variable.get("FORCE_FAIL")`).
When the flag is `1`, the first try of task `extract` fails on purpose. The retry succeeds and then deletes the Variable and pops the env var in its own process.

## Grafana
Provisioning lives in `dashboards/provisioning` (mounted to /etc/grafana/provisioning). Dashboard JSON lives in `dashboards/json` (mounted to /var/lib/grafana/dashboards).
Datasource uids: `warehouse` (PostgreSQL, user grafana_ro, database warehouse) and `prometheus` (http://prometheus:9090).
Dashboard uids: `sales`, `pipeline-health`, `quality`.

## Airflow
DAG ids: `etl_orders` and `elt_gold`. Image built from `dags/Dockerfile` on apache/airflow:2.11.2-python3.11.
The scheduler starts with `dags/start_scheduler.sh`, which runs `airflow db migrate`, creates the admin user, grants `SELECT ON dag_run TO mcp_ro` and starts the scheduler.
REST API at http://localhost:8080/api/v1 with basic auth (AIRFLOW_ADMIN_USER, AIRFLOW_ADMIN_PASSWORD).

## Tests
Host tests: `uv run pytest` (Python 3.11, no Java, no Airflow).
DAG tests run in the Airflow container: `docker compose run --rm airflow-scheduler python -m pytest tests/dags`.
Spark tests run in the Spark container: `docker compose run --rm spark python3 -m pytest tests/spark`.
Tests that need the live stack carry `@pytest.mark.integration`.

## Host port overrides
Prometheus, Grafana, Airflow and MCP host ports come from `PROMETHEUS_PORT`, `GRAFANA_PORT`, `AIRFLOW_PORT` and `MCP_PORT` in `.env` (defaults 9090, 3000, 8080, 8765). On Windows, Hyper-V can reserve 8765; `netsh int ipv4 show excludedportrange protocol=tcp` shows the ranges.
Host-side tests and scripts must read these env vars (tests/conftest.py loads `.env`) and never hard-code the port.
