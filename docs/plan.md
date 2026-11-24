# Build plan

## Key decisions
- I froze the interfaces first in `docs/contracts`: event schema, tables, Delta paths, metric names, MCP tools and env vars. Every part was built against those files.
- Airflow 2.11.2, because Airflow 3 removed task SLAs and I need them.
- Spark is triggered by a small stdlib HTTP service inside the Spark container. No Docker socket and no JVM in the Airflow image.
- Metrics go through the OpenTelemetry SDK to the collector, which Prometheus scrapes. One path for every metric.
- PostgreSQL holds the raw, staging, dw (star schema), gold, quality and governance schemas. Delta holds bronze, silver and gold.
- Grafana reads PostgreSQL gold. The MCP server reads Delta gold with the `deltalake` package, so it needs no JVM.
- One compose file holds every service, port and memory cap, so the 6.6 GB budget is checked in one place.

## Steps
1. Plan and contracts
2. Data model: star schema, dim_date, unknown members, SCD2, dictionary and mapping
3. Streaming ingest, built first to prove the contracts were complete
4. Lakehouse, orchestration and quality, governance, dashboards and FinOps, observability and the MCP server
5. Review: ruff, mypy, bandit and a read of every diff, looking for bugs and for code I did not need
6. Merge one part at a time and rerun the full suite after each
7. Integration: `docker compose up`, one command for the full flow, `docker stats` during the first demo
8. Recovery drills
9. Browser checks of the dashboards
10. Security audit, fixing every high and medium finding
11. Diagrams, docs and CI

## Repo tree
```
sql/          DDL, dim_date, unknown members, indexes, roles
ingest/       generator, producer, consumer, dlq replay
spark/        bronze, silver, gold jobs, trigger service, Databricks notes
dags/         etl and elt DAGs plus task code
quality/      checks and runner writing quality.dq_results
governance/   lineage, catalog, retention
dashboards/   Grafana JSON and provisioning YAML
finops/       cost report, idle cleanup, seed
mcp/          read-only MCP server
infra/        Terraform for floci, Prometheus and collector config, demo and drill scripts
docs/         contracts, diagrams, drills, walkthrough
tests/        one folder per part
```
