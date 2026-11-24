# Code map

Where things live. `rg -n <symbol>` finds the rest.

| Path | What it holds | Key symbols |
|---|---|---|
| docs/contracts/ | frozen interfaces | order_event.schema.json, tables.md, delta_tables.md, metrics.md, mcp_tools.md, finops_mock.json, env.md |
| sql/ | PostgreSQL model | 00_roles.sh, 01_schema.sql, 02_dim_date.sql, 03_unknown_members.sql, 04_grants.sql, load_dimensions.sql |
| ingest/generator.py | synthetic data | customers, products, stores, events |
| ingest/ | streaming ingest | producer, consumer, replay_dlq, seed |
| spark/ | lakehouse | jobs, trigger, time_travel |
| dags/ | orchestration | etl_orders, elt_gold, dag_common |
| quality/ | data quality | runner, telemetry |
| governance/ | governance | catalog, lineage, retention |
| dashboards/ | Grafana | JSON dashboards, provisioning |
| finops/ | FinOps | cost_report, cleanup, seed |
| mcp/ | MCP server | pipeline_mcp.server |
| infra/ | infrastructure | terraform, prometheus.yml, otel-collector.yaml, demo.sh, drills.sh |
| docker-compose.yml | runtime | every service and mem_limit |
| tests/<part>/ | tests | unit tests run on the host, integration tests marked `integration` |
