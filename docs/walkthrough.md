# Walkthrough

This explains each part in plain words, in the order data moves through it. Each section ends with the question an interviewer is most likely to ask and a short answer.

## 1. The contracts

Before any code, I wrote down the interfaces in `docs/contracts`: the event JSON schema, every table and column type, the Delta paths, the metric names, the MCP tool shapes and the env vars. Every other part was built against these files. When a builder needed something that was not written down, it stopped and I fixed the contract. That happened once, for retention: no contract said which column measures a row's age.

> **Why freeze contracts first?** Six parts were built in parallel. Without a written interface, each one guesses and the guesses do not fit together at the end.

## 2. The generator

`ingest/generator.py` makes customers, products, stores and order line events from a seed. The same seed always gives the same event ids, so a test can ask "did these exact 500 events land?" Events are 30 seconds apart and end at the current time. The producer can add duplicates and broken fields on purpose.

> **Why seeded?** A drill or test that uses random ids can only check totals. With seeded ids it can check each event.

## 3. Ingest: Kafka, the consumer, Redis and the dead-letter topic

The producer writes events to the Kafka topic `orders.v1`. The consumer reads in batches of 500 and does four things in a fixed order:

1. Validates each event against the JSON schema. Bad ones go to `orders.dlq.v1` with the error and the original text.
2. Inserts the good ones into `raw.order_events` with `ON CONFLICT (event_id) DO NOTHING` and commits.
3. Marks each event id as seen in Redis, with a 7 day expiry.
4. Commits the Kafka offset.

If the consumer dies anywhere in that list, Kafka hands the batch out again and the primary key throws away whatever already landed. Redis catches most duplicates before they reach the database. The primary key catches the rest.

> **Exactly once?** No, it is at least once delivery with an idempotent write, which gives the same result. The kill drill proves it: 50,000 events, killed mid stream, 50,000 rows and no duplicates.

## 4. The star schema

`sql/01_schema.sql` builds a Kimball star. The fact table `dw.fact_order_line` has one row per order line. It points at four dimensions through integer surrogate keys: customer, product, store and date. Each dimension also keeps its natural key (`customer_id` and so on), and each has a row with key -1 meaning "unknown". If an event names a product the shop does not know, the fact row points at -1 instead of being dropped or holding a NULL.

`dim_customer` is SCD Type 2. When a customer's city or address changes, the old row gets an end date and a new row starts. A fact row links to the version that was valid when the sale happened. Name and email are Type 1, so they are overwritten in place. `tests/model/test_model.py` proves the address change case.

`dim_date` is generated once for 2020 to 2030 with keys like 20261125.

> **Why surrogate keys?** They make SCD2 possible (one customer, many versions) and keep joins on small integers. The -1 member keeps foreign keys as real constraints.

## 5. ETL: `etl_orders`

The Airflow DAG `etl_orders` runs daily:

1. `load_dimensions` runs `sql/load_dimensions.sql` (the SCD2 logic).
2. `extract` reads the day's raw events in batches of 5,000 through a server-side cursor. It transforms them in Python (dimension key lookups, the price calculation) and writes them to `staging.fact_order_line`.
3. `load_fact` copies staging into the fact table with `ON CONFLICT (event_id) DO NOTHING`.
4. `quality` runs the checks.

Running it twice for the same day adds nothing the second time. A test proves it by comparing row counts.

> **Why staging first?** The transform can fail halfway without touching the fact table, and the final insert is one statement that is either all in or all out.

## 6. ELT: `elt_gold`

`elt_gold` waits for `etl_orders` to pass quality, then rebuilds the gold tables for one load date entirely in SQL: delete that date's rows, insert the new aggregate, all in one transaction. Then it runs quality again and tells Spark to refresh the lakehouse.

> **ETL vs ELT here?** ETL transforms in Python before loading. ELT loads first and transforms inside the database with SQL. The ELT idempotency tool is "replace the partition", where ETL's is "skip what exists".

## 7. Retries, SLAs and the forced failure

Every task retries twice with exponential backoff and has a one hour SLA. Callbacks record the DAG duration as a metric. If the Airflow Variable `FORCE_FAIL` is 1, the first try of `extract` fails on purpose. The retry succeeds and deletes the variable. The demo turns this on every time, and `tests/integration/test_airflow_api.py` checks through the REST API that the task failed once, succeeded on try 2 and that the flag is gone.

## 8. Data quality

`quality/runner.py` checks nulls in key columns, unique event ids, foreign keys that resolve, raw count equals fact count for the day, freshness and the expected schema. Each result is written to `quality.dq_results` and exported as a Prometheus metric. Any failed check fails the Airflow task, and a test breaks a rule on purpose to prove it.

## 9. The lakehouse

Spark runs in its own container in local mode with a 1 GB driver. A tiny HTTP service (`spark/trigger.py`) waits for `POST /run/all` from Airflow and runs the jobs in one reused SparkSession:

- **bronze** reads raw events from PostgreSQL over JDBC and MERGEs on event_id
- **silver** parses and types each event with schema enforcement, also a MERGE
- **gold** rebuilds the daily and product aggregates

Delta keeps every version. `spark/time_travel.py` shows a wrong-schema write being rejected and an old version being read and restored. The recovery drill deletes all of silver and restores it. `spark/DATABRICKS.md` maps each piece to Databricks.

> **Why a trigger service instead of SparkSubmitOperator?** The Airflow image stays small with no Java, and no container gets the Docker socket.

## 10. Governance

`docs/data_dictionary.csv` lists every column with its owner, PII tag and retention period. `governance/catalog.py` loads it into `governance.catalog`. `governance/retention.py` deletes rows past retention only when run with `--apply`. `governance/lineage.py` writes the lineage graph from one hand-kept edge list, and a test fails if the committed graph is out of date.

## 11. Observability

The consumer and DAG tasks send metrics and traces with OpenTelemetry to the collector, which exposes them to Prometheus. There are four pipeline metrics: rows processed, consumer lag, quality score and DAG duration. Grafana has three dashboards (sales, pipeline health and quality), all loaded from JSON files at startup. A Playwright test opens each one and checks that it has data.

## 12. FinOps

floci emulates AWS on port 4566. `finops/seed.py` creates instances, volumes, buckets and a budget, some idle and some tagged `keep=true`. `finops/cost_report.py` reads Cost Explorer and Budgets and writes a CSV and a bar chart by service and tag. `finops/cleanup.py` lists idle resources and deletes them only with `--apply`. It never touches `keep=true` resources.

## 13. The MCP server

An AI agent can ask the pipeline three read-only questions: how the DAG runs went, what the quality scores are and what the gold numbers say. `gold_query` accepts only named queries with checked parameters, so there is no way to send it SQL. The server needs a bearer token, rejects unknown browser origins, binds to localhost and logs every call without secrets. The OWASP review is in `docs/security/mcp-review.md`.

## 14. Fitting in 8 GB

Every container has a memory cap and the total is 6.6 GB. After the first demo I read `docker stats`: the scheduler peaked at 84 percent, so it took memory from PostgreSQL and the webserver, which had plenty spare. After the 100k demo it hit 93 percent. The cause was the ETL extract holding a whole day of rows in memory, so I changed extract to stream in batches instead of raising the cap again.

## 15. Things that broke and what I learned

- Kafka tests failed now and then on Windows. `localhost` resolved to IPv6 first while Kafka listened on IPv4 only. Advertising `127.0.0.1` fixed it.
- The full Airflow image loaded dozens of unused providers and the webserver timed out at startup. The slim image with only the Postgres and OpenTelemetry extras starts in under 2 minutes.
- The first kill drill said events were lost. They were not: the drill stopped counting while the consumer was still restarting. The fix was in the drill, and the lesson is to check a scary number before believing it.
