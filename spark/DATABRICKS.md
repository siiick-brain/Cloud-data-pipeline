# Moving the lakehouse to Databricks

How each local piece in `spark/` maps to Databricks.

| Local piece | Databricks equivalent |
|---|---|
| Compose project and the `delta` volume | A **workspace** per environment (dev, prod). Tables live in cloud storage (S3/ADLS) behind a Unity Catalog external location instead of `/data/delta`. |
| `spark/Dockerfile`, local mode, driver 1g | A **job cluster** (or serverless jobs compute) on a Databricks Runtime that ships Spark and Delta, so no jars to bake. The JDBC driver for PostgreSQL is included in DBR. Size the cluster per job instead of one 1g driver. |
| `DELTA_ROOT/bronze`, `silver`, `gold` paths | **Unity Catalog**: catalog `pipeline`, schemas `bronze`, `silver`, `gold`, managed tables `bronze.order_events`, `silver.order_lines`, `gold.daily_sales`, `gold.product_sales`. `table_path()` becomes a three-part table name. UC adds grants, lineage and audit on top. |
| `JDBC_URL`, `JDBC_USER`, `JDBC_PASSWORD` env vars | A **secret scope** (`dbutils.secrets.get`) or a UC **connection** with Lakehouse Federation, so `raw.order_events` can be read as a foreign table. |
| `jobs.bronze` (JDBC read, MERGE on event_id) | A DLT/Lakeflow **streaming table** fed by Auto Loader or a Kafka source, or kept as a MERGE notebook task. Lakeflow Connect can ingest PostgreSQL by CDC. |
| `jobs.silver` (parse, cast, schema enforced) | A **Delta Live Tables** table with `@dlt.expect_or_drop` expectations for the order_event rules. The pipeline enforces the declared schema. Use `APPLY CHANGES` keyed on `event_id` for the MERGE. |
| `jobs.gold` (daily_sales, product_sales overwrite) | DLT **materialized views** that DLT recomputes incrementally, so no full overwrite. |
| `trigger.py` (`POST /run/<job>`, one job at a time) | A Databricks **job** (Workflows) with tasks bronze, silver, gold chained by dependencies, `max_concurrent_runs: 1`. Airflow calls it with `DatabricksRunNowOperator` instead of HTTP to `spark:8000`. Health is the job run state. |
| `time_travel.py` | Works unchanged on UC tables: `VERSION AS OF`, `RESTORE TABLE`, and `DESCRIBE HISTORY`. Schema enforcement is the same Delta behavior. Set `delta.logRetentionDuration` and `VACUUM` retention to control how far back you can go. |
| `tests/spark` in the container | The same pytest suite against a local SparkSession in CI, plus **Databricks Asset Bundles** (`databricks bundle validate/deploy`) to ship the job and DLT pipeline per workspace. |
