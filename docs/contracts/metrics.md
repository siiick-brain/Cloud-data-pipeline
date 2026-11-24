# Metrics and tracing contract

All metrics and traces go through OTLP HTTP to the collector (`OTEL_EXPORTER_OTLP_ENDPOINT`, default `http://otel-collector:4318`).
The collector exposes metrics to Prometheus on `otel-collector:8889`. Prometheus job name: `otel-collector`.
Names below are the Prometheus names after export.

| Prometheus metric | OTel instrument | Type | Labels | Emitted by |
|---|---|---|---|---|
| pipeline_rows_processed_total | pipeline.rows_processed | counter | stage (consumer, etl, elt, spark), outcome (ok, duplicate, dead_letter) | ingest consumer, DAG tasks |
| pipeline_consumer_lag | pipeline.consumer_lag | gauge | topic, partition | ingest consumer |
| pipeline_quality_score | pipeline.quality_score | gauge | check_name, table_name | quality runner |
| pipeline_dag_duration_seconds | pipeline.dag_duration_seconds | gauge | dag_id, state | DAG success and failure callbacks |

The collector `prometheus` exporter must set `add_metric_suffixes: true` and translate dots to underscores, so `pipeline.rows_processed` becomes `pipeline_rows_processed_total`.
Metric export interval: 5 seconds.

Service names (`OTEL_SERVICE_NAME`): `ingest-consumer`, `ingest-producer`, `airflow`, `spark-jobs`.
Spans: one span per consumer batch (`consume_batch`) and one span per DAG task (`task.<task_id>`).
