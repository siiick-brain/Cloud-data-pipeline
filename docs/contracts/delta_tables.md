# Delta contract

Root path inside the Spark and MCP containers: `/data/delta` (named volume `delta`). Env var `DELTA_ROOT`.

| Table | Path | Columns | Write mode |
|---|---|---|---|
| bronze.order_events | $DELTA_ROOT/bronze/order_events | event_id string, payload string (raw JSON), load_date date, ingested_at timestamp | MERGE on event_id, insert when not matched |
| silver.order_lines | $DELTA_ROOT/silver/order_lines | event_id string, order_id string, line_number int, customer_id string, product_id string, store_id string, quantity int, unit_price decimal(10,2), currency string, net_amount decimal(12,2), occurred_at timestamp, load_date date | MERGE on event_id, schema enforced (no mergeSchema) |
| gold.daily_sales | $DELTA_ROOT/gold/daily_sales | sale_date date, store_id string, order_lines long, units long, revenue decimal(14,2) | overwrite |
| gold.product_sales | $DELTA_ROOT/gold/product_sales | sale_date date, product_id string, units long, revenue decimal(14,2) | overwrite |

Bronze reads `raw.order_events` from PostgreSQL over JDBC.

Trigger service: `POST http://spark:8000/run/<job>` where job is one of `bronze`, `silver`, `gold`, `all`.
Response `{"job": str, "status": "ok" or "failed", "duration_s": float}`. HTTP 200 on ok, 500 on failed, 404 on unknown job.
The service runs one job at a time and is reachable only on the compose network.
