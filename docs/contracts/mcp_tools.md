# MCP tool contract

Server: streamable HTTP on 127.0.0.1:8765, path `/mcp`. Bearer token from env `MCP_TOKEN`. No token in env means the server refuses to start.
Allowed Origin values from env `MCP_ALLOWED_ORIGINS` (comma separated). A request with an Origin header outside the list gets 403. A request with no Origin header is allowed (non-browser client).
Inside compose the container binds 0.0.0.0 but the host port is published only on 127.0.0.1.

## pipeline_status
Source: Airflow metadata database (`AIRFLOW_DB_URL`), table `dag_run`.
Input: `dag_id` (optional, one of etl_orders, elt_gold), `limit` int 1 to 50, default 10.
Output: list of `{dag_id: str, run_id: str, state: str, start_date: ISO 8601 or null, end_date: ISO 8601 or null}`, newest first.

## dq_scores
Source: `quality.dq_results` (`WAREHOUSE_RO_URL`).
Input: `table_name` (optional), `limit` int 1 to 100, default 20.
Output: list of `{check_name: str, table_name: str, passed: bool, score: float, checked_at: ISO 8601}`, newest first.

## gold_query
Source: Delta gold tables under `DELTA_ROOT`.
Input: `query` (one name from the table below) and `params` (object).

| query | params | output rows |
|---|---|---|
| daily_sales | date_from date, date_to date (ISO, range at most 366 days) | {sale_date, store_id, order_lines, units, revenue} ordered by sale_date, store_id |
| top_products | limit int 1 to 50 | {product_id, units, revenue} ordered by revenue desc |
| store_sales | store_id matching ^S\d{3}$ | {sale_date, store_id, order_lines, units, revenue} ordered by sale_date |

Any other query name or a bad param returns an error and runs nothing.
Output: `{query: str, row_count: int, rows: list}`. Decimals as floats, dates as ISO strings. At most 1000 rows.

## Audit log
JSON lines to stdout and to the file in `MCP_AUDIT_LOG`: `{ts, tool, params, caller_origin, status, row_count, duration_ms}`.
Never logs the token, the Authorization header or any connection string.
