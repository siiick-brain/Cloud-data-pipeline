# Source to target mapping

| Target | Source | Rule |
|---|---|---|
| raw.order_events.event_id | event.event_id | cast to uuid |
| raw.order_events.payload | whole event | stored as jsonb after JSON schema validation |
| raw.order_events.load_date | consumer clock | UTC date at insert |
| raw.customers, raw.products, raw.stores | `ingest/generator.py` master data | `ingest/seed.py`, insert if missing |
| dw.dim_customer | raw.customers | SCD2 on city and address, Type 1 on full_name and email (`sql/load_dimensions.sql`) |
| dw.dim_product | raw.products | Type 1 upsert on product_id |
| dw.dim_store | raw.stores | Type 1 upsert on store_id |
| dw.dim_date | generated | `sql/02_dim_date.sql`, 2020-01-01 to 2030-12-31 |
| dw.fact_order_line.event_id | payload->>'event_id' | cast to uuid |
| dw.fact_order_line.order_id | payload->>'order_id' | as is |
| dw.fact_order_line.line_number | payload->>'line_number' | cast to smallint |
| dw.fact_order_line.customer_key | payload->>'customer_id' | dim_customer row where customer_id matches and occurred_at is in [valid_from, valid_to). No match gives -1 |
| dw.fact_order_line.product_key | payload->>'product_id' | dim_product lookup. No match gives -1 |
| dw.fact_order_line.store_key | payload->>'store_id' | dim_store lookup. No match gives -1 |
| dw.fact_order_line.date_key | payload->>'occurred_at' | to_char(UTC date, 'YYYYMMDD')::int. Outside dim_date range gives -1 |
| dw.fact_order_line.quantity | payload->>'quantity' | cast to int |
| dw.fact_order_line.unit_price | payload->>'unit_price' | cast to numeric(10,2) |
| dw.fact_order_line.currency | payload->>'currency' | as is |
| dw.fact_order_line.net_amount | quantity, unit_price | quantity * unit_price |
| dw.fact_order_line.occurred_at | payload->>'occurred_at' | cast to timestamptz |
| dw.fact_order_line.load_date | raw.order_events.load_date | as is |
| gold.daily_sales | dw.fact_order_line, dim_store | group by load_date, UTC date of occurred_at, store_id. order_lines = count, units = sum(quantity), revenue = sum(net_amount) |
| gold.product_sales | dw.fact_order_line, dim_product | group by load_date, UTC date of occurred_at, product_id, category |
| Delta bronze.order_events | raw.order_events over JDBC | MERGE on event_id, insert only |
| Delta silver.order_lines | bronze.order_events | parse payload, cast types, drop rows that fail casts, MERGE on event_id |
| Delta gold.daily_sales, gold.product_sales | silver.order_lines | same aggregation as the PostgreSQL gold tables, without load_date |
