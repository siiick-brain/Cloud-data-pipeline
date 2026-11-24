# Table contract (PostgreSQL database `warehouse`)

Source of truth for types is `sql/01_schema.sql`. Names and types here match it.

## raw
| Table | Columns |
|---|---|
| raw.order_events | event_id uuid PK, payload jsonb NOT NULL, kafka_partition int, kafka_offset bigint, received_at timestamptz DEFAULT now(), load_date date NOT NULL (UTC date of received_at) |
| raw.customers | customer_id text PK, full_name text, email text, city text, address text, updated_at timestamptz |
| raw.products | product_id text PK, product_name text, category text, list_price numeric(10,2), updated_at timestamptz |
| raw.stores | store_id text PK, store_name text, city text, updated_at timestamptz |

## staging
| Table | Columns |
|---|---|
| staging.fact_order_line | same columns as dw.fact_order_line without loaded_at. UNLOGGED, truncated at the start of each ETL load |

## dw (Kimball star)
Surrogate keys are `*_key` integers. Natural keys are `*_id`. Every dimension has key -1 for unknown.

| Table | Columns |
|---|---|
| dw.dim_customer (SCD2) | customer_key int PK identity, customer_id text NOT NULL, full_name text, email text, city text, address text, valid_from timestamptz NOT NULL, valid_to timestamptz NOT NULL DEFAULT '9999-12-31', is_current boolean NOT NULL. One current row per customer_id. Tracked columns: city, address. Other columns overwrite in place |
| dw.dim_product (SCD1) | product_key int PK identity, product_id text UNIQUE NOT NULL, product_name text, category text, list_price numeric(10,2) |
| dw.dim_store (SCD1) | store_key int PK identity, store_id text UNIQUE NOT NULL, store_name text, city text |
| dw.dim_date | date_key int PK (YYYYMMDD), full_date date UNIQUE, year smallint, quarter smallint, month smallint, month_name text, day smallint, day_of_week smallint (ISO, 1 is Monday), day_name text, is_weekend boolean. Rows for 2020-01-01 to 2030-12-31 plus key -1 with NULL full_date |
| dw.fact_order_line | event_id uuid PK, order_id text NOT NULL, line_number smallint NOT NULL, customer_key int NOT NULL FK, product_key int NOT NULL FK, store_key int NOT NULL FK, date_key int NOT NULL FK, quantity int NOT NULL CHECK > 0, unit_price numeric(10,2) NOT NULL, currency char(3) NOT NULL, net_amount numeric(12,2) NOT NULL (quantity * unit_price), occurred_at timestamptz NOT NULL, load_date date NOT NULL, loaded_at timestamptz DEFAULT now() |

Fact customer_key is the dim_customer row valid at occurred_at. Unknown natural keys map to -1.
The unknown members (-1) use the literal value `Unknown` for every text column.

## gold
| Table | Columns |
|---|---|
| gold.daily_sales | load_date date, sale_date date, store_id text, order_lines int, units int, revenue numeric(14,2). PK (load_date, sale_date, store_id) |
| gold.product_sales | load_date date, sale_date date, product_id text, category text, units int, revenue numeric(14,2). PK (load_date, sale_date, product_id) |

## quality
| Table | Columns |
|---|---|
| quality.dq_results | id bigint PK identity, run_id text NOT NULL, dag_id text, check_name text NOT NULL, table_name text NOT NULL, passed boolean NOT NULL, observed numeric, threshold numeric, score numeric(5,4) NOT NULL between 0 and 1, details text, checked_at timestamptz NOT NULL DEFAULT now() |

## governance
| Table | Columns |
|---|---|
| governance.catalog | table_schema text, table_name text, column_name text, data_type text, owner text, pii_tag text (one of none, email, name, address), retention_days int, description text. PK (table_schema, table_name, column_name) |

## Roles
| Role | Rights |
|---|---|
| pipeline | read and write on raw, staging, dw, gold, quality, governance |
| mcp_ro | SELECT on quality.dq_results, CONNECT on airflow db and SELECT on its dag_run table |
| grafana_ro | SELECT on dw, gold and quality |
