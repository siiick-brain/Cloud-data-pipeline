# Data dictionary

Generated from `docs/data_dictionary.csv`, which is also loaded into `governance.catalog`.


## raw.order_events

| Column | Type | Null | PII | Owner | Retention days | Description |
|---|---|---|---|---|---|---|
| event_id | uuid | no | none | ingest | 90 | Event id from the producer. Primary key and dedupe key |
| payload | jsonb | no | none | ingest | 90 | Full event as received after schema validation |
| kafka_partition | integer | yes | none | ingest | 90 | Kafka partition the event came from |
| kafka_offset | bigint | yes | none | ingest | 90 | Kafka offset of the event |
| received_at | timestamptz | no | none | ingest | 90 | When the consumer inserted the row |
| load_date | date | no | none | ingest | 90 | UTC date of received_at. Partition key for loads |

## raw.customers

| Column | Type | Null | PII | Owner | Retention days | Description |
|---|---|---|---|---|---|---|
| customer_id | text | no | none | ingest | 365 | Natural key of the customer in the source system |
| full_name | text | yes | name | ingest | 365 | Customer full name |
| email | text | yes | email | ingest | 365 | Customer email address |
| city | text | yes | none | ingest | 365 | Customer city |
| address | text | yes | address | ingest | 365 | Customer street address |
| updated_at | timestamptz | no | none | ingest | 365 | Last change time in the source. Becomes valid_from for SCD2 |

## raw.products

| Column | Type | Null | PII | Owner | Retention days | Description |
|---|---|---|---|---|---|---|
| product_id | text | no | none | ingest | 365 | Natural key of the product |
| product_name | text | yes | none | ingest | 365 | Product name |
| category | text | yes | none | ingest | 365 | Product category |
| list_price | numeric(10,2) | yes | none | ingest | 365 | Current list price in USD |
| updated_at | timestamptz | no | none | ingest | 365 | Last change time in the source |

## raw.stores

| Column | Type | Null | PII | Owner | Retention days | Description |
|---|---|---|---|---|---|---|
| store_id | text | no | none | ingest | 365 | Natural key of the store |
| store_name | text | yes | none | ingest | 365 | Store name |
| city | text | yes | none | ingest | 365 | Store city |
| updated_at | timestamptz | no | none | ingest | 365 | Last change time in the source |

## staging.fact_order_line

| Column | Type | Null | PII | Owner | Retention days | Description |
|---|---|---|---|---|---|---|
| * | same as dw.fact_order_line | no | none | warehouse | 1 | Unlogged landing copy of fact rows. Truncated each ETL run |

## dw.dim_customer

| Column | Type | Null | PII | Owner | Retention days | Description |
|---|---|---|---|---|---|---|
| customer_key | integer | no | none | warehouse | 1825 | Surrogate key. -1 is the unknown member |
| customer_id | text | no | none | warehouse | 1825 | Natural key from raw.customers |
| full_name | text | yes | name | warehouse | 1825 | Customer full name (Type 1) |
| email | text | yes | email | warehouse | 1825 | Customer email (Type 1) |
| city | text | yes | none | warehouse | 1825 | Customer city (Type 2 tracked) |
| address | text | yes | address | warehouse | 1825 | Customer street address (Type 2 tracked) |
| valid_from | timestamptz | no | none | warehouse | 1825 | Start of the row version. First version starts at 1900-01-01 |
| valid_to | timestamptz | no | none | warehouse | 1825 | End of the row version. 9999-12-31 for the current row |
| is_current | boolean | no | none | warehouse | 1825 | True for the one current row per customer_id |

## dw.dim_product

| Column | Type | Null | PII | Owner | Retention days | Description |
|---|---|---|---|---|---|---|
| product_key | integer | no | none | warehouse | 1825 | Surrogate key. -1 is the unknown member |
| product_id | text | no | none | warehouse | 1825 | Natural key from raw.products |
| product_name | text | yes | none | warehouse | 1825 | Product name (Type 1) |
| category | text | yes | none | warehouse | 1825 | Product category (Type 1) |
| list_price | numeric(10,2) | yes | none | warehouse | 1825 | List price in USD (Type 1) |

## dw.dim_store

| Column | Type | Null | PII | Owner | Retention days | Description |
|---|---|---|---|---|---|---|
| store_key | integer | no | none | warehouse | 1825 | Surrogate key. -1 is the unknown member |
| store_id | text | no | none | warehouse | 1825 | Natural key from raw.stores |
| store_name | text | yes | none | warehouse | 1825 | Store name (Type 1) |
| city | text | yes | none | warehouse | 1825 | Store city (Type 1) |

## dw.dim_date

| Column | Type | Null | PII | Owner | Retention days | Description |
|---|---|---|---|---|---|---|
| date_key | integer | no | none | warehouse | 1825 | YYYYMMDD integer. -1 is the unknown member |
| full_date | date | yes | none | warehouse | 1825 | Calendar date. NULL only for the unknown member |
| year | smallint | yes | none | warehouse | 1825 | Calendar year |
| quarter | smallint | yes | none | warehouse | 1825 | Calendar quarter 1 to 4 |
| month | smallint | yes | none | warehouse | 1825 | Month number 1 to 12 |
| month_name | text | yes | none | warehouse | 1825 | Month name in English |
| day | smallint | yes | none | warehouse | 1825 | Day of month |
| day_of_week | smallint | yes | none | warehouse | 1825 | ISO day of week. 1 is Monday |
| day_name | text | yes | none | warehouse | 1825 | Day name in English |
| is_weekend | boolean | yes | none | warehouse | 1825 | True on Saturday and Sunday |

## dw.fact_order_line

| Column | Type | Null | PII | Owner | Retention days | Description |
|---|---|---|---|---|---|---|
| event_id | uuid | no | none | warehouse | 1825 | Grain: one row per order line event. Primary key |
| order_id | text | no | none | warehouse | 1825 | Order id (degenerate dimension) |
| line_number | smallint | no | none | warehouse | 1825 | Line number within the order |
| customer_key | integer | no | none | warehouse | 1825 | FK to the dim_customer version valid at occurred_at |
| product_key | integer | no | none | warehouse | 1825 | FK to dim_product |
| store_key | integer | no | none | warehouse | 1825 | FK to dim_store |
| date_key | integer | no | none | warehouse | 1825 | FK to dim_date for the UTC date of occurred_at |
| quantity | integer | no | none | warehouse | 1825 | Units sold. Always above zero |
| unit_price | numeric(10,2) | no | none | warehouse | 1825 | Price per unit at sale time |
| currency | char(3) | no | none | warehouse | 1825 | ISO 4217 currency code |
| net_amount | numeric(12,2) | no | none | warehouse | 1825 | quantity times unit_price |
| occurred_at | timestamptz | no | none | warehouse | 1825 | When the sale happened |
| load_date | date | no | none | warehouse | 1825 | Load date partition copied from raw.order_events |
| loaded_at | timestamptz | no | none | warehouse | 1825 | When the row was inserted |

## gold.daily_sales

| Column | Type | Null | PII | Owner | Retention days | Description |
|---|---|---|---|---|---|---|
| load_date | date | no | none | analytics | 730 | Load date partition that produced the row |
| sale_date | date | no | none | analytics | 730 | UTC sale date |
| store_id | text | no | none | analytics | 730 | Store natural key |
| order_lines | integer | no | none | analytics | 730 | Count of order lines |
| units | integer | no | none | analytics | 730 | Sum of quantity |
| revenue | numeric(14,2) | no | none | analytics | 730 | Sum of net_amount |

## gold.product_sales

| Column | Type | Null | PII | Owner | Retention days | Description |
|---|---|---|---|---|---|---|
| load_date | date | no | none | analytics | 730 | Load date partition that produced the row |
| sale_date | date | no | none | analytics | 730 | UTC sale date |
| product_id | text | no | none | analytics | 730 | Product natural key |
| category | text | yes | none | analytics | 730 | Product category |
| units | integer | no | none | analytics | 730 | Sum of quantity |
| revenue | numeric(14,2) | no | none | analytics | 730 | Sum of net_amount |

## quality.dq_results

| Column | Type | Null | PII | Owner | Retention days | Description |
|---|---|---|---|---|---|---|
| id | bigint | no | none | quality | 180 | Row id |
| run_id | text | no | none | quality | 180 | Airflow run id or a manual run label |
| dag_id | text | yes | none | quality | 180 | DAG that ran the check |
| check_name | text | no | none | quality | 180 | Check name such as not_null or freshness |
| table_name | text | no | none | quality | 180 | Table the check ran on |
| passed | boolean | no | none | quality | 180 | True when the check met its threshold |
| observed | numeric | yes | none | quality | 180 | Measured value |
| threshold | numeric | yes | none | quality | 180 | Value the check compares against |
| score | numeric(5,4) | no | none | quality | 180 | Score from 0 to 1 |
| details | text | yes | none | quality | 180 | Short explanation for a failure |
| checked_at | timestamptz | no | none | quality | 180 | When the check ran |

## governance.catalog

| Column | Type | Null | PII | Owner | Retention days | Description |
|---|---|---|---|---|---|---|
| table_schema | text | no | none | governance |  | Schema of the documented column |
| table_name | text | no | none | governance |  | Table of the documented column |
| column_name | text | no | none | governance |  | Column name |
| data_type | text | yes | none | governance |  | Declared type |
| owner | text | yes | none | governance |  | Team that owns the column |
| pii_tag | text | no | none | governance |  | none email name or address |
| retention_days | integer | yes | none | governance |  | How long rows are kept |
| description | text | yes | none | governance |  | Plain description |
