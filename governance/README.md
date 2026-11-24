# Governance

Catalog, lineage and retention for the warehouse. `docs/data_dictionary.csv` is the single source for
owners, PII tags and retention days. Code reads that file; nothing here repeats its values.

## Owners
Each column has an owning team in the CSV `owner` column:

| Owner | Tables |
|---|---|
| ingest | raw.* |
| warehouse | staging.*, dw.* |
| analytics | gold.* |
| quality | quality.dq_results |
| governance | governance.catalog |

Load the CSV into `governance.catalog` (upsert, safe to rerun):

```
python -m governance.catalog
```

It reads `WAREHOUSE_URL`. Query owners with
`SELECT DISTINCT table_schema, table_name, owner FROM governance.catalog`.

## PII handling
`pii_tag` is one of `none`, `email`, `name` or `address` (a CHECK constraint enforces it). Tagged columns
today are `full_name`, `email` and `address` in `raw.customers` and `dw.dim_customer`. Gold tables and Delta
tables carry no PII. Grafana (`grafana_ro`) can read dw, so dashboards must not select tagged columns.
Find them with `SELECT * FROM governance.catalog WHERE pii_tag <> 'none'`.

## Retention
`retention_days` comes from `governance.catalog`. Each table's age column is fixed by the Retention section of
`docs/contracts/tables.md` and copied into `AGE_COLUMNS` in `governance/retention.py`. A row is deleted when
its age column is older than `now() - retention_days`:

| Table | Age column | Days |
|---|---|---|
| raw.order_events | received_at | 90 |
| dw.fact_order_line | occurred_at | 1825 |
| gold.daily_sales, gold.product_sales | load_date | 730 |
| quality.dq_results | checked_at | 180 |

All other tables are exempt: master data, dimensions, staging, dim_date and the catalog.

```
python -m governance.retention          # dry run: prints what would be deleted
python -m governance.retention --apply  # deletes and commits
```

Load the catalog first. If it is empty, nothing gets deleted.

## Lineage
`governance/lineage.py` holds one hand-written edge list `(source, target, job)`. Everything else is generated:

```
python -m governance.lineage > lineage.json
```

This prints JSON with OpenLineage dataset names (Kafka topic `orders.v1` in `kafka://kafka:9092`,
`warehouse.<schema>.<table>` in `postgres://postgres:5432`, Delta paths under `/data/delta` in `file`) and
rewrites `governance/lineage.mmd`. A test fails if the committed `.mmd` is stale.

## How to add a table
1. Add the DDL in `sql/` and the columns in `docs/contracts/tables.md` (or `delta_tables.md`).
2. Add one CSV row per column to `docs/data_dictionary.csv` with owner, pii_tag and retention_days.
3. Add its edges to `EDGES` in `governance/lineage.py` and run `python -m governance.lineage`.
4. If rows should expire, add its age column to the Retention section of `tables.md` and to `AGE_COLUMNS`.
5. Run `python -m governance.catalog` and the tests in `tests/governance`.
