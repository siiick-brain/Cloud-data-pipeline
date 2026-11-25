"""ETL: raw.order_events of one load date (the run's ds) into dw.fact_order_line.

load_dimensions -> extract -> load_fact -> quality
extract reads raw events of the load date, transforms them in Python (docs/source_to_target.md)
and lands them in the truncated staging.fact_order_line. load_fact inserts with ON CONFLICT DO NOTHING,
so a rerun adds nothing.
"""

from __future__ import annotations

import os
from collections import defaultdict
from contextlib import closing
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, NamedTuple

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from dag_common import DAG_KWARGS, connect, count_rows, run_quality, traced
from psycopg2.extras import execute_values

LOAD_DIMENSIONS_SQL = Path(__file__).resolve().parents[1] / "sql" / "load_dimensions.sql"
CENT = Decimal("0.01")
BATCH = 5000  # raw rows per fetch in extract
RAW_SQL = "SELECT payload, load_date FROM raw.order_events WHERE load_date = %s"
COLUMNS = ("event_id, order_id, line_number, customer_key, product_key, store_key, date_key, quantity, "
           "unit_price, currency, net_amount, occurred_at, load_date")
# COLUMNS is a constant, never input
STAGE_SQL = f"INSERT INTO staging.fact_order_line ({COLUMNS}) VALUES %s"  # noqa: S608  # nosec B608


class Lookups(NamedTuple):
    customers: dict[str, list[tuple[datetime, datetime, int]]]  # customer_id -> (valid_from, valid_to, key)
    products: dict[str, int]
    stores: dict[str, int]
    dates: set[int]


def read_lookups(cur: Any) -> Lookups:
    cur.execute("SELECT customer_id, valid_from, valid_to, customer_key FROM dw.dim_customer")
    customers: dict[str, list] = defaultdict(list)
    for customer_id, valid_from, valid_to, key in cur.fetchall():
        customers[customer_id].append((valid_from, valid_to, key))
    cur.execute("SELECT product_id, product_key FROM dw.dim_product")
    products = dict(cur.fetchall())
    cur.execute("SELECT store_id, store_key FROM dw.dim_store")
    stores = dict(cur.fetchall())
    cur.execute("SELECT date_key FROM dw.dim_date WHERE date_key <> -1")
    return Lookups(customers, products, stores, {row[0] for row in cur.fetchall()})


def transform(payload: dict, load_date: Any, lookups: Lookups) -> tuple:
    """One raw payload to one staging row. Unknown natural keys and dates outside dim_date map to -1."""
    occurred_at = datetime.fromisoformat(payload["occurred_at"]).astimezone(UTC)
    versions = lookups.customers.get(payload["customer_id"], ())
    customer_key = next((key for start, end, key in versions if start <= occurred_at < end), -1)
    date_key = int(occurred_at.strftime("%Y%m%d"))
    quantity = int(payload["quantity"])
    unit_price = Decimal(str(payload["unit_price"])).quantize(CENT)
    return (
        payload["event_id"], payload["order_id"], int(payload["line_number"]), customer_key,
        lookups.products.get(payload["product_id"], -1), lookups.stores.get(payload["store_id"], -1),
        date_key if date_key in lookups.dates else -1,
        quantity, unit_price, payload["currency"], (quantity * unit_price).quantize(CENT),
        occurred_at, load_date,
    )


@traced
def load_dimensions(**context: Any) -> None:
    with closing(connect()) as conn, conn, conn.cursor() as cur:
        cur.execute(LOAD_DIMENSIONS_SQL.read_text())


@traced
def extract(**context: Any) -> int:
    # Force fail flag (docs/contracts/env.md): first try fails, the successful retry clears the flag
    force_fail = Variable.get("FORCE_FAIL", default_var="") == "1"
    if force_fail and context["ti"].try_number <= 1:
        raise AirflowException("FORCE_FAIL is 1: the first try of extract fails on purpose")
    staged = 0
    with closing(connect()) as conn, conn, conn.cursor() as cur:
        lookups = read_lookups(cur)
        cur.execute("TRUNCATE staging.fact_order_line")
        # server-side cursor streams the day in batches, so a 100k day never sits in task memory
        with conn.cursor(name="raw_events") as src:
            src.execute(RAW_SQL, (context["ds"],))
            while batch := src.fetchmany(BATCH):
                rows = [transform(payload, load_date, lookups) for payload, load_date in batch]
                execute_values(cur, STAGE_SQL, rows, page_size=1000)
                staged += len(rows)
    if force_fail:
        Variable.delete("FORCE_FAIL")
        os.environ.pop("AIRFLOW_VAR_FORCE_FAIL", None)
    return staged


@traced
def load_fact(**context: Any) -> int:
    with closing(connect()) as conn, conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM staging.fact_order_line")
        staged = cur.fetchone()[0]
        cur.execute(f"INSERT INTO dw.fact_order_line ({COLUMNS}) "  # noqa: S608  # nosec B608
                    f"SELECT {COLUMNS} FROM staging.fact_order_line ON CONFLICT (event_id) DO NOTHING")
        inserted = cur.rowcount
    count_rows("etl", "ok", inserted)
    count_rows("etl", "duplicate", staged - inserted)
    return inserted


with DAG("etl_orders", description="raw order events to the star schema", tags=["etl"], **DAG_KWARGS) as dag:
    (
        PythonOperator(task_id="load_dimensions", python_callable=load_dimensions)
        >> PythonOperator(task_id="extract", python_callable=extract)
        >> PythonOperator(task_id="load_fact", python_callable=load_fact)
        >> PythonOperator(task_id="quality", python_callable=run_quality)
    )
