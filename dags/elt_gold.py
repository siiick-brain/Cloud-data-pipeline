"""ELT: rebuild one load date (the run's ds) of the gold marts in SQL, check them, then run the Spark jobs.

wait_for_etl -> build_gold -> quality -> trigger_spark
build_gold deletes and reinserts the load date of gold.daily_sales and gold.product_sales in one transaction.
"""

from __future__ import annotations

import json
import os
import urllib.request
from contextlib import closing
from typing import Any

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from dag_common import DAG_KWARGS, connect, count_rows, run_quality, traced

DAILY_SQL = """
INSERT INTO gold.daily_sales (load_date, sale_date, store_id, order_lines, units, revenue)
SELECT f.load_date, (f.occurred_at AT TIME ZONE 'UTC')::date, s.store_id, count(*), sum(f.quantity),
       sum(f.net_amount)
FROM dw.fact_order_line f JOIN dw.dim_store s ON s.store_key = f.store_key
WHERE f.load_date = %(load_date)s
GROUP BY 1, 2, 3
"""
PRODUCT_SQL = """
INSERT INTO gold.product_sales (load_date, sale_date, product_id, category, units, revenue)
SELECT f.load_date, (f.occurred_at AT TIME ZONE 'UTC')::date, p.product_id, p.category, sum(f.quantity),
       sum(f.net_amount)
FROM dw.fact_order_line f JOIN dw.dim_product p ON p.product_key = f.product_key
WHERE f.load_date = %(load_date)s
GROUP BY 1, 2, 3, 4
"""


@traced
def build_gold(**context: Any) -> int:
    params = {"load_date": context["ds"]}
    with closing(connect()) as conn, conn, conn.cursor() as cur:  # one transaction for both marts
        cur.execute("DELETE FROM gold.daily_sales WHERE load_date = %(load_date)s", params)
        cur.execute(DAILY_SQL, params)
        inserted = cur.rowcount
        cur.execute("DELETE FROM gold.product_sales WHERE load_date = %(load_date)s", params)
        cur.execute(PRODUCT_SQL, params)
        inserted += cur.rowcount
    count_rows("elt", "ok", inserted)
    return inserted


@traced
def trigger_spark(**context: Any) -> dict:
    """POST SPARK_TRIGGER_URL/run/all. HTTP 500 or status failed fails the task, which then retries."""
    url = os.environ["SPARK_TRIGGER_URL"].rstrip("/") + "/run/all"
    if not url.startswith(("http://", "https://")):
        raise AirflowException(f"SPARK_TRIGGER_URL must be http(s): {url}")
    request = urllib.request.Request(url, method="POST")  # noqa: S310  # nosec B310 (scheme checked above)
    with urllib.request.urlopen(request, timeout=1800) as response:  # noqa: S310  # nosec B310
        result = json.load(response)
    if result.get("status") != "ok":
        raise AirflowException(f"spark job failed: {result}")
    return result


with DAG("elt_gold", description="fact to gold marts, then Spark", tags=["elt"], **DAG_KWARGS) as dag:
    (
        ExternalTaskSensor(task_id="wait_for_etl", external_dag_id="etl_orders", external_task_id="quality",
                           mode="reschedule", poke_interval=60, timeout=6 * 3600)
        >> PythonOperator(task_id="build_gold", python_callable=build_gold)
        >> PythonOperator(task_id="quality", python_callable=run_quality)
        >> PythonOperator(task_id="trigger_spark", python_callable=trigger_spark)
    )
