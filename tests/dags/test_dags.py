"""The etl_orders and elt_gold DAGs are idempotent, the force fail flag works once, metrics are emitted."""

from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace

import dag_common
import elt_gold
import etl_orders
import pendulum
import pytest
from airflow.exceptions import AirflowException
from airflow.models import Variable

# dag.test deletes the previous run of the same date, which needs the ab_user table mapped
from airflow.providers.fab.auth_manager.models import User  # noqa: F401
from airflow.utils.state import DagRunState

from tests.dags.conftest import DAY
from tests.quality.conftest import points

pytestmark = pytest.mark.integration

LOGICAL = pendulum.datetime(2030, 6, 15, tz="UTC")


def counts(cur):
    out = {}
    for table in ("raw.order_events", "dw.fact_order_line", "gold.daily_sales", "gold.product_sales"):
        cur.execute(f"SELECT count(*) FROM {table} WHERE load_date = %s", (DAY,))
        out[table] = cur.fetchone()[0]
    return out


def run(dag):
    for task in dag.tasks:
        task.retry_delay = timedelta(0)
    dag_run = dag.test(execution_date=LOGICAL)
    assert dag_run.state == DagRunState.SUCCESS, dag_run
    return dag_run


def test_dags_load_with_contract_settings():
    for dag in (etl_orders.dag, elt_gold.dag):
        assert dag.default_args["retries"] >= 1 and dag.default_args["retry_exponential_backoff"]
        assert dag.default_args["sla"] and dag.on_failure_callback and dag.on_success_callback
    assert etl_orders.dag.dag_id == "etl_orders" and "extract" in etl_orders.dag.task_ids
    assert elt_gold.dag.dag_id == "elt_gold"


def test_etl_twice_leaves_row_counts_unchanged(seeded, memory_otel):
    run(etl_orders.dag)
    first = counts(seeded)
    assert first["dw.fact_order_line"] == first["raw.order_events"] == 4
    run(etl_orders.dag)
    assert counts(seeded) == first
    seeded.execute("SELECT product_key = -1, date_key, customer_key = -1, net_amount FROM dw.fact_order_line "
                   "WHERE load_date = %s ORDER BY line_number", (DAY,))
    rows = seeded.fetchall()
    assert [r[0] for r in rows] == [False, False, True, False]  # unknown product maps to -1
    assert rows[3][1] == -1 and rows[0][1] == 20300615  # 2019 is outside dim_date
    assert not any(r[2] for r in rows)
    assert rows[2][3] == Decimal("12.75")
    seeded.execute("SELECT count(*), bool_and(passed) FROM quality.dq_results WHERE dag_id = 'etl_orders' "
                   "AND details LIKE %s", (f"load_date={DAY} %",))
    assert seeded.fetchone() == (14, True)  # 7 checks, two runs
    rows_metric = points(memory_otel[0], "pipeline.rows_processed")
    assert rows_metric[frozenset({"stage": "etl", "outcome": "ok"}.items())] == 4
    assert rows_metric[frozenset({"stage": "etl", "outcome": "duplicate"}.items())] == 4
    span_names = {s.name for s in memory_otel[1].get_finished_spans()}
    assert {"task.load_dimensions", "task.extract", "task.load_fact", "task.quality"} <= span_names


def test_elt_twice_leaves_row_counts_unchanged_and_triggers_spark(seeded, spark_stub, memory_otel):
    run(etl_orders.dag)
    run(elt_gold.dag)
    first = counts(seeded)
    run(elt_gold.dag)
    assert counts(seeded) == first
    assert (first["gold.daily_sales"], first["gold.product_sales"]) == (2, 3)
    seeded.execute("SELECT sale_date::text, order_lines, units, revenue FROM gold.daily_sales "
                   "WHERE load_date = %s ORDER BY sale_date", (DAY,))
    assert seeded.fetchall() == [("2019-12-31", 1, 1, Decimal("10.50")),
                                 ("2030-06-15", 3, 6, Decimal("44.25"))]
    assert spark_stub[0] == ["/run/all", "/run/all"]
    rows_metric = points(memory_otel[0], "pipeline.rows_processed")
    assert rows_metric[frozenset({"stage": "elt", "outcome": "ok"}.items())] == 10


def test_forced_failure_then_retry_succeeds_and_clears_flag(seeded, monkeypatch):
    Variable.set("FORCE_FAIL", "1")
    monkeypatch.setenv("AIRFLOW_VAR_FORCE_FAIL", "1")
    dag_run = run(etl_orders.dag)
    extract = dag_run.get_task_instance("extract")
    assert extract.try_number == 2  # first try failed on purpose, the retry passed
    assert Variable.get("FORCE_FAIL", default_var=None) is None
    assert "AIRFLOW_VAR_FORCE_FAIL" not in os.environ
    assert counts(seeded)["dw.fact_order_line"] == 4


def test_broken_rule_fails_quality_task(seeded):
    context = {"ti": SimpleNamespace(task_id="quality"), "ds": DAY, "run_id": "test-broken",
               "dag": SimpleNamespace(dag_id="etl_orders")}
    with pytest.raises(AirflowException, match="row_count_reconciliation"):  # raw loaded, fact empty
        dag_common.run_quality(**context)


def test_spark_failure_fails_trigger(spark_stub):
    spark_stub[1]["code"] = 500
    with pytest.raises(Exception, match="500"):
        elt_gold.trigger_spark(ti=SimpleNamespace(task_id="trigger_spark"))


def test_transform_follows_source_to_target():
    lookups = etl_orders.Lookups(
        customers={"C1": [(datetime(1900, 1, 1, tzinfo=UTC), datetime(2026, 3, 1, tzinfo=UTC), 7),
                          (datetime(2026, 3, 1, tzinfo=UTC), datetime(9999, 12, 31, tzinfo=UTC), 8)]},
        products={"P1": 3}, stores={"S1": 4}, dates={20260301},
    )
    payload = {"event_id": "6f1c1f0e-1d1a-4c1e-9a1b-1c1d1e1f1a1b", "occurred_at": "2026-03-01T00:00:00Z",
               "order_id": "O00000001", "line_number": 2, "customer_id": "C1", "product_id": "P9",
               "store_id": "S1", "quantity": 3, "unit_price": 19.99, "currency": "USD"}
    row = etl_orders.transform(payload, DAY, lookups)
    assert row[:7] == ("6f1c1f0e-1d1a-4c1e-9a1b-1c1d1e1f1a1b", "O00000001", 2, 8, -1, 4, 20260301)
    assert row[7:11] == (3, Decimal("19.99"), "USD", Decimal("59.97"))
    unknown = etl_orders.transform({**payload, "customer_id": "C2", "occurred_at": "2031-01-01T00:00:00Z"},
                                   DAY, lookups)
    assert (unknown[3], unknown[6]) == (-1, -1)


def test_callbacks_emit_duration_and_log(memory_otel, caplog):
    start = pendulum.datetime(2026, 1, 1, tz="UTC")
    dag_run = SimpleNamespace(dag_id="etl_orders", start_date=start, end_date=start.add(seconds=42))
    etl_orders.dag.on_failure_callback({"dag_run": dag_run})
    gauge = points(memory_otel[0], "pipeline.dag_duration_seconds")
    assert gauge[frozenset({"dag_id": "etl_orders", "state": "failed"}.items())] == 42
    ti = SimpleNamespace(dag_id="etl_orders", task_id="extract", try_number=1)
    dag_common.log_task_failure({"ti": ti, "exception": ValueError("boom")})
    dag_common.log_sla_miss(etl_orders.dag, "extract", "", [], [])
    assert "boom" in caplog.text and "SLA missed" in caplog.text
