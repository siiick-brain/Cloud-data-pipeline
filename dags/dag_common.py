"""Shared by etl_orders and elt_gold: default args, callbacks, task spans, row counts and the quality task."""

from __future__ import annotations

import functools
import logging
import os
from collections.abc import Callable
from datetime import timedelta
from typing import Any

import pendulum
import psycopg2
from airflow.exceptions import AirflowException

from quality import runner, telemetry

log = logging.getLogger(__name__)


def log_task_failure(context: dict) -> None:
    ti = context["ti"]
    log.error("task %s.%s failed on try %s: %s",
              ti.dag_id, ti.task_id, ti.try_number, context.get("exception"))


def log_sla_miss(dag: Any, task_list: str, blocking_task_list: str, slas: list, blocking_tis: list) -> None:
    log.warning("SLA missed in %s: %s (blocked by %s)", dag.dag_id, task_list, blocking_task_list)


def record_duration(context: dict, state: str) -> None:
    """DAG success and failure callback: pipeline.dag_duration_seconds{dag_id, state}."""
    dag_run = context["dag_run"]
    seconds = ((dag_run.end_date or pendulum.now("UTC")) - dag_run.start_date).total_seconds()
    telemetry.instrument("gauge", "pipeline.dag_duration_seconds").set(
        seconds, {"dag_id": dag_run.dag_id, "state": state})
    telemetry.flush()


DAG_KWARGS: dict[str, Any] = {
    "schedule": "@daily",
    "start_date": pendulum.datetime(2026, 1, 1, tz="UTC"),
    "catchup": False,
    "max_active_runs": 1,
    "on_success_callback": functools.partial(record_duration, state="success"),
    "on_failure_callback": functools.partial(record_duration, state="failed"),
    "sla_miss_callback": log_sla_miss,
    "default_args": {
        "owner": "data",
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=10),
        "sla": timedelta(hours=1),
        "on_failure_callback": log_task_failure,
    },
}


def connect() -> Any:
    return psycopg2.connect(os.environ["WAREHOUSE_URL"])


def traced(fn: Callable[..., Any]) -> Callable[..., Any]:
    """Run a task callable inside span task.<task_id> and flush telemetry before the task process exits."""

    @functools.wraps(fn)
    def wrapper(**context: Any) -> Any:
        try:
            with telemetry.tracer().start_as_current_span(f"task.{context['ti'].task_id}"):
                return fn(**context)
        finally:
            telemetry.flush()

    return wrapper


def count_rows(stage: str, outcome: str, n: int) -> None:
    telemetry.instrument("counter", "pipeline.rows_processed").add(n, {"stage": stage, "outcome": outcome})


@traced
def run_quality(**context: Any) -> None:
    """Checks for this DAG and load date. Results are committed before a broken rule fails the task."""
    conn = connect()
    try:
        results = runner.run(conn, context["ds"], context["dag"].dag_id, context["run_id"])
        conn.commit()
    finally:
        conn.close()
    failed = [f"{r['check_name']} on {r['table_name']}" for r in results if not r["passed"]]
    if failed:
        raise AirflowException(f"quality rules broken: {', '.join(failed)}")
