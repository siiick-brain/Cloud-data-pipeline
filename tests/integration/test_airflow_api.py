"""Airflow REST checks after `make demo`: both DAGs, the forced failure, the retry and the success."""

from __future__ import annotations

import os

import pytest
import requests

pytestmark = pytest.mark.integration

API = f"http://127.0.0.1:{os.environ.get('AIRFLOW_PORT', '8080')}/api/v1"
AUTH = (os.environ.get("AIRFLOW_ADMIN_USER", "admin"), os.environ.get("AIRFLOW_ADMIN_PASSWORD", ""))


def get(path: str, **params) -> requests.Response:
    return requests.get(f"{API}{path}", auth=AUTH, params=params, timeout=30)


def demo_run(dag_id: str) -> dict:
    runs = get(f"/dags/{dag_id}/dagRuns", order_by="-start_date", limit=50).json()["dag_runs"]
    demo = [r for r in runs if r["dag_run_id"].startswith("demo_")]
    assert demo, f"no demo run for {dag_id}; run make demo first"
    return demo[0]


def test_both_dags_are_listed():
    ids = {d["dag_id"] for d in get("/dags").json()["dags"]}
    assert {"etl_orders", "elt_gold"} <= ids


@pytest.mark.parametrize("dag_id", ["etl_orders", "elt_gold"])
def test_demo_run_succeeded(dag_id):
    assert demo_run(dag_id)["state"] == "success"


def test_extract_failed_once_then_the_retry_succeeded():
    run_id = demo_run("etl_orders")["dag_run_id"]
    tries = get(f"/dags/etl_orders/dagRuns/{run_id}/taskInstances/extract/tries").json()["task_instances"]
    states = [t["state"] for t in sorted(tries, key=lambda t: t["try_number"])]
    assert states[0] in ("failed", "up_for_retry")
    assert states[-1] == "success"
    assert len(states) == 2


def test_force_fail_flag_is_cleared_after_the_retry():
    assert get("/variables/FORCE_FAIL").status_code == 404
