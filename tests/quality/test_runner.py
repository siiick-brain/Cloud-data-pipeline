"""Quality checks write dq_results, emit pipeline.quality_score and fail on a broken rule."""

from __future__ import annotations

import json
import uuid

import pytest

from quality import runner
from tests.quality.conftest import points

pytestmark = pytest.mark.integration

DAY = "2020-02-02"  # inside dim_date, far from any demo load date
RECON = frozenset({"check_name": "row_count_reconciliation", "table_name": "dw.fact_order_line"}.items())


def add_raw(cur, day=DAY, received="now()"):
    event_id = str(uuid.uuid4())
    cur.execute(
        "INSERT INTO raw.order_events (event_id, payload, load_date, received_at) "
        f"VALUES (%s, %s, %s, {received})",
        (event_id, json.dumps({"event_id": event_id}), day),
    )
    return event_id


def add_fact(cur, event_id, order_id="O99999999", line=1, day=DAY, loaded="now()"):
    cur.execute(
        "INSERT INTO dw.fact_order_line (event_id, order_id, line_number, customer_key, product_key, "
        "store_key, date_key, quantity, unit_price, currency, net_amount, occurred_at, load_date, loaded_at) "
        "VALUES (%s, %s, %s, -1, -1, -1, 20200202, 2, 5.00, 'USD', 10.00, '2020-02-02T10:00:00Z', %s, "
        f"{loaded})",
        (event_id, order_id, line, day),
    )


def by_name(results):
    return {(r["check_name"], r["table_name"]): r for r in results}


def test_consistent_load_passes_every_check_and_writes_results(wh_conn, memory_otel):
    cur = wh_conn.cursor()
    add_fact(cur, add_raw(cur))
    results = runner.run(wh_conn, DAY, "etl_orders", "run-ok")
    assert results and all(r["passed"] for r in results), [r for r in results if not r["passed"]]
    names = {r["check_name"] for r in results}
    assert names == {"nulls", "uniqueness", "referential_integrity", "row_count_reconciliation",
                     "freshness", "schema_validation"}
    cur.execute("SELECT count(*), min(score), bool_and(passed) FROM quality.dq_results "
                "WHERE run_id = 'run-ok'")
    assert cur.fetchone() == (len(results), 1, True)
    scores = points(memory_otel[0], "pipeline.quality_score")
    assert scores[RECON] == 1


def test_missing_fact_breaks_reconciliation_and_freshness(wh_conn, memory_otel):
    cur = wh_conn.cursor()
    add_raw(cur, received="now() + interval '3 hours'")
    results = by_name(runner.run(wh_conn, DAY, "etl_orders", "run-missing"))
    recon = results[("row_count_reconciliation", "dw.fact_order_line")]
    assert (recon["passed"], recon["observed"], recon["score"]) == (False, 1, 0)
    assert results[("freshness", "dw.fact_order_line")]["passed"] is False
    scores = points(memory_otel[0], "pipeline.quality_score")
    assert scores[RECON] == 0


def test_stale_fact_fails_freshness(wh_conn):
    cur = wh_conn.cursor()
    add_fact(cur, add_raw(cur), loaded="now() - interval '48 hours'")
    fresh = by_name(runner.run(wh_conn, DAY, "etl_orders", "run-stale"))[("freshness", "dw.fact_order_line")]
    assert fresh["passed"] is False and fresh["observed"] >= 47


def test_duplicate_business_key_fails_uniqueness(wh_conn):
    cur = wh_conn.cursor()
    add_fact(cur, add_raw(cur))
    add_fact(cur, add_raw(cur))  # same order_id and line_number, new event_id
    result = by_name(runner.run(wh_conn, DAY, "etl_orders", "run-dup"))[("uniqueness", "dw.fact_order_line")]
    assert (result["passed"], result["observed"]) == (False, 1)


def test_null_in_gold_fails_nulls(wh_conn):
    cur = wh_conn.cursor()
    cur.execute("INSERT INTO gold.product_sales VALUES (%s, %s, 'P99999', NULL, 1, 1.00)", (DAY, DAY))
    results = by_name(runner.run(wh_conn, DAY, "elt_gold", "run-gold"))
    assert results[("nulls", "gold.product_sales")]["passed"] is False
    assert results[("nulls", "gold.daily_sales")]["passed"] is True
    assert results[("schema_validation", "gold.daily_sales")]["passed"] is True


def test_schema_drift_fails_schema_validation(wh_conn, monkeypatch):
    daily = {**runner.SCHEMAS["gold.daily_sales"], "margin": "numeric"}
    drifted = {**runner.SCHEMAS, "gold.daily_sales": daily}
    monkeypatch.setattr(runner, "SCHEMAS", drifted)
    results = by_name(runner.run(wh_conn, DAY, "elt_gold", "run-drift"))
    result = results[("schema_validation", "gold.daily_sales")]
    assert (result["passed"], result["observed"]) == (False, 1)


class KeepOpen:
    """The test connection, with commit and close turned off so the test can still roll back."""

    def __init__(self, conn):
        self._conn = conn

    def __getattr__(self, name):
        return getattr(self._conn, name)

    def commit(self):
        pass

    def close(self):
        pass


def test_runner_exits_non_zero_on_broken_rule(wh_conn, monkeypatch):
    monkeypatch.setattr(runner.psycopg2, "connect", lambda _url: KeepOpen(wh_conn))
    add_raw(wh_conn.cursor())
    with pytest.raises(SystemExit) as exc:
        runner.main([DAY, "--dag-id", "etl_orders"])
    assert exc.value.code == 1


def test_runner_exits_zero_when_every_rule_holds(wh_conn, monkeypatch):
    monkeypatch.setattr(runner.psycopg2, "connect", lambda _url: KeepOpen(wh_conn))
    with pytest.raises(SystemExit) as exc:
        runner.main([DAY])  # no dag id runs every group
    assert exc.value.code == 0
    cur = wh_conn.cursor()
    cur.execute("SELECT count(DISTINCT table_name) FROM quality.dq_results WHERE run_id LIKE 'manual__%%' "
                "AND dag_id IS NULL AND checked_at = now()")
    assert cur.fetchone()[0] == 4


def test_real_providers_export_over_otlp_http(monkeypatch):
    monkeypatch.undo()  # drop the in-memory providers
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:9")
    monkeypatch.setenv("OTEL_SERVICE_NAME", "airflow")
    from opentelemetry.sdk.metrics import MeterProvider

    from quality import telemetry

    telemetry.providers.cache_clear()
    meter_provider, tracer_provider = telemetry.providers()
    try:
        assert isinstance(meter_provider, MeterProvider)
        assert tracer_provider.resource.attributes["service.name"] == "airflow"
        with telemetry.tracer().start_as_current_span("task.test"):
            pass
    finally:
        meter_provider.shutdown()
        tracer_provider.shutdown()
        telemetry.providers.cache_clear()
