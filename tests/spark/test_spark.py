"""Spark lakehouse tests. Run inside the Spark image (see docs/contracts/env.md)."""

from __future__ import annotations

import datetime as dt
import json
import os
import threading
import urllib.error
import urllib.request
from decimal import Decimal

import pytest

pytest.importorskip("pyspark")

from spark import jobs, time_travel, trigger  # noqa: E402


@pytest.fixture(scope="session")
def spark():
    return jobs.get_spark()


@pytest.fixture
def root(tmp_path, monkeypatch):
    monkeypatch.setenv("DELTA_ROOT", str(tmp_path))
    return str(tmp_path)


def event(n, product="P00001", store="S001", qty=2, price=1.25, occurred="2026-11-20T10:00:00Z"):
    return {
        "event_id": f"00000000-0000-4000-8000-{n:012d}",
        "schema_version": 1,
        "occurred_at": occurred,
        "order_id": f"O{n:08d}",
        "line_number": 1,
        "customer_id": "C000001",
        "product_id": product,
        "store_id": store,
        "quantity": qty,
        "unit_price": price,
        "currency": "USD",
    }


def raw_df(spark, events):
    rows = [(e["event_id"], json.dumps(e), dt.date(2026, 11, 20)) for e in events]
    return spark.createDataFrame(rows, "event_id string, payload string, load_date date")


def test_bronze_merge_is_idempotent(spark, root):
    src = raw_df(spark, [event(1), event(2)])
    assert jobs.bronze(spark, src) == 2
    jobs.bronze(spark, raw_df(spark, [event(2), event(3)]))
    df = spark.read.format("delta").load(jobs.table_path("bronze.order_events"))
    assert df.count() == 3
    assert [f.name for f in df.schema] == ["event_id", "payload", "load_date", "ingested_at"]
    assert dict((f.name, f.dataType.simpleString()) for f in df.schema)["ingested_at"] == "timestamp"


def test_silver_parses_casts_and_merges(spark, root):
    jobs.bronze(spark, raw_df(spark, [event(1, qty=3, price=19.99), event(2)]))
    assert jobs.silver(spark) == 2
    assert jobs.silver(spark) == 2  # rerun does not duplicate
    df = spark.read.format("delta").load(jobs.table_path("silver.order_lines"))
    types = {f.name: f.dataType.simpleString() for f in df.schema}
    assert types == {
        "event_id": "string", "order_id": "string", "line_number": "int", "customer_id": "string",
        "product_id": "string", "store_id": "string", "quantity": "int", "unit_price": "decimal(10,2)",
        "currency": "string", "net_amount": "decimal(12,2)", "occurred_at": "timestamp", "load_date": "date",
    }
    assert [f.name for f in df.schema] == list(types)
    assert df.count() == 2
    row = df.where("order_id = 'O00000001'").first()
    assert row.net_amount == Decimal("59.97")
    assert row.occurred_at == dt.datetime(2026, 11, 20, 10, 0)


def test_silver_rejects_schema_drift(spark, root):
    jobs.bronze(spark, raw_df(spark, [event(1)]))
    jobs.silver(spark)
    bad = spark.createDataFrame([("x", 1)], "event_id string, extra int")
    with pytest.raises(Exception, match="(?i)schema|column|extra"):
        bad.write.format("delta").mode("append").save(jobs.table_path("silver.order_lines"))


def test_gold_aggregates_and_overwrites(spark, root):
    events = [
        event(1, store="S001", product="P00001", qty=2, price=1.25),
        event(2, store="S001", product="P00002", qty=1, price=10.00),
        event(3, store="S002", product="P00001", qty=4, price=1.25, occurred="2026-11-21T23:59:59Z"),
    ]
    jobs.bronze(spark, raw_df(spark, events))
    jobs.silver(spark)
    jobs.gold(spark)
    jobs.gold(spark)  # second run overwrites, not appends
    daily = spark.read.format("delta").load(jobs.table_path("gold.daily_sales"))
    assert {f.name: f.dataType.simpleString() for f in daily.schema} == {
        "sale_date": "date", "store_id": "string", "order_lines": "bigint", "units": "bigint",
        "revenue": "decimal(14,2)",
    }
    got = {(r.sale_date, r.store_id): (r.order_lines, r.units, r.revenue) for r in daily.collect()}
    assert got == {
        (dt.date(2026, 11, 20), "S001"): (2, 3, Decimal("12.50")),
        (dt.date(2026, 11, 21), "S002"): (1, 4, Decimal("5.00")),
    }
    prod = spark.read.format("delta").load(jobs.table_path("gold.product_sales"))
    assert [f.name for f in prod.schema] == ["sale_date", "product_id", "units", "revenue"]
    assert prod.count() == 3


def test_run_dispatch(spark, root, monkeypatch):
    calls = []
    for name in ("bronze", "silver", "gold"):
        monkeypatch.setitem(jobs.JOBS, name, lambda s, n=name: calls.append(n))
    jobs.run("all", spark)
    assert calls == ["bronze", "silver", "gold"]
    with pytest.raises(KeyError):
        jobs.run("nope", spark)


def test_time_travel_demo(spark, tmp_path):
    result = time_travel.demo(spark, str(tmp_path / "tt"))
    assert result["bad_write_rejected"] is True
    assert result["rows_v0"] == 2
    assert result["rows_broken"] == 0
    assert result["rows_restored"] == 3


def test_time_travel_main(spark, root, capsys):
    time_travel.main()
    assert "restored" in capsys.readouterr().out


@pytest.fixture
def server(monkeypatch):
    srv = trigger.make_server(0)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{srv.server_address[1]}"
    srv.shutdown()
    srv.server_close()


def call(url, method="GET"):
    req = urllib.request.Request(url, method=method)
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as err:
        return err.code, json.loads(err.read())


def test_trigger_health(server):
    assert call(server + "/health")[0] == 200
    assert call(server + "/nope")[0] == 404


def test_trigger_ok_failed_unknown(server, monkeypatch):
    monkeypatch.setattr(jobs, "run", lambda job, spark: None)
    monkeypatch.setattr(jobs, "get_spark", lambda: None)
    status, body = call(server + "/run/gold", "POST")
    assert status == 200
    assert body["job"] == "gold" and body["status"] == "ok" and isinstance(body["duration_s"], float)

    def boom(job, spark):
        raise RuntimeError("x")

    monkeypatch.setattr(jobs, "run", boom)
    status, body = call(server + "/run/all", "POST")
    assert (status, body["status"], body["job"]) == (500, "failed", "all")
    assert call(server + "/run/nope", "POST")[0] == 404


def test_trigger_runs_one_job_at_a_time(server, monkeypatch):
    active, peak = [0], [0]
    gate = threading.Lock()

    def slow(job, spark):
        with gate:
            active[0] += 1
            peak[0] = max(peak[0], active[0])
        threading.Event().wait(0.3)
        with gate:
            active[0] -= 1

    monkeypatch.setattr(jobs, "run", slow)
    monkeypatch.setattr(jobs, "get_spark", lambda: None)
    threads = [threading.Thread(target=call, args=(server + "/run/bronze", "POST")) for _ in range(3)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert peak[0] == 1


@pytest.mark.integration
def test_bronze_reads_raw_over_jdbc(spark, root, monkeypatch):
    if not os.environ.get("JDBC_PASSWORD"):
        monkeypatch.setenv("JDBC_PASSWORD", os.environ.get("PIPELINE_DB_PASSWORD", ""))
    df = jobs.read_raw(spark)
    assert [f.name for f in df.schema] == ["event_id", "payload", "load_date"]
    read = df.count()
    jobs.bronze(spark)
    # raw can grow while the test runs, so check no duplicates and nothing missing
    bronze = spark.read.format("delta").load(jobs.table_path("bronze.order_events"))
    assert bronze.count() == bronze.select("event_id").distinct().count() >= read
