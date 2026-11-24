"""DAG tests run in the Airflow image against the live warehouse. They commit, so they clean up after."""

from __future__ import annotations

import contextlib
import importlib.util
import json
import sys
import threading
import uuid
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import pytest

# Airflow puts dags/ on sys.path when it parses DAGs; pytest needs the same
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "dags"))

from tests.quality.conftest import memory_otel  # noqa: F401  (autouse in-memory telemetry)

if importlib.util.find_spec("airflow") is None:
    collect_ignore_glob = ["test_*.py"]  # host runs have no Airflow

DAY = "2030-06-15"  # after the DAG start_date and inside dim_date
EVENTS = [
    # customer, product, store, occurred_at, quantity, unit_price
    ("C990001", "P99001", "S999", "2030-06-15T09:00:00Z", 2, 10.5),
    ("C990001", "P99001", "S999", "2030-06-15T10:00:00Z", 1, 10.5),
    ("C990001", "P99002", "S999", "2030-06-15T11:00:00Z", 3, 4.25),  # P99002 is not in raw.products
    ("C990002", "P99001", "S999", "2019-12-31T23:59:00Z", 1, 10.5),  # outside dim_date
]


def _cleanup(cur):
    cur.execute("DELETE FROM quality.dq_results WHERE details LIKE %s", (f"load_date={DAY} %",))
    for table in ("gold.daily_sales", "gold.product_sales", "dw.fact_order_line", "raw.order_events"):
        cur.execute(f"DELETE FROM {table} WHERE load_date = %s", (DAY,))
    cur.execute("TRUNCATE staging.fact_order_line")
    cur.execute("DELETE FROM dw.dim_customer WHERE customer_id IN ('C990001', 'C990002')")
    cur.execute("DELETE FROM dw.dim_product WHERE product_id = 'P99001'")
    cur.execute("DELETE FROM dw.dim_store WHERE store_id = 'S999'")
    cur.execute("DELETE FROM raw.customers WHERE customer_id IN ('C990001', 'C990002')")
    cur.execute("DELETE FROM raw.products WHERE product_id = 'P99001'")
    cur.execute("DELETE FROM raw.stores WHERE store_id = 'S999'")


@pytest.fixture
def seeded():
    """Raw master data and four raw events for DAY, committed. Removed again after the test."""
    import psycopg2
    from dag_common import connect

    conn = connect()
    conn.autocommit = True
    cur = conn.cursor()
    _cleanup(cur)
    cur.execute("INSERT INTO raw.customers VALUES ('C990001', 'Test One', 'one@example.com', 'Accra', "
                "'1 Test Road', '2020-01-01'), ('C990002', 'Test Two', 'two@example.com', 'Lagos', "
                "'2 Test Road', '2020-01-01')")
    cur.execute("INSERT INTO raw.products VALUES ('P99001', 'Test Product', 'books', 10.50, '2020-01-01')")
    cur.execute("INSERT INTO raw.stores VALUES ('S999', 'Test Store', 'Accra', '2020-01-01')")
    for i, (cid, pid, sid, at, qty, price) in enumerate(EVENTS):
        event_id = str(uuid.uuid4())
        payload = {"event_id": event_id, "schema_version": 1, "occurred_at": at, "order_id": "O99990000",
                   "line_number": i + 1, "customer_id": cid, "product_id": pid, "store_id": sid,
                   "quantity": qty, "unit_price": price, "currency": "USD"}
        cur.execute("INSERT INTO raw.order_events (event_id, payload, load_date) VALUES (%s, %s, %s)",
                    (event_id, json.dumps(payload), DAY))
    try:
        yield cur
    finally:
        with contextlib.suppress(psycopg2.Error):
            _cleanup(cur)
        conn.close()


@pytest.fixture
def spark_stub(monkeypatch):
    """Local stand-in for the Spark trigger service. Yields the list of request paths it received."""
    calls: list[str] = []
    status = {"code": 200}

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            calls.append(self.path)
            body = json.dumps({"job": "all", "status": "ok" if status["code"] == 200 else "failed",
                               "duration_s": 0.1}).encode()
            self.send_response(status["code"])
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    monkeypatch.setenv("SPARK_TRIGGER_URL", f"http://127.0.0.1:{server.server_port}")
    try:
        yield calls, status
    finally:
        server.shutdown()
