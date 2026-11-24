"""Unit tests for the pipeline MCP server: HTTP guard, audit log and the three tools."""

from __future__ import annotations

import json
import os
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import psycopg2
import pyarrow as pa
import pytest
from deltalake import write_deltalake
from pipeline_mcp import server
from starlette.testclient import TestClient

TOKEN = "unit-test-token-0123456789"
ORIGINS = ["http://localhost", "http://127.0.0.1"]
HEADERS = {
    "accept": "application/json, text/event-stream",
    "content-type": "application/json",
    "authorization": f"Bearer {TOKEN}",
}


# ---------- fixtures ----------


@pytest.fixture
def audit_file(tmp_path, monkeypatch):
    path = tmp_path / "audit.log"
    monkeypatch.setenv("MCP_AUDIT_LOG", str(path))
    return path


@pytest.fixture
def client(audit_file):
    with TestClient(server.create_app(TOKEN, ORIGINS)) as c:
        yield c


def _dec(v):
    return Decimal(v).quantize(Decimal("0.01"))


@pytest.fixture
def delta_root(tmp_path, monkeypatch):
    root = tmp_path / "delta"
    daily = pa.table(
        {
            "sale_date": pa.array([date(2026, 1, 2), date(2026, 1, 1), date(2026, 1, 1), date(2026, 3, 1)]),
            "store_id": ["S002", "S002", "S001", "S001"],
            "order_lines": pa.array([3, 2, 1, 4], pa.int64()),
            "units": pa.array([6, 4, 1, 8], pa.int64()),
            "revenue": pa.array(
                [_dec("60.50"), _dec("40.25"), _dec("10.00"), _dec("80.00")], pa.decimal128(14, 2)
            ),
        }
    )
    product = pa.table(
        {
            "sale_date": pa.array([date(2026, 1, 1), date(2026, 1, 2), date(2026, 1, 1), date(2026, 1, 1)]),
            "product_id": ["P1", "P1", "P2", "P3"],
            "units": pa.array([1, 2, 5, 1], pa.int64()),
            "revenue": pa.array(
                [_dec("10.00"), _dec("20.00"), _dec("25.00"), _dec("1.00")], pa.decimal128(14, 2)
            ),
        }
    )
    write_deltalake(str(root / "gold" / "daily_sales"), daily)
    write_deltalake(str(root / "gold" / "product_sales"), product)
    monkeypatch.setenv("DELTA_ROOT", str(root))
    return root


class _NoCloseConn:
    """Wraps the rolled-back test connection so the server's close() leaves it open."""

    def __init__(self, conn):
        self._conn = conn

    def cursor(self, *a, **kw):
        return self._conn.cursor(*a, **kw)

    def close(self):
        pass


DAG_RUNS = [  # (dag_id, run_id, state, execution_date, start_date, end_date)
    (
        "etl_orders",
        "r1",
        "success",
        datetime(2026, 1, 1, tzinfo=UTC),
        datetime(2026, 1, 1, 0, 1, tzinfo=UTC),
        datetime(2026, 1, 1, 0, 5, tzinfo=UTC),
    ),
    (
        "etl_orders",
        "r2",
        "running",
        datetime(2026, 1, 2, tzinfo=UTC),
        datetime(2026, 1, 2, 0, 1, tzinfo=UTC),
        None,
    ),
    ("elt_gold", "g1", "queued", datetime(2026, 1, 3, tzinfo=UTC), None, None),
]


class _FakeAirflowConn:
    """Stands in for the Airflow metadata DB (the test role cannot create a dag_run table)."""

    def __init__(self):
        self.executed = []
        self.description = [(c,) for c in ("dag_id", "run_id", "state", "start_date", "end_date")]

    def cursor(self):
        return self

    def execute(self, sql, args):
        self.executed.append((sql, args))
        rows = [r for r in DAG_RUNS if args["dag_id"] is None or r[0] == args["dag_id"]]
        rows.sort(key=lambda r: r[3], reverse=True)
        self._rows = [r[:3] + r[4:] for r in rows[: args["limit"]]]

    def fetchall(self):
        return self._rows

    def close(self):
        pass


@pytest.fixture
def db(monkeypatch):
    """Warehouse connection in a transaction that is rolled back, plus a fake Airflow DB."""
    conn = psycopg2.connect(os.environ["WAREHOUSE_URL"])
    cur = conn.cursor()
    cur.execute("DELETE FROM quality.dq_results")
    cur.execute(
        "INSERT INTO quality.dq_results (run_id, check_name, table_name, passed, score, checked_at) VALUES "
        "('x','not_null','dw.fact_sales',true,1.0,'2026-01-01T00:00Z'),"
        "('x','unique','dw.fact_sales',false,0.5,'2026-01-02T00:00Z'),"
        "('x','fresh','gold.daily_sales',true,0.9876,'2026-01-03T00:00Z')"
    )
    calls = []
    airflow = _FakeAirflowConn()

    def fake_connect(url, **kw):
        calls.append((url, kw))
        return airflow if url == "postgresql://airflow-url" else _NoCloseConn(conn)

    monkeypatch.setattr(server.psycopg2, "connect", fake_connect)
    monkeypatch.setenv("AIRFLOW_DB_URL", "postgresql://airflow-url")
    monkeypatch.setenv("WAREHOUSE_RO_URL", "postgresql://warehouse-url")
    try:
        yield calls
    finally:
        conn.rollback()
        conn.close()


def rpc(client, method, params=None, headers=HEADERS):
    body = {"jsonrpc": "2.0", "id": 1, "method": method}
    if params is not None:
        body["params"] = params
    return client.post("/mcp", json=body, headers=headers)


def call(client, tool, args):
    r = rpc(client, "tools/call", {"name": tool, "arguments": args})
    assert r.status_code == 200, r.text
    return r.json()["result"]


def ok(result):
    assert result["isError"] is False, result
    content = result["structuredContent"]
    return content["result"] if set(content) == {"result"} else content


def dates(date_from, date_to):
    return {"date_from": date_from, "date_to": date_to}


def audit_lines(path):
    return [json.loads(line) for line in path.read_text().splitlines()]


# ---------- HTTP guard ----------


def test_health_needs_no_auth_and_returns_no_data(client):
    r = client.get("/health")
    assert r.status_code == 200
    assert r.text == "ok"


def test_missing_token_is_401(client):
    h = {k: v for k, v in HEADERS.items() if k != "authorization"}
    r = rpc(client, "tools/list", headers=h)
    assert r.status_code == 401
    assert r.headers["www-authenticate"] == "Bearer"


@pytest.mark.parametrize("auth", [f"Bearer {TOKEN}x", "Bearer ", f"Basic {TOKEN}", TOKEN, "Bearer wrong"])
def test_wrong_token_is_401(client, auth):
    r = rpc(client, "tools/list", headers={**HEADERS, "authorization": auth})
    assert r.status_code == 401


def test_bearer_scheme_is_case_insensitive(client):
    r = rpc(client, "tools/list", headers={**HEADERS, "authorization": f"bearer {TOKEN}"})
    assert r.status_code == 200


@pytest.mark.parametrize(
    "origin", ["http://evil.example", "http://localhost.evil.example", "null", "http://localhost:1"]
)
def test_bad_origin_is_403(client, origin):
    r = rpc(client, "tools/list", headers={**HEADERS, "origin": origin})
    assert r.status_code == 403


def test_allowed_origin_and_no_origin_pass(client):
    assert rpc(client, "tools/list", headers={**HEADERS, "origin": "http://localhost"}).status_code == 200
    assert rpc(client, "tools/list").status_code == 200


def test_denied_requests_are_audited_without_token(client, audit_file):
    rpc(client, "tools/list", headers={**HEADERS, "authorization": "Bearer leaked-guess"})
    rpc(client, "tools/list", headers={**HEADERS, "origin": "http://evil.example"})
    lines = audit_lines(audit_file)
    assert [x["status"] for x in lines] == ["unauthorized", "forbidden"]
    assert lines[1]["caller_origin"] == "http://evil.example"
    assert "leaked-guess" not in audit_file.read_text()


def test_exactly_three_tools_are_listed(client):
    r = rpc(client, "tools/list")
    names = sorted(t["name"] for t in r.json()["result"]["tools"])
    assert names == ["dq_scores", "gold_query", "pipeline_status"]


# ---------- startup ----------


def test_main_refuses_to_start_without_token(monkeypatch):
    monkeypatch.delenv("MCP_TOKEN", raising=False)
    with pytest.raises(SystemExit):
        server.main()
    monkeypatch.setenv("MCP_TOKEN", "  ")
    with pytest.raises(SystemExit):
        server.main()


def test_main_binds_per_contract(monkeypatch):
    seen = {}
    monkeypatch.setenv("MCP_TOKEN", TOKEN)
    monkeypatch.setenv("MCP_ALLOWED_ORIGINS", "http://localhost, http://127.0.0.1 ,")
    monkeypatch.delenv("MCP_HOST", raising=False)
    monkeypatch.setattr(
        server.uvicorn, "run", lambda app, host, port, **kw: seen.update(host=host, port=port)
    )
    monkeypatch.setattr(server, "create_app", lambda token, origins: seen.update(origins=origins))
    server.main()
    assert seen == {"host": "127.0.0.1", "port": 8765, "origins": ["http://localhost", "http://127.0.0.1"]}
    monkeypatch.setenv("MCP_HOST", "0.0.0.0")
    server.main()
    assert seen["host"] == "0.0.0.0"


# ---------- pipeline_status ----------


def test_pipeline_status_newest_first(client, db, audit_file):
    rows = ok(call(client, "pipeline_status", {}))
    assert [r["run_id"] for r in rows] == ["g1", "r2", "r1"]
    assert rows[2] == {
        "dag_id": "etl_orders",
        "run_id": "r1",
        "state": "success",
        "start_date": "2026-01-01T00:01:00+00:00",
        "end_date": "2026-01-01T00:05:00+00:00",
    }
    assert rows[0]["start_date"] is None and rows[0]["end_date"] is None
    assert db[0][0] == "postgresql://airflow-url"
    assert "default_transaction_read_only=on" in db[0][1]["options"]


def test_pipeline_status_filter_and_limit(client, db):
    rows = ok(call(client, "pipeline_status", {"dag_id": "etl_orders", "limit": 1}))
    assert [r["run_id"] for r in rows] == ["r2"]


@pytest.mark.parametrize(
    "args",
    [
        {"limit": 0},
        {"limit": 51},
        {"dag_id": "other"},
        {"dag_id": "etl_orders' OR '1'='1"},
        {"dag_id": "etl_orders; DROP TABLE dag_run"},
    ],
)
def test_pipeline_status_bad_params(client, db, args, audit_file):
    res = call(client, "pipeline_status", args)
    assert res["isError"] is True
    assert db == []  # nothing ran
    assert audit_lines(audit_file)[-1]["status"] == "error"


# ---------- dq_scores ----------


def test_dq_scores_newest_first(client, db):
    rows = ok(call(client, "dq_scores", {}))
    assert [r["check_name"] for r in rows] == ["fresh", "unique", "not_null"]
    assert rows[0] == {
        "check_name": "fresh",
        "table_name": "gold.daily_sales",
        "passed": True,
        "score": 0.9876,
        "checked_at": "2026-01-03T00:00:00+00:00",
    }
    assert db[0][0] == "postgresql://warehouse-url"


def test_dq_scores_filter_and_limit(client, db):
    rows = ok(call(client, "dq_scores", {"table_name": "dw.fact_sales", "limit": 1}))
    assert [r["check_name"] for r in rows] == ["unique"]
    assert ok(call(client, "dq_scores", {"table_name": "nope"})) == []


@pytest.mark.parametrize(
    "args", [{"limit": 0}, {"limit": 101}, {"table_name": "x' OR 1=1 --"}, {"table_name": "a" * 129}]
)
def test_dq_scores_bad_params(client, db, args):
    assert call(client, "dq_scores", args)["isError"] is True
    assert db == []


def test_db_failure_returns_generic_error(client, monkeypatch, audit_file):
    def boom(url, **kw):
        raise psycopg2.OperationalError("password authentication failed for user mcp_ro secretpw")

    monkeypatch.setattr(server.psycopg2, "connect", boom)
    monkeypatch.setenv("WAREHOUSE_RO_URL", "postgresql://mcp_ro:secretpw@db/warehouse")
    res = call(client, "dq_scores", {})
    assert res["isError"] is True
    assert "secretpw" not in json.dumps(res)
    assert "secretpw" not in audit_file.read_text()


# ---------- gold_query ----------


def test_gold_daily_sales(client, delta_root):
    out = ok(
        call(client, "gold_query", {"query": "daily_sales", "params": dates("2026-01-01", "2026-01-31")})
    )
    assert out["query"] == "daily_sales"
    assert out["row_count"] == 3
    assert out["rows"] == [
        {"sale_date": "2026-01-01", "store_id": "S001", "order_lines": 1, "units": 1, "revenue": 10.0},
        {"sale_date": "2026-01-01", "store_id": "S002", "order_lines": 2, "units": 4, "revenue": 40.25},
        {"sale_date": "2026-01-02", "store_id": "S002", "order_lines": 3, "units": 6, "revenue": 60.5},
    ]


def test_gold_top_products(client, delta_root):
    out = ok(call(client, "gold_query", {"query": "top_products", "params": {"limit": 2}}))
    assert out == {
        "query": "top_products",
        "row_count": 2,
        "rows": [
            {"product_id": "P1", "units": 3, "revenue": 30.0},
            {"product_id": "P2", "units": 5, "revenue": 25.0},
        ],
    }


def test_gold_store_sales(client, delta_root):
    out = ok(call(client, "gold_query", {"query": "store_sales", "params": {"store_id": "S001"}}))
    assert [r["sale_date"] for r in out["rows"]] == ["2026-01-01", "2026-03-01"]
    assert all(r["store_id"] == "S001" for r in out["rows"])


def test_gold_row_cap(client, delta_root, monkeypatch):
    monkeypatch.setattr(server, "MAX_ROWS", 2)
    out = ok(call(client, "gold_query", {"query": "store_sales", "params": {"store_id": "S001"}}))
    assert out["row_count"] == 2
    out = ok(
        call(client, "gold_query", {"query": "daily_sales", "params": dates("2026-01-01", "2026-12-31")})
    )
    assert out["row_count"] == 2 and len(out["rows"]) == 2


def test_gold_missing_table_is_error(client, tmp_path, monkeypatch):
    monkeypatch.setenv("DELTA_ROOT", str(tmp_path / "empty"))
    res = call(client, "gold_query", {"query": "top_products", "params": {"limit": 1}})
    assert res["isError"] is True
    assert str(tmp_path) not in json.dumps(res)


@pytest.mark.parametrize(
    "query,params",
    [
        ("unknown", {}),
        ("DROP TABLE x", {}),
        ("daily_sales; SELECT 1", {"date_from": "2026-01-01", "date_to": "2026-01-02"}),
        ("../daily_sales", {"limit": 1}),
        ("__class__", {}),
        ("daily_sales", {}),
        ("daily_sales", {"date_from": "2026-01-02", "date_to": "2026-01-01"}),
        ("daily_sales", {"date_from": "2025-01-01", "date_to": "2026-01-02"}),
        ("daily_sales", {"date_from": "2026-1-1", "date_to": "2026-01-02"}),
        ("daily_sales", {"date_from": "2026-01-01' OR 1=1", "date_to": "2026-01-02"}),
        ("daily_sales", {"date_from": 20260101, "date_to": "2026-01-02"}),
        ("daily_sales", {"date_from": "2026-02-30", "date_to": "2026-03-01"}),
        ("daily_sales", {"date_from": "2026-01-01", "date_to": "2026-01-02", "extra": 1}),
        ("top_products", {"limit": 0}),
        ("top_products", {"limit": 51}),
        ("top_products", {"limit": "5"}),
        ("top_products", {"limit": False}),
        ("store_sales", {"store_id": "S01"}),
        ("store_sales", {"store_id": "S0011"}),
        ("store_sales", {"store_id": "S001\n"}),
        ("store_sales", {"store_id": "S١٢٣"}),
        ("store_sales", {"store_id": "S001' OR '1'='1"}),
        ("store_sales", {"store_id": ["S001"]}),
    ],
)
def test_gold_rejects_unknown_query_and_bad_params(client, delta_root, monkeypatch, query, params):
    def must_not_run(*a, **kw):
        raise AssertionError("Delta was read for a rejected request")

    monkeypatch.setattr(server, "DeltaTable", must_not_run)
    res = call(client, "gold_query", {"query": query, "params": params})
    assert res["isError"] is True


def test_gold_accepts_366_day_range(client, delta_root):
    out = ok(
        call(client, "gold_query", {"query": "daily_sales", "params": dates("2024-01-01", "2024-12-31")})
    )
    assert out["row_count"] == 0


# ---------- audit log ----------


def test_audit_log_records_calls_without_secrets(client, db, delta_root, audit_file, capsys, monkeypatch):
    monkeypatch.setenv("WAREHOUSE_RO_URL", "postgresql://mcp_ro:dbsecret@postgres/warehouse")
    call(client, "dq_scores", {"limit": 2})
    rpc(
        client,
        "tools/call",
        {"name": "gold_query", "arguments": {"query": "top_products", "params": {"limit": 1}}},
        headers={**HEADERS, "origin": "http://127.0.0.1"},
    )
    call(client, "gold_query", {"query": "nope", "params": {}})
    lines = audit_lines(audit_file)
    assert [x["tool"] for x in lines] == ["dq_scores", "gold_query", "gold_query"]
    first = lines[0]
    assert set(first) == {"ts", "tool", "params", "caller_origin", "status", "row_count", "duration_ms"}
    assert first["params"] == {"table_name": None, "limit": 2}
    assert first["status"] == "ok" and first["row_count"] == 2
    assert first["caller_origin"] is None
    assert lines[1]["caller_origin"] == "http://127.0.0.1" and lines[1]["row_count"] == 1
    assert lines[2]["status"] == "error" and lines[2]["row_count"] == 0
    stdout = capsys.readouterr().out
    for text in (audit_file.read_text(), stdout):
        assert TOKEN not in text
        assert "dbsecret" not in text
        assert "authorization" not in text.lower()
        assert "postgresql://" not in text
    assert sum(1 for line in stdout.splitlines() if line.startswith('{"ts"')) == 3


def test_audit_works_without_file(monkeypatch, capsys):
    monkeypatch.delenv("MCP_AUDIT_LOG", raising=False)
    server.audit(tool="x", params={}, caller_origin=None, status="ok", row_count=0, duration_ms=1)
    assert '"tool": "x"' in capsys.readouterr().out


def test_no_string_built_sql_in_source():
    src = Path(server.__file__).read_text(encoding="utf-8")
    assert ".format(" not in src
    assert 'f"SELECT' not in src and "f'SELECT" not in src
    assert "% (" not in src
