"""Retention dry run by default, deletes only with --apply."""

from __future__ import annotations

import pytest

from governance import catalog, retention


class FakeCursor:
    def __init__(self, conn):
        self.conn = conn
        self.result = None

    def execute(self, query, params=None):
        text = query if isinstance(query, str) else repr(query)
        self.conn.sql.append((text, params))
        if "governance.catalog" in text:
            self.result = [
                ("gold", "daily_sales", 730),
                ("quality", "dq_results", 180),
                ("raw", "customers", 365),
            ]
        elif "count(*)" in text:
            self.result = [(3,)]
        else:
            self.rowcount = 3

    def fetchall(self):
        return self.result

    def fetchone(self):
        return self.result[0]

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class FakeConn:
    def __init__(self):
        self.sql = []
        self.committed = False
        self.closed = False

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        self.committed = True

    def close(self):
        self.closed = True


@pytest.fixture
def fake(monkeypatch):
    conn = FakeConn()
    monkeypatch.setattr(retention.psycopg2, "connect", lambda url: conn)
    return conn


def test_age_columns_match_contract():
    assert retention.AGE_COLUMNS == {
        ("raw", "order_events"): "received_at",
        ("dw", "fact_order_line"): "occurred_at",
        ("gold", "daily_sales"): "load_date",
        ("gold", "product_sales"): "load_date",
        ("quality", "dq_results"): "checked_at",
    }


def test_dry_run_is_default_and_deletes_nothing(fake, capsys):
    retention.main([])
    out = capsys.readouterr().out
    assert "would delete 3 rows from gold.daily_sales (load_date older than 730 days)" in out
    assert "quality.dq_results" in out
    assert "raw.customers" not in out  # exempt even though the catalog has retention_days
    assert not any("DELETE" in q for q, _ in fake.sql)
    assert fake.closed and not fake.committed


def test_apply_deletes_and_commits(fake, capsys):
    retention.main(["--apply"])
    assert "deleted 3 rows from gold.daily_sales" in capsys.readouterr().out
    deletes = [p for q, p in fake.sql if "DELETE" in q]
    assert deletes == [(730,), (180,)]
    assert fake.committed and fake.closed


@pytest.mark.integration
def test_apply_removes_only_expired_rows(wh_conn):
    catalog.load(wh_conn)
    cur = wh_conn.cursor()
    cur.execute(
        "INSERT INTO quality.dq_results (run_id, check_name, table_name, passed, score, checked_at) VALUES "
        "('ret-test', 'c', 't', true, 1, now() - interval '400 days'), "
        "('ret-test', 'c', 't', true, 1, now())"
    )
    retention.run(wh_conn, apply=False)
    cur.execute("SELECT count(*) FROM quality.dq_results WHERE run_id = 'ret-test'")
    assert cur.fetchone()[0] == 2
    retention.run(wh_conn, apply=True)
    cur.execute("SELECT count(*) FROM quality.dq_results WHERE run_id = 'ret-test'")
    assert cur.fetchone()[0] == 1
