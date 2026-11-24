"""Data dictionary CSV loads into governance.catalog."""

from __future__ import annotations

import pytest

from governance import catalog


def test_read_dictionary_keeps_catalog_columns():
    rows = catalog.read_dictionary()
    assert len(rows) == 95
    by_key = {r[:3]: r for r in rows}
    email = by_key[("raw", "customers", "email")]
    assert email == ("raw", "customers", "email", "text", "ingest", "email", 365, "Customer email address")
    assert by_key[("dw", "fact_order_line", "unit_price")][3] == "numeric(10,2)"
    assert by_key[("governance", "catalog", "owner")][6] is None


def test_read_dictionary_pii_tags_are_allowed():
    assert {r[5] for r in catalog.read_dictionary()} <= {"none", "email", "name", "address"}


class FakeCursor:
    def __init__(self):
        self.calls = []

    def executemany(self, sql, rows):
        self.calls.append((sql, list(rows)))

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class FakeConn:
    def __init__(self):
        self.cur = FakeCursor()
        self.committed = False
        self.closed = False

    def cursor(self):
        return self.cur

    def commit(self):
        self.committed = True

    def close(self):
        self.closed = True


def test_load_upserts_every_row():
    conn = FakeConn()
    assert catalog.load(conn) == 95
    sql, rows = conn.cur.calls[0]
    assert "ON CONFLICT (table_schema, table_name, column_name) DO UPDATE" in sql
    assert len(rows) == 95


def test_main_commits_and_closes(monkeypatch, capsys):
    conn = FakeConn()
    monkeypatch.setattr(catalog.psycopg2, "connect", lambda url: conn)
    catalog.main()
    assert conn.committed and conn.closed
    assert "95" in capsys.readouterr().out


@pytest.mark.integration
def test_load_is_idempotent(wh_conn):
    cur = wh_conn.cursor()
    catalog.load(wh_conn)
    cur.execute("UPDATE governance.catalog SET owner = 'someone' WHERE table_name = 'customers'")
    catalog.load(wh_conn)
    cur.execute("SELECT count(*) FROM governance.catalog")
    assert cur.fetchone()[0] == 95
    cur.execute(
        "SELECT owner, pii_tag, retention_days FROM governance.catalog "
        "WHERE table_schema = 'raw' AND table_name = 'customers' AND column_name = 'email'"
    )
    assert cur.fetchone() == ("ingest", "email", 365)
