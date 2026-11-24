"""The star schema, dim_date, unknown members and SCD2 behave as the contract says."""

from __future__ import annotations

import pytest

from tests.conftest import ROOT

pytestmark = pytest.mark.integration

LOAD_DIMS = (ROOT / "sql" / "load_dimensions.sql").read_text()


def scalar(cur, sql, params=None):
    cur.execute(sql, params)
    return cur.fetchone()[0]


def seed_customer(cur, cid="C900001", city="Nairobi", address="1 Main Street", updated="2026-01-01"):
    cur.execute(
        "INSERT INTO raw.customers VALUES (%s, 'Test User', 't@example.com', %s, %s, %s) "
        "ON CONFLICT (customer_id) DO UPDATE SET city = EXCLUDED.city, address = EXCLUDED.address, "
        "updated_at = EXCLUDED.updated_at",
        (cid, city, address, updated),
    )


def test_dim_date_covers_range(wh_conn):
    cur = wh_conn.cursor()
    assert scalar(cur, "SELECT count(*) FROM dw.dim_date WHERE date_key > 0") == 4018
    assert scalar(cur, "SELECT min(full_date)::text FROM dw.dim_date") == "2020-01-01"
    assert scalar(cur, "SELECT max(full_date)::text FROM dw.dim_date") == "2030-12-31"
    assert scalar(cur, "SELECT date_key FROM dw.dim_date WHERE full_date = '2026-02-28'") == 20260228


@pytest.mark.parametrize("table,key", [
    ("dw.dim_customer", "customer_key"), ("dw.dim_product", "product_key"),
    ("dw.dim_store", "store_key"), ("dw.dim_date", "date_key"),
])
def test_unknown_member(wh_conn, table, key):
    assert scalar(wh_conn.cursor(), f"SELECT count(*) FROM {table} WHERE {key} = -1") == 1


def test_scd2_address_change_closes_old_row_and_opens_new(wh_conn):
    cur = wh_conn.cursor()
    seed_customer(cur)
    cur.execute(LOAD_DIMS)
    seed_customer(cur, address="2 New Road", updated="2026-03-01")
    cur.execute(LOAD_DIMS)
    cur.execute(
        "SELECT address, valid_from::date::text, valid_to::date::text, is_current FROM dw.dim_customer "
        "WHERE customer_id = 'C900001' ORDER BY valid_from"
    )
    assert cur.fetchall() == [
        ("1 Main Street", "1900-01-01", "2026-03-01", False),
        ("2 New Road", "2026-03-01", "9999-12-31", True),
    ]


def test_dimension_load_is_idempotent(wh_conn):
    cur = wh_conn.cursor()
    seed_customer(cur)
    cur.execute(LOAD_DIMS)
    before = scalar(cur, "SELECT count(*) FROM dw.dim_customer")
    cur.execute(LOAD_DIMS)
    assert scalar(cur, "SELECT count(*) FROM dw.dim_customer") == before


def test_type1_change_overwrites_in_place(wh_conn):
    cur = wh_conn.cursor()
    seed_customer(cur)
    cur.execute(LOAD_DIMS)
    cur.execute("UPDATE raw.customers SET email = 'new@example.com' WHERE customer_id = 'C900001'")
    cur.execute(LOAD_DIMS)
    cur.execute("SELECT count(*), max(email) FROM dw.dim_customer WHERE customer_id = 'C900001'")
    assert cur.fetchone() == (1, "new@example.com")


def test_fact_rejects_orphan_foreign_key(wh_conn):
    import psycopg2

    cur = wh_conn.cursor()
    with pytest.raises(psycopg2.errors.ForeignKeyViolation):
        cur.execute(
            "INSERT INTO dw.fact_order_line (event_id, order_id, line_number, customer_key, product_key, "
            "store_key, date_key, quantity, unit_price, currency, net_amount, occurred_at, load_date) "
            "VALUES (gen_random_uuid(), 'O00000001', 1, 999999, -1, -1, -1, 1, 1, 'USD', 1, now(), now())"
        )


def test_roles_are_least_privilege(wh_conn):
    cur = wh_conn.cursor()
    assert scalar(cur, "SELECT has_table_privilege('grafana_ro', 'gold.daily_sales', 'INSERT')") is False
    assert scalar(cur, "SELECT has_table_privilege('mcp_ro', 'raw.customers', 'SELECT')") is False
    assert scalar(cur, "SELECT has_table_privilege('mcp_ro', 'quality.dq_results', 'SELECT')") is True


def test_data_dictionary_covers_every_column(wh_conn):
    import csv

    documented = {
        (r["table_schema"], r["table_name"], r["column_name"])
        for r in csv.DictReader((ROOT / "docs" / "data_dictionary.csv").open(encoding="utf-8"))
    }
    cur = wh_conn.cursor()
    cur.execute(
        "SELECT table_schema, table_name, column_name FROM information_schema.columns "
        "WHERE table_schema IN ('raw', 'dw', 'gold', 'quality', 'governance')"
    )
    missing = set(cur.fetchall()) - documented
    assert not missing
