"""Data quality checks for one load date. Writes quality.dq_results and emits pipeline.quality_score.

Usage: python -m quality.runner YYYY-MM-DD [--dag-id etl_orders|elt_gold]. Exits 1 when any rule is broken.
Every check is one SQL query returning one number (observed). A check passes when observed <= threshold.
Score is 1 for a pass and 0 for a fail.
"""

from __future__ import annotations

import argparse
import os
from datetime import UTC, datetime
from typing import Any

import psycopg2

from quality import telemetry

# Expected columns and information_schema data types, from docs/contracts/tables.md
SCHEMAS: dict[str, dict[str, str]] = {
    "raw.order_events": {
        "event_id": "uuid", "payload": "jsonb", "kafka_partition": "integer", "kafka_offset": "bigint",
        "received_at": "timestamp with time zone", "load_date": "date",
    },
    "dw.fact_order_line": {
        "event_id": "uuid", "order_id": "text", "line_number": "smallint", "customer_key": "integer",
        "product_key": "integer", "store_key": "integer", "date_key": "integer", "quantity": "integer",
        "unit_price": "numeric", "currency": "character", "net_amount": "numeric",
        "occurred_at": "timestamp with time zone", "load_date": "date",
        "loaded_at": "timestamp with time zone",
    },
    "gold.daily_sales": {
        "load_date": "date", "sale_date": "date", "store_id": "text", "order_lines": "integer",
        "units": "integer", "revenue": "numeric",
    },
    "gold.product_sales": {
        "load_date": "date", "sale_date": "date", "product_id": "text", "category": "text",
        "units": "integer", "revenue": "numeric",
    },
}

SCHEMA_SQL = """
SELECT count(*) FROM unnest(%(cols)s::text[], %(types)s::text[]) AS e(col, typ)
WHERE NOT EXISTS (
    SELECT 1 FROM information_schema.columns c
    WHERE c.table_schema = %(schema)s AND c.table_name = %(table)s
      AND c.column_name = e.col AND c.data_type = e.typ)
"""
# rows of the load date with any NULL column
NULLS_SQL = "SELECT count(*) FROM {table} t WHERE t.load_date = %(load_date)s AND NOT (t IS NOT NULL)"
UNIQUE_SQL = """
SELECT count(*) - count(DISTINCT (order_id, line_number))
FROM dw.fact_order_line WHERE load_date = %(load_date)s
"""
RI_SQL = """
SELECT count(*) FROM dw.fact_order_line f
LEFT JOIN dw.dim_customer c ON c.customer_key = f.customer_key
LEFT JOIN dw.dim_product p ON p.product_key = f.product_key
LEFT JOIN dw.dim_store s ON s.store_key = f.store_key
LEFT JOIN dw.dim_date d ON d.date_key = f.date_key
WHERE f.load_date = %(load_date)s
  AND (c.customer_key IS NULL OR p.product_key IS NULL OR s.store_key IS NULL OR d.date_key IS NULL)
"""
RECONCILE_SQL = """
SELECT abs((SELECT count(*) FROM raw.order_events WHERE load_date = %(load_date)s)
         - (SELECT count(*) FROM dw.fact_order_line WHERE load_date = %(load_date)s))
"""
# hours the newest raw event of the load date is ahead of the newest fact load (0 when the fact is current)
FRESH_SQL = """
SELECT greatest(0, extract(epoch FROM
    (SELECT max(received_at) FROM raw.order_events WHERE load_date = %(load_date)s)
  - coalesce((SELECT max(loaded_at) FROM dw.fact_order_line WHERE load_date = %(load_date)s),
             %(load_date)s::timestamptz)) / 3600)
"""


def checks(dag_id: str | None) -> list[tuple[str, str, str, float]]:
    """(check_name, table_name, sql, threshold) for one DAG, or for every DAG when dag_id is None."""
    groups: dict[str, list[tuple[str, str, str, float]]] = {
        "etl_orders": [
            ("schema_validation", "raw.order_events", SCHEMA_SQL, 0),
            ("schema_validation", "dw.fact_order_line", SCHEMA_SQL, 0),
            ("nulls", "dw.fact_order_line", NULLS_SQL.format(table="dw.fact_order_line"), 0),
            ("uniqueness", "dw.fact_order_line", UNIQUE_SQL, 0),
            ("referential_integrity", "dw.fact_order_line", RI_SQL, 0),
            ("row_count_reconciliation", "dw.fact_order_line", RECONCILE_SQL, 0),
            ("freshness", "dw.fact_order_line", FRESH_SQL, 1),
        ],
        "elt_gold": [
            ("schema_validation", "gold.daily_sales", SCHEMA_SQL, 0),
            ("schema_validation", "gold.product_sales", SCHEMA_SQL, 0),
            ("nulls", "gold.daily_sales", NULLS_SQL.format(table="gold.daily_sales"), 0),
            ("nulls", "gold.product_sales", NULLS_SQL.format(table="gold.product_sales"), 0),
        ],
    }
    return groups[dag_id] if dag_id else [c for group in groups.values() for c in group]


def run(conn: Any, load_date: str, dag_id: str | None, run_id: str) -> list[dict]:
    """Run the checks, insert one dq_results row per check (caller commits) and set the score gauge."""
    results = []
    gauge = telemetry.instrument("gauge", "pipeline.quality_score")
    with conn.cursor() as cur:
        for check_name, table, sql, threshold in checks(dag_id):
            schema, name = table.split(".")
            expected = SCHEMAS[table]
            # sql comes from the constants above, never from input
            cur.execute(sql, {"load_date": load_date, "schema": schema, "table": name,  # nosemgrep
                              "cols": list(expected), "types": list(expected.values())})
            observed = cur.fetchone()[0]
            passed = observed is not None and observed <= threshold
            score = 1 if passed else 0
            cur.execute(
                "INSERT INTO quality.dq_results (run_id, dag_id, check_name, table_name, passed, observed, "
                "threshold, score, details) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (run_id, dag_id, check_name, table, passed, observed, threshold, score,
                 f"load_date={load_date} observed={observed} threshold={threshold}"),
            )
            gauge.set(score, {"check_name": check_name, "table_name": table})
            results.append({"check_name": check_name, "table_name": table, "passed": passed,
                            "observed": observed, "threshold": threshold, "score": score})
    return results


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("load_date", help="YYYY-MM-DD")
    parser.add_argument("--dag-id", choices=["etl_orders", "elt_gold"], help="default: every check")
    args = parser.parse_args(argv)
    run_id = f"manual__{datetime.now(UTC).isoformat()}"
    conn = psycopg2.connect(os.environ["WAREHOUSE_URL"])
    try:
        results = run(conn, args.load_date, args.dag_id, run_id)
        conn.commit()
    finally:
        conn.close()
    telemetry.flush()
    for r in results:
        verdict = "PASS" if r["passed"] else "FAIL"
        print(f"{verdict} {r['check_name']} {r['table_name']} observed={r['observed']}")
    raise SystemExit(0 if all(r["passed"] for r in results) else 1)


if __name__ == "__main__":
    main()
