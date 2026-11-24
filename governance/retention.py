"""Delete rows past retention. Dry run by default; pass --apply to delete.

Run: python -m governance.retention [--apply] (reads WAREHOUSE_URL).
retention_days comes from governance.catalog; age columns come from docs/contracts/tables.md (Retention).
"""

from __future__ import annotations

import argparse
import os

import psycopg2
from psycopg2 import sql

# Tables not listed here are exempt (see the Retention section of tables.md).
AGE_COLUMNS = {
    ("raw", "order_events"): "received_at",
    ("dw", "fact_order_line"): "occurred_at",
    ("gold", "daily_sales"): "load_date",
    ("gold", "product_sales"): "load_date",
    ("quality", "dq_results"): "checked_at",
}

RULES = """
SELECT table_schema, table_name, max(retention_days)
FROM governance.catalog
WHERE retention_days IS NOT NULL
GROUP BY table_schema, table_name
ORDER BY table_schema, table_name
"""


def run(conn, apply: bool) -> None:
    with conn.cursor() as cur:
        cur.execute(RULES)
        rules = [(s, t, days) for s, t, days in cur.fetchall() if (s, t) in AGE_COLUMNS]
        for schema, table, days in rules:
            column = AGE_COLUMNS[(schema, table)]
            where = sql.SQL("{} < now() - make_interval(days => %s)").format(sql.Identifier(column))
            target = sql.Identifier(schema, table)
            if apply:
                cur.execute(sql.SQL("DELETE FROM {} WHERE ").format(target) + where, (days,))
                print(f"deleted {cur.rowcount} rows from {schema}.{table}")
            else:
                cur.execute(sql.SQL("SELECT count(*) FROM {} WHERE ").format(target) + where, (days,))
                count = cur.fetchone()[0]
                print(f"would delete {count} rows from {schema}.{table} ({column} older than {days} days)")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="delete the rows instead of only counting them")
    args = parser.parse_args(argv)
    conn = psycopg2.connect(os.environ["WAREHOUSE_URL"])
    try:
        run(conn, args.apply)
        if args.apply:
            conn.commit()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
