"""Load docs/data_dictionary.csv into governance.catalog. Upsert, so reruns are safe.

Run: python -m governance.catalog (reads WAREHOUSE_URL).
"""

from __future__ import annotations

import csv
import os
from pathlib import Path

import psycopg2

DICTIONARY = Path(__file__).resolve().parents[1] / "docs" / "data_dictionary.csv"

UPSERT = """
INSERT INTO governance.catalog
    (table_schema, table_name, column_name, data_type, owner, pii_tag, retention_days, description)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (table_schema, table_name, column_name) DO UPDATE SET
    data_type = EXCLUDED.data_type,
    owner = EXCLUDED.owner,
    pii_tag = EXCLUDED.pii_tag,
    retention_days = EXCLUDED.retention_days,
    description = EXCLUDED.description
"""


def read_dictionary(path: Path = DICTIONARY) -> list[tuple]:
    """Rows in catalog column order. The CSV nullable column is not part of the catalog."""
    with path.open(newline="", encoding="utf-8") as f:
        return [
            (
                r["table_schema"],
                r["table_name"],
                r["column_name"],
                r["data_type"],
                r["owner"],
                r["pii_tag"],
                int(r["retention_days"]) if r["retention_days"] else None,
                r["description"],
            )
            for r in csv.DictReader(f)
        ]


def load(conn) -> int:
    rows = read_dictionary()
    with conn.cursor() as cur:
        cur.executemany(UPSERT, rows)
    return len(rows)


def main() -> None:
    conn = psycopg2.connect(os.environ["WAREHOUSE_URL"])
    try:
        count = load(conn)
        conn.commit()
    finally:
        conn.close()
    print(f"upserted {count} catalog rows")


if __name__ == "__main__":
    main()
