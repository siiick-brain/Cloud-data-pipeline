"""Load generator master data (customers, products, stores) into the raw source tables."""

from __future__ import annotations

import os

import psycopg2
from psycopg2.extras import execute_values

from ingest import generator

N_CUSTOMERS = int(os.environ.get("SEED_CUSTOMERS", "1000"))
N_PRODUCTS = int(os.environ.get("SEED_PRODUCTS", "200"))
N_STORES = int(os.environ.get("SEED_STORES", "20"))


def seed(conn) -> dict[str, int]:
    rows: dict[str, list[tuple]] = {
        "raw.customers": [
            (c["customer_id"], c["full_name"], c["email"], c["city"], c["address"])
            for c in generator.customers(N_CUSTOMERS)
        ],
        "raw.products": [
            (p["product_id"], p["product_name"], p["category"], p["list_price"])
            for p in generator.products(N_PRODUCTS)
        ],
        "raw.stores": [(s["store_id"], s["store_name"], s["city"]) for s in generator.stores(N_STORES)],
    }
    columns = {
        "raw.customers": "customer_id, full_name, email, city, address",
        "raw.products": "product_id, product_name, category, list_price",
        "raw.stores": "store_id, store_name, city",
    }
    with conn, conn.cursor() as cur:
        for table, values in rows.items():
            # table and column names come from the dicts above, never from input
            execute_values(
                cur,
                f"INSERT INTO {table} ({columns[table]}) VALUES %s ON CONFLICT DO NOTHING",  # noqa: S608  # nosec B608
                values,
            )
    return {table: len(values) for table, values in rows.items()}


def main() -> None:
    with psycopg2.connect(os.environ["WAREHOUSE_URL"]) as conn:
        print(seed(conn))


if __name__ == "__main__":
    main()
