"""Seeded synthetic e-commerce data: customers, products, stores and order line events."""

from __future__ import annotations

import random
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta

SCHEMA_VERSION = 1
CITIES = ["Nairobi", "Lagos", "Accra", "Cairo", "Kigali", "Lusaka", "Dakar", "Tunis"]
CATEGORIES = ["books", "games", "garden", "kitchen", "sports", "toys"]


def customers(n: int, seed: int = 42) -> list[dict]:
    rng = random.Random(seed)  # noqa: S311 - seeded test data, not crypto
    return [
        {
            "customer_id": f"C{i:06d}",
            "full_name": f"Customer {i}",
            "email": f"customer{i}@example.com",
            "city": rng.choice(CITIES),
            "address": f"{rng.randint(1, 999)} Main Street",
        }
        for i in range(1, n + 1)
    ]


def products(n: int, seed: int = 42) -> list[dict]:
    rng = random.Random(seed + 1)  # noqa: S311 - seeded test data, not crypto
    return [
        {
            "product_id": f"P{i:05d}",
            "product_name": f"Product {i}",
            "category": rng.choice(CATEGORIES),
            "list_price": round(rng.uniform(2, 500), 2),
        }
        for i in range(1, n + 1)
    ]


def stores(n: int, seed: int = 42) -> list[dict]:
    rng = random.Random(seed + 2)  # noqa: S311 - seeded test data, not crypto
    return [
        {"store_id": f"S{i:03d}", "store_name": f"Store {i}", "city": rng.choice(CITIES)}
        for i in range(1, n + 1)
    ]


def events(
    count: int,
    seed: int = 42,
    n_customers: int = 1000,
    n_products: int = 200,
    n_stores: int = 20,
    start: datetime = datetime(2026, 1, 1, tzinfo=UTC),
    dup_rate: float = 0.0,
    bad_rate: float = 0.0,
) -> Iterator[dict]:
    """Yield order line events. dup_rate repeats an earlier event, bad_rate breaks one field."""
    rng = random.Random(seed + 3)  # noqa: S311 - seeded test data, not crypto
    prices = {p["product_id"]: p["list_price"] for p in products(n_products, seed)}
    recent: list[dict] = []
    for i in range(count):
        if recent and rng.random() < dup_rate:
            yield dict(rng.choice(recent))
            continue
        product_id = f"P{rng.randint(1, n_products):05d}"
        event = {
            "event_id": str(uuid.UUID(int=rng.getrandbits(128), version=4)),
            "schema_version": SCHEMA_VERSION,
            "occurred_at": (start + timedelta(seconds=i * 30)).isoformat().replace("+00:00", "Z"),
            "order_id": f"O{i // 3:08d}",
            "line_number": i % 3 + 1,
            "customer_id": f"C{rng.randint(1, n_customers):06d}",
            "product_id": product_id,
            "store_id": f"S{rng.randint(1, n_stores):03d}",
            "quantity": rng.randint(1, 5),
            "unit_price": prices[product_id],
            "currency": "USD",
        }
        if rng.random() < bad_rate:
            event[rng.choice(["quantity", "currency", "event_id"])] = "bad"
        recent = (recent + [event])[-100:]
        yield event
