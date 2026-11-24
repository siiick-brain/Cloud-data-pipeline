"""Shared fixtures. Integration tests use the compose stack through host ports."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


def _load_dotenv() -> None:
    env = ROOT / ".env"
    if env.exists():
        for line in env.read_text().splitlines():
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                os.environ.setdefault(key, value)


_load_dotenv()
os.environ.setdefault(
    "WAREHOUSE_URL",
    f"postgresql://pipeline:{os.environ.get('PIPELINE_DB_PASSWORD', '')}@localhost:5432/warehouse",
)


@pytest.fixture
def wh_conn():
    """Warehouse connection inside a transaction that is rolled back after the test."""
    import psycopg2

    conn = psycopg2.connect(os.environ["WAREHOUSE_URL"])
    try:
        yield conn
    finally:
        conn.rollback()
        conn.close()
