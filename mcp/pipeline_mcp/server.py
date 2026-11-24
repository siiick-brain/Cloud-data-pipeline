"""Read-only pipeline MCP server (contract: docs/contracts/mcp_tools.md).

Three tools: pipeline_status (Airflow dag_run), dq_scores (quality.dq_results) and
gold_query (allowlisted reads of the Delta gold tables). All SQL is fixed text with bound
parameters. Every HTTP request except GET /health needs the bearer token from MCP_TOKEN.

Run: python -m pipeline_mcp.server
"""

from __future__ import annotations

import contextvars
import hmac
import json
import os
import re
import sys
import time
from contextlib import closing
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

import psycopg2
import pyarrow.dataset as ds
import uvicorn
from deltalake import DeltaTable
from mcp.server.mcpserver import MCPServer
from mcp.server.transport_security import TransportSecuritySettings
from starlette.requests import Request
from starlette.responses import PlainTextResponse
from starlette.types import ASGIApp, Receive, Scope, Send

PORT = 8765
MAX_ROWS = 1000
DAG_IDS = ("etl_orders", "elt_gold")
# Each session is read only with a short statement timeout; the mcp_ro role only has SELECT too.
PG_OPTIONS = "-c default_transaction_read_only=on -c statement_timeout=5000"
STORE_ID = re.compile(r"S[0-9]{3}")
ISO_DATE = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}")
TABLE_NAME = re.compile(r"[A-Za-z0-9_.]{1,128}")
SALES_COLUMNS = ["sale_date", "store_id", "order_lines", "units", "revenue"]

PIPELINE_STATUS_SQL = (
    "SELECT dag_id, run_id, state, start_date, end_date FROM dag_run "
    "WHERE (%(dag_id)s IS NULL OR dag_id = %(dag_id)s) "
    "ORDER BY execution_date DESC, id DESC LIMIT %(limit)s"
)
DQ_SCORES_SQL = (
    "SELECT check_name, table_name, passed, score, checked_at FROM quality.dq_results "
    "WHERE (%(table_name)s IS NULL OR table_name = %(table_name)s) "
    "ORDER BY checked_at DESC, id DESC LIMIT %(limit)s"
)


class BadRequest(ValueError):
    """A rejected tool argument. Its message is safe to show to the caller."""


_origin: contextvars.ContextVar[str | None] = contextvars.ContextVar("caller_origin", default=None)


# ---------- audit ----------


def audit(**record: Any) -> None:
    """Write one JSON line to stdout and to MCP_AUDIT_LOG. Callers never pass secrets."""
    line = json.dumps({"ts": datetime.now(UTC).isoformat(), **record}, default=str)
    print(line, flush=True)
    path = os.environ.get("MCP_AUDIT_LOG")
    if path:
        with open(path, "a", encoding="utf-8") as f:
            f.write(line + "\n")


def _run_tool(tool: str, params: dict[str, Any], fn: Any) -> Any:
    """Run fn() and audit it. Error text shown to the caller is our own, never a driver message."""
    start = time.monotonic()
    status, rows = "error", 0
    try:
        result = fn()
        rows = result["row_count"] if isinstance(result, dict) else len(result)
        status = "ok"
        return result
    except BadRequest:
        raise
    except Exception:
        raise RuntimeError(f"{tool} failed") from None
    finally:
        audit(
            tool=tool,
            params=params,
            caller_origin=_origin.get(),
            status=status,
            row_count=rows,
            duration_ms=round((time.monotonic() - start) * 1000, 1),
        )


# ---------- validation and conversion ----------


def _int(value: Any, name: str, lo: int, hi: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not lo <= value <= hi:
        raise BadRequest(f"{name} must be an integer from {lo} to {hi}")
    return value


def _date(value: Any, name: str) -> date:
    if not isinstance(value, str) or not ISO_DATE.fullmatch(value):
        raise BadRequest(f"{name} must be an ISO date YYYY-MM-DD")
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise BadRequest(f"{name} is not a valid date") from None


def _keys(params: Any, expected: set[str]) -> dict[str, Any]:
    if not isinstance(params, dict) or set(params) != expected:
        raise BadRequest(f"params must have exactly: {', '.join(sorted(expected))}")
    return params


def _json_value(v: Any) -> Any:
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, datetime):
        return (v.astimezone(UTC) if v.tzinfo else v).isoformat()
    if isinstance(v, date):
        return v.isoformat()
    return v


def _rows(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{k: _json_value(v) for k, v in r.items()} for r in records]


# ---------- data access ----------


def _fetch(url_env: str, sql: str, args: dict[str, Any]) -> list[dict[str, Any]]:
    with closing(psycopg2.connect(os.environ[url_env], options=PG_OPTIONS)) as conn:
        cur = conn.cursor()
        cur.execute(sql, args)
        cols = [c[0] for c in cur.description or ()]
        return _rows([dict(zip(cols, row, strict=True)) for row in cur.fetchall()])


def _gold(table: str) -> ds.Dataset:
    return DeltaTable(os.path.join(os.environ["DELTA_ROOT"], "gold", table)).to_pyarrow_dataset()


def _daily_sales(params: Any) -> list[dict[str, Any]]:
    p = _keys(params, {"date_from", "date_to"})
    d1, d2 = _date(p["date_from"], "date_from"), _date(p["date_to"], "date_to")
    if d1 > d2 or (d2 - d1).days + 1 > 366:
        raise BadRequest("date range must be ordered and at most 366 days")
    t = _gold("daily_sales").to_table(filter=(ds.field("sale_date") >= d1) & (ds.field("sale_date") <= d2))
    t = t.sort_by([("sale_date", "ascending"), ("store_id", "ascending")])
    return t.select(SALES_COLUMNS).slice(0, MAX_ROWS).to_pylist()


def _top_products(params: Any) -> list[dict[str, Any]]:
    limit = _int(_keys(params, {"limit"})["limit"], "limit", 1, 50)
    t = _gold("product_sales").to_table(columns=["product_id", "units", "revenue"])
    t = t.group_by("product_id").aggregate([("units", "sum"), ("revenue", "sum")])
    t = t.rename_columns({"units_sum": "units", "revenue_sum": "revenue"})
    t = t.sort_by([("revenue", "descending"), ("product_id", "ascending")])
    return t.select(["product_id", "units", "revenue"]).slice(0, min(limit, MAX_ROWS)).to_pylist()


def _store_sales(params: Any) -> list[dict[str, Any]]:
    store_id = _keys(params, {"store_id"})["store_id"]
    if not isinstance(store_id, str) or not STORE_ID.fullmatch(store_id):
        raise BadRequest("store_id must match ^S\\d{3}$")
    t = _gold("daily_sales").to_table(filter=ds.field("store_id") == store_id).sort_by("sale_date")
    return t.select(SALES_COLUMNS).slice(0, MAX_ROWS).to_pylist()


GOLD_QUERIES = {"daily_sales": _daily_sales, "top_products": _top_products, "store_sales": _store_sales}


# ---------- tools ----------


def pipeline_status(dag_id: str | None = None, limit: int = 10) -> list[dict[str, Any]]:
    """Latest Airflow DAG runs, newest first. dag_id: etl_orders or elt_gold (optional). limit: 1 to 50."""
    params = {"dag_id": dag_id, "limit": limit}

    def run() -> list[dict[str, Any]]:
        if dag_id is not None and dag_id not in DAG_IDS:
            raise BadRequest("dag_id must be etl_orders or elt_gold")
        args = {"dag_id": dag_id, "limit": _int(limit, "limit", 1, 50)}
        return _fetch("AIRFLOW_DB_URL", PIPELINE_STATUS_SQL, args)

    return _run_tool("pipeline_status", params, run)


def dq_scores(table_name: str | None = None, limit: int = 20) -> list[dict[str, Any]]:
    """Latest data quality check results, newest first. table_name optional. limit: 1 to 100."""
    params = {"table_name": table_name, "limit": limit}

    def run() -> list[dict[str, Any]]:
        if table_name is not None and not (isinstance(table_name, str) and TABLE_NAME.fullmatch(table_name)):
            raise BadRequest("table_name must be 1 to 128 letters, digits, '_' or '.'")
        args = {"table_name": table_name, "limit": _int(limit, "limit", 1, 100)}
        return _fetch("WAREHOUSE_RO_URL", DQ_SCORES_SQL, args)

    return _run_tool("dq_scores", params, run)


def gold_query(query: str, params: dict[str, Any]) -> dict[str, Any]:
    """Run one allowlisted gold query.

    daily_sales {date_from, date_to} (ISO dates, at most 366 days); top_products {limit 1 to 50};
    store_sales {store_id like S001}. Returns {query, row_count, rows}, at most 1000 rows.
    """

    def run() -> dict[str, Any]:
        handler = GOLD_QUERIES.get(query) if isinstance(query, str) else None
        if handler is None:
            raise BadRequest(f"query must be one of: {', '.join(GOLD_QUERIES)}")
        rows = _rows(handler(params))
        return {"query": query, "row_count": len(rows), "rows": rows}

    return _run_tool("gold_query", {"query": query, "params": params}, run)


# ---------- HTTP ----------


class Guard:
    """Origin allowlist, then GET /health, then constant-time bearer token check."""

    def __init__(self, app: ASGIApp, token: str, origins: list[str]) -> None:
        self.app, self.token, self.origins = app, token.encode(), set(origins)

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        headers = Request(scope).headers
        origin = headers.get("origin")
        if origin is not None and origin not in self.origins:
            return await self._deny(scope, receive, send, 403, "forbidden", origin)
        if scope["path"] == "/health" and scope["method"] == "GET":
            return await PlainTextResponse("ok")(scope, receive, send)
        scheme, _, supplied = headers.get("authorization", "").partition(" ")
        if scheme.lower() != "bearer" or not hmac.compare_digest(supplied.encode(), self.token):
            return await self._deny(scope, receive, send, 401, "unauthorized", origin)
        reset = _origin.set(origin)
        try:
            await self.app(scope, receive, send)
        finally:
            _origin.reset(reset)

    @staticmethod
    async def _deny(
        scope: Scope, receive: Receive, send: Send, code: int, status: str, origin: str | None
    ) -> None:
        audit(tool=None, params={}, caller_origin=origin, status=status, row_count=0, duration_ms=0)
        extra = {"WWW-Authenticate": "Bearer"} if code == 401 else None
        await PlainTextResponse(status, status_code=code, headers=extra)(scope, receive, send)


def create_app(token: str, origins: list[str]) -> ASGIApp:
    server = MCPServer("pipeline", instructions="Read-only pipeline status, data quality and gold sales.")
    for fn in (pipeline_status, dq_scores, gold_query):
        server.add_tool(fn)
    # Origin is enforced by Guard against MCP_ALLOWED_ORIGINS, so the SDK's localhost-only check is off.
    app = server.streamable_http_app(
        stateless_http=True,
        json_response=True,
        transport_security=TransportSecuritySettings(enable_dns_rebinding_protection=False),
    )
    return Guard(app, token, origins)


def main() -> None:
    token = os.environ.get("MCP_TOKEN", "").strip()
    if not token:
        sys.exit("MCP_TOKEN is not set; refusing to start")
    origins = [o.strip() for o in os.environ.get("MCP_ALLOWED_ORIGINS", "").split(",") if o.strip()]
    host = os.environ.get("MCP_HOST", "127.0.0.1")
    uvicorn.run(create_app(token, origins), host=host, port=PORT, server_header=False)


if __name__ == "__main__":  # pragma: no cover
    main()
