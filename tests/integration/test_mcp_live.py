"""The running MCP container answers real HTTP calls in time (in-process tests cannot catch a hung server)."""

from __future__ import annotations

import os

import pytest
import requests

pytestmark = pytest.mark.integration

BASE = f"http://127.0.0.1:{os.environ.get('MCP_PORT', '8765')}/mcp"
HEADERS = {
    "Authorization": f"Bearer {os.environ.get('MCP_TOKEN', '')}",
    "Accept": "application/json, text/event-stream",
    "Content-Type": "application/json",
}


def rpc(method: str, params: dict | None = None) -> dict:
    body = {"jsonrpc": "2.0", "id": 1, "method": method, **({"params": params} if params else {})}
    r = requests.post(BASE, headers=HEADERS, json=body, timeout=10)
    assert r.status_code == 200, r.text
    return r.json()["result"]


def test_initialize_and_list_tools():
    info = rpc("initialize", {"protocolVersion": "2025-06-18", "capabilities": {},
                              "clientInfo": {"name": "live-check", "version": "1"}})
    assert info["serverInfo"]["name"] == "pipeline"
    assert {t["name"] for t in rpc("tools/list")["tools"]} == {"pipeline_status", "dq_scores", "gold_query"}


@pytest.mark.parametrize("name,args", [
    ("pipeline_status", {"limit": 2}),
    ("dq_scores", {"limit": 2}),
    ("gold_query", {"query": "top_products", "params": {"limit": 3}}),
])
def test_each_tool_answers(name, args):
    assert rpc("tools/call", {"name": name, "arguments": args})["isError"] is False


def test_unknown_query_is_refused():
    result = rpc("tools/call", {"name": "gold_query", "arguments": {"query": "DROP TABLE x", "params": {}}})
    assert result["isError"] is True


def test_missing_token_is_rejected():
    r = requests.post(BASE, headers={k: v for k, v in HEADERS.items() if k != "Authorization"},
                      json={"jsonrpc": "2.0", "id": 1, "method": "tools/list"}, timeout=10)
    assert r.status_code == 401
