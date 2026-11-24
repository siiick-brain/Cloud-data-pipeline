"""Lineage graph generated from one edge list."""

from __future__ import annotations

import json

from governance import lineage


def names(job=None):
    graph = lineage.to_json()
    return {
        (e["source"]["name"], e["target"]["name"])
        for e in graph["edges"]
        if job is None or e["job"]["name"] == job
    }


def test_covers_every_hop():
    edges = names()
    assert ("orders.v1", "warehouse.raw.order_events") in edges
    assert ("warehouse.raw.customers", "warehouse.dw.dim_customer") in edges
    assert ("warehouse.staging.fact_order_line", "warehouse.dw.fact_order_line") in edges
    assert ("warehouse.dw.fact_order_line", "warehouse.gold.daily_sales") in edges
    assert ("warehouse.raw.order_events", "/data/delta/bronze/order_events") in edges
    assert ("/data/delta/bronze/order_events", "/data/delta/silver/order_lines") in edges
    assert ("/data/delta/silver/order_lines", "/data/delta/gold/product_sales") in edges


def test_openlineage_namespaces():
    graph = lineage.to_json()
    namespaces = {e["source"]["namespace"] for e in graph["edges"]} | {
        e["target"]["namespace"] for e in graph["edges"]
    }
    assert namespaces == {"kafka://kafka:9092", "postgres://postgres:5432", "file"}
    assert {e["job"]["namespace"] for e in graph["edges"]} == {"cloud-data-pipeline"}
    assert len(graph["datasets"]) == len({(d["namespace"], d["name"]) for d in graph["datasets"]})


def test_jobs_match_contract():
    assert {j for _, _, j in lineage.EDGES} == {
        "ingest.consumer",
        "etl_orders",
        "elt_gold",
        "spark.bronze",
        "spark.silver",
        "spark.gold",
    }


def test_mermaid_has_every_edge():
    text = lineage.to_mermaid()
    assert text.startswith("flowchart LR\n")
    assert text.count("-->") == len(lineage.EDGES)
    assert 'kafka_orders_v1["kafka orders.v1"]' in text
    assert "pg_raw_order_events -->|spark.bronze| delta_bronze_order_events" in text


def test_committed_mmd_is_current():
    assert lineage.MMD_PATH.read_text() == lineage.to_mermaid()


def test_main_writes_file_and_prints_json(tmp_path, monkeypatch, capsys):
    out = tmp_path / "lineage.mmd"
    monkeypatch.setattr(lineage, "MMD_PATH", out)
    lineage.main()
    assert out.read_text() == lineage.to_mermaid()
    assert json.loads(capsys.readouterr().out) == lineage.to_json()
