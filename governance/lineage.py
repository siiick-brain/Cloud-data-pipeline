"""Lineage graph generated from one hand-written edge list.

Run: python -m governance.lineage. Prints OpenLineage-style JSON and rewrites governance/lineage.mmd.
Dataset ids are "<store>:<name>" where store is kafka, pg (PostgreSQL warehouse) or delta.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

MMD_PATH = Path(__file__).with_name("lineage.mmd")
JOB_NAMESPACE = "cloud-data-pipeline"

# (source, target, job). The only place lineage is written by hand.
EDGES = [
    ("kafka:orders.v1", "pg:raw.order_events", "ingest.consumer"),
    ("pg:raw.customers", "pg:dw.dim_customer", "etl_orders"),
    ("pg:raw.products", "pg:dw.dim_product", "etl_orders"),
    ("pg:raw.stores", "pg:dw.dim_store", "etl_orders"),
    ("pg:raw.order_events", "pg:staging.fact_order_line", "etl_orders"),
    ("pg:dw.dim_customer", "pg:staging.fact_order_line", "etl_orders"),
    ("pg:dw.dim_product", "pg:staging.fact_order_line", "etl_orders"),
    ("pg:dw.dim_store", "pg:staging.fact_order_line", "etl_orders"),
    ("pg:dw.dim_date", "pg:staging.fact_order_line", "etl_orders"),
    ("pg:staging.fact_order_line", "pg:dw.fact_order_line", "etl_orders"),
    ("pg:dw.fact_order_line", "pg:gold.daily_sales", "elt_gold"),
    ("pg:dw.dim_store", "pg:gold.daily_sales", "elt_gold"),
    ("pg:dw.fact_order_line", "pg:gold.product_sales", "elt_gold"),
    ("pg:dw.dim_product", "pg:gold.product_sales", "elt_gold"),
    ("pg:raw.order_events", "delta:bronze.order_events", "spark.bronze"),
    ("delta:bronze.order_events", "delta:silver.order_lines", "spark.silver"),
    ("delta:silver.order_lines", "delta:gold.daily_sales", "spark.gold"),
    ("delta:silver.order_lines", "delta:gold.product_sales", "spark.gold"),
]


def dataset(node: str) -> dict:
    """OpenLineage dataset naming: Kafka topic, PostgreSQL database.schema.table, Delta file path."""
    store, name = node.split(":", 1)
    if store == "kafka":
        return {"namespace": "kafka://kafka:9092", "name": name}
    if store == "pg":
        return {"namespace": "postgres://postgres:5432", "name": f"warehouse.{name}"}
    return {"namespace": "file", "name": "/data/delta/" + name.replace(".", "/")}


def to_json() -> dict:
    nodes = list(dict.fromkeys(n for s, t, _ in EDGES for n in (s, t)))
    return {
        "datasets": [dataset(n) for n in nodes],
        "edges": [
            {"source": dataset(s), "target": dataset(t), "job": {"namespace": JOB_NAMESPACE, "name": j}}
            for s, t, j in EDGES
        ],
    }


def _node_id(node: str) -> str:
    return re.sub(r"\W", "_", node.replace(":", "_"))


def to_mermaid() -> str:
    lines = ["flowchart LR"]
    for node in dict.fromkeys(n for s, t, _ in EDGES for n in (s, t)):
        store, name = node.split(":", 1)
        label = name if store == "pg" else f"{store} {name}"
        lines.append(f'    {_node_id(node)}["{label}"]')
    lines += [f"    {_node_id(s)} -->|{j}| {_node_id(t)}" for s, t, j in EDGES]
    return "\n".join(lines) + "\n"


def main() -> None:
    MMD_PATH.write_text(to_mermaid(), encoding="utf-8", newline="\n")
    print(json.dumps(to_json(), indent=2))


if __name__ == "__main__":
    main()
