"""Collector -> Prometheus names from docs/contracts/metrics.md, and the pipeline-health board."""

from __future__ import annotations

import json
import os
import re
import subprocess
import time
import uuid

import pytest
import requests
import yaml

from tests.conftest import ROOT

CONTRACT_METRICS = re.findall(
    r"^\| (pipeline_\w+) \|", (ROOT / "docs/contracts/metrics.md").read_text(), flags=re.M
)
EMITTER_IMAGE = "python:3.11-slim-bookworm"
OTEL_PKGS = "opentelemetry-sdk==1.45.2 opentelemetry-exporter-otlp-proto-http==1.45.2"


def test_contract_lists_four_metrics():
    assert CONTRACT_METRICS == [
        "pipeline_rows_processed_total",
        "pipeline_consumer_lag",
        "pipeline_quality_score",
        "pipeline_dag_duration_seconds",
    ]


def test_collector_config_matches_contract():
    cfg = yaml.safe_load((ROOT / "infra/otel-collector.yaml").read_text())
    assert cfg["receivers"]["otlp"]["protocols"]["http"]["endpoint"] == "0.0.0.0:4318"
    prom = cfg["exporters"]["prometheus"]
    assert prom["endpoint"] == "0.0.0.0:8889"
    assert prom["add_metric_suffixes"] is True
    metrics = cfg["service"]["pipelines"]["metrics"]
    assert metrics["receivers"] == ["otlp"]
    assert "prometheus" in metrics["exporters"]


def test_prometheus_scrapes_collector_job():
    cfg = yaml.safe_load((ROOT / "infra/prometheus.yml").read_text())
    jobs = {j["job_name"]: j for j in cfg["scrape_configs"]}
    assert jobs["otel-collector"]["static_configs"][0]["targets"] == ["otel-collector:8889"]


def test_pipeline_health_dashboard_has_one_panel_per_metric():
    dash = json.loads((ROOT / "dashboards/json/pipeline_health.json").read_text())
    assert dash["uid"] == "pipeline-health"
    panels = [p for p in dash["panels"] if p.get("type") != "row"]
    assert len(panels) == len(CONTRACT_METRICS)
    for metric, panel in zip(CONTRACT_METRICS, panels, strict=True):
        assert panel["datasource"]["uid"] == "prometheus"
        assert all(t["datasource"]["uid"] == "prometheus" for t in panel["targets"])
        assert any(metric in t["expr"] for t in panel["targets"]), (metric, panel["title"])


@pytest.mark.integration
def test_otlp_instruments_reach_prometheus_with_contract_names():
    """The collector has no host port, so the emitter runs in a container on pipeline_default."""
    run_id = uuid.uuid4().hex
    subprocess.run(
        ["docker", "run", "--rm", "--network", "pipeline_default",
         "-v", f"{ROOT / 'tests/observability'}:/obs:ro", EMITTER_IMAGE, "sh", "-c",
         f"pip install -q --disable-pip-version-check {OTEL_PKGS} && python /obs/emit_metrics.py {run_id}"],
        check=True, timeout=300,
    )
    url = f"http://127.0.0.1:{os.environ.get('PROMETHEUS_PORT', '9090')}/api/v1/query"
    missing = set(CONTRACT_METRICS)
    deadline = time.time() + 60
    while missing and time.time() < deadline:
        for name in sorted(missing):
            query = f'{name}{{exported_instance="{run_id}"}}'
            body = requests.get(url, params={"query": query}, timeout=5).json()
            if body["data"]["result"]:
                missing.discard(name)
        time.sleep(2)
    assert not missing, f"not in Prometheus: {sorted(missing)}"
