"""Telemetry goes to in-memory readers in tests, so nothing waits on a collector."""

from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def memory_otel(monkeypatch):
    """Swap the OTLP providers for in-memory ones. Yields the metric reader and span exporter."""
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import InMemoryMetricReader
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

    from quality import telemetry

    reader, spans = InMemoryMetricReader(), InMemorySpanExporter()
    tracer_provider = TracerProvider()
    tracer_provider.add_span_processor(SimpleSpanProcessor(spans))
    providers = (MeterProvider(metric_readers=[reader]), tracer_provider)
    monkeypatch.setattr(telemetry, "providers", lambda: providers)
    telemetry.instrument.cache_clear()
    yield reader, spans
    telemetry.instrument.cache_clear()


def points(reader, name):
    """{frozenset(labels): value} for one metric name from an in-memory reader."""
    out = {}
    data = reader.get_metrics_data()
    for rm in data.resource_metrics if data else []:
        for sm in rm.scope_metrics:
            for metric in sm.metrics:
                if metric.name == name:
                    for p in metric.data.data_points:
                        out[frozenset(p.attributes.items())] = p.value
    return out
