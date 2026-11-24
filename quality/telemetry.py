"""OTLP HTTP metrics and spans for the quality runner and the DAGs (docs/contracts/metrics.md).

Private providers, not the global ones: Airflow installs its own global MeterProvider when OTEL_ON is set.
Built on first use so DAG parsing never starts exporter threads.
"""

from __future__ import annotations

import functools
import os
from typing import Any


@functools.cache
def providers() -> tuple[Any, Any]:
    from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    os.environ.setdefault("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector:4318")
    resource = Resource.create()  # service.name from OTEL_SERVICE_NAME
    reader = PeriodicExportingMetricReader(OTLPMetricExporter(), export_interval_millis=5000)
    tracer_provider = TracerProvider(resource=resource)
    tracer_provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
    return MeterProvider(resource=resource, metric_readers=[reader]), tracer_provider


@functools.cache
def instrument(kind: str, name: str) -> Any:
    """kind is 'counter' or 'gauge'."""
    meter = providers()[0].get_meter("pipeline")
    return meter.create_counter(name) if kind == "counter" else meter.create_gauge(name)


def tracer() -> Any:
    return providers()[1].get_tracer("pipeline")


def flush() -> None:
    """Push now: Airflow task processes exit with os._exit, which skips the SDK's atexit flush."""
    meter_provider, tracer_provider = providers()
    meter_provider.force_flush()
    tracer_provider.force_flush()
