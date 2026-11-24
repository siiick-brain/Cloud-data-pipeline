"""Send one sample of each metrics.md instrument to the collector, from a container on pipeline_default.

Usage: python emit_metrics.py <run_id>. run_id goes in service.instance.id, which Prometheus shows as
`exported_instance`, so contract labels keep contract values.
"""

import sys

from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource

run_id = sys.argv[1]
reader = PeriodicExportingMetricReader(
    OTLPMetricExporter(endpoint="http://otel-collector:4318/v1/metrics"), export_interval_millis=5000
)
resource = Resource.create({"service.name": "obs-test", "service.instance.id": run_id})
provider = MeterProvider(resource=resource, metric_readers=[reader])
meter = provider.get_meter("tests.observability")

meter.create_counter("pipeline.rows_processed").add(3, {"stage": "consumer", "outcome": "ok"})
meter.create_gauge("pipeline.consumer_lag").set(7, {"topic": "obs-test", "partition": "0"})
meter.create_gauge("pipeline.quality_score").set(0.9, {"check_name": "obs_test", "table_name": "obs_test"})
meter.create_gauge("pipeline.dag_duration_seconds").set(12.5, {"dag_id": "obs_test", "state": "success"})

provider.force_flush()
provider.shutdown()
print("sent", run_id)
