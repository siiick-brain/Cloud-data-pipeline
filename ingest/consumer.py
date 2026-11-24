"""Load orders.v1 into raw.order_events. Bad events go to orders.dlq.v1.

Order per batch: DB insert commits, then DLQ flush, then Redis dedupe SET NX, then the Kafka offset commit.
A crash anywhere replays the batch and the event_id primary key drops what already landed.
"""

from __future__ import annotations

import json
import logging
import os
import signal
import tempfile
import time
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import psycopg2
import redis
from confluent_kafka import Consumer, Producer
from jsonschema import Draft202012Validator
from opentelemetry import metrics, trace

from ingest.producer import DLQ_TOPIC, TOPIC, ensure_topics, kafka_conf

log = logging.getLogger("ingest.consumer")

GROUP_ID = "orders-raw-loader"
BATCH_SIZE = 500
DEDUPE_TTL = 7 * 24 * 3600
DIM_TTL = 3600
DIMS = (("customer", "raw.customers"), ("product", "raw.products"), ("store", "raw.stores"))
# /tmp/heartbeat in the Linux container; the OS temp dir when run on a dev host
HEARTBEAT = Path(tempfile.gettempdir()) / "heartbeat"
SCHEMA_PATH = Path(__file__).resolve().parents[1] / "docs" / "contracts" / "order_event.schema.json"
VALIDATOR = Draft202012Validator(json.loads(SCHEMA_PATH.read_text()))

INSERT_SQL = """
INSERT INTO raw.order_events (event_id, payload, kafka_partition, kafka_offset)
SELECT * FROM unnest(%s::uuid[], %s::jsonb[], %s::int[], %s::bigint[])
ON CONFLICT (event_id) DO NOTHING
RETURNING event_id::text
"""

meter = metrics.get_meter("ingest.consumer")
ROWS = meter.create_counter("pipeline.rows_processed")
LAG = meter.create_gauge("pipeline.consumer_lag")
tracer = trace.get_tracer("ingest.consumer")


def validate(raw: bytes | str | None) -> tuple[dict | None, str | None]:
    """Return (event, None) for a contract-valid event, else (None, reason)."""
    try:
        event = json.loads(raw or b"")
    except (ValueError, TypeError) as exc:
        return None, f"invalid JSON: {exc}"
    error = next(iter(VALIDATOR.iter_errors(event)), None)
    if error is not None:
        return None, f"schema: {error.message} at {list(error.absolute_path)}"
    return event, None


class Db:
    """Holds the warehouse connection and retries connection loss with capped exponential backoff."""

    def __init__(self, connect: Callable[[], Any], sleep: Callable[[float], None] = time.sleep):
        self.connect, self.sleep = connect, sleep
        self.conn: Any = None

    def run(self, fn: Callable[[Any], Any]) -> Any:
        delay = 1
        while True:
            try:
                if self.conn is None or self.conn.closed:
                    self.conn = self.connect()
                return fn(self.conn)
            except (psycopg2.OperationalError, psycopg2.InterfaceError) as exc:
                log.warning("database unavailable, retry in %ss: %s", delay, exc)
                if self.conn is not None:
                    self.conn.close()
                self.sleep(delay)
                delay = min(delay * 2, 30)


def cache_dimensions(cur: Any, r: Any, events: list[dict]) -> int:
    """Fill dim:<kind>:<id> for ids found in the raw source tables. Returns how many ids are unknown."""
    unknown = 0
    for kind, table in DIMS:
        ids = sorted({e[f"{kind}_id"] for e in events})
        cached = r.mget([f"dim:{kind}:{i}" for i in ids])
        missing = [i for i, hit in zip(ids, cached, strict=True) if hit is None]
        if not missing:
            continue
        # kind and table come from DIMS, never from input
        cur.execute(f"SELECT {kind}_id FROM {table} WHERE {kind}_id = ANY(%s)", (missing,))  # noqa: S608  # nosec B608
        found = [row[0] for row in cur.fetchall()]
        pipe = r.pipeline()
        for i in found:
            pipe.set(f"dim:{kind}:{i}", 1, ex=DIM_TTL)
        pipe.execute()
        unknown += len(missing) - len(found)
    return unknown


def land(conn: Any, r: Any, fresh: list[tuple[dict, Any]]) -> set[str]:
    with conn.cursor() as cur:
        unknown = cache_dimensions(cur, r, [e for e, _ in fresh])
        if unknown:
            log.info("%d unknown dimension ids, landed anyway", unknown)
        cur.execute(INSERT_SQL, (
            [e["event_id"] for e, _ in fresh],
            [json.dumps(e) for e, _ in fresh],
            [m.partition() for _, m in fresh],
            [m.offset() for _, m in fresh],
        ))
        inserted = {row[0] for row in cur.fetchall()}
    conn.commit()
    return inserted


def dlq_value(msg: Any, error: str) -> str:
    raw = msg.value()
    return json.dumps({
        "error": error,
        "raw": raw.decode("utf-8", errors="replace") if isinstance(raw, bytes) else str(raw),
        "failed_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "source_offset": msg.offset(),
    })


def process_batch(msgs: list, consumer: Any, dlq: Any, r: Any, db: Db) -> dict[str, int]:
    with tracer.start_as_current_span("consume_batch") as span:
        good: list[tuple[dict, Any]] = []
        bad: list[tuple[Any, str]] = []
        for m in msgs:
            if m.error():
                log.warning("kafka error: %s", m.error())
                continue
            event, error = validate(m.value())
            if event is None:
                bad.append((m, error or "invalid"))
            else:
                good.append((event, m))
        seen = r.mget([f"dedupe:{e['event_id']}" for e, _ in good]) if good else []
        fresh = [pair for pair, hit in zip(good, seen, strict=True) if hit is None]
        inserted: set[str] = db.run(lambda conn: land(conn, r, fresh)) if fresh else set()
        for m, error in bad:
            dlq.produce(DLQ_TOPIC, value=dlq_value(m, error), key=m.key())
        dlq.flush()
        if inserted:
            pipe = r.pipeline()
            for event_id in inserted:
                pipe.set(f"dedupe:{event_id}", 1, nx=True, ex=DEDUPE_TTL)
            pipe.execute()
        if good or bad:
            consumer.commit(asynchronous=False)
        counts = {"ok": len(inserted), "duplicate": len(good) - len(inserted), "dead_letter": len(bad)}
        for outcome, n in counts.items():
            if n:
                ROWS.add(n, {"stage": "consumer", "outcome": outcome})
        span.set_attribute("messaging.batch.message_count", len(msgs))
        span.set_attributes({f"rows.{k}": v for k, v in counts.items()})
        return counts


def record_lag(consumer: Any) -> dict[tuple[str, int], int]:
    lags = {}
    for tp in consumer.position(consumer.assignment()):
        if tp.offset < 0:
            continue
        _, high = consumer.get_watermark_offsets(tp, cached=True)
        lags[(tp.topic, tp.partition)] = max(high - tp.offset, 0)
        LAG.set(lags[(tp.topic, tp.partition)], {"topic": tp.topic, "partition": str(tp.partition)})
    return lags


def run(consumer: Any, dlq: Any, r: Any, db: Db, stop: Callable[[], bool]) -> None:
    consumer.subscribe([TOPIC])
    while not stop():
        HEARTBEAT.touch()
        msgs = consumer.consume(num_messages=BATCH_SIZE, timeout=1.0)
        if msgs:
            process_batch(msgs, consumer, dlq, r, db)
        record_lag(consumer)


def setup_telemetry() -> None:
    """OTLP HTTP export of metrics (every 5 s) and spans. Service name comes from OTEL_SERVICE_NAME."""
    from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    os.environ.setdefault("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector:4318")
    reader = PeriodicExportingMetricReader(OTLPMetricExporter(), export_interval_millis=5000)
    metrics.set_meter_provider(MeterProvider(metric_readers=[reader]))
    provider = TracerProvider()
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
    trace.set_tracer_provider(provider)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    setup_telemetry()
    conf = kafka_conf()
    ensure_topics(conf)
    consumer = Consumer({**conf, "group.id": GROUP_ID, "enable.auto.commit": False,
                         "auto.offset.reset": "earliest"})
    dlq = Producer({**conf, "enable.idempotence": True})
    r = redis.Redis.from_url(os.environ["REDIS_URL"])
    db = Db(lambda: psycopg2.connect(os.environ["WAREHOUSE_URL"]))
    stopping: list[bool] = []
    signal.signal(signal.SIGTERM, lambda *_: stopping.append(True))
    try:
        run(consumer, dlq, r, db, stop=lambda: bool(stopping))
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
