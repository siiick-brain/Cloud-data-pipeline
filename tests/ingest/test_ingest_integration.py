"""Ingest integration tests against the compose kafka, redis and postgres (host ports in env.md)."""

from __future__ import annotations

import json
import os
import random
import subprocess
import sys
import time
import uuid

import psycopg2
import pytest
from confluent_kafka import Consumer, Producer, TopicPartition

from ingest import consumer, generator, producer, replay_dlq
from tests.conftest import ROOT

pytestmark = pytest.mark.integration

os.environ.setdefault("KAFKA_BOOTSTRAP", "127.0.0.1:9094")
os.environ.setdefault("REDIS_URL", f"redis://:{os.environ.get('REDIS_PASSWORD', '')}@localhost:6379/0")
CONF = {"bootstrap.servers": os.environ["KAFKA_BOOTSTRAP"]}


def fresh_events(n, **kw):
    return list(generator.events(n, seed=random.randrange(10**9), **kw))


def publish(events):
    producer.send(Producer(CONF), events)


def landed(ids):
    with psycopg2.connect(os.environ["WAREHOUSE_URL"]) as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*), count(DISTINCT event_id) FROM raw.order_events "
                    "WHERE event_id = ANY(%s::uuid[])", (list(ids),))
        return cur.fetchone()


def wait_for(check, timeout=120):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if check():
            return True
        time.sleep(0.5)
    return False


def start_consumer():
    env = {**os.environ, "OTEL_SDK_DISABLED": "true", "PYTHONPATH": str(ROOT)}
    return subprocess.Popen([sys.executable, "-m", "ingest.consumer"], cwd=ROOT, env=env,
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


@pytest.fixture
def loader():
    producer.ensure_topics(CONF)
    proc = start_consumer()
    yield proc
    proc.kill()
    proc.wait()


def dlq_raws(wanted, timeout=60):
    """Read orders.dlq.v1 from the start and return the raw payloads that are in `wanted`."""
    kc = Consumer({**CONF, "group.id": f"test-{uuid.uuid4()}", "auto.offset.reset": "earliest"})
    kc.assign([TopicPartition(producer.DLQ_TOPIC, 0, 0)])
    found: dict[str, dict] = {}
    deadline = time.time() + timeout
    while set(found) != wanted and time.time() < deadline:
        for m in kc.consume(500, 1.0):
            if not m.error():
                record = json.loads(m.value())
                if record["raw"] in wanted:
                    found[record["raw"]] = record
    kc.close()
    return found


def test_duplicates_are_rejected(loader):
    events = fresh_events(300, dup_rate=0.3)
    ids = {e["event_id"] for e in events}
    assert len(ids) < len(events)
    publish(events)
    publish(events)  # the whole batch again
    assert wait_for(lambda: landed(ids) == (len(ids), len(ids)))
    time.sleep(3)
    assert landed(ids) == (len(ids), len(ids))


def test_malformed_events_go_to_dlq_and_replay_fixes(loader):
    events = fresh_events(200, bad_rate=0.2)
    bad = [e for e in events if consumer.validate(json.dumps(e))[0] is None]
    good_ids = {e["event_id"] for e in events if e not in bad}
    fixable = {**fresh_events(1)[0], "currency": "usd"}
    publish(events + [fixable])
    wanted = {json.dumps(e) for e in bad + [fixable]}
    found = dlq_raws(wanted)
    assert set(found) == wanted
    assert all(r["error"] and r["failed_at"].endswith("Z") for r in found.values())
    assert wait_for(lambda: landed(good_ids) == (len(good_ids), len(good_ids)))
    bad_uuids = [e["event_id"] for e in bad if e["event_id"] != "bad"] + [fixable["event_id"]]
    assert landed(bad_uuids)[0] == 0

    kc = Consumer({**CONF, "group.id": "orders-dlq-replay", "enable.auto.commit": False})
    result = replay_dlq.replay(kc, Producer(CONF), lambda e: {**e, "currency": str(e["currency"]).upper()})
    kc.close()
    assert result["replayed"] >= 1
    assert wait_for(lambda: landed([fixable["event_id"]])[0] == 1)


def test_kill_and_restart_loses_nothing_and_adds_no_duplicates():
    producer.ensure_topics(CONF)
    events = fresh_events(20000)
    ids = {e["event_id"] for e in events}
    proc = start_consumer()
    try:
        publish(events)
        assert wait_for(lambda: landed(ids)[0] > 0)
        proc.kill()  # hard kill mid stream, no clean shutdown
        proc.wait()
        partial = landed(ids)[0]
        assert 0 < partial < len(ids), "consumer finished before the kill; raise the event count"
        proc = start_consumer()
        assert wait_for(lambda: landed(ids)[0] == len(ids), timeout=180)
        time.sleep(3)
        assert landed(ids) == (len(ids), len(ids))
    finally:
        proc.kill()
        proc.wait()
