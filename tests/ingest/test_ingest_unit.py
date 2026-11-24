"""Unit tests: producer, consumer and DLQ replay against in-memory fakes."""

from __future__ import annotations

import json
import os

import pytest

from ingest import consumer, generator, producer, replay_dlq, seed
from tests.ingest.fakes import FakeConn, FakeConsumer, FakeMsg, FakeProducer, FakeRedis


def good_event(**overrides):
    event = next(generator.events(1))
    event.update(overrides)
    return event


def msg(event, offset=0, partition=0):
    raw = json.dumps(event).encode() if isinstance(event, dict) else event
    key = event.get("event_id", "").encode() if isinstance(event, dict) else None
    return FakeMsg(raw, key=key, partition=partition, offset=offset)


@pytest.fixture
def parts():
    conn = FakeConn(dims={"raw.customers": {"C000001"}, "raw.products": set(), "raw.stores": set()})
    db = consumer.Db(lambda: conn, sleep=lambda s: None)
    return conn, db, FakeRedis(), FakeProducer()


# validation


def test_validate_accepts_generated_events():
    for event in generator.events(50):
        assert consumer.validate(json.dumps(event).encode()) == (event, None)


@pytest.mark.parametrize("raw", [b"not json", b"[1, 2]", b"", None, b"\xff\xfe"])
def test_validate_rejects_non_objects(raw):
    event, error = consumer.validate(raw)
    assert event is None and error


@pytest.mark.parametrize("field,value", [
    ("quantity", "bad"), ("currency", "usd"), ("event_id", "bad"), ("schema_version", 2), ("extra", 1),
])
def test_validate_rejects_contract_violations(field, value):
    event, error = consumer.validate(json.dumps(good_event(**{field: value})))
    assert event is None and error


# consumer batch


def test_batch_lands_valid_events_and_commits_after_db(parts):
    conn, db, r, dlq = parts
    events = list(generator.events(5))
    kc = FakeConsumer([])
    counts = consumer.process_batch([msg(e, i) for i, e in enumerate(events)], kc, dlq, r, db)
    assert counts == {"ok": 5, "duplicate": 0, "dead_letter": 0}
    assert set(conn.table) == {e["event_id"] for e in events}
    stored = conn.table[events[2]["event_id"]]
    assert json.loads(stored[1]) == events[2] and stored[3] == 2
    assert conn.commits == 1 and kc.commits == 1
    assert all(r.store[f"dedupe:{e['event_id']}"] == 1 for e in events)
    assert r.ttl[f"dedupe:{events[0]['event_id']}"] == 7 * 24 * 3600


def test_batch_rejects_duplicates_by_redis_and_primary_key(parts):
    conn, db, r, dlq = parts
    a, b, c = generator.events(3)
    kc = FakeConsumer([])
    consumer.process_batch([msg(a)], kc, dlq, r, db)
    r.store.clear()  # simulate a crash between insert and SET: the primary key still guards
    counts = consumer.process_batch([msg(a), msg(b), msg(b), msg(c)], kc, dlq, r, db)
    assert counts == {"ok": 2, "duplicate": 2, "dead_letter": 0}
    counts = consumer.process_batch([msg(c)], kc, dlq, r, db)  # caught by Redis, no DB call
    assert counts == {"ok": 0, "duplicate": 1, "dead_letter": 0}
    assert len(conn.table) == 3


def test_batch_sends_bad_events_to_dlq(parts):
    conn, db, r, dlq = parts
    bad = good_event(quantity="bad")
    kc = FakeConsumer([])
    counts = consumer.process_batch([msg(bad, offset=7), FakeMsg(b"{oops", offset=8)], kc, dlq, r, db)
    assert counts == {"ok": 0, "duplicate": 0, "dead_letter": 2}
    assert conn.table == {} and dlq.flushed == 1 and kc.commits == 1
    topic, key, value = dlq.sent[0]
    record = json.loads(value)
    assert topic == "orders.dlq.v1" and key == bad["event_id"].encode()
    assert set(record) == {"error", "raw", "failed_at", "source_offset"}
    assert record["source_offset"] == 7 and json.loads(record["raw"]) == bad
    assert record["failed_at"].endswith("Z")
    assert dlq.sent[1][1] is None


def test_batch_skips_kafka_errors_without_commit(parts):
    _, db, r, dlq = parts
    kc = FakeConsumer([])
    counts = consumer.process_batch([FakeMsg(None, error="broker down")], kc, dlq, r, db)
    assert counts == {"ok": 0, "duplicate": 0, "dead_letter": 0} and kc.commits == 0


def test_dimension_cache_is_filled_for_known_ids_only(parts):
    conn, db, r, dlq = parts
    event = good_event(customer_id="C000001", product_id="P99999", store_id="S999")
    consumer.process_batch([msg(event)], FakeConsumer([]), dlq, r, db)
    assert r.store["dim:customer:C000001"] == 1 and r.ttl["dim:customer:C000001"] == 3600
    assert "dim:product:P99999" not in r.store and "dim:store:S999" not in r.store
    assert event["event_id"] in conn.table  # unknown dimensions still land


def test_db_connection_loss_is_retried_with_backoff():
    conns = [FakeConn(fail_next=1), FakeConn()]
    delays: list[float] = []
    db = consumer.Db(lambda: conns.pop(0) if len(conns) > 1 else conns[0], sleep=delays.append)
    event = good_event()
    counts = consumer.process_batch([msg(event)], FakeConsumer([]), FakeProducer(), FakeRedis(), db)
    assert counts["ok"] == 1 and delays == [1]
    assert event["event_id"] in db.conn.table


def test_db_reconnect_backoff_grows_and_caps():
    delays: list[float] = []
    attempts = iter(range(10))

    def connect():
        if next(attempts) < 7:
            raise consumer.psycopg2.OperationalError("connection refused")
        return FakeConn()

    db = consumer.Db(connect, sleep=delays.append)
    assert db.run(lambda conn: "done") == "done"
    assert delays == [1, 2, 4, 8, 16, 30, 30]


def test_record_lag_reports_assigned_partitions():
    assert consumer.record_lag(FakeConsumer([], high=10)) == {("orders.v1", 0): 6}


def test_run_loop_touches_heartbeat_and_processes(parts, tmp_path, monkeypatch):
    conn, db, r, dlq = parts
    monkeypatch.setattr(consumer, "HEARTBEAT", tmp_path / "heartbeat")
    kc = FakeConsumer([[msg(e) for e in generator.events(3)], []])
    ticks = iter([False, False, True])
    consumer.run(kc, dlq, r, db, stop=lambda: next(ticks))
    assert (tmp_path / "heartbeat").exists()
    assert len(conn.table) == 3 and kc.subscribed == ["orders.v1"]


def test_consumer_main_wires_services(monkeypatch):
    seen = {}
    monkeypatch.setenv("KAFKA_BOOTSTRAP", "k:9092")
    monkeypatch.setenv("REDIS_URL", "redis://r:6379/0")
    monkeypatch.setenv("WAREHOUSE_URL", "postgresql://x")
    monkeypatch.setenv("OTEL_SDK_DISABLED", "true")
    monkeypatch.setattr(consumer, "Consumer", lambda conf: seen.setdefault("consumer", FakeConsumer([])))
    monkeypatch.setattr(consumer, "Producer", lambda conf: FakeProducer())
    monkeypatch.setattr(consumer, "ensure_topics", lambda conf: seen.setdefault("topics", conf))
    monkeypatch.setattr(consumer.redis.Redis, "from_url", staticmethod(lambda url: FakeRedis()))
    monkeypatch.setattr(consumer, "run", lambda kc, dlq, r, db, stop: seen.update(db=db, stop=stop()))
    consumer.main()
    assert seen["topics"]["bootstrap.servers"] == "k:9092"
    assert seen["stop"] is False and seen["consumer"].closed


def test_setup_telemetry_is_safe_to_call(monkeypatch):
    monkeypatch.setenv("OTEL_SDK_DISABLED", "true")
    consumer.setup_telemetry()


# producer


class FakeFuture:
    def __init__(self):
        self.done = False

    def result(self):
        self.done = True


class FakeAdmin:
    def __init__(self, existing):
        self.topics = {name: None for name in existing}
        self.created: list = []

    def list_topics(self, timeout=None):
        return self

    def create_topics(self, new_topics):
        self.created = new_topics
        return {t.topic: FakeFuture() for t in new_topics}


def test_ensure_topics_creates_only_missing(monkeypatch):
    admin = FakeAdmin(existing=["orders.v1"])
    monkeypatch.setattr(producer, "AdminClient", lambda conf: admin)
    assert producer.ensure_topics({"bootstrap.servers": "x"}) == ["orders.dlq.v1"]
    assert admin.created[0].num_partitions == 1
    admin = FakeAdmin(existing=["orders.v1", "orders.dlq.v1"])
    monkeypatch.setattr(producer, "AdminClient", lambda conf: admin)
    assert producer.ensure_topics({}) == [] and admin.created == []


def test_ensure_topics_partition_counts(monkeypatch):
    admin = FakeAdmin(existing=[])
    monkeypatch.setattr(producer, "AdminClient", lambda conf: admin)
    producer.ensure_topics({})
    assert {t.topic: t.num_partitions for t in admin.created} == {"orders.v1": 3, "orders.dlq.v1": 1}


def test_send_keys_by_event_id_and_survives_full_queue():
    p = FakeProducer(buffer_errors=1)
    events = list(generator.events(4, bad_rate=1.0))
    assert producer.send(p, iter(events)) == 4
    assert [k for _, k, _ in p.sent] == [str(e["event_id"]) for e in events]
    assert {t for t, _, _ in p.sent} == {"orders.v1"} and p.flushed == 1


def test_producer_main_parses_args(monkeypatch):
    seen = {}
    monkeypatch.setenv("KAFKA_BOOTSTRAP", "k:9092")
    monkeypatch.setattr(producer, "ensure_topics", lambda conf: [])
    monkeypatch.setattr(producer, "Producer", lambda conf: FakeProducer())
    monkeypatch.setattr(producer, "send", lambda p, events: seen.setdefault("events", list(events)) and 0)
    producer.main(["--events", "200", "--dup-rate", "0.5", "--bad-rate", "0.1"])
    ids = [e["event_id"] for e in seen["events"]]
    assert len(ids) == 200 and len(set(ids)) < 200


# DLQ replay


def dlq_record(raw, offset=0):
    value = json.dumps({"error": "x", "raw": raw, "failed_at": "2026-01-01T00:00:00Z", "source_offset": 0})
    return FakeMsg(value.encode(), offset=offset, topic="orders.dlq.v1")


def test_replay_republishes_fixed_events_only():
    fixable = good_event(currency="usd")
    hopeless = "{not json"
    kc = FakeConsumer([[dlq_record(json.dumps(fixable), 0), dlq_record(hopeless, 1)]], high=2)
    out = FakeProducer()

    def fix(event):
        return {**event, "currency": event["currency"].upper()}

    assert replay_dlq.replay(kc, out, fix) == {"replayed": 1, "still_bad": 1}
    topic, key, value = out.sent[0]
    assert topic == "orders.v1" and key == fixable["event_id"] and json.loads(value)["currency"] == "USD"
    assert kc.assigned[0].offset == 0 and out.flushed == 1


def test_replay_without_fix_and_empty_topic():
    event = good_event()
    kc = FakeConsumer([[dlq_record(json.dumps(event))]], high=1)
    assert replay_dlq.replay(kc, FakeProducer()) == {"replayed": 1, "still_bad": 0}
    assert replay_dlq.replay(FakeConsumer([], high=0), FakeProducer()) == {"replayed": 0, "still_bad": 0}


def test_replay_stops_when_topic_runs_dry():
    kc = FakeConsumer([[], [dlq_record("{")]], high=5)
    assert replay_dlq.replay(kc, FakeProducer()) == {"replayed": 0, "still_bad": 0}


def test_replay_treats_unreadable_dlq_records_as_bad():
    kc = FakeConsumer([[FakeMsg(b"garbage", offset=0), FakeMsg(None, error="x", offset=0)]], high=1)
    assert replay_dlq.replay(kc, FakeProducer(), lambda e: e) == {"replayed": 0, "still_bad": 1}


def test_replay_main(monkeypatch, capsys):
    monkeypatch.setenv("KAFKA_BOOTSTRAP", "k:9092")
    monkeypatch.setattr(replay_dlq, "Consumer", lambda conf: FakeConsumer([], high=0))
    monkeypatch.setattr(replay_dlq, "Producer", lambda conf: FakeProducer())
    replay_dlq.main()
    assert "replayed" in capsys.readouterr().out


# seed (existing module, kept covered)


def test_seed_inserts_all_master_data(monkeypatch):
    calls = []
    monkeypatch.setattr(seed, "execute_values", lambda cur, sql, values: calls.append((sql, values)))
    result = seed.seed(FakeSeedConn())
    assert result == {"raw.customers": 1000, "raw.products": 200, "raw.stores": 20}
    assert all("ON CONFLICT DO NOTHING" in sql for sql, _ in calls)


def test_seed_main(monkeypatch, capsys):
    monkeypatch.setenv("WAREHOUSE_URL", os.environ.get("WAREHOUSE_URL", "postgresql://x"))
    monkeypatch.setattr(seed, "execute_values", lambda cur, sql, values: None)
    monkeypatch.setattr(seed.psycopg2, "connect", lambda url: FakeSeedConn())
    seed.main()
    assert "raw.stores" in capsys.readouterr().out


class FakeSeedConn:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def cursor(self):
        return self
