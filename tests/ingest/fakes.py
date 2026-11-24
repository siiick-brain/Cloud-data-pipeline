"""In-memory stand-ins for Kafka, Redis and PostgreSQL used by the ingest unit tests."""

from __future__ import annotations

import psycopg2


class FakeMsg:
    def __init__(self, value, key=None, partition=0, offset=0, error=None, topic="orders.v1"):
        self._value, self._key, self._partition = value, key, partition
        self._offset, self._error, self._topic = offset, error, topic

    def value(self):
        return self._value

    def key(self):
        return self._key

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset

    def error(self):
        return self._error

    def topic(self):
        return self._topic


class FakeRedis:
    def __init__(self):
        self.store: dict[str, object] = {}
        self.ttl: dict[str, int] = {}
        self._queued: list = []

    def mget(self, keys):
        return [self.store.get(k) for k in keys]

    def set(self, key, value, nx=False, ex=None):
        if nx and key in self.store:
            return None
        self.store[key], self.ttl[key] = value, ex
        return True

    def pipeline(self):
        return self

    def execute(self):
        return []


class FakeCursor:
    def __init__(self, conn):
        self.conn = conn
        self.rows: list[tuple] = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        if self.conn.fail_next > 0:
            self.conn.fail_next -= 1
            self.conn.closed = 1
            raise psycopg2.OperationalError("server closed the connection unexpectedly")
        if sql.lstrip().startswith("INSERT INTO raw.order_events"):
            ids, payloads, partitions, offsets = params
            self.rows = []
            for row in zip(ids, payloads, partitions, offsets, strict=True):
                if row[0] not in self.conn.pending and row[0] not in self.conn.table:
                    self.conn.pending[row[0]] = row
                    self.rows.append((row[0],))
        else:
            table = sql.split("FROM ")[1].split()[0]
            self.rows = [(i,) for i in params[0] if i in self.conn.dims.get(table, set())]

    def fetchall(self):
        return self.rows


class FakeConn:
    def __init__(self, dims=None, fail_next=0):
        self.table: dict[str, tuple] = {}
        self.pending: dict[str, tuple] = {}
        self.dims = dims or {}
        self.fail_next = fail_next
        self.closed = 0
        self.commits = 0

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        self.table.update(self.pending)
        self.pending.clear()
        self.commits += 1

    def close(self):
        self.closed = 1


class FakeProducer:
    def __init__(self, buffer_errors=0):
        self.sent: list[tuple] = []
        self.flushed = 0
        self.buffer_errors = buffer_errors

    def produce(self, topic, value=None, key=None):
        if self.buffer_errors:
            self.buffer_errors -= 1
            raise BufferError("queue full")
        self.sent.append((topic, key, value))

    def poll(self, timeout=0):
        return 0

    def flush(self, timeout=None):
        self.flushed += 1
        return 0


class FakePartition:
    def __init__(self, topic, partition, offset=-1001):
        self.topic, self.partition, self.offset = topic, partition, offset


class FakeConsumer:
    def __init__(self, batches, high=10, stop_after=None):
        self.batches = list(batches)
        self.commits = 0
        self.closed = False
        self.subscribed: list[str] = []
        self.assigned: list = []
        self.high = high
        self.position_offset = 4

    def subscribe(self, topics):
        self.subscribed = topics

    def list_topics(self, topic=None, timeout=None):
        return self

    def assign(self, partitions):
        self.assigned = partitions

    def consume(self, num_messages=1, timeout=-1):
        return self.batches.pop(0) if self.batches else []

    def commit(self, asynchronous=True):
        self.commits += 1

    def assignment(self):
        return [FakePartition("orders.v1", 0), FakePartition("orders.v1", 1)]

    def get_watermark_offsets(self, tp, timeout=None, cached=False):
        return (0, self.high)

    def position(self, partitions):
        return [FakePartition(p.topic, p.partition, self.position_offset if p.partition == 0 else -1001)
                for p in partitions]

    def close(self):
        self.closed = True
