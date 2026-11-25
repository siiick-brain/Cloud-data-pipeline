"""Send generator events to orders.v1 keyed by event_id. Creates the topics if missing."""

from __future__ import annotations

import argparse
import json
import os
from collections.abc import Iterable

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from ingest import generator

TOPIC = "orders.v1"
DLQ_TOPIC = "orders.dlq.v1"
PARTITIONS = {TOPIC: 3, DLQ_TOPIC: 1}


def kafka_conf() -> dict:
    return {"bootstrap.servers": os.environ["KAFKA_BOOTSTRAP"]}


def ensure_topics(conf: dict) -> list[str]:
    admin = AdminClient(conf)
    existing = admin.list_topics(timeout=30).topics
    missing = [NewTopic(name, num_partitions=n, replication_factor=1)
               for name, n in PARTITIONS.items() if name not in existing]
    if missing:
        for future in admin.create_topics(missing).values():
            future.result()
    return [t.topic for t in missing]


def send(producer: Producer, events: Iterable[dict]) -> int:
    sent = 0
    for event in events:
        value, key = json.dumps(event), str(event.get("event_id"))
        while True:
            try:
                producer.produce(TOPIC, value=value, key=key)
                break
            except BufferError:
                producer.poll(1)
        producer.poll(0)
        sent += 1
    producer.flush()
    return sent


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--events", type=int, required=True)
    parser.add_argument("--dup-rate", type=float, default=0.0)
    parser.add_argument("--bad-rate", type=float, default=0.0)
    parser.add_argument("--seed", type=int, default=42, help="change it to get new event ids")
    parser.add_argument("--currency", default="USD", help="lowercase values fail the schema (DLQ drill)")
    args = parser.parse_args(argv)
    conf = kafka_conf()
    ensure_topics(conf)
    producer = Producer({**conf, "enable.idempotence": True, "linger.ms": 50})
    events = (
        {**e, "currency": args.currency} if e.get("currency") == "USD" else e
        for e in generator.events(args.events, seed=args.seed, dup_rate=args.dup_rate, bad_rate=args.bad_rate)
    )
    print(f"sent {send(producer, events)} events to {TOPIC}")


if __name__ == "__main__":
    main()
