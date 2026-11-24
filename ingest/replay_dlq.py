"""Re-validate orders.dlq.v1 records, optionally after a fix function, and republish good ones to orders.v1.

Reads the DLQ from its start to the current end without committing offsets, so a rerun replays again.
That is safe: the loader drops events it already landed.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

from confluent_kafka import Consumer, Producer, TopicPartition

from ingest.consumer import validate
from ingest.producer import DLQ_TOPIC, kafka_conf, send

Fix = Callable[[dict], dict]


def repair(value: bytes | None, fix: Fix | None) -> dict | None:
    try:
        raw = json.loads(value or b"")["raw"]
        if fix is not None:
            raw = json.dumps(fix(json.loads(raw)))
    except (ValueError, TypeError, KeyError, AttributeError):
        return None
    event, _ = validate(raw)
    return event


def replay(consumer: Any, producer: Any, fix: Fix | None = None) -> dict[str, int]:
    consumer.list_topics(DLQ_TOPIC, timeout=30)  # wait for a broker connection first
    low, high = consumer.get_watermark_offsets(TopicPartition(DLQ_TOPIC, 0), timeout=10)
    good: list[dict] = []
    bad = 0
    if high > low:
        consumer.assign([TopicPartition(DLQ_TOPIC, 0, low)])
        last = low - 1
        while last < high - 1:
            msgs = consumer.consume(num_messages=500, timeout=5.0)
            if not msgs:
                break
            for m in msgs:
                if m.error():
                    continue
                last = m.offset()
                event = repair(m.value(), fix)
                if event is None:
                    bad += 1
                else:
                    good.append(event)
    send(producer, good)
    return {"replayed": len(good), "still_bad": bad}


def main() -> None:
    conf = kafka_conf()
    consumer = Consumer({**conf, "group.id": "orders-dlq-replay", "enable.auto.commit": False})
    try:
        print(replay(consumer, Producer({**conf, "enable.idempotence": True})))
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
