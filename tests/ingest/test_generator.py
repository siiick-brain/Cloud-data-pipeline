"""Generated events end at the current time, so dashboards with a recent time range show them."""

from datetime import UTC, datetime, timedelta

from ingest import generator


def ts(event):
    return datetime.fromisoformat(event["occurred_at"].replace("Z", "+00:00"))


def test_events_end_near_now_and_ids_stay_seeded():
    first = list(generator.events(100))
    again = list(generator.events(100))
    assert [e["event_id"] for e in first] == [e["event_id"] for e in again]
    assert datetime.now(UTC) - ts(first[-1]) < timedelta(minutes=5)
    assert ts(first[0]) < ts(first[-1])


def test_explicit_start_is_kept():
    start = datetime(2026, 1, 1, tzinfo=UTC)
    assert ts(next(generator.events(1, start=start))) == start


def test_order_ids_never_collide_across_seeds():
    import json

    from ingest.consumer import validate

    a = {(e["order_id"], e["line_number"]) for e in generator.events(300, seed=1)}
    b = {(e["order_id"], e["line_number"]) for e in generator.events(300, seed=2)}
    assert a.isdisjoint(b)
    assert all(validate(json.dumps(e))[0] for e in generator.events(10, seed=999_999))
