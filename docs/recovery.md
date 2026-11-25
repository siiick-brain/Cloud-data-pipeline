# Recovery drills

Run with `make drills` on a stack that already ran `make demo`. Each drill sends event ids the pipeline has never seen (fresh generator seeds) and checks exact counts.
Numbers below are from the run on 2026-11-25 on a Windows 11 laptop with Docker Desktop (8 CPUs, 16 GB), with the 100k demo already loaded.

| Drill | Result | Numbers |
|---|---|---|
| 1. Kill the consumer mid batch | pass | 5,500 of 50,000 events had landed at the SIGKILL. After a restart all 50,000 landed, each once. Caught up 85 s after the restart. |
| 2. Stop PostgreSQL for 60 s | pass | 20,000 of 20,000 events landed. The consumer caught up 66 s after PostgreSQL came back and every service was healthy again after 103 s. Nobody restarted anything by hand. |
| 3. Fix a schema problem and replay the DLQ | pass | 500 events with a lowercase currency all went to the DLQ (0 landed). Replay with the uppercase fix landed all 500. A second replay landed 0 new rows. |
| 4. Break a Delta table and restore it | pass | silver.order_lines had 96,958 rows. A delete left 0. `restoreToVersion` brought back 96,958 rows in 63 s. |

## Why each one holds

1. **No lost events.** The consumer commits Kafka offsets only after the PostgreSQL insert commits. A kill before the offset commit means the batch is read again.
   **No duplicates.** The raw table's primary key is `event_id` and the insert uses `ON CONFLICT DO NOTHING`, so a replayed batch adds nothing. Redis dedupe stops most repeats before the insert.
2. The consumer retries its database connection with backoff instead of exiting. Every compose service has `restart: unless-stopped`, so the Airflow scheduler restarts on its own after it loses its metadata database.
3. Bad events keep the original payload in the DLQ record. `python -m ingest.replay_dlq --uppercase-currency` applies a fix, validates again and republishes only events that now pass. The primary key makes a second replay harmless.
4. Delta keeps every table version in its transaction log. The drill reads the last good version from `history()` and calls `restoreToVersion`. The pipeline's own time travel demo is `spark/time_travel.py`.

## What the first run taught me

The first run of drill 1 reported 6,500 of 50,000 landed. A direct count showed all 50,000 had landed with no duplicates. The bug was in the drill script: it read two equal counts while the restarted consumer was still starting and stopped too early. The script now waits for the consumer health check and three equal counts before it reports.
