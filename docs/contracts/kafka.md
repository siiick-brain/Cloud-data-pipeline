# Kafka and Redis contract

| Topic | Key | Value | Partitions |
|---|---|---|---|
| orders.v1 | event_id | order event JSON (order_event.schema.json) | 3 |
| orders.dlq.v1 | event_id or null | `{"error": str, "raw": str, "failed_at": UTC ISO 8601, "source_offset": int}` | 1 |

Consumer group: `orders-raw-loader`. Offsets are committed only after the raw insert commits.

Redis dedupe key: `dedupe:<event_id>`, value `1`, TTL 7 days, set with SET NX after the raw insert commits.
The raw table primary key on event_id is the final guard, so a crash between insert and SET never creates a duplicate.

Redis dimension cache keys: `dim:customer:<customer_id>`, `dim:product:<product_id>`, `dim:store:<store_id>`, value `1`, TTL 1 hour.
An event whose customer, product or store id is unknown is still landed. The warehouse load maps it to the -1 member.
