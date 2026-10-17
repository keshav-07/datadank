---
publishedDate: "2026-10-16"
updatedDate: "2026-10-16"
reviewedDate: "2026-10-16"
title: "How do you build reliable event-driven ingestion from S3 when events can be duplicated or late?"
seoTitle: "S3 Event-Driven Ingestion and Idempotency"
description: "S3 event-driven ingestion: S3 notifications vs EventBridge, at-least-once delivery, duplicates and ordering, and an idempotent file manifest you can run in SQL."
technology: ["aws"]
topic: ["s3", "event-driven", "idempotency", "eventbridge", "ingestion"]
difficulty: "Medium"
questionType: ["scenario", "architecture"]
estimatedMinutes: 12
interviewRelevance: "High"
shortAnswer: "S3 emits an event when an object is created or deleted, either directly to SQS, SNS or Lambda (event notifications) or to EventBridge for richer filtering and many targets. Delivery is at least once, usually within seconds but sometimes a minute or more, and not in order, so design for duplicates, delays and the occasional missed run. Put an SQS queue between S3 and the processor for buffering, retries and a dead-letter queue. Make processing idempotent: record each file (bucket, key, version or ETag) in a manifest with a conditional write, derive output locations from the input key, and load downstream with MERGE on a business key. Use the sequencer field to order events for the same key, and run a periodic reconciliation (listing or S3 Inventory) to catch anything missed."
followUps: ["When would you choose EventBridge over direct S3 event notifications?", "How do you avoid processing a file before the upload is complete?", "What happens if two events arrive for the same key out of order?", "How do you backfill files that landed before the notification was configured?"]
related: ["articles:aws/s3-for-data-engineers", "articles:aws/lambda-for-data-pipelines", "articles:aws/cloudwatch-monitoring", "articles:etl-elt/idempotency-in-data-pipelines", "system-designs:event-driven-architecture"]
versionContext: "Delivery guarantees, the sequencer field, filter overlap rules and EventBridge behaviour checked against the Amazon S3 User Guide in October 2026. The ordering and dedup model runs on Python 3.11 with simulated events, and the manifest and MERGE load run on PostgreSQL 16 as a stand-in for DynamoDB and the warehouse. The CLI configuration was written from the documentation and not executed."
sources:
  - { label: "Amazon S3 User Guide: Amazon S3 Event Notifications", url: "https://docs.aws.amazon.com/AmazonS3/latest/userguide/EventNotifications.html" }
  - { label: "Amazon S3 User Guide: Event message structure (sequencer)", url: "https://docs.aws.amazon.com/AmazonS3/latest/userguide/notification-content-structure.html" }
  - { label: "Amazon S3 User Guide: Using EventBridge", url: "https://docs.aws.amazon.com/AmazonS3/latest/userguide/EventBridge.html" }
  - { label: "Amazon S3 User Guide: Configuring notifications with object key name filtering", url: "https://docs.aws.amazon.com/AmazonS3/latest/userguide/notification-how-to-filtering.html" }
---

## Detailed explanation

### Two ways to get events out of S3

| | S3 event notifications | Amazon EventBridge |
|---|---|---|
| Targets | SQS, SNS, Lambda | Any EventBridge target (SQS, Lambda, Step Functions, Kinesis, API destinations, other buses and accounts) |
| Filtering | Event type plus one key prefix and suffix per rule; rules for the same event type cannot overlap | Content filtering on any field (key patterns, size, source IP, requester) |
| Setup | Per-bucket notification configuration | Enable EventBridge on the bucket once; all event types are sent, rules choose what to route |
| Extras | Lowest moving parts | Archive and replay, many rules per event, cross-account buses |

Both deliver **at least once**, normally within seconds but occasionally after a minute or longer, and **without ordering guarantees**.

### A robust shape

```text
S3 (raw/) --event--> SQS queue (+ DLQ) --> processor (Lambda, or a scheduled batch reading the queue)
                                              |-- manifest: conditional insert per (bucket, key, version)
                                              |-- output written to a key derived from the input key
                                              `-- downstream MERGE on a business key
reconciliation job (hourly or daily): list or S3 Inventory vs manifest -> enqueue anything missing
```

- **SQS in the middle** absorbs bursts, retries failures with a visibility timeout, and moves poison files to a dead-letter queue instead of losing them.
- **Manifest table** (DynamoDB with a condition expression, or a warehouse table) records each file once. A duplicate event finds the row and stops.
- **Deterministic outputs**: write `curated/orders/dt=.../<input-file-name>.parquet`, so a rerun replaces rather than adds.
- **Downstream merge**: even if a file is processed twice, `MERGE` on `order_id` keeps one row per order.
- **Ordering**: for the same key, compare the event's `sequencer` (hexadecimal; larger is later, comparable only for the same key) and ignore older events. Across keys there is no order; partition and event-time logic should not depend on arrival order.
- **Reconciliation**: notifications can be misconfigured, deleted or delayed, and files written before the configuration never produced events. A periodic comparison catches them.

## Example

Ordering and duplicate handling for events on the same key. Sequencers have different lengths, so pad them before comparing:

```python
events = [
    {"key": "raw/orders/2026-10-15/a.json", "event": "ObjectCreated:Put", "sequencer": "0066A1B2C3D4E5F601"},
    {"key": "raw/orders/2026-10-15/a.json", "event": "ObjectCreated:Put", "sequencer": "0066A1B2C3D4E5F601"},  # duplicate
    {"key": "raw/orders/2026-10-15/b.json", "event": "ObjectCreated:Put", "sequencer": "0066A1B2C3D4E5F700"},
    {"key": "raw/orders/2026-10-15/a.json", "event": "ObjectRemoved:Delete", "sequencer": "0066A1B2C3D4E5F5FF"},  # older, arrives late
]

def seq_value(sequencer, width=32):
    return int(sequencer.rjust(width, "0"), 16)

latest = {}          # key -> highest sequencer seen
actions = []
for e in events:
    seen = latest.get(e["key"])
    if seen is not None and seq_value(e["sequencer"]) <= seen:
        actions.append(f"skip   {e['event']:22} {e['key']} (duplicate or older)")
        continue
    latest[e["key"]] = seq_value(e["sequencer"])
    actions.append(f"apply  {e['event']:22} {e['key']}")
print("\n".join(actions))
```

```text
apply  ObjectCreated:Put      raw/orders/2026-10-15/a.json
skip   ObjectCreated:Put      raw/orders/2026-10-15/a.json (duplicate or older)
apply  ObjectCreated:Put      raw/orders/2026-10-15/b.json
skip   ObjectRemoved:Delete   raw/orders/2026-10-15/a.json (duplicate or older)
```

The late delete happened before the latest put, so applying it would wrongly remove a live file from the index. The manifest and an idempotent load, here in PostgreSQL as a stand-in for DynamoDB and the warehouse:

```sql
CREATE TABLE file_manifest (
  bucket      text NOT NULL,
  object_key  text NOT NULL,
  version_id  text NOT NULL,          -- or the ETag when versioning is off
  status      text NOT NULL DEFAULT 'processing',
  PRIMARY KEY (bucket, object_key, version_id)
);

CREATE TABLE orders (order_id bigint PRIMARY KEY, amount numeric(12,2), updated_at timestamptz);
CREATE TABLE orders_staging (order_id bigint, amount numeric(12,2), updated_at timestamptz);

-- Event 1 and its duplicate: only the first insert claims the file
INSERT INTO file_manifest (bucket, object_key, version_id)
VALUES ('acme-raw', 'orders/2026-10-15/a.json', 'v1')
ON CONFLICT DO NOTHING
RETURNING object_key;

INSERT INTO file_manifest (bucket, object_key, version_id)
VALUES ('acme-raw', 'orders/2026-10-15/a.json', 'v1')
ON CONFLICT DO NOTHING
RETURNING object_key;

-- The file contains an order twice (a retry by the producer) and an update
INSERT INTO orders_staging VALUES
  (101, 25.00, '2026-10-15 09:00+00'),
  (101, 25.00, '2026-10-15 09:00+00'),
  (102, 40.00, '2026-10-15 09:05+00');

MERGE INTO orders AS t
USING (SELECT DISTINCT ON (order_id) order_id, amount, updated_at
       FROM orders_staging ORDER BY order_id, updated_at DESC) AS s
ON t.order_id = s.order_id
WHEN MATCHED AND s.updated_at > t.updated_at THEN
  UPDATE SET amount = s.amount, updated_at = s.updated_at
WHEN NOT MATCHED THEN
  INSERT (order_id, amount, updated_at) VALUES (s.order_id, s.amount, s.updated_at);

-- Reprocessing the same file again changes nothing
MERGE INTO orders AS t
USING (SELECT DISTINCT ON (order_id) order_id, amount, updated_at
       FROM orders_staging ORDER BY order_id, updated_at DESC) AS s
ON t.order_id = s.order_id
WHEN MATCHED AND s.updated_at > t.updated_at THEN
  UPDATE SET amount = s.amount, updated_at = s.updated_at
WHEN NOT MATCHED THEN
  INSERT (order_id, amount, updated_at) VALUES (s.order_id, s.amount, s.updated_at);

UPDATE file_manifest SET status = 'done'
WHERE bucket = 'acme-raw' AND object_key = 'orders/2026-10-15/a.json' AND version_id = 'v1';

SELECT order_id, amount FROM orders ORDER BY order_id;
SELECT object_key, status FROM file_manifest;
```

The first manifest insert returns the key and the second returns no row, so the duplicate event stops there. `orders` ends with two rows (101 and 102) however many times the file is processed. Configuring the bucket to send `.json` files under `raw/orders/` to a queue:

<!-- noexec -->
```bash
aws s3api put-bucket-notification-configuration --bucket acme-raw \
  --notification-configuration '{
    "QueueConfigurations": [{
      "QueueArn": "arn:aws:sqs:eu-west-1:111122223333:orders-landed",
      "Events": ["s3:ObjectCreated:*"],
      "Filter": {"Key": {"FilterRules": [
        {"Name": "prefix", "Value": "raw/orders/"},
        {"Name": "suffix", "Value": ".json"}
      ]}}
    }]
  }'
```

The queue's access policy must allow `s3.amazonaws.com` to send messages, with `aws:SourceArn` set to the bucket ARN and `aws:SourceAccount` to the bucket owner.

## Trade-offs and pitfalls

- **Assuming exactly-once delivery**: duplicates are rare but documented; without a manifest or merge, they become duplicate rows.
- **Recursive writes**: a processor triggered on `raw/` that writes back to `raw/` triggers itself. Write outputs elsewhere.
- **Partial uploads**: an object is visible and the event fires only when the PUT or multipart upload completes, so a single object is safe; a dataset made of many files is not complete just because one file arrived. Use a `_SUCCESS` marker or manifest file from the producer and trigger on that.
- **Overlapping filter rules** for the same event type are rejected; plan prefixes per consumer, or use EventBridge rules when many consumers need overlapping subsets.
- **Lambda directly on S3 events at high volume**: no buffer and limited retries; SQS in front gives backpressure and a DLQ.
- **Relying on events alone**: without reconciliation, a missed or misrouted event is never noticed.

## Follow-up answers

- **EventBridge over direct notifications**: when several consumers need different or overlapping subsets, when you need content filtering beyond prefix and suffix, archive and replay, cross-account routing, or Step Functions as a direct target.
- **Incomplete uploads**: S3 objects are atomic, so the event means that object is complete; for multi-file datasets trigger on a completion marker written last, or batch on a schedule.
- **Out-of-order events for one key**: keep the highest `sequencer` per key and ignore anything lower; for the data itself, make loads use event time and business keys rather than arrival order.
- **Backfill**: list the prefix (or use S3 Inventory reports) for the missed period, compare with the manifest, and send the missing keys through the same queue so the same idempotent code processes them.
