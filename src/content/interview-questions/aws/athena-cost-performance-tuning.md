---
publishedDate: "2026-10-16"
updatedDate: "2026-10-16"
reviewedDate: "2026-10-16"
title: "How do you reduce Amazon Athena query cost and make queries faster?"
seoTitle: "Athena Cost and Performance Tuning: Interview"
description: "Athena cost and performance tuning: cut bytes scanned with Parquet, partitions, projection and compaction, then use workgroup limits, result reuse and reservations."
technology: ["aws"]
topic: ["athena", "query-optimization", "cost-optimization", "partitioning"]
difficulty: "Medium"
questionType: ["optimization", "scenario"]
estimatedMinutes: 10
interviewRelevance: "High"
shortAnswer: "On per-query billing Athena charges for bytes scanned (rounded up to the megabyte, 10 MB minimum per query), so cost and speed both come from reading less. Store data as compressed Parquet or ORC (or Iceberg) so queries read only the needed columns and skip row groups; partition by the columns queries filter on and use partition projection; keep files around 128 MB or larger by compacting small files; select only required columns and filter on partition columns directly. Then govern it: workgroups with per-query and per-workgroup data limits, query result reuse for repeated dashboards, CTAS or materialised summary tables for hot aggregations, and capacity reservations when concurrency, not bytes, is the problem."
followUps: ["How does partition projection work and when is it better than the Glue catalogue partitions?", "Why can a query with a WHERE on a date still scan the whole table?", "When would you move a workload from Athena to Redshift?", "How do you compact an Iceberg table in Athena?"]
related: ["articles:aws/athena", "interview-questions:aws/s3-data-lake-layout", "articles:data-warehousing/partitioning-clustering-data-layout", "articles:delta-lake/parquet-vs-avro-vs-orc"]
versionContext: "Billing rules, result reuse limits, capacity reservation limits and query advice checked against the Amazon Athena User Guide (engine version 3) and the Athena pricing page in October 2026. Athena SQL needs an AWS account and was written from the documentation, not executed. The scan and billing model runs on Python 3.11 with illustrative table sizes and compression ratios."
sources:
  - { label: "Amazon Athena pricing", url: "https://aws.amazon.com/athena/pricing/" }
  - { label: "Amazon Athena: Optimize queries", url: "https://docs.aws.amazon.com/athena/latest/ug/performance-tuning-query-optimization-techniques.html" }
  - { label: "Amazon Athena: Partition projection", url: "https://docs.aws.amazon.com/athena/latest/ug/partition-projection.html" }
  - { label: "Amazon Athena: Reuse query results", url: "https://docs.aws.amazon.com/athena/latest/ug/reusing-query-results.html" }
  - { label: "Amazon Athena: Per-query and per-workgroup data usage controls", url: "https://docs.aws.amazon.com/athena/latest/ug/workgroups-setting-control-limits-cloudwatch.html" }
  - { label: "Amazon Athena: Manage query processing capacity", url: "https://docs.aws.amazon.com/athena/latest/ug/capacity-management.html" }
---

## Detailed explanation

Athena (engine version 3, based on Trino) reads files from S3 at query time. Unless you use capacity reservations, you are billed for the **bytes it reads**, so the order of work is: read fewer bytes, read them efficiently, then control who can run what.

### 1. Read fewer bytes

| Lever | Why it helps | How |
|---|---|---|
| Columnar format | Reads only referenced columns; row-group min/max statistics skip data | Parquet or ORC with Snappy or ZSTD; convert raw JSON/CSV with CTAS or a Glue job |
| Partitioning | Prunes whole S3 prefixes | Partition by the dominant filter (usually date); filter on the partition column itself |
| Partition projection | Athena computes partition locations from rules instead of looking them up in the catalogue | `projection.enabled` table properties for date, integer, enum or injected keys |
| Fewer, larger files | Less per-file overhead and S3 request time | Target roughly 128 MB or more per file; compact streaming output; `OPTIMIZE ... REWRITE DATA USING BIN_PACK` on Iceberg |
| Select less | `SELECT *` on Parquet still reads every column | List the columns; push filters into subqueries before joins |
| Pre-aggregate | Dashboards hit a small table instead of raw events | CTAS, scheduled `INSERT INTO`, or Iceberg summary tables |

### 2. Make the engine's work cheaper

- **Joins**: in a distributed hash join the right side is the build side and must fit in memory, so put the larger table on the left unless Glue column statistics are available for the cost-based optimizer to reorder joins.
- **Top N**: use `ORDER BY ... LIMIT` instead of `row_number()` over the whole table; avoid a final `ORDER BY` you do not need.
- **Approximations**: `approx_distinct`, `approx_percentile` instead of exact counts when a small error is acceptable.
- **Deduplicate with `max_by`/`min_by`** instead of window functions where possible.
- **Large results**: `UNLOAD` to Parquet in S3 instead of streaming millions of rows through the result set.

### 3. Govern and reuse

- **Workgroups** separate teams and enforce settings: result location and encryption, the engine version, a **per-query data scanned limit** (the query is cancelled when it exceeds it) and **per-workgroup limits** that trigger alarms or actions.
- **Query result reuse** returns a stored result when the same query text runs again within a maximum age (default 60 minutes, up to 7 days); good for dashboards over data that changes daily.
- **Capacity reservations** give dedicated DPUs (minimum 4 per reservation) billed per DPU-hour instead of per byte. Use them when queueing and concurrency, not bytes, are the problem, or when you need predictable spend.

## Example

Convert raw JSON into partitioned Parquet with CTAS, then query with pruning:

<!-- noexec -->
```sql
CREATE TABLE analytics.events_parquet
WITH (
  format = 'PARQUET',
  write_compression = 'ZSTD',
  external_location = 's3://acme-lake-curated/events_parquet/',
  partitioned_by = ARRAY['dt']
) AS
SELECT event_id, user_id, event_type, amount, event_ts,
       date_format(event_ts, '%Y-%m-%d') AS dt          -- partition column must be last
FROM raw.events_json
WHERE event_ts >= TIMESTAMP '2026-10-01 00:00:00';

-- Prunes to one partition and reads three columns
SELECT event_type, count(*) AS events, sum(amount) AS revenue
FROM analytics.events_parquet
WHERE dt = '2026-10-15'
GROUP BY event_type;

-- Does not prune on a Hive-style table: the filter is on a derived expression, not dt
SELECT count(*) FROM analytics.events_parquet
WHERE date(event_ts) = DATE '2026-10-15';
```

A CTAS statement can write at most 100 partitions; load wider date ranges with further `INSERT INTO` statements. The billing model in Python, with illustrative sizes (a 2 TB raw JSON table, 30 days, 12 columns):

```python
import math

MB = 1024 ** 2

def billed_mb(bytes_scanned):
    """Per-query billing: round up to the MB, with a 10 MB minimum."""
    return max(10, math.ceil(bytes_scanned / MB))

raw_json_bytes = 2 * 1024 ** 4                  # 2 TB of JSON for 30 days
parquet_bytes = raw_json_bytes / 6              # illustrative compression ratio for Parquet with ZSTD
columns_total, columns_read = 12, 3
days_total, days_read = 30, 1

scenarios = {
    "JSON, no partitions": raw_json_bytes,
    "Parquet, no partitions": parquet_bytes * columns_read / columns_total,
    "Parquet, one dt partition": parquet_bytes * columns_read / columns_total * days_read / days_total,
    "tiny lookup query": 300 * 1024,             # 300 KB is still billed as 10 MB
}
for name, scanned in scenarios.items():
    print(f"{name:27} scanned={scanned / MB:>12,.1f} MB  billed={billed_mb(scanned):>9,} MB")
```

```text
JSON, no partitions         scanned= 2,097,152.0 MB  billed=2,097,152 MB
Parquet, no partitions      scanned=    87,381.3 MB  billed=   87,382 MB
Parquet, one dt partition   scanned=     2,912.7 MB  billed=    2,913 MB
tiny lookup query           scanned=         0.3 MB  billed=       10 MB
```

The same question costs about 700 times less in bytes scanned after conversion and pruning. Multiply billed megabytes by the regional per-TB rate on the pricing page for an estimate. Many tiny queries (for example a BI tool issuing hundreds of lookups) are dominated by the 10 MB minimum, so result reuse or a small cached table helps there.

## Trade-offs and pitfalls

- **Partitioning on a high-cardinality column** creates many tiny files and slows planning more than it saves.
- **Small files from Firehose or streaming jobs**: scans get slower even when bytes are low. Compact closed partitions.
- **Crawlers as the partition registration mechanism**: slow and costly at scale; projection or the writing job is better.
- **Result reuse on changing data**: a reused result can be stale up to the maximum age you set; keep the age shorter than the data refresh interval.
- **Capacity reservations left idle** cost money whether queries run or not; size them to concurrency and remove them when not needed.
- **Over-tuning SQL before fixing layout**: format, partitions and file sizes usually dwarf query rewrites.

## Follow-up answers

- **Partition projection** computes partitions from table properties (for example `projection.dt.type = date` with a range), so new partitions are queryable without `ALTER TABLE ADD PARTITION` or crawlers and planning avoids catalogue calls. It fits predictable keys; arbitrary values still need the catalogue.
- **Full scan despite a date filter**: the filter is on a non-partition column or an expression of it, the partition column has a different type or format than the literal, or the table is not partitioned the way you think. Check `EXPLAIN` and the scanned bytes in the query statistics.
- **Move to Redshift** when many concurrent BI users run the same joins all day, when you need sub-second dashboards, or when frequent updates make a warehouse with sort keys and materialised views cheaper than rescanning S3.
- **Iceberg compaction in Athena**: `OPTIMIZE table REWRITE DATA USING BIN_PACK WHERE dt < '2026-10-01'` rewrites small files, and `VACUUM` expires old snapshots and removes orphan files.
