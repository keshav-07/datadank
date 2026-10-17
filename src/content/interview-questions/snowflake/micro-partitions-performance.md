---
publishedDate: "2026-10-04"
updatedDate: "2026-10-17"
reviewedDate: "2026-10-17"
title: "How do micro-partitions affect Snowflake query performance?"
seoTitle: "Snowflake Micro-Partitions: Interview Answer"
description: "Interview answer: Snowflake prunes micro-partitions using per-column min/max metadata, so data layout and clustering decide how much a query must scan."
inventoryId: "INT-21"
technology: ["snowflake"]
topic: ["micro-partitions", "performance"]
difficulty: "Medium"
questionType: ["conceptual", "optimization"]
estimatedMinutes: 7
interviewRelevance: "High"
shortAnswer: "Snowflake stores each table as many small immutable micro-partitions and keeps metadata such as the minimum and maximum value of every column in each one. When a query filters on a column, Snowflake skips micro-partitions whose ranges cannot match, which is called pruning. So performance depends on data layout: if values of the filtered column are well clustered, most partitions are skipped; if they are scattered, the query scans almost everything, and a clustering key may help on very large tables."
followUps: ["How can you tell from the query profile whether pruning worked?", "When is a clustering key worth its cost?", "Why might a query on a well-clustered table still scan everything?"]
related: ["articles:snowflake/micro-partitions-clustering-pruning", "articles:data-warehousing/partitioning-clustering-data-layout", "interview-questions:snowflake/when-to-use-clustering-keys"]
versionContext: "Describes Snowflake behaviour as documented in October 2026. The Snowflake SQL is not executed here (noexec); the pruning model runs on plain Python 3 and imitates min/max pruning, not Snowflake internals."
sources:
  - { label: "Snowflake documentation: Micro-partitions and data clustering", url: "https://docs.snowflake.com/en/user-guide/tables-clustering-micropartitions" }
  - { label: "Snowflake documentation: QUERY_HISTORY view", url: "https://docs.snowflake.com/en/sql-reference/account-usage/query_history" }
---

## Detailed explanation

Every Snowflake table is split automatically into **micro-partitions**: immutable, columnar files of roughly 50 to 500 MB of uncompressed data, created in the order rows are loaded. For each column of each micro-partition, the cloud services layer keeps metadata such as the minimum, the maximum and the number of distinct values.

1. When a query filters on a column, the optimiser compares the filter with each micro-partition's min/max range **before** any data is read.
2. Micro-partitions whose range cannot contain a match are skipped (**pruned**); only the rest are sent to the warehouse, and only the referenced columns are read from them.
3. So the work a query does depends on **layout**: if the filtered column's values are well clustered (narrow, non-overlapping ranges per micro-partition), a selective filter reads a handful of partitions; if they are scattered, almost every range overlaps the filter and the query scans nearly everything, whatever the warehouse size.

Tables loaded over time are **naturally clustered** on their load-time columns, which is why date filters on event tables are usually fast and filters on, say, customer ID are not. A **clustering key** asks Automatic Clustering to keep a large table organised on other columns; the **search optimization service** is the tool for needle-in-a-haystack lookups instead.

## Example

How a late, unsorted backfill hurts pruning on a date column, and how sorting it before loading avoids most of the damage:

```python
from datetime import date, timedelta

def parts_from(rows, size=500):
    """Group rows (in load order) into fixed-size micro-partitions with min/max of the date column."""
    return [(min(c), max(c)) for c in (rows[i:i + size] for i in range(0, len(rows), size))]

def scanned(parts, lo, hi):
    """A range filter lo <= d <= hi must read every partition whose [min, max] overlaps it."""
    return sum(1 for mn, mx in parts if mn <= hi and mx >= lo)

start = date(2026, 1, 1)
daily = [start + timedelta(days=d) for d in range(60) for _ in range(250)]   # 60 days, loaded in order

# A backfill of the same 60 days arrives later, rows in random date order (here: round-robin by day)
backfill = [start + timedelta(days=i % 60) for i in range(6000)]

week = (date(2026, 2, 1), date(2026, 2, 7))
in_order = parts_from(daily)
after_backfill = parts_from(daily + backfill)
print("partitions:", len(in_order), "->", len(after_backfill))
print("one-week filter, daily loads only :", scanned(in_order, *week), "partitions scanned")
print("one-week filter, after backfill   :", scanned(after_backfill, *week), "partitions scanned")
sorted_backfill = parts_from(daily + sorted(backfill))
print("same backfill sorted before load  :", scanned(sorted_backfill, *week), "partitions scanned")
```

```text
partitions: 30 -> 42
one-week filter, daily loads only : 4 partitions scanned
one-week filter, after backfill   : 16 partitions scanned
same backfill sorted before load  : 6 partitions scanned
```

Every backfill partition holds dates from the whole range, so each one overlaps any week you filter on. Sorting the backfill (or rewriting the table with `INSERT OVERWRITE ... ORDER BY`) restores narrow ranges.

Checking pruning on a real account:

<!-- noexec -->
```sql
-- Snowflake SQL (not executed here)
-- Queries that read most of their table's partitions
SELECT query_id, LEFT(query_text, 80) AS query_start,
       partitions_scanned, partitions_total,
       ROUND(100 * partitions_scanned / NULLIF(partitions_total, 0)) AS pct_scanned
FROM snowflake.account_usage.query_history
WHERE start_time >= DATEADD('day', -7, CURRENT_TIMESTAMP())
  AND partitions_total > 10000
ORDER BY pct_scanned DESC, partitions_total DESC
LIMIT 20;

-- How well is the table clustered on the column you filter by?
SELECT SYSTEM$CLUSTERING_INFORMATION('analytics.events', '(TO_DATE(event_ts))');
```

In the query profile, the TableScan operator shows the same **Partitions scanned** and **Partitions total**.

## Why pruning might fail

- The filter wraps the column in a function or casts it (`WHERE TO_CHAR(event_ts, 'YYYY-MM-DD') = ...`); filter on the raw column with a range instead.
- Values are scattered: loads not ordered by the filtered column, backfills, or large `UPDATE`/`MERGE` statements that rewrite rows from many ranges into new micro-partitions.
- The filter is on a column of another table in a join, so the scanned table only prunes if Snowflake can derive a filter from the join.
- The filter is not selective: a predicate matching 40% of rows has to read about 40% of the data however the table is clustered.

## Trade-offs

- A clustering key improves pruning for its columns only, and reclustering costs credits (billed by data ingested on the newer Optima Clustering engine, by compute on Clustering Classic). It pays off on large tables queried far more than they change.
- Sorting data before loading is free and often enough for tables that are loaded once or in batches.
- Search optimization helps very selective equality, substring and `VARIANT` lookups that clustering cannot serve, at the cost of build and maintenance compute plus storage.

## Common mistakes

1. Clustering small or rarely queried tables.
2. Choosing a unique ID as the clustering key; use search optimization for ID lookups.
3. Scaling up the warehouse when the profile shows nearly every partition scanned.
4. Judging a table by `SYSTEM$CLUSTERING_INFORMATION` alone instead of partitions scanned for the queries that matter.
