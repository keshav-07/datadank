---
publishedDate: "2026-10-16"
updatedDate: "2026-10-16"
reviewedDate: "2026-10-16"
title: "How do you choose Redshift distribution styles and sort keys?"
seoTitle: "Redshift Distribution and Sort Keys: Interview"
description: "Redshift distribution and sort keys: when to use AUTO, KEY, EVEN or ALL, compound vs interleaved sort keys, and spotting skew and redistribution."
technology: ["aws", "data-warehousing"]
topic: ["redshift", "distribution-keys", "sort-keys", "query-optimization"]
difficulty: "Medium"
questionType: ["optimization", "conceptual"]
estimatedMinutes: 10
interviewRelevance: "High"
shortAnswer: "Distribution decides which slice stores each row; sort keys decide the order rows are stored in each slice, so zone maps can skip blocks. Start with AUTO for both and let automatic table optimization act on the real workload. Override when you know better: distribute a large fact table and its biggest dimension on the shared join key (KEY) so joins are co-located, use ALL for small, slowly changing dimensions, and EVEN for tables that are not joined or have no good key. A KEY column needs many distinct, evenly spread values, or one slice holds most rows. Sort on the column most range filters use, usually a timestamp, as a compound key with the most filtered column first; interleaved keys are rarely worth their maintenance cost."
followUps: ["What does DS_BCAST_INNER in an EXPLAIN plan tell you?", "How do you detect and fix distribution skew?", "Does Redshift Serverless still use distribution and sort keys?", "Why can a sort key stop helping over time?"]
related: ["articles:aws/redshift", "articles:data-warehousing/star-schema", "articles:data-warehousing/partitioning-clustering-data-layout", "system-designs:cloud-data-warehouse-platform"]
versionContext: "Distribution styles, sort key types, automatic table optimization, SVV_TABLE_INFO columns and EXPLAIN redistribution labels checked against the Amazon Redshift Database Developer Guide in October 2026. Redshift SQL needs a cluster or Serverless workgroup and was written from the documentation, not executed. The slice distribution model runs on Python 3.11 with generated data."
sources:
  - { label: "Amazon Redshift: Working with data distribution styles", url: "https://docs.aws.amazon.com/redshift/latest/dg/c_choosing_dist_sort.html" }
  - { label: "Amazon Redshift: Working with sort keys", url: "https://docs.aws.amazon.com/redshift/latest/dg/t_Sorting_data.html" }
  - { label: "Amazon Redshift: Automatic table optimization", url: "https://docs.aws.amazon.com/redshift/latest/dg/t_Creating_tables.html" }
  - { label: "Amazon Redshift: Evaluating the query plan (redistribution labels)", url: "https://docs.aws.amazon.com/redshift/latest/dg/c_data_redistribution.html" }
  - { label: "Amazon Redshift: SVV_TABLE_INFO", url: "https://docs.aws.amazon.com/redshift/latest/dg/r_SVV_TABLE_INFO.html" }
---

## Detailed explanation

A Redshift cluster (or Serverless workgroup) splits each compute node into **slices**, and every table's rows are spread over those slices. Two physical design choices matter most.

### Distribution style: where rows live

| Style | Placement | Use for | Watch out for |
|---|---|---|---|
| `AUTO` (default) | Redshift picks ALL for small tables, then KEY or EVEN as they grow | Most tables, especially when you do not know the workload yet | Check what it chose: `diststyle` shows `AUTO(ALL)`, `AUTO(KEY(col))` or `AUTO(EVEN)` |
| `KEY` | Rows with the same key value go to the same slice | Large fact table and its largest joined dimension, on the join column | Skew if the key has few values or a dominant value (nulls, "unknown") |
| `ALL` | Full copy on every node | Small dimensions that rarely change | Multiplies storage and slows loads; never for large or frequently updated tables |
| `EVEN` | Round-robin | Tables not joined, or with no good key, staging tables | Joins to it need redistribution |

When two tables are joined on columns that are not both distribution keys, Redshift must move data at query time: it **broadcasts** the inner table to every node (`DS_BCAST_INNER`) or **redistributes** one or both sides (`DS_DIST_INNER`, `DS_DIST_BOTH`). Co-located joins show `DS_DIST_NONE` or, for an ALL dimension, `DS_DIST_ALL_NONE`.

### Sort keys: the order inside each slice

Redshift stores columns in 1 MB blocks and keeps min/max values per block (**zone maps**). A range filter on the leading sort key column lets it skip every block whose range cannot match.

- **`SORTKEY AUTO`** is the recommended default; automatic table optimization picks keys from observed queries.
- **Compound** (default type): columns in priority order; helps filters on a **prefix** of the key (the first column, or the first two). Put the most common range filter first, typically a date or timestamp.
- **Interleaved**: equal weight to up to eight columns, for filters on any one of them; needs `VACUUM REINDEX` and is costly to maintain on tables that are loaded often. Rarely the right answer today.

Sorting decays as new rows arrive unsorted; automatic vacuum sort and loading data in sort-key order (for example by date) keep `unsorted` low.

## Example

A star-schema design that co-locates the biggest join and sorts the fact table by time:

<!-- noexec -->
```sql
CREATE TABLE dim_customer (
  customer_id   BIGINT       NOT NULL,
  segment       VARCHAR(32),
  country_code  CHAR(2)
)
DISTSTYLE KEY DISTKEY (customer_id)
SORTKEY (customer_id);

CREATE TABLE dim_product (
  product_id  INT NOT NULL,
  category    VARCHAR(64)
)
DISTSTYLE ALL;                         -- small, rarely changes

CREATE TABLE fact_orders (
  order_id     BIGINT        NOT NULL,
  customer_id  BIGINT        NOT NULL,
  product_id   INT           NOT NULL,
  order_ts     TIMESTAMP     NOT NULL,
  amount       DECIMAL(12,2)
)
DISTSTYLE KEY DISTKEY (customer_id)    -- same key as dim_customer: co-located join
COMPOUND SORTKEY (order_ts);

-- Check what the planner does: look for DS_DIST_NONE and DS_DIST_ALL_NONE, not DS_BCAST_INNER
EXPLAIN
SELECT c.segment, p.category, sum(f.amount)
FROM fact_orders f
JOIN dim_customer c ON c.customer_id = f.customer_id
JOIN dim_product  p ON p.product_id  = f.product_id
WHERE f.order_ts >= '2026-10-01'
GROUP BY 1, 2;

-- Skew, unsorted percentage and the style Redshift actually applied
SELECT "table", diststyle, sortkey1, skew_rows, unsorted, stats_off, tbl_rows
FROM svv_table_info
ORDER BY skew_rows DESC NULLS LAST;
```

Why the KEY column must have many evenly spread values: this model hashes rows to 16 slices for three candidate keys of the same fact table and reports `skew_rows`-style ratios (largest slice divided by smallest).

```python
import random
from collections import Counter
from zlib import crc32

random.seed(7)
SLICES = 16
rows = []
for _ in range(200_000):
    customer = random.randint(1, 50_000)
    country = random.choices(["US", "IN", "GB", "DE", "BR"], weights=[60, 20, 10, 5, 5])[0]
    status = random.choices(["complete", "refunded", "pending"], weights=[90, 7, 3])[0]
    rows.append((customer, country, status))

def slice_counts(values):
    counts = Counter(crc32(str(v).encode()) % SLICES for v in values)
    return [counts.get(s, 0) for s in range(SLICES)]

for name, idx in [("customer_id", 0), ("country_code", 1), ("status", 2)]:
    counts = slice_counts(row[idx] for row in rows)
    used = [c for c in counts if c]
    ratio = max(counts) / min(used)
    print(f"{name:12} slices used={len(used):2}/16  largest/smallest={ratio:7.2f}  "
          f"largest slice holds {max(counts) / len(rows):.0%} of rows")
```

```text
customer_id  slices used=16/16  largest/smallest=   1.04  largest slice holds 6% of rows
country_code slices used= 3/16  largest/smallest=   4.36  largest slice holds 65% of rows
status       slices used= 3/16  largest/smallest=  29.62  largest slice holds 90% of rows
```

With `country_code` as the distribution key, five values land on only three slices (some countries hash to the same slice), thirteen slices sit idle and one slice does about 65% of the work for every scan and join. `customer_id` spreads evenly, and if the main join is on it, the join is also co-located.

## Trade-offs and pitfalls

- **Optimising for one query**: a distribution key helps joins on that key only; other joins on the same table still redistribute. Pick the key of the largest, most frequent join.
- **Low-cardinality or null-heavy keys** cause skew; check `skew_rows` after loading real data.
- **ALL on a growing table**: loads and storage multiply by the number of nodes; switch to KEY or AUTO when it grows.
- **Filtering on a function of the sort key** (`date_trunc('day', order_ts)` compared with a value) can defeat zone-map pruning; compare the raw column with a range.
- **Leaving interleaved keys on hot tables** makes vacuum slow; convert them to compound or AUTO.
- **Forgetting statistics**: stale `stats_off` misleads the planner as much as bad keys; automatic analyze usually handles it, but check after big loads.

## Follow-up answers

- **`DS_BCAST_INNER`**: the inner table of a join is copied to every node because it is not distributed on the join key. Fine for a tiny table, expensive for a large one; fix with a matching KEY, ALL for small dimensions, or by letting AUTO choose.
- **Detecting skew**: `svv_table_info.skew_rows` (ratio of the fullest to emptiest slice), per-slice rows in `svv_diskusage`, and uneven step times in `svl_query_summary` or the query profiler. Fix by choosing a higher-cardinality key, handling null or default keys, or using EVEN.
- **Redshift Serverless**: tables still have distribution and sort keys, and automatic table optimization works the same way; you just do not choose node types or counts, capacity is in RPUs.
- **Sort keys decaying**: new rows land in an unsorted region until vacuum sorts them, and if queries change to filter on another column, the existing key no longer prunes. Monitor `unsorted` and query patterns, and let AUTO re-evaluate.
