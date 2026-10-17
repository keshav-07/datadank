---
title: "Top 50 Snowflake Interview Questions and Answers"
seoTitle: "Top 50 Snowflake Interview Questions and Answers"
description: "Top 50 Snowflake interview questions with answers: virtual warehouses, micro-partitions, loading, streams and tasks, Time Travel, RBAC and cost."
technology: ["snowflake", "data-engineering"]
topic: ["interview-questions"]
difficulty: "Intermediate"
section: "learn"
publishedDate: "2026-10-16"
updatedDate: "2026-10-16"
reviewedDate: "2026-10-16"
related: ["interview-questions:snowflake/time-travel-vs-fail-safe", "interview-questions:snowflake/zero-copy-clone-uses", "interview-questions:snowflake/streams-tasks-incremental-pipeline"]
versionContext: "Product names and defaults as of October 2026; code snippets state their dialect or runtime"
---

These 50 Snowflake interview questions cover architecture, virtual warehouses, micro-partitions and performance, loading, streams and tasks, dynamic tables, Time Travel, security, sharing and cost. They are grouped from beginner (freshers) to advanced (experienced). Each one starts with the answer you would give in the first 20 seconds; open the full answer for the detailed explanation, an example, how it shows up in real work, common mistakes and the follow-up questions interviewers ask.

## Question list

<details class="qb-index" open>
<summary>All 50 questions</summary>

1. [Explain Snowflake's three-layer architecture](#1-explain-snowflakes-three-layer-architecture)
2. [Virtual warehouses: sizes, credits, auto-suspend and auto-resume](#2-virtual-warehouses-sizes-credits-auto-suspend-and-auto-resume)
3. [Micro-partitions and partition pruning](#3-micro-partitions-and-partition-pruning)
4. [Time Travel: retention by edition and how to query history](#4-time-travel-retention-by-edition-and-how-to-query-history)
5. [Fail-safe and how it differs from Time Travel](#5-fail-safe-and-how-it-differs-from-time-travel)
6. [Permanent vs transient vs temporary tables](#6-permanent-vs-transient-vs-temporary-tables)
7. [Zero-copy cloning](#7-zero-copy-cloning)
8. [How do you recover a dropped or overwritten table?](#8-how-do-you-recover-a-dropped-or-overwritten-table)
9. [Stages: user, table, named internal and external](#9-stages-user-table-named-internal-and-external)
10. [Snowpipe auto-ingest](#10-snowpipe-auto-ingest)
11. [Tasks: schedules, task graphs and serverless tasks](#11-tasks-schedules-task-graphs-and-serverless-tasks)
12. [VARIANT and semi-structured data](#12-variant-and-semi-structured-data)
13. [Snowflake RBAC: system roles and role hierarchy](#13-snowflake-rbac-system-roles-and-role-hierarchy)
14. [Snowflake editions and what each adds](#14-snowflake-editions-and-what-each-adds)
15. [INFORMATION_SCHEMA vs ACCOUNT_USAGE (latency and retention)](#15-information_schema-vs-account_usage-latency-and-retention)
16. [Scale up vs scale out: multi-cluster warehouses and scaling policies](#16-scale-up-vs-scale-out-multi-cluster-warehouses-and-scaling-policies)
17. [Clustering keys, clustering depth and Automatic Clustering](#17-clustering-keys-clustering-depth-and-automatic-clustering)
18. [Snowflake caching layers: result, warehouse (local disk) and metadata](#18-snowflake-caching-layers-result-warehouse-local-disk-and-metadata)
19. [COPY INTO options and load metadata](#19-copy-into-options-and-load-metadata)
20. [File sizing and format best practices for bulk loading](#20-file-sizing-and-format-best-practices-for-bulk-loading)
21. [Streams: standard, append-only, insert-only; offsets and staleness](#21-streams-standard-append-only-insert-only-offsets-and-staleness)
22. [Dynamic Tables: target lag and refresh modes](#22-dynamic-tables-target-lag-and-refresh-modes)
23. [Materialized views in Snowflake and their limitations](#23-materialized-views-in-snowflake-and-their-limitations)
24. [LATERAL FLATTEN for nested JSON](#24-lateral-flatten-for-nested-json)
25. [INFER_SCHEMA and table schema evolution](#25-infer_schema-and-table-schema-evolution)
26. [Secure Data Sharing and reader accounts](#26-secure-data-sharing-and-reader-accounts)
27. [Access-role vs functional-role design and future grants](#27-access-role-vs-functional-role-design-and-future-grants)
28. [Dynamic data masking policies](#28-dynamic-data-masking-policies)
29. [Row access policies](#29-row-access-policies)
30. [Reading the Query Profile: spilling, exploding joins, poor pruning](#30-reading-the-query-profile-spilling-exploding-joins-poor-pruning)
31. [Cost control: resource monitors and budgets](#31-cost-control-resource-monitors-and-budgets)
32. [Snowpark: DataFrame API and pushdown](#32-snowpark-dataframe-api-and-pushdown)
33. [UDFs, UDTFs and stored procedures (languages and use cases)](#33-udfs-udtfs-and-stored-procedures-languages-and-use-cases)
34. [External tables vs Iceberg tables](#34-external-tables-vs-iceberg-tables)
35. [MERGE in Snowflake and the nondeterministic-merge error](#35-merge-in-snowflake-and-the-nondeterministic-merge-error)
36. [Gen2 and Snowpark-optimized warehouses](#36-gen2-and-snowpark-optimized-warehouses)
37. [Cortex AI SQL functions (AI_COMPLETE, AI_CLASSIFY and others)](#37-cortex-ai-sql-functions-ai_complete-ai_classify-and-others)
38. [Search Optimization Service vs clustering](#38-search-optimization-service-vs-clustering)
39. [Snowpipe vs Snowpipe Streaming vs COPY INTO](#39-snowpipe-vs-snowpipe-streaming-vs-copy-into)
40. [Build an incremental CDC pipeline with Streams and Tasks](#40-build-an-incremental-cdc-pipeline-with-streams-and-tasks)
41. [Dynamic Tables vs Streams & Tasks vs materialized views](#41-dynamic-tables-vs-streams--tasks-vs-materialized-views)
42. [Listings and cross-region/cross-cloud sharing](#42-listings-and-cross-regioncross-cloud-sharing)
43. [Tag-based masking and sensitive data classification](#43-tag-based-masking-and-sensitive-data-classification)
44. [Snowflake performance tuning checklist](#44-snowflake-performance-tuning-checklist)
45. [Query Acceleration Service](#45-query-acceleration-service)
46. [Analysing spend with ACCOUNT_USAGE views](#46-analysing-spend-with-account_usage-views)
47. [Iceberg tables: Snowflake-managed vs externally managed catalogs](#47-iceberg-tables-snowflake-managed-vs-externally-managed-catalogs)
48. [Transactions and locking behaviour in Snowflake](#48-transactions-and-locking-behaviour-in-snowflake)
49. [Replication and failover for business continuity](#49-replication-and-failover-for-business-continuity)
50. [Hybrid tables (Unistore)](#50-hybrid-tables-unistore)

</details>

## Beginner Snowflake interview questions (freshers)

### 1. Explain Snowflake's three-layer architecture

<p class="qb-meta">Beginner · Very Common · Architecture</p>

<p class="qb-label">Short answer</p>

Snowflake separates three layers. Database storage holds all data as compressed, columnar micro-partitions in cloud object storage. Compute consists of independent virtual warehouses that run queries. Cloud services handle authentication, metadata, query optimisation, transactions and access control. Storage and compute scale independently, and many warehouses can read the same data without contention.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

Snowflake describes its design as a hybrid of shared-disk and shared-nothing architectures.

**1. Database storage layer**
- Data loaded into tables is reorganised into immutable **micro-partitions**: columnar, compressed and encrypted (see [Micro-partitions and partition pruning](#3-micro-partitions-and-partition-pruning)).
- It sits in the cloud provider's object storage (S3, Azure Blob, GCS), managed entirely by Snowflake. You don't access the files directly.
- You pay for compressed storage (including Time Travel and Fail-safe versions).

**2. Compute layer (virtual warehouses)**
- Each **virtual warehouse** is an MPP cluster of compute nodes that executes queries and DML.
- Warehouses are independent: an ETL warehouse and a BI warehouse don't compete for CPU or memory, even when they read the same tables (the "shared data" part).
- They cache data on local SSD while running (see [Snowflake caching layers: result, warehouse (local disk) and metadata](#18-snowflake-caching-layers-result-warehouse-local-disk-and-metadata)), are billed per second while running (60-second minimum each time they start), and can auto-suspend.

**3. Cloud services layer**
- The "brain": authentication, access control, the metadata store (micro-partition statistics), query parsing and optimisation, transaction management, and infrastructure management.
- It serves metadata-only queries (`COUNT(*)` on a table, `SHOW` commands) and the result cache without a running warehouse.
- Cloud services usage is billed only when it exceeds 10% of daily warehouse compute.

**Why it matters**
- Isolate workloads with separate warehouses, so no "noisy neighbour" queries slow each other down.
- Elastic scaling: resize or add clusters in seconds without moving data.
- Zero-copy cloning, Time Travel and data sharing are metadata operations on shared storage (see [Zero-copy cloning](#7-zero-copy-cloning), [Secure Data Sharing and reader accounts](#26-secure-data-sharing-and-reader-accounts)).

<p class="qb-label">Example</p>

```sql
-- Dialect: Snowflake SQL
-- Two isolated workloads reading the same table with different warehouses
USE WAREHOUSE etl_wh;
INSERT INTO analytics.fct_orders SELECT * FROM staging.orders_clean;

USE WAREHOUSE bi_wh;
SELECT order_date, SUM(amount) FROM analytics.fct_orders GROUP BY order_date;
```

<p class="qb-label">Real-world use case</p>

A retailer runs nightly loads on an XL warehouse, analyst queries on a multi-cluster Medium warehouse, and data-science feature builds on a Snowpark-optimized warehouse, all against the same tables with no copies and no contention.

<p class="qb-label">Common mistakes and red flags</p>

- Saying Snowflake stores data "on the warehouse".
- Thinking a suspended warehouse means the data is unavailable to other warehouses.
- Not knowing what the cloud services layer does.

<p class="qb-label">Follow-up questions</p>

- Can you query without a warehouse? — Metadata-only queries and result-cache hits, yes; anything scanning data needs a running warehouse.
- What is the 10% rule? — Cloud services credits are charged only for the portion exceeding 10% of that day's warehouse credits.

</details>

### 2. Virtual warehouses: sizes, credits, auto-suspend and auto-resume

<p class="qb-meta">Beginner · Very Common · Virtual Warehouses</p>

<p class="qb-label">Short answer</p>

Warehouses come in T-shirt sizes from X-Small to 6X-Large. Each size up doubles compute and credits per hour (XS = 1, S = 2, M = 4 … 6XL = 512 for standard warehouses). Billing is per second with a 60-second minimum each time the warehouse starts. Auto-suspend stops billing after a period of inactivity, and auto-resume restarts the warehouse when a query arrives.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Sizes and credits (standard Gen1 warehouses, credits per hour)**
| Size | XS | S | M | L | XL | 2XL | 3XL | 4XL | 5XL | 6XL |
|---|---|---|---|---|---|---|---|---|---|---|
| Credits/hour | 1 | 2 | 4 | 8 | 16 | 32 | 64 | 128 | 256 | 512 |

Gen2 and Snowpark-optimized warehouses have different credit rates (see [Gen2 and Snowpark-optimized warehouses](#36-gen2-and-snowpark-optimized-warehouses)). Check current pricing for your edition and region.

**Billing behaviour**
- Per-second billing while running, with a **60-second minimum** each time it resumes. Ten 5-second queries spread across ten resumes cost 10 minutes, not 50 seconds.
- Resizing takes effect for new queries. Running queries finish on the old size.
- A suspended warehouse costs nothing for compute. Storage is billed separately.

**Auto-suspend and auto-resume**
- `AUTO_SUSPEND = 60` (seconds) is a common setting for ETL and ad-hoc warehouses. BI warehouses may use a longer value to keep the local cache warm (see [Snowflake caching layers: result, warehouse (local disk) and metadata](#18-snowflake-caching-layers-result-warehouse-local-disk-and-metadata)).
- `AUTO_RESUME = TRUE` starts the warehouse on demand. Resume is usually a second or two.
- Too short a suspend on a bursty workload causes repeated 60-second minimums and a cold cache. Too long wastes idle credits.

**Sizing rules of thumb**
- A bigger size helps **one large query** (more memory and parallelism, less spilling). It roughly halves runtime per size up for well-parallelised work, so cost may stay similar.
- It doesn't fix **concurrency queuing**. Use multi-cluster for that (see [Scale up vs scale out: multi-cluster warehouses and scaling policies](#16-scale-up-vs-scale-out-multi-cluster-warehouses-and-scaling-policies)).
- Start small, measure with the Query Profile (spilling, queueing), then adjust.

<p class="qb-label">Example</p>

```sql
-- Dialect: Snowflake SQL
CREATE WAREHOUSE IF NOT EXISTS transform_wh
  WAREHOUSE_SIZE = 'MEDIUM'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE
  STATEMENT_TIMEOUT_IN_SECONDS = 3600;

ALTER WAREHOUSE transform_wh SET WAREHOUSE_SIZE = 'LARGE';   -- scale up for a heavy backfill
```

<p class="qb-label">Real-world use case</p>

A BI warehouse with `AUTO_SUSPEND = 600` sat idle for most of each 10-minute window between sporadic dashboard queries. Lowering it to 120 seconds cut that warehouse's credits by about a third, with little cache impact.

<p class="qb-label">Common mistakes and red flags</p>

- Never-suspending warehouses.
- Choosing the largest size "to be safe".
- Thinking bigger warehouses fix queueing.

<p class="qb-label">Follow-up questions</p>

- What is `STATEMENT_TIMEOUT_IN_SECONDS`? — A guardrail that cancels queries running longer than the limit on that warehouse.
- How do you limit a warehouse's spend? — Resource monitors and budgets (see [Cost control: resource monitors and budgets](#31-cost-control-resource-monitors-and-budgets)).

</details>

### 3. Micro-partitions and partition pruning

<p class="qb-meta">Beginner · Very Common · Storage</p>

<p class="qb-label">Short answer</p>

Snowflake automatically splits every table into immutable micro-partitions of roughly 50–500 MB of uncompressed data, stored column by column. For each micro-partition it records metadata such as min/max values, distinct counts and null counts per column. At query time it skips micro-partitions whose ranges can't match the filters. That skipping is pruning.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**What a micro-partition is**
- A contiguous unit of storage created as data is loaded or modified, in insertion order by default.
- Columnar within the partition, with per-column compression chosen automatically.
- **Immutable:** UPDATE, DELETE and MERGE write new micro-partitions and mark the old ones as removed. The old ones are retained for Time Travel and Fail-safe (see [Time Travel: retention by edition and how to query history](#4-time-travel-retention-by-edition-and-how-to-query-history), [Fail-safe and how it differs from Time Travel](#5-fail-safe-and-how-it-differs-from-time-travel)).

**Metadata stored per micro-partition (per column)**
- Range of values (min/max), number of distinct values, null count, and other properties used by the optimizer.
- This metadata lives in the cloud services layer, so pruning decisions happen before any data is read.

**Pruning**
- `WHERE order_date = '2026-03-01'` reads only the micro-partitions whose `order_date` range includes that date.
- How effective it is depends on **how well the data is clustered** by the filter column. If every micro-partition spans all dates (randomly ordered loads), nothing can be skipped.
- The Query Profile shows "Partitions scanned" vs "Partitions total". That ratio is the first thing to check on a slow query.

**What helps pruning**
- Loading data in an order that matches common filters (time-ordered loads naturally cluster by date).
- Clustering keys for large tables whose natural order doesn't match query filters (see [Clustering keys, clustering depth and Automatic Clustering](#17-clustering-keys-clustering-depth-and-automatic-clustering)).
- Sargable predicates on the raw column (see [What makes a predicate sargable?](/sql/sql-interview-questions/#46-what-makes-a-predicate-sargable)).
- Search optimization for selective point lookups (see [Search Optimization Service vs clustering](#38-search-optimization-service-vs-clustering)).

There is no user-defined partitioning (no `PARTITION BY` DDL for standard tables). Micro-partitioning is automatic.

<p class="qb-label">Example</p>

```sql
-- Dialect: Snowflake SQL
-- Inspect how well a table is clustered on a column (higher depth = more overlap = worse pruning)
SELECT SYSTEM$CLUSTERING_INFORMATION('analytics.fct_events', '(event_date)');

-- A pruning-friendly filter: half-open range on the raw column
SELECT COUNT(*)
FROM analytics.fct_events
WHERE event_date >= '2026-03-01' AND event_date < '2026-03-08';
```

<p class="qb-label">Real-world use case</p>

A 40 TB events table loaded daily in time order pruned well on `event_date`. Queries filtering on `country`, though, scanned everything, because country values were spread across all micro-partitions. That led to a clustering-key discussion (see [Clustering keys, clustering depth and Automatic Clustering](#17-clustering-keys-clustering-depth-and-automatic-clustering)).

<p class="qb-label">Common mistakes and red flags</p>

- Trying to create partitions or indexes on standard tables.
- Not checking partitions scanned in the profile.
- Believing micro-partitions are updated in place.

<p class="qb-label">Follow-up questions</p>

- Why does a small UPDATE create a lot of storage? — Every touched micro-partition is rewritten; the old versions are retained for Time Travel/Fail-safe.
- What is clustering depth? — The average number of overlapping micro-partitions for a value range; lower is better.

</details>

### 4. Time Travel: retention by edition and how to query history

<p class="qb-meta">Beginner · Very Common · Data Protection</p>

<p class="qb-label">Short answer</p>

Time Travel lets you query, clone or restore data as it existed at a past point within the retention period. You use `AT` or `BEFORE` with a timestamp, offset or query ID, and `UNDROP` for dropped objects. Standard edition allows up to 1 day. Enterprise edition and above allow up to 90 days for permanent objects. Transient and temporary tables allow at most 1 day.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Retention**
| Object type | Standard edition | Enterprise and above |
|---|---|---|
| Permanent table/schema/database | 0–1 day (default 1) | 0–90 days (default 1) |
| Transient table | 0–1 day | 0–1 day |
| Temporary table | 0–1 day (and only for the session lifetime) | 0–1 day |

- Set with `DATA_RETENTION_TIME_IN_DAYS` at account, database, schema or table level. Lower levels override higher ones.
- `MIN_DATA_RETENTION_TIME_IN_DAYS` (account level) enforces a floor.
- Setting retention to 0 disables Time Travel for that object.

**What you can do**
- Query history: `SELECT ... FROM t AT(TIMESTAMP => ...)`, `AT(OFFSET => -3600)`, or `BEFORE(STATEMENT => '<query_id>')`.
- Clone at a point in time: `CREATE TABLE t_restore CLONE t BEFORE(STATEMENT => '...')`.
- Restore dropped objects: `UNDROP TABLE | SCHEMA | DATABASE`.

**Costs:** changed and deleted micro-partitions are kept for the retention period, so storage grows with churn × retention. Large, frequently rewritten staging tables should usually be transient with short or zero retention (see [Permanent vs transient vs temporary tables](#6-permanent-vs-transient-vs-temporary-tables)).

**After Time Travel ends**, permanent-table data moves into **Fail-safe** for 7 days, recoverable only by Snowflake Support (see [Fail-safe and how it differs from Time Travel](#5-fail-safe-and-how-it-differs-from-time-travel)).

<p class="qb-label">Example</p>

```sql
-- Dialect: Snowflake SQL
ALTER TABLE analytics.fct_orders SET DATA_RETENTION_TIME_IN_DAYS = 30;   -- Enterprise+

-- Data as of 2 hours ago
SELECT COUNT(*) FROM analytics.fct_orders AT(OFFSET => -60*60*2);

-- Before a bad DELETE, then restore via clone + swap
CREATE OR REPLACE TABLE analytics.fct_orders_restore
  CLONE analytics.fct_orders BEFORE(STATEMENT => '01b2c3d4-0000-1111-0000-000000000abc');
ALTER TABLE analytics.fct_orders SWAP WITH analytics.fct_orders_restore;

UNDROP TABLE analytics.dim_promotions;
```

<p class="qb-label">Real-world use case</p>

A developer ran an UPDATE without a WHERE clause on a production dimension. Using the query ID, the team cloned the table as it was before the statement and swapped it back in within 10 minutes. No backup restore was needed.

<p class="qb-label">Common mistakes and red flags</p>

- Saying Standard edition supports 90 days.
- Setting 90-day retention on high-churn staging tables, which inflates storage.
- Confusing Time Travel with Fail-safe.

<p class="qb-label">Follow-up questions</p>

- What happens if you drop and recreate a table with the same name, then UNDROP? — UNDROP fails until you rename the current table; then the dropped version can be restored.
- Does Time Travel work on views? — No; it applies to tables, schemas and databases (data, not view definitions).

</details>

### 5. Fail-safe and how it differs from Time Travel

<p class="qb-meta">Beginner · Very Common · Data Protection</p>

<p class="qb-label">Short answer</p>

Fail-safe is a non-configurable 7-day period that starts after Time Travel retention ends, for permanent tables only. During it, Snowflake can recover data, but only through Snowflake Support and on a best-effort basis. You can't query it yourself. Transient and temporary tables have no Fail-safe, which is one reason they are cheaper.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Timeline for a permanent table's changed data**
```text
change happens --> [ Time Travel: 0-90 days, self-service ] --> [ Fail-safe: 7 days, Support only ] --> purged
```

| Aspect | Time Travel | Fail-safe |
|---|---|---|
| Duration | Configurable (0–1 day Standard; 0–90 days Enterprise+) | Fixed 7 days (permanent objects) |
| Access | Self-service SQL (AT/BEFORE, CLONE, UNDROP) | Snowflake Support only |
| Purpose | Operational recovery, auditing, cloning past states | Last-resort disaster recovery |
| Applies to | Permanent, transient (≤1 day), temporary (≤1 day) | Permanent only |
| Cost | Storage for retained versions | Storage for retained versions |

**Cost implications**
- Storage billing includes active, Time Travel *and* Fail-safe bytes. A table rewritten daily retains roughly (retention days + 7) days of historical micro-partitions.
- Check `TABLE_STORAGE_METRICS` for `ACTIVE_BYTES`, `TIME_TRAVEL_BYTES` and `FAILSAFE_BYTES` per table.
- For reproducible staging and intermediate tables, use **transient** tables to avoid Fail-safe storage (see [Permanent vs transient vs temporary tables](#6-permanent-vs-transient-vs-temporary-tables)).

**Not a backup strategy by itself:** Fail-safe isn't guaranteed self-service recovery. For stronger protection, use Time Travel, cross-region replication (see [Replication and failover for business continuity](#49-replication-and-failover-for-business-continuity)), and (where available) Snowflake's backup features, plus pipeline reprocessability.

**Interview framing:** describe the two as layers. Time Travel is your own undo button for mistakes within the retention window. Fail-safe is Snowflake's emergency net after that window closes, which you can't rely on for routine recovery. Then mention the cost link, because senior interviewers often follow up with storage questions.

<p class="qb-label">Example</p>

```sql
-- Dialect: Snowflake SQL
-- Where is storage going? Active vs Time Travel vs Fail-safe per table
SELECT table_catalog, table_schema, table_name,
       active_bytes / POWER(1024, 3)       AS active_gb,
       time_travel_bytes / POWER(1024, 3)  AS time_travel_gb,
       failsafe_bytes / POWER(1024, 3)     AS failsafe_gb
FROM snowflake.account_usage.table_storage_metrics
WHERE deleted = FALSE
ORDER BY failsafe_bytes DESC
LIMIT 20;
```

<p class="qb-label">Real-world use case</p>

A cost review found 35% of storage was Fail-safe bytes from staging tables truncated and reloaded hourly. Recreating them as transient tables removed that storage, because they could be rebuilt from raw data anyway.

<p class="qb-label">Common mistakes and red flags</p>

- Thinking you can query Fail-safe.
- Thinking Fail-safe duration is configurable.
- Ignoring Fail-safe in storage cost analysis.

<p class="qb-label">Follow-up questions</p>

- Can you convert a permanent table to transient? — Not with ALTER; recreate it (CTAS or clone into a transient table) and swap.
- Do clones carry Fail-safe? — Clones share micro-partitions; storage is billed once, and Fail-safe applies according to the clone's table type.

**In depth:** [What is the difference between Snowflake Time Travel and Fail-safe?](/interview/snowflake/time-travel-vs-fail-safe/)

</details>

### 6. Permanent vs transient vs temporary tables

<p class="qb-meta">Beginner · Very Common · Table Types</p>

<p class="qb-label">Short answer</p>

Permanent tables have full Time Travel (up to 90 days on Enterprise) and 7 days of Fail-safe. Transient tables persist until dropped, but have at most 1 day of Time Travel and no Fail-safe, so they are cheaper for reproducible data. Temporary tables exist only for the session that created them, are invisible to other sessions, and also have no Fail-safe.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

| Property | Permanent | Transient | Temporary |
|---|---|---|---|
| Persistence | Until dropped | Until dropped | Until session ends |
| Visible to other sessions | Yes | Yes | No |
| Time Travel | 0–90 days (Enterprise+); 0–1 Standard | 0–1 day | 0–1 day (within session) |
| Fail-safe | 7 days | None | None |
| Storage cost | Highest | Lower | Lower (short-lived) |
| Typical use | Facts, dimensions, anything hard to reproduce | Staging, intermediate, dbt models rebuilt each run | Session scratch work, procedure intermediates |

**Other variants**
- **Transient databases and schemas:** every table created inside them is transient.
- **External tables:** metadata over files in a stage (see [External tables vs Iceberg tables](#34-external-tables-vs-iceberg-tables)).
- **Iceberg tables:** open-format tables in your external volume (see [Iceberg tables: Snowflake-managed vs externally managed catalogs](#47-iceberg-tables-snowflake-managed-vs-externally-managed-catalogs)).
- **Dynamic tables, hybrid tables, event tables:** specialised types (see [Dynamic Tables: target lag and refresh modes](#22-dynamic-tables-target-lag-and-refresh-modes), [Hybrid tables (Unistore)](#50-hybrid-tables-unistore)).

**Naming conflicts:** a temporary table can share a name with a permanent table in the same schema, and it **shadows** that table within the session. This is a source of confusing bugs.

**Choosing**
- Can the data be rebuilt from upstream in reasonable time? Then it can be transient.
- Is it a system of record, or would rebuilding be costly? Keep it permanent.
- dbt supports `transient: true` (the default for Snowflake tables in dbt) and `transient: false` per model. Set gold tables to permanent if you need longer Time Travel and Fail-safe protection.

<p class="qb-label">Example</p>

```sql
-- Dialect: Snowflake SQL
CREATE TRANSIENT SCHEMA IF NOT EXISTS staging;          -- all tables inside are transient

CREATE TRANSIENT TABLE staging.orders_clean (
    order_id NUMBER, customer_id NUMBER, amount NUMBER(12,2), order_date DATE
) DATA_RETENTION_TIME_IN_DAYS = 0;

CREATE TEMPORARY TABLE tmp_batch AS
SELECT * FROM staging.orders_clean WHERE order_date = CURRENT_DATE - 1;

CREATE TABLE analytics.fct_orders (
    order_id NUMBER, customer_id NUMBER, amount NUMBER(12,2), order_date DATE
) DATA_RETENTION_TIME_IN_DAYS = 14;                      -- permanent, Enterprise+
```

<p class="qb-label">Real-world use case</p>

A dbt project's 300 staging and intermediate models were transient (dbt's default on Snowflake), while 40 gold marts were explicitly permanent with 14-day retention. That cut storage cost while keeping recoverability where it mattered.

<p class="qb-label">Common mistakes and red flags</p>

- Using permanent tables for high-churn scratch data.
- Making a system-of-record table transient.
- Forgetting temporary tables shadow permanent tables with the same name.

<p class="qb-label">Follow-up questions</p>

- Can you clone a permanent table into a transient one? — Yes (`CREATE TRANSIENT TABLE ... CLONE ...`); the reverse is not allowed.
- Are temporary tables billed? — Yes, for storage while they exist; they're dropped at session end.

</details>

### 7. Zero-copy cloning

<p class="qb-meta">Beginner · Very Common · Cloning</p>

<p class="qb-label">Short answer</p>

`CLONE` creates a copy of a table, schema or database almost instantly by copying only metadata. The clone points to the same micro-partitions as the source. Storage is consumed only when either side changes data, and only for the new or changed micro-partitions. Clones are fully independent and writable, and can be taken at a past point in time using Time Travel.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**How it works**
- At clone time, Snowflake creates new metadata referencing the source's existing micro-partitions. No data is copied.
- After that, writes to either object create new micro-partitions owned by that object (copy-on-write at micro-partition level).
- Clones can be **nested** (schema and database clones include their child objects) and **point-in-time**: `CLONE t AT(TIMESTAMP => ...)`.

**What gets cloned (and what doesn't)**
- Tables, schemas and databases, along with streams, tasks, stages and file formats in cloned schemas or databases, with caveats. Cloned tasks are suspended by default. Cloned streams on cloned tables may begin with no unconsumed records in some cases. Check the documentation's cloning considerations.
- **Privileges:** a cloned table does not inherit the source table's grants (unless you use `COPY GRANTS` for tables). Child objects in cloned databases and schemas keep their grants, but the container itself doesn't inherit the source's grants.
- **Internal (named) stages are not cloned with their files**, while external stages are cloned as definitions.
- Pipes that reference internal table stages are not cloned. Check the docs for pipe-specific rules.

**Common uses**
- **Dev and test environments:** clone production into `DEV_DB` in seconds for realistic testing (see [Dev/test/prod environments and promoting data code](/career/data-engineering-interview-questions/#68-devtestprod-environments-and-promoting-data-code)).
- **CI:** per-pull-request clones that are dropped after the run (see [dbt clone and zero-copy CI environments](/dbt/dbt-interview-questions/#50-dbt-clone-and-zero-copy-ci-environments) for `dbt clone`).
- **Safe experiments and backfills:** clone, run the risky change, compare, swap (see [Write-Audit-Publish and blue-green deployments for data](/career/data-engineering-interview-questions/#98-write-audit-publish-and-blue-green-deployments-for-data)).
- **Point-in-time snapshots** for audits or month-end freezes.

**Costs and caveats**
- A long-lived clone of a frequently changing table gradually accumulates its own storage as the two diverge.
- Masking and row access policies attached to cloned tables continue to apply, which is good for protecting PII in dev.

<p class="qb-label">Example</p>

```sql
-- Dialect: Snowflake SQL
CREATE OR REPLACE DATABASE analytics_dev CLONE analytics;              -- full dev copy in seconds

CREATE TABLE analytics.fct_orders_month_end
  CLONE analytics.fct_orders AT(TIMESTAMP => '2026-03-31 23:59:59'::TIMESTAMP_LTZ);

CREATE TABLE analytics.dim_customer_test CLONE analytics.dim_customer COPY GRANTS;
```

<p class="qb-label">Real-world use case</p>

A team rebuilds `ANALYTICS_QA` from production every night with one CLONE statement. Testers always have production-shaped data with masking policies still enforced. The clone costs almost nothing because few tables change in QA.

<p class="qb-label">Common mistakes and red flags</p>

- Thinking clones double storage immediately.
- Assuming grants and internal stage files are cloned.
- Forgetting cloned tasks start suspended.

<p class="qb-label">Follow-up questions</p>

- Can you clone across accounts? — Not with CLONE; use data sharing or replication.
- What happens to a clone when the source is dropped? — The clone remains intact; shared micro-partitions are retained while any object references them.

**In depth:** [What is zero-copy cloning in Snowflake, and how would you use it?](/interview/snowflake/zero-copy-clone-uses/)

</details>

### 8. How do you recover a dropped or overwritten table?

<p class="qb-meta">Beginner · Common · Data Protection</p>

<p class="qb-label">Short answer</p>

For a dropped table, run `UNDROP TABLE`, renaming any newer table with the same name first. For an overwritten or corrupted table, find the query ID of the bad statement and clone the table `BEFORE(STATEMENT => ...)`, or use `AT(TIMESTAMP => ...)`. Validate the clone, then swap it in. All of this works only within the Time Travel retention period.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Scenario A: table dropped**
1. `SHOW TABLES HISTORY LIKE 'name' IN SCHEMA s;` lists dropped versions with their `dropped_on` time.
2. If a new table with the same name exists, rename it first: `ALTER TABLE s.name RENAME TO s.name_new;`
3. `UNDROP TABLE s.name;` restores the most recently dropped version. Repeat to go further back through versions.
4. Re-check grants. UNDROP restores the table's privileges as they were.

**Scenario B: bad DML or CREATE OR REPLACE**
1. Find the culprit query: `QUERY_HISTORY` filtered by table name and time, or the Snowsight query history.
2. Clone the pre-change state: `CREATE TABLE t_restore CLONE t BEFORE(STATEMENT => '<query_id>');`
3. Validate row counts and spot checks against expectations.
4. Swap atomically: `ALTER TABLE t SWAP WITH t_restore;`, then drop the bad version later.
5. For `CREATE OR REPLACE`, the old table is effectively dropped. Its prior version can be found with `SHOW TABLES HISTORY` and restored with UNDROP after renaming the current one.

**Scenario C: beyond Time Travel**
- Data is in Fail-safe (7 days, permanent tables only). Contact Snowflake Support (see [Fail-safe and how it differs from Time Travel](#5-fail-safe-and-how-it-differs-from-time-travel)).
- Otherwise rebuild from upstream raw data. This is why reproducible pipelines and a retained bronze layer matter.

**Downstream:** check which dependent objects (views, streams, tasks, dynamic tables) were affected, and whether any streams went stale or consumed bad data.

<p class="qb-label">Example</p>

```sql
-- Dialect: Snowflake SQL
SHOW TABLES HISTORY LIKE 'DIM_STORE' IN SCHEMA analytics.core;

ALTER TABLE analytics.core.dim_store RENAME TO analytics.core.dim_store_bad;
UNDROP TABLE analytics.core.dim_store;

-- Or, restore from just before a specific bad statement
CREATE OR REPLACE TABLE analytics.core.dim_store_restore
  CLONE analytics.core.dim_store BEFORE(STATEMENT => '01b3aa21-0602-9b1e-0000-4b2100c3d4e5');
ALTER TABLE analytics.core.dim_store SWAP WITH analytics.core.dim_store_restore;
```

<p class="qb-label">Real-world use case</p>

A dbt run with a bug replaced `dim_store` with an empty table at 02:00. The on-call engineer cloned it before the dbt statement's query ID, swapped it back, and paused the job, all before the 07:00 dashboards refreshed.

<p class="qb-label">Common mistakes and red flags</p>

- Running UNDROP while a same-named table exists.
- Restoring in place without validating.
- Not knowing the retention period of the table in question.

<p class="qb-label">Follow-up questions</p>

- Can you UNDROP a schema or database? — Yes, within retention, with the same rename rule.
- How do you prevent this? — Least-privilege roles, CI review, and Write-Audit-Publish for critical tables (see [Write-Audit-Publish and blue-green deployments for data](/career/data-engineering-interview-questions/#98-write-audit-publish-and-blue-green-deployments-for-data)).

</details>

### 9. Stages: user, table, named internal and external

<p class="qb-meta">Beginner · Very Common · Data Loading</p>

<p class="qb-label">Short answer</p>

A stage is a location for data files used for loading and unloading. Internal stages store files inside Snowflake: user stages (`@~`), table stages (`@%table`) and named internal stages (`@my_stage`). External stages point to your cloud storage (S3, Azure Blob/ADLS, GCS), usually through a storage integration for secure, credential-free access.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

| Stage | Reference | Scope | Notes |
|---|---|---|---|
| User stage | `@~` | One user | Auto-created; can't be altered or dropped; no file format options |
| Table stage | `@%orders` | One table | Auto-created; only that table can load from it; no grants |
| Named internal stage | `@raw.landing_stage` | Schema object | Grantable, file format defaults, directory tables, encryption options |
| Named external stage | `@raw.s3_orders` | Schema object | Points at cloud storage URL; uses storage integration or credentials |

**Working with internal stages**
- Upload with `PUT file://local/path @stage` (SnowSQL, Snowflake CLI or drivers), then load with `COPY INTO`.
- Download unloaded files with `GET`.
- Snowflake encrypts files at rest automatically.

**External stages**
- Create a **storage integration**, a cloud IAM trust (AWS IAM role, Azure service principal, GCP service account), so no keys sit in SQL.
- `CREATE STAGE ... URL = 's3://bucket/path/' STORAGE_INTEGRATION = s3_int FILE_FORMAT = ...`.
- Can drive **Snowpipe auto-ingest** through cloud event notifications (see [Snowpipe auto-ingest](#10-snowpipe-auto-ingest)).

**Useful features**
- `LIST @stage` shows files. Query staged files directly with `SELECT $1, $2 FROM @stage (FILE_FORMAT => ...)` for inspection.
- **Directory tables** on stages catalogue files (path, size, last modified) and can be refreshed automatically. They're useful for unstructured data pipelines.
- **File format objects** keep parsing options reusable.

<p class="qb-label">Example</p>

```sql
-- Dialect: Snowflake SQL
CREATE STORAGE INTEGRATION s3_lake_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/snowflake-lake-reader'
  STORAGE_ALLOWED_LOCATIONS = ('s3://acme-lake/raw/');

CREATE FILE FORMAT raw.ff_parquet TYPE = PARQUET;

CREATE STAGE raw.s3_orders
  URL = 's3://acme-lake/raw/orders/'
  STORAGE_INTEGRATION = s3_lake_int
  FILE_FORMAT = raw.ff_parquet;

LIST @raw.s3_orders PATTERN = '.*2026-03-01.*';
```

<p class="qb-label">Real-world use case</p>

A team replaced hardcoded AWS keys in stage definitions with a storage integration bound to a read-only IAM role scoped to one bucket prefix. That removed long-lived secrets and passed a security audit.

<p class="qb-label">Common mistakes and red flags</p>

- Embedding access keys in `CREATE STAGE`.
- Using user stages for shared production pipelines.
- Not knowing table stages can't be granted to other roles.

<p class="qb-label">Follow-up questions</p>

- What is `STORAGE_ALLOWED_LOCATIONS`? — The bucket/path allowlist the integration can access, limiting blast radius.
- Can you unload to a stage? — Yes, `COPY INTO @stage FROM (query)` writes files (CSV, JSON, Parquet).

</details>

### 10. Snowpipe auto-ingest

<p class="qb-meta">Beginner · Very Common · Data Loading</p>

<p class="qb-label">Short answer</p>

Snowpipe is a serverless, continuous loader. A pipe wraps a `COPY INTO` statement. With auto-ingest, cloud storage event notifications (S3 events through SQS, Azure Event Grid, GCS Pub/Sub) tell Snowflake when new files land, and Snowflake loads them within about a minute using its own compute. You pay per GB ingested rather than for a running warehouse.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**How it works**
1. Create an external stage on the landing path (see [Stages: user, table, named internal and external](#9-stages-user-table-named-internal-and-external)).
2. `CREATE PIPE ... AUTO_INGEST = TRUE AS COPY INTO ...`.
3. Configure the cloud notification to the pipe's channel. `SHOW PIPES` gives the `notification_channel` (for example the SQS ARN on AWS).
4. New files trigger loads. There is also a REST API (`insertFiles`) for non-event-driven triggering.

**Behaviour**
- **Latency:** typically under a minute or two after arrival, but not guaranteed.
- **Deduplication:** each pipe keeps **14 days** of load history, so a file name already loaded by that pipe is skipped.
- **Ordering:** not guaranteed. Files may load in any order, so design downstream logic to handle that (event time, MERGE).
- **Errors:** defaults to `ON_ERROR = SKIP_FILE`. Errors are visible in `COPY_HISTORY`, `VALIDATE_PIPE_LOAD`, and through error notifications to cloud messaging.
- **Backfill:** `ALTER PIPE ... REFRESH` loads files staged in the last 7 days that weren't loaded.

**Cost:** Snowflake has moved Snowpipe billing to a simpler per-GB model (a fixed credit amount per GB ingested) instead of the older per-second compute plus per-1,000-files charge. Check current pricing. Very small files are still inefficient operationally.

**Monitoring:** `SYSTEM$PIPE_STATUS('pipe')` (execution state, pending file count), `PIPE_USAGE_HISTORY`, and `COPY_HISTORY`.

<p class="qb-label">Example</p>

```sql
-- Dialect: Snowflake SQL
CREATE OR REPLACE PIPE raw.orders_pipe
  AUTO_INGEST = TRUE
AS
COPY INTO raw.orders (order_id, customer_id, amount, order_ts, _source_file)
FROM (SELECT $1:order_id::NUMBER, $1:customer_id::NUMBER, $1:amount::NUMBER(12,2),
             $1:order_ts::TIMESTAMP_NTZ, METADATA$FILENAME
      FROM @raw.s3_orders)
FILE_FORMAT = (FORMAT_NAME = 'raw.ff_parquet');

SHOW PIPES LIKE 'ORDERS_PIPE' IN SCHEMA raw;     -- notification_channel for the S3 event config
SELECT SYSTEM$PIPE_STATUS('raw.orders_pipe');
ALTER PIPE raw.orders_pipe REFRESH;               -- pick up files staged in the last 7 days
```

<p class="qb-label">Real-world use case</p>

An e-commerce app drops order files to S3 every few minutes. Snowpipe loads them within about a minute with no warehouse kept running, and a Stream plus Task (or a Dynamic Table) transforms them downstream (see [Build an incremental CDC pipeline with Streams and Tasks](#40-build-an-incremental-cdc-pipeline-with-streams-and-tasks)).

<p class="qb-label">Common mistakes and red flags</p>

- Expecting ordered loads.
- Overwriting files with the same name and expecting a reload (the pipe skips names it has already loaded).
- Not monitoring pipe status after IAM or notification changes.

<p class="qb-label">Follow-up questions</p>

- How is Snowpipe different from COPY? — Serverless and event-driven vs warehouse-run and scheduled (see [Snowpipe vs Snowpipe Streaming vs COPY INTO](#39-snowpipe-vs-snowpipe-streaming-vs-copy-into)).
- What happens if notifications are misconfigured? — Files land but nothing loads; `SYSTEM$PIPE_STATUS` shows no activity, and `REFRESH` can backfill.

</details>

### 11. Tasks: schedules, task graphs and serverless tasks

<p class="qb-meta">Beginner · Very Common · Streams & Tasks</p>

<p class="qb-label">Short answer</p>

A task runs a SQL statement, a stored procedure call or Snowflake Scripting on a schedule (a CRON expression or interval) or after predecessor tasks, forming a task graph (DAG). Tasks run on a user-specified warehouse, or serverless with compute managed by Snowflake. They are created suspended and must be resumed. A `WHEN` condition such as `SYSTEM$STREAM_HAS_DATA` skips runs with nothing to do.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Scheduling options**
- `SCHEDULE = '5 MINUTE'` or `SCHEDULE = 'USING CRON 0 2 * * * UTC'`.
- **Child tasks** use `AFTER parent_task` (multiple predecessors are allowed) instead of a schedule. Only the root task has a schedule.
- **Triggered tasks** run when a stream on a table has new data, without a fixed schedule (`WHEN SYSTEM$STREAM_HAS_DATA(...)` with no SCHEDULE).
- **Finalizer tasks** run after a graph completes (success or failure), which is useful for cleanup and notifications.

**Compute**
| Mode | Configure | Billing | Good for |
|---|---|---|---|
| User-managed warehouse | `WAREHOUSE = etl_wh` | Warehouse credits (60 s minimum per resume) | Predictable heavy work, sharing a warehouse |
| Serverless | Omit WAREHOUSE (`USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE` hint) | Serverless credits, sized automatically | Short, frequent tasks; avoiding idle warehouse time |

**Operations**
- New tasks are **suspended**. Resume children first, then the root (or use `SYSTEM$TASK_DEPENDENTS_ENABLE` to resume the graph).
- Monitor with `TASK_HISTORY` (table function and ACCOUNT_USAGE) and the Snowsight task graph view.
- Controls: `SUSPEND_TASK_AFTER_NUM_FAILURES`, `USER_TASK_TIMEOUT_MS`, error notifications through a notification integration, `EXECUTE TASK` for a manual run.
- Overlap: by default a new run doesn't start while the previous graph run is still executing (`ALLOW_OVERLAPPING_EXECUTION = FALSE`).
- Ownership and privileges: the owning role needs `EXECUTE TASK` (an account-level privilege) and usage on the warehouse.

**Tasks vs external orchestrators:** tasks are great for Snowflake-internal pipelines. Cross-system dependencies are usually better handled by Airflow or similar (see [Airflow vs Dagster vs Prefect vs cloud-native orchestrators](/career/data-engineering-interview-questions/#51-airflow-vs-dagster-vs-prefect-vs-cloud-native-orchestrators)).

<p class="qb-label">Example</p>

```sql
-- Dialect: Snowflake SQL
CREATE OR REPLACE TASK etl.t_root_load
  SCHEDULE = 'USING CRON 0 * * * * UTC'          -- hourly
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'   -- serverless
  SUSPEND_TASK_AFTER_NUM_FAILURES = 3
AS
  CALL etl.load_raw_orders();

CREATE OR REPLACE TASK etl.t_build_silver
  WAREHOUSE = transform_wh
  AFTER etl.t_root_load
AS
  MERGE INTO silver.orders t USING raw.orders_stream s ON t.order_id = s.order_id
  WHEN MATCHED THEN UPDATE SET t.amount = s.amount
  WHEN NOT MATCHED THEN INSERT (order_id, amount) VALUES (s.order_id, s.amount);

ALTER TASK etl.t_build_silver RESUME;   -- children first
ALTER TASK etl.t_root_load RESUME;
```

<p class="qb-label">Real-world use case</p>

A team runs a five-task graph hourly: load, merge silver, rebuild two gold aggregates in parallel, then a finalizer that sends a Slack notification on failure. No external scheduler is needed for this Snowflake-only flow.

<p class="qb-label">Common mistakes and red flags</p>

- Forgetting tasks start suspended.
- Running a frequent task on a large dedicated warehouse that is mostly idle.
- No failure alerting.

<p class="qb-label">Follow-up questions</p>

- What is the minimum schedule interval? — One minute for scheduled tasks; triggered tasks react to stream data.
- How do you pass values between tasks? — `SYSTEM$SET_RETURN_VALUE` / `SYSTEM$GET_PREDECESSOR_RETURN_VALUE`, or task graph configuration.

</details>

### 12. VARIANT and semi-structured data

<p class="qb-meta">Beginner · Very Common · Semi-Structured Data</p>

<p class="qb-label">Short answer</p>

VARIANT is Snowflake's type for semi-structured data such as JSON, Avro, Parquet, ORC and XML. Snowflake stores it in an optimised columnar form, extracting common paths into sub-columns for pruning and fast access. You query it with path notation (`col:key.nested[0]`), cast with `::TYPE`, and explode arrays with FLATTEN. OBJECT and ARRAY types are also available.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Types**
- `VARIANT`: any value (object, array, string, number, boolean, null).
- `OBJECT`: key-value pairs. `ARRAY`: ordered list. Structured versions (`OBJECT(name STRING, age NUMBER)`, `ARRAY(NUMBER)`) give typed schemas.
- Size limit: a single VARIANT value was historically limited to 16 MB compressed. Snowflake has raised object size limits in newer releases. Check current documentation before relying on the larger limit.

**Querying**
- Path access: `payload:customer.address.city`, or `payload['customer']['address']['city']`.
- Element access: `payload:items[0].sku`.
- Casting: `payload:amount::NUMBER(12,2)`. Without a cast, values are VARIANT and strings display with quotes.
- Case sensitivity: **key names are case-sensitive** in path notation.
- Functions: `PARSE_JSON`, `TRY_PARSE_JSON`, `OBJECT_CONSTRUCT`, `ARRAY_AGG`, `GET_PATH`, `TYPEOF`, `OBJECT_KEYS`, `IS_NULL_VALUE` (JSON null vs SQL NULL).

**Storage and performance**
- On load, Snowflake columnarises frequently occurring paths with consistent types, so filters on `payload:event_type` can prune micro-partitions like regular columns.
- Paths with mixed types, or deeply nested arrays, are less optimised. Extract hot fields into typed columns in silver for best performance and clarity (see [How do you model event or clickstream data?](/career/data-engineering-interview-questions/#40-how-do-you-model-event-or-clickstream-data)).

**JSON null vs SQL NULL:** a key with a JSON `null` value is a VARIANT null, not SQL NULL. `IS NULL` doesn't match it, and `STRIP_NULL_VALUE` / `IS_NULL_VALUE` handle it. The file format option `STRIP_NULL_VALUES` can drop such keys during load.

<p class="qb-label">Example</p>

```sql
-- Dialect: Snowflake SQL
CREATE OR REPLACE TABLE raw.app_events (payload VARIANT, _loaded_at TIMESTAMP_NTZ);

INSERT INTO raw.app_events
SELECT PARSE_JSON('{"event_id":"e1","type":"checkout","user":{"id":42,"tier":"gold"},
                    "items":[{"sku":"A1","qty":2},{"sku":"B7","qty":1}],"coupon":null}'),
       CURRENT_TIMESTAMP();

SELECT payload:event_id::STRING   AS event_id,
       payload:user.id::NUMBER    AS user_id,
       payload:user.tier::STRING  AS tier,
       payload:items[0].sku::STRING AS first_sku,
       IS_NULL_VALUE(payload:coupon) AS coupon_is_json_null
FROM raw.app_events
WHERE payload:type::STRING = 'checkout';
```
Output: e1, 42, gold, A1, TRUE.

<p class="qb-label">Real-world use case</p>

A product team adds new event properties weekly. Raw events stay in VARIANT with no schema changes needed, and silver models type the 20 fields used in KPIs into columns.

<p class="qb-label">Common mistakes and red flags</p>

- Forgetting to cast, then comparing VARIANT with strings unexpectedly.
- Assuming key names are case-insensitive.
- Leaving all analytics on raw VARIANT paths forever.

<p class="qb-label">Follow-up questions</p>

- How do you explode arrays? — `LATERAL FLATTEN` (see [LATERAL FLATTEN for nested JSON](#24-lateral-flatten-for-nested-json)).
- How do you load JSON arrays as rows? — `STRIP_OUTER_ARRAY = TRUE` in the JSON file format.

</details>

### 13. Snowflake RBAC: system roles and role hierarchy

<p class="qb-meta">Beginner · Very Common · Security & RBAC</p>

<p class="qb-label">Short answer</p>

Snowflake grants privileges on objects to roles, and roles to users or to other roles, forming a hierarchy in which parent roles inherit child roles' privileges. The system roles are ORGADMIN, ACCOUNTADMIN, SECURITYADMIN, USERADMIN, SYSADMIN and PUBLIC. Custom roles should roll up to SYSADMIN, so administrators can manage the objects they create.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**System roles**
| Role | Purpose |
|---|---|
| ORGADMIN | Organisation-level ops: create accounts, view org usage (being superseded by GLOBALORGADMIN in organisation accounts — check current docs) |
| ACCOUNTADMIN | Top role in an account; combines SYSADMIN + SECURITYADMIN; billing, account parameters. Grant to very few users, with MFA |
| SECURITYADMIN | Manage grants globally (`MANAGE GRANTS`); inherits USERADMIN |
| USERADMIN | Create and manage users and roles |
| SYSADMIN | Create warehouses, databases and objects; recommended parent of all custom roles |
| PUBLIC | Granted to every user automatically; avoid granting anything sensitive to it |

**Key concepts**
- **Ownership:** every object is owned by one role, which has full control and can grant privileges on it.
- **Role hierarchy:** `GRANT ROLE analyst TO ROLE sysadmin` lets SYSADMIN inherit analyst's privileges. Privileges flow **up** the hierarchy.
- **Primary and secondary roles:** a session has a primary role, used for creating objects and ownership. `USE SECONDARY ROLES ALL` adds privileges from all granted roles for queries.
- **Database roles:** roles scoped inside a database, which can be granted to account roles and included in shares.
- **Future grants:** automatically grant privileges on objects created later in a schema or database (see [Access-role vs functional-role design and future grants](#27-access-role-vs-functional-role-design-and-future-grants)).
- **Managed access schemas:** only the schema owner (or MANAGE GRANTS) can grant privileges on objects, not individual object owners.

**Best practices**
- Grant ACCOUNTADMIN to at least two people (for break-glass), use MFA, and never use it as a default role.
- Don't create objects with ACCOUNTADMIN. Ownership ends up in the wrong place.
- Build custom roles in two layers, access roles and functional roles (see [Access-role vs functional-role design and future grants](#27-access-role-vs-functional-role-design-and-future-grants)), all rolled up under SYSADMIN.
- Keep grants in code (Terraform or SQL in CI) and review them periodically (see [Infrastructure as Code (Terraform) for data platforms](/career/data-engineering-interview-questions/#69-infrastructure-as-code-terraform-for-data-platforms)).

<p class="qb-label">Example</p>

```sql
-- Dialect: Snowflake SQL
USE ROLE USERADMIN;
CREATE ROLE IF NOT EXISTS analyst_finance;
GRANT ROLE analyst_finance TO ROLE SYSADMIN;          -- roll up custom role
GRANT ROLE analyst_finance TO USER priya_k;

USE ROLE SECURITYADMIN;
GRANT USAGE ON WAREHOUSE bi_wh TO ROLE analyst_finance;
GRANT USAGE ON DATABASE analytics TO ROLE analyst_finance;
GRANT USAGE ON SCHEMA analytics.finance TO ROLE analyst_finance;
GRANT SELECT ON ALL TABLES IN SCHEMA analytics.finance TO ROLE analyst_finance;

SHOW GRANTS TO ROLE analyst_finance;
```

<p class="qb-label">Real-world use case</p>

An audit found dozens of tables owned by ACCOUNTADMIN, created by engineers who used it as their default role. The team transferred ownership to a SYSADMIN-rolled deployment role and restricted ACCOUNTADMIN to two break-glass users.

<p class="qb-label">Common mistakes and red flags</p>

- Using ACCOUNTADMIN day to day.
- Custom roles not rolled up to SYSADMIN, which leaves orphaned objects.
- Granting privileges to PUBLIC.

<p class="qb-label">Follow-up questions</p>

- What is the difference between SECURITYADMIN and USERADMIN? — USERADMIN creates users/roles; SECURITYADMIN additionally manages all grants.
- How do secondary roles help? — Users can query with privileges from all their roles without switching, while ownership stays with the primary role.

</details>

### 14. Snowflake editions and what each adds

<p class="qb-meta">Beginner · Common · Platform</p>

<p class="qb-label">Short answer</p>

Snowflake has four editions. Standard covers core features, with Time Travel up to 1 day. Enterprise adds up to 90 days of Time Travel, multi-cluster warehouses, materialized views, search optimization, masking and row access policies, and the Query Acceleration Service. Business Critical adds stronger security and compliance (Tri-Secret Secure, PrivateLink support, HIPAA/PCI-oriented controls) and failover. Virtual Private Snowflake (VPS) is a fully isolated deployment.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

| Feature area | Standard | Enterprise | Business Critical | VPS |
|---|---|---|---|---|
| Time Travel max | 1 day | 90 days | 90 days | 90 days |
| Multi-cluster warehouses | — | ✓ | ✓ | ✓ |
| Materialized views, search optimization, QAS | — | ✓ | ✓ | ✓ |
| Column-level security (masking), row access policies | — | ✓ | ✓ | ✓ |
| Periodic rekeying of encrypted data | — | ✓ | ✓ | ✓ |
| Tri-Secret Secure (customer-managed key) | — | — | ✓ | ✓ |
| Private connectivity (PrivateLink, etc.) | — | — | ✓ | ✓ |
| Failover/failback (failover groups) | — | — | ✓ | ✓ |
| Support for regulated data (e.g. HIPAA, PCI DSS) | — | — | ✓ | ✓ |
| Dedicated, isolated environment | — | — | — | ✓ |

Feature-to-edition mappings change over time (features move down to lower editions or get new prerequisites). Always confirm against Snowflake's current edition documentation before advising.

**How editions affect design**
- On Standard, you can't rely on masking policies or multi-cluster warehouses, so you need different patterns (secure views per audience, separate warehouses).
- Compliance requirements such as PHI or PCI often mandate Business Critical.
- The credit price differs by edition, so a higher edition costs more per credit.

<p class="qb-label">Example</p>

```sql
-- Dialect: Snowflake SQL
-- Check the account's edition and region (requires appropriate privileges)
SELECT CURRENT_ACCOUNT(), CURRENT_REGION();
SHOW ORGANIZATION ACCOUNTS;   -- ORGADMIN/GLOBALORGADMIN: lists accounts with EDITION column
```

<p class="qb-label">Real-world use case</p>

A healthtech startup on Enterprise needed to store PHI under a business associate agreement. It moved its production account to Business Critical and kept development accounts on Enterprise to manage cost.

<p class="qb-label">Common mistakes and red flags</p>

- Designing with masking policies on a Standard account.
- Assuming 90-day Time Travel on Standard.
- Not considering the edition's impact on credit price.

<p class="qb-label">Follow-up questions</p>

- What is Tri-Secret Secure? — Data keys protected by a composite of a Snowflake-managed key and a customer-managed key in the cloud KMS.
- Can different accounts in an org have different editions? — Yes.

</details>

### 15. INFORMATION_SCHEMA vs ACCOUNT_USAGE (latency and retention)

<p class="qb-meta">Beginner · Common · Metadata</p>

<p class="qb-label">Short answer</p>

Each database's `INFORMATION_SCHEMA` gives near-real-time metadata about that database's objects, plus table functions with short history (typically 7–14 days). Dropped objects are not included. `SNOWFLAKE.ACCOUNT_USAGE` covers the whole account, includes dropped objects, keeps about a year of history, and has latency from roughly 45 minutes to a few hours.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

| Aspect | INFORMATION_SCHEMA | ACCOUNT_USAGE |
|---|---|---|
| Scope | One database (plus account-level table functions) | Entire account |
| Latency | None / near-real-time | Typically 45 min – 3 h (varies by view) |
| History retention | Short (e.g. 7 days for QUERY_HISTORY functions; 14 days for COPY_HISTORY) | ~365 days for most views |
| Dropped objects | Not included | Included (with DELETED timestamp) |
| Access | Objects the current role can see | Requires privileges on the SNOWFLAKE database (e.g. via database roles) |
| Typical use | Live checks inside pipelines, recent history | Auditing, cost analysis, long-term trends |

**Representative views**
- INFORMATION_SCHEMA: `TABLES`, `COLUMNS`, `VIEWS`, and table functions `QUERY_HISTORY()`, `COPY_HISTORY()`, `TASK_HISTORY()`, `DYNAMIC_TABLE_REFRESH_HISTORY()`, `WAREHOUSE_METERING_HISTORY()`.
- ACCOUNT_USAGE: `QUERY_HISTORY`, `ACCESS_HISTORY`, `LOGIN_HISTORY`, `WAREHOUSE_METERING_HISTORY`, `TABLE_STORAGE_METRICS`, `TAG_REFERENCES`, `GRANTS_TO_ROLES`, `POLICY_REFERENCES`.
- **ORGANIZATION_USAGE** gives cross-account views for the organisation. **READER_ACCOUNT_USAGE** covers reader accounts.

**Practical guidance**
- In a pipeline step that needs "did my COPY just load files?", use the INFORMATION_SCHEMA table functions. ACCOUNT_USAGE may not have the rows yet.
- For dashboards on cost, security and auditing over months, use ACCOUNT_USAGE. Copy it into your own history tables if you need retention beyond a year.
- `SHOW` commands give current state quickly. Use `RESULT_SCAN(LAST_QUERY_ID())` to query their output with SQL.

<p class="qb-label">Example</p>

```sql
-- Dialect: Snowflake SQL
-- Near-real-time: queries in the last hour for the current user's role scope
SELECT query_id, query_text, execution_status, total_elapsed_time
FROM TABLE(analytics.INFORMATION_SCHEMA.QUERY_HISTORY(
       END_TIME_RANGE_START => DATEADD(hour, -1, CURRENT_TIMESTAMP()),
       RESULT_LIMIT => 100));

-- Long-term: who accessed a sensitive table in the last 90 days
SELECT ah.query_start_time, ah.user_name, obj.value:objectName::STRING AS object_name
FROM snowflake.account_usage.access_history ah,
     LATERAL FLATTEN(INPUT => ah.base_objects_accessed) obj
WHERE obj.value:objectName::STRING = 'ANALYTICS.CORE.DIM_CUSTOMER'
  AND ah.query_start_time >= DATEADD(day, -90, CURRENT_TIMESTAMP());
```

<p class="qb-label">Real-world use case</p>

A post-load check in a pipeline queried ACCOUNT_USAGE.COPY_HISTORY, found nothing because of latency, and falsely alerted "no files loaded". Switching to the INFORMATION_SCHEMA `COPY_HISTORY()` table function fixed the false alarms.

<p class="qb-label">Common mistakes and red flags</p>

- Using ACCOUNT_USAGE for real-time pipeline checks.
- Expecting INFORMATION_SCHEMA to show dropped tables.
- Granting broad SNOWFLAKE database access instead of specific database roles.

<p class="qb-label">Follow-up questions</p>

- How do you keep history beyond 365 days? — Schedule incremental copies of ACCOUNT_USAGE views into your own tables.
- What is ACCESS_HISTORY? — An Enterprise+ view recording which objects and columns each query read or wrote, used for auditing and lineage.

</details>

## Intermediate Snowflake interview questions

### 16. Scale up vs scale out: multi-cluster warehouses and scaling policies

<p class="qb-meta">Intermediate · Very Common · Virtual Warehouses</p>

<p class="qb-label">Short answer</p>

Scaling up means a bigger warehouse size, for faster complex queries. Scaling out means a multi-cluster warehouse that adds identical clusters when queries queue, for more concurrency. Multi-cluster (Enterprise edition and above) runs between a minimum and maximum cluster count, under a Standard (favours performance) or Economy (favours cost) scaling policy.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**When to scale up**
- Individual queries are slow because of large scans, joins or sorts.
- The Query Profile shows **spilling** to local or remote storage.
- Batch jobs that would benefit from more parallelism.

**When to scale out**
- Many concurrent users or queries, with time spent **queued** (`QUEUED_OVERLOAD_TIME` in query history).
- Dashboards at 9 AM, or many small concurrent ELT jobs.

**Multi-cluster settings**
- `MIN_CLUSTER_COUNT` and `MAX_CLUSTER_COUNT` (up to a documented maximum, which varies by size).
- **Maximized mode:** min = max, so all clusters always run when the warehouse runs. Use for predictable, consistently high concurrency.
- **Auto-scale mode:** min < max. Snowflake starts and stops clusters based on load.

**Scaling policies (auto-scale mode)**
| Policy | Starts extra cluster | Shuts down cluster | Goal |
|---|---|---|---|
| Standard (default) | As soon as queries queue (or are predicted to) | After 2–3 consecutive checks show load fits on fewer clusters | Minimise queuing |
| Economy | Only if it estimates enough load to keep the new cluster busy for ~6 minutes | After 5–6 consecutive checks | Conserve credits; tolerate some queuing |

**Cost:** each running cluster bills at the warehouse size's rate. A Medium warehouse with 3 clusters running burns 12 credits per hour.

**Query acceleration** is another lever for outlier queries with large scans (see [Query Acceleration Service](#45-query-acceleration-service)).

<p class="qb-label">Example</p>

```sql
-- Dialect: Snowflake SQL
CREATE OR REPLACE WAREHOUSE bi_wh
  WAREHOUSE_SIZE = 'MEDIUM'
  MIN_CLUSTER_COUNT = 1
  MAX_CLUSTER_COUNT = 4
  SCALING_POLICY = 'STANDARD'
  AUTO_SUSPEND = 300
  AUTO_RESUME = TRUE;

-- Diagnose: queued vs execution time for the warehouse
SELECT DATE_TRUNC('hour', start_time) AS hr,
       AVG(queued_overload_time) / 1000 AS avg_queue_sec,
       AVG(execution_time) / 1000       AS avg_exec_sec
FROM snowflake.account_usage.query_history
WHERE warehouse_name = 'BI_WH' AND start_time > DATEADD(day, -7, CURRENT_TIMESTAMP())
GROUP BY 1 ORDER BY 1;
```

<p class="qb-label">Real-world use case</p>

Monday-morning dashboards queued for 40 seconds on a Large warehouse. Moving to a Medium multi-cluster warehouse (1–4 clusters, Standard policy) removed queuing and cost less overall, because extra clusters ran only during the peak.

<p class="qb-label">Common mistakes and red flags</p>

- Scaling up to fix queuing.
- Maximized mode with a high cluster count left on all day.
- Not knowing multi-cluster requires Enterprise edition or higher.

<p class="qb-label">Follow-up questions</p>

- Do clusters share a cache? — No; each cluster has its own local disk cache.
- What about the result cache? — Shared across warehouses through the cloud services layer (see [Snowflake caching layers: result, warehouse (local disk) and metadata](#18-snowflake-caching-layers-result-warehouse-local-disk-and-metadata)).

</details>

### 17. Clustering keys, clustering depth and Automatic Clustering

<p class="qb-meta">Intermediate · Very Common · Performance</p>

<p class="qb-label">Short answer</p>

A clustering key tells Snowflake which columns to co-locate data by in micro-partitions, so filters on those columns prune well. The Automatic Clustering service reclusters in the background as data changes and is billed as serverless credits. Use clustering keys only on large, frequently queried tables whose natural load order doesn't match common filters.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**When to cluster**
- Large tables, typically multi-terabyte or with many thousands of micro-partitions. Small tables gain little.
- Queries frequently filter (or join) on the candidate column(s), and the profile shows poor pruning.
- The table is queried far more often than it changes, because reclustering cost grows with DML churn.

**Choosing keys**
- Prefer columns used in **range or equality filters**, often a date plus one more column.
- Order the key from **lower to higher cardinality**, usually with no more than 3–4 columns.
- For very high-cardinality columns (timestamps, IDs), cluster on an **expression** that reduces cardinality: `TO_DATE(event_ts)` or `TRUNC(customer_id, -3)`.
- Avoid keys with extremely low cardinality (a boolean) or extreme cardinality (a unique ID), which give little benefit for the cost.

**Measuring**
- `SYSTEM$CLUSTERING_INFORMATION('tbl', '(cols)')` returns average depth, average overlaps and a partition-depth histogram.
- `SYSTEM$CLUSTERING_DEPTH` returns the average depth only.
- Track pruning ratios in the Query Profile before and after.

**Automatic Clustering**
- Enabled when you define a clustering key. It can be suspended or resumed per table (`ALTER TABLE ... SUSPEND RECLUSTER`).
- Cost is visible in `AUTOMATIC_CLUSTERING_HISTORY`.
- Initial clustering of a large existing table can be expensive. Sometimes it's cheaper to rebuild the table once, sorted (`CREATE TABLE ... AS SELECT ... ORDER BY`), then let automatic clustering maintain it.

<p class="qb-label">Example</p>

```sql
-- Dialect: Snowflake SQL
ALTER TABLE analytics.fct_events CLUSTER BY (event_date, TO_VARCHAR(country_code));

-- Check effect and cost
SELECT SYSTEM$CLUSTERING_INFORMATION('analytics.fct_events');

SELECT DATE_TRUNC('day', start_time) AS d, SUM(credits_used) AS clustering_credits
FROM snowflake.account_usage.automatic_clustering_history
WHERE table_name = 'FCT_EVENTS'
GROUP BY 1 ORDER BY 1;
```

<p class="qb-label">Real-world use case</p>

A 25 TB clickstream table queried by date and country was clustered on `(event_date, country_code)`. Partitions scanned for typical dashboard queries fell from about 70% to 4%. Clustering cost about 30 credits a day, far below the savings in query credits.

<p class="qb-label">Common mistakes and red flags</p>

- Clustering small tables.
- Using a unique ID or raw timestamp as the key.
- Ignoring reclustering cost on heavily updated tables.

<p class="qb-label">Follow-up questions</p>

- Clustering vs search optimization? — Clustering improves range/equality pruning on a few columns; search optimization accelerates selective lookups on many columns (see [Search Optimization Service vs clustering](#38-search-optimization-service-vs-clustering)).
- Does clustering guarantee sort order? — No; it improves co-location, not a strict global sort.

</details>

### 18. Snowflake caching layers: result, warehouse (local disk) and metadata

<p class="qb-meta">Intermediate · Very Common · Performance</p>

<p class="qb-label">Short answer</p>

The result cache (cloud services layer) returns the stored result of an identical query when the underlying data hasn't changed. It is kept for 24 hours, renewed on each reuse up to 31 days, and shared across warehouses. The warehouse (local disk) cache keeps recently read micro-partition data on the warehouse's SSDs and is lost when the warehouse suspends. Metadata answers some queries, such as `COUNT(*)` and `MIN`/`MAX`, without reading data.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

| Cache | Lives in | Lifetime | Shared? | Needs warehouse? |
|---|---|---|---|---|
| Result cache | Cloud services | 24 h, extended on reuse up to 31 days from first execution | Yes, across users/warehouses (role must have access) | No |
| Warehouse / local disk cache | Warehouse SSD/memory | While warehouse runs; dropped on suspend, may be reduced on resize | Per warehouse (per cluster) | Yes |
| Metadata | Cloud services | Always current | Yes | No (for metadata-only queries) |

**Result cache conditions:** the new query must be syntactically equivalent, the underlying data and micro-partitions unchanged, no non-deterministic functions such as `CURRENT_TIMESTAMP()` or `RANDOM()` (with some exceptions), no UDFs or external functions that prevent reuse, and a role with privileges on the objects. It can be disabled per session with `USE_CACHED_RESULT = FALSE`, which is useful in benchmarking.

**Warehouse cache implications**
- Repeated BI queries over the same recent data run faster on a warm warehouse.
- Aggressive auto-suspend saves credits but loses the warm cache. Balance it for BI warehouses (see [Virtual warehouses: sizes, credits, auto-suspend and auto-resume](#2-virtual-warehouses-sizes-credits-auto-suspend-and-auto-resume)).
- Different warehouses don't share local caches.

**Metadata cache**
- `SELECT COUNT(*) FROM t`, and `MIN`/`MAX` on some columns, are answered from micro-partition metadata.
- `SHOW` and `DESCRIBE` commands and information schema lookups are served by cloud services.

**Benchmarking tip:** disable the result cache and use a fresh warehouse (or suspend and resume) to measure true cold performance. Otherwise improvements are illusory.

<p class="qb-label">Example</p>

```sql
-- Dialect: Snowflake SQL
ALTER SESSION SET USE_CACHED_RESULT = FALSE;     -- benchmark without result cache

SELECT region, SUM(amount) FROM analytics.fct_orders GROUP BY region;

-- Reuse a previous result explicitly and post-process it
SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) WHERE "SUM(AMOUNT)" > 100000;
```

<p class="qb-label">Real-world use case</p>

A dashboard tile ran the same query for hundreds of users each morning. After the first run, every request was a result-cache hit with zero warehouse cost, until the nightly load changed the table.

<p class="qb-label">Common mistakes and red flags</p>

- Claiming the warehouse cache survives suspension.
- Benchmarking with the result cache on.
- Putting `CURRENT_TIMESTAMP()` into dashboard queries needlessly, which disables result reuse.

<p class="qb-label">Follow-up questions</p>

- Does a result-cache hit cost credits? — Generally no warehouse credits; it is served by cloud services.
- Does DML on the table invalidate the result cache? — Yes, any change to the underlying micro-partitions does.

</details>

### 19. COPY INTO options and load metadata

<p class="qb-meta">Intermediate · Very Common · Data Loading</p>

<p class="qb-label">Short answer</p>

`COPY INTO <table>` bulk-loads staged files using a warehouse. Its key options control errors (`ON_ERROR`), dry runs (`VALIDATION_MODE`), file selection (`FILES`, `PATTERN`), column mapping (`MATCH_BY_COLUMN_NAME`), cleanup (`PURGE`) and reloading (`FORCE`). Snowflake keeps load metadata per table for 64 days and skips files it already loaded, which makes repeated COPY runs idempotent by default.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Important options**
| Option | Purpose |
|---|---|
| `FILE_FORMAT` | Type and parsing (CSV delimiter, skip header, null strings, JSON strip outer array, etc.) |
| `ON_ERROR` | `ABORT_STATEMENT` (default for bulk COPY), `CONTINUE`, `SKIP_FILE`, `SKIP_FILE_<n>`, `SKIP_FILE_<n>%` |
| `VALIDATION_MODE` | `RETURN_ERRORS` / `RETURN_<n>_ROWS` / `RETURN_ALL_ERRORS`: validate without loading |
| `PATTERN` / `FILES` | Restrict which staged files load |
| `MATCH_BY_COLUMN_NAME` | Map Parquet/ORC/Avro/JSON (and CSV with header parsing) fields to columns by name |
| `PURGE` | Delete files from the stage after a successful load |
| `FORCE` | Reload files even if load metadata says they were loaded (risk of duplicates) |
| `LOAD_UNCERTAIN_FILES` | Load files whose load status is unknown (older than 64 days) |
| `TRUNCATECOLUMNS` / `ENFORCE_LENGTH` | Handle strings longer than the column |
| `INCLUDE_METADATA` | Map file metadata (file name, row number, last modified) into columns |

**Load metadata and idempotency**
- Each table remembers which files (by name and checksum or ETag) it loaded in the last **64 days**. Re-running the same COPY skips them.
- Files older than 64 days with unknown status are skipped unless you set `LOAD_UNCERTAIN_FILES = TRUE`.
- **TRUNCATE TABLE clears load metadata** for that table, so files may reload (see [DELETE vs TRUNCATE vs DROP](/sql/sql-interview-questions/#14-delete-vs-truncate-vs-drop)).

**Transformations during load:** `COPY INTO t FROM (SELECT $1:order_id::NUMBER, METADATA$FILENAME, ... FROM @stage)` supports column reordering, casting and metadata columns. Joins and complex logic are not supported in COPY transformations.

**Monitoring:** `COPY_HISTORY` (table function or ACCOUNT_USAGE view) and `LOAD_HISTORY` show files, rows, errors and status.

<p class="qb-label">Example</p>

```sql
-- Dialect: Snowflake SQL
-- Dry run first
COPY INTO raw.orders FROM @raw.s3_orders
  PATTERN = '.*dt=2026-03-01/.*[.]parquet'
  MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
  VALIDATION_MODE = RETURN_ERRORS;

-- Real load with file lineage columns
COPY INTO raw.orders (order_id, customer_id, amount, order_ts, _source_file, _loaded_at)
FROM (
  SELECT $1:order_id::NUMBER, $1:customer_id::NUMBER, $1:amount::NUMBER(12,2),
         $1:order_ts::TIMESTAMP_NTZ, METADATA$FILENAME, CURRENT_TIMESTAMP()
  FROM @raw.s3_orders
)
PATTERN = '.*dt=2026-03-01/.*[.]parquet'
ON_ERROR = 'SKIP_FILE';

SELECT file_name, status, row_count, first_error_message
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(TABLE_NAME => 'RAW.ORDERS',
                                           START_TIME => DATEADD(hour, -6, CURRENT_TIMESTAMP())));
```

<p class="qb-label">Real-world use case</p>

A loader used `FORCE = TRUE` "to be safe" and duplicated two days of data when a job was retried. Removing FORCE restored Snowflake's built-in file-level idempotency.

<p class="qb-label">Common mistakes and red flags</p>

- `FORCE = TRUE` as a default.
- `ON_ERROR = CONTINUE` without monitoring rejected rows.
- Not knowing about the 64-day load metadata window.

<p class="qb-label">Follow-up questions</p>

- How do you see rejected rows? — `VALIDATE(table, JOB_ID => '...')` returns errors from a past COPY.
- Does Snowpipe use the same metadata? — Snowpipe keeps its own load history (14 days) per pipe (see [Snowpipe auto-ingest](#10-snowpipe-auto-ingest)).

</details>

### 20. File sizing and format best practices for bulk loading

<p class="qb-meta">Intermediate · Common · Data Loading</p>

<p class="qb-label">Short answer</p>

Aim for compressed files of roughly 100–250 MB. That lets COPY parallelise across the warehouse's threads without the overhead of many tiny files or the serial bottleneck of one huge file. Prefer columnar or typed formats (Parquet) or compressed delimited files, organise files in date-based paths, and keep schemas consistent.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Why size matters**
- A warehouse loads files in parallel, one file per thread. An X-Small has 8 threads. Larger sizes have proportionally more.
- **One 20 GB file** loads on a single thread, leaving the warehouse mostly idle.
- **A million 10 KB files** spend most of their time on per-file overhead (listing, opening, load metadata). Snowpipe also charges a small per-file overhead (see [Snowpipe auto-ingest](#10-snowpipe-auto-ingest)).
- About **100–250 MB compressed** per file is Snowflake's long-standing guidance for COPY and Snowpipe.

**Format choices**
| Format | Notes |
|---|---|
| Parquet | Typed, columnar, compressed; supports `MATCH_BY_COLUMN_NAME` and schema detection |
| CSV (gzip) | Fast and simple; define delimiter, quoting, `NULL_IF`, `SKIP_HEADER`, `FIELD_OPTIONALLY_ENCLOSED_BY` |
| JSON | Flexible, but verbose; load into VARIANT or map fields; split very large arrays (`STRIP_OUTER_ARRAY`) |
| Avro / ORC | Supported; Avro common from Kafka pipelines |

**Organisation**
- Use partition-like paths (`/orders/dt=2026-03-01/hour=05/`) so COPY can target subsets with `PATTERN`, and lifecycle rules can expire old prefixes.
- Avoid one giant flat prefix with millions of files, which makes listing slow.
- Keep a consistent schema per path. Use schema evolution deliberately (see [INFER_SCHEMA and table schema evolution](#25-infer_schema-and-table-schema-evolution)).

**Warehouse sizing for loads**
- Loading many files benefits from bigger warehouses only up to the point where files outnumber threads. With 10 files, an XL warehouse is wasted.
- For continuous small batches, Snowpipe or Snowpipe Streaming is usually cheaper than keeping a warehouse running (see [Snowpipe vs Snowpipe Streaming vs COPY INTO](#39-snowpipe-vs-snowpipe-streaming-vs-copy-into)).

<p class="qb-label">Example</p>

```sql
-- Dialect: Snowflake SQL
CREATE OR REPLACE FILE FORMAT raw.ff_csv_gz
  TYPE = CSV
  COMPRESSION = GZIP
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  NULL_IF = ('', 'NULL', '\\N')
  EMPTY_FIELD_AS_NULL = TRUE;

-- Load one hour's partition of files
COPY INTO raw.web_events
FROM @raw.s3_events
PATTERN = '.*dt=2026-03-01/hour=05/.*[.]csv[.]gz'
FILE_FORMAT = (FORMAT_NAME = 'raw.ff_csv_gz');
```

<p class="qb-label">Real-world use case</p>

A vendor sent one 40 GB gzipped CSV nightly, and the load took 90 minutes on a Large warehouse. Asking the vendor to split it into about 200 files of 200 MB each brought the load down to 6 minutes on a Medium warehouse.

<p class="qb-label">Common mistakes and red flags</p>

- Scaling up the warehouse to load a single huge file.
- Streaming tiny files into COPY every few seconds.
- No `NULL_IF` or quoting rules, so NULLs load as strings.

<p class="qb-label">Follow-up questions</p>

- How do you split a huge file? — Upstream export settings, or a pre-processing step (cloud function/Spark) that rewrites into chunks.
- Does Parquet need the same sizing? — Yes; similar guidance applies, with row groups inside files.

</details>

### 21. Streams: standard, append-only, insert-only; offsets and staleness

<p class="qb-meta">Intermediate · Very Common · Streams & Tasks</p>

<p class="qb-label">Short answer</p>

A stream is a change-tracking object on a table (or view) that records an offset and returns the rows changed since that offset, with metadata columns `METADATA$ACTION`, `METADATA$ISUPDATE` and `METADATA$ROW_ID`. Consuming the stream in a DML statement that commits advances the offset. Standard streams track inserts, updates and deletes. Append-only streams track inserts only. Insert-only streams are for external and Iceberg tables.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Types**
| Type | Tracks | Use on |
|---|---|---|
| Standard (delta) | Inserts, updates (as DELETE + INSERT pair), deletes; net changes | Tables, views, directory tables |
| Append-only | Inserts only (cheaper; ignores updates/deletes) | Tables, views (e.g. insert-only raw landing tables) |
| Insert-only | Inserts only | External tables, Iceberg tables |

**How the offset works**
- A stream stores no data. It is a pointer into the table's version history.
- Querying a stream with SELECT doesn't advance it. Using it as a source of **DML in a committed transaction** (INSERT, MERGE, CTAS) advances the offset to the time of that transaction.
- Every statement in one transaction sees the same change set, so you can read the stream several times within one explicit transaction.
- Standard streams return **net** changes: a row inserted and then deleted between offsets doesn't appear.

**Staleness**
- A stream becomes **stale** if its offset falls outside the source table's data retention period. Then its changes can no longer be returned.
- To help, Snowflake temporarily extends the table's retention for unconsumed streams, up to `MAX_DATA_EXTENSION_TIME_IN_DAYS` (default 14 days).
- Check `SHOW STREAMS` (`stale`, `stale_after`). Consume regularly, at least within the extension window.
- A stale stream must be recreated, and you need a re-sync strategy such as a full compare.

**Useful functions**
- `SYSTEM$STREAM_HAS_DATA('stream')` lets a task skip runs when there are no changes, which saves credits (see [Tasks: schedules, task graphs and serverless tasks](#11-tasks-schedules-task-graphs-and-serverless-tasks)).
- Streams on **views** (including secure views) track changes in the underlying tables. There are limits on supported view types.

<p class="qb-label">Example</p>

```sql
-- Dialect: Snowflake SQL
CREATE OR REPLACE STREAM raw.orders_stream ON TABLE raw.orders APPEND_ONLY = FALSE;

-- Inspect pending changes (does not consume)
SELECT order_id, METADATA$ACTION, METADATA$ISUPDATE
FROM raw.orders_stream;

SHOW STREAMS LIKE 'ORDERS_STREAM' IN SCHEMA raw;   -- check STALE and STALE_AFTER
```

Sample: order 1 inserted, then its amount updated before the stream is consumed. The stream shows one INSERT row with the latest values (net change for a new row). If order 2 existed before the offset and was updated, it shows a DELETE (old) and an INSERT (new) row, both with `METADATA$ISUPDATE = TRUE`.

<p class="qb-label">Real-world use case</p>

A Snowpipe-fed `raw.orders` table has an append-only stream. A task merges new rows into `silver.orders` every 5 minutes and only runs when `SYSTEM$STREAM_HAS_DATA` returns TRUE.

<p class="qb-label">Common mistakes and red flags</p>

- Expecting SELECT to consume the stream.
- Two consumers sharing one stream. Each consumer needs its own stream, or one consumer's commit advances the offset for both.
- Letting streams go stale on rarely processed tables.

<p class="qb-label">Follow-up questions</p>

- How do you handle updates in a MERGE from a standard stream? — Filter `METADATA$ACTION = 'INSERT'` rows for new values (including the insert half of updates) and use DELETE rows where `METADATA$ISUPDATE = FALSE` for true deletes.
- Can multiple streams exist on one table? — Yes, each with its own independent offset.

</details>

### 22. Dynamic Tables: target lag and refresh modes

<p class="qb-meta">Intermediate · Very Common · Dynamic Tables</p>

<p class="qb-label">Short answer</p>

A Dynamic Table is defined by a SELECT query and a target lag, which is how stale its data may get relative to its sources. Snowflake automatically refreshes it, incrementally when the query supports it or with a full refresh otherwise, and orchestrates chains of dynamic tables. This replaces hand-written Streams and Tasks for many declarative transformation pipelines.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Key properties**
- `TARGET_LAG = '5 minutes'` (or hours or days), or `DOWNSTREAM`, which means "refresh only as often as dependent dynamic tables need".
- `WAREHOUSE = ...` is the warehouse used for refreshes (dynamic table refreshes run on a user-specified warehouse).
- `REFRESH_MODE = AUTO | INCREMENTAL | FULL`. AUTO picks incremental if the query supports it. Setting INCREMENTAL explicitly makes creation fail if the query can't be refreshed incrementally, which is useful for catching costly full refreshes.
- `INITIALIZE = ON_CREATE | ON_SCHEDULE` controls whether the first refresh happens immediately.

**Incremental refresh support**
- Works for many common constructs: projections, filters, inner and outer joins, GROUP BY aggregates, UNION ALL, many window functions, and QUALIFY in some forms.
- Some functions and constructs force FULL refresh, or are unsupported for incremental. Check `SHOW DYNAMIC TABLES` (the `refresh_mode` and `refresh_mode_reason` columns) and the docs' supported-constructs list, which grows over time.
- Non-deterministic functions such as `CURRENT_TIMESTAMP` have restrictions.

**Operations**
- Monitor with `DYNAMIC_TABLE_REFRESH_HISTORY`, the Snowsight graph view, and `DYNAMIC_TABLE_GRAPH_HISTORY`.
- Manual refresh with `ALTER DYNAMIC TABLE ... REFRESH`. Suspend and resume per table.
- Chained dynamic tables refresh in dependency order with snapshot consistency across the graph.

**Lag and cost trade-off:** a shorter lag means more frequent refreshes and more warehouse time. Choose the lag from consumer needs, and set intermediate tables to `DOWNSTREAM`.

<p class="qb-label">Example</p>

```sql
-- Dialect: Snowflake SQL
CREATE OR REPLACE DYNAMIC TABLE silver.orders_clean
  TARGET_LAG = DOWNSTREAM
  WAREHOUSE = transform_wh
  REFRESH_MODE = INCREMENTAL
AS
SELECT order_id, customer_id, amount, order_ts::DATE AS order_date
FROM raw.orders
WHERE amount IS NOT NULL;

CREATE OR REPLACE DYNAMIC TABLE gold.daily_revenue
  TARGET_LAG = '15 minutes'
  WAREHOUSE = transform_wh
AS
SELECT order_date, SUM(amount) AS revenue, COUNT(*) AS orders
FROM silver.orders_clean
GROUP BY order_date;

SELECT name, refresh_action, state, refresh_start_time, refresh_end_time
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(NAME => 'GOLD.DAILY_REVENUE'))
ORDER BY refresh_start_time DESC LIMIT 10;
```

<p class="qb-label">Real-world use case</p>

A team replaced 12 streams, 12 tasks and their MERGE statements with 6 dynamic tables. The pipeline became plain SELECT statements, with lag managed by Snowflake and fewer moving parts to monitor.

<p class="qb-label">Common mistakes and red flags</p>

- Setting a 1-minute lag everywhere "for freshness".
- Not checking whether refreshes are incremental or full.
- Expecting dynamic tables to handle procedural logic or side effects.

<p class="qb-label">Follow-up questions</p>

- What's the minimum target lag? — One minute (check current limits).
- Can dbt create dynamic tables? — Yes, with the `dynamic_table` materialization in dbt-snowflake.

</details>

### 23. Materialized views in Snowflake and their limitations

<p class="qb-meta">Intermediate · Common · Performance</p>

<p class="qb-label">Short answer</p>

Snowflake materialized views (Enterprise edition and above) store the precomputed results of a query on a single table and are kept up to date by a serverless background service. Queries always return current data. They can't contain joins, window functions, most non-deterministic functions, UDFs or HAVING, and only some aggregate functions are supported.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**How they work**
- Background maintenance applies base-table changes to the materialized results, billed as serverless credits plus storage.
- If maintenance is behind, Snowflake combines the materialized data with the newest base-table changes at query time, so results are always current.
- The optimizer may **automatically rewrite** queries against the base table to use a matching materialized view.
- You can define clustering keys on the materialized view itself, which helps when it is organised differently from the base table.

**Main limitations (check current documentation for exact rules)**
- **Single base table only:** no joins, including self joins.
- No window functions, no HAVING, no ORDER BY, no LIMIT, and no UDFs (including external functions).
- Aggregates limited to a supported set (for example SUM, COUNT, MIN, MAX, AVG, APPROX_COUNT_DISTINCT and some others). COUNT(DISTINCT) has restrictions.
- No non-deterministic functions such as `CURRENT_TIMESTAMP`.
- Can't be built on another materialized view, on a standard view or (generally) on a dynamic table.
- DML directly on the materialized view isn't allowed.

**Good use cases**
- A large base table queried repeatedly with the same aggregation or filter.
- A different clustering for a selective subset: a materialized view filtered to recent data and clustered differently from the base table.
- Pre-projecting a few columns from very wide tables, or flattening semi-structured fields.

**Avoid when** the base table changes constantly and is rarely queried, because maintenance cost exceeds query savings. For multi-table logic, use Dynamic Tables (see [Dynamic Tables vs Streams & Tasks vs materialized views](#41-dynamic-tables-vs-streams--tasks-vs-materialized-views)).

<p class="qb-label">Example</p>

```sql
-- Dialect: Snowflake SQL
CREATE OR REPLACE MATERIALIZED VIEW analytics.mv_sales_by_store_day
  CLUSTER BY (store_id)
AS
SELECT store_id, sale_date, SUM(amount) AS revenue, COUNT(*) AS txns
FROM analytics.fct_sales
GROUP BY store_id, sale_date;

-- Maintenance cost
SELECT DATE_TRUNC('day', start_time) AS d, SUM(credits_used) AS mv_credits
FROM snowflake.account_usage.materialized_view_refresh_history
WHERE table_name = 'MV_SALES_BY_STORE_DAY'
GROUP BY 1 ORDER BY 1;
```

<p class="qb-label">Real-world use case</p>

Store managers' dashboards filtered `fct_sales` (clustered by date) by `store_id`, which pruned poorly. A materialized view aggregated to store-day and clustered by `store_id` made the dashboards fast, at modest maintenance cost.

<p class="qb-label">Common mistakes and red flags</p>

- Trying to put joins in a materialized view.
- Not monitoring maintenance cost.
- Expecting them on Standard edition.

<p class="qb-label">Follow-up questions</p>

- Are MV results ever stale? — No; Snowflake returns current results even if background refresh is behind.
- Secure materialized views? — Supported, for sharing or hiding definitions.

</details>

### 24. LATERAL FLATTEN for nested JSON

<p class="qb-meta">Intermediate · Very Common · Semi-Structured Data</p>

<p class="qb-label">Short answer</p>

`FLATTEN` is a table function that expands an array or object into one row per element. Each row exposes `VALUE`, `KEY`, `INDEX`, `PATH`, `SEQ` and `THIS`. Use it with `LATERAL` (or `TABLE(FLATTEN(...))` in a comma join) so each parent row joins to its elements. `OUTER => TRUE` keeps parents with empty arrays, and nested arrays need chained FLATTENs.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Parameters**
- `INPUT => expr`: the array or object to expand.
- `PATH => 'items'`: an optional path inside the input.
- `OUTER => TRUE`: emit one row with NULL values when the input is empty or missing. Like a LEFT JOIN, this keeps the parent.
- `RECURSIVE => TRUE`: expand all nested levels. It's useful for discovering keys, but rarely for modelling.
- `MODE => 'ARRAY' | 'OBJECT' | 'BOTH'`: what to expand.

**Output columns**
| Column | Meaning |
|---|---|
| `VALUE` | The element (VARIANT) |
| `KEY` | Key name (for objects) |
| `INDEX` | Position in array |
| `PATH` | Path to the element |
| `SEQ` | Unique sequence per input row (useful for grouping back) |
| `THIS` | The element being flattened |

**Patterns**
- **Order → line items:** one FLATTEN over `payload:items`.
- **Nested arrays:** `items[*].discounts[*]` needs a second FLATTEN on `f1.value:discounts`.
- **Objects to key-value rows:** FLATTEN an object to get `KEY` and `VALUE` (attribute maps).
- **Re-aggregate:** `ARRAY_AGG` / `OBJECT_AGG` with GROUP BY the parent key (or `SEQ`).

**Pitfalls**
- Without `OUTER => TRUE`, parent rows with empty or NULL arrays disappear, which silently drops orders with no items.
- Flattening multiple independent arrays from the same parent in one query creates a **Cartesian product** (items × payments). Flatten them in separate CTEs and join by the parent key.

<p class="qb-label">Example</p>

```sql
-- Dialect: Snowflake SQL
-- payload: {"order_id":"O1","items":[{"sku":"A1","qty":2,"discounts":[{"code":"X","amt":1.5}]},
--                                      {"sku":"B7","qty":1,"discounts":[]}]}
SELECT o.payload:order_id::STRING   AS order_id,
       i.index                      AS line_no,
       i.value:sku::STRING          AS sku,
       i.value:qty::NUMBER          AS qty,
       d.value:code::STRING         AS discount_code,
       d.value:amt::NUMBER(10,2)    AS discount_amt
FROM raw.orders_json o,
     LATERAL FLATTEN(INPUT => o.payload:items) i,
     LATERAL FLATTEN(INPUT => i.value:discounts, OUTER => TRUE) d;
```
Output: (O1, 0, A1, 2, X, 1.50) and (O1, 1, B7, 1, NULL, NULL). Without `OUTER => TRUE` on the discounts FLATTEN, the B7 line would be lost.

<p class="qb-label">Real-world use case</p>

An e-commerce silver model explodes order JSON into `order_lines` and `line_discounts` tables using FLATTEN with `OUTER => TRUE`. Reconciliation shows the line counts match the source system exactly.

<p class="qb-label">Common mistakes and red flags</p>

- Losing parents with empty arrays.
- Flattening two sibling arrays together, which multiplies rows.
- Using `RECURSIVE => TRUE` for production modelling.

<p class="qb-label">Follow-up questions</p>

- PySpark equivalent? — `explode` / `explode_outer` / `posexplode` (see [explode, flattening nested JSON and arrays](/pyspark/pyspark-interview-questions/#32-explode-flattening-nested-json-and-arrays)).
- How do you discover all keys in messy JSON? — `FLATTEN(..., RECURSIVE => TRUE)` and aggregate distinct `PATH` values with `TYPEOF`.

</details>

### 25. INFER_SCHEMA and table schema evolution

<p class="qb-meta">Intermediate · Common · Data Loading</p>

<p class="qb-label">Short answer</p>

`INFER_SCHEMA` reads staged Parquet, Avro, ORC, JSON or CSV files and returns column names and types. You can create a table from it with `CREATE TABLE ... USING TEMPLATE`. Table schema evolution (`ENABLE_SCHEMA_EVOLUTION = TRUE`) lets COPY and Snowpipe add new columns automatically, or drop NOT NULL constraints, when incoming files contain new fields.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Schema detection**
- `SELECT * FROM TABLE(INFER_SCHEMA(LOCATION => '@stage/path', FILE_FORMAT => 'ff'))` returns `COLUMN_NAME`, `TYPE`, `NULLABLE`, `EXPRESSION`, `FILENAMES` and `ORDER_ID`.
- `CREATE TABLE t USING TEMPLATE (SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*)) FROM TABLE(INFER_SCHEMA(...)))` builds the DDL automatically.
- Useful for onboarding new feeds quickly. Review the inferred types, because numbers may be inferred as NUMBER(38,0) or FLOAT, and timestamps may be inferred from strings unexpectedly.
- For CSV, use a file format with `PARSE_HEADER = TRUE`.

**Schema evolution on load**
- Set `ENABLE_SCHEMA_EVOLUTION = TRUE` on the table, and load with `MATCH_BY_COLUMN_NAME` (for Parquet, Avro, ORC, JSON and CSV with header).
- When files contain new columns, Snowflake adds them to the table. When required columns are missing, it can drop the NOT NULL constraint.
- The role needs `EVOLVE SCHEMA` (or OWNERSHIP) on the table.
- Limits apply: a cap on how many columns can be added per COPY, and type changes are not evolved automatically. Check the docs.

**Governance**
- Automatic evolution is convenient in **bronze or raw** landing. In silver and gold, explicit, reviewed schema changes and contracts are safer (see [What are data contracts, and how do you enforce them?](/career/data-engineering-interview-questions/#83-what-are-data-contracts-and-how-do-you-enforce-them), [Model contracts](/dbt/dbt-interview-questions/#41-model-contracts)).
- Monitor added columns through `INFORMATION_SCHEMA.COLUMNS` changes, or by alerting on DDL in query history.

**Alternative:** land raw data as a single VARIANT column (see [VARIANT and semi-structured data](#12-variant-and-semi-structured-data)). Schema changes then never break loads, and typing happens downstream.

<p class="qb-label">Example</p>

```sql
-- Dialect: Snowflake SQL
SELECT column_name, type, nullable
FROM TABLE(INFER_SCHEMA(LOCATION => '@raw.s3_orders/dt=2026-03-01/',
                        FILE_FORMAT => 'raw.ff_parquet'));

CREATE OR REPLACE TABLE raw.orders_auto
  USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
    FROM TABLE(INFER_SCHEMA(LOCATION => '@raw.s3_orders/dt=2026-03-01/',
                            FILE_FORMAT => 'raw.ff_parquet')))
  ENABLE_SCHEMA_EVOLUTION = TRUE;

COPY INTO raw.orders_auto FROM @raw.s3_orders
  FILE_FORMAT = (FORMAT_NAME = 'raw.ff_parquet')
  MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
```

<p class="qb-label">Real-world use case</p>

A partner added a `loyalty_tier` field to their Parquet export without notice. With schema evolution enabled on the raw table, the load succeeded and the column appeared automatically. A DDL-change alert then prompted the team to model it in silver.

<p class="qb-label">Common mistakes and red flags</p>

- Enabling evolution on curated tables consumed by BI.
- Trusting inferred types blindly.
- Forgetting `MATCH_BY_COLUMN_NAME`, which evolution requires.

<p class="qb-label">Follow-up questions</p>

- What happens if a column's type changes upstream? — Evolution doesn't change types; the load may fail or coerce — handle via VARIANT landing or explicit migration.
- Does Snowpipe support evolution? — Yes, with the same table setting and MATCH_BY_COLUMN_NAME.

</details>

### 26. Secure Data Sharing and reader accounts

<p class="qb-meta">Intermediate · Very Common · Data Sharing</p>

<p class="qb-label">Short answer</p>

Secure Data Sharing lets a provider account grant other Snowflake accounts live, read-only access to selected objects without copying data. The consumer queries the provider's storage with its own compute. A share contains database privileges, and consumers create a database from it. Reader accounts are provider-managed accounts for consumers who don't have Snowflake, with compute paid by the provider.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**How it works**
1. The provider creates a **share** and grants `USAGE` on the database and schemas, plus `SELECT` on tables, secure views, or secure materialized views and UDFs.
2. The provider adds consumer accounts to the share.
3. The consumer runs `CREATE DATABASE ... FROM SHARE provider.share_name` and grants access to its own roles (for example `IMPORTED PRIVILEGES`).
4. Data is **live**: provider updates are visible immediately. No copy or ETL is needed. The provider pays storage, and the consumer pays compute.

**Rules and features**
- Shared objects are **read-only** for consumers.
- Share **secure views** (not regular views) to protect the underlying logic and data. Row-level filtering per consumer can use `CURRENT_ACCOUNT()` in the secure view, or a row access policy (see [Row access policies](#29-row-access-policies)).
- Direct shares work within the **same region and cloud**. Cross-region or cross-cloud sharing uses **listings with auto-fulfillment** or replication (see [Listings and cross-region/cross-cloud sharing](#42-listings-and-cross-regioncross-cloud-sharing)).
- **Reader accounts:** the provider creates and manages them, and pays for their warehouses. Use resource monitors to cap that spend.
- **Data clean rooms** build on sharing for privacy-preserving joins between parties.

**Governance**
- Monitor consumer usage through provider views (`DATA_SHARING_USAGE` schema for listings, and `ACCESS_HISTORY` with appropriate visibility).
- Masking policies on shared objects still apply, evaluated in the consumer context in specific ways. Test carefully, for example with `IS_ROLE_IN_SESSION` vs `CURRENT_ACCOUNT` patterns.

<p class="qb-label">Example</p>

```sql
-- Dialect: Snowflake SQL (provider)
CREATE SHARE partner_sales_share;
GRANT USAGE ON DATABASE analytics TO SHARE partner_sales_share;
GRANT USAGE ON SCHEMA analytics.shared TO SHARE partner_sales_share;
GRANT SELECT ON VIEW analytics.shared.v_partner_sales TO SHARE partner_sales_share;  -- secure view
ALTER SHARE partner_sales_share ADD ACCOUNTS = myorg.partner_acct;

-- Dialect: Snowflake SQL (consumer)
CREATE DATABASE partner_sales FROM SHARE provider_org.provider_acct.partner_sales_share;
GRANT IMPORTED PRIVILEGES ON DATABASE partner_sales TO ROLE analyst;
```
The secure view filters rows with `WHERE partner_account = CURRENT_ACCOUNT()`, so each partner sees only its own data.

<p class="qb-label">Real-world use case</p>

A CPG manufacturer shares daily sell-through data with 30 retailers through one secure view filtered by `CURRENT_ACCOUNT()`. This replaced nightly SFTP extracts, and retailers always see current data.

<p class="qb-label">Common mistakes and red flags</p>

- Sharing regular views (not allowed; secure views are required).
- Expecting consumers to write to shared tables.
- Forgetting reader-account compute is billed to the provider.

<p class="qb-label">Follow-up questions</p>

- Does sharing copy data? — No, for same-region direct shares; cross-region uses replication behind listings.
- How do you revoke access? — Remove the account from the share or revoke grants; access ends immediately.

</details>

### 27. Access-role vs functional-role design and future grants

<p class="qb-meta">Intermediate · Common · Security & RBAC</p>

<p class="qb-label">Short answer</p>

Use two layers of roles. Access roles hold object privileges (for example `AR_SALES_RO` and `AR_SALES_RW` per schema). Functional roles represent job functions (`FR_ANALYST_SALES`, `FR_ENGINEER`) and are granted the right access roles. Users get only functional roles. Future grants ensure new tables automatically get the access-role privileges.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Why two layers**
- Object privileges are defined once per data domain, in access roles.
- Job functions are combinations of access roles, so onboarding is "grant one functional role".
- When a new schema appears, you create its access roles and attach them to the relevant functional roles, without touching every user.

**Typical structure**
```text
User --> FR_ANALYST_SALES --> AR_SALES_RO        --> SELECT on analytics.sales.*
                          --> AR_MARKETING_RO    --> SELECT on analytics.marketing.*
                          --> AR_WH_BI_USAGE     --> USAGE on bi_wh
All FR_* and AR_* --> roll up to SYSADMIN
```

Database roles are a good fit for access roles inside a database. They are also shareable (see [Snowflake RBAC: system roles and role hierarchy](#13-snowflake-rbac-system-roles-and-role-hierarchy)).

**Future grants**
- `GRANT SELECT ON FUTURE TABLES IN SCHEMA analytics.sales TO ROLE ar_sales_ro;`
- Applies to objects created *after* the grant. Combine it with `GRANT ... ON ALL TABLES` for existing objects.
- **Precedence:** schema-level future grants take precedence over database-level future grants for the same object type. If a schema has any future grants defined, database-level future grants for that type don't apply in that schema.
- Managed access schemas centralise grant control.

**Other design points**
- Separate read-only, read-write and admin access roles per schema or domain.
- Use dedicated **service roles** for pipelines (dbt, ingestion) with least privilege.
- Use **row access and masking policies** for fine-grained control instead of making roles per region (see [RBAC vs ABAC](/career/data-engineering-interview-questions/#64-rbac-vs-abac), [Dynamic data masking policies](#28-dynamic-data-masking-policies)).
- Manage everything as code, and review it quarterly.

<p class="qb-label">Example</p>

```sql
-- Dialect: Snowflake SQL
USE ROLE SECURITYADMIN;
CREATE ROLE IF NOT EXISTS ar_sales_ro;
CREATE ROLE IF NOT EXISTS fr_analyst_sales;

GRANT USAGE ON DATABASE analytics TO ROLE ar_sales_ro;
GRANT USAGE ON SCHEMA analytics.sales TO ROLE ar_sales_ro;
GRANT SELECT ON ALL TABLES    IN SCHEMA analytics.sales TO ROLE ar_sales_ro;
GRANT SELECT ON FUTURE TABLES IN SCHEMA analytics.sales TO ROLE ar_sales_ro;
GRANT SELECT ON ALL VIEWS     IN SCHEMA analytics.sales TO ROLE ar_sales_ro;
GRANT SELECT ON FUTURE VIEWS  IN SCHEMA analytics.sales TO ROLE ar_sales_ro;

GRANT ROLE ar_sales_ro       TO ROLE fr_analyst_sales;
GRANT ROLE fr_analyst_sales  TO ROLE SYSADMIN;
GRANT ROLE fr_analyst_sales  TO USER priya_k;
```

<p class="qb-label">Real-world use case</p>

Every new dbt model used to need a manual grant ticket, until the team added future grants on each mart schema to the access roles. New models became queryable by the right analysts the moment they were created.

<p class="qb-label">Common mistakes and red flags</p>

- Granting object privileges directly to functional roles or users.
- Expecting future grants to cover existing objects.
- Mixing database- and schema-level future grants without understanding precedence.

<p class="qb-label">Follow-up questions</p>

- How do you audit who can access a table? — `SHOW GRANTS ON TABLE ...`, `GRANTS_TO_ROLES` / `GRANTS_TO_USERS` in ACCOUNT_USAGE, plus role hierarchy traversal.
- What does dbt do with grants? — The `grants` config applies privileges after each build (see [grants and persist_docs configs](/dbt/dbt-interview-questions/#32-grants-and-persist_docs-configs)).

</details>

### 28. Dynamic data masking policies

<p class="qb-meta">Intermediate · Very Common · Governance</p>

<p class="qb-label">Short answer</p>

A masking policy is a schema-level object containing a SQL expression that decides at query time what value a user sees for a column, based on context such as `CURRENT_ROLE()`, `IS_ROLE_IN_SESSION()` or other columns. You attach it to columns (or tags). The stored data is unchanged, and unauthorised users see masked, partial or NULL values. It requires Enterprise edition or above.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**How it works**
- `CREATE MASKING POLICY p AS (val STRING) RETURNS STRING -> CASE WHEN <authorised> THEN val ELSE <masked> END;`
- `ALTER TABLE t MODIFY COLUMN email SET MASKING POLICY p;`
- Applied whenever the column is read: SELECT, views built on the table, shares, and also the column's appearance in WHERE and JOIN (users filter on masked values).
- **Conditional masking:** the policy signature can take extra columns (for example a `consent_flag`) to decide per row.
- **Tag-based masking:** attach the policy to a tag, and every column carrying the tag is masked automatically (see [Tag-based masking and sensitive data classification](#43-tag-based-masking-and-sensitive-data-classification)).

**Choosing the condition**
- `IS_ROLE_IN_SESSION('PII_READER')` respects role hierarchy and secondary roles. It is usually better than `CURRENT_ROLE() IN (...)`, which checks only the primary role.
- `INVOKER_ROLE()` is relevant inside views and owner's-rights contexts.
- Avoid hardcoding users. Use roles or a mapping table (memoizable functions can help performance).

**Masking styles:** full mask (`'***'`), partial (`'****' || RIGHT(phone, 4)`), hashed (`SHA2(val)` for joinable pseudonyms; see [How do you protect PII: masking, tokenisation, hashing, encryption](/career/data-engineering-interview-questions/#65-how-do-you-protect-pii-masking-tokenisation-hashing-encryption)), NULL, or a date truncated to month or year.

**Operational notes**
- A column can have only one masking policy.
- Policy changes take effect immediately, which supports central governance.
- Separation of duties: a governance role owns policies, and table owners can't remove them (with `APPLY MASKING POLICY` privileges managed centrally).
- Masked values in WHERE clauses can change query results for unauthorised roles. Test critical reports under each role.

<p class="qb-label">Example</p>

```sql
-- Dialect: Snowflake SQL
CREATE OR REPLACE MASKING POLICY governance.policies.mask_email AS (val STRING) RETURNS STRING ->
  CASE
    WHEN IS_ROLE_IN_SESSION('PII_READER') THEN val
    WHEN IS_ROLE_IN_SESSION('SUPPORT_AGENT') THEN REGEXP_REPLACE(val, '^[^@]+', '****')
    ELSE '***MASKED***'
  END;

ALTER TABLE analytics.core.dim_customer
  MODIFY COLUMN email SET MASKING POLICY governance.policies.mask_email;
```
Example results for `priya@example.com`: PII_READER sees `priya@example.com`, SUPPORT_AGENT sees `****@example.com`, and everyone else sees `***MASKED***`.

<p class="qb-label">Real-world use case</p>

A bank masks account numbers for all roles except fraud investigators, and shows the last four digits to call-centre agents. One policy covers 45 tables through tag-based masking.

<p class="qb-label">Common mistakes and red flags</p>

- Using `CURRENT_ROLE()` and breaking under secondary roles or role hierarchy.
- Masking only in BI tools.
- Expecting masking on Standard edition.

<p class="qb-label">Follow-up questions</p>

- What is external tokenization? — Masking policies that call an external function to detokenise values held by a third-party tokenisation provider.
- Do masking policies apply to clones? — Yes; policies stay attached to cloned columns.

</details>

### 29. Row access policies

<p class="qb-meta">Intermediate · Common · Governance</p>

<p class="qb-label">Short answer</p>

A row access policy is a schema-level object with a Boolean expression evaluated per row at query time. Rows where it returns TRUE are visible. You attach it to a table or view on one or more columns. Policies typically consult a mapping table (user or role to allowed regions) through memoizable functions or subqueries. This gives row-level security without separate views per audience. It requires Enterprise edition or above.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Pattern**
1. A **mapping table** says who can see what: `(role_name, region)` or `(user_name, cost_center)`.
2. A policy: `CREATE ROW ACCESS POLICY rap AS (region STRING) RETURNS BOOLEAN -> IS_ROLE_IN_SESSION('ADMIN') OR EXISTS (SELECT 1 FROM mapping m WHERE m.role_name = CURRENT_ROLE() AND m.region = region)`.
3. Attach it: `ALTER TABLE t ADD ROW ACCESS POLICY rap ON (region);`

**Behaviour**
- Evaluated before other filters. Users can't bypass it with clever SQL.
- One row access policy per table or view, but it can take several columns.
- It works together with masking policies. A given column can appear in a row access policy signature or a masking policy signature, but not both.
- In **shares**, use `CURRENT_ACCOUNT()` to filter by consumer account (see [Secure Data Sharing and reader accounts](#26-secure-data-sharing-and-reader-accounts)).

**Performance**
- Policies add a subquery or join per query. Keep mapping tables small, use memoizable functions to cache lookups, and cluster the protected table by the policy column if that column is also a common filter.

**Testing:** `EXECUTE USING POLICY_CONTEXT(CURRENT_ROLE => 'X')` simulates what a role sees. Run it in CI for critical tables.

<p class="qb-label">Example</p>

```sql
-- Dialect: Snowflake SQL
CREATE OR REPLACE TABLE governance.mapping.role_region (role_name STRING, region STRING);
INSERT INTO governance.mapping.role_region VALUES ('FR_SALES_EMEA', 'EMEA'), ('FR_SALES_APAC', 'APAC');

CREATE OR REPLACE ROW ACCESS POLICY governance.policies.rap_region AS (row_region STRING) RETURNS BOOLEAN ->
  IS_ROLE_IN_SESSION('FR_SALES_GLOBAL')
  OR EXISTS (SELECT 1 FROM governance.mapping.role_region m
             WHERE IS_ROLE_IN_SESSION(m.role_name) AND m.region = row_region);

ALTER TABLE analytics.sales.fct_orders ADD ROW ACCESS POLICY governance.policies.rap_region ON (region);

EXECUTE USING POLICY_CONTEXT(CURRENT_ROLE => 'FR_SALES_EMEA')
AS SELECT region, COUNT(*) FROM analytics.sales.fct_orders GROUP BY region;
```
Under FR_SALES_EMEA the result shows only EMEA. Under FR_SALES_GLOBAL it shows all regions.

<p class="qb-label">Real-world use case</p>

A global sales mart serves 12 regional teams from one table. Onboarding a new region is a single insert into the mapping table, replacing 12 copies of regional views that used to drift apart.

<p class="qb-label">Common mistakes and red flags</p>

- Building a view per region instead of a policy.
- Large, unindexed mapping logic that slows every query.
- Not testing policies per role.

<p class="qb-label">Follow-up questions</p>

- How do you see which policies apply to a table? — `POLICY_REFERENCES` table function or ACCOUNT_USAGE view.
- How do you avoid policy logic slowing queries? — Keep the mapping table small, wrap lookups in memoizable functions, and test plans under representative roles.

</details>

### 30. Reading the Query Profile: spilling, exploding joins, poor pruning

<p class="qb-meta">Intermediate · Very Common · Performance</p>

<p class="qb-label">Short answer</p>

The Query Profile shows the operator tree with time and row statistics. Check the most expensive nodes first. Look at partitions scanned vs total (pruning), bytes spilled to local or remote storage (memory pressure), row explosion in joins, a large share of time in remote disk I/O or network, and queuing and compilation time in the overview.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Where to start**
1. **Overview:** compilation time, execution time, queued time (queuing points to concurrency; see [Scale up vs scale out: multi-cluster warehouses and scaling policies](#16-scale-up-vs-scale-out-multi-cluster-warehouses-and-scaling-policies)), and bytes scanned.
2. **Most expensive nodes:** sorted by percentage of total time.
3. **Statistics pane per node:** rows in and out, partitions scanned and total, bytes spilled, network bytes.

**Common problems and fixes**
| Symptom in profile | Likely cause | Fix |
|---|---|---|
| Partitions scanned ≈ total | Filters don't prune (non-sargable, unclustered column) | Sargable predicates (see [What makes a predicate sargable?](/sql/sql-interview-questions/#46-what-makes-a-predicate-sargable)), clustering key (see [Clustering keys, clustering depth and Automatic Clustering](#17-clustering-keys-clustering-depth-and-automatic-clustering)), search optimization (see [Search Optimization Service vs clustering](#38-search-optimization-service-vs-clustering)) |
| Bytes spilled to local storage | Operator memory exceeded | Reduce data earlier, or larger warehouse |
| Bytes spilled to remote storage | Severe memory pressure | Larger warehouse, split query, fix explosion |
| Join output rows ≫ input rows | Fan-out / missing join key | Fix join keys, pre-aggregate (see [Diagnose a join that multiplies rows (fan-out)](/sql/sql-interview-questions/#28-diagnose-a-join-that-multiplies-rows-fan-out)) |
| Huge "Processing" in a single aggregate/sort | Large GROUP BY/ORDER BY | Pre-aggregate, remove unneeded ORDER BY |
| TableScan time dominated by remote I/O | Cold cache, large scans | Prune more, select fewer columns |
| Many repeated subplans | CTE recomputed | Materialise intermediate results |
| `CartesianJoin` node | Missing or non-equi join condition | Fix join |
| Long compilation | Very complex SQL/views, huge IN lists | Simplify, break up, avoid giant generated SQL |

**Programmatic access:** `GET_QUERY_OPERATOR_STATS(query_id)` returns per-operator statistics in SQL, which is useful for automated regression checks. `QUERY_HISTORY` gives totals per query (spill bytes, partitions scanned).

<p class="qb-label">Example</p>

```sql
-- Dialect: Snowflake SQL
-- Find recent heavy spillers and poor pruners
SELECT query_id, warehouse_name, total_elapsed_time / 1000 AS sec,
       bytes_spilled_to_local_storage / POWER(1024, 3)  AS spill_local_gb,
       bytes_spilled_to_remote_storage / POWER(1024, 3) AS spill_remote_gb,
       partitions_scanned, partitions_total,
       ROUND(partitions_scanned / NULLIF(partitions_total, 0), 3) AS scan_ratio
FROM snowflake.account_usage.query_history
WHERE start_time > DATEADD(day, -1, CURRENT_TIMESTAMP())
  AND (bytes_spilled_to_remote_storage > 0 OR partitions_scanned / NULLIF(partitions_total, 0) > 0.8)
  AND partitions_total > 1000
ORDER BY total_elapsed_time DESC
LIMIT 20;
```

<p class="qb-label">Real-world use case</p>

A nightly model spilled 400 GB to remote storage on a Large warehouse. The profile showed a join exploding to 9 billion rows because of a missing date condition on an SCD2 join. Fixing the join removed the spill entirely, and the job then ran on a Medium warehouse.

<p class="qb-label">Common mistakes and red flags</p>

- Fixing spill by upsizing without checking for explosions.
- Ignoring queuing time and blaming query speed.
- Looking only at total time, not per-operator time.

<p class="qb-label">Follow-up questions</p>

- What's the difference between local and remote spill? — Local spills to the warehouse's SSD; remote spills to cloud storage and is much slower.
- How do you compare two versions of a query? — Run both with result cache off and compare profiles and `GET_QUERY_OPERATOR_STATS`.

</details>

### 31. Cost control: resource monitors and budgets

<p class="qb-meta">Intermediate · Very Common · Cost Management</p>

<p class="qb-label">Short answer</p>

Resource monitors track warehouse credit consumption against a quota over an interval (daily, weekly, monthly). At thresholds they can notify, suspend the warehouse after running queries finish, or suspend immediately. Budgets cover broader spend, including serverless features, for the account or groups of objects, with notifications when projected spend exceeds a limit.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Resource monitors**
- Apply at **account level** (all warehouses) or to **specific warehouses**. A warehouse can have only one monitor.
- `CREDIT_QUOTA`, `FREQUENCY` (DAILY, WEEKLY, MONTHLY, YEARLY, NEVER) and `START_TIMESTAMP`.
- `TRIGGERS ON 75 PERCENT DO NOTIFY ON 100 PERCENT DO SUSPEND ON 110 PERCENT DO SUSPEND_IMMEDIATE`.
- They cover **warehouse compute** (and the cloud services portion tied to warehouses). They don't control serverless features such as Snowpipe, automatic clustering or serverless tasks.
- Only ACCOUNTADMIN can create them by default. Notifications go to users who enable them.

**Budgets**
- Track spend for the **account** or **custom budgets** (groups of warehouses, databases, tables, pipes, tasks and so on), including serverless features.
- Set a monthly spending limit. Snowflake sends notifications (email, cloud queue, webhook) when spend is projected to exceed it.
- Budgets notify. They don't suspend. Combine them with resource monitors for hard stops on warehouses.

**Other guardrails**
- `AUTO_SUSPEND` and right-sizing (see [Virtual warehouses: sizes, credits, auto-suspend and auto-resume](#2-virtual-warehouses-sizes-credits-auto-suspend-and-auto-resume)).
- `STATEMENT_TIMEOUT_IN_SECONDS` and `STATEMENT_QUEUED_TIMEOUT_IN_SECONDS` on warehouses and users.
- Separate warehouses per team, so cost is attributable (see [FinOps: tagging, chargeback and showback](/career/data-engineering-interview-questions/#72-finops-tagging-chargeback-and-showback)).
- Alerts (`CREATE ALERT`) that check custom cost conditions on a schedule.
- Review serverless feature costs: clustering, materialized views, search optimization, Snowpipe, replication.

<p class="qb-label">Example</p>

```sql
-- Dialect: Snowflake SQL
USE ROLE ACCOUNTADMIN;
CREATE OR REPLACE RESOURCE MONITOR rm_bi_monthly
  WITH CREDIT_QUOTA = 500
       FREQUENCY = MONTHLY
       START_TIMESTAMP = IMMEDIATELY
  TRIGGERS ON 75 PERCENT DO NOTIFY
           ON 100 PERCENT DO SUSPEND
           ON 110 PERCENT DO SUSPEND_IMMEDIATE;

ALTER WAREHOUSE bi_wh SET RESOURCE_MONITOR = rm_bi_monthly;

ALTER WAREHOUSE adhoc_wh SET STATEMENT_TIMEOUT_IN_SECONDS = 1800;
```

<p class="qb-label">Real-world use case</p>

A runaway cross-join from an analyst's notebook burned 300 credits overnight on an unlimited warehouse. After the incident, every non-production warehouse got a resource monitor with SUSPEND at 100%, and ad-hoc warehouses got a 30-minute statement timeout.

<p class="qb-label">Common mistakes and red flags</p>

- Thinking resource monitors cap serverless costs.
- Monitors that only notify, with nobody watching the notifications.
- Hard-suspending production ETL warehouses without an escalation path.

<p class="qb-label">Follow-up questions</p>

- What happens to running queries on SUSPEND? — They finish; SUSPEND_IMMEDIATE cancels them.
- How do you find who spent the credits? — Query attribution and query tags in ACCOUNT_USAGE (see [Analysing spend with ACCOUNT_USAGE views](#46-analysing-spend-with-account_usage-views)).

</details>

### 32. Snowpark: DataFrame API and pushdown

<p class="qb-meta">Intermediate · Common · Snowpark</p>

<p class="qb-label">Short answer</p>

Snowpark is Snowflake's developer framework for Python, Java and Scala. You write DataFrame code (select, filter, join, groupBy) that is lazily translated into SQL and executed inside Snowflake, so data stays in the platform. Custom logic runs as UDFs, UDTFs and stored procedures in Snowflake's secure runtime, with packages from the Anaconda channel or uploaded artifacts.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**DataFrame API**
- `session.table("orders").filter(col("status") == "paid").group_by("region").agg(sum_("amount"))` builds a query plan. Nothing executes until an **action** (`collect()`, `show()`, `count()`, `save_as_table()`, `to_pandas()`).
- Operations **push down** as SQL to the warehouse, using Snowflake's optimizer and elastic compute.
- `df.explain()` or `df.queries` shows the generated SQL.

**Running custom Python**
- **UDFs and UDTFs:** register Python functions, including vectorised (pandas) UDFs that process batches for performance.
- **Stored procedures:** orchestrate multi-step logic with Python, using the Snowpark session inside.
- **Packages:** many open-source packages are available through the Snowflake Anaconda channel. Others can be uploaded as stage artifacts. Artifact repository support (for example PyPI packages) exists in newer releases.
- **Snowpark-optimized warehouses** give much more memory per node for ML training and memory-heavy UDFs (see [Gen2 and Snowpark-optimized warehouses](#36-gen2-and-snowpark-optimized-warehouses)).
- **Snowpark Container Services** run arbitrary containers when the Python runtime isn't enough.

**pandas on Snowflake**
- The `modin.pandas` API backed by Snowflake (Snowpark pandas) lets pandas-style code execute at warehouse scale without pulling data locally.

**Pitfalls**
- `to_pandas()` / `collect()` on huge results pulls everything to the client, with memory blowups and slow transfer.
- Row-by-row Python UDFs where built-in SQL functions exist are much slower.
- Many small actions in loops each send a query. Batch logic into one plan.

<p class="qb-label">Example</p>

```python
# Runtime: Python 3.11, snowflake-snowpark-python
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, sum as sum_, to_date

# connection parameters come from a config/secret store, not hardcoded
session = Session.builder.configs(connection_parameters).create()

orders = session.table("analytics.silver.orders")
daily = (orders
         .filter(col("status") == "paid")
         .with_column("order_date", to_date(col("order_ts")))
         .group_by("order_date", "region")
         .agg(sum_("amount").alias("revenue")))

print(daily.queries["queries"][-1])          # inspect generated SQL
daily.write.mode("overwrite").save_as_table("analytics.gold.daily_revenue_by_region")
```
`connection_parameters` is a dict loaded from your configuration (for example a `connections.toml` profile or a secrets manager).

<p class="qb-label">Real-world use case</p>

A data-science team moved feature engineering from local pandas on CSV extracts to Snowpark DataFrames. Jobs that previously ran out of laptop memory now run as pushed-down SQL on a warehouse, and features are written straight to tables.

<p class="qb-label">Common mistakes and red flags</p>

- Calling `to_pandas()` on billions of rows.
- Writing Python UDFs for things built-in functions do.
- Thinking Snowpark runs Spark.

<p class="qb-label">Follow-up questions</p>

- Snowpark vs PySpark? — Similar DataFrame style; Snowpark compiles to Snowflake SQL and runs in Snowflake, PySpark runs on Spark clusters.
- How do you unit test Snowpark code? — Local testing mode (in the Snowpark library) or small fixture tables in a test schema.

</details>

### 33. UDFs, UDTFs and stored procedures (languages and use cases)

<p class="qb-meta">Intermediate · Common · Programmability</p>

<p class="qb-label">Short answer</p>

Snowflake supports scalar UDFs, table functions (UDTFs) and stored procedures in SQL, JavaScript, Python, Java and Scala. Stored procedures can also use Snowflake Scripting. UDFs and UDTFs run inside queries without side effects. Procedures run statements and control flow, executing with owner's rights (the default) or caller's rights. Choose the language by need: SQL for speed and inlining, Python or Java for libraries and complex parsing.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Function types**
| Type | Returns | Called in | Typical use |
|---|---|---|---|
| Scalar UDF | One value per row | SELECT/WHERE | Calculations, parsing, normalisation |
| Vectorised Python UDF | One value per row (batch-processed) | SELECT | Faster Python per-row logic using pandas batches |
| UDTF | Rows (table) | FROM `TABLE(f(...))` | Exploding/parsing records, per-partition logic with `OVER (PARTITION BY ...)` |
| UDAF (Python) | Aggregate value | GROUP BY | Custom aggregations |
| Stored procedure | Optional value/table | `CALL` | DDL/DML orchestration, admin jobs, dynamic SQL |
| External function | Value via remote service | SELECT | Calling an external API (being complemented by external access integrations) |

**Language trade-offs**
- **SQL UDFs:** fastest, and can be inlined by the optimizer.
- **Python, Java, Scala:** rich libraries, run in a sandbox. Network access requires **external access integrations** (with network rules and secrets).
- **JavaScript:** lightweight, but less common now that Python is available.

**Owner's vs caller's rights (procedures)**
- **Owner's rights (default):** runs with the owner's privileges. Callers can do controlled operations they couldn't do directly. There are restrictions, for example on session variables and some session-level commands.
- **Caller's rights (`EXECUTE AS CALLER`):** runs with the caller's privileges and session context, which suits utilities that act on behalf of the user.

**Security and practice**
- **Secure UDFs** hide their definition (needed when shared).
- Validate identifiers and use bind variables in dynamic SQL to prevent injection (see [Stored procedures vs user-defined functions](/sql/sql-interview-questions/#37-stored-procedures-vs-user-defined-functions)).
- Keep business logic in version-controlled transformations (dbt). Use procedures for operational tasks.

<p class="qb-label">Example</p>

```sql
-- Dialect: Snowflake SQL
-- Python UDTF that splits a delimited tag string into rows
CREATE OR REPLACE FUNCTION util.split_tags(tags STRING)
RETURNS TABLE (tag STRING, position INT)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
HANDLER = 'SplitTags'
AS
$$
class SplitTags:
    def process(self, tags):
        if tags is None:
            return
        for i, t in enumerate(tags.split("|")):
            cleaned = t.strip().lower()
            if cleaned:
                yield (cleaned, i)
$$;

SELECT p.product_id, t.tag, t.position
FROM products p,
     TABLE(util.split_tags(p.tag_string)) t;
```
Sample: `tag_string = 'Summer| Sale ||Kids'` gives (summer, 0), (sale, 1), (kids, 3).

<p class="qb-label">Real-world use case</p>

A team replaced a JavaScript UDF that parsed user-agent strings with a vectorised Python UDF using a maintained parsing library. Accuracy improved, and throughput rose several-fold through batch processing.

<p class="qb-label">Common mistakes and red flags</p>

- Python UDFs for simple arithmetic.
- Procedures running dynamic SQL built from unvalidated input.
- Not knowing about owner's vs caller's rights.

<p class="qb-label">Follow-up questions</p>

- How do UDFs call external APIs? — Via external access integrations (network rules + secrets) or external functions through an API gateway.
- What is a memoizable function? — A UDF whose results are cached (useful for lookups in policies).

</details>

### 34. External tables vs Iceberg tables

<p class="qb-meta">Intermediate · Occasional · Iceberg Tables</p>

<p class="qb-label">Short answer</p>

External tables are read-only metadata over files (Parquet, CSV, JSON and so on) in an external stage. Each row is exposed as a VARIANT `VALUE` column plus optional virtual columns, and they are refreshed manually or through notifications. Iceberg tables are full table-format tables with ACID transactions, schema evolution, Time Travel and (when Snowflake-managed) DML. For new lakehouse designs, Iceberg is usually preferred.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

| Aspect | External table | Iceberg table |
|---|---|---|
| Data format | Raw files (Parquet, CSV, JSON, Avro, ORC, Delta via specific support) | Apache Iceberg (Parquet data + Iceberg metadata) |
| Writes from Snowflake | No (read-only) | Yes (Snowflake-managed); limited/expanding for external catalogs |
| Schema | VARIANT `VALUE` + defined virtual columns | Typed columns, schema evolution |
| Partition pruning | Via partition columns derived from file paths | Iceberg partition specs + file statistics |
| Metadata refresh | `ALTER EXTERNAL TABLE ... REFRESH` or auto-refresh via notifications | Commits update metadata; external catalogs refresh |
| Transactions / Time Travel | No | Yes |
| Performance | Generally slower | Close to native for Snowflake-managed |
| Multi-engine | Files readable by anyone, no table semantics | Full table semantics across engines |

**When external tables still make sense**
- Quick querying of an existing raw-file lake without converting it.
- Data owned by another system where you only need occasional reads.
- A staging step before loading into native or Iceberg tables.

**Practical tips for external tables**
- Define **partition columns** from `METADATA$FILENAME` path parts, so date filters prune files.
- Create **materialized views** on external tables for frequently queried subsets.
- Keep file sizes healthy (see [File sizing and format best practices for bulk loading](#20-file-sizing-and-format-best-practices-for-bulk-loading)).

<p class="qb-label">Example</p>

```sql
-- Dialect: Snowflake SQL
CREATE OR REPLACE EXTERNAL TABLE raw.ext_clicks (
    event_date DATE AS TO_DATE(SPLIT_PART(SPLIT_PART(METADATA$FILENAME, 'dt=', 2), '/', 1)),
    user_id    STRING AS (VALUE:user_id::STRING),
    page       STRING AS (VALUE:page::STRING)
)
PARTITION BY (event_date)
LOCATION = @raw.s3_clicks
FILE_FORMAT = (TYPE = PARQUET)
AUTO_REFRESH = TRUE;

SELECT page, COUNT(*) FROM raw.ext_clicks
WHERE event_date = '2026-03-01'
GROUP BY page;
```

<p class="qb-label">Real-world use case</p>

An analytics team first exposed a 3-year Parquet archive as an external table for ad-hoc queries. As usage grew, they migrated the actively used years to Iceberg tables for better performance and DML. The cold archive stayed as an external table.

<p class="qb-label">Common mistakes and red flags</p>

- External tables without partition columns, so every query scans all files.
- Expecting to UPDATE external tables.
- Forgetting auto-refresh notifications, so new files are invisible.

<p class="qb-label">Follow-up questions</p>

- Can external tables read Delta Lake? — Snowflake has offered Delta Lake support for external tables and Iceberg-based reading of Delta via catalog integrations; check current options.
- How do streams work on them? — Insert-only streams are supported on external tables (see [Streams: standard, append-only, insert-only; offsets and staleness](#21-streams-standard-append-only-insert-only-offsets-and-staleness)).

</details>

### 35. MERGE in Snowflake and the nondeterministic-merge error

<p class="qb-meta">Intermediate · Common · DML</p>

<p class="qb-label">Short answer</p>

Snowflake's MERGE upserts and deletes in one atomic statement. If several source rows match the same target row, the result would depend on processing order, so Snowflake raises an error by default (`ERROR_ON_NONDETERMINISTIC_MERGE = TRUE`). The fix is to deduplicate the source to one row per key with a deterministic rule before merging, not to turn the parameter off.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**The error**
- Typical cause: a CDC batch with several updates for the same key, or a join in the USING clause that fans out.
- With the parameter set to FALSE, Snowflake picks one matching source row arbitrarily for UPDATE or DELETE. That produces non-repeatable, silently wrong results.

**Deterministic source**
```text
USING (
  SELECT * FROM changes
  QUALIFY ROW_NUMBER() OVER (PARTITION BY key ORDER BY commit_ts DESC, seq DESC) = 1
) s
```

**Other Snowflake MERGE details**
- **Multiple WHEN clauses** with conditions are allowed. The first matching clause wins, in order.
- `WHEN NOT MATCHED BY SOURCE` (deleting target rows absent from the source) is not part of Snowflake's MERGE. Use a separate DELETE with an anti-join for full-sync semantics.
- **Performance:**
  - The target is joined to the source, so add target pruning predicates (for example only the last 7 days of partitions) when changes are recent.
  - Clustering the target by the merge key, or a date related to it, reduces micro-partitions rewritten.
  - Only touched micro-partitions are rewritten, but a wide spread of keys can touch many.
- **Idempotency:** add conditions like `s.commit_ts > t.commit_ts`, so replays don't regress data (see [What is idempotency, and why is it critical in pipelines?](/career/data-engineering-interview-questions/#19-what-is-idempotency-and-why-is-it-critical-in-pipelines)).
- **MERGE ALL BY NAME** (newer syntax) maps columns by name for update and insert when source and target share column names. Check availability.

<p class="qb-label">Example</p>

```sql
-- Dialect: Snowflake SQL
MERGE INTO silver.inventory t
USING (
    SELECT sku, warehouse_id, on_hand, event_ts
    FROM raw.inventory_events
    WHERE event_ts >= DATEADD(hour, -2, CURRENT_TIMESTAMP())
    QUALIFY ROW_NUMBER() OVER (PARTITION BY sku, warehouse_id ORDER BY event_ts DESC) = 1
) s
ON t.sku = s.sku AND t.warehouse_id = s.warehouse_id
WHEN MATCHED AND s.event_ts > t.event_ts THEN UPDATE SET
     t.on_hand = s.on_hand, t.event_ts = s.event_ts
WHEN NOT MATCHED THEN INSERT (sku, warehouse_id, on_hand, event_ts)
     VALUES (s.sku, s.warehouse_id, s.on_hand, s.event_ts);
```
Sample: SKU A1 in warehouse W1 has events at 10:00 (on_hand 50) and 10:05 (on_hand 47). The deduplicated source keeps 10:05, and the target becomes 47. Without deduplication, Snowflake raises the nondeterministic-merge error.

<p class="qb-label">Real-world use case</p>

A pipeline started failing with "Duplicate row detected during DML action" after the source began emitting two status events within one batch. Adding a QUALIFY deduplication fixed it permanently. An earlier "fix" of disabling the parameter had been silently corrupting stock levels.

<p class="qb-label">Common mistakes and red flags</p>

- Setting `ERROR_ON_NONDETERMINISTIC_MERGE = FALSE`.
- Deduplicating with a non-deterministic ORDER BY.
- Full-target scans on every 5-minute merge.

<p class="qb-label">Follow-up questions</p>

- Does the same error apply to UPDATE ... FROM? — A separate parameter, `ERROR_ON_NONDETERMINISTIC_UPDATE`, governs multi-match updates and is off by default, so dedupe the source there too.
- How do you delete rows missing from a full snapshot? — A separate DELETE with NOT EXISTS against the snapshot, in the same transaction.

</details>

### 36. Gen2 and Snowpark-optimized warehouses

<p class="qb-meta">Intermediate · Occasional · Virtual Warehouses</p>

<p class="qb-label">Short answer</p>

Gen2 standard warehouses run on newer hardware with software optimisations that speed up analytics and DML-heavy work (DELETE, UPDATE, MERGE, large scans), at a different credit rate than Gen1. Snowpark-optimized warehouses give much more memory per node (and configurable CPU architecture or memory resources) for memory-hungry workloads such as ML training, large UDFs and big in-memory operations.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Warehouse types**
| Type | Designed for | Notes |
|---|---|---|
| Standard (Gen1) | General SQL workloads | Classic credit rates (see [Virtual warehouses: sizes, credits, auto-suspend and auto-resume](#2-virtual-warehouses-sizes-credits-auto-suspend-and-auto-resume)) |
| Standard Gen2 | Faster analytics and DML | Request with `GENERATION = '2'` (older syntax `RESOURCE_CONSTRAINT = STANDARD_GEN_2`); higher credits/hour, often lower total cost via shorter runtime |
| Snowpark-optimized | Memory-intensive Python/Java/Scala, ML training, large UDTFs | Much more memory per node (multiple memory options via `RESOURCE_CONSTRAINT`); higher credits/hour; slower to provision |
| Interactive / adaptive variants | Newer specialised warehouse types for low-latency or auto-managed workloads | Availability evolving; check current docs |

**Choosing**
- Benchmark **total credits per job**, not credits per hour. A Gen2 warehouse that finishes 2 times faster at 1.35 times the hourly rate is cheaper.
- MERGE-heavy CDC pipelines and large transformation jobs are prime Gen2 candidates.
- Use **Snowpark-optimized** only when jobs fail or spill because of memory in UDFs or ML training. For plain SQL, standard warehouses are more cost-effective.

**Rollout approach**
1. Pick representative jobs.
2. Run them on Gen1 and Gen2 with the result cache disabled, and compare runtime and credits (`QUERY_ATTRIBUTION_HISTORY`).
3. Switch warehouses with an `ALTER WAREHOUSE`. No code changes are needed.
4. Monitor for regressions.

Snowflake has been making Gen2 the default for new standard warehouses in supported regions. Credit multipliers, defaults and regional availability change, so verify them in current documentation.

<p class="qb-label">Example</p>

```sql
-- Dialect: Snowflake SQL
CREATE OR REPLACE WAREHOUSE cdc_merge_wh
  WAREHOUSE_SIZE = 'MEDIUM'
  GENERATION = '2'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE;

CREATE OR REPLACE WAREHOUSE ml_train_wh
  WAREHOUSE_SIZE = 'MEDIUM'
  WAREHOUSE_TYPE = 'SNOWPARK-OPTIMIZED'
  RESOURCE_CONSTRAINT = 'MEMORY_16X'
  AUTO_SUSPEND = 120;
```

<p class="qb-label">Real-world use case</p>

A team moved its hourly MERGE workloads to Gen2 Medium warehouses. Runtimes fell by roughly 40%, and total credits for those jobs dropped despite the higher hourly rate. Their XGBoost training procedure moved to a Snowpark-optimized warehouse after repeatedly running out of memory on standard ones.

<p class="qb-label">Common mistakes and red flags</p>

- Comparing only credits/hour.
- Using Snowpark-optimized warehouses for normal SQL.
- Assuming Gen2 is available in every region.

<p class="qb-label">Follow-up questions</p>

- Do I need to change SQL for Gen2? — No; it's a warehouse property.
- Why are Snowpark-optimized warehouses slower to start? — Their larger-memory nodes come from a different resource pool.

</details>

### 37. Cortex AI SQL functions (AI_COMPLETE, AI_CLASSIFY and others)

<p class="qb-meta">Intermediate · Occasional · AI Features</p>

<p class="qb-label">Short answer</p>

Snowflake Cortex provides managed LLM and ML capabilities callable from SQL, so data stays in Snowflake under existing governance. Examples include `AI_COMPLETE` (text generation), `AI_CLASSIFY`, `AI_FILTER`, `AI_EXTRACT`, `AI_SENTIMENT`, `AI_SUMMARIZE_AGG`, `AI_TRANSLATE`, `AI_EMBED` and `AI_PARSE_DOCUMENT`. Related services include Cortex Search (hybrid search) and Cortex Analyst (natural language to SQL over semantic views).

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Why data engineers care**
- Unstructured text (tickets, reviews, documents) can be enriched in pipelines with plain SQL. Classify tickets, extract fields from contracts, translate reviews, all in a dbt model or dynamic table.
- No data movement to external APIs, with RBAC applied as normal.

**Function families (current naming)**
| Purpose | Functions (examples) |
|---|---|
| Generation / reasoning | `AI_COMPLETE` (choose model; prompt with columns) |
| Classification / filtering | `AI_CLASSIFY`, `AI_FILTER` |
| Extraction | `AI_EXTRACT`, `AI_PARSE_DOCUMENT` (PDF/images to text/layout) |
| Summarisation / sentiment | `AI_SUMMARIZE_AGG` (across rows), `AI_SENTIMENT` |
| Translation | `AI_TRANSLATE` |
| Embeddings / similarity | `AI_EMBED`, `AI_SIMILARITY` |
| Cost estimation | `AI_COUNT_TOKENS` |

**Naming change:** older `SNOWFLAKE.CORTEX.*` functions (`COMPLETE`, `SENTIMENT`, `SUMMARIZE`, `TRANSLATE`, `EXTRACT_ANSWER`) still exist in many accounts. Snowflake has introduced the `AI_*` family as the forward path and announced deprecation of some older names. Also in 2026, Snowflake Intelligence was rebranded **Snowflake CoWork** and Cortex Code was rebranded **CoCo**.

**Engineering considerations**
- **Cost:** billed per token (input and output) by model. Estimate with `AI_COUNT_TOKENS`, run on samples first, and process incrementally, so rows aren't re-scored on every run.
- **Determinism:** LLM outputs vary. Store results (materialise them) rather than recomputing in views, and record the model and prompt version.
- **Quality:** build evaluation sets, and constrain outputs (classification labels, JSON schemas in `AI_COMPLETE` structured output).
- **Governance:** control access with the `SNOWFLAKE.CORTEX_USER` database role, plus model allowlists at account level.
- **Model availability** varies by region. Cross-region inference can be enabled where allowed.

<p class="qb-label">Example</p>

```sql
-- Dialect: Snowflake SQL
-- Incrementally classify new support tickets and store results
INSERT INTO gold.ticket_enriched (ticket_id, category, sentiment, model_version, enriched_at)
SELECT t.ticket_id,
       AI_CLASSIFY(t.body, ['billing', 'bug', 'feature_request', 'account_access']):labels[0]::STRING AS category,
       AI_SENTIMENT(t.body)                                                                    AS sentiment,
       'ai_classify_v1'                                                                         AS model_version,
       CURRENT_TIMESTAMP()
FROM silver.support_tickets t
LEFT JOIN gold.ticket_enriched e ON e.ticket_id = t.ticket_id
WHERE e.ticket_id IS NULL;
```
The output shapes of `AI_CLASSIFY` and `AI_SENTIMENT` (objects or arrays) are defined in the function documentation. Adjust the path extraction to the documented return format for your account's version.

<p class="qb-label">Real-world use case</p>

A support organisation auto-routes 20,000 tickets a day by classifying each new ticket once in an incremental model. Agents see the category and sentiment, and leadership dashboards track categories over time without manual tagging.

<p class="qb-label">Common mistakes and red flags</p>

- Calling LLM functions inside views queried by dashboards, which re-scores and pays on every load.
- No token-cost estimate before running on millions of rows.
- Treating LLM output as ground truth without evaluation.

<p class="qb-label">Follow-up questions</p>

- What is Cortex Search? — A managed hybrid (vector + keyword) search service over text, used for RAG (see [Data engineering for ML and GenAI: feature stores, embeddings, vector stores](/career/data-engineering-interview-questions/#100-data-engineering-for-ml-and-genai-feature-stores-embeddings-vector-stores)).
- What is Cortex Analyst? — A service that answers natural-language questions with SQL, grounded by semantic models/views (see [What is a semantic/metrics layer?](/career/data-engineering-interview-questions/#75-what-is-a-semanticmetrics-layer)).

</details>

## Advanced Snowflake interview questions (experienced)

### 38. Search Optimization Service vs clustering

<p class="qb-meta">Advanced · Common · Performance</p>

<p class="qb-label">Short answer</p>

The Search Optimization Service builds and maintains a persistent search access path, an index-like structure, for selected columns. Highly selective queries (equality, IN, substring, geospatial, VARIANT field lookups) can then skip almost all micro-partitions. Clustering physically co-locates data for range scans on a few key columns. Search optimization targets "needle in a haystack" lookups on many columns. Both cost serverless credits plus storage.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

| Aspect | Clustering key | Search Optimization Service |
|---|---|---|
| Mechanism | Reorganise micro-partitions by key | Separate search access path structure |
| Best queries | Range/equality filters on key columns, returning many rows | Highly selective filters returning few rows |
| Number of columns | Few (key order matters) | Many columns, each configured by method |
| Predicate types | Comparisons, ranges | Equality, IN, `LIKE`/substring (`SUBSTRING` method), `GEO`, semi-structured paths, and some joins |
| Cost | Reclustering credits | Build/maintenance credits + storage for the access path |
| Edition | All | Enterprise and above |

**Configuring**
- Enable for specific columns and methods: `ADD SEARCH OPTIMIZATION ON EQUALITY(order_id), SUBSTRING(email)`.
- Check progress and coverage with `DESCRIBE SEARCH OPTIMIZATION ON tbl`, or `SHOW TABLES` (search optimization progress column).
- Estimate cost first with `SYSTEM$ESTIMATE_SEARCH_OPTIMIZATION_COSTS`.

**Good candidates**
- Large tables where users look up one customer, order, device or email out of billions of rows.
- Log or event tables searched by ID or text fragment.
- Tables where adding a clustering key for that column would conflict with the main date clustering.

**Poor candidates**
- Queries returning a large share of the table. Pruning wouldn't help anyway.
- Small tables, or tables with constant heavy churn that would keep rebuilding the access path.

<p class="qb-label">Example</p>

```sql
-- Dialect: Snowflake SQL
SELECT SYSTEM$ESTIMATE_SEARCH_OPTIMIZATION_COSTS('ops.api_logs', 'EQUALITY(request_id)');

ALTER TABLE ops.api_logs ADD SEARCH OPTIMIZATION ON EQUALITY(request_id, user_id), SUBSTRING(error_message);

-- Selective lookup that benefits
SELECT * FROM ops.api_logs WHERE request_id = 'a91c-7f2e-0b44';
```

<p class="qb-label">Real-world use case</p>

Support engineers searched a 60 TB API log table by `request_id`, and each query scanned for minutes on a Large warehouse. After enabling search optimization on equality for `request_id`, lookups returned in seconds on an X-Small warehouse. The table kept its date clustering for analytical queries.

<p class="qb-label">Common mistakes and red flags</p>

- Treating it as a general-purpose index for all queries.
- Enabling it on every column without a cost estimate.
- Not knowing it requires Enterprise edition.

<p class="qb-label">Follow-up questions</p>

- How do you see if a query used it? — The Query Profile shows a "Search Optimization Access" node and lower partitions scanned.
- Can it help joins? — It can help selective joins where the smaller side's values filter the large table, for supported predicates.

</details>

### 39. Snowpipe vs Snowpipe Streaming vs COPY INTO

<p class="qb-meta">Advanced · Common · Data Loading</p>

<p class="qb-label">Short answer</p>

COPY INTO is scheduled bulk loading of staged files on your warehouse, which suits large periodic batches. Snowpipe is event-driven, serverless file loading with near-real-time latency (about a minute). Snowpipe Streaming writes rows directly into tables through an SDK or REST API with no files, at seconds-level latency. It suits Kafka, CDC and high-volume event streams.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

| Aspect | COPY INTO | Snowpipe | Snowpipe Streaming |
|---|---|---|---|
| Input | Staged files | Staged files | Rows via SDK/REST (no files) |
| Trigger | Your scheduler | Cloud event notifications / REST | Client application continuously |
| Compute | Your warehouse | Serverless | Serverless |
| Typical latency | Minutes–hours (batch) | ~1 minute | Seconds |
| Dedup mechanism | 64-day load metadata per table | 14-day load history per pipe | Channels with offset tokens; client tracks committed offsets |
| Best for | Large backfills, daily batches | Continuous file drops | Kafka/CDC/event streams, IoT |
| Cost model | Warehouse credits | Serverless ingest pricing | Serverless streaming pricing (throughput-based) |

**Snowpipe Streaming details**
- Clients open **channels** to a table and append rows with **offset tokens**. After a restart, the client asks Snowflake for the last committed offset and resumes from there, which gives exactly-once ingestion when used correctly.
- The **Snowflake Kafka connector** can use Snowpipe Streaming under the hood, writing directly from topics.
- A newer **high-performance architecture** (GA in 2025) uses a PIPE object and a new SDK, with throughput-based pricing and higher throughput. The classic architecture remains for existing users. Check current docs for differences and migration guidance.
- Data becomes queryable within seconds. Downstream transformations can use Dynamic Tables or Streams and Tasks (see [Build an incremental CDC pipeline with Streams and Tasks](#40-build-an-incremental-cdc-pipeline-with-streams-and-tasks)–[Dynamic Tables vs Streams & Tasks vs materialized views](#41-dynamic-tables-vs-streams--tasks-vs-materialized-views)).

**Choosing**
- Data already arrives as files, and around a minute is fine: Snowpipe.
- Events come from Kafka or an application, and you need seconds of latency or want to avoid file creation: Snowpipe Streaming.
- A large historical backfill or nightly vendor dump: COPY INTO with a right-sized warehouse.

<p class="qb-label">Example</p>

```sql
-- Dialect: Snowflake SQL — target table for a Kafka connector using Snowpipe Streaming
CREATE OR REPLACE TABLE raw.clickstream (
    record_metadata VARIANT,   -- topic, partition, offset, timestamp (populated by connector)
    record_content  VARIANT    -- event payload
);
-- Downstream: incremental transform with a dynamic table
CREATE OR REPLACE DYNAMIC TABLE silver.clicks
  TARGET_LAG = '1 minute'
  WAREHOUSE = transform_wh
AS
SELECT record_content:event_id::STRING AS event_id,
       record_content:user_id::STRING  AS user_id,
       record_content:ts::TIMESTAMP_NTZ AS event_ts
FROM raw.clickstream;
```

<p class="qb-label">Real-world use case</p>

A fintech moved transaction events from Kafka → S3 files → Snowpipe (about 3 minutes end to end) to the Kafka connector with Snowpipe Streaming. Rows became queryable in under 10 seconds, and file-management overhead disappeared.

<p class="qb-label">Common mistakes and red flags</p>

- Writing tiny files every second to feed Snowpipe for "real time".
- Ignoring offset management with the Streaming SDK.
- Using COPY on a 24/7 warehouse for continuous trickle loads.

<p class="qb-label">Follow-up questions</p>

- How does exactly-once work with Streaming? — The client commits offset tokens per channel and resumes from the last committed token after failure.
- Can Snowpipe Streaming write to Iceberg tables? — Snowflake has added Streaming support for Snowflake-managed Iceberg tables; check current docs for limits.

</details>

### 40. Build an incremental CDC pipeline with Streams and Tasks

<p class="qb-meta">Advanced · Very Common · Streams & Tasks</p>

<p class="qb-label">Short answer</p>

Land changes into a raw table (Snowpipe, a connector, or CDC events), and put a stream on it. A task that runs on a schedule (or is triggered) when the stream has data deduplicates the change set to the latest event per key and MERGEs it into the target, applying updates, inserts and deletes. Committing the MERGE advances the stream offset, so each change is processed exactly once.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Design**
1. **Raw landing (append-only):** CDC events (`op`, key, payload, `commit_ts`/LSN) arrive through Snowpipe or the Kafka connector.
2. **Append-only stream** on the raw table, which is cheaper and fits insert-only landing.
3. **Task** with `WHEN SYSTEM$STREAM_HAS_DATA('raw.cust_cdc_stream')`:
   - Select from the stream, keep the **latest event per key** (`QUALIFY ROW_NUMBER() ... ORDER BY commit_ts DESC, lsn DESC = 1`).
   - `MERGE` into silver: delete on `op = 'D'`, update when the source is newer, insert when not matched.
4. **Downstream:** another stream on silver feeds gold, or replace that layer with Dynamic Tables (see [Dynamic Tables vs Streams & Tasks vs materialized views](#41-dynamic-tables-vs-streams--tasks-vs-materialized-views)).

**Correctness details**
- **Exactly once:** the stream offset advances only if the MERGE transaction commits. On failure, the next run reprocesses the same changes, and the MERGE stays idempotent because of the "source newer" condition.
- **Ordering:** files and batches may arrive out of order, so always compare commit timestamps or LSNs rather than trusting arrival order (see [What ordering guarantees does Kafka provide?](/career/data-engineering-interview-questions/#59-what-ordering-guarantees-does-kafka-provide)).
- **Multiple statements on one change set:** wrap them in an explicit transaction, so all of them read the same stream contents (for example, MERGE silver plus insert an audit log row).
- **Staleness:** keep the task frequent enough to avoid stream staleness (see [Streams: standard, append-only, insert-only; offsets and staleness](#21-streams-standard-append-only-insert-only-offsets-and-staleness)).
- **Schema changes:** VARIANT payloads tolerate new fields, and typed columns need controlled evolution.

**Monitoring:** task history failures, stream lag (`SHOW STREAMS`), row counts reconciled with the source, and alerts on `SUSPEND_TASK_AFTER_NUM_FAILURES`.

<p class="qb-label">Example</p>

```sql
-- Dialect: Snowflake SQL
CREATE OR REPLACE STREAM raw.cust_cdc_stream ON TABLE raw.customer_cdc APPEND_ONLY = TRUE;

CREATE OR REPLACE TASK etl.t_merge_customers
  WAREHOUSE = transform_wh
  SCHEDULE = '5 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('raw.cust_cdc_stream')
AS
MERGE INTO silver.customers t
USING (
    SELECT payload:customer_id::NUMBER       AS customer_id,
           payload:full_name::STRING         AS full_name,
           payload:email::STRING             AS email,
           op,
           commit_ts
    FROM raw.cust_cdc_stream
    QUALIFY ROW_NUMBER() OVER (PARTITION BY payload:customer_id::NUMBER
                               ORDER BY commit_ts DESC, lsn DESC) = 1
) s
ON t.customer_id = s.customer_id
WHEN MATCHED AND s.op = 'D' THEN DELETE
WHEN MATCHED AND s.op <> 'D' AND s.commit_ts > t.commit_ts THEN UPDATE SET
     t.full_name = s.full_name, t.email = s.email, t.commit_ts = s.commit_ts
WHEN NOT MATCHED AND s.op <> 'D' THEN INSERT (customer_id, full_name, email, commit_ts)
     VALUES (s.customer_id, s.full_name, s.email, s.commit_ts);

ALTER TASK etl.t_merge_customers RESUME;
```

Sample change set: customer 5 has events I (09:00), U (09:02) and U (09:04). Only the 09:04 version is merged. Customer 6 has D (09:03) and is deleted from silver.

<p class="qb-label">Real-world use case</p>

A company replicates 30 OLTP tables into Snowflake with this pattern. One generic stored procedure generates the stream, task and MERGE for each table from a config table, and the source-to-target lag stays under 10 minutes.

<p class="qb-label">Common mistakes and red flags</p>

- No in-batch dedupe, which causes nondeterministic MERGE errors (see [MERGE in Snowflake and the nondeterministic-merge error](#35-merge-in-snowflake-and-the-nondeterministic-merge-error)).
- Relying on arrival order.
- Sharing one stream between two consuming tasks.

<p class="qb-label">Follow-up questions</p>

- When would you use Dynamic Tables instead? — When the logic is declarative SELECTs and a target lag is acceptable; Snowflake manages the incremental refresh (see [Dynamic Tables: target lag and refresh modes](#22-dynamic-tables-target-lag-and-refresh-modes)).
- How do you backfill? — Load historical changes into raw (they flow through the stream), or rebuild silver from a snapshot then resume CDC from a known position.

**In depth:** [How do you build an incremental pipeline with Snowflake streams and tasks?](/interview/snowflake/streams-tasks-incremental-pipeline/)

</details>

### 41. Dynamic Tables vs Streams & Tasks vs materialized views

<p class="qb-meta">Advanced · Common · Dynamic Tables</p>

<p class="qb-label">Short answer</p>

Dynamic Tables are declarative: you write a SELECT and a target lag, and Snowflake handles incremental refresh and dependency ordering. Streams & Tasks are imperative: you control change capture, scheduling and custom DML (MERGE with deletes, SCD2, calls to procedures). Materialized views are always-current, single-table, automatically maintained query accelerators with limited SQL. Choose by the flexibility, latency and control each case needs.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

| Aspect | Dynamic Tables | Streams & Tasks | Materialized views |
|---|---|---|---|
| Programming model | Declarative SELECT + lag | Imperative DML + schedule | Declarative SELECT |
| Joins | Yes | Yes (in your SQL) | No (single table) |
| Freshness | Target lag (≥ 1 min) | Your schedule (≥ 1 min) or triggered | Always current (results merged at query time if behind) |
| Incremental processing | Automatic when supported | You write it (stream + MERGE) | Automatic |
| Custom logic (SCD2, deletes, procedures, external calls) | Limited to SELECT semantics | Full control | No |
| Orchestration | Automatic DAG by dependencies | Task graphs you define | N/A |
| Compute | Your warehouse | Warehouse or serverless | Serverless maintenance |
| Edition | All | All | Enterprise+ |

**Rules of thumb**
- **Dynamic Tables** for most transformation layers that are "a SELECT over sources", with minutes-level freshness, especially chains of joins and aggregations.
- **Streams & Tasks** when you need imperative behaviour: SCD2 with expire-and-insert, hard deletes from CDC with custom ordering rules, writing to several tables per change set, calling procedures or external functions, or exact control of when work happens.
- **Materialized views** to speed up repeated queries on a single large table (aggregations, projections) without changing consumers. The optimizer can rewrite queries to use them.
- **dbt** can drive any of these: dynamic table materialization, incremental models with MERGE, or scheduled runs.

**Hybrid patterns are common:** Streams & Tasks for raw-to-silver CDC MERGE (with deletes and ordering), then Dynamic Tables for silver-to-gold aggregates.

<p class="qb-label">Example</p>

```sql
-- Dialect: Snowflake SQL
-- Materialized view: single-table accelerator
CREATE OR REPLACE MATERIALIZED VIEW gold.mv_events_by_day AS
SELECT event_date, event_type, COUNT(*) AS events
FROM silver.events
GROUP BY event_date, event_type;

-- Dynamic table: multi-table declarative transform
CREATE OR REPLACE DYNAMIC TABLE gold.customer_order_summary
  TARGET_LAG = '30 minutes'
  WAREHOUSE = transform_wh
AS
SELECT c.customer_id, c.segment, COUNT(o.order_id) AS orders, SUM(o.amount) AS revenue
FROM silver.customers c
LEFT JOIN silver.orders o ON o.customer_id = c.customer_id
GROUP BY c.customer_id, c.segment;
```

<p class="qb-label">Real-world use case</p>

A platform team uses Streams & Tasks only for the CDC MERGE layer, where delete semantics and LSN ordering matter, and Dynamic Tables for 40 downstream marts. On-call issues dropped because there were far fewer tasks to babysit.

<p class="qb-label">Common mistakes and red flags</p>

- Trying to implement SCD2 history with a dynamic table that only reflects current source state.
- Using materialized views with joins (not supported).
- Rebuilding every layer as Streams & Tasks out of habit.

<p class="qb-label">Follow-up questions</p>

- Can a stream be created on a dynamic table? — Yes; streams on dynamic tables are supported, enabling hybrid patterns.
- How do costs compare? — DTs and Tasks use your warehouse (or serverless tasks); MVs bill serverless maintenance — compare via refresh history and metering views.

</details>

### 42. Listings and cross-region/cross-cloud sharing

<p class="qb-meta">Advanced · Occasional · Data Sharing</p>

<p class="qb-label">Short answer</p>

Listings package a share (plus metadata, documentation, sample queries and optional usage terms) for distribution, either privately to specific accounts or publicly on Snowflake Marketplace. Cross-Cloud Auto-Fulfillment replicates the listing's data to consumer regions automatically, so consumers in other regions or clouds can get it without the provider managing replication.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Direct share vs listing**
| Aspect | Direct share | Listing |
|---|---|---|
| Reach | Accounts in same region/cloud | Any region/cloud (with auto-fulfillment); public or private |
| Metadata | None | Title, description, data dictionary, sample SQL, terms |
| Monetisation | No | Paid listings / usage-based pricing on Marketplace |
| Usage analytics | Limited | Listing consumption metrics for providers |
| Setup | SQL `CREATE SHARE` | Provider Studio (UI) or listing APIs; still built on shares |

**Cross-region mechanics**
- Auto-fulfillment uses replication under the hood. The provider pays replication and storage costs in the target regions, and a refresh schedule controls freshness.
- The older manual alternative: replicate the database to an account in the other region, then share locally (see [Replication and failover for business continuity](#49-replication-and-failover-for-business-continuity)).

**Other sharing constructs**
- **Organisation-level listings** share inside an organisation across accounts (an internal marketplace).
- **Native Apps** distribute code and data together (Snowflake Native App Framework), running in the consumer account.
- **Data clean rooms** support controlled joint analysis without exposing raw data.

**Governance considerations**
- Version and document schemas for consumers, because breaking changes affect external parties.
- Use secure views and contract-like stability for shared objects.
- Track consumption, and set refresh frequency against cost.

<p class="qb-label">Example</p>

```sql
-- Dialect: Snowflake SQL — listing-related monitoring (provider side; available views vary by setup)
SELECT listing_global_name, consumer_account_name, SUM(job_count) AS queries
FROM snowflake.data_sharing_usage.listing_access_history
WHERE query_date >= DATEADD(day, -30, CURRENT_DATE)
GROUP BY 1, 2
ORDER BY queries DESC;
```
The view and column names above are illustrative. Confirm the exact provider usage views and columns in current documentation for your account type.

<p class="qb-label">Real-world use case</p>

A weather-data provider publishes a Marketplace listing with auto-fulfillment to AWS, Azure and GCP regions. Consumers anywhere mount the data in minutes, and the provider tracks which datasets each customer queries.

<p class="qb-label">Common mistakes and red flags</p>

- Assuming direct shares work cross-region.
- Ignoring replication costs of auto-fulfillment.
- No schema stability promise for external consumers.

<p class="qb-label">Follow-up questions</p>

- How fresh is cross-region data? — Depends on the configured replication/refresh interval.
- What is a Native App? — A packaged application (code + optional data) that consumers install and run in their own account.

</details>

### 43. Tag-based masking and sensitive data classification

<p class="qb-meta">Advanced · Occasional · Governance</p>

<p class="qb-label">Short answer</p>

Object tags are key-value metadata you attach to accounts, databases, schemas, tables and columns. Tags propagate through lineage features and are queryable for governance. Attaching a masking policy to a tag protects every column carrying that tag, including future ones. Sensitive data classification scans columns and suggests or applies system tags such as `SNOWFLAKE.CORE.SEMANTIC_CATEGORY` and `PRIVACY_CATEGORY`.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Tags**
- `CREATE TAG governance.tags.pii ALLOWED_VALUES 'email', 'phone', 'name';`
- `ALTER TABLE t MODIFY COLUMN email SET TAG governance.tags.pii = 'email';`
- Inheritance: tags set on a table or schema are inherited by child objects for discovery (`TAG_REFERENCES_WITH_LINEAGE`, `TAG_REFERENCES_ALL_COLUMNS`).
- Query usage with `ACCOUNT_USAGE.TAG_REFERENCES`.

**Tag-based masking**
- `ALTER TAG governance.tags.pii SET MASKING POLICY mask_string, MASKING POLICY mask_number;` (one policy per data type).
- Every column tagged `pii` gets masked by the policy matching its type, with no per-column policy assignment needed.
- Inside the policy, `SYSTEM$GET_TAG_ON_CURRENT_COLUMN('governance.tags.pii')` returns the tag value, so one policy can mask emails and phones differently.
- A column-level policy set directly takes precedence over a tag-based policy.

**Classification**
- Snowflake's sensitive data classification analyses column contents and names to detect semantic categories (EMAIL, PHONE_NUMBER, NAME, NATIONAL_IDENTIFIER and so on) and privacy categories (IDENTIFIER, QUASI_IDENTIFIER, SENSITIVE).
- It can run manually (`SYSTEM$CLASSIFY`) or automatically on a schedule through classification profiles, with optional automatic tagging.
- Results should be reviewed. False positives and negatives happen.

**End-to-end governance flow:** classify, apply tags automatically, enforce masking through tag-based policies, then monitor coverage (untagged PII-looking columns, tagged columns without policies) in a governance dashboard.

<p class="qb-label">Example</p>

```sql
-- Dialect: Snowflake SQL
CREATE TAG IF NOT EXISTS governance.tags.pii ALLOWED_VALUES 'email', 'phone', 'name';

CREATE OR REPLACE MASKING POLICY governance.policies.mask_pii_str AS (val STRING) RETURNS STRING ->
  CASE
    WHEN IS_ROLE_IN_SESSION('PII_READER') THEN val
    WHEN SYSTEM$GET_TAG_ON_CURRENT_COLUMN('governance.tags.pii') = 'phone' THEN '******' || RIGHT(val, 4)
    ELSE '***MASKED***'
  END;

ALTER TAG governance.tags.pii SET MASKING POLICY governance.policies.mask_pii_str;

ALTER TABLE analytics.core.dim_customer MODIFY COLUMN phone SET TAG governance.tags.pii = 'phone';

SELECT object_name, column_name, tag_value
FROM snowflake.account_usage.tag_references
WHERE tag_name = 'PII' AND domain = 'COLUMN';
```

<p class="qb-label">Real-world use case</p>

A healthcare company runs automatic classification on new schemas weekly. Detected PHI columns are tagged, and tag-based masking protects them immediately. The governance team reviews new tags rather than chasing every new table by hand.

<p class="qb-label">Common mistakes and red flags</p>

- Tagging without attaching policies, which documents risk but doesn't protect anything.
- Assuming classification is perfect.
- Defining one masking policy per column when tags would scale.

<p class="qb-label">Follow-up questions</p>

- Can a tag have multiple masking policies? — One per data type (string, number, etc.).
- Do tags propagate to derived tables? — Snowflake supports automatic tag propagation from source to target objects through lineage features in newer releases; check current docs and configuration.

</details>

### 44. Snowflake performance tuning checklist

<p class="qb-meta">Advanced · Very Common · Performance</p>

<p class="qb-label">Short answer</p>

Find the costliest queries from query history, then read each one's profile. Fix the SQL (pruning-friendly filters, correct joins, fewer columns, no unnecessary sorts). Fix the data layout (clustering, search optimization, materialized views or dynamic tables for hot aggregates). Fix the workload setup (right-size and isolate warehouses, multi-cluster for concurrency, the Query Acceleration Service for outliers). Re-measure after each change.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**1. Prioritise**
- Rank queries by total credits or elapsed time × frequency (`QUERY_HISTORY`, `QUERY_ATTRIBUTION_HISTORY`).
- Group by `query_parameterized_hash` to find repeated patterns.

**2. SQL-level**
- Select only the needed columns. Columnar storage reads exactly those.
- Use sargable filters on clustered columns (see [What makes a predicate sargable?](/sql/sql-interview-questions/#46-what-makes-a-predicate-sargable)), and filter early in CTEs.
- Make sure joins have complete keys and no fan-out. Pre-aggregate before joining facts (see [Diagnose a join that multiplies rows (fan-out)](/sql/sql-interview-questions/#28-diagnose-a-join-that-multiplies-rows-fan-out)).
- Avoid `ORDER BY` without `LIMIT` in intermediate steps, avoid huge `IN` lists (join to a table instead), and avoid `SELECT DISTINCT` used to mask duplicates.
- Use `QUALIFY` instead of self joins for "latest row" logic.

**3. Data layout**
- Clustering keys for large tables with mismatched filters (see [Clustering keys, clustering depth and Automatic Clustering](#17-clustering-keys-clustering-depth-and-automatic-clustering)).
- Search optimization for point lookups (see [Search Optimization Service vs clustering](#38-search-optimization-service-vs-clustering)).
- Materialized views or dynamic tables for repeated aggregates (see [Dynamic Tables: target lag and refresh modes](#22-dynamic-tables-target-lag-and-refresh-modes), [Materialized views in Snowflake and their limitations](#23-materialized-views-in-snowflake-and-their-limitations)).
- Incremental models instead of full rebuilds (see [Incremental models and is_incremental()](/dbt/dbt-interview-questions/#15-incremental-models-and-is_incremental)).
- Type and extract hot VARIANT paths into columns (see [VARIANT and semi-structured data](#12-variant-and-semi-structured-data)).

**4. Warehouse and workload**
- Separate warehouses per workload (ETL, BI, data science) to avoid contention.
- Size per workload: bigger for spill-heavy batch, multi-cluster for concurrent BI.
- The Query Acceleration Service for unpredictable large scans (see [Query Acceleration Service](#45-query-acceleration-service)).
- Gen2 warehouses can speed up DML-heavy and analytical workloads (see [Gen2 and Snowpark-optimized warehouses](#36-gen2-and-snowpark-optimized-warehouses)).
- Set `STATEMENT_TIMEOUT_IN_SECONDS` to stop runaway queries.

**5. Caching**
- Keep dashboard SQL deterministic to benefit from the result cache. Choose auto-suspend for BI warehouses with the local cache in mind (see [Snowflake caching layers: result, warehouse (local disk) and metadata](#18-snowflake-caching-layers-result-warehouse-local-disk-and-metadata)).

**6. Verify**
- Benchmark with `USE_CACHED_RESULT = FALSE`, record before and after (credits, time, bytes scanned), and keep a regression check.

<p class="qb-label">Example</p>

```sql
-- Dialect: Snowflake SQL
-- Top repeated query patterns by total execution time in the last 7 days
SELECT query_parameterized_hash,
       ANY_VALUE(query_text)                AS sample_query,
       COUNT(*)                              AS runs,
       SUM(total_elapsed_time) / 1000 / 3600 AS total_hours,
       AVG(partitions_scanned / NULLIF(partitions_total, 0)) AS avg_scan_ratio
FROM snowflake.account_usage.query_history
WHERE start_time > DATEADD(day, -7, CURRENT_TIMESTAMP())
  AND execution_status = 'SUCCESS'
GROUP BY query_parameterized_hash
ORDER BY total_hours DESC
LIMIT 15;
```

<p class="qb-label">Real-world use case</p>

A two-week tuning sprint on the top 15 query patterns cut warehouse spend by 35%. The work was mostly sargable filter rewrites, one clustering key, two dynamic tables replacing repeated aggregations, and splitting one shared warehouse into ETL and BI warehouses.

<p class="qb-label">Common mistakes and red flags</p>

- Tuning random slow queries instead of the costliest patterns.
- Upsizing as the first move.
- Changing several things at once with no measurement.

<p class="qb-label">Follow-up questions</p>

- What is `query_parameterized_hash`? — A hash of the query with literals removed, grouping repeated query shapes.
- How do you prevent regressions? — Track key queries' runtime/bytes in a monitoring table and alert on deviations.

</details>

### 45. Query Acceleration Service

<p class="qb-meta">Advanced · Occasional · Performance</p>

<p class="qb-label">Short answer</p>

The Query Acceleration Service (QAS) offloads eligible parts of large queries, mainly big table scans with filters and aggregations, to shared serverless compute. Outlier queries then finish faster without permanently resizing the warehouse. You enable it per warehouse with a scale factor that caps the extra compute, and pay serverless credits only when it is used. It requires Enterprise edition or above.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**When it helps**
- Ad-hoc or unpredictable analytics where a few queries scan huge amounts of data while most queries are small.
- Queries with **large scans plus selective filters or aggregations**, where the scan can be parallelised.
- Workloads where upsizing the whole warehouse for occasional big queries would waste credits.

**When it doesn't**
- Queries without enough eligible scan work (small tables, highly pruned scans).
- Queries dominated by non-parallelisable steps such as large sorts or certain joins, or with LIMIT without ORDER BY in some cases.
- Persistent heavy workloads, which may be better served by a larger warehouse or better layout.

**Using it**
- Check eligibility: `SYSTEM$ESTIMATE_QUERY_ACCELERATION('<query_id>')` and the `QUERY_ACCELERATION_ELIGIBLE` view (ACCOUNT_USAGE).
- Enable: `ALTER WAREHOUSE w SET ENABLE_QUERY_ACCELERATION = TRUE QUERY_ACCELERATION_MAX_SCALE_FACTOR = 8;`
  - The scale factor is a multiplier of the warehouse size's compute that QAS may lease. `0` means no limit.
- Monitor cost in `QUERY_ACCELERATION_HISTORY`, and per query in `QUERY_HISTORY` (`query_acceleration_bytes_scanned` and related columns).

**Comparison**
| Lever | Effect | Good for |
|---|---|---|
| Bigger warehouse | All queries get more compute | Consistently heavy workloads |
| Multi-cluster | More concurrency | Queuing |
| QAS | Extra compute for eligible scan-heavy queries only | Outlier large scans |
| Clustering / SOS | Less data to scan | Repeated filter patterns |

<p class="qb-label">Example</p>

```sql
-- Dialect: Snowflake SQL
SELECT query_id, eligible_query_acceleration_time, upper_limit_scale_factor
FROM snowflake.account_usage.query_acceleration_eligible
WHERE warehouse_name = 'ADHOC_WH' AND start_time > DATEADD(day, -7, CURRENT_TIMESTAMP())
ORDER BY eligible_query_acceleration_time DESC
LIMIT 10;

ALTER WAREHOUSE adhoc_wh SET
  ENABLE_QUERY_ACCELERATION = TRUE
  QUERY_ACCELERATION_MAX_SCALE_FACTOR = 8;
```

<p class="qb-label">Real-world use case</p>

An analyst warehouse (Medium) served mostly small queries, but a few weekly deep-dive queries scanned 20 TB and took 40 minutes. Enabling QAS with a scale factor of 8 cut those to about 8 minutes, at a fraction of the cost of running the warehouse permanently at 2XL.

<p class="qb-label">Common mistakes and red flags</p>

- Enabling QAS everywhere without checking eligibility.
- No scale factor limit, so cost surprises follow.
- Expecting QAS to fix bad joins.

<p class="qb-label">Follow-up questions</p>

- How is QAS billed? — Serverless credits for the acceleration compute used.
- Does QAS help concurrency? — Not directly; it accelerates individual eligible queries.

</details>

### 46. Analysing spend with ACCOUNT_USAGE views

<p class="qb-meta">Advanced · Common · Cost Management</p>

<p class="qb-label">Short answer</p>

The `SNOWFLAKE.ACCOUNT_USAGE` schema holds up to a year of usage history, with some latency (typically 45 minutes to 3 hours). Use `WAREHOUSE_METERING_HISTORY` for warehouse credits, `METERING_DAILY_HISTORY` for all services, `QUERY_ATTRIBUTION_HISTORY` for per-query compute attribution, the serverless-feature histories, and `STORAGE_USAGE` or `TABLE_STORAGE_METRICS` for storage. The `ORGANIZATION_USAGE` views give org-wide and currency views.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Key views**
| View | What it answers |
|---|---|
| `WAREHOUSE_METERING_HISTORY` | Credits per warehouse per hour (compute + cloud services) |
| `METERING_DAILY_HISTORY` | Daily credits by service type, including the cloud services adjustment |
| `QUERY_HISTORY` | Per-query runtime, bytes, spill, warehouse, user, role, query tag |
| `QUERY_ATTRIBUTION_HISTORY` | Compute credits attributed to individual queries (excludes idle time) |
| `AUTOMATIC_CLUSTERING_HISTORY`, `MATERIALIZED_VIEW_REFRESH_HISTORY`, `SEARCH_OPTIMIZATION_HISTORY`, `PIPE_USAGE_HISTORY`, `SERVERLESS_TASK_HISTORY`, `REPLICATION_USAGE_HISTORY` | Serverless feature credits |
| `STORAGE_USAGE`, `DATABASE_STORAGE_USAGE_HISTORY`, `TABLE_STORAGE_METRICS` | Storage bytes incl. Time Travel and Fail-safe |
| `ORGANIZATION_USAGE.USAGE_IN_CURRENCY_DAILY` | Spend in currency across accounts |

**Analyses worth building**
1. **Warehouse credits trend** by day and warehouse, with week-over-week change.
2. **Idle cost:** warehouse metered credits minus credits attributed to queries, which shows auto-suspend and sizing waste.
3. **Cost by team or pipeline** from query tags or roles through `QUERY_ATTRIBUTION_HISTORY` (see [FinOps: tagging, chargeback and showback](/career/data-engineering-interview-questions/#72-finops-tagging-chargeback-and-showback)).
4. **Top query patterns** by credits (`query_parameterized_hash`; see [Snowflake performance tuning checklist](#44-snowflake-performance-tuning-checklist)).
5. **Serverless creep:** clustering or MV maintenance growing faster than query savings.
6. **Storage:** largest tables, and Fail-safe or Time Travel bytes (see [Fail-safe and how it differs from Time Travel](#5-fail-safe-and-how-it-differs-from-time-travel)).

**Caveats**
- ACCOUNT_USAGE has latency. Use `INFORMATION_SCHEMA` table functions for near-real-time, short-window data (see [INFORMATION_SCHEMA vs ACCOUNT_USAGE (latency and retention)](#15-information_schema-vs-account_usage-latency-and-retention)).
- Credits to currency depends on your contract price. Use ORGANIZATION_USAGE currency views where available.
- Grant access to these views carefully through database roles on the SNOWFLAKE database (for example `SNOWFLAKE.USAGE_VIEWER`).

<p class="qb-label">Example</p>

```sql
-- Dialect: Snowflake SQL
-- Idle (unattributed) credits per warehouse over the last 30 days
WITH metered AS (
    SELECT warehouse_name, SUM(credits_used_compute) AS metered_credits
    FROM snowflake.account_usage.warehouse_metering_history
    WHERE start_time >= DATEADD(day, -30, CURRENT_TIMESTAMP())
    GROUP BY warehouse_name
),
attributed AS (
    SELECT warehouse_name, SUM(credits_attributed_compute) AS query_credits
    FROM snowflake.account_usage.query_attribution_history
    WHERE start_time >= DATEADD(day, -30, CURRENT_TIMESTAMP())
    GROUP BY warehouse_name
)
SELECT m.warehouse_name,
       m.metered_credits,
       COALESCE(a.query_credits, 0)                         AS query_credits,
       m.metered_credits - COALESCE(a.query_credits, 0)     AS idle_credits,
       ROUND(100 * (m.metered_credits - COALESCE(a.query_credits, 0)) / NULLIF(m.metered_credits, 0), 1) AS idle_pct
FROM metered m
LEFT JOIN attributed a ON a.warehouse_name = m.warehouse_name
ORDER BY idle_credits DESC;
```

<p class="qb-label">Real-world use case</p>

The idle-credit report showed a BI warehouse at 55% idle because of a 15-minute auto-suspend. Changing it to 2 minutes saved about 1,000 credits a month, with negligible impact on dashboards.

<p class="qb-label">Common mistakes and red flags</p>

- Expecting ACCOUNT_USAGE to be real-time.
- Ignoring serverless and storage costs.
- Giving everyone ACCOUNTADMIN to see cost data.

<p class="qb-label">Follow-up questions</p>

- How long is ACCOUNT_USAGE retained? — Typically 365 days for most views.
- Does query attribution include idle time? — No; idle warehouse time isn't attributed to queries.

</details>

### 47. Iceberg tables: Snowflake-managed vs externally managed catalogs

<p class="qb-meta">Advanced · Common · Iceberg Tables</p>

<p class="qb-label">Short answer</p>

Snowflake Iceberg tables store data and metadata in Apache Iceberg format in your own cloud storage, defined through an external volume, so other engines can read them. With Snowflake as the catalog, Snowflake manages the metadata and supports full DML and table maintenance. With an external catalog (AWS Glue, an Iceberg REST catalog, Polaris/Open Catalog and others), Snowflake reads, and in newer releases can write, tables that other engines manage.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Building blocks**
- **External volume:** an account-level object pointing to your S3, GCS or Azure location, with IAM trust (like a storage integration).
- **Catalog integration:** used for externally managed tables. It tells Snowflake where to find the Iceberg metadata (Glue, REST catalog, object storage metadata files).
- **Snowflake Open Catalog / Polaris** and the **Horizon Iceberg REST endpoint** let external engines (Spark, Trino, Flink) discover and access Snowflake-managed Iceberg tables.

**Snowflake-managed (Snowflake as catalog)**
| Aspect | Behaviour |
|---|---|
| Writes | Full DML from Snowflake (INSERT, MERGE, UPDATE, DELETE), COPY, Snowpipe, Snowpipe Streaming |
| Maintenance | Snowflake handles compaction, snapshot expiry, clustering |
| Features | Time Travel, cloning, data sharing, policies (with some limits vs native tables) |
| Interop | External engines read via REST catalog/Open Catalog; data sits in your storage |
| Cost | Storage billed by your cloud provider, not Snowflake; compute via warehouses |

**Externally managed (external catalog)**
- Another system owns commits and metadata. Snowflake queries the tables, refreshing metadata manually or automatically.
- Write support to externally managed catalogs (catalog-linked databases with REST catalogs) has been expanding. Check current docs for what is GA.

**Iceberg vs native tables**
- Native tables give the most features and the simplest operation, and Snowflake optimises their storage.
- Iceberg tables give engine interoperability and data in your own bucket, with some feature differences, for example in specific data types, Fail-safe and certain policies. Performance on Snowflake-managed Iceberg is close to native for many workloads.

<p class="qb-label">Example</p>

```sql
-- Dialect: Snowflake SQL
CREATE OR REPLACE EXTERNAL VOLUME lake_vol
  STORAGE_LOCATIONS = ((
    NAME = 's3_lake'
    STORAGE_PROVIDER = 'S3'
    STORAGE_BASE_URL = 's3://acme-lakehouse/iceberg/'
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/snowflake-iceberg-rw'
  ));

CREATE OR REPLACE ICEBERG TABLE analytics.lake.orders (
    order_id    NUMBER(38,0),
    customer_id NUMBER(38,0),
    amount      NUMBER(12,2),
    order_ts    TIMESTAMP_NTZ(6)
)
  CATALOG = 'SNOWFLAKE'
  EXTERNAL_VOLUME = 'lake_vol'
  BASE_LOCATION = 'orders/';
```

<p class="qb-label">Real-world use case</p>

A company runs Spark ML jobs on Databricks and BI on Snowflake. Gold tables became Snowflake-managed Iceberg tables in the company's own S3 bucket. Snowflake writes them, and Spark reads the same files through the Iceberg REST catalog, with no nightly copies.

<p class="qb-label">Common mistakes and red flags</p>

- Assuming Iceberg tables have every native-table feature.
- Letting two catalogs both "own" the same table.
- Forgetting that storage costs move to your cloud bill.

<p class="qb-label">Follow-up questions</p>

- What does the catalog do? — Holds the pointer to the current metadata file and coordinates atomic commits (see [How do open table formats make ACID possible on object storage?](/career/data-engineering-interview-questions/#82-how-do-open-table-formats-make-acid-possible-on-object-storage)).
- Can you convert an externally managed table to Snowflake-managed? — Snowflake supports converting supported external Iceberg tables to Snowflake as catalog (`ALTER ICEBERG TABLE ... CONVERT TO MANAGED`).

</details>

### 48. Transactions and locking behaviour in Snowflake

<p class="qb-meta">Advanced · Occasional · Transactions</p>

<p class="qb-label">Short answer</p>

Snowflake supports ACID transactions with READ COMMITTED isolation. Each statement sees data committed before it started, and a transaction sees its own changes. Reads never block. DML statements take locks: concurrent UPDATE, DELETE and MERGE statements on the same table can block or queue each other, while concurrent INSERTs generally don't conflict. DDL auto-commits any open transaction.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Transaction basics**
- **Autocommit** is on by default, so each statement is its own transaction.
- **Explicit transactions:** `BEGIN` (or `BEGIN TRANSACTION`) … `COMMIT` / `ROLLBACK`.
- **DDL implicitly commits** the current transaction before and after running (CREATE, ALTER, DROP). Don't mix DDL into multi-step DML transactions.
- In stored procedures, transaction behaviour is scoped to the procedure, and there are rules about transactions started inside and outside the procedure.

**Isolation and visibility**
- READ COMMITTED for tables: a statement sees data committed before the statement began, plus the transaction's own uncommitted changes.
- Snapshot reads mean SELECTs never wait for writers.

**Locking**
- UPDATE, DELETE and MERGE lock the affected table (or, in some cases, partitions) for the duration of the transaction. Other writers to the same table **wait**, until `LOCK_TIMEOUT` (default 43,200 seconds = 12 hours) expires.
- INSERT and COPY into the same table can usually run concurrently.
- Hybrid tables use row-level locking (see [Hybrid tables (Unistore)](#50-hybrid-tables-unistore)).
- Inspect locks with `SHOW LOCKS` and `SHOW TRANSACTIONS`. Clear stuck ones with `SELECT SYSTEM$ABORT_TRANSACTION(<id>)`.
- Deadlocks are detected and one transaction is aborted.

**Design implications**
- Avoid long-running explicit transactions holding locks on hot tables.
- Serialise writers to the same table: one MERGE task per target, or queueing in the orchestrator.
- Use the stream-per-consumer pattern, so independent consumers don't contend (see [Streams: standard, append-only, insert-only; offsets and staleness](#21-streams-standard-append-only-insert-only-offsets-and-staleness)).
- Set a reasonable `LOCK_TIMEOUT` for pipelines, so they fail fast rather than waiting 12 hours.

<p class="qb-label">Example</p>

```sql
-- Dialect: Snowflake SQL
ALTER SESSION SET LOCK_TIMEOUT = 600;   -- fail after 10 minutes instead of 12 hours

BEGIN;
DELETE FROM gold.daily_sales WHERE sales_date = '2026-03-01';
INSERT INTO gold.daily_sales
SELECT sales_date, store_id, SUM(amount) FROM silver.sales
WHERE sales_date = '2026-03-01' GROUP BY sales_date, store_id;
COMMIT;

SHOW TRANSACTIONS;   -- inspect open transactions if something seems blocked
```

<p class="qb-label">Real-world use case</p>

Two overlapping dbt runs each tried to MERGE into the same large table. The second waited for hours behind the first's lock and missed its SLA. Preventing overlapping runs in the scheduler and setting `LOCK_TIMEOUT` to 15 minutes turned silent waits into fast, visible failures.

<p class="qb-label">Common mistakes and red flags</p>

- Assuming a CREATE TABLE inside BEGIN…COMMIT is rolled back on failure.
- Not knowing about the default 12-hour lock timeout.
- Running parallel MERGEs into the same table and expecting parallel speed.

<p class="qb-label">Follow-up questions</p>

- Can readers see uncommitted data? — No.
- How do you abort a stuck transaction? — `SYSTEM$ABORT_TRANSACTION(transaction_id)` with the right privileges.

</details>

### 49. Replication and failover for business continuity

<p class="qb-meta">Advanced · Occasional · Business Continuity</p>

<p class="qb-label">Short answer</p>

Snowflake replicates databases, and with replication or failover groups also users, roles, warehouses, integrations and other account objects, to accounts in other regions or clouds within the same organisation. Secondary copies are refreshed on a schedule. Failover groups (Business Critical edition and above) let you promote a secondary to primary during an outage, and client redirect moves connections with a connection URL that you repoint.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Objects**
- **Replication group:** a set of databases and/or account objects replicated together to one or more target accounts as read-only secondaries. It is available on lower editions for databases and shares (check edition matrix).
- **Failover group:** a replication group whose secondaries can be **promoted** to primary (Business Critical and above).
- **Connection objects (client redirect):** a stable connection URL that points to the current primary account. Failover includes repointing it.

**How refresh works**
- `REPLICATION_SCHEDULE = '10 MINUTE'` (or CRON) on the group, or a manual `ALTER ... REFRESH`.
- Each refresh copies changed data incrementally and applies a consistent point-in-time snapshot of the group's objects.
- **RPO** is roughly the refresh interval plus refresh duration. **RTO** is the time to promote plus redirecting clients.

**What to consider**
- **Cost:** data transfer (cross-region and cross-cloud egress), compute for replication, and storage in the target account.
- **Scope:** replicate account objects (users, roles, grants, warehouses, network policies, integrations) in the same failover group. Otherwise the DR account has data but nobody can log in or access it.
- **Pipelines after failover:** Snowpipe, tasks, streams and external stages need configuration for the DR region (for example notifications and storage locations). Test the runbook.
- **Testing:** schedule DR drills that promote, validate, and fail back.

<p class="qb-label">Example</p>

```sql
-- Dialect: Snowflake SQL (on the primary account)
CREATE FAILOVER GROUP fg_core
  OBJECT_TYPES = DATABASES, ROLES, USERS, WAREHOUSES, INTEGRATIONS, NETWORK POLICIES
  ALLOWED_DATABASES = analytics, raw
  ALLOWED_INTEGRATION_TYPES = STORAGE INTEGRATIONS, NOTIFICATION INTEGRATIONS
  ALLOWED_ACCOUNTS = myorg.dr_account_us_west
  REPLICATION_SCHEDULE = '10 MINUTE';

-- (on the DR account) create the secondary, and during an outage promote it
CREATE FAILOVER GROUP fg_core AS REPLICA OF myorg.prod_account_us_east.fg_core;
ALTER FAILOVER GROUP fg_core PRIMARY;
```

<p class="qb-label">Real-world use case</p>

A payments company replicates its core databases and all account objects to a second region every 10 minutes. In a quarterly drill, it promotes the DR account, repoints the connection URL, and verifies dashboards and ingestion recover within the agreed 1-hour RTO.

<p class="qb-label">Common mistakes and red flags</p>

- Replicating databases but not roles, users and integrations.
- Never testing failover.
- Ignoring egress costs of frequent cross-cloud refresh.

<p class="qb-label">Follow-up questions</p>

- What is client redirect? — Connection objects providing a URL that follows the primary, so clients reconnect without config changes.
- Is replication the same as Time Travel? — No; Time Travel protects against logical errors in place, replication against regional outages.

</details>

### 50. Hybrid tables (Unistore)

<p class="qb-meta">Advanced · Occasional · Table Types</p>

<p class="qb-label">Short answer</p>

Hybrid tables are Snowflake's row-oriented, transactional table type for operational workloads. They require a primary key, enforce primary key, unique and foreign key constraints, support secondary indexes and row-level locking, and give low-latency point reads and writes. They can be joined with standard columnar tables in the same SQL.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**What's different from standard tables**
| Aspect | Standard table | Hybrid table |
|---|---|---|
| Storage | Columnar micro-partitions | Row store (with columnar copy for analytics) |
| Primary key | Optional, informational | Required, enforced |
| Unique / foreign keys | Informational | Enforced |
| Indexes | None (pruning-based) | Secondary indexes |
| Locking | Table/partition-level for DML | Row-level |
| Best for | Analytics, large scans | High-concurrency single-row reads/writes, app state |

**Good use cases**
- Application state alongside analytics: order status, user sessions, feature flags.
- Pipeline control and metadata tables (watermarks, job runs) with many concurrent small updates.
- Serving lookups to low-latency APIs.

**Limits and considerations**
- Throughput and storage quotas, plus feature differences: some features such as Time Travel durations, cloning, replication and Fail-safe behave differently or have limits. Check the current docs.
- They're priced differently (storage and request-based components), so model the cost.
- Availability varies by cloud and region (originally AWS commercial regions, expanding).
- They aren't a replacement for a dedicated high-scale OLTP database for very large transactional applications.

**Snowflake Postgres** is another offering in this area: a managed PostgreSQL service in Snowflake for full OLTP compatibility. Pick based on the PostgreSQL ecosystem requirements of the application.

<p class="qb-label">Example</p>

```sql
-- Dialect: Snowflake SQL
CREATE OR REPLACE HYBRID TABLE ops.pipeline_watermarks (
    pipeline_name   VARCHAR(100) NOT NULL PRIMARY KEY,
    last_watermark  TIMESTAMP_NTZ NOT NULL,
    updated_at      TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    INDEX idx_updated_at (updated_at)
);

UPDATE ops.pipeline_watermarks
SET last_watermark = '2026-03-01 05:00:00', updated_at = CURRENT_TIMESTAMP()
WHERE pipeline_name = 'orders_cdc';
```

<p class="qb-label">Real-world use case</p>

A team kept job state and watermarks in a standard table, and concurrent updates from 50 pipelines queued behind table locks. Moving the control table to a hybrid table with row-level locking eliminated the waits.

<p class="qb-label">Common mistakes and red flags</p>

- Using hybrid tables for large analytical scans.
- Assuming every standard-table feature applies.
- Ignoring regional availability and quotas.

<p class="qb-label">Follow-up questions</p>

- Are constraints enforced on standard tables? — Only NOT NULL; PK/FK/UNIQUE are informational (see [Indexes (clustered/non-clustered) and why columnar warehouses differ](/sql/sql-interview-questions/#45-indexes-clusterednon-clustered-and-why-columnar-warehouses-differ)).
- Can you query hybrid and standard tables together? — Yes, in the same query and transaction rules apply.

</details>
