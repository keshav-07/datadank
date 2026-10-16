---
title: "50 Scenario-Based Data Engineering Interview Questions and Answers"
seoTitle: "50 Scenario-Based Data Engineer Interview Questions"
description: "50 scenario-based data engineering interview questions with sample answers: failed pipelines, late data, duplicates, backfills, migrations and design."
technology: ["career", "data-engineering", "system-design"]
topic: ["interview-questions"]
difficulty: "Intermediate"
section: "career"
publishedDate: "2026-10-16"
updatedDate: "2026-10-16"
reviewedDate: "2026-10-16"
related: ["interview-questions:kafka/consumer-lag-causes-fixes", "interview-questions:airflow/slow-scheduler-stuck-tasks"]
versionContext: "Product names and defaults as of October 2026; code snippets state their dialect or runtime"
---

These 50 scenario-based questions cover incidents, late-arriving data, duplicates, backfills, cost spikes, migrations and system design scenarios with concrete numbers. They are grouped from beginner (freshers) to advanced (experienced). Each one starts with the answer you would give in the first 20 seconds; open the full answer for the detailed explanation, an example, how it shows up in real work, common mistakes and the follow-up questions interviewers ask.

## Question list

<details class="qb-index" open>
<summary>All 50 questions</summary>

1. [Ingest from a REST API with strict rate limits](#1-ingest-from-a-rest-api-with-strict-rate-limits)
2. [Daily vendor files over SFTP arrive in inconsistent formats](#2-daily-vendor-files-over-sftp-arrive-in-inconsistent-formats)
3. [Source and target row counts don't match](#3-source-and-target-row-counts-dont-match)
4. [A business user says a dashboard number is wrong](#4-a-business-user-says-a-dashboard-number-is-wrong)
5. [The driver crashes with OOM after a collect()](#5-the-driver-crashes-with-oom-after-a-collect)
6. [Reprocess a single corrupted partition](#6-reprocess-a-single-corrupted-partition)
7. [Should this request be batch or streaming?](#7-should-this-request-be-batch-or-streaming)
8. [Daily reports are off because of time-zone handling](#8-daily-reports-are-off-because-of-time-zone-handling)
9. [A rerun double-counted revenue](#9-a-rerun-double-counted-revenue)
10. [PII found in the raw zone and application logs](#10-pii-found-in-the-raw-zone-and-application-logs)
11. [Object storage costs grow 20% a month](#11-object-storage-costs-grow-20-a-month)
12. [The business wants hourly instead of daily refresh](#12-the-business-wants-hourly-instead-of-daily-refresh)
13. [Set up testing and CI for a brand-new pipeline](#13-set-up-testing-and-ci-for-a-brand-new-pipeline)
14. [Upstream API was down for two days; how do you recover?](#14-upstream-api-was-down-for-two-days-how-do-you-recover)
15. [The nightly pipeline failed at 3 AM; executive dashboards refresh at 7 AM](#15-the-nightly-pipeline-failed-at-3-am-executive-dashboards-refresh-at-7-am)
16. [Late-arriving events are making daily aggregates wrong](#16-late-arriving-events-are-making-daily-aggregates-wrong)
17. [Duplicates discovered in a production fact table](#17-duplicates-discovered-in-a-production-fact-table)
18. [A Snowflake dashboard query takes 3 minutes; target is 10 seconds](#18-a-snowflake-dashboard-query-takes-3-minutes-target-is-10-seconds)
19. [Upstream added and renamed columns without notice (schema drift)](#19-upstream-added-and-renamed-columns-without-notice-schema-drift)
20. [Snowflake cost doubled overnight](#20-snowflake-cost-doubled-overnight)
21. [Databricks bill spiked 60% this month](#21-databricks-bill-spiked-60-this-month)
22. [Recurring SLA breaches on a critical pipeline](#22-recurring-sla-breaches-on-a-critical-pipeline)
23. [The data lake has millions of tiny files](#23-the-data-lake-has-millions-of-tiny-files)
24. [Kafka consumer lag keeps growing](#24-kafka-consumer-lag-keeps-growing)
25. [Out-of-order events in a streaming aggregation](#25-out-of-order-events-in-a-streaming-aggregation)
26. [A join on customer_id is skewed by one huge customer (See also Q264)](#26-a-join-on-customer_id-is-skewed-by-one-huge-customer-see-also-q264)
27. [An incremental load silently missed records](#27-an-incremental-load-silently-missed-records)
28. [Airflow tasks stuck in queued state](#28-airflow-tasks-stuck-in-queued-state)
29. [Auditors need lineage and a data catalog](#29-auditors-need-lineage-and-a-data-catalog)
30. [Migrate orchestration from Airflow to a managed service](#30-migrate-orchestration-from-airflow-to-a-managed-service)
31. [A hot partition in a Kafka topic](#31-a-hot-partition-in-a-kafka-topic)
32. [Source sends full daily dumps; detect only the changes](#32-source-sends-full-daily-dumps-detect-only-the-changes)
33. [Share data with an external partner securely](#33-share-data-with-an-external-partner-securely)
34. [A MERGE into a large table runs for hours](#34-a-merge-into-a-large-table-runs-for-hours)
35. [Design the dimensional model for e-commerce orders](#35-design-the-dimensional-model-for-e-commerce-orders)
36. [A pipeline gets slower every month](#36-a-pipeline-gets-slower-every-month)
37. [Make KPIs consistent across three BI tools](#37-make-kpis-consistent-across-three-bi-tools)
38. [Backfill two years of history without breaking production](#38-backfill-two-years-of-history-without-breaking-production)
39. [A Spark job went from 40 minutes to 4 hours](#39-a-spark-job-went-from-40-minutes-to-4-hours)
40. [Design a real-time clickstream pipeline (50k events/sec)](#40-design-a-real-time-clickstream-pipeline-50k-eventssec)
41. [CDC from a PostgreSQL OLTP database into the warehouse](#41-cdc-from-a-postgresql-oltp-database-into-the-warehouse)
42. [Migrate an on-prem Oracle/Teradata warehouse to the cloud](#42-migrate-an-on-prem-oracleteradata-warehouse-to-the-cloud)
43. [SCD Type 2 on a 500-million-row customer dimension](#43-scd-type-2-on-a-500-million-row-customer-dimension)
44. [Fulfil a GDPR deletion request across lake, warehouse and backups (See also Q85)](#44-fulfil-a-gdpr-deletion-request-across-lake-warehouse-and-backups-see-also-q85)
45. [Design a data platform from scratch for a growing startup](#45-design-a-data-platform-from-scratch-for-a-growing-startup)
46. [Multiple jobs writing to the same table cause conflicts](#46-multiple-jobs-writing-to-the-same-table-cause-conflicts)
47. [Build a data quality framework for 200 tables](#47-build-a-data-quality-framework-for-200-tables)
48. [Merge customer data after an acquisition (entity resolution)](#48-merge-customer-data-after-an-acquisition-entity-resolution)
49. [Build point-in-time-correct training data for an ML model](#49-build-point-in-time-correct-training-data-for-an-ml-model)
50. [Add multi-region disaster recovery to the platform](#50-add-multi-region-disaster-recovery-to-the-platform)

</details>

## Beginner scenario-based questions (freshers)

### 1. Ingest from a REST API with strict rate limits

<p class="qb-meta">Beginner · Common</p>

**Scenario:** You need daily data from a SaaS API: 2 M records, a maximum of 100 records per page, a rate limit of 10 requests per second, and a daily quota of 50,000 requests. It supports `updated_since` filtering and cursor pagination. The full history is 30 M records, and data must be loaded by 07:00.

<details class="qb-more">
<summary>Sample answer and approach</summary>

<p class="qb-label">What the interviewer is testing</p>

- Capacity math against limits.
- Incremental extraction, pagination, retries and checkpoints.
- Raw landing and idempotent loading.

<p class="qb-label">Step-by-step approach</p>

1. **Do the math:**
   - Daily changes: if 2 M records are updated per day, that's 20,000 pages, which takes 2,000 seconds (about 33 minutes) at 10 requests per second. It fits the 50,000 quota.
   - Initial history: 30 M records is 300,000 requests, so about 6 days at the quota. Plan a phased backfill, or ask the vendor for a bulk export.
2. **Incremental:** `updated_since = last_watermark - overlap (15 min)`. Store the watermark after a successful load.
3. **Pagination:** follow the cursors until done, and guard against loops (see [API ingestion with pagination (offset, cursor, next-link)](/python/python-interview-questions/#46-api-ingestion-with-pagination-offset-cursor-next-link)).
4. **Rate limiting:** a token bucket at 9 requests per second for a safety margin. Honour `Retry-After` on 429. Exponential backoff with jitter for 5xx (see [Retries with exponential backoff and rate limiting](/python/python-interview-questions/#47-retries-with-exponential-backoff-and-rate-limiting)).
5. **Checkpointing:** persist the cursor per page, so a crash resumes mid-run.
6. **Land the raw pages** as JSON in object storage partitioned by run date, then load to the warehouse and MERGE on ID.
7. **Quota monitoring:** track requests used, and alert near 80%.

<p class="qb-label">Sample strong answer</p>

"First the math: 2 M changed records at 100 per page is 20,000 requests, about 35 minutes at a safe 9 requests per second, and 40% of the daily quota. Fine. The 30 M-record history needs 300,000 requests, so I'd either ask the vendor for a bulk export or backfill over 6–7 days within the quota, alongside the daily incremental. The extractor is a Python job: `updated_since` with a 15-minute overlap, cursor pagination with a checkpoint after each page, a token-bucket limiter, and retries only on 429 and 5xx, honouring `Retry-After`. Pages land as raw JSON in S3 by date, then a COPY and MERGE on `id` into the warehouse, which is idempotent, so overlaps and reruns don't duplicate. Starting at 05:00 leaves a margin before 07:00, and I'd alert if quota use passes 80% or the run isn't done by 06:15."

<p class="qb-label">Architecture and flow</p>

```text
Scheduler 05:00 ─▶ extractor (updated_since = watermark - 15m)
                      │  token bucket 9 rps, retries 429/5xx, cursor checkpoint per page
                      ▼
                  S3 raw/api_x/dt=YYYY-MM-DD/page-*.json
                      ▼
                  COPY → staging ─▶ MERGE on id ─▶ target table ─▶ update watermark
Monitoring: quota used %, run duration, records/page anomalies
```

<p class="qb-label">Alternatives and trade-offs</p>

| Option | Pros | Cons |
|---|---|---|
| Vendor bulk export for history | Saves quota/time | May need manual request |
| Parallel workers | Faster | Must share one rate limiter; same quota |
| Managed connector | No code | Connector may not support the API/cost |
| Webhooks for changes | Near-real-time | Delivery guarantees vary; need replay |

<p class="qb-label">Follow-up questions</p>

- How do you detect deletes? — Deleted-records endpoint, webhooks, or periodic full ID reconciliation.
- What if the run exceeds quota? — Resume next day from checkpoint; prioritise critical objects.
- How do you test this? — Mock API with pagination and 429 responses (see [Mocking external services in tests](/python/python-interview-questions/#48-mocking-external-services-in-tests)).

</details>

### 2. Daily vendor files over SFTP arrive in inconsistent formats

<p class="qb-meta">Beginner · Common</p>

**Scenario:** Twelve vendors drop daily CSV and Excel files on an SFTP server. Problems include changing column orders and names, different delimiters, mixed date formats (DD/MM vs MM/DD), occasional missing files, and duplicate re-uploads. Finance relies on the data by 09:00.

<details class="qb-more">
<summary>Sample answer and approach</summary>

<p class="qb-label">What the interviewer is testing</p>

- Robust file ingestion design: landing, validation and quarantine.
- Per-vendor configuration instead of code changes.
- Handling missing, late and duplicate files.
- Communication with vendors and agreements.

<p class="qb-label">Step-by-step approach</p>

1. **Land the raw files:** copy everything from SFTP to object storage under `vendor/date/` immutably, recording a checksum (for duplicate detection) and the arrival time.
2. **Configure each vendor:** expected filename pattern, file type, delimiter, encoding, a header mapping (vendor column to canonical column), the date format, required columns, and the expected arrival window.
3. **Parse and validate:** apply the config, map headers by name (not by position), parse dates with the vendor-specific format, validate rows (see [Coding: validate records and split good from bad](/python/python-interview-questions/#34-coding-validate-records-and-split-good-from-bad)), and quarantine bad rows.
4. **Duplicates:** skip files with identical checksums. For re-uploads with changes, replace that vendor-day partition (idempotent).
5. **Missing or late files:** alert if not received by the window end, and publish with a "vendor X missing" flag.
6. **Load** into canonical staging tables, then transform.
7. **Vendor management:** share a file spec. Send automated rejection reports to vendors.

<p class="qb-label">Sample strong answer</p>

"I'd separate transport from parsing. A job polls SFTP every 15 minutes and copies new files to S3 under `raw/vendor=X/dt=YYYY-MM-DD/` with a SHA-256 checksum, so byte-identical re-uploads are skipped. Each vendor has a YAML config: filename pattern, delimiter, encoding, a header-name mapping to our canonical columns, the date format, and the expected arrival time. The parser maps columns by header name, so reordering doesn't matter, and an unknown or missing required column fails that file with a clear message. Rows failing validation go to quarantine, and the vendor gets an automatic report. Loads replace the vendor-day partition, so a corrected re-upload simply overwrites. If a file hasn't arrived by 08:00, we alert and finance sees which vendors are missing. Onboarding a new vendor or format change becomes a config PR, not new code."

<p class="qb-label">Architecture and flow</p>

```text
SFTP ─▶ poller (every 15m) ─▶ S3 raw/vendor=X/dt=D/ (+checksum, arrival time)
                                   │
                    vendor config (YAML: mapping, delimiter, date format, window)
                                   ▼
                     parser + validation ─┬─▶ canonical staging (replace vendor-day)
                                          └─▶ quarantine + vendor rejection report
Alerts: missing file by 08:00, schema mismatch, high reject rate
```

<p class="qb-label">Alternatives and trade-offs</p>

| Option | Pros | Cons |
|---|---|---|
| Hard-coded parser per vendor | Quick initially | Code change for every format tweak |
| Config-driven parser | Scales to many vendors | Upfront framework effort |
| Managed file ingestion (Auto Loader, Snowpipe) | Incremental, scalable | Still need per-vendor mapping |
| Vendors push to API instead | Structured | Requires vendor capability |

<p class="qb-label">Follow-up questions</p>

- How do you detect DD/MM vs MM/DD? — Configured per vendor; validate with checks (day > 12 detection) and reject ambiguous changes.
- How do you handle Excel files? — Parse with openpyxl/pandas with explicit sheet/columns; prefer CSV in agreements.
- What if a vendor re-sends last week's file with corrections? — Replace that vendor-date partition and reprocess downstream.

</details>

### 3. Source and target row counts don't match

<p class="qb-meta">Beginner · Very Common</p>

**Scenario:** A nightly load copies `orders` from a MySQL source into the warehouse. Monitoring shows the source has 1,204,331 rows for yesterday, and the target has 1,198,870, a difference of 5,461 (0.45%). The business asks whether reports are wrong.

<details class="qb-more">
<summary>Sample answer and approach</summary>

<p class="qb-label">What the interviewer is testing</p>

- A methodical reconciliation approach.
- Knowing the usual causes: timing, filters, duplicates, time zones, deletes, failed batches.
- Distinguishing expected differences from bugs.

<p class="qb-label">Step-by-step approach</p>

1. **Make sure both counts measure the same thing:** the same definition of "yesterday" (timezone), the same filters (soft-deleted rows, test orders), and the counts taken at a comparable point in time.
2. **Narrow it down:** compare counts by hour or by ID ranges to find where the gap sits.
3. **Find the missing keys:** export the source IDs for the gap window and anti-join them against the target (or compare hash buckets).
4. **Typical causes:**
   - Timezone boundary (orders between 00:00 and 05:30 local).
   - Late commits after the extract watermark (see [An incremental load silently missed records](#27-an-incremental-load-silently-missed-records)).
   - Rows rejected by validation or quarantine.
   - Filters in the transformation.
   - Dedupe that removed real rows.
   - A failed batch.
   - Hard deletes in the source.
5. **Fix and backfill** the root cause, and reprocess the window.
6. **Prevent:** automated reconciliation (counts and sums per day) with tolerances and alerts.

<p class="qb-label">Sample strong answer</p>

"I'd first check we're comparing like with like. The source count uses MySQL server time (UTC), and the target counts by `order_date` in the business timezone. Recounting the target in UTC narrows the gap to 3,102. Then I'd bucket by hour, and the gap is concentrated at 23:50–00:10 UTC. Our extract runs at 00:05 with `updated_at < run_start`, and long transactions committed after that with earlier timestamps were skipped and never picked up, a classic watermark gap. An anti-join of IDs confirms the missing rows all have `updated_at` within the last 15 minutes before the extract. The fix is a lookback overlap on the watermark (re-read the last 30 minutes) with MERGE on `order_id`, then a backfill of the last 7 days. I'd add a daily reconciliation check of counts and sums of `amount` per day in UTC, alerting above 0.01%. To the business: yes, yesterday's revenue was understated by about 0.25%, corrected by noon."

<p class="qb-label">Architecture and flow</p>

```text
Source counts ──┐
                ├─ align definitions (timezone, filters, snapshot time)
Target counts ──┘
        ▼
Bucket by hour / id range ─▶ locate gap ─▶ anti-join keys ─▶ classify cause
        ▼
Fix (e.g. watermark overlap + MERGE) ─▶ backfill window ─▶ automated daily reconciliation (counts + sums, tolerance alerts)
```

<p class="qb-label">Alternatives and trade-offs</p>

| Check | Catches | Cost |
|---|---|---|
| Row counts | Missing/extra rows | Cheap |
| Sums of key measures | Value corruption | Cheap |
| Key-level anti-joins | Exact missing records | Moderate |
| Row hash comparison | Any column difference | Higher |

<p class="qb-label">Follow-up questions</p>

- What differences are acceptable? — Documented ones (test orders filtered, quarantined rows) reported explicitly.
- How do you reconcile billions of rows? — Bucketed aggregates/hashes per partition, drill down only into mismatched buckets.
- What if the source has hard deletes? — Target may have extra rows; need CDC or periodic full-key comparison.

</details>

### 4. A business user says a dashboard number is wrong

<p class="qb-meta">Beginner · Very Common</p>

**Scenario:** The VP of Sales messages you: "The dashboard says 1,240 new customers last month, but my team counted about 1,400 in Salesforce. Which is right? I'm presenting tomorrow."

<details class="qb-more">
<summary>Sample answer and approach</summary>

<p class="qb-label">What the interviewer is testing</p>

- Calm stakeholder handling and clarifying questions.
- Tracing a metric through lineage to its definition and data.
- Distinguishing definition differences from data bugs.
- Communication and follow-up: documentation, a semantic layer.

<p class="qb-label">Step-by-step approach</p>

1. **Acknowledge and clarify:** what exactly did they count (accounts, contacts, opportunities won)? Which date field (created vs closed-won)? Which filters (region, test accounts)?
2. **Trace the metric:** dashboard → semantic model or SQL → mart → staging → source. Find the definition: "new customers = accounts with their first paid invoice in the month".
3. **Reproduce both numbers:** query the mart for 1,240, and query the Salesforce data for about 1,400 using their definition.
4. **Explain the gap:** for example, Salesforce counts closed-won opportunities (including deals not yet invoiced, 120 of them) plus duplicate accounts (40). Or there's a real bug, such as a missing region or broken join.
5. **Resolve:** if it's a definition difference, explain it clearly and offer both metrics, properly labelled. If it's a bug, fix it, backfill and tell people.
6. **Prevent:** a documented metric definition, a semantic layer, and a glossary.

<p class="qb-label">Sample strong answer</p>

"I'd reply quickly that I'm on it and will have an answer this afternoon, and ask how her team counted: from Salesforce opportunities marked closed-won in March? Then I'd trace our metric. The dashboard uses `new_customers` from the semantic layer, defined as accounts whose first paid invoice is in the month. Reproducing her number from Salesforce data gives 1,398 closed-won deals. The difference: 118 deals closed in late March but invoiced in April, and 40 duplicate account records for the same customer. Neither is a pipeline bug, just two definitions. I'd send her a short note with both numbers, what each means, and the reconciliation table, and suggest that for a sales presentation 'closed-won deals' might be the better metric. We could add it to the dashboard, clearly labelled. I'd also put both definitions into our metric glossary and ask the CRM admin to merge the duplicate accounts."

<p class="qb-label">Architecture and flow</p>

```text
User question ─▶ clarify definition/date/filters
        ▼
Dashboard → semantic layer/metric definition → mart → staging → source (lineage)
        ▼
Reproduce both numbers ─▶ reconcile differences (definition vs data issue)
        ├─ definition: explain, label, offer correct metric
        └─ bug: fix, backfill, notify
        ▼
Document in metric glossary / semantic layer
```

<p class="qb-label">Alternatives and trade-offs</p>

| Response | Pros | Cons |
|---|---|---|
| "Our number is right" | Fast | Damages trust; may be wrong |
| Full reconciliation with explanation | Builds trust, finds real bugs | Takes time |
| Change metric to match user | Pleases user | Inconsistent definitions elsewhere |
| Centralised metric definitions | Prevents recurrence | Requires governance effort |

<p class="qb-label">Follow-up questions</p>

- How do you avoid this happening repeatedly? — Semantic layer, metric owners, descriptions on dashboards (see [What is a semantic/metrics layer?](/career/data-engineering-interview-questions/#75-what-is-a-semanticmetrics-layer), [dbt Semantic Layer and MetricFlow (See also Q99)](/etl-elt/dbt-interview-questions/#43-dbt-semantic-layer-and-metricflow-see-also-q99)).
- What if it was a bug and numbers were presented already? — Issue a correction with impact, root cause and fix.
- How do you find which tables feed the dashboard? — Exposures/lineage tools (see [Exposures](/etl-elt/dbt-interview-questions/#26-exposures)).

</details>

### 5. The driver crashes with OOM after a collect()

<p class="qb-meta">Beginner · Common</p>

**Scenario:** A PySpark job reads 150 M customer records, filters them, calls `.collect()` to loop in Python and build an export CSV, and crashes with `java.lang.OutOfMemoryError` (or "Total size of serialized results is bigger than spark.driver.maxResultSize"). Someone proposes raising driver memory to 64 GB.

<details class="qb-more">
<summary>Sample answer and approach</summary>

<p class="qb-label">What the interviewer is testing</p>

- Understanding of driver vs executor roles.
- Knowing distributed alternatives to collect.
- Resisting the temptation to "just add memory".

<p class="qb-label">Step-by-step approach</p>

1. **Explain the cause:** `collect()` pulls every row into the driver's memory, as Python objects, which is even larger. The driver is a single machine.
2. **Ask why it's collected:** usually to write a file, call an API, or compute something that Spark can do itself.
3. **Replace it:**
   - **Writing files:** `df.write.csv(...)` from the executors, in parallel, with `coalesce(n)` if they want fewer files, or a single file for small results.
   - **API calls:** `foreachPartition` with batched calls and rate limits.
   - **Computations:** use Spark aggregations, and only collect small results.
   - **Inspection:** `limit(n)` / `take(n)` / `show`.
   - **Iterating locally:** `toLocalIterator()` streams partitions to the driver one at a time (it's still slow, but bounded memory).
4. **Explain why raising memory is the wrong default:** it's expensive, the data will grow again, and it's still single-threaded processing.

<p class="qb-label">Sample strong answer</p>

"`collect()` moves all 150 M rows into the driver JVM, then into Python objects, so even 64 GB may not be enough, and we'd be processing on one machine anyway. I'd ask what the loop does. Here it formats rows and writes a CSV, which Spark can do directly: `df.select(...).write.option('header', True).csv('s3://exports/customers/2026-03-01/')`, executed in parallel by the executors. If the consumer needs a single file, I'd check the size. For a few GB, `coalesce(1)` after the heavy work, or merging part files afterwards. For huge outputs I'd agree on multiple files with a manifest. If the loop calls an external API per row, I'd use `foreachPartition`, so each executor opens one client and batches requests. Raising driver memory treats the symptom. The fix is keeping data distributed."

<p class="qb-label">Architecture and flow</p>

```text
Anti-pattern: executors ─▶ collect() ─▶ DRIVER (all rows) ─▶ Python loop ─▶ file    ✗ OOM

Fix:
executors ─▶ df.write.csv/parquet (parallel) ─▶ s3://exports/... (+ _SUCCESS/manifest)
executors ─▶ foreachPartition(batch API calls with rate limit)
driver    ◀─ only small aggregates / take(n)
```

<p class="qb-label">Alternatives and trade-offs</p>

| Option | Use | Caveat |
|---|---|---|
| `df.write` | Files/tables | Multiple files by default |
| `foreachPartition` | External sinks | Need idempotency, rate limits |
| `toLocalIterator` | Must process on driver | Slow; sequential |
| `toPandas()` on aggregated result | Small results | Still bounded by driver memory |

<p class="qb-label">Follow-up questions</p>

- What's `spark.driver.maxResultSize`? — Limit on total serialized results returned to the driver (default 1 GB).
- Is `coalesce(1)` safe? — Only for small outputs, and after heavy transformations (see [repartition vs coalesce](/pyspark/pyspark-interview-questions/#7-repartition-vs-coalesce)).
- What else causes driver OOM? — Large broadcasts, huge plans, many small files metadata (see [Driver OOM vs executor OOM: causes and fixes](/pyspark/pyspark-interview-questions/#43-driver-oom-vs-executor-oom-causes-and-fixes)).

</details>

### 6. Reprocess a single corrupted partition

<p class="qb-meta">Beginner · Common</p>

**Scenario:** The `sales` fact table, partitioned by `sale_date`, has corrupted data for 2026-03-14: a bug doubled discount values. Other days are fine. Dashboards for that day show wrong margins. The bug is fixed in code, and raw data for that day is still available.

<details class="qb-more">
<summary>Sample answer and approach</summary>

<p class="qb-label">What the interviewer is testing</p>

- Idempotent, partition-scoped reprocessing.
- Downstream impact management (aggregates, extracts).
- Verification and communication.

<p class="qb-label">Step-by-step approach</p>

1. **Confirm the scope:** only 2026-03-14? Check neighbouring days and the deployment window of the bug.
2. **Find the downstream dependencies:** aggregates, marts and extracts built from that partition (lineage).
3. **Reprocess atomically:** rerun the job for `sale_date = 2026-03-14` with the fixed code, replacing the partition (`replaceWhere`, `INSERT OVERWRITE PARTITION`, delete+insert in a transaction).
4. **Rebuild the downstream** partitions and aggregates for that date (weekly and monthly rollups that include it).
5. **Verify:** discount totals against the source, margin sanity checks, and tests.
6. **Communicate:** the corrected numbers and any reports that used the bad data.
7. **Prevent:** a test or range check on discounts, for example `discount <= gross_amount`.

<p class="qb-label">Sample strong answer</p>

"I'd first confirm the blast radius. The bug shipped on the 14th and was fixed on the 15th, so only that day's partition is affected, and lineage shows it feeds the daily margin mart and the March monthly rollup. Because the job is parameterised by date and writes with `replaceWhere sale_date = '2026-03-14'`, I can rerun just that date with the fixed code, and the replacement is atomic, so readers see either the old or new partition. Then I'd rebuild the downstream daily margin for the 14th and the March rollup. I'd verify discount totals against the POS source for that day, check that margins look normal, and run the tests. Then I'd tell finance the 14th's margins were corrected and by how much. Finally, I'd add a test that discount never exceeds the gross amount, which would have caught this before the dashboard."

<p class="qb-label">Architecture and flow</p>

```text
Identify scope (date=2026-03-14) ─▶ lineage: downstream assets
        ▼
rerun job --date 2026-03-14 (fixed code) ─▶ REPLACE partition (atomic)
        ▼
rebuild downstream: daily margin (03-14), monthly rollup (March)
        ▼
verify vs source totals + tests ─▶ communicate correction ─▶ add discount range test
```

<p class="qb-label">Alternatives and trade-offs</p>

| Option | Pros | Cons |
|---|---|---|
| Partition replace | Precise, fast | Requires partitioned, parameterised jobs |
| Full table rebuild | Simple | Expensive, slow |
| In-place UPDATE fix | Quick for simple bugs | Diverges from code logic; harder to audit |
| Time travel restore | Instant rollback | Only if a good prior version exists; doesn't apply fix |

<p class="qb-label">Follow-up questions</p>

- What if the job isn't idempotent? — Make it so before rerunning (delete+insert by partition), or you'll duplicate.
- What if raw data expired? — Reprocess from the oldest retained layer, or fix via targeted UPDATE with documented logic.
- How do you know downstream rollups include that day? — Lineage plus date-range logic of each model.

</details>

### 7. Should this request be batch or streaming?

<p class="qb-meta">Beginner · Common</p>

**Scenario:** The marketing team asks for a "real-time" campaign performance dashboard. Data comes from ad platform APIs (which update hourly), web events (streaming), and CRM conversions (nightly export). Today the dashboard refreshes daily.

<details class="qb-more">
<summary>Sample answer and approach</summary>

<p class="qb-label">What the interviewer is testing</p>

- Clarifying the real latency need vs the stated wish.
- Understanding source constraints (you can't be fresher than your sources).
- Cost and complexity trade-offs, and incremental options (micro-batch).

<p class="qb-label">Step-by-step approach</p>

1. **Ask what decision the data drives** and how quickly: pausing a failing campaign within an hour? Daily budget allocation?
2. **Check the source freshness limits:** ad APIs are hourly and CRM is nightly, so true real-time is impossible for conversion metrics.
3. **Define the freshness SLA per metric:** spend and impressions hourly, web engagement every 5–15 minutes if needed, conversions daily.
4. **Pick the simplest architecture that meets it:**
   - Hourly micro-batch for ad spend.
   - Web events through streaming ingestion with 5–15 minute aggregation (or a continuous pipeline).
   - Conversions daily, with clearly labelled latency.
5. **Estimate the cost:** streaming infrastructure and on-call vs scheduled jobs.
6. **Communicate:** the dashboard shows a "data as of" time per metric.

<p class="qb-label">Sample strong answer</p>

"I'd ask marketing what they'd do differently with real-time data. Usually the answer is 'pause underperforming campaigns during the day', which needs hourly or 15-minute freshness, not seconds. Then I'd point out the source limits: ad platforms update spend hourly and CRM conversions arrive nightly, so the dashboard can't be fresher than that, whatever we build. My proposal is an hourly micro-batch for ad spend and impressions, web engagement metrics every 15 minutes from the event stream we already have, and conversions daily, with each tile showing its 'as of' time. That's achievable with scheduled incremental jobs or a 15-minute triggered pipeline, at a fraction of the cost and on-call burden of a full streaming architecture. If they later need conversion attribution within minutes, we'd revisit with the CRM team about streaming conversions."

<p class="qb-label">Architecture and flow</p>

```text
Ad APIs (hourly) ──────▶ hourly micro-batch ─────┐
Web events (stream) ───▶ 15-min aggregation ─────┼─▶ campaign mart ─▶ dashboard (per-tile "as of")
CRM conversions (daily) ▶ nightly batch ─────────┘
Decision driver: "pause campaigns intraday" → 15–60 min freshness is enough
```

<p class="qb-label">Alternatives and trade-offs</p>

| Option | Latency | Cost/complexity | Fit |
|---|---|---|---|
| Daily batch | 24h | Lowest | Current state; too slow |
| Hourly/15-min micro-batch | 15–60 min | Low | Matches sources and decisions |
| Full streaming | Seconds | High (infra, on-call) | Not justified by sources |
| Hybrid (stream web, batch others) | Mixed | Medium | Good if web metrics matter most |

<p class="qb-label">Follow-up questions</p>

- When is streaming clearly justified? — Fraud detection, operational alerting, personalisation — decisions within seconds/minutes (see [What is a data swamp, and how do you prevent one?](/career/data-engineering-interview-questions/#43-what-is-a-data-swamp-and-how-do-you-prevent-one)).
- How do you avoid two codebases? — Engines with unified batch/stream APIs, or incremental batch at short intervals.
- How do you set expectations? — Freshness SLAs per metric and visible timestamps.

</details>

### 8. Daily reports are off because of time-zone handling

<p class="qb-meta">Beginner · Common</p>

**Scenario:** A retailer operates in India (IST, UTC+5:30) and the US (several time zones). Daily sales reports group by `DATE(order_ts)`, where `order_ts` is stored in UTC. Store managers in India say their daily totals don't match the till, and US West Coast managers say late-evening sales show up on the next day.

<details class="qb-more">
<summary>Sample answer and approach</summary>

<p class="qb-label">What the interviewer is testing</p>

- Understanding timestamp storage vs business-day logic.
- Correct conversion per store timezone, including DST.
- Data modelling (a local date column), and communication of the fix.

<p class="qb-label">Step-by-step approach</p>

1. **Confirm:** timestamps are stored in UTC (good). The reports take the UTC date, which is wrong for local business days.
2. **Define the business day:** the store-local calendar date, possibly with a trading-day cut-off (for example a business day running 04:00 to 04:00 for late-night stores).
3. **Add the store timezone** (IANA zone, such as `Asia/Kolkata` or `America/Los_Angeles`) to `dim_store`.
4. **Compute `local_business_date`** in the fact table: `CONVERT_TIMEZONE('UTC', store_tz, order_ts)::date`, which handles DST automatically.
5. **Recompute the history** for the affected period, and update reports to group by `local_business_date`.
6. **Handle global reports:** decide whether company-wide daily totals use UTC days or headquarters' timezone, and document it.
7. **Test:** cases around midnight and DST transitions (see [datetime and time zones with zoneinfo](/python/python-interview-questions/#36-datetime-and-time-zones-with-zoneinfo)).

<p class="qb-label">Sample strong answer</p>

"The data is stored correctly in UTC, but `DATE(order_ts)` gives the UTC date, while a store manager in Mumbai thinks in IST, five and a half hours ahead. Sales from 00:00 to 05:30 IST land on the previous day, and a Los Angeles sale at 9 PM PST is already the next day in UTC. I'd add an IANA timezone to `dim_store` and compute a `local_business_date` column in the fact table with `CONVERT_TIMEZONE('UTC', store_tz, order_ts)::date`. Using IANA zones rather than fixed offsets handles US daylight saving correctly. Store-level reports group by `local_business_date`. For company-wide totals, finance picks a convention, say headquarters time, and we label it on the dashboard. I'd backfill the column for the last two years, add tests with orders at 23:59 and 00:01 local and on DST change days, and tell store managers when their numbers will match the till."

<p class="qb-label">Architecture and flow</p>

```text
fct_orders.order_ts (UTC) ─┐
dim_store.timezone (IANA) ─┴─▶ local_business_date = CONVERT_TIMEZONE('UTC', tz, order_ts)::date
                                           │
             store reports: GROUP BY local_business_date
             global reports: documented convention (HQ tz or UTC), labelled
Tests: midnight boundaries, DST transitions
```

<p class="qb-label">Alternatives and trade-offs</p>

| Option | Pros | Cons |
|---|---|---|
| Store local timestamps only | Easy local reporting | Hard global comparisons; DST ambiguity |
| UTC + computed local date (recommended) | Correct both ways | Need store tz dimension |
| Fixed offsets (+05:30) | Simple | Wrong for DST regions |
| Convert in BI tool | No pipeline change | Inconsistent across tools |

<p class="qb-label">Follow-up questions</p>

- What about stores that change timezone (rare)? — SCD2 on store timezone, or effective-dated.
- How do you handle online orders without a store? — Customer timezone or company default, documented.
- Snowflake TIMESTAMP_TZ vs NTZ? — Store UTC (NTZ by convention, or TZ/LTZ explicitly); avoid ambiguous session-dependent behaviour.

</details>

### 9. A rerun double-counted revenue

<p class="qb-meta">Beginner · Common</p>

**Scenario:** A daily job failed halfway through loading `fct_payments`. The on-call engineer reran it, and revenue for that day doubled on dashboards. The job does `INSERT INTO fct_payments SELECT ... FROM staging WHERE payment_date = :run_date`, with no transaction around the steps.

<details class="qb-more">
<summary>Sample answer and approach</summary>

<p class="qb-label">What the interviewer is testing</p>

- Understanding idempotency and why append-style loads fail on reruns.
- How to fix both the data and the design.
- Operational practices for reruns.

<p class="qb-label">Step-by-step approach</p>

1. **Fix the data first:** delete the run date's rows and reload them once, or dedupe by `payment_id` keeping one copy. Verify against the source.
2. **Explain the cause:** the first attempt inserted part or all of the rows before failing (or failed after the insert), and the rerun inserted them again.
3. **Redesign for idempotency:**
   - **Delete+insert in one transaction** for the run date: `BEGIN; DELETE ... WHERE payment_date = :d; INSERT ...; COMMIT;`.
   - Or **MERGE** on `payment_id`.
   - Or a partition overwrite (`INSERT OVERWRITE`, `replaceWhere`).
4. **Add tests:** a uniqueness test on `payment_id`, and a reconciliation of daily totals with the source.
5. **Runbook:** reruns are safe by design, and on-call doesn't need special steps.

<p class="qb-label">Sample strong answer</p>

"The job appends rows for the date, so any rerun after a partial or complete insert duplicates them. That's why revenue doubled. I'd fix the data first: delete all `fct_payments` rows for that date and reload from staging once inside a transaction, then check that the daily total matches the payment provider's settlement report. Then I'd make the load idempotent. Either wrap it as `BEGIN; DELETE FROM fct_payments WHERE payment_date = :d; INSERT ...; COMMIT;`, or use a MERGE on `payment_id`, so running it once or five times gives the same result. I'd add a uniqueness test on `payment_id` that fails the pipeline before dashboards refresh, plus a daily reconciliation check. The rerun then becomes the standard recovery action in the runbook, with no manual cleanup."

<p class="qb-label">Architecture and flow</p>

```text
Before: INSERT ... WHERE payment_date = :d     (rerun ⇒ duplicates)

After (any of):
BEGIN; DELETE target WHERE payment_date = :d; INSERT ... ; COMMIT;
MERGE target USING staging ON payment_id ...
INSERT OVERWRITE / replaceWhere payment_date = :d
+ unique test on payment_id + daily reconciliation vs provider
```

<p class="qb-label">Alternatives and trade-offs</p>

| Pattern | Pros | Cons |
|---|---|---|
| Delete+insert by partition | Simple, deterministic | Must cover full partition scope |
| MERGE on key | Handles late updates | Needs reliable unique key |
| Partition overwrite | Atomic in table formats | Partition-aligned loads only |
| Dedupe downstream | Quick patch | Hides root cause, costs on every read |

<p class="qb-label">Follow-up questions</p>

- What makes a pipeline idempotent? — Same inputs/params produce the same final state regardless of how many times it runs (see [What changed in Airflow 3 (Assets, DAG versioning, Task SDK)?](/career/data-engineering-interview-questions/#86-what-changed-in-airflow-3-assets-dag-versioning-task-sdk)).
- What if the job writes to an external system (API)? — Idempotency keys or upserts on the receiving side.
- How do you prevent manual reruns from causing harm? — Idempotent design plus orchestrator-managed reruns with parameters.

</details>

### 10. PII found in the raw zone and application logs

<p class="qb-meta">Beginner · Common</p>

**Scenario:** A security scan finds full credit card numbers and customer emails in (a) raw JSON files in the data lake (2 years, readable by 120 engineers) and (b) application and pipeline logs shipped to a log platform with 90-day retention. Compliance asks for remediation within 2 weeks.

<details class="qb-more">
<summary>Sample answer and approach</summary>

<p class="qb-label">What the interviewer is testing</p>

- Incident-style containment for sensitive data exposure.
- Remediation: deletion or redaction, access restriction, rotation if needed.
- Prevention: classification, masking at ingestion, log hygiene.

<p class="qb-label">Step-by-step approach</p>

1. **Contain immediately:** restrict access to the affected lake prefixes and log indices to a small incident group. Notify security and compliance. Card data may trigger PCI DSS obligations.
2. **Scope:** use classification scans (pattern detection for PANs with a Luhn check, and emails) to find every affected location: files, tables, derived datasets, logs, backups.
3. **Remediate the lake:** rewrite the affected files with the card numbers removed or tokenised (keep only the last 4 digits if needed). Delete the originals, including object versions, then purge table versions (VACUUM or time travel limits).
4. **Remediate the logs:** delete or redact the affected log entries if the platform allows it. Otherwise reduce retention or purge the indices.
5. **Fix the sources:**
   - The application should never send full PANs. Tokenise with the payment provider.
   - Log redaction filters in the logging libraries.
   - Schema rules rejecting PAN-like values.
6. **Ongoing:** automated classification scans, tag-based masking policies, least-privilege access, and PII-free logging standards.

<p class="qb-label">Sample strong answer</p>

"I'd treat it as a security incident. Within the hour, lock down the affected lake prefixes and log indices to the incident team, and inform security and compliance, because card data has regulatory implications. Then scope it with a pattern scan: 16-digit sequences that pass Luhn, and email regexes, across the lake, tables, derived datasets and log indices. For the lake, a remediation job rewrites the affected JSON files, replacing PANs with tokens or masked values, deletes the old object versions, and vacuums any table versions. For logs, we purge the affected indices or use the platform's delete API. The real fix is upstream: the checkout service should send the payment provider's token, never the card number, and we'd add a logging filter that redacts PAN and email patterns before logs leave the service. Going forward, I'd run weekly automated classification scans, apply masking policies by tag, and add a CI check that blocks PAN-like test data."

<p class="qb-label">Architecture and flow</p>

```text
Detect (scan) ─▶ contain (restrict access) ─▶ scope (classification across lake, tables, logs, backups)
        ▼
Remediate: rewrite/tokenise files → delete versions → VACUUM ; purge/redact log indices
        ▼
Prevent: tokenisation at source, log redaction filters, ingestion validation (reject PAN patterns),
         classification + tag-based masking, least privilege, periodic scans
```

<p class="qb-label">Alternatives and trade-offs</p>

| Option | Pros | Cons |
|---|---|---|
| Delete affected data entirely | Clean | May lose needed non-PII data |
| Rewrite with tokens/masks | Keeps usable data | Rewrite effort |
| Restrict access only | Fast | Data still present; compliance gap |
| Encryption at rest | Baseline | Doesn't stop authorised readers seeing PII |

<p class="qb-label">Follow-up questions</p>

- What is tokenisation vs masking? — Tokenisation replaces data with a reversible (via vault) or non-reversible token; masking hides data at query time.
- How do you prevent PII in logs? — Structured logging with allow-listed fields, redaction filters, reviews (see [Logging in pipelines](/python/python-interview-questions/#27-logging-in-pipelines)).
- Do you need to notify customers? — Depends on regulations and exposure assessment; legal/compliance decide.

</details>

### 11. Object storage costs grow 20% a month

<p class="qb-meta">Beginner · Common</p>

**Scenario:** The data lake bucket grew from 400 TB to 830 TB in four months, and the S3 bill grows about 20% a month. Nobody knows what's taking the space. The lake holds raw landing files, Delta tables, Spark checkpoints, temporary job outputs, and old exports.

<details class="qb-more">
<summary>Sample answer and approach</summary>

<p class="qb-label">What the interviewer is testing</p>

- Storage analysis: inventory, prefixes, versions.
- Lifecycle policies, table maintenance, and the storage classes.
- Retention policy decisions with stakeholders.

<p class="qb-label">Step-by-step approach</p>

1. **Measure:**
   - S3 Storage Lens or Inventory reports grouped by prefix, storage class and age.
   - Count non-current object versions (versioning can silently double storage).
2. **Typical culprits:**
   - Delta or Iceberg tables never vacuumed, or with snapshots never expired.
   - Versioned buckets without non-current version expiration.
   - Temp and scratch outputs never cleaned up.
   - Duplicate raw copies.
   - Uncompressed CSV or JSON in the raw zone.
   - Old checkpoints.
   - Incomplete multipart uploads.
3. **Actions:**
   - VACUUM and expire snapshots on the table formats.
   - Lifecycle rules: transition raw data to Infrequent Access or Glacier after 30–90 days, expire temp prefixes after 7 days, expire non-current versions after N days, abort incomplete multipart uploads.
   - Compress or convert raw data to Parquet when it's reprocessable.
   - Delete unused datasets after confirming with their owners.
4. **Govern:** tag buckets and prefixes by owner, set retention policies per data class, and report monthly.

<p class="qb-label">Sample strong answer</p>

"I'd pull an S3 Inventory report and group it by top-level prefix and object age. That usually explains it in an hour. Here it shows 280 TB of non-current versions (versioning was on with no expiration rule), 150 TB of Delta tables never vacuumed after daily MERGEs, 60 TB of `tmp/` outputs from failed jobs, and 90 TB of 3-year-old raw JSON nobody reads. Fixes: a lifecycle rule expiring non-current versions after 14 days, and aborting incomplete multipart uploads after 3 days. Weekly VACUUM, or predictive optimisation, on Delta tables with 7-day retention. A 7-day expiry on `tmp/`. Transitioning raw data older than 90 days to Glacier Instant Retrieval, with expiry after the agreed retention (say 2 years, confirmed with compliance). That should cut storage by about half, and growth becomes predictable. I'd also require owner tags per prefix and send a monthly storage report per team."

<p class="qb-label">Architecture and flow</p>

```text
S3 Inventory / Storage Lens ─▶ breakdown by prefix, class, age, versions
        ├─ non-current versions → lifecycle: expire after 14d
        ├─ Delta/Iceberg old files → VACUUM / expire snapshots
        ├─ tmp/ scratch → expire after 7d
        ├─ raw > 90d → Glacier IR, expire per retention policy
        └─ incomplete multipart → abort after 3d
Governance: owner tags, retention policy per data class, monthly storage report
```

<p class="qb-label">Alternatives and trade-offs</p>

| Action | Savings | Risk |
|---|---|---|
| Expire non-current versions | Large if versioning on | Lose accidental-delete protection beyond window |
| VACUUM tables | Large for MERGE-heavy tables | Limits time travel |
| Archive tiers | 50–80% cheaper storage | Retrieval latency/cost |
| Delete unused datasets | Big | Must confirm ownership/legal holds |

<p class="qb-label">Follow-up questions</p>

- Why does versioning increase cost silently? — Overwrites/deletes keep previous versions billed.
- How do you avoid deleting needed data? — Ownership tags, access logs (last accessed), staged deletion with notice.
- Does Snowflake have similar issues? — Time Travel/Fail-safe and clones add storage; review retention and transient tables (see [Dynamic Tables vs Streams & Tasks vs materialized views](/snowflake/snowflake-interview-questions/#41-dynamic-tables-vs-streams--tasks-vs-materialized-views)).

</details>

### 12. The business wants hourly instead of daily refresh

<p class="qb-meta">Beginner · Common</p>

**Scenario:** The sales operations team wants their pipeline dashboard refreshed hourly instead of daily. The current daily pipeline takes 90 minutes, mostly full rebuilds of 120 dbt models, on a Medium Snowflake warehouse. Sources are Salesforce (synced by Fivetran every 6 hours) and an internal Postgres database.

<details class="qb-more">
<summary>Sample answer and approach</summary>

<p class="qb-label">What the interviewer is testing</p>

- Checking that upstream freshness supports the request.
- Converting to incremental processing and selective scheduling.
- Cost estimation and SLA definition.

<p class="qb-label">Step-by-step approach</p>

1. **Clarify:** which metrics need hourly refresh (probably a few dashboard tiles, not all 120 models)? Working hours only?
2. **Upstream freshness:** Fivetran syncs Salesforce every 6 hours, so increase the sync frequency (cost implications) or the "hourly" request is meaningless for Salesforce data.
3. **Scope:** select the models that feed the dashboard (`+exposure:sales_pipeline_dashboard`), perhaps 25 models.
4. **Make them incremental** where large, so each run processes an hour of changes.
5. **Schedule:** hourly during business hours for that selection, with the full nightly build unchanged.
6. **Estimate the cost:** hourly runs at about 5 minutes on a Small warehouse vs the current daily cost, plus the Fivetran sync cost increase.
7. **SLA:** for example, data at most 75 minutes old during 08:00–20:00.

<p class="qb-label">Sample strong answer</p>

"First I'd check feasibility: Salesforce only syncs every 6 hours, so we'd raise the Fivetran sync to hourly for the 5 objects the dashboard uses, which is a modest cost increase. Then I'd scope it. The dashboard exposure depends on 22 of the 120 models, so only those need hourly runs. Three of them are large full rebuilds, and I'd make them incremental with a 2-hour lookback and merge on IDs. An hourly job from 08:00 to 20:00 runs `dbt build --select +exposure:sales_pipeline_dashboard` on a Small warehouse, about 4–6 minutes per run, while the nightly full build stays as is. Cost is roughly 12 short runs a day, a few extra credits per day plus the connector cost, which I'd present to the business with the SLA: 'dashboard data no older than about 75 minutes during business hours'. Then I'd add freshness monitoring on the dashboard sources."

<p class="qb-label">Architecture and flow</p>

```text
Fivetran Salesforce sync: 6h → 1h (selected objects)
Postgres CDC/incremental ─────────────────────────────┐
                                                      ▼
Hourly job 08:00–20:00: dbt build --select +exposure:sales_pipeline_dashboard (22 models, incremental)
Nightly job: full dbt build (all 120 models)
SLA: ≤75 min data age in business hours; freshness alerts
```

<p class="qb-label">Alternatives and trade-offs</p>

| Option | Pros | Cons |
|---|---|---|
| Run whole project hourly | Simple | Expensive, slow |
| Subset + incremental | Targeted, cheap | More scheduling configs |
| Dynamic tables with target lag | Declarative freshness | Platform-specific; cost per refresh |
| Keep daily, add intraday "flash" metrics | Minimal change | Two definitions risk |

<p class="qb-label">Follow-up questions</p>

- What if they then ask for real-time? — Revisit with sources' latency limits and costs (see [Should this request be batch or streaming?](#7-should-this-request-be-batch-or-streaming)).
- How do you avoid hourly runs overlapping? — Orchestrator concurrency = 1 and runtime well below interval.
- How do you communicate freshness? — "Data as of" timestamp on the dashboard.

</details>

### 13. Set up testing and CI for a brand-new pipeline

<p class="qb-meta">Beginner · Common</p>

**Scenario:** You're building a new pipeline: a Python extractor from a partner API, landing in S3, loaded into Snowflake, and transformed with dbt (about 30 models). The team has no existing CI. You need to set up testing and deployment practices before the first production release in 6 weeks.

<details class="qb-more">
<summary>Sample answer and approach</summary>

<p class="qb-label">What the interviewer is testing</p>

- The layers of testing for data pipelines (unit, integration, data tests, contract tests).
- A practical CI/CD workflow with environments.
- Balancing rigour against time.

<p class="qb-label">Step-by-step approach</p>

1. **Repository and branching:** one repo with `extractor/`, `dbt/` and infra, using trunk-based development with PRs.
2. **Python extractor:**
   - Linting and formatting (ruff), type checks (mypy).
   - Unit tests with pytest for parsing, pagination and retry logic, using mocked HTTP (see [pytest fixtures and parametrize](/python/python-interview-questions/#28-pytest-fixtures-and-parametrize), [Mocking external services in tests](/python/python-interview-questions/#48-mocking-external-services-in-tests)).
   - A contract test validating sample API responses against a schema (Pydantic models).
3. **dbt:**
   - SQL linting (SQLFluff).
   - Unit tests for complex logic, and data tests (PK unique and not_null, relationships, accepted values).
   - Source freshness.
   - Slim CI with deferral to prod (see [Slim CI with state:modified and `--defer`](/etl-elt/dbt-interview-questions/#44-slim-ci-with-statemodified-and---defer)).
4. **Environments:** dev (personal schemas), CI (per-PR schema), and prod (service accounts only).
5. **CI pipeline (on PR):** lint, unit tests, `dbt build --select state:modified+` in the CI schema, and test results posted to the PR.
6. **CD (on merge):** build and push the extractor image, then `dbt build` in prod through the orchestrator. Infra changes go through Terraform plan and apply with approval.
7. **Post-deploy monitoring:** freshness, row counts, test failures, and alerts to the team channel.

<p class="qb-label">Sample strong answer</p>

"In 6 weeks I'd set up a minimal but solid foundation. In week one, a repo with a GitHub Actions workflow: ruff and mypy on the Python extractor, pytest unit tests with `responses` mocks covering pagination, 429 retries and malformed payloads, and Pydantic models acting as the contract for API responses. For dbt: SQLFluff, PK and relationship tests on every model, unit tests on the two models with tricky business logic, and source freshness on the landing tables. CI runs on every PR: lint, then unit tests, then `dbt build --select state:modified+ --defer` into a per-PR schema in Snowflake, with results posted on the PR. On merge, CD builds the extractor container and the orchestrator picks up the new dbt code for the next prod run. Prod runs use a service account with key-pair auth, and developers can't write to prod. After launch, freshness and test failures alert our channel. Later I'd add data diffing against prod for refactors."

<p class="qb-label">Architecture and flow</p>

```text
PR opened ─▶ CI:
   ├─ extractor: ruff, mypy, pytest (mocked API), contract tests (Pydantic)
   └─ dbt: sqlfluff, dbt build state:modified+ --defer (CI schema), unit + data tests
          ▼ results on PR, required checks
merge to main ─▶ CD: build/push extractor image, deploy dbt code; Terraform plan/apply (approval)
prod runs (service account) ─▶ monitoring: freshness, tests, row counts → alerts
```

<p class="qb-label">Alternatives and trade-offs</p>

| Practice | Value | Cost |
|---|---|---|
| Unit tests for extractor | Catches logic bugs fast | Mocks maintenance |
| dbt data tests | Catches data issues | Warehouse compute |
| Slim CI | Fast feedback | Needs prod manifest management |
| Full integration env | Highest confidence | Expensive to maintain |
| Data diff on PRs | Detects unintended changes | Tooling setup |

<p class="qb-label">Follow-up questions</p>

- What tests do you write first? — PK tests on models and unit tests on parsing/retry logic — highest value per effort.
- How do you test against real API changes? — Scheduled contract checks against the partner sandbox.
- How do you manage secrets in CI? — CI secrets store or OIDC to cloud secrets manager; never in repo (see [Configuration and secrets via environment variables](/python/python-interview-questions/#10-configuration-and-secrets-via-environment-variables)).

</details>

### 14. Upstream API was down for two days; how do you recover?

<p class="qb-meta">Beginner · Common</p>

**Scenario:** A partner's API was unavailable from Friday 18:00 to Sunday 20:00. Your hourly ingestion job failed 50 times, and the dashboards show a gap for the weekend. The API is back, with normal rate limits (10 requests per second). The weekend volume is roughly 3× a normal day because of a promotion.

<details class="qb-more">
<summary>Sample answer and approach</summary>

<p class="qb-label">What the interviewer is testing</p>

- Recovery planning: backfill within rate limits, prioritisation, idempotency.
- Communication with stakeholders.
- Improvements in resilience: alert noise, automatic catch-up.

<p class="qb-label">Step-by-step approach</p>

1. **Confirm the API is healthy** and that it can serve historical windows (`updated_since` back to Friday).
2. **Stop the alert storm:** pause the hourly job, or make it skip while a backfill runs, so the two don't compete.
3. **Plan the backfill:**
   - Window: Friday 17:00 (with overlap) to now.
   - Estimate requests: about 3× a normal day for 2 days, which is roughly 6 days of normal volume. Check the time this takes at 10 requests per second, and the daily quota.
   - Run it in chunks (by hour) with checkpoints. Use idempotent loads (MERGE on IDs), so overlap is harmless.
4. **Prioritise:** load the data critical dashboards need first, if the API supports filtering, for example orders before marketing events.
5. **Downstream:** rebuild the affected partitions and aggregates for the weekend dates.
6. **Communicate:** the gap, the expected time to full recovery, and which numbers are provisional.
7. **Improve:**
   - Automatic catch-up logic (the watermark-based job naturally catches up after outages, if it's built incrementally).
   - Alert deduplication (one incident, not 50 alerts).
   - An upstream status integration.

<p class="qb-label">Sample strong answer</p>

"Because our extractor uses a watermark (`updated_since` = the last successful run minus overlap), the next successful run would try to fetch the whole weekend at once. That's probably too big for one run, so I'd pause the hourly schedule and run a dedicated backfill. I'd split Friday 17:00 to now into hourly windows, fetch each with pagination and our 9-requests-per-second limiter, land the raw pages, and MERGE on IDs, with a checkpoint per window. Weekend volume is about 6 normal days' worth, so at 9 requests per second that's roughly 3 hours, within the daily quota. Then I'd rebuild Friday-to-Sunday partitions downstream, re-enable the hourly job, and confirm counts against the partner's own reports. Stakeholders get a note in the morning: weekend data restored by 11:00, numbers final after reconciliation. Afterwards, I'd make the job auto-chunk large catch-up windows, and group repeated failures into a single incident alert."

<p class="qb-label">Architecture and flow</p>

```text
Outage: Fri 18:00 → Sun 20:00  (50 failed hourly runs → 1 incident)
        ▼
Pause hourly job ─▶ backfill: hourly windows (Fri 17:00 → now), rate-limited, checkpointed
        ▼
raw landing ─▶ MERGE on id (idempotent) ─▶ rebuild weekend partitions/aggregates
        ▼
reconcile with partner totals ─▶ resume hourly job ─▶ comms: restored + final status
Improvements: auto-chunked catch-up, alert dedup, upstream status checks
```

<p class="qb-label">Alternatives and trade-offs</p>

| Option | Pros | Cons |
|---|---|---|
| Let the normal job catch up | No extra work | Huge single run; rate-limit/timeouts |
| Chunked backfill | Controlled, resumable | Needs orchestration |
| Ask partner for bulk export | Fast | Depends on partner |
| Accept the gap | No effort | Wrong numbers; unacceptable for revenue data |

<p class="qb-label">Follow-up questions</p>

- What if the API only returns the last 24 hours? — Need partner export or accept/flag data loss; push for longer history.
- How do you avoid duplicate alerts? — Alert on state changes, group by job, suppress repeats.
- How do you prioritise downstream rebuilds? — Critical exposures first (see [Recurring SLA breaches on a critical pipeline](#22-recurring-sla-breaches-on-a-critical-pipeline)).

</details>

## Intermediate scenario-based questions

### 15. The nightly pipeline failed at 3 AM; executive dashboards refresh at 7 AM

<p class="qb-meta">Intermediate · Very Common</p>

**Scenario:** You're on call. At 03:05 you're paged: the nightly `finance_daily` Airflow DAG (ingest 40 tables from Postgres and Salesforce → dbt build of ~300 models on Snowflake, normally 01:00–04:30) failed in the `stg_salesforce__opportunities` model. The CFO's revenue dashboard refreshes at 07:00 and is used in an 08:00 leadership meeting.

<details class="qb-more">
<summary>Sample answer and approach</summary>

<p class="qb-label">What the interviewer is testing</p>

- Calm, structured incident handling: triage, communicate, then fix.
- Ability to protect the SLA (partial recovery, prioritisation) rather than just "fix the bug".
- Knowledge of retries, repair runs and idempotent reruns.
- Follow-through: root cause, prevention and the post-mortem.

<p class="qb-label">Step-by-step approach</p>

1. **Acknowledge and assess the impact (5 min).** Which downstream models and dashboards depend on the failed node (lineage, exposures)? Is yesterday's data still being served, or is anything now broken or partial?
2. **Communicate early.** Post in the incident channel: what failed, the impact, the next update time, and whether the 07:00 refresh is at risk.
3. **Diagnose.** Read the task log and the compiled SQL. Classify the error as transient (warehouse timeout, lock, network) or deterministic (schema change, bad data, expired credential).
4. **Recover.** If it's transient, retry only the failed node and its descendants (Airflow "clear downstream", `dbt retry`, or `dbt build --select stg_salesforce__opportunities+`). If it's deterministic, apply the smallest safe fix: a hotfix to the model through an expedited PR, quarantining bad rows, or temporarily excluding a non-critical column.
5. **Protect the SLA.** If the full fix will take longer than 07:00, build the critical path first (`+exposure:cfo_revenue`), and let non-critical marts finish later. If correct data is impossible by 07:00, keep yesterday's data with a visible "data as of" banner rather than publishing wrong numbers.
6. **Verify.** Row counts, key tests, and a spot-check of revenue against the source.
7. **Follow up.** Write a blameless post-mortem with the timeline, root cause, and actions such as a contract test or alert.

<p class="qb-label">Sample strong answer</p>

"First I'd acknowledge the page and check blast radius: the lineage shows the failed staging model feeds `fct_revenue` and the CFO dashboard exposure, and because we use `dbt build`, the downstream models were skipped, so yesterday's numbers are still intact. I'd post a short update in the incident channel within 10 minutes with the next update time. The log shows `invalid identifier 'AMOUNT__C'`: Salesforce renamed a field overnight. That's deterministic, so retrying won't help. I'd map the renamed column in the staging model, push a reviewed hotfix, and run `dbt build --select stg_salesforce__opportunities+ --target prod`, prioritising `+exposure:cfo_revenue`. That chain takes about 40 minutes, so we'd finish around 04:30, well before 07:00. I'd then reconcile total pipeline value against Salesforce's report. Afterwards I'd add a source schema check that alerts on missing columns before the build, and propose a change-notification agreement with the Salesforce admin team."

<p class="qb-label">Architecture and flow</p>

```text
03:05 page ─▶ triage (lineage, impact) ─▶ comms (#incident, ETA)
             │
             ├─ transient? ──yes──▶ retry failed node + descendants
             └─ deterministic ─────▶ minimal hotfix (PR) ─▶ rebuild critical path first
                                                      │
             verify (tests, reconciliation) ◀─────────┘
             │
             07:00 dashboard refresh ─▶ post-mortem + prevention (schema checks, alerts)
```

<p class="qb-label">Alternatives and trade-offs</p>

| Option | When | Trade-off |
|---|---|---|
| Full rerun of DAG | Small DAGs | Wastes time/credits; may miss SLA |
| Repair/retry failed subtree | Most cases | Requires idempotent tasks |
| Serve stale data with banner | Fix impossible in time | Transparent but delays decisions |
| Hotfix in prod directly | Never without review | Speed vs risk; use expedited PR |

<p class="qb-label">Follow-up questions</p>

- What if the fix needs an upstream team who is asleep? — Escalate per runbook, publish stale-but-correct data, and fix when they're available.
- How would you prevent this class of failure? — Source contract/schema checks, freshness gates, and alerts before transformations (see [What are data contracts, and how do you enforce them?](/career/data-engineering-interview-questions/#83-what-are-data-contracts-and-how-do-you-enforce-them), [Source freshness](/etl-elt/dbt-interview-questions/#21-source-freshness)).
- How do you make reruns safe? — Idempotent models and partition replacement (see [What changed in Airflow 3 (Assets, DAG versioning, Task SDK)?](/career/data-engineering-interview-questions/#86-what-changed-in-airflow-3-assets-dag-versioning-task-sdk)).

</details>

### 16. Late-arriving events are making daily aggregates wrong

<p class="qb-meta">Intermediate · Very Common</p>

**Scenario:** A mobile app sends events to Kafka, which land in a lakehouse table (~300 M events/day). A daily job computes `daily_active_users` and `revenue_by_day` at 02:00 UTC for the previous day. About 3% of events arrive 1–72 hours late (phones offline), so yesterday's numbers keep changing in the source, but the aggregates never update. Finance reports a 2.5% mismatch with the payments system.

<details class="qb-more">
<summary>Sample answer and approach</summary>

<p class="qb-label">What the interviewer is testing</p>

- Event time vs processing time, and why both matter.
- Lookback, reprocessing and correction strategies.
- How to communicate "provisional vs final" numbers.
- Distinct-count pitfalls when recomputing aggregates.

<p class="qb-label">Step-by-step approach</p>

1. **Measure lateness.** Plot the distribution of `ingested_at - event_ts`. If p99 is 48 h and p99.9 is 70 h, that sets the window you need to cover.
2. **Keep both timestamps** on every event (event time and ingestion time).
3. **Incremental selection by ingestion time:** each run finds the event dates touched by newly ingested rows.
4. **Recompute the affected days fully.** Don't add deltas to aggregates like distinct users. Use partition replacement (`replaceWhere`, `insert_overwrite`, `delete+insert`) for those dates.
5. **Bound the work** with a lookback of, say, 3 days in daily runs, plus a weekly 14-day correction run.
6. **Communicate finality.** Label days within the window as provisional in dashboards, and mark them final after 72 h.
7. **Monitor** the events arriving beyond the window, and alert if they're non-zero.

<p class="qb-label">Sample strong answer</p>

"I'd first quantify lateness from `ingested_at - event_ts`. Say 99.9% arrive within 72 hours. Then I'd change the job from 'process yesterday' to 'process the event dates touched by anything ingested since the last run, limited to the last 4 days'. For each affected date I recompute the whole day from the event table, which is essential for distinct counts like DAU, and atomically replace that date's partition. So a late event from 3 days ago correctly updates that day. Revenue gets the same treatment, and reconciliation with payments then matches within the window. I'd add a `is_final` flag (event_date < today − 3) so finance knows which days are settled, and a monitor counting events arriving more than 72 hours late. A weekly job reprocesses 14 days as a safety net. If this were streaming, I'd use watermarks of about 72 hours for the stateful aggregation, or keep streaming for low-latency provisional numbers and batch for the final correction."

<p class="qb-label">Architecture and flow</p>

```text
Kafka ─▶ bronze_events (event_ts, ingested_at)
             │  new rows since last run (by ingested_at)
             ▼
   affected_dates = DISTINCT event_date (≤ 4 days back)
             │
             ▼
   recompute full day(s) from bronze ─▶ REPLACE partitions in gold_daily_metrics
             │                                  │
   monitor: events later than 72h          dashboards: is_final flag
```

<p class="qb-label">Alternatives and trade-offs</p>

| Approach | Pros | Cons |
|---|---|---|
| Fixed "yesterday only" | Simple | Wrong with late data |
| Lookback + full-day recompute | Correct, bounded cost | Recomputes a few days daily |
| Additive delta updates | Cheap | Breaks distinct counts and ratios |
| Streaming with watermark | Low latency | State cost; very late data dropped |
| Lambda (stream provisional + batch final) | Fast + correct | Two code paths unless unified engine |

<p class="qb-label">Follow-up questions</p>

- How do you handle a device that uploads 30 days late? — Count it in monitoring; the weekly/monthly correction or targeted backfill picks it up.
- Why not just merge late events into the aggregate? — Distinct counts aren't additive; you'd double-count users.
- How does dbt help? — Incremental models with lookback or microbatch `lookback` (see [The microbatch incremental strategy](/etl-elt/dbt-interview-questions/#39-the-microbatch-incremental-strategy), [Late-arriving data in incremental models (lookback windows)](/etl-elt/dbt-interview-questions/#47-late-arriving-data-in-incremental-models-lookback-windows)).

</details>

### 17. Duplicates discovered in a production fact table

<p class="qb-meta">Intermediate · Very Common</p>

**Scenario:** An analyst finds that `fct_orders` (1.2 B rows, Snowflake, incremental dbt model) has about 180,000 duplicated `order_id`s from the last 10 days. Revenue dashboards are overstated by about 0.8%. The model uses an `append` strategy, and the upstream Fivetran connector had a resync on day 1 of that window.

<details class="qb-more">
<summary>Sample answer and approach</summary>

<p class="qb-label">What the interviewer is testing</p>

- Root-cause thinking: why duplicates appeared, and why tests didn't catch them.
- Safe remediation on a large production table.
- Prevention through idempotent load design and tests.
- Stakeholder communication about restated numbers.

<p class="qb-label">Step-by-step approach</p>

1. **Quantify:** how many keys, which dates, and whether the duplicates are exact copies or differ in some columns.
2. **Find the cause.** Here, an append strategy plus a connector resync re-sent rows. Check for missing `unique_key` and missing PK tests.
3. **Contain:** pause the job or fix the strategy so it doesn't get worse. Tell the dashboard owners.
4. **Remediate:**
   - Option A: switch to merge with `unique_key='order_id'` and do a targeted rebuild of the 10 affected days (delete+insert by date).
   - Option B: a one-off dedupe of the affected window with `QUALIFY ROW_NUMBER()`, keeping the latest `_fivetran_synced`, in a transaction.
   - Take a zero-copy clone first, for rollback.
5. **Verify:** a unique test passes, and revenue reconciles to the source.
6. **Prevent:** `unique` and `not_null` tests on the PK (with `dbt build`, so they block downstream), merge or delete+insert strategy, and a source-level duplicate monitor.
7. **Communicate:** share the restatement notice with the corrected figures and the time window.

<p class="qb-label">Sample strong answer</p>

"I'd confirm the scope first: 180k duplicated order IDs, all ingested after the connector resync on the 3rd, as exact copies with different `_fivetran_synced` values. The model appended every incoming row and had no PK test, so nothing caught it. I'd clone `fct_orders` as a backup, which is instant in Snowflake, then fix the model to `incremental_strategy='merge'` with `unique_key='order_id'` and a dedupe in the source CTE keeping the latest sync. For remediation I'd run a delete+insert of the 10 affected order dates from the cleaned staging model inside one transaction, and verify zero duplicates and revenue matching the source system per day. Then I'd add `unique`/`not_null` tests on `order_id`. Since we run `dbt build`, a failure blocks the downstream marts. I'd also send finance a short note explaining the 0.8% restatement for those dates, with before and after numbers."

<p class="qb-label">Architecture and flow</p>

```text
Detect (analyst / test) ─▶ quantify (keys, dates, cause)
        │
        ├─ clone fct_orders (rollback point)
        ├─ fix model: append → merge(unique_key) + source dedupe
        ├─ rebuild affected dates (delete+insert, 1 txn)
        ├─ verify: unique test, revenue reconciliation
        └─ prevent: PK tests in dbt build, duplicate monitor, restatement comms
```

<p class="qb-label">Alternatives and trade-offs</p>

| Option | Pros | Cons |
|---|---|---|
| Full refresh of model | Simple, clean | Hours of compute; source retention risk |
| Targeted rebuild of affected dates | Fast, scoped | Need accurate date scope |
| In-place DELETE of duplicates | Quick | Must pick "right" row; careful with ties |
| Dedupe in a downstream view | No table change | Hides problem; costs every query |

<p class="qb-label">Follow-up questions</p>

- What if duplicates differ in amount? — Determine the authoritative version (latest sync or source system of record) and document the rule.
- How would you detect this earlier? — PK tests, row-count anomaly monitors, and source-to-target reconciliation.
- Why did append fail here? — Append isn't idempotent under replays/resyncs (see [What changed in Airflow 3 (Assets, DAG versioning, Task SDK)?](/career/data-engineering-interview-questions/#86-what-changed-in-airflow-3-assets-dag-versioning-task-sdk), [Incremental strategies by adapter (append, merge, delete+insert, insert_overwrite, microbatch)](/etl-elt/dbt-interview-questions/#38-incremental-strategies-by-adapter-append-merge-deleteinsert-insert_overwrite-microbatch)).

</details>

### 18. A Snowflake dashboard query takes 3 minutes; target is 10 seconds

<p class="qb-meta">Intermediate · Very Common</p>

**Scenario:** A Tableau dashboard queries `fct_sales` (4 B rows, 1.2 TB) joined to three dimensions, filtered by the last 90 days and a region, grouped by week and product category. It runs on a Medium warehouse, takes about 3 minutes, and 60 sales managers open it every morning at 08:30, causing queuing.

<details class="qb-more">
<summary>Sample answer and approach</summary>

<p class="qb-label">What the interviewer is testing</p>

- Using the Query Profile to find the real bottleneck.
- Pruning, clustering, result caching and pre-aggregation trade-offs.
- Concurrency (multi-cluster) vs single-query speed (warehouse size).
- Cost awareness.

<p class="qb-label">Step-by-step approach</p>

1. **Query Profile:** partitions scanned vs total (pruning), the most expensive operators, spill, and join explosion.
2. **Pruning:** the date filter should prune heavily. If `sale_date` isn't well clustered (data loaded out of order), clustering depth is poor. Check `SYSTEM$CLUSTERING_INFORMATION`.
3. **Query shape:** filters applied in BI as functions on columns (`DATE_TRUNC` in WHERE) or casts can prevent pruning. Check the generated SQL.
4. **Pre-aggregate:** the dashboard only needs week × category × region. A dynamic table or materialized view at that grain shrinks 4 B rows to about 50 k.
5. **Concurrency:** the 08:30 spike needs a multi-cluster warehouse (min 1, max 4), not a bigger size.
6. **Caching:** the result cache helps identical queries. Tableau extracts or live connections affect this.
7. **Measure cost:** clustering credits plus dynamic table refresh vs warehouse savings.

<p class="qb-label">Sample strong answer</p>

"I'd start with the Query Profile. It shows the table scan reading 85% of micro-partitions even though we only want 90 days, so pruning isn't working. The SQL from Tableau filters on `DATE_TRUNC('week', sale_ts)`, and the table is loaded by late batch jobs, so date ordering is poor. First fix: filter on a raw `sale_date` column, and add clustering on `sale_date`. That alone took the query to about 25 seconds in a test. But the dashboard only needs about 50,000 aggregated rows, so the real fix is a dynamic table at week × category × region with a 1-hour target lag, which queries in about 1 second. For the 08:30 rush I'd make the BI warehouse multi-cluster (max 3) rather than upsizing, and route the dashboard to it. Cost-wise, the dynamic table refresh is a few credits a day, far less than 60 three-minute queries on a Medium warehouse every morning."

<p class="qb-label">Architecture and flow</p>

```text
Before: Tableau ─▶ fct_sales (4B rows, poor pruning) ─▶ 3 min, queuing at 08:30

After:
fct_sales (CLUSTER BY sale_date) ─▶ DT agg_sales_weekly (target_lag 1h, ~50k rows)
                                                  │
Tableau ─▶ BI warehouse (Small, multi-cluster 1–3) ─▶ ~1 s, result cache for repeats
```

<p class="qb-label">Alternatives and trade-offs</p>

| Option | Pros | Cons |
|---|---|---|
| Bigger warehouse | Fast to try | Costly; doesn't fix pruning |
| Clustering key | Better pruning for many queries | Reclustering credits |
| Dynamic table / MV aggregate | Sub-second dashboards | Freshness lag; maintenance |
| Search optimization | Point lookups | Not for range scans/aggregations |
| Tableau extract | Offloads warehouse | Extra copy, refresh scheduling |

<p class="qb-label">Follow-up questions</p>

- How do you know clustering helped? — Partitions scanned/total in profile; clustering depth trend.
- When would you choose QAS? — Large scans with outlier queries; it offloads portions to serverless compute (see [Replication and failover for business continuity](/snowflake/snowflake-interview-questions/#49-replication-and-failover-for-business-continuity)).
- How do you keep cost in check? — Resource monitors, auto-suspend, and measuring credits before/after.

</details>

### 19. Upstream added and renamed columns without notice (schema drift)

<p class="qb-meta">Intermediate · Very Common</p>

**Scenario:** A product team's microservice writes JSON events to S3, ingested hourly by Auto Loader into a Databricks bronze table, then transformed by dbt into silver and gold. Yesterday they renamed `price` to `unit_price` and added `discount_pct`. Bronze ingestion kept working, but silver now shows NULL prices for 18 hours of data and the revenue dashboard dropped by 40%.

<details class="qb-more">
<summary>Sample answer and approach</summary>

<p class="qb-label">What the interviewer is testing</p>

- Designing ingestion that survives drift without silently corrupting downstream data.
- Detection and alerting on contract changes.
- Recovery (backfill) and organisational fixes (contracts, ownership).

<p class="qb-label">Step-by-step approach</p>

1. **Contain:** stop or flag downstream publishing (or mark the dashboard degraded). Notify consumers.
2. **Short-term fix:** map both names in silver, `coalesce(unit_price, price) AS unit_price`, and add `discount_pct`. Then reprocess the 18 affected hours from bronze. Bronze kept the raw data, so it's replayable.
3. **Why it was silent:** bronze evolution added the new column, and silver selected `price`, which was now NULL. There was no test on the null rate.
4. **Detection:**
   - `not_null` tests, or null-rate anomaly checks on critical columns.
   - Auto Loader `_rescued_data` and schema-change notifications.
   - Schema diff alerts on bronze.
5. **Prevention:**
   - A data contract (schema registry or JSON Schema) with CI checks in the producer repo.
   - Deprecation windows: send both fields for N days.
   - Contract-enforced silver models (see [Model contracts](/etl-elt/dbt-interview-questions/#41-model-contracts)).
6. **Communicate,** and do a post-mortem with the producer team.

<p class="qb-label">Sample strong answer</p>

"I'd immediately mark the revenue dashboard as degraded and tell finance. Since bronze stores the raw events, nothing is lost. I'd update the silver model to `coalesce(unit_price, price)`, add `discount_pct`, and reprocess the last 24 hours of partitions with an idempotent `replaceWhere`. Revenue is back within an hour or two. The bigger problem is that this was silent. Bronze correctly evolved the schema, but nobody tested that `price` was populated. I'd add a not_null test with an error threshold on silver price, plus a null-rate anomaly monitor. Longer term, I'd agree a data contract with the product team: a JSON Schema in their repo, with CI that flags breaking changes, and a rule that renames ship both fields for two weeks with notice in our channel. On our side, contracted silver models fail loudly instead of producing NULLs."

<p class="qb-label">Architecture and flow</p>

```text
Producer service ──(contract: JSON Schema + CI check)──▶ S3 events
        │                                                 │
        └─ deprecation window (old+new fields)           Auto Loader (addNewColumns, _rescued_data)
                                                          ▼
                                                   bronze (raw, replayable)
                                                          ▼
                                   silver (explicit columns, coalesce mappings, contract)
                                      │  tests: not_null/null-rate, schema diff alerts
                                      ▼
                                   gold ─▶ dashboards
```

<p class="qb-label">Alternatives and trade-offs</p>

| Approach | Pros | Cons |
|---|---|---|
| Permissive evolution everywhere | Never breaks ingestion | Silent downstream errors |
| Fail on any schema change | Loud | Frequent outages for harmless additions |
| Evolve bronze, enforce silver contracts | Balanced | Needs tests and mapping work |
| Schema registry with compatibility rules | Prevents breaking changes at source | Requires producer adoption |

<p class="qb-label">Follow-up questions</p>

- How would Kafka + Avro change this? — Schema Registry compatibility modes (backward/forward) block incompatible producer changes.
- What's a non-breaking change? — Adding optional fields; renames and type changes are breaking.
- How do you find all impacted models? — Column-level lineage (see [What is data lineage, and how is it captured?](/career/data-engineering-interview-questions/#66-what-is-data-lineage-and-how-is-it-captured), [Lineage and system tables](/databricks/databricks-interview-questions/#24-lineage-and-system-tables)).

</details>

### 20. Snowflake cost doubled overnight

<p class="qb-meta">Intermediate · Very Common</p>

**Scenario:** Your Snowflake account normally consumes about 400 credits a day. Yesterday it used 820. Finance noticed this morning. There are 25 warehouses, 3,000 dbt models, Fivetran, BI tools, and a new data science team onboarded last week.

<details class="qb-more">
<summary>Sample answer and approach</summary>

<p class="qb-label">What the interviewer is testing</p>

- A fast, data-driven investigation with ACCOUNT_USAGE and ORGANIZATION_USAGE views.
- Distinguishing warehouse compute, serverless features and cloud services.
- Containment without breaking production.
- Long-term guardrails such as resource monitors, budgets and tags.

<p class="qb-label">Step-by-step approach</p>

1. **Break it down by service:** `METERING_DAILY_HISTORY` gives warehouse vs serverless (Snowpipe, auto-clustering, MVs, search optimisation, serverless tasks, AI functions) vs cloud services.
2. **If warehouses:** `WAREHOUSE_METERING_HISTORY` by warehouse and hour. Find the spike's warehouse and time.
3. **Drill into queries:** `QUERY_HISTORY` / `QUERY_ATTRIBUTION_HISTORY` for that warehouse and window, grouped by user, role, query tag and query hash. Look for the top credit consumers.
4. **Typical culprits:**
   - A warehouse resized to X-Large and never reverted.
   - Auto-suspend disabled.
   - A runaway query or loop (a BI refresh every minute).
   - A new data science notebook scanning huge tables.
   - A full refresh of big dbt models.
   - Reclustering after a big data change.
   - Serverless features enabled broadly.
5. **Contain:** suspend or resize the offending warehouse, cancel runaway queries, and talk to the owner.
6. **Prevent:**
   - Resource monitors per warehouse with suspend actions.
   - Budgets.
   - `STATEMENT_TIMEOUT_IN_SECONDS`.
   - Auto-suspend of 60 seconds.
   - Query tags for attribution.
   - A separate warehouse for data science.
   - Anomaly alerts.

<p class="qb-label">Sample strong answer</p>

"I'd query `METERING_DAILY_HISTORY` first. It shows the extra 420 credits are warehouse compute, not serverless. `WAREHOUSE_METERING_HISTORY` points at `DS_WH`, which went from about 10 credits a day to 380, starting at 14:00. `QUERY_HISTORY` for that warehouse shows a new data scientist ran a notebook that re-ran a cross-join-heavy feature query every 15 minutes, and someone resized the warehouse to 2X-Large to 'make it faster'. I'd contact the user, cancel the scheduled loop, resize back to Medium, and confirm auto-suspend is 60 seconds. To prevent a repeat, I'd put a resource monitor on `DS_WH` at 50 credits a day that notifies at 80% and suspends at 100%, set a 1-hour statement timeout, and remove MODIFY rights on warehouses from non-admin roles. A daily cost-anomaly alert on `WAREHOUSE_METERING_HISTORY` would also catch this hours earlier. Then I'd pair with the DS team to rewrite the feature query incrementally."

<p class="qb-label">Architecture and flow</p>

```text
Spike detected ─▶ METERING_DAILY_HISTORY (which service?)
                     ├─ warehouses ─▶ WAREHOUSE_METERING_HISTORY (which WH, when?)
                     │                    └─▶ QUERY_HISTORY / ATTRIBUTION (who, what, query_tag)
                     ├─ serverless ─▶ AUTOMATIC_CLUSTERING / PIPE / SEARCH_OPT / TASK histories
                     └─ cloud services ─▶ metadata-heavy patterns (SHOW, tiny queries)
Contain (suspend/resize/cancel) ─▶ Guardrails (resource monitors, budgets, timeouts, RBAC on WH changes, anomaly alerts)
```

<p class="qb-label">Alternatives and trade-offs</p>

| Guardrail | Pros | Cons |
|---|---|---|
| Resource monitor suspend | Hard stop | Can stop critical work mid-run |
| Notify-only monitors | No disruption | Relies on humans reacting |
| Statement timeouts | Stops runaways | Must exempt legitimate long jobs |
| Per-team warehouses + tags | Clear attribution | More warehouses to manage |
| Budgets (account/custom) | Forecasting and alerts | Not immediate enforcement |

<p class="qb-label">Follow-up questions</p>

- Why can cloud services costs appear? — Billed when they exceed 10% of daily warehouse usage (metadata-heavy workloads).
- How do you attribute costs to teams? — Query tags, per-team warehouses, object tags and `QUERY_ATTRIBUTION_HISTORY`.
- What if the spike was serverless reclustering? — Check `AUTOMATIC_CLUSTERING_HISTORY`; reconsider clustering keys or suspend reclustering temporarily.

</details>

### 21. Databricks bill spiked 60% this month

<p class="qb-meta">Intermediate · Common</p>

**Scenario:** The monthly Databricks bill rose from $120k to $192k. You have 8 workspaces, about 600 jobs, many interactive clusters, SQL warehouses, Lakeflow pipelines, and some model serving. Cloud VM costs, billed separately by AWS, also rose 45%.

<details class="qb-more">
<summary>Sample answer and approach</summary>

<p class="qb-label">What the interviewer is testing</p>

- Using system billing tables for attribution.
- Common waste patterns on Databricks.
- Balancing DBU and cloud infrastructure costs.
- Governance through policies, tags and budgets.

<p class="qb-label">Step-by-step approach</p>

1. **Attribute:** `system.billing.usage` joined with `list_prices`, grouped by SKU, workspace, job, warehouse, cluster and tags. Compare this month with last month.
2. **Find the movers:** the top increases by product (ALL_PURPOSE, JOBS, SQL, DLT/pipelines, MODEL_SERVING, serverless).
3. **Common waste patterns:**
   - All-purpose clusters running scheduled jobs.
   - Clusters without auto-termination.
   - Continuous pipelines that only need hourly freshness.
   - Oversized SQL warehouses with long auto-stop.
   - Jobs whose runtime regressed (skew, small files).
   - Model-serving endpoints that don't scale to zero.
4. **Cloud cost side:** VM hours (classic compute), storage growth (no VACUUM), and cross-region egress.
5. **Fix:**
   - Move jobs to job or serverless compute.
   - Policies (auto-termination, max workers, required tags).
   - Triggered pipelines.
   - Right-size warehouses.
   - VACUUM and predictive optimisation.
6. **Govern:** budgets with alerts, tag enforcement, and a monthly FinOps review per team.

<p class="qb-label">Sample strong answer</p>

"I'd run a month-over-month diff on `system.billing.usage` by `billing_origin_product` and `usage_metadata`. It shows three drivers. All-purpose compute up $35k, mostly two always-on clusters used to schedule 40 notebooks. A Lakeflow pipeline switched to continuous mode for a demo and never switched back, $18k. And a new model-serving endpoint with scale-to-zero disabled, $12k. The cloud VM increase lines up with the always-on clusters. Fixes: move the 40 notebooks to serverless jobs, revert the pipeline to triggered every 30 minutes, and enable scale-to-zero on the dev endpoint. That gets us back near $125k. Then guardrails: a compute policy requiring auto-termination of 60 minutes or less and team tags, budget alerts per workspace at 80% of forecast, and a weekly report of the top 20 cost movers to team leads."

<p class="qb-label">Architecture and flow</p>

```text
system.billing.usage ⨝ list_prices ─▶ cost by product/workspace/job/warehouse/tags (MoM diff)
        │
        ├─ ALL_PURPOSE ↑  → jobs on interactive clusters → serverless/job compute
        ├─ PIPELINES ↑    → continuous mode → triggered schedule
        ├─ MODEL_SERVING ↑→ no scale-to-zero → enable for non-prod
        └─ JOBS ↑         → runtime regressions → tune (skew/small files)
Cloud bill: VM hours (classic), storage (VACUUM), egress
Guardrails: compute policies, tags, budgets + alerts, weekly FinOps review
```

<p class="qb-label">Alternatives and trade-offs</p>

| Lever | Savings | Risk |
|---|---|---|
| Serverless jobs | Removes idle/startup waste | Higher per-DBU rate for long steady jobs |
| Spot instances | Large VM savings | Preemption for long stages |
| Photon | Faster → fewer hours | Higher DBU rate; benchmark |
| Triggered vs continuous | Big for sparse data | Higher latency |
| Aggressive auto-termination | Cuts idle | Slower dev restarts |

<p class="qb-label">Follow-up questions</p>

- How do you attribute serverless usage? — Budget policies/tags on serverless usage (see [Controlling DBU cost on Databricks](/databricks/databricks-interview-questions/#35-controlling-dbu-cost-on-databricks), [Monitoring spend with system.billing tables](/databricks/databricks-interview-questions/#45-monitoring-spend-with-systembilling-tables)).
- How do you stop recurrence? — Policies enforced at creation, anomaly alerts, and ownership via tags.
- What about storage costs? — Old Delta versions not vacuumed; set retention and enable predictive optimization (see [VACUUM: default retention and the risks](/databricks/databricks-interview-questions/#17-vacuum-default-retention-and-the-risks), [Predictive optimization](/databricks/databricks-interview-questions/#20-predictive-optimization)).

</details>

### 22. Recurring SLA breaches on a critical pipeline

<p class="qb-meta">Intermediate · Common</p>

**Scenario:** A daily risk-reporting pipeline must deliver by 06:00. It missed the SLA 9 times in the last 30 days, typically finishing between 06:20 and 07:10. It depends on 4 upstream sources (one external vendor file arriving between 00:30 and 04:00), runs 180 dbt models, and shares a Snowflake warehouse with ad-hoc analysts.

<details class="qb-more">
<summary>Sample answer and approach</summary>

<p class="qb-label">What the interviewer is testing</p>

- Root-cause analysis across upstream arrival, compute contention and DAG design.
- Critical-path thinking.
- SLO definition, monitoring and stakeholder management.
- Engineering changes vs agreements with upstreams.

<p class="qb-label">Step-by-step approach</p>

1. **Collect timing data for 30 days:** upstream arrival times, job start and end, per-model durations, warehouse queuing.
2. **Correlate the misses with causes.** For example, 6 of 9 misses had the vendor file arriving after 03:30, and 3 had warehouse queuing during analyst batch queries.
3. **Critical path:** which models determine the end time? Optimise those, and run the rest after the deadline.
4. **Fixes:**
   - **Event-driven start** on file arrival instead of a fixed schedule (sensors, file-arrival triggers).
   - **Dedicated warehouse** for the pipeline, so there's no analyst contention, or multi-cluster.
   - **Split the DAG:** the critical-path subset (`+exposure:risk_report`) runs first. Non-critical marts follow.
   - **Incremental models** for the heavy parts.
   - **Vendor SLA:** negotiate an earlier delivery, or a partial file plus a correction.
5. **Monitoring:** predicted completion time alerts ("at current pace you'll miss 06:00"), plus upstream arrival alerts.
6. **SLO and communication:** define the SLO (for example 06:00 on 95% of days), publish the status page, and give business users an early warning when a miss is likely.

<p class="qb-label">Sample strong answer</p>

"I'd pull 30 days of run metadata and upstream arrival times. In 6 of the 9 misses, the vendor file arrived after 03:30, and our DAG started at a fixed 02:00, waiting on a sensor, then ran everything serially after. In the other 3, the job queued for 25–40 minutes behind analyst queries on the shared warehouse. So: give the pipeline its own warehouse, which removes the queueing. Restructure the DAG so everything not dependent on the vendor file runs at 00:30. When the file lands, run only the vendor-dependent critical path to the risk report, about 35 minutes after tuning two slow models to incremental. That gives a worst-case finish around 04:45 even with a 04:00 arrival. I'd add an alert at 04:15 if the file hasn't arrived, so we can warn risk managers early, and raise a contractual SLA with the vendor for 03:00 delivery. We'd track SLO attainment weekly."

<p class="qb-label">Architecture and flow</p>

```text
00:30  internal sources ready ─▶ build non-vendor models (dedicated WH)
00:30–04:00 vendor file arrives ─▶ file-arrival trigger
                                   └─▶ critical path only: +exposure:risk_report (~35 min)
                                          └─▶ 06:00 SLA ✔
after deadline: non-critical marts
Alerts: vendor file not arrived by 04:15; predicted completion > 05:45
```

<p class="qb-label">Alternatives and trade-offs</p>

| Option | Pros | Cons |
|---|---|---|
| Bigger warehouse | Simple | Doesn't fix arrival delays |
| Dedicated warehouse | Removes contention | Cost of separate compute |
| Event-driven triggers | Starts as soon as possible | Must handle missing files |
| Partial publish (without vendor data) | Meets time | Report incomplete; needs clear labelling |
| Upstream SLA negotiation | Root fix | Depends on vendor |

<p class="qb-label">Follow-up questions</p>

- How do you predict a miss early? — Compare progress vs historical per-model durations; alert on projected end time.
- What's an SLO vs SLA? — SLO internal target; SLA external commitment with consequences (see [What are the dimensions of data quality?](/career/data-engineering-interview-questions/#15-what-are-the-dimensions-of-data-quality)).
- How do you prevent new models slowing the critical path? — CI checks on DAG changes to critical exposures; performance budgets.

</details>

### 23. The data lake has millions of tiny files

<p class="qb-meta">Intermediate · Common</p>

**Scenario:** An S3 data lake table `events` (Parquet, Hive-partitioned by `date/hour/country`) has 14 M files averaging 60 KB, for 800 GB in total. Spark queries spend minutes listing files, Athena queries time out, and S3 request costs rose to $3k/month. Ingestion writes every minute from a streaming job.

<details class="qb-more">
<summary>Sample answer and approach</summary>

<p class="qb-label">What the interviewer is testing</p>

- Root causes of small files: partitioning granularity, write frequency, parallelism.
- Compaction and layout fixes, ideally with a table format.
- Fixing the writer, not just cleaning up afterwards.

<p class="qb-label">Step-by-step approach</p>

1. **Root cause:**
   - 24 hours × 200 countries × 1,440 writes per day, with several tasks each, means huge fan-out.
   - The `hour` and `country` partitions are too fine for 800 GB.
2. **Fix the writer:**
   - A longer trigger (5–15 minutes).
   - Fewer output files per micro-batch (coalesce or repartition by partition column, optimized writes).
   - Coarser partitioning (date only).
3. **Adopt a table format** (Delta or Iceberg) for atomic compaction, metadata-based planning (no listing), and clustering instead of deep partitioning.
4. **Compact the history:** a one-off job rewriting each day into files of about 256 MB–1 GB, then ongoing scheduled compaction (OPTIMIZE, Iceberg `rewrite_data_files`).
5. **Clean up:** remove the old small files after a retention period (VACUUM, expire snapshots).
6. **Verify:** file count, average size, query planning time, S3 request costs.

<p class="qb-label">Sample strong answer</p>

"The layout is the problem. Partitioning by date, hour and country gives about 4,800 partitions a day, and a writer committing every minute puts several files into each. At 800 GB total we should have a few thousand files, not 14 million. I'd change the streaming job to a 5-minute trigger, repartition by date before writing, and drop hour and country as partition columns. Then I'd convert the table to Delta, partitioned by date only and clustered by country, or use Iceberg with day partitioning. A one-off compaction job rewrites each historical day into roughly 512 MB files, about 1,600 files in total. Going forward, auto-compaction plus nightly OPTIMIZE keeps files healthy, and VACUUM removes the replaced files after 7 days. Query planning drops from minutes to seconds, and the S3 GET and LIST costs fall by orders of magnitude."

<p class="qb-label">Architecture and flow</p>

```text
Before: stream (1-min commits) ─▶ s3://lake/events/date=/hour=/country=/  (14M × 60KB)

After:
stream (5-min trigger, repartition(date), optimized writes) ─▶ Delta/Iceberg events
     partitioned by date, clustered by country
         │ nightly OPTIMIZE / rewrite_data_files (target 512MB)
         │ VACUUM / expire snapshots
         ▼
  Spark/Athena: metadata-based planning, ~1–2k files
```

<p class="qb-label">Alternatives and trade-offs</p>

| Fix | Pros | Cons |
|---|---|---|
| Periodic compaction only | Quick | Problem recurs; compaction cost |
| Fix writer + coarser partitions | Root cause fixed | Requires pipeline change |
| Table format (Delta/Iceberg) | Atomic compaction, metadata pruning | Migration effort; reader compatibility |
| Longer trigger | Fewer files | Higher latency |

<p class="qb-label">Follow-up questions</p>

- Why do small files hurt? — Per-file open/list overhead, poor compression, more S3 requests, driver memory for planning (see [What is the small-files problem, and how do you fix it?](/career/data-engineering-interview-questions/#55-what-is-the-small-files-problem-and-how-do-you-fix-it)).
- How do you compact without disrupting readers? — Table formats commit atomically; readers see old or new snapshot.
- What's a good target file size? — Typically 128 MB–1 GB depending on engine and query patterns.

</details>

### 24. Kafka consumer lag keeps growing

<p class="qb-meta">Intermediate · Common</p>

**Scenario:** A consumer group processing `payments` (24 partitions) into a database falls further behind each day. Lag grew from about 0 to 40 M messages over a week. Producer throughput rose from 5k to 12k messages per second after a new market launch. The consumers do per-message enrichment API calls and single-row database inserts.

<details class="qb-more">
<summary>Sample answer and approach</summary>

<p class="qb-label">What the interviewer is testing</p>

- Diagnosing whether the bottleneck is consumer processing, partitions, sinks or skew.
- Scaling strategies: batching, parallelism, partitions.
- Kafka fundamentals: partitions limit consumer parallelism, rebalancing, retention risk.

<p class="qb-label">Step-by-step approach</p>

1. **Measure:** lag per partition (even or skewed?), consumer throughput, processing time per message, rebalance frequency, errors and retries.
2. **Find the bottleneck:** per-message API calls (about 20 ms each) plus single-row inserts (about 5 ms) cap each consumer at roughly 40 messages per second. Twenty-four partitions with one consumer each is about 1k per second, far below 12k per second.
3. **Fixes, in order of impact:**
   - **Batch processing:** poll batches (`max.poll.records`), do bulk database writes (COPY or multi-row inserts), and use batch or cached enrichment.
   - **Async or concurrent enrichment** inside the consumer, with bounded concurrency (see [Concurrent API calls with asyncio](/python/python-interview-questions/#42-concurrent-api-calls-with-asyncio)), or cache enrichment results (see [Coding: implement an LRU cache](/python/python-interview-questions/#49-coding-implement-an-lru-cache)).
   - **Scale consumers** up to the partition count, and increase partitions if needed (this changes key-to-partition mapping for future messages).
   - **Fix skew:** a hot key in one partition needs a key redesign (see [A hot partition in a Kafka topic](#31-a-hot-partition-in-a-kafka-topic)).
4. **Stability:** tune `max.poll.interval.ms` to avoid rebalances during long batches, use the cooperative-sticky assignor, and handle poison messages through a DLQ.
5. **Retention risk:** make sure lag doesn't exceed topic retention (7 days), or data is lost. Temporarily raise retention.
6. **Monitor:** lag alerts and end-to-end latency SLOs.

<p class="qb-label">Sample strong answer</p>

"First I'd check lag per partition. It's growing evenly across all 24, so it's throughput, not skew. Each message does a synchronous enrichment call and a single-row insert, about 25 ms per message, so a consumer handles roughly 40 per second, and 24 consumers about 1,000 per second against 12,000 coming in. Adding consumers alone can't fix that, because we're capped at 24 by partitions. I'd restructure processing: poll batches of 500, enrich with a cached lookup (the enrichment data changes daily, so we can preload it), and write with batched inserts or COPY. That pushes each consumer to thousands per second. As an immediate mitigation, I'd raise topic retention from 7 to 14 days, so lag doesn't turn into data loss while we deploy. After the fix, lag should drain within hours. I'd add alerting on lag above 5 minutes of traffic, and consider increasing partitions to 48 for headroom as the market grows."

<p class="qb-label">Architecture and flow</p>

```text
Before: poll 1 msg ─▶ API call (20ms) ─▶ INSERT row (5ms)   ≈ 40 msg/s/consumer × 24 = ~1k/s  << 12k/s

After:
poll batch (500) ─▶ enrich from in-memory cache (refreshed daily) ─▶ bulk write (COPY/multi-row)
      │                                                                     │
      └─ commit offsets after successful batch write ◀───────────────────────┘
Ops: retention ↑ temporarily, DLQ for poison messages, lag alerts, partitions 24 → 48 (headroom)
```

<p class="qb-label">Alternatives and trade-offs</p>

| Option | Pros | Cons |
|---|---|---|
| More consumers | Easy | Capped at partition count |
| More partitions | More parallelism | Key→partition remap; ordering changes for keys |
| Batching | Huge throughput gains | Larger retry units |
| Async enrichment | Overlaps I/O | Complexity, rate limits |
| Stream processor (Spark/Flink) | Scalable, batching built in | New platform to operate |

<p class="qb-label">Follow-up questions</p>

- How do you preserve exactly-once into the DB? — Idempotent upserts keyed by payment_id, offsets committed after writes.
- What causes frequent rebalances? — Processing exceeding `max.poll.interval.ms`, unstable consumers, deployments.
- Why does lag risk data loss? — Messages older than retention are deleted before being consumed.

**In depth:** [Consumer lag keeps growing. How do you diagnose and fix it?](/interview/kafka/consumer-lag-causes-fixes/)

</details>

### 25. Out-of-order events in a streaming aggregation

<p class="qb-meta">Intermediate · Common</p>

**Scenario:** IoT sensors send temperature readings through Kafka. A Spark streaming job computes 5-minute average temperatures per device for alerts. Network issues cause readings to arrive up to 20 minutes out of order, and some devices buffer for hours. Alerts sometimes fire on incomplete windows, and the averages differ from the batch-computed truth.

<details class="qb-more">
<summary>Sample answer and approach</summary>

<p class="qb-label">What the interviewer is testing</p>

- Event-time windowing and watermarks vs processing time.
- The trade-off between latency and completeness.
- Handling very late data separately.

<p class="qb-label">Step-by-step approach</p>

1. **Use event time** (the sensor timestamp), not arrival time, for windows.
2. **Measure the lateness distribution:** say 99% of readings arrive within 15 minutes, and a long tail within hours.
3. **Set the watermark** (for example 20 minutes) with append mode, so each window is emitted once, after it's mostly complete. Or use update mode for provisional results.
4. **Alerting logic:** decide whether alerts need complete windows. Critical alerts might use provisional results (update mode) with a confidence flag. Reporting uses finalised windows.
5. **Very late data** (beyond the watermark) is dropped from streaming state. Capture it with a separate path, the raw table, plus periodic batch corrections of affected windows.
6. **Clock issues:** devices with bad clocks need sanity checks (reject future timestamps, or correct with an ingestion-time offset).

<p class="qb-label">Sample strong answer</p>

"The alerts fire on incomplete windows because the job either uses processing time or has too short a watermark. I'd make sure windows use the device event timestamp, then set the watermark from the measured lateness. Since 99% of readings arrive within 15 minutes, a 20-minute watermark in append mode emits each 5-minute window once, with most data present. For alerting, a 20-minute delay may be too slow, so I'd run update mode for a fast provisional signal: alert only if the average is above the threshold and at least, say, 80% of expected readings are in. Then a final check confirms once the window closes. Readings that arrive hours late are dropped by streaming state, but they still land in the raw Delta table, and a nightly batch job recomputes the affected windows for reporting accuracy. I'd also reject readings with timestamps in the future and flag devices with clock drift."

<p class="qb-label">Architecture and flow</p>

```text
Sensors ─▶ Kafka ─▶ Spark stream (event_ts)
                    ├─ raw Delta (all readings, incl. very late)
                    ├─ withWatermark(event_ts, 20m) + window(5m) ─▶ final averages (append)
                    └─ update-mode provisional averages + completeness % ─▶ alerting
Nightly batch: recompute windows touched by late data from raw ─▶ corrected reporting table
```

<p class="qb-label">Alternatives and trade-offs</p>

| Approach | Latency | Completeness | Cost |
|---|---|---|---|
| Short watermark (2m) | Low | Low | Low state |
| Long watermark (2h) | High | High | Large state |
| Update mode + final mode | Low provisional + correct final | High eventually | Two outputs |
| Batch correction | Next day | Complete | Extra batch job |

<p class="qb-label">Follow-up questions</p>

- What happens to data later than the watermark? — Not guaranteed to be included in stateful aggregates (typically dropped; see [Watermarks and late data in Structured Streaming](/pyspark/pyspark-interview-questions/#47-watermarks-and-late-data-in-structured-streaming)).
- How do you pick the watermark? — Lateness percentiles vs latency requirements and state size.
- How would Flink differ? — Similar watermarks; allowed lateness and side outputs for late events are first-class.

</details>

### 26. A join on customer_id is skewed by one huge customer (See also Q264)

<p class="qb-meta">Intermediate · Very Common</p>

**Scenario:** A B2B marketplace's nightly Spark job joins `line_items` (3 B rows) with `customer_contracts` (40 M rows) on `customer_id`. One enterprise customer accounts for 22% of line items. The join stage shows 1,999 tasks finishing in 1–2 minutes and one task running 95 minutes before failing with executor OOM.

<details class="qb-more">
<summary>Sample answer and approach</summary>

<p class="qb-label">What the interviewer is testing</p>

- Recognising skew from task metrics.
- Choosing between AQE, broadcast, salting and hot-key isolation.
- Explaining why adding resources doesn't help.

<p class="qb-label">Step-by-step approach</p>

1. **Confirm the skew:** top-N key counts and that task's shuffle read size.
2. **Check AQE:** is skew-join handling enabled? Do the thresholds trigger (factor 5, 256 MB)? Is the join type supported?
3. **Can the other side be broadcast?** At 40 M rows it's probably too large. Can it be filtered to active contracts first?
4. **Isolate the hot key:**
   - Split `line_items` into the hot customer and everything else.
   - Join the hot customer's rows to its few contract rows with a broadcast, which is tiny for one customer.
   - Join the rest normally, then union the results.
5. **Or salt** the hot key with N buckets and replicate the matching contract rows N times.
6. **Validate:** results are identical to a reference (row counts), and the runtime improves.

<p class="qb-label">Sample strong answer</p>

"One task with 95 minutes and OOM, while the rest finish in 2 minutes, is skew. The top-keys query confirms customer 'C-1001' has 660 M line items. AQE skew handling should split that partition, so I'd check it's enabled and tune `skewedPartitionThresholdInBytes` if the partition is just under the threshold. If AQE isn't enough, the cleanest fix here is isolating the hot key. That customer has only about 300 contract rows, so I'd filter `line_items` for C-1001 and broadcast-join them with its contracts. Spark then spreads those 660 M rows across all tasks with no shuffle on the skewed key. The remaining customers go through the normal sort-merge join, and I union the two results. The stage drops to a few minutes. I'd make the hot-key list dynamic, computed from yesterday's top keys, and add a monitor on key concentration, so the next big customer doesn't surprise us. Adding executors wouldn't help, because one key always goes to one task."

<p class="qb-label">Architecture and flow</p>

```text
line_items ─┬─ filter(customer_id IN hot_keys) ─▶ broadcast join (hot contracts, tiny) ─┐
            └─ filter(NOT hot_keys) ─────────▶ sort-merge join (other contracts) ───────┼─▶ union ─▶ output
hot_keys = top-N customers by yesterday's volume (dynamic)                               │
monitor: key concentration %, max task duration ─────────────────────────────────────────┘
```

<p class="qb-label">Alternatives and trade-offs</p>

| Fix | Pros | Cons |
|---|---|---|
| AQE skew join | Zero code | Thresholds; joins only |
| Hot-key isolation + broadcast | Precise, efficient | Extra code paths; maintain hot list |
| Salting | General | Replicates other side; complexity |
| Bigger executors | Might avoid OOM | Still one slow task; costly |

<p class="qb-label">Follow-up questions</p>

- Why not just increase shuffle partitions? — The skewed key still hashes to one partition.
- How do NULL keys cause similar issues? — All NULLs hash together; filter them out of joins (see [How do you detect data skew?](/pyspark/pyspark-interview-questions/#21-how-do-you-detect-data-skew)).
- How do you verify correctness after the change? — Compare row counts and aggregates with the previous output.

</details>

### 27. An incremental load silently missed records

<p class="qb-meta">Intermediate · Common</p>

**Scenario:** An hourly job loads changed rows from a MySQL `orders` table using `WHERE updated_at > :last_max_updated_at`, then sets the new watermark to `max(updated_at)` of the batch. A monthly audit found about 0.2% of orders missing in the warehouse, all with `updated_at` values close to batch boundaries.

<details class="qb-more">
<summary>Sample answer and approach</summary>

<p class="qb-label">What the interviewer is testing</p>

- The classic watermark pitfalls: long transactions committing late, clock precision, `>` vs `>=`, and timestamps set by application servers with clock skew.
- Fixes: overlap windows plus idempotent merges, CDC, a monotonically increasing sequence.
- Detection: reconciliation.

<p class="qb-label">Step-by-step approach</p>

1. **Explain the failure modes:**
   - **Late commits:** a transaction starts at 10:59:58 (and sets `updated_at` then) but commits at 11:00:05, after the 11:00 extract has already read up to 10:59:59. The next run uses `> 10:59:59`, so the row is missed forever.
   - **Equal timestamps** at the boundary with strict `>`, when several rows share the max timestamp and some weren't visible yet.
   - **Application clock skew** across servers.
   - **Timestamp precision** truncation (seconds vs microseconds).
2. **Fix:**
   - **Overlap:** select `updated_at >= last_watermark - 15 minutes`, and MERGE on the PK (idempotent), so duplicates are harmless.
   - **Watermark as of the extract start minus a safety margin,** not the max seen.
   - Or switch to **log-based CDC** (binlog), which captures commits in order (see [CDC from a PostgreSQL OLTP database into the warehouse](#41-cdc-from-a-postgresql-oltp-database-into-the-warehouse)).
3. **Repair:** backfill the gap with a reconciliation, comparing IDs over the audit window.
4. **Detect:** daily count and sum reconciliation per day (see [Source and target row counts don't match](#3-source-and-target-row-counts-dont-match)).

<p class="qb-label">Sample strong answer</p>

"The pattern `updated_at > max(updated_at)` assumes rows become visible in timestamp order, which isn't true. A transaction can stamp `updated_at` at 10:59:58 and commit at 11:00:05, after our extract read the table, and the next run starts after 10:59:59, so that row is skipped forever. I'd change the extraction to re-read a 15-minute overlap, `updated_at >= watermark - 15 minutes`, and MERGE on `order_id`, so re-reading is harmless. The watermark becomes the extract start time minus the overlap, not the max value seen. That covers long transactions up to 15 minutes, and I'd check the longest transaction durations in MySQL to size it. Longer term, I'd move to binlog-based CDC with Debezium or a managed connector, which captures every committed change in commit order. For the missing 0.2%, I'd backfill by comparing order IDs for the last 90 days and loading the gaps, then add a daily reconciliation of counts per day."

<p class="qb-label">Architecture and flow</p>

```text
Before: WHERE updated_at > last_max  ─▶ append/merge  ─▶ watermark = max(updated_at)   ✗ late commits lost

After (query-based):
WHERE updated_at >= (extract_start_prev - 15m) ─▶ MERGE on order_id (idempotent)
watermark = extract_start - 15m      + daily reconciliation (counts/sums)

Long term: MySQL binlog ─▶ CDC (Debezium/managed) ─▶ ordered changes ─▶ MERGE
```

<p class="qb-label">Alternatives and trade-offs</p>

| Approach | Pros | Cons |
|---|---|---|
| Overlap + MERGE | Simple, fixes most cases | Re-reads data; needs PK |
| Monotonic sequence column (DB-assigned) | Reliable ordering | Requires schema change; still commit-order issue unless assigned at commit |
| Log-based CDC | Complete, includes deletes | More infrastructure |
| Periodic full reconciliation | Catches anything | Expensive on large tables |

<p class="qb-label">Follow-up questions</p>

- Does `>=` alone fix it? — No; late commits can have timestamps well before the watermark, so you need an overlap.
- How do you size the overlap? — Longest expected transaction duration and clock skew, plus margin.
- How do deletes get missed? — Query-based extraction can't see deleted rows (see [Airflow executors: Local vs Celery vs Kubernetes](/career/data-engineering-interview-questions/#84-airflow-executors-local-vs-celery-vs-kubernetes)).

</details>

### 28. Airflow tasks stuck in queued state

<p class="qb-meta">Intermediate · Common</p>

**Scenario:** On a self-managed Airflow deployment (CeleryExecutor or KubernetesExecutor, about 400 DAGs), many tasks sit in "queued" for 30+ minutes every morning between 02:00 and 04:00, and some DAG runs miss their SLAs. The worker nodes show low CPU.

<details class="qb-more">
<summary>Sample answer and approach</summary>

<p class="qb-label">What the interviewer is testing</p>

- Understanding the Airflow components: scheduler, executor, workers, pools, the metadata DB.
- The concurrency limits at every level.
- A systematic troubleshooting approach.

<p class="qb-label">Step-by-step approach</p>

1. **Concurrency settings, checked layer by layer:**
   - `core.parallelism` (global running task slots).
   - `max_active_tasks_per_dag` / `max_active_runs_per_dag`.
   - **Pools** (default pool 128 slots, custom pools exhausted?).
   - Celery `worker_concurrency` × number of workers.
   - Kubernetes namespace quotas or pod pending reasons.
2. **Queue mismatch:** tasks assigned to a Celery queue that no worker listens to stay queued forever.
3. **Scheduler health:** scheduler loop time, DAG parsing time (heavy top-level code in DAG files), the number of schedulers, and metadata database performance (connections, locks).
4. **Kubernetes:** pods pending because of insufficient node capacity, image pull delays, resource requests too large, or cluster autoscaler lag.
5. **Workload shape:** 400 DAGs all scheduled at 02:00 create a thundering herd. Stagger schedules, or use data-aware or asset-triggered scheduling.
6. **Fix and monitor:** adjust the limits, add workers or autoscaling (KEDA for Celery), and track queued-time metrics.

<p class="qb-label">Sample strong answer</p>

"Low worker CPU with many queued tasks means tasks aren't being handed to workers, so I'd check the limits layer by layer. In this case `core.parallelism` was 32, sized for the original setup, while 6 Celery workers with concurrency 16 offered 96 slots, so at most 32 tasks ran globally. Also, 150 DAGs were scheduled exactly at 02:00. Scheduler metrics showed DAG file parsing taking 90 seconds per loop, because several DAG files called a database at import time. Fixes: raise `parallelism` to match worker capacity (about 128 with 8 workers), move the import-time database calls into tasks or into cached Variables, and stagger schedules or switch dependent DAGs to asset-based triggers. I'd also check that the `default_pool` isn't throttling, and add a pool for the Snowflake-heavy tasks so they don't overload the warehouse. After the changes, queued time dropped from 30 minutes to under 1 minute. I'd alert on p95 queued duration."

<p class="qb-label">Architecture and flow</p>

```text
DAG files ─▶ Scheduler (parse loop, scheduling) ─▶ Executor (Celery/K8s) ─▶ Workers/Pods
                 │                                   │                     │
     parse time, top-level code       parallelism, pools, queues     concurrency, capacity, quotas
                 │                                   │                     │
             Metadata DB (connections, locks) ───────┴─────────────────────┘
Check each layer for the binding limit; stagger schedules / asset triggers to flatten peaks
```

<p class="qb-label">Alternatives and trade-offs</p>

| Fix | Pros | Cons |
|---|---|---|
| Raise parallelism/pools | Immediate | Can overload downstream systems |
| More workers / autoscaling | Capacity for peaks | Cost |
| Stagger schedules | Smooth load | Changes delivery times |
| Asset/data-aware scheduling | Runs when inputs ready | Requires DAG redesign |
| Managed Airflow (MWAA, Astronomer, Cloud Composer) | Less ops | Cost, less control |

<p class="qb-label">Follow-up questions</p>

- What's the difference between queued and scheduled states? — Scheduled: decided to run; queued: sent to executor awaiting a worker slot.
- Why avoid heavy top-level code in DAG files? — It runs on every parse loop, slowing scheduling (see [Airflow catchup, backfill, start_date and logical date](/career/data-engineering-interview-questions/#49-airflow-catchup-backfill-start_date-and-logical-date)).
- How do pools help? — Limit concurrent tasks against a shared resource (e.g. a database).

**In depth:** [Airflow is slow and tasks sit in scheduled or queued. How do you find the bottleneck?](/interview/airflow/slow-scheduler-stuck-tasks/)

</details>

### 29. Auditors need lineage and a data catalog

<p class="qb-meta">Intermediate · Occasional</p>

**Scenario:** An insurance company's auditors want to know, for 25 regulatory reports, where every number comes from, who can access the underlying data, and what changed in the last 12 months. The current state is undocumented pipelines across Snowflake, dbt, Airflow and some Python scripts. The audit is in 10 weeks.

<details class="qb-more">
<summary>Sample answer and approach</summary>

<p class="qb-label">What the interviewer is testing</p>

- Practical lineage capture from the tools you already have.
- Prioritising the in-scope reports.
- Access and change evidence (audit logs, Git history).
- Making it sustainable after the audit.

<p class="qb-label">Step-by-step approach</p>

1. **Scope:** list the 25 reports and their source tables. Trace backwards only what's needed first.
2. **Use native lineage:**
   - dbt manifest and docs for model lineage, with exposures for the reports (see [Exposures](/etl-elt/dbt-interview-questions/#26-exposures)).
   - Snowflake `ACCESS_HISTORY` and object dependencies, or Unity Catalog lineage.
   - OpenLineage from Airflow for task-level lineage.
3. **Fill the gaps:** Python scripts outside dbt need manual documentation or instrumentation (OpenLineage integration), or should be migrated into dbt.
4. **Catalog:** choose a tool (Snowflake Horizon, Unity Catalog, DataHub, Atlan, Collibra) to ingest the metadata, then add owners, descriptions and classification tags.
5. **Access evidence:** role grants and access history for the in-scope tables (who queried them).
6. **Change evidence:** Git history and PR reviews for transformation code, deployment logs, and schema change logs.
7. **Package it:** per report, a lineage diagram, metric definitions, owners, controls (tests), access list, and change log.

<p class="qb-label">Sample strong answer</p>

"I'd focus on the 25 reports, not the whole estate. Most logic is already in dbt, so I'd add exposures for each report and generate lineage from the dbt manifest. For the Snowflake side, `ACCESS_HISTORY` confirms which tables each report's queries actually read, and who accessed them. The few Python scripts feeding two reports I'd either instrument with OpenLineage or port into dbt models. Porting is better long term. All of this goes into a catalog, Snowflake Horizon here since we're mostly on Snowflake, or DataHub if we need cross-platform coverage, with owners, descriptions and PII tags. For change evidence, our Git history shows every reviewed change to those models over 12 months, linked to deployments. Each report gets an audit pack: lineage diagram, metric definitions, data quality tests, access list, change log. After the audit, CI requires exposures and owners for any regulatory model, so we don't scramble next year."

<p class="qb-label">Architecture and flow</p>

```text
Reports (25) ─▶ dbt exposures ─▶ dbt manifest lineage ─┐
Airflow (OpenLineage) ───────────────────────────────────┼─▶ Catalog (Horizon/UC/DataHub)
Snowflake ACCESS_HISTORY / object dependencies ──────────┤    owners, tags, descriptions
Python scripts → instrument or migrate to dbt ───────────┘
Evidence: grants + access history (who), Git PRs + deploy logs (what changed), tests (controls)
Output: audit pack per report
```

<p class="qb-label">Alternatives and trade-offs</p>

| Option | Pros | Cons |
|---|---|---|
| Native lineage only (dbt + warehouse) | Free, accurate within tools | Gaps across systems |
| Commercial catalog | Cross-system, UI for auditors | Cost, integration effort |
| Open-source catalog (DataHub, OpenMetadata) | Flexible | Ops burden |
| Manual documentation | Fast for one audit | Goes stale |

<p class="qb-label">Follow-up questions</p>

- How do you get column-level lineage? — dbt platform/Fusion, Unity Catalog, or SQL-parsing lineage tools (see [What is data lineage, and how is it captured?](/career/data-engineering-interview-questions/#66-what-is-data-lineage-and-how-is-it-captured)).
- How do you prove access controls work? — Role design docs, grant reports, access history, masking policy tests.
- How do you keep it current? — CI checks on metadata, ownership reviews, automated ingestion.

</details>

### 30. Migrate orchestration from Airflow to a managed service

<p class="qb-meta">Intermediate · Occasional</p>

**Scenario:** A team runs self-hosted Airflow 2.x on VMs with 250 DAGs. Upgrades are painful, the scheduler crashes monthly, and two engineers spend about 30% of their time on Airflow operations. Leadership asks you to evaluate migrating to a managed option (MWAA, Astronomer, Cloud Composer, or native Lakeflow Jobs / Snowflake tasks), and to execute it in 4 months.

<details class="qb-more">
<summary>Sample answer and approach</summary>

<p class="qb-label">What the interviewer is testing</p>

- Evaluation criteria: compatibility, features, cost, lock-in.
- A low-risk migration plan (inventory, compatibility, parallel run, cutover).
- Handling the Airflow 2 → 3 changes if relevant.

<p class="qb-label">Step-by-step approach</p>

1. **Inventory:** DAGs, operators and providers used, custom plugins, connections and variables, schedules, SLAs, and dependencies on the VM environment (local files, system packages).
2. **Options:**
   - **Managed Airflow** (MWAA, Astronomer, Composer): minimal code change, same paradigm.
   - **Platform-native** (Lakeflow Jobs, Snowflake tasks, dbt platform jobs): less ops and tighter integration, but a rewrite and potential lock-in, and cross-system orchestration is weaker.
3. **Decide using the criteria:**
   - Compatibility with existing DAGs.
   - Cross-system needs.
   - Cost: managed fees vs the engineers' time.
   - Security and networking (private connectivity to sources).
   - Upgrade path (Airflow 3 support).
4. **Prepare:** move secrets to a secrets backend, remove top-level code and local file dependencies, and package dependencies (`requirements` or images).
5. **Migrate in waves:** low-risk DAGs first. Run in parallel with the destinations writing to separate targets, or in a shadow mode, then cut over by pausing the old DAGs.
6. **Decommission** the old infrastructure, and document runbooks.

<p class="qb-label">Sample strong answer</p>

"Since most of the 250 DAGs orchestrate across Postgres, S3, Snowflake and APIs, I'd stay in the Airflow paradigm and move to managed Airflow rather than rewrite into a platform-native scheduler. I'd shortlist MWAA and Astronomer based on our cloud, networking and Airflow 3 support. Month 1: inventory operators, plugins and connections; fix DAGs with heavy top-level code or local-disk dependencies; move connections to a secrets backend; and build the dependency image. A compatibility test runs every DAG's parse and a dry run in the new environment. Months 2–3: migrate in four waves, starting with low-risk internal DAGs. Each DAG is paused on the old cluster and unpaused on the new one at a run boundary, so nothing double-runs, with quick rollback by swapping back. Month 4: the critical finance DAGs, with extra monitoring, then decommission the VMs. Success criteria: no missed SLAs during migration, and ops time down from 30% to under 5%."

<p class="qb-label">Architecture and flow</p>

```text
Inventory (DAGs, operators, plugins, connections) ─▶ compatibility fixes (top-level code, local files, secrets)
        ▼
Managed Airflow env (image with deps, secrets backend, private networking)
        ▼
Wave 1 (low risk) ─▶ Wave 2 ─▶ Wave 3 ─▶ Wave 4 (critical) — pause old / unpause new at run boundaries
        ▼
Monitor SLAs, rollback plan ─▶ decommission self-hosted Airflow
```

<p class="qb-label">Alternatives and trade-offs</p>

| Option | Pros | Cons |
|---|---|---|
| Managed Airflow | Minimal rewrite, same skills | Ongoing fees; some config limits |
| Platform-native schedulers | Deep integration, low ops | Rewrite; weaker cross-system orchestration |
| Dagster/Prefect | Modern features | Rewrite and new concepts |
| Stay self-hosted, invest in automation | Full control | Ops burden remains |

<p class="qb-label">Follow-up questions</p>

- What changes in Airflow 3? — Task SDK, removal of direct metadata DB access from tasks, asset-based scheduling enhancements, new UI (see [What are XComs, and what are their limits?](/career/data-engineering-interview-questions/#48-what-are-xcoms-and-what-are-their-limits)–[Airflow vs Dagster vs Prefect vs cloud-native orchestrators](/career/data-engineering-interview-questions/#51-airflow-vs-dagster-vs-prefect-vs-cloud-native-orchestrators)).
- How do you avoid double runs during cutover? — Pause old DAG before unpausing new; align on logical dates; idempotent tasks.
- How do you migrate connections securely? — Secrets backend (AWS Secrets Manager, Vault) referenced by both environments.

</details>

### 31. A hot partition in a Kafka topic

<p class="qb-meta">Intermediate · Occasional</p>

**Scenario:** The `order_events` topic has 32 partitions keyed by `merchant_id`. One large merchant produces 40% of all events, so partition 7 receives about 13 times the average traffic. Its consumer lags for hours during sales, while the other consumers are idle. Ordering per order must be preserved.

<details class="qb-more">
<summary>Sample answer and approach</summary>

<p class="qb-label">What the interviewer is testing</p>

- Kafka key-based partitioning and ordering guarantees.
- Choosing keys that balance load while preserving the required ordering.
- Migration considerations when changing keys.

<p class="qb-label">Step-by-step approach</p>

1. **Clarify the ordering requirement:** is it per order (`order_id`) or per merchant? Usually per order or per customer, not per merchant.
2. **Re-key:** use `order_id` (or a composite `merchant_id + order_id`) as the message key, which spreads the large merchant across all partitions while keeping per-order ordering.
3. **If per-merchant ordering is truly needed:** split hot merchants into sub-streams (`merchant_id + bucket`, where bucket is a hash of `order_id` mod N) and accept ordering per bucket. Or give the big merchant a dedicated topic with more partitions.
4. **Migration:** changing keys changes partition assignment. Do it with a new topic version (`order_events_v2`), dual-write or migrate the producers, then switch the consumers at a known offset or time. Consumers must tolerate the transition.
5. **Consumer side:** if the consumer aggregates per merchant, use a stateful stream processor that handles the redistribution correctly.
6. **Monitor:** per-partition throughput and lag dashboards.

<p class="qb-label">Sample strong answer</p>

"Keying by `merchant_id` means all of the big merchant's events go to one partition, so one consumer does 40% of the work. I'd first confirm what ordering we actually need. For order processing it's per order, not per merchant. Then the fix is to key by `order_id`, which spreads the merchant's events evenly across all 32 partitions while every event of a given order still lands in the same partition, in order. Because changing the key remaps partitions, I'd create `order_events_v2` keyed by `order_id`, switch the producers, and have consumers drain v1 to the end before reading v2, so there's no reordering for in-flight orders. If some downstream logic did need per-merchant ordering, I'd handle that in a stream processor that aggregates per merchant from the evenly distributed topic. Afterwards, per-partition lag dashboards would show balanced load."

<p class="qb-label">Architecture and flow</p>

```text
Before: key=merchant_id ─▶ partition 7 (40% traffic) ─▶ lagging consumer; others idle

After:
producers ─▶ order_events_v2 (key=order_id, 32+ partitions) ─▶ balanced consumers
migration: producers switch → consumers drain v1 → read v2 from start of cutover
per-merchant aggregates: stream processor groups by merchant_id downstream
```

<p class="qb-label">Alternatives and trade-offs</p>

| Option | Pros | Cons |
|---|---|---|
| Key by order_id | Balanced, preserves per-order ordering | Loses per-merchant ordering |
| Salted merchant key (merchant+bucket) | Balance with partial merchant ordering | Ordering only within bucket |
| Dedicated topic for hot merchant | Isolation | More topics to manage |
| More partitions only | More capacity | Doesn't fix hot key with same key |

<p class="qb-label">Follow-up questions</p>

- Why can't more consumers fix it? — One partition is consumed by one consumer in a group.
- What happens to ordering when you add partitions? — Key-to-partition mapping changes for new messages; plan migrations.
- How would you detect hot partitions? — Per-partition bytes-in/messages-in metrics and lag per partition.

</details>

### 32. Source sends full daily dumps; detect only the changes

<p class="qb-meta">Intermediate · Common</p>

**Scenario:** A legacy ERP exports a full daily CSV of the `products` table (40 M rows, 120 columns, ~25 GB). There's no `updated_at` column and no CDC. Downstream needs an SCD2 dimension and a change feed for a search index. Loading and fully reprocessing everything daily takes 3 hours.

<details class="qb-more">
<summary>Sample answer and approach</summary>

<p class="qb-label">What the interviewer is testing</p>

- Snapshot differencing: hash comparison, inserts, updates and deletes.
- Efficient implementation at scale.
- Feeding SCD2 and downstream consumers.

<p class="qb-label">Step-by-step approach</p>

1. **Load today's dump** into staging (Parquet or Delta conversion first, for speed).
2. **Compute a row hash** of the business columns (normalised, NULL-safe) per primary key.
3. **Compare with yesterday's snapshot** (or the current state table) by full outer join on the key:
   - Key only in today: **insert**.
   - Key in both, hash differs: **update**.
   - Key only in yesterday: **delete** (if the dump is guaranteed complete).
4. **Produce a change table** (op, key, columns, `snapshot_date`), then apply it to SCD2 (see [SCD Type 2 on a 500-million-row customer dimension](#43-scd-type-2-on-a-500-million-row-customer-dimension)) and publish it to the search indexer.
5. **Guard against bad dumps:** if the row count drops by more than X%, stop. A truncated file would otherwise look like mass deletes.
6. **Retention:** keep N days of snapshots for reprocessing.
7. **Tools:** Databricks `AUTO CDC FROM SNAPSHOT`, dbt snapshots (check strategy), or plain SQL hash diffs.

<p class="qb-label">Sample strong answer</p>

"Without timestamps or CDC, I'd diff snapshots. Each day the CSV is loaded into a staging Delta table, and I compute `row_hash = sha2(concat_ws('||', coalesce(col1, '∅'), ...))` over the 120 business columns, normalising whitespace and case where appropriate. A full outer join with yesterday's current-state table on `product_id` classifies inserts, updates (hash changed) and deletes (missing today). Typically only about 1–2% of rows change, so the change table is small. That change set drives the SCD2 merge for `dim_product` and is published to the search index, so neither reprocesses 40 M rows. A safety check stops the run if today's row count is more than 5% below yesterday's, because a truncated export would look like a million deletes. The whole diff takes minutes instead of hours. On Databricks, `AUTO CDC FROM SNAPSHOT` implements exactly this pattern declaratively."

<p class="qb-label">Architecture and flow</p>

```text
ERP full dump (CSV) ─▶ staging_today (Parquet/Delta) + row_hash
                               │ FULL OUTER JOIN on product_id
current_state (yesterday) ─────┘
        ├─ only today → INSERT
        ├─ hash differs → UPDATE
        └─ only yesterday → DELETE (guard: row-count drop check)
                 ▼
         change_feed(op, key, cols, snapshot_date)
             ├─▶ SCD2 dim_product
             └─▶ search index updater
current_state := staging_today (swap)
```

<p class="qb-label">Alternatives and trade-offs</p>

| Approach | Pros | Cons |
|---|---|---|
| Hash diff in SQL/Spark | Efficient, transparent | Must handle NULLs/normalisation |
| Column-by-column compare | No hash collisions | Slow, verbose for 120 cols |
| dbt snapshot (check all) | Built-in SCD2 | Compares on every run; cost at large scale |
| AUTO CDC FROM SNAPSHOT | Declarative | Databricks-specific |
| Ask source for CDC | Best long-term | Legacy constraints |

<p class="qb-label">Follow-up questions</p>

- Hash collisions? — Use SHA-256 (negligible); MD5 acceptable for change detection at moderate scale but weaker.
- What if the dump is late or missing? — Skip diff (no deletes inferred), alert, keep yesterday's state.
- How do you handle column additions? — Version the hash definition; recompute hashes for both snapshots on schema change.

</details>

### 33. Share data with an external partner securely

<p class="qb-meta">Intermediate · Common</p>

**Scenario:** A retailer must share daily sell-through data (store × SKU × day, about 50 M rows per year) with 30 suppliers. Each supplier may only see its own products, without customer PII. Some suppliers use Snowflake, some Databricks, and some only want CSV files. Today this happens through emailed spreadsheets.

<details class="qb-more">
<summary>Sample answer and approach</summary>

<p class="qb-label">What the interviewer is testing</p>

- Choosing sharing mechanisms per consumer platform.
- Row-level security per partner, and data minimisation.
- Auditability, revocation and cost.

<p class="qb-label">Step-by-step approach</p>

1. **Curate a share-ready dataset:** aggregated, with no PII, and documented columns. Store it in a dedicated schema.
2. **Row-level isolation:** a supplier-entitlement mapping table plus secure views or row access policies filtering by the consumer's account or identity. Or separate views per supplier.
3. **Mechanisms:**
   - Snowflake consumers: **Secure Data Sharing** or listings (no copies, live data; see [Secure Data Sharing and reader accounts](/snowflake/snowflake-interview-questions/#26-secure-data-sharing-and-reader-accounts)).
   - Databricks or open consumers: **Delta Sharing** (see [Delta Sharing](/databricks/databricks-interview-questions/#15-delta-sharing)). Snowflake-to-non-Snowflake via reader accounts is an option too.
   - File-only consumers: scheduled exports to per-supplier SFTP or object storage prefixes with short-lived credentials or signed URLs, encrypted.
4. **Contracts and legal:** a data-sharing agreement, usage terms, and retention.
5. **Operations:** freshness SLAs, a schema change policy, monitoring of consumption, and audit logs of access.
6. **Revocation:** remove grants or the share immediately when a contract ends.

<p class="qb-label">Sample strong answer</p>

"I'd build one curated, PII-free `partner_sell_through` table and expose it through secure views with a row access policy driven by a supplier-entitlement table. Each supplier only sees rows for its own SKUs. For the Snowflake suppliers, that's a direct share or private listing per supplier account, which gives live data, no copies, and access I can revoke instantly. Databricks suppliers get the same data via Delta Sharing, or, if we're Snowflake-only, via a reader account or Iceberg-based sharing. The CSV-only suppliers get a nightly export job writing per-supplier encrypted files to their own SFTP folder or object-storage prefix, generated from the same filtered views so the logic is identical. Everything is documented in a data dictionary, with a 30-day notice policy for schema changes, and access logs reviewed monthly. When a supplier contract ends, we revoke their share or credentials the same day."

<p class="qb-label">Architecture and flow</p>

```text
gold.sell_through ─▶ partner_sell_through (aggregated, no PII)
                          │  row access policy / secure views (supplier entitlements)
          ┌───────────────┼───────────────────────────────┐
          ▼               ▼                               ▼
 Snowflake shares    Delta Sharing / reader accounts   Nightly per-supplier exports (encrypted, SFTP/S3 prefix)
 (Snowflake suppliers) (Databricks/open clients)        (CSV-only suppliers)
Governance: data sharing agreement, change policy, access logs, instant revocation
```

<p class="qb-label">Alternatives and trade-offs</p>

| Mechanism | Pros | Cons |
|---|---|---|
| Native sharing (Snowflake/Delta Sharing) | Live, no copies, revocable | Consumer platform dependency |
| File exports | Universal | Copies proliferate; harder revocation |
| API | Controlled, real-time | Build and maintain API |
| Clean rooms | Joint analysis without raw access | More complex setup |

<p class="qb-label">Follow-up questions</p>

- How do you test row-level isolation? — Simulate consumer contexts and assert no foreign rows; automated tests on policies.
- Who pays for compute in Snowflake sharing? — Consumers pay for their queries (reader accounts: provider pays).
- How do you handle a supplier asking for customer-level data? — Privacy review; aggregated or clean-room approaches.

</details>

### 34. A MERGE into a large table runs for hours

<p class="qb-meta">Intermediate · Common</p>

**Scenario:** A nightly MERGE upserts about 5 M changed rows into `fct_events` (8 B rows, 3 TB) on Snowflake (or Delta). It now runs for 3+ hours on a Large warehouse. The MERGE condition is `ON t.event_id = s.event_id`. Most changes affect the last 7 days, but some updates reach back up to 60 days.

<details class="qb-more">
<summary>Sample answer and approach</summary>

<p class="qb-label">What the interviewer is testing</p>

- Why MERGE scans the whole target without pruning.
- Pruning predicates, clustering, and narrowing the source.
- Strategy alternatives: delete+insert by partition, splitting append from update.

<p class="qb-label">Step-by-step approach</p>

1. **Profile:** the target scan reads most micro-partitions or files, because `event_id` is random relative to the physical layout.
2. **Add a pruning predicate:** include `t.event_date >= :min_source_date` (computed from the source batch) in the ON clause, or use `incremental_predicates` in dbt (see [Incremental strategies by adapter (append, merge, delete+insert, insert_overwrite, microbatch)](/etl-elt/dbt-interview-questions/#38-incremental-strategies-by-adapter-append-merge-deleteinsert-insert_overwrite-microbatch)).
3. **Cluster the target** by `event_date` (or liquid clustering on `event_date` and `event_id`), so the date predicate prunes well.
4. **Narrow the source:** dedupe to one row per key, and remove rows identical to the target (hash compare) to shrink the MERGE.
5. **Split the work:**
   - Most of the 5 M rows may be brand-new events. Append those, and MERGE only the true updates.
   - Or **delete+insert per affected date partition** for the dates in the batch.
6. **Databricks specifics:** deletion vectors, Photon and low-shuffle MERGE reduce the rewrite cost (see [MERGE INTO for upserts and its performance](/databricks/databricks-interview-questions/#21-merge-into-for-upserts-and-its-performance)).
7. **Measure** partitions scanned, bytes written and duration before and after.

<p class="qb-label">Sample strong answer</p>

"The MERGE joins on `event_id` only, so Snowflake can't prune and scans all 8 B rows to find matches. I'd first compute the minimum `event_date` in the source batch, 60 days back at worst, and add `AND t.event_date >= :min_date` to the ON clause. With the table clustered by `event_date`, that prunes about 95% of the table. I'd also check the batch: typically 80% of the 5 M rows are new events that never existed, so I'd insert those directly and MERGE only the roughly 1 M real updates, after deduping by `event_id`. With pruning and a smaller MERGE, it should drop from 3 hours to around 10–15 minutes. If updates are spread across too many dates for pruning to help, delete+insert per affected date can be cheaper than a row-level MERGE. I'd validate the result with counts per date and a uniqueness test on `event_id`."

<p class="qb-label">Architecture and flow</p>

```text
source batch (5M) ─▶ dedupe by event_id ─▶ split:
     ├─ new events (not in target recent window) ─▶ INSERT (append)
     └─ updates (~1M) ─▶ MERGE ON t.event_id = s.event_id AND t.event_date >= :min_date
                                  │ target clustered by event_date → pruning
                                  ▼
                           fct_events (8B rows)
Metrics: partitions scanned/total, bytes written, duration
```

<p class="qb-label">Alternatives and trade-offs</p>

| Option | Pros | Cons |
|---|---|---|
| Pruning predicate + clustering | Big win, small change | Predicate must cover all updated dates |
| Split inserts vs updates | Smaller MERGE | Extra logic |
| Delete+insert by date | Simple, predictable | Rewrites whole dates |
| Bigger warehouse | Quick | Expensive, doesn't fix scan |
| Deletion vectors (Delta) | Less rewrite | Read overhead until compaction |

<p class="qb-label">Follow-up questions</p>

- What if updates can be years old? — Partition the MERGE by date ranges, or route very old corrections to a separate periodic job.
- Why dedupe the source? — Duplicate source keys cause errors or nondeterministic results (see [unique_key and on_schema_change](/etl-elt/dbt-interview-questions/#16-unique_key-and-on_schema_change)).
- How do you verify pruning? — Query profile partitions scanned; Delta operation metrics for files read/rewritten.

</details>

### 35. Design the dimensional model for e-commerce orders

<p class="qb-meta">Intermediate · Very Common</p>

**Scenario:** An e-commerce company wants analytics on orders: revenue by product, category, customer segment, channel, promotion and day. They also want returns analysis, an order funnel (placed → shipped → delivered), and customer lifetime value. Sources: orders, order lines, payments, shipments, returns, products (with category changes), customers (segment changes), and promotions.

<details class="qb-more">
<summary>Sample answer and approach</summary>

<p class="qb-label">What the interviewer is testing</p>

- Grain declaration and fact table types.
- Conformed dimensions, SCD choices and bridge or role-playing dimensions.
- Handling returns, multiple payments and order status changes.

<p class="qb-label">Step-by-step approach</p>

1. **Business processes:** ordering, payment, fulfilment and returns.
2. **Facts:**
   - `fct_order_line` (grain: one row per order line), a transaction fact with quantity, gross, discount, tax and net amounts.
   - `fct_order_fulfillment` (grain: one row per order), an accumulating snapshot with timestamps for placed, paid, shipped and delivered, plus lag measures.
   - `fct_return_line` (grain: one row per returned line), with the reason and refund amount.
   - Optionally `fct_payment` (one row per payment) for split payments.
3. **Dimensions:**
   - `dim_date` (role-playing: order date, ship date, delivery date).
   - `dim_customer` (SCD2 on segment).
   - `dim_product` (SCD2 on category and price tier).
   - `dim_channel`, `dim_promotion`, `dim_geography`.
4. **Keys:** surrogate keys, with the fact rows referencing the dimension version valid at order time.
5. **Derived marts:** daily revenue aggregates and a customer LTV table built from facts.
6. **Edge cases:**
   - Multi-currency (store the original and converted amounts).
   - Order-level discounts allocated to lines.
   - Cancelled orders (a status column or a separate fact).
   - Late-arriving dimensions, handled with unknown members.

<p class="qb-label">Sample strong answer</p>

"I'd start with grain. The core is `fct_order_line`, one row per order line, because revenue by product and promotion needs line-level detail. It carries quantity, gross, allocated discount, tax and net amounts in transaction currency and USD, with keys to the date (order date), customer, product, promotion, channel and geography dimensions. Order-level discounts are allocated to lines proportionally, so sums add up. For the funnel, an accumulating snapshot `fct_order_fulfillment` has one row per order, with placed, paid, shipped and delivered timestamps and lag measures, updated as status changes. Returns get their own `fct_return_line`, linked to the original order line, so net revenue equals sales minus refunds. `dim_customer` and `dim_product` are SCD2, because segment and category changes must not rewrite history, and the facts use the surrogate key valid at order time. `dim_date` plays multiple roles. CLV is a derived mart over facts per customer. Unknown-member rows handle late dimension data."

<p class="qb-label">Architecture and flow</p>

```text
                    dim_date (order/ship/delivery roles)
                                 │
dim_customer (SCD2) ─┐           │           ┌─ dim_product (SCD2)
dim_channel ─────────┼── fct_order_line ─────┼─ dim_promotion
dim_geography ───────┘  (grain: order line)  └─ (currency handled in fact)
                                 │
fct_order_fulfillment (accumulating snapshot: one row per order, milestone dates)
fct_return_line (grain: returned line → links to order line)
Derived marts: agg_daily_revenue, customer_ltv
```

<p class="qb-label">Alternatives and trade-offs</p>

| Choice | Option A | Option B | Notes |
|---|---|---|---|
| Fact grain | Order line | Order | Line grain needed for product analysis |
| Returns | Separate fact | Negative lines in sales fact | Separate fact clearer; netting in marts |
| Customer changes | SCD2 | SCD1 | SCD2 preserves historical segment |
| Wide table (OBT) | Fewer joins for BI | Duplication, harder changes | Can be built on top of star |

<p class="qb-label">Follow-up questions</p>

- How do you handle an order with two payment methods? — Separate `fct_payment` at payment grain; don't duplicate order lines (see [Gaps and islands: longest streak of consecutive login days](/sql/sql-interview-questions/#39-gaps-and-islands-longest-streak-of-consecutive-login-days)).
- How do you model product bundles? — Bridge table or explode bundle components into lines with allocation.
- What is an accumulating snapshot? — Fact row per process instance updated as milestones occur (see [Explain transactional, periodic snapshot and accumulating snapshot fact tables](/career/data-engineering-interview-questions/#32-explain-transactional-periodic-snapshot-and-accumulating-snapshot-fact-tables)).

</details>

### 36. A pipeline gets slower every month

<p class="qb-meta">Intermediate · Common</p>

**Scenario:** A daily customer analytics pipeline took 25 minutes a year ago. It now takes 2 hours 40 minutes, growing about 15% a month. Data volume grew 2× in the same period. No major code changes. It runs on Spark (or Snowflake) with mostly full-table rebuilds.

<details class="qb-more">
<summary>Sample answer and approach</summary>

<p class="qb-label">What the interviewer is testing</p>

- Recognising non-linear growth patterns: full rebuilds, accumulating state, small files, unbounded history scans.
- A plan to make runtime proportional to new data, not total data.
- Capacity planning and trend monitoring.

<p class="qb-label">Step-by-step approach</p>

1. **Profile the trend:** runtime per step over months. Which steps grew faster than the data?
2. **Typical causes:**
   - **Full rebuilds** over ever-growing history: cost grows with total size, not daily volume.
   - **Joins against growing tables** without pruning.
   - **Window functions over all history** (running totals since day one).
   - **Small files and metadata growth** (see [The data lake has millions of tiny files](#23-the-data-lake-has-millions-of-tiny-files)).
   - **Unvacuumed tables** with many versions.
   - **Skew growth** (big customers getting bigger).
   - **Shared warehouse contention** growing over time.
3. **Fix:**
   - Convert large steps to incremental processing (see [Incremental models and is_incremental()](/etl-elt/dbt-interview-questions/#15-incremental-models-and-is_incremental)).
   - Partition or cluster for pruning.
   - Incremental or state-based computations for cumulative metrics (keep running totals and add the daily delta).
   - Compaction and maintenance schedules.
4. **Plan capacity:** project the runtime against the SLA based on the growth rate, and set alerts when the runtime crosses thresholds.

<p class="qb-label">Sample strong answer</p>

"Runtime growing 6× while data grew 2× suggests the work scales with total history, or worse. I'd chart runtime per stage over 12 months. In this case, three models dominate: a full rebuild of `customer_metrics` that recomputes lifetime aggregates from all orders every day, a join between events and a growing session table without date pruning, and a table with 40,000 small files from daily appends. Fixes: make `customer_metrics` incremental by maintaining lifetime totals and adding each day's delta, recomputing only the customers with new activity. Add a date filter so the events-sessions join only touches the last 2 days. Compact the small-file table, with auto-compaction going forward. That brings runtime back to around 30 minutes, and future growth tracks daily volume instead of total history. I'd add a monthly trend report and an alert if runtime exceeds 60% of the SLA window, so we catch the next regression early."

<p class="qb-label">Architecture and flow</p>

```text
Runtime trend per stage (12 months) ─▶ identify super-linear stages
     ├─ full rebuild over all history → incremental (delta-based aggregates)
     ├─ unpruned joins → partition/cluster + date predicates
     ├─ small files / versions → compaction + VACUUM
     └─ skew growth → hot-key handling
Capacity plan: projected runtime vs SLA, alert at 60% of window
```

<p class="qb-label">Alternatives and trade-offs</p>

| Option | Pros | Cons |
|---|---|---|
| Scale compute | Immediate | Cost grows with data; masks inefficiency |
| Incremental redesign | Sustainable | Engineering effort; correctness care |
| Archive cold history | Smaller working set | Access patterns for history change |
| Pre-aggregated snapshots | Fast reads | Additional storage, refresh logic |

<p class="qb-label">Follow-up questions</p>

- How do you validate incremental results equal full rebuilds? — Periodic full rebuild comparison (audit_helper/diffs) on samples.
- What metrics would you track? — Runtime per model, bytes scanned, rows processed, partitions pruned.
- When is full rebuild still fine? — Small tables where simplicity beats incremental complexity.

</details>

### 37. Make KPIs consistent across three BI tools

<p class="qb-meta">Intermediate · Common</p>

**Scenario:** Finance uses Power BI, marketing uses Tableau, and product uses Looker. "Active customers", "net revenue" and "churn rate" differ across the three by 3–12%. Each tool has its own calculated fields over the same Snowflake marts. Leadership wants one number per KPI within one quarter.

<details class="qb-more">
<summary>Sample answer and approach</summary>

<p class="qb-label">What the interviewer is testing</p>

- Metric governance: definitions, owners and approval.
- Technical options: a semantic layer, certified tables, warehouse-native metrics.
- Migration and adoption strategy across teams.

<p class="qb-label">Step-by-step approach</p>

1. **Inventory the definitions** of each KPI in every tool, and document the differences (filters, time grain, currency, treatment of refunds and trials).
2. **Agree on definitions** with the business owners: a metric council or the finance owner for financial KPIs. Write them up in a glossary with a name, formula, grain, filters, owner and version.
3. **Choose where the logic lives:**
   - **Semantic layer** (dbt Semantic Layer/MetricFlow, Cube, AtScale, LookML as a hub, Snowflake semantic views, Databricks metric views) queried by all the tools.
   - Or **certified metric tables** (pre-computed KPIs at common grains) in Snowflake that all tools read directly.
4. **Implement:** metrics as code with tests. Reconcile the old and new numbers, and explain the differences.
5. **Migrate the dashboards:** replace tool-specific calculated fields with references to the semantic layer or certified tables. Badge certified dashboards.
6. **Govern:** changes go through PR plus owner approval, and usage of non-certified calculations is monitored.
7. **Communicate:** a changelog and a "why your number changed" note.

<p class="qb-label">Sample strong answer</p>

"This is half governance, half engineering. I'd first document how each tool calculates the three KPIs. For example, Tableau's 'net revenue' excludes refunds issued after month-end, and Looker's 'active customers' counts free trials. Then I'd get finance and product leadership to agree one definition for each, with an owner. For implementation, I'd define the metrics once in the dbt Semantic Layer (or Snowflake semantic views, if we standardise on Snowflake) on top of the existing marts. Power BI and Tableau connect through the semantic layer integrations, and for Looker we'd map the LookML measures to the same definitions, or point it at certified metric tables generated from the semantic layer. Every dashboard tile showing these KPIs gets migrated and labelled 'certified'. We'd publish a reconciliation showing old vs new numbers and why. Changes to definitions then require a PR approved by the metric owner, so they can't drift again."

<p class="qb-label">Architecture and flow</p>

```text
Snowflake marts (fct_orders, dim_customer, subscriptions)
        ▼
Metric definitions as code (semantic layer: net_revenue, active_customers, churn_rate)
   owners + tests + versioning (PR + owner approval)
        ├─▶ Power BI (connector / certified tables)
        ├─▶ Tableau (connector / certified tables)
        └─▶ Looker (LookML mapped to same definitions or certified tables)
Glossary + certified badges + reconciliation report + changelog
```

<p class="qb-label">Alternatives and trade-offs</p>

| Option | Pros | Cons |
|---|---|---|
| Universal semantic layer | Single definition, flexible queries | Tool integration maturity varies; new component |
| Certified metric tables | Works with any tool | Fixed grains; table sprawl |
| One BI tool for everyone | Consistency by default | Politically hard; migration cost |
| Documentation only | Cheap | Definitions still drift in tools |

<p class="qb-label">Follow-up questions</p>

- How do you handle a team that needs a variant metric? — Create a separately named, documented metric (e.g. `active_customers_incl_trials`).
- How do you detect drift later? — Periodic comparisons of dashboard values vs certified metric outputs.
- Where do warehouse-native semantic views fit? — Same goal, defined in the warehouse and usable by AI/BI tools (see [What is a semantic/metrics layer?](/career/data-engineering-interview-questions/#75-what-is-a-semanticmetrics-layer), [dbt Semantic Layer and MetricFlow (See also Q99)](/etl-elt/dbt-interview-questions/#43-dbt-semantic-layer-and-metricflow-see-also-q99)).

</details>

## Advanced scenario-based questions (experienced)

### 38. Backfill two years of history without breaking production

<p class="qb-meta">Advanced · Very Common</p>

**Scenario:** A new business rule changes how `net_revenue` is calculated. You must recompute two years (730 days) of a 6 B-row fact table on Databricks, partitioned by `order_date`, while daily production loads keep running, dashboards stay available, and the backfill budget is about $8k of compute.

<details class="qb-more">
<summary>Sample answer and approach</summary>

<p class="qb-label">What the interviewer is testing</p>

- Safe, idempotent, chunked backfill design.
- Isolating backfill compute and avoiding conflicts with production writes.
- Validation and cutover strategy.
- Cost and time estimation.

<p class="qb-label">Step-by-step approach</p>

1. **Clarify:** do all two years need the new rule, or only from an effective date? Does history need to stay queryable during the backfill? Is the source raw data still available for all 730 days?
2. **Estimate:** time one representative day on the target cluster (say 4 minutes, $9), then multiply by 730 for about 49 hours and $6.5k. Run several days in parallel to finish in a weekend.
3. **Design for idempotency:** each unit is one day (or a month) written with `replaceWhere order_date = ...`, so reruns are safe.
4. **Isolate:** a separate job cluster or serverless budget, a lower priority, and no shared all-purpose clusters.
5. **Avoid conflicts:** write the backfill into a **shadow table** (`fct_orders_v2`), or into partitions production doesn't touch (older dates). Production keeps writing recent days.
6. **Validate** each chunk: row counts equal the old table, the revenue difference is explained by the rule, and tests pass.
7. **Cut over:** swap a view or rename the table, update the daily job to the new logic the same day, and keep the old table for 30 days.
8. **Track progress** in a control table (date → status), so failed days are retried individually.

<p class="qb-label">Sample strong answer</p>

"I'd avoid touching the live table during the backfill. I'd create `fct_orders_v2` with the new logic and backfill it month by month using a Lakeflow Job For-each task over 24 months, concurrency 4, on a dedicated job cluster with spot workers. Each month writes with `replaceWhere` on its date range, so any month can be rerun safely, and a control table records status and row counts. Timing one month first gives me the real cost. If it's around $250 per month, 24 months comes to roughly $6k, within budget. Meanwhile the daily job writes the current day to both tables, so v2 doesn't fall behind. Validation per month compares row counts and recomputes revenue under the old rule from v2 to make sure only the intended change happened. Finance signs off on the deltas for a sample of months. Cutover is a view swap (`fct_orders` → v2) done in a quiet window, with the old table retained for a month for rollback."

<p class="qb-label">Architecture and flow</p>

```text
                ┌──────────── daily prod job ───────────┐
raw/bronze ─────┤                                       ├─▶ fct_orders (current logic) ─┐
                │                                       └─▶ fct_orders_v2 (new logic)   │
                └─ backfill job: for-each month (x4 parallel)                            │
                     replaceWhere month ─▶ fct_orders_v2 ─▶ validate ─▶ control table   │
                                                                                        ▼
                                               cutover: view fct_orders → v2 (old kept 30d)
```

<p class="qb-label">Alternatives and trade-offs</p>

| Option | Pros | Cons |
|---|---|---|
| In-place partition rewrite | No extra storage | Consumers see mixed logic mid-backfill |
| Shadow table + swap | Clean cutover, easy rollback | Double storage temporarily, dual writes |
| Full rebuild in one job | Simple | One failure restarts everything; huge cluster |
| Effective-dating the rule (no backfill) | Cheapest | History inconsistent with new definition |

<p class="qb-label">Follow-up questions</p>

- What if raw data for year one is gone? — Backfill from the oldest available layer (silver), or apply the rule only from available history, documenting the limitation.
- How do you stop the backfill starving production? — Separate compute, concurrency limits, schedule off-peak.
- How would you do this in dbt? — Microbatch with `--event-time-start/end`, or var-driven date ranges (see [The microbatch incremental strategy](/etl-elt/dbt-interview-questions/#39-the-microbatch-incremental-strategy)).

</details>

### 39. A Spark job went from 40 minutes to 4 hours

<p class="qb-meta">Advanced · Very Common</p>

**Scenario:** A nightly PySpark job on Databricks joins `orders` (2 TB) with `customers` (60 GB) and `promotions` (200 MB), aggregates, and writes to Delta. It ran in 40 minutes for months. Last week it jumped to 4 hours. No code changed, but a platform team upgraded the runtime version, and marketing launched a promotion with one partner account generating huge volume.

<details class="qb-more">
<summary>Sample answer and approach</summary>

<p class="qb-label">What the interviewer is testing</p>

- A systematic performance investigation (compare runs, Spark UI).
- Recognising skew, plan changes, spill, small files and cluster issues.
- Prioritising fixes by evidence.

<p class="qb-label">Step-by-step approach</p>

1. **Compare a good run with a bad run:** input sizes, the runtime version, the cluster config and the physical plans (`explain`, the SQL tab).
2. **Find the slowest stage.** Is the task duration distribution skewed (one task taking hours), or is everything slower evenly?
3. **Check the hypotheses:**
   - Skew from the partner account: one task with huge shuffle read.
   - Plan regression after the upgrade: a broadcast join became a sort-merge join, or AQE settings changed.
   - Data growth or small files: the input file count exploded.
   - Spill or GC from memory pressure.
   - Cluster: autoscaling capped, or spot losses.
4. **Fix the top cause:** AQE skew join, salting or isolating the hot key, a broadcast hint for promotions, file compaction.
5. **Re-measure,** then add a duration alert and a regression check.

<p class="qb-label">Sample strong answer</p>

"I'd open the Spark UI for last month's run and last night's. The bad run's join stage has 799 tasks finishing in about 2 minutes, and one task running 3.5 hours with 300 GB of shuffle read. That's skew, and it lines up with the partner promotion: one `customer_id` now has 25% of orders. I'd also check the plan diff, because the runtime upgrade could change the join strategy. Promotions was still broadcast, fine. The fix is to make sure AQE skew-join handling is enabled and tuned, which splits the oversized partition automatically. If that's not enough, because the skewed side is an aggregation, I'd salt the hot key or process it separately, then union. I'd also filter rows with NULL customer IDs out of the join. After the fix the job ran in 45 minutes. To catch this earlier, I'd add a job duration warning at 1.5× the median, plus a daily top-keys report on the join column."

<p class="qb-label">Architecture and flow</p>

```text
Good run vs bad run
  ├─ inputs (rows/bytes/files)   ─┐
  ├─ runtime/config diff          ├─▶ locate slowest stage ─▶ task-time distribution
  └─ physical plan diff           ─┘                               │
                                      ┌─ one huge task ─▶ skew ─▶ AQE skew join / salting / isolate key
                                      ├─ all tasks slower ─▶ spill/GC/plan ─▶ memory, partitions, broadcast
                                      └─ many tiny tasks ─▶ small files ─▶ compaction
```

<p class="qb-label">Alternatives and trade-offs</p>

| Fix | Pros | Cons |
|---|---|---|
| AQE skew join | Automatic, no code | Joins only; thresholds need tuning |
| Salting | Works for aggregations too | Code complexity; replicates other side |
| Separate hot-key path | Precise | Extra logic to maintain |
| Bigger cluster | Quick | Doesn't fix skew; wastes money |

<p class="qb-label">Follow-up questions</p>

- What if the plan changed after the upgrade? — Pin/override configs temporarily, file a regression, compare `explain` output (see [Using explain() to read physical plans](/pyspark/pyspark-interview-questions/#27-using-explain-to-read-physical-plans)).
- How do you prove skew numerically? — `groupBy(key).count()` top-N vs median (see [How do you detect data skew?](/pyspark/pyspark-interview-questions/#21-how-do-you-detect-data-skew)).
- What monitoring would you add? — Duration SLOs per job, stage-level metrics export, data volume anomaly checks.

</details>

### 40. Design a real-time clickstream pipeline (50k events/sec)

<p class="qb-meta">Advanced · Very Common</p>

**Scenario:** Design a pipeline for web and app clickstream: a peak of 50,000 events per second (about 4 B events/day, ~1 KB each, ~4 TB/day raw). Requirements: real-time dashboards (active users per minute, with under 1 minute of latency), sessionised data for analysts by the next morning, 13 months of retention, GDPR deletes, and a budget-conscious team of 5 engineers.

<details class="qb-more">
<summary>Sample answer and approach</summary>

<p class="qb-label">What the interviewer is testing</p>

- End-to-end streaming architecture with capacity math.
- Separation of the real-time and analytical paths, and exactly-once or deduplication design.
- Storage formats, retention, privacy and cost.
- Operational simplicity for a small team.

<p class="qb-label">Step-by-step approach</p>

1. **Clarify:** latency per use case, ordering needs, the dedupe key (event_id), PII fields, and whether any consumers need sub-second latency (fraud, personalisation).
2. **Ingestion:** an SDK sends to a collector API, which writes to a managed Kafka (MSK, Confluent or Event Hubs). Size it at 50 MB/s × 2 for headroom: about 48–96 partitions keyed by `user_id` or `session_id`, replication 3, 7 days of retention.
3. **Stream processing:** Spark Structured Streaming or Flink: parse, validate, dedupe on `event_id` within a watermark, enrich with geo and device, then write to bronze and silver Delta or Iceberg tables every minute.
4. **Real-time metrics:** a windowed aggregation (1 minute, 10-minute watermark) to a low-latency store (a Delta table read by the dashboard, or a real-time OLAP engine such as Druid, Pinot or ClickHouse if latency needs to be sub-second).
5. **Batch layer:** hourly or nightly sessionisation (30-minute inactivity gap), gold marts, and dbt models.
6. **Storage:** Parquet-based table formats partitioned or clustered by date, with compaction. Retention: bronze 30 days, silver 13 months.
7. **Privacy:** hash or pseudonymise `user_id`, store the PII mapping separately for GDPR deletes (see [How does GDPR "right to erasure" affect lake and warehouse design?](/career/data-engineering-interview-questions/#97-how-does-gdpr-right-to-erasure-affect-lake-and-warehouse-design), [Fulfil a GDPR deletion request across lake, warehouse and backups (See also Q85)](#44-fulfil-a-gdpr-deletion-request-across-lake-warehouse-and-backups-see-also-q85)), and apply consent flags.
8. **Ops:** monitor consumer lag, end-to-end latency, DLQs for bad events, schema registry, and IaC.

<p class="qb-label">Sample strong answer</p>

"At 50k events per second of about 1 KB, that's around 50 MB/s at peak, so I'd use managed Kafka with 64 partitions keyed by `session_id`, enough for ordering per session and parallelism with headroom. A Structured Streaming job on job compute reads Kafka, validates against the schema registry, sends bad events to a DLQ topic, dedupes by `event_id` within a 2-hour watermark, and writes silver Delta every 60 seconds with optimized writes and auto-compaction. A second small stream computes active users per minute with HyperLogLog-style approximate distinct counts and a 5-minute watermark, written to a small Delta table the dashboard polls, which gives roughly 1–2 minutes end to end. Sessionisation and marts run as an hourly batch with dbt, which is simpler to backfill. User IDs are pseudonymised at ingest, with the mapping in a restricted table, so a GDPR delete means deleting the mapping plus targeted deletes. With five engineers I'd lean on managed services and serverless where possible, and keep a single processing engine to limit operational load."

<p class="qb-label">Architecture and flow</p>

```text
Web/App SDK ─▶ Collector API (stateless, autoscaled) ─▶ Kafka (64 partitions, RF=3, 7d)
                                                          │            │
                                                          │            └─▶ DLQ topic (bad events)
                                     ┌────────────────────┴────────────────────┐
                                     ▼                                         ▼
                 Stream job A: parse, dedupe(event_id, wm 2h),     Stream job B: 1-min active users
                 enrich ─▶ bronze/silver Delta (1-min commits)      (approx distinct, wm 5m) ─▶ rt_metrics
                                     │                                         │
                                     ▼                                         ▼
                 Hourly batch (dbt): sessions (30-min gap), marts         Real-time dashboard
                                     │
                                     ▼
                     BI / analysts / ML features   |  GDPR: pseudonymised ids + mapping table
```

<p class="qb-label">Alternatives and trade-offs</p>

| Choice | Option A | Option B | Notes |
|---|---|---|---|
| Stream engine | Spark Structured Streaming | Flink | Spark aligns with lakehouse batch; Flink lower latency, richer state |
| Real-time serving | Delta table + BI polling | Druid/Pinot/ClickHouse | OLAP engine for sub-second, high-concurrency |
| Distinct users | Exact (state heavy) | Approximate (HLL) | Approx is cheaper; state bounded |
| Sessionisation | Streaming stateful | Batch hourly | Batch easier to correct/backfill |
| Kafka | Self-managed | Managed | Managed reduces ops for small team |

<p class="qb-label">Follow-up questions</p>

- How do you handle a 5× traffic spike? — Kafka buffers; autoscaling consumers; rate limits on collector; partitions sized for headroom.
- How do you get exactly-once into Delta? — Checkpoints + idempotent Delta sink (see [Checkpoints and exactly-once in Structured Streaming](/pyspark/pyspark-interview-questions/#48-checkpoints-and-exactly-once-in-structured-streaming)).
- How would you reprocess a bad day? — Replay from bronze (or Kafka within retention) into a rebuilt partition.

</details>

### 41. CDC from a PostgreSQL OLTP database into the warehouse

<p class="qb-meta">Advanced · Very Common</p>

**Scenario:** The orders service runs PostgreSQL 16 (2 TB, 120 tables, ~3,000 writes per second at peak). Analytics currently runs nightly `SELECT *` dumps that take 5 hours and slow the primary database. The business wants data in Snowflake within 15 minutes, deletes reflected, and no impact on the OLTP workload.

<details class="qb-more">
<summary>Sample answer and approach</summary>

<p class="qb-label">What the interviewer is testing</p>

- Log-based CDC concepts: WAL, logical replication, replication slots.
- The initial snapshot plus streaming changes, ordering and deletes.
- The apply strategy in the warehouse (MERGE, or a raw change log plus current-state views).
- Operational risks: slot lag, schema changes, failover.

<p class="qb-label">Step-by-step approach</p>

1. **Choose the tooling:**
   - Debezium on Kafka Connect (self-managed or Confluent).
   - Managed options: Fivetran, AWS DMS, Snowflake Openflow, Lakeflow Connect.
   - The trade-off is ops effort vs cost and control.
2. **Source setup:**
   - `wal_level=logical`, a publication for the selected tables, a replication slot, and a dedicated replication user.
   - Read from a replica if supported (PostgreSQL 16 supports logical decoding on standbys).
   - Check primary keys or `REPLICA IDENTITY` on every table.
3. **Initial load:** a consistent snapshot (Debezium incremental snapshots, or a parallel export), then the stream from the slot position.
4. **Transport:** Kafka topics per table (keyed by PK) with Avro or Protobuf and a schema registry.
5. **Land:** the Snowflake Kafka connector with Snowpipe Streaming, into raw change tables (op, LSN, before and after, ts).
6. **Apply:** MERGE into current-state tables every 5–10 minutes (dynamic tables or Streams + Tasks). Order by LSN, take the latest per PK, and apply deletes as hard or soft deletes. Keep the change log for history and SCD2 (see [SCD Type 2 on a 500-million-row customer dimension](#43-scd-type-2-on-a-500-million-row-customer-dimension)).
7. **Operations:**
   - Monitor slot lag. An unconsumed slot retains WAL and can fill the disk, so set `max_slot_wal_keep_size`.
   - Handle schema changes, and document the failover procedure for the slot.
   - Run reconciliation counts.

<p class="qb-label">Sample strong answer</p>

"I'd replace the dumps with log-based CDC using Debezium's PostgreSQL connector, which reads the WAL through a logical replication slot. That adds very little load compared with full scans. Debezium does an initial incremental snapshot table by table without locking, then streams inserts, updates and deletes into Kafka topics keyed by primary key, with Avro schemas in a registry. The Snowflake Kafka connector lands changes with Snowpipe Streaming into a raw changes table per entity, in seconds. A dynamic table, or a task every 5 minutes, dedupes to the latest change per key by LSN and MERGEs into the current-state tables. Deletes become soft-delete flags, so downstream incremental models see them. The biggest operational risk is replication slot lag filling the primary's disk, so I'd alert on slot lag and set `max_slot_wal_keep_size`. I'd also make sure every table has a primary key or a defined replica identity, and run daily count reconciliations per table."

<p class="qb-label">Architecture and flow</p>

```text
PostgreSQL 16 (WAL, logical slot, publication)
        │  (low overhead, ordered changes, LSN)
        ▼
Debezium (Kafka Connect) ── initial incremental snapshot + streaming
        │
        ▼
Kafka topics per table (key = PK, Avro + Schema Registry)
        │
        ▼
Snowflake Kafka Connector (Snowpipe Streaming) ─▶ RAW.<table>_changes (op, lsn, before, after, ts)
        │
        ▼  every 5 min: latest per PK by LSN
MERGE / Dynamic Table ─▶ CORE.<table> (current state, soft deletes) ─▶ dbt marts / SCD2
Monitoring: slot lag, connector status, end-to-end latency, daily row-count reconciliation
```

<p class="qb-label">Alternatives and trade-offs</p>

| Option | Pros | Cons |
|---|---|---|
| Debezium + Kafka | Open, flexible, multi-consumer | Kafka + Connect ops burden |
| Managed ELT (Fivetran etc.) | Fast to set up, low ops | Per-row cost; less control |
| Query-based incremental (updated_at) | Simple | Misses deletes; load on source; clock issues (see [Airflow executors: Local vs Celery vs Kubernetes](/career/data-engineering-interview-questions/#84-airflow-executors-local-vs-celery-vs-kubernetes)) |
| Warehouse-native connectors (Openflow, Lakeflow Connect) | Integrated governance | Feature coverage varies |

<p class="qb-label">Follow-up questions</p>

- What happens if the connector is down for a day? — Slot retains WAL (watch disk); on resume it catches up; if WAL was dropped, re-snapshot.
- How do you handle a column added to a source table? — Schema registry evolution, connector propagates; downstream evolution rules (see [Upstream added and renamed columns without notice (schema drift)](#19-upstream-added-and-renamed-columns-without-notice-schema-drift)).
- How do you handle tables without a PK? — Set `REPLICA IDENTITY FULL` (heavier) or add keys.

</details>

### 42. Migrate an on-prem Oracle/Teradata warehouse to the cloud

<p class="qb-meta">Advanced · Common</p>

**Scenario:** A bank runs a 40 TB Teradata warehouse with about 3,500 tables, 1,200 BTEQ and stored-procedure jobs, and 400 BI reports. The data centre contract ends in 14 months. Target: Snowflake (or Databricks), with dbt for transformations. The regulators require reports to stay identical through the migration.

<details class="qb-more">
<summary>Sample answer and approach</summary>

<p class="qb-label">What the interviewer is testing</p>

- Migration strategy and phasing (assess, migrate, validate, cut over).
- Code conversion approach and automation.
- Data validation at scale, and parallel runs.
- Risk management, governance and stakeholder alignment.

<p class="qb-label">Step-by-step approach</p>

1. **Assess:**
   - Inventory objects, jobs, dependencies and usage (query logs to find unused tables, often 30–40%).
   - Classify the complexity of each object.
   - Map out security and regulatory needs.
2. **Plan the waves** by business domain, starting with a low-risk but meaningful domain as a pilot. Define the target architecture (raw → staging → marts, RBAC, environments, CI/CD).
3. **Move the data:**
   - Historical bulk export to cloud storage, then COPY.
   - Incremental sync (CDC, or delta extracts) during the parallel run.
4. **Convert the code:**
   - Automated tools (SnowConvert, other converters) for DDL and SQL.
   - Refactor stored procedures and BTEQ into dbt models and orchestrated jobs.
   - Don't lift and shift bad patterns blindly.
5. **Validate:**
   - Row counts and checksums per table.
   - Column-level aggregates.
   - Report-level comparisons.
   - Automated diff tooling (audit_helper, data-diff).
6. **Parallel run:** at least one full month-end close, with signed-off reconciliations.
7. **Cut over:** repoint BI, freeze the legacy system, keep a rollback plan, then decommission.
8. **People:** training, a runbook, and support during hypercare.

<p class="qb-label">Sample strong answer</p>

"I'd start with a 6-week assessment: pull 90 days of Teradata query logs to see which of the 3,500 tables and 400 reports are actually used. Typically a third can be retired, which shrinks scope. Then I'd group the rest into about 6 domain waves and pilot with one, say retail deposits. Data moves by bulk export to S3 and COPY for history, plus daily incremental extracts during the parallel run. For code, I'd run an automated converter for DDL and simple SQL, but rewrite the complex BTEQ and stored procedures as dbt models with tests, rather than translating procedural code line by line. Every table gets automated validation: counts, sums of numeric columns, and hash comparisons on keys. Every regulatory report is compared field by field. Each wave runs in parallel through at least one month-end, and the business signs off before cutover. A rollback plan keeps Teradata read-only for 60 days after each wave. With 14 months, I'd aim to finish migration by month 11, leaving buffer."

<p class="qb-label">Architecture and flow</p>

```text
Assess (logs, lineage, usage) ─▶ Wave plan (domains) ─▶ Pilot wave
                                                          │
Teradata ──bulk export──▶ S3/ADLS ─▶ COPY ─▶ Snowflake RAW
        └─incremental (CDC/deltas) during parallel run──┘
                                                          │
Converted DDL + dbt models (refactored BTEQ/SPs) ─▶ STAGING ─▶ MARTS ─▶ BI (repointed)
                                                          │
Validation: counts, checksums, column aggregates, report diffs ─▶ parallel run (month-end) ─▶ sign-off ─▶ cutover
```

<p class="qb-label">Alternatives and trade-offs</p>

| Strategy | Pros | Cons |
|---|---|---|
| Lift and shift (convert as-is) | Fastest | Carries technical debt; procedural code poor fit |
| Re-architect everything | Clean | High risk, long timeline |
| Hybrid (convert simple, refactor complex) | Balanced | Needs good triage |
| Big-bang cutover | One switch | High risk |
| Wave-based cutover | Lower risk, learning | Longer dual-running cost |

<p class="qb-label">Follow-up questions</p>

- How do you validate 40 TB efficiently? — Aggregates and hash sums per partition/column instead of row-by-row diffs; sample-level diffs for drill-down.
- What about Teradata-specific SQL (QUALIFY, volatile tables)? — Map to Snowflake equivalents (QUALIFY exists; temp tables) via converter + review.
- How do you handle report owners resisting change? — Early involvement, side-by-side comparison dashboards, clear sign-off criteria.

</details>

### 43. SCD Type 2 on a 500-million-row customer dimension

<p class="qb-meta">Advanced · Common</p>

**Scenario:** A telecom maintains `dim_customer` with SCD2 history: 500 M rows total, about 80 M current. A daily CDC feed delivers roughly 2 M changed customers. Today's MERGE-based SCD2 job takes 3.5 hours on Snowflake and occasionally creates two "current" rows for a customer. The SLA is 1 hour.

<details class="qb-more">
<summary>Sample answer and approach</summary>

<p class="qb-label">What the interviewer is testing</p>

- A correct SCD2 algorithm (close old versions, insert new ones), atomically.
- Performance at scale: pruning, change detection, clustering.
- Data quality invariants (one current row per key, no overlaps).
- Handling of late or out-of-order changes.

<p class="qb-label">Step-by-step approach</p>

1. **Diagnose the slowness:** the Query Profile probably shows a full scan of 500 M rows for the MERGE join, and change detection comparing every column.
2. **Restrict the work:**
   - Join the changes only to **current** rows (`is_current = TRUE`), clustered by `is_current, customer_id`, or kept in a separate current table.
   - Pre-compute a hash diff (`row_hash`) of the tracked attributes, so you compare one column.
3. **Dedupe the feed:** latest change per customer per batch, ordered by source sequence.
4. **Atomic two-step in one transaction:**
   - UPDATE the current rows to set `valid_to` and `is_current = FALSE` where the hash differs.
   - INSERT the new versions.
   - Or use a single MERGE with a "union of changes" staging trick.
5. **Invariants:** test one current row per key, no overlapping intervals, and `valid_from < valid_to`.
6. **Late changes:** if an older change arrives, decide whether to insert it into history by splitting intervals (complex), or treat it as a correction (simpler, documented).
7. **Consider tooling:** dbt snapshots (state-compare based), Lakeflow AUTO CDC (sequence-aware), or a dynamic-table approach.

<p class="qb-label">Sample strong answer</p>

"The double-current rows suggest the MERGE either ran on a batch with duplicate keys or the update and insert weren't atomic. And 3.5 hours suggests we're scanning all 500 M rows. I'd restructure it. First, dedupe the 2 M changes to one row per customer by source sequence, and compute a `row_hash` over the tracked columns. Second, join only against current rows. I'd cluster `dim_customer` by `(is_current, customer_id)`, so the 80 M current rows prune well, or maintain a separate `dim_customer_current` table. Then, in one transaction, I'd expire changed current rows and insert new versions with `valid_from = change_ts`. Unchanged hashes are skipped, which is usually 30–50% of CDC rows. Tests assert exactly one current row per customer and no overlapping ranges. This pattern typically drops the runtime to 15–30 minutes. For out-of-order changes I'd use the source sequence as `valid_from` and, if a late change predates the current version, route it to a correction process rather than complicate the daily path."

<p class="qb-label">Architecture and flow</p>

```text
CDC changes (2M/day) ─▶ dedupe latest per customer (by seq) + row_hash
                               │
                               ▼  join only current rows (pruned by is_current, customer_id)
              ┌──────── changed hash? ─────────┐
              │yes                              │no → skip
              ▼                                 
   BEGIN; UPDATE current SET valid_to=change_ts, is_current=false;
          INSERT new version (valid_from=change_ts, valid_to=9999-12-31, is_current=true); COMMIT;
                               │
                               ▼
   Tests: 1 current/key, no overlaps, valid_from < valid_to
```

<p class="qb-label">Alternatives and trade-offs</p>

| Approach | Pros | Cons |
|---|---|---|
| Single MERGE (union trick) | One statement | Harder to read/debug |
| UPDATE + INSERT in transaction | Clear, efficient with pruning | Two statements |
| dbt snapshot | Standard, little code | Compares state per run; slower at huge scale |
| AUTO CDC (Databricks) | Ordering & late data handled | Platform-specific |
| Separate current + history tables | Fast current lookups | Two tables to keep consistent |

<p class="qb-label">Follow-up questions</p>

- Why hash diffs? — One comparison instead of dozens; NULL-safe if you coalesce before hashing.
- How do fact tables join to the right version? — Surrogate key lookup by business key and event time within validity range (see [How do you design and load an SCD Type 2 dimension?](/career/data-engineering-interview-questions/#36-how-do-you-design-and-load-an-scd-type-2-dimension)).
- How would you rebuild history if logic changes? — Replay CDC log from raw change tables in order.

</details>

### 44. Fulfil a GDPR deletion request across lake, warehouse and backups (See also Q85)

<p class="qb-meta">Advanced · Common</p>

**Scenario:** A customer invokes the right to erasure. Their data exists in: raw JSON files in S3 (7 years), Delta bronze, silver and gold tables, Snowflake marts, Kafka topics (7-day retention), ML feature tables, BI extracts, and nightly backups. The legal deadline is 30 days. You get about 200 such requests per month.

<details class="qb-more">
<summary>Sample answer and approach</summary>

<p class="qb-label">What the interviewer is testing</p>

- Finding all copies of personal data (lineage, catalog, tagging).
- Delete mechanics in immutable or append-only stores (time travel, VACUUM, backups).
- Designing for erasure: pseudonymisation and crypto-shredding.
- Automation, evidence and audit.

<p class="qb-label">Step-by-step approach</p>

1. **Identify the subject:** resolve all their identifiers (customer_id, email, device IDs) through the identity mapping.
2. **Locate the data:** a PII catalog and tags, plus lineage, show every table and file holding the subject's data or derived data.
3. **Delete or anonymise per store:**
   - Delta: `DELETE`, then `REORG ... APPLY (PURGE)` if deletion vectors are on, then `VACUUM` after the retention period.
   - Snowflake: `DELETE`. Time Travel and Fail-safe retain data for up to 90 + 7 days, which needs to be documented in the retention policy.
   - Raw files: rewrite the affected files, or use per-subject encryption keys.
   - Kafka: retention expires within 7 days, or use compacted topics with tombstones.
   - Backups: let them expire on a defined schedule, and record the suppression list so restores re-apply deletions.
4. **Derived data:** aggregates usually contain no personal data, so leave them. ML training sets need policy decisions.
5. **Automate:** a request intake, then a workflow that runs the deletes per system, verifies them, and records evidence.
6. **Design for the future:**
   - Pseudonymised IDs in analytics, with the PII held in one vault table.
   - **Crypto-shredding:** encrypt PII with a per-user key, and delete the key.

<p class="qb-label">Sample strong answer</p>

"I'd treat this as a repeatable workflow, not a one-off, since we get 200 a month. Step one is resolving the person's identifiers through our identity table. Then our PII-tagged catalog and lineage give the list of tables holding those identifiers. An orchestrated job runs deletes per system: Delta tables with DELETE, then REORG PURGE and a scheduled VACUUM, so files are physically removed after 7 days, and Snowflake DELETEs, with our documented policy that Time Travel and Fail-safe copies age out within 97 days. Raw S3 JSON is the hardest. Going forward I'd stop storing raw PII there: pseudonymise at ingestion, and keep the PII in an encrypted vault keyed per customer, so erasure means deleting the vault row and key (crypto-shredding). For the backlog of raw files, a monthly rewrite job filters out the subjects. Backups age out on 35-day retention, and the suppression list is re-applied on any restore. Every step logs evidence for the DPO."

<p class="qb-label">Architecture and flow</p>

```text
Erasure request ─▶ identity resolution (all ids) ─▶ lineage/PII catalog: affected assets
        │
        ▼ orchestrated per-system deletes
  Delta: DELETE → REORG PURGE → VACUUM     Snowflake: DELETE (TT/Fail-safe age-out documented)
  Raw S3: monthly rewrite job / crypto-shred key   Kafka: retention/tombstones
  Feature store, BI extracts: delete/refresh       Backups: expire + suppression list on restore
        │
        ▼
  Verification queries ─▶ evidence log ─▶ DPO report
Future: pseudonymous ids in analytics + PII vault with per-user keys
```

<p class="qb-label">Alternatives and trade-offs</p>

| Approach | Pros | Cons |
|---|---|---|
| Physical deletes everywhere | Clear compliance | Expensive rewrites of immutable data |
| Pseudonymisation + vault | Most systems hold no direct PII | Requires upfront design; re-identification controls |
| Crypto-shredding | Fast erasure, even in backups | Key management complexity |
| Anonymised aggregates retained | Keeps analytics | Must ensure true anonymisation (k-anonymity concerns) |

<p class="qb-label">Follow-up questions</p>

- Is Time Travel a compliance issue? — Document retention and ensure data ages out within the legal window; minimise retention on PII tables.
- How do you prove deletion? — Verification queries recorded with timestamps, plus audit logs.
- What about ML models trained on the data? — Policy-driven: retrain periodically without deleted subjects; legal guidance needed.

</details>

### 45. Design a data platform from scratch for a growing startup

<p class="qb-meta">Advanced · Common</p>

**Scenario:** A Series B fintech startup (300 employees, growing fast) has a Postgres production database, Stripe, HubSpot, Segment events (~20 M/day), and Google Sheets. There's no data team yet beyond you and one analyst. They need investor metrics, product analytics, finance reporting, and SOC 2 readiness within 6 months. The data budget is about $15k/month.

<details class="qb-more">
<summary>Sample answer and approach</summary>

<p class="qb-label">What the interviewer is testing</p>

- Pragmatic architecture choices for a small team, using managed services.
- Prioritisation: what to build first.
- Governance and security from day one (fintech, SOC 2).
- A growth path that avoids early over-engineering.

<p class="qb-label">Step-by-step approach</p>

1. **Clarify** the top use cases and stakeholders (investor KPIs, finance close, product funnels), the data volumes, compliance requirements, and the existing cloud (AWS or GCP).
2. **Stack:**
   - **Ingestion:** a managed ELT tool (Fivetran or Airbyte Cloud) for Stripe, HubSpot and Postgres CDC. Segment warehouse destination for events.
   - **Warehouse:** Snowflake, BigQuery or Databricks SQL. Pick one, based on cloud and skills.
   - **Transformation:** dbt with the staging → marts layering, tests and docs.
   - **Orchestration:** dbt platform jobs, or a lightweight orchestrator. Avoid running Airflow yourself early on.
   - **BI:** one tool with a semantic layer for KPI definitions.
3. **Governance from day one:**
   - SSO, role-based access, and PII tagging with masking.
   - Separate prod and dev environments.
   - Audit logs, secrets in a manager, and IaC (Terraform).
4. **Delivery plan:**
   - Month 1: ingestion and core finance and investor metrics.
   - Months 2–3: product analytics.
   - Months 4–6: data quality, catalog and SOC 2 controls.
5. **Cost:** warehouse auto-suspend, small warehouses, ELT row-based pricing awareness, and monthly reviews.
6. **Evolution path:** streaming or reverse ETL only when needed, and dbt Mesh or domains when the team grows.

<p class="qb-label">Sample strong answer</p>

"With two people and six months, I'd buy rather than build. Fivetran for Stripe, HubSpot and Postgres via log-based CDC from a read replica, and Segment's native warehouse destination for events. Snowflake as the warehouse, since we're on AWS and it needs little ops. dbt on the dbt platform for transformations, CI and scheduling, so we don't operate Airflow. One BI tool, with KPI definitions in dbt's semantic layer, so investor metrics like ARR and net revenue retention mean one thing. From day one: SSO, roles per function, PII tagged and masked, prod and dev databases, Terraform for warehouse objects, and secrets in AWS Secrets Manager. That covers much of what SOC 2 auditors look for. Month 1 delivers the finance and investor dashboard, the most valuable thing. Months 2 and 3 cover product funnels. By month 6, tests on every model, freshness alerts, and documentation. Estimated cost is about $6–9k/month at current volumes, well within budget. I'd revisit streaming only when a real-time use case such as fraud scoring appears."

<p class="qb-label">Architecture and flow</p>

```text
Postgres (read replica, CDC) ┐
Stripe, HubSpot, Sheets ─────┼─▶ Managed ELT (Fivetran/Airbyte) ─▶ Snowflake RAW
Segment events ──────────────┘                                     │
                                                                   ▼
                                 dbt (staging → intermediate → marts, tests, docs, semantic layer)
                                                                   │
                                     BI dashboards (finance, investor KPIs, product) ◀──┘
Cross-cutting: SSO + RBAC, PII tags/masking, Terraform, secrets manager, prod/dev envs, cost monitors
```

<p class="qb-label">Alternatives and trade-offs</p>

| Decision | Option A | Option B | Rationale |
|---|---|---|---|
| Ingestion | Managed ELT | Custom scripts | Managed saves months for small team; cost per row grows later |
| Warehouse | Snowflake/BigQuery | Lakehouse (Databricks) | SQL-first team → warehouse; lakehouse if ML-heavy early |
| Orchestration | dbt platform jobs | Self-hosted Airflow | Low ops vs flexibility |
| Streaming | Later | Now | No current real-time requirement |

<p class="qb-label">Follow-up questions</p>

- When would you add a data lake? — When raw file/event volume, ML workloads or cost make warehouse-only storage inefficient.
- What would you monitor first? — Freshness of key sources, dbt test failures, warehouse spend.
- How do you scale the team's practices? — Code review, conventions, CI, ownership per domain.

</details>

### 46. Multiple jobs writing to the same table cause conflicts

<p class="qb-meta">Advanced · Occasional</p>

**Scenario:** Three jobs write to `silver.customer_profile` (Delta, 300 M rows): an hourly CRM MERGE, a streaming job updating `last_seen_at`, and a nightly GDPR delete job. Since adding the streaming job, the hourly MERGE fails 3–5 times a day with `ConcurrentAppendException`, and the GDPR job sometimes fails too.

<details class="qb-more">
<summary>Sample answer and approach</summary>

<p class="qb-label">What the interviewer is testing</p>

- Understanding of optimistic concurrency in table formats.
- Designing writer ownership and data layout to avoid conflicts.
- Options: retries, partitioning or clustering, row-level concurrency, splitting tables.

<p class="qb-label">Step-by-step approach</p>

1. **Understand the conflict:** each writer reads a snapshot and rewrites files. If two transactions touch the same files, the later commit fails (see [Optimistic concurrency control and write conflicts in Delta](/databricks/databricks-interview-questions/#39-optimistic-concurrency-control-and-write-conflicts-in-delta)).
2. **Reduce overlap:**
   - **Split responsibilities:** move frequently updated columns (`last_seen_at`) to a separate table (`customer_activity`), joined in a view or gold model. Different tables, no conflicts.
   - **Partitioning or clustering** so writers touch disjoint files, with explicit predicates in the MERGE conditions.
   - **Row-level concurrency** (Databricks, with deletion vectors and liquid clustering), so updates to different rows in the same files don't conflict.
3. **Coordinate writers:** schedule the GDPR deletes in a window when the hourly MERGE isn't running, or run them through the same orchestrated job.
4. **Retry with backoff** for the remaining transient conflicts. The jobs must be idempotent.
5. **Streaming writer frequency:** batch `last_seen_at` updates every 15 minutes instead of continuously, if acceptable.

<p class="qb-label">Sample strong answer</p>

"Delta uses optimistic concurrency. The streaming job is constantly rewriting files across the whole table to update `last_seen_at`, so the hourly MERGE almost always finds that files it read have changed. The cleanest fix is to stop having three writers on one hot table. I'd move `last_seen_at` into a narrow `customer_activity` table owned only by the streaming job, and expose a `customer_profile_v` view, or a gold table that joins them. The CRM MERGE then only conflicts with the GDPR job, and I'd schedule GDPR deletes right after the hourly MERGE in the same Lakeflow job, so they run sequentially. As extra protection, I'd enable deletion vectors and liquid clustering on `customer_id`, which gives row-level conflict detection, and add a retry with backoff around the MERGE for rare conflicts. The MERGE is idempotent, so retries are safe. Since the change, the conflicts went to zero."

<p class="qb-label">Architecture and flow</p>

```text
Before: CRM MERGE (hourly) ┐
        Streaming updates  ├──▶ silver.customer_profile  ✗ conflicts
        GDPR deletes       ┘

After:
CRM MERGE ─▶ GDPR deletes (same job, sequential) ─▶ silver.customer_profile
Streaming (15-min micro-batches) ─────────────────▶ silver.customer_activity (last_seen_at)
                                   view/gold: profile ⨝ activity
+ deletion vectors + liquid clustering (row-level concurrency), retry w/ backoff
```

<p class="qb-label">Alternatives and trade-offs</p>

| Option | Pros | Cons |
|---|---|---|
| Split hot columns into separate table | Eliminates conflicts | Join at read time |
| Row-level concurrency | Fewer conflicts without redesign | Platform/version-specific |
| Serialize writers (single job) | Simple | Lower freshness/throughput |
| Retries only | Easy | Wasted work; may still fail |
| Partition by writer domain | Disjoint files | Only if natural partition exists |

<p class="qb-label">Follow-up questions</p>

- Why don't appends conflict? — Blind appends don't read existing data; under WriteSerializable they don't conflict (see [Optimistic concurrency control and write conflicts in Delta](/databricks/databricks-interview-questions/#39-optimistic-concurrency-control-and-write-conflicts-in-delta)).
- How does Snowflake handle concurrent DML? — Locking at table/partition level; statements queue rather than fail (behaviour differs).
- How do you make retries safe? — Idempotent MERGE logic and deterministic sources.

</details>

### 47. Build a data quality framework for 200 tables

<p class="qb-meta">Advanced · Common</p>

**Scenario:** A company has 200 critical tables across Snowflake and Databricks, fed by dbt and Spark pipelines. Quality issues are found by business users 2–3 times a week. Leadership asks you to design a data quality framework within a quarter, with a team of 3.

<details class="qb-more">
<summary>Sample answer and approach</summary>

<p class="qb-label">What the interviewer is testing</p>

- Prioritisation (not every table is equal).
- Combining rule-based tests, anomaly detection, contracts and ownership.
- Alerting that is actionable, incident processes, and metrics.
- A tooling choice that is pragmatic.

<p class="qb-label">Step-by-step approach</p>

1. **Tier the tables:** Tier 1 (exec and regulatory, about 30), Tier 2 (operational, about 70), Tier 3 (exploratory). Assign owners.
2. **Define the dimensions:** freshness, volume, schema, validity, uniqueness, referential integrity, reconciliation.
3. **Baseline checks for every table**, generated from metadata:
   - PK unique and not_null, freshness, row-count anomaly, schema change detection.
4. **Business rules** for Tier 1 and 2: range, accepted-value and reconciliation tests, written with the owners.
5. **Where the checks run:**
   - dbt tests (in `build`, as circuit breakers).
   - Expectations in Spark or Lakeflow pipelines.
   - Warehouse-native monitors (Snowflake DMFs, Databricks data quality monitoring).
   - Observability tooling (elementary, Monte Carlo and similar) for anomalies.
6. **Alerting and incidents:** route alerts to the owners by tier (Tier 1 pages, Tier 2 goes to a Slack channel), with a runbook and severity levels.
7. **Prevent at the source:** data contracts for Tier 1 sources (see [What are data contracts, and how do you enforce them?](/career/data-engineering-interview-questions/#83-what-are-data-contracts-and-how-do-you-enforce-them)).
8. **Measure:** incidents found by users vs by checks, time to detect, time to resolve, and a quality score per table on a dashboard.

<p class="qb-label">Sample strong answer</p>

"With three people, I'd go for coverage on what matters most. Week 1–2: work with domain leads to tier the 200 tables. Around 30 are Tier 1, feeding exec dashboards and regulatory reports, each with a named owner. Then I'd auto-generate baseline checks for all 200 from metadata: primary-key uniqueness and not_null, freshness against each table's SLA, row-count anomaly detection, and schema-change alerts. In dbt these become tests run in `dbt build`, so failures block downstream models. For Spark pipelines, they become expectations. For Tier 1, we'd add business rules with the owners, such as reconciliation of revenue against the billing system, valid currency codes, and no negative quantities. Alerts route by tier: Tier 1 to the on-call pager with a runbook, the rest to team channels with daily digests. The success metric is the share of incidents caught by checks before users, aiming to go from about 0% to 80% in a quarter, plus mean time to detect. A quality dashboard shows status per table and per owner."

<p class="qb-label">Architecture and flow</p>

```text
Catalog/metadata ─▶ tiering + owners
        ▼
Baseline checks (auto-generated): PK, freshness, volume anomaly, schema drift   ─┐
Business rules (Tier 1/2): ranges, accepted values, reconciliation              ├─▶ results store
Execution: dbt build tests | Spark/Lakeflow expectations | DMFs/monitors | anomaly tool ─┘
        ▼
Alert routing by tier (pager / Slack digest) ─▶ runbooks ─▶ incident review
        ▼
DQ dashboard: score per table/owner, MTTD, % caught before users
```

<p class="qb-label">Alternatives and trade-offs</p>

| Approach | Pros | Cons |
|---|---|---|
| Tests in pipelines (dbt/expectations) | Blocks bad data | Only known rules |
| Anomaly detection tools | Finds unknown issues | False positives; cost |
| Warehouse-native monitors | No extra infra | Platform-specific |
| Contracts at source | Prevents issues | Requires producer buy-in |
| Manual spot checks | Cheap | Doesn't scale |

<p class="qb-label">Follow-up questions</p>

- How do you avoid alert fatigue? — Tiering, thresholds, grouping, and reviewing noisy checks weekly.
- Who fixes issues? — Table owners with platform support; clear RACI.
- How do you measure success? — % incidents detected by checks, MTTD/MTTR, number of user-reported issues (see [Data observability vs data quality testing](/career/data-engineering-interview-questions/#45-data-observability-vs-data-quality-testing), [Designing a test strategy across layers](/etl-elt/dbt-interview-questions/#35-designing-a-test-strategy-across-layers)).

</details>

### 48. Merge customer data after an acquisition (entity resolution)

<p class="qb-meta">Advanced · Occasional</p>

**Scenario:** Your company acquired a competitor. You have 12 M customers, and they have 5 M. An estimated 15–25% overlap, but there are no shared IDs. Fields include name, email (sometimes missing), phone, address and date of birth (partial). The business wants a unified customer view for marketing and a single count of customers within 3 months.

<details class="qb-more">
<summary>Sample answer and approach</summary>

<p class="qb-label">What the interviewer is testing</p>

- Entity resolution design: standardisation, blocking, matching (deterministic and probabilistic), clustering and survivorship.
- Precision vs recall trade-offs, and human review.
- Golden record management and ongoing matching.

<p class="qb-label">Step-by-step approach</p>

1. **Standardise:**
   - Lower-case and trim emails, apply Gmail dot rules if appropriate.
   - Normalise phone numbers to E.164.
   - Parse and standardise addresses.
   - Normalise names (case, accents, nicknames).
2. **Blocking:** only compare candidate pairs that share a blocking key (email, phone, postcode plus a surname prefix). This avoids 12 M × 5 M comparisons.
3. **Matching rules:**
   - **Deterministic:** exact normalised email, or exact phone plus surname, gives high confidence.
   - **Probabilistic:** a weighted similarity score (Jaro-Winkler on names, address similarity, DOB match), or a Fellegi-Sunter or ML model trained on labelled pairs.
4. **Thresholds:** auto-match above a high threshold, send a grey zone to manual review, and treat the rest as non-matches.
5. **Clustering:** connected components over the matched pairs to form entities, with a check for overly large clusters (bad blocking keys).
6. **Survivorship rules** for the golden record: most recent email, most complete address, a source priority.
7. **Persist** a cross-reference table (`master_customer_id` ↔ source IDs), and run matching incrementally for new customers.
8. **Measure:** precision and recall from labelled samples, and the overlap estimate.

<p class="qb-label">Sample strong answer</p>

"I'd build a matching pipeline rather than a one-off join. First, standardise contact data: normalised emails, E.164 phones, parsed addresses. Then blocking keys (email, phone, postcode plus the first three letters of the surname) generate candidate pairs, maybe tens of millions instead of 60 trillion. Exact email or phone-plus-name matches are auto-merged. For the rest I'd compute similarity features and use a probabilistic model, using a library like Splink on Spark or the warehouse, calibrated on a few thousand pairs labelled by the CRM team. Scores above 0.95 merge automatically, 0.8–0.95 go to a review queue, and below that stay separate. Connected components form customer clusters, and I'd cap and inspect any cluster larger than about 10 records. Survivorship rules pick the golden values, and a cross-reference table maps the master ID to both systems' IDs, so marketing gets one customer count. New customers are matched incrementally every day. I'd report precision and recall, with marketing agreeing that over-merging is worse than under-merging for email campaigns."

<p class="qb-label">Architecture and flow</p>

```text
Source A (12M) ┐                                   ┌─▶ auto-merge (score ≥ 0.95)
Source B (5M)  ┴─▶ standardise ─▶ blocking keys ─▶ candidate pairs ─▶ scoring ─┼─▶ review queue (0.8–0.95)
                                                                               └─▶ non-match
matched pairs ─▶ connected components (cluster size checks) ─▶ survivorship ─▶ golden records
                                                     └─▶ xref: master_customer_id ↔ source ids
Daily incremental matching for new/changed records; precision/recall monitoring
```

<p class="qb-label">Alternatives and trade-offs</p>

| Approach | Pros | Cons |
|---|---|---|
| Deterministic only | Simple, explainable | Misses fuzzy matches |
| Probabilistic/ML | Higher recall | Needs labels, tuning, explainability |
| Commercial MDM/ER tool | Mature workflows | Cost, integration |
| Manual review at scale | Accurate | Impossible for millions |

<p class="qb-label">Follow-up questions</p>

- How do you avoid chaining errors in clusters? — Transitivity checks, cluster size limits, stricter edges for large clusters.
- How do you handle GDPR here? — Purpose limitation, consent merge rules, keep provenance in xref.
- How do you measure accuracy without full ground truth? — Labelled samples, stratified by score bands.

</details>

### 49. Build point-in-time-correct training data for an ML model

<p class="qb-meta">Advanced · Occasional</p>

**Scenario:** Data scientists are training a churn model on 3 years of history: 5 M customers, monthly labels (churned within the next 30 days). Features include support tickets, payments, plan changes and usage aggregates. The offline model shows 0.92 AUC, but production performance is 0.71. You suspect data leakage.

<details class="qb-more">
<summary>Sample answer and approach</summary>

<p class="qb-label">What the interviewer is testing</p>

- Understanding of point-in-time correctness and leakage.
- As-of joins, SCD2 features and feature timestamps.
- Feature stores and training/serving consistency.

<p class="qb-label">Step-by-step approach</p>

1. **Find the leakage:** features computed using data after the prediction time. For example, "total tickets" computed today and joined to an observation from last year, or a status column that already reflects the churn.
2. **Define observation points:** `(customer_id, as_of_ts)` for each training example, with the label computed strictly after `as_of_ts`.
3. **Make features time-aware:**
   - Event features aggregated only from events with `event_ts < as_of_ts` (windows such as the last 30 or 90 days).
   - Dimension attributes from SCD2 versions valid at `as_of_ts` (see [How do you design and load an SCD Type 2 dimension?](/career/data-engineering-interview-questions/#36-how-do-you-design-and-load-an-scd-type-2-dimension)).
   - Account for **data availability delays:** if payments land a day late, use `< as_of_ts − 1 day`.
4. **As-of joins:** a point-in-time join from observations to feature tables keyed by entity and timestamp. Feature stores (Databricks Feature Engineering, SageMaker, Feast, Snowflake Feature Store) offer this.
5. **Consistency:** compute features with the same code for training and serving, whether a feature store or shared definitions.
6. **Validate:** compare feature distributions in training vs production, and do a temporal train/test split (train on older periods, test on newer).

<p class="qb-label">Sample strong answer</p>

"A gap from 0.92 to 0.71 usually means leakage. Looking at the training query, features were computed from current tables: 'tickets in the last 90 days' used today's data for every historical row, and `account_status` already reflected cancellation. I'd rebuild the training set around explicit observation points: for each customer and month-end, `as_of_ts`, with the label being churn in the following 30 days. Every feature is computed from data available before `as_of_ts`. Event aggregates use `event_ts < as_of_ts`, adjusted for ingestion delays. Attributes like plan come from the SCD2 version valid at that time. Implementing this as feature tables with timestamps in the feature store gives point-in-time joins for training and the same definitions online. I'd also switch to a temporal split, training on years 1–2 and testing on year 3. That should bring offline AUC to a realistic level that matches production."

<p class="qb-label">Architecture and flow</p>

```text
Observations: (customer_id, as_of_ts, label = churn in (as_of_ts, as_of_ts+30d])
        │
        │ point-in-time join (features with ts < as_of_ts - availability_delay)
        ▼
Feature tables (timestamped): ticket_counts_90d, payment_failures_30d, usage_trends, plan (SCD2)
        ▼
Training set ─▶ temporal split (train old, test recent) ─▶ model
Same feature definitions ─▶ online/batch serving (feature store) ─▶ consistent predictions
```

<p class="qb-label">Alternatives and trade-offs</p>

| Approach | Pros | Cons |
|---|---|---|
| Ad-hoc SQL as-of joins | Flexible | Easy to get wrong; slow at scale |
| Feature store with PIT joins | Correctness, reuse, serving consistency | Platform adoption |
| Snapshot tables per month | Simple to reason about | Storage; granularity fixed |
| Random train/test split | Easy | Hides temporal leakage |

<p class="qb-label">Follow-up questions</p>

- What is training/serving skew? — Features computed differently (or with different data) at training vs inference time.
- How do you handle late-arriving events in features? — Availability delays in PIT logic; recompute features with corrected data for retraining.
- Why temporal splits? — Mimic production (predict future from past) and expose leakage.

</details>

### 50. Add multi-region disaster recovery to the platform

<p class="qb-meta">Advanced · Occasional</p>

**Scenario:** A payments analytics platform runs in one cloud region: Snowflake, S3 lake, Airflow, and Kafka. Regulators require recovery from a full regional outage with RPO ≤ 1 hour and RTO ≤ 4 hours for tier-1 reporting. The budget is limited, so you can't simply double everything.

<details class="qb-more">
<summary>Sample answer and approach</summary>

<p class="qb-label">What the interviewer is testing</p>

- RPO and RTO concepts applied per component.
- Replication options and their costs.
- Failover runbooks, testing, and data consistency after failover.

<p class="qb-label">Step-by-step approach</p>

1. **Tier the workloads:** tier 1 (regulatory reporting) needs DR. Exploratory analytics can tolerate a longer outage.
2. **Per component:**
   - **Snowflake:** database replication plus failover groups to a secondary account in another region (Business Critical edition), with a refresh schedule of 30 minutes or less for RPO. Client redirect for connection failover.
   - **Object storage:** cross-region replication for tier-1 prefixes (with replication time control if strict).
   - **Kafka:** MirrorMaker 2 or a managed replication tool to a standby cluster, or rely on replaying from sources when they're durable.
   - **Orchestration:** code in Git, infrastructure as code (Terraform) to redeploy Airflow in the DR region, and secrets replicated.
   - **Databricks (if used):** workspace config as code, Unity Catalog metadata considerations, deep clones or replicated storage.
3. **Consistency:** define the recovery point (last replica refresh), and run reconciliation and catch-up jobs after failover.
4. **Runbook:** decision criteria, failover steps, DNS or connection redirect, communications, and failback.
5. **Test:** quarterly DR drills measuring the actual RPO and RTO.
6. **Cost control:** standby compute stays suspended (warehouses at zero cost when idle), and only tier-1 data is replicated.

<p class="qb-label">Sample strong answer</p>

"I'd scope DR to tier-1 reporting rather than the whole platform. On Snowflake, I'd put the tier-1 databases in a failover group replicated to a secondary account in another region every 30 minutes. That meets the 1-hour RPO, and client redirect lets BI tools reconnect with a connection URL switch. Warehouses in the secondary account stay suspended, so we pay mainly for replication and storage. The raw S3 prefixes feeding tier-1 get cross-region replication. Airflow and dbt are code in Git with Terraform, so standing up orchestration in the DR region takes under an hour, and secrets are replicated in the secrets manager. Kafka sources are replayable from the upstream databases via CDC, so instead of a hot standby Kafka cluster, we'd restart CDC into a DR cluster from the last replicated offsets or a fresh snapshot. A runbook defines who declares a disaster, the steps, and validation queries. We'd run a DR drill every quarter, and the last one measured RTO at 2.5 hours."

<p class="qb-label">Architecture and flow</p>

```text
Primary region                                   DR region
Snowflake (tier-1 DBs) ──failover group (30m)──▶ Snowflake secondary (suspended WH) ◀─ client redirect
S3 tier-1 prefixes ──cross-region replication──▶ S3 replica
Airflow/dbt (Git + Terraform) ────────────────▶ redeploy on demand (IaC)
Kafka/CDC ── replay from source CDC / mirror ──▶ standby or rebuilt cluster
Secrets manager ──replicated────────────────────▶ secrets
Runbook + quarterly drills (measure RPO/RTO)
```

<p class="qb-label">Alternatives and trade-offs</p>

| Approach | RPO/RTO | Cost |
|---|---|---|
| Backup & restore only | Hours–days | Low |
| Pilot light (replicated data, cold compute) | ≤1h / hours | Medium |
| Warm standby (some compute running) | Minutes / <1h | Higher |
| Active-active | Near zero | Highest, complex consistency |

<p class="qb-label">Follow-up questions</p>

- What's RPO vs RTO? — Max acceptable data loss window vs max time to restore service.
- How do you fail back? — Reverse replication direction once primary recovers, reconcile, switch back in a window.
- What breaks in DR drills commonly? — Missing secrets/grants, hard-coded endpoints, untested runbooks.

</details>
