---
title: "Top 50 Databricks Interview Questions and Answers"
seoTitle: "Top 50 Databricks Interview Questions and Answers"
description: "Top 50 Databricks interview questions with answers: Delta Lake internals, Unity Catalog, Lakeflow pipelines and jobs, Auto Loader and compute."
technology: ["databricks", "delta-lake", "data-engineering"]
topic: ["interview-questions"]
difficulty: "Intermediate"
section: "learn"
publishedDate: "2026-10-16"
updatedDate: "2026-10-16"
reviewedDate: "2026-10-16"
related: ["interview-questions:delta-lake/what-delta-lake-solves", "interview-questions:delta-lake/time-travel-use-cases-limits", "interview-questions:delta-lake/schema-evolution-safety", "interview-questions:delta-lake/vacuum-retention-risks", "interview-questions:databricks/unity-catalog", "interview-questions:delta-lake/delta-merge-internals-conflicts", "interview-questions:delta-lake/optimize-zorder-vs-liquid-clustering"]
versionContext: "Product names and defaults as of October 2026; code snippets state their dialect or runtime"
---

These 50 Databricks interview questions cover Delta Lake internals and maintenance, Unity Catalog, Lakeflow Declarative Pipelines and Jobs, Auto Loader, compute choices, streaming and cost. They are grouped from beginner (freshers) to advanced (experienced). Each one starts with the answer you would give in the first 20 seconds; open the full answer for the detailed explanation, an example, how it shows up in real work, common mistakes and the follow-up questions interviewers ask.

## Question list

<details class="qb-index" open>
<summary>All 50 questions</summary>

1. [Databricks architecture: control plane vs compute plane (classic and serverless)](#1-databricks-architecture-control-plane-vs-compute-plane-classic-and-serverless)
2. [What is Delta Lake, and how is it different from plain Parquet?](#2-what-is-delta-lake-and-how-is-it-different-from-plain-parquet)
3. [Delta time travel and RESTORE](#3-delta-time-travel-and-restore)
4. [Schema enforcement vs schema evolution (mergeSchema)](#4-schema-enforcement-vs-schema-evolution-mergeschema)
5. [Unity Catalog: metastore and the three-level namespace](#5-unity-catalog-metastore-and-the-three-level-namespace)
6. [Managed vs external tables, and volumes](#6-managed-vs-external-tables-and-volumes)
7. [Lakeflow Spark Declarative Pipelines (formerly DLT): streaming tables vs materialized views](#7-lakeflow-spark-declarative-pipelines-formerly-dlt-streaming-tables-vs-materialized-views)
8. [Lakeflow Jobs (formerly Workflows): tasks, retries and repair runs](#8-lakeflow-jobs-formerly-workflows-tasks-retries-and-repair-runs)
9. [All-purpose vs job compute vs SQL warehouse vs serverless](#9-all-purpose-vs-job-compute-vs-sql-warehouse-vs-serverless)
10. [What is Photon?](#10-what-is-photon)
11. [Databricks Runtime versions and LTS](#11-databricks-runtime-versions-and-lts)
12. [Implementing medallion architecture on Databricks (See also Q32)](#12-implementing-medallion-architecture-on-databricks-see-also-q32)
13. [Notebooks, Git folders, dbutils and widgets](#13-notebooks-git-folders-dbutils-and-widgets)
14. [Secret scopes and secrets management](#14-secret-scopes-and-secrets-management)
15. [Delta Sharing](#15-delta-sharing)
16. [The Delta transaction log: JSON commits and checkpoints](#16-the-delta-transaction-log-json-commits-and-checkpoints)
17. [VACUUM: default retention and the risks](#17-vacuum-default-retention-and-the-risks)
18. [OPTIMIZE, optimized writes and auto compaction](#18-optimize-optimized-writes-and-auto-compaction)
19. [Z-ORDER](#19-z-order)
20. [Predictive optimization](#20-predictive-optimization)
21. [MERGE INTO for upserts and its performance](#21-merge-into-for-upserts-and-its-performance)
22. [Change Data Feed](#22-change-data-feed)
23. [Unity Catalog securables, privileges and inheritance](#23-unity-catalog-securables-privileges-and-inheritance)
24. [Lineage and system tables](#24-lineage-and-system-tables)
25. [Pipeline expectations for data quality](#25-pipeline-expectations-for-data-quality)
26. [Triggered vs continuous pipelines; development vs production mode](#26-triggered-vs-continuous-pipelines-development-vs-production-mode)
27. [Auto Loader: directory listing vs file notification](#27-auto-loader-directory-listing-vs-file-notification)
28. [COPY INTO vs Auto Loader](#28-copy-into-vs-auto-loader)
29. [Job parameters, task values and conditional tasks](#29-job-parameters-task-values-and-conditional-tasks)
30. [Cluster sizing, autoscaling and spot instances](#30-cluster-sizing-autoscaling-and-spot-instances)
31. [Compute policies and instance pools](#31-compute-policies-and-instance-pools)
32. [SQL warehouses: classic vs pro vs serverless](#32-sql-warehouses-classic-vs-pro-vs-serverless)
33. [Structured Streaming with Delta as source and sink](#33-structured-streaming-with-delta-as-source-and-sink)
34. [Declarative Automation Bundles (formerly Databricks Asset Bundles) for CI/CD](#34-declarative-automation-bundles-formerly-databricks-asset-bundles-for-cicd)
35. [Controlling DBU cost on Databricks](#35-controlling-dbu-cost-on-databricks)
36. [Shallow clone vs deep clone](#36-shallow-clone-vs-deep-clone)
37. [Identity columns and constraints in Delta tables](#37-identity-columns-and-constraints-in-delta-tables)
38. [Serverless compute for jobs and notebooks](#38-serverless-compute-for-jobs-and-notebooks)
39. [Optimistic concurrency control and write conflicts in Delta](#39-optimistic-concurrency-control-and-write-conflicts-in-delta)
40. [Liquid Clustering vs partitioning vs Z-ORDER](#40-liquid-clustering-vs-partitioning-vs-z-order)
41. [Deletion vectors](#41-deletion-vectors)
42. [AUTO CDC (formerly APPLY CHANGES) for SCD Type 1/2 in pipelines](#42-auto-cdc-formerly-apply-changes-for-scd-type-12-in-pipelines)
43. [Row filters and column masks in Unity Catalog](#43-row-filters-and-column-masks-in-unity-catalog)
44. [Auto Loader schema inference, evolution and rescued data](#44-auto-loader-schema-inference-evolution-and-rescued-data)
45. [Monitoring spend with system.billing tables](#45-monitoring-spend-with-systembilling-tables)
46. [Lakehouse Federation](#46-lakehouse-federation)
47. [Preventing small files from streaming and frequent writes](#47-preventing-small-files-from-streaming-and-frequent-writes)
48. [Lakeflow Connect managed ingestion](#48-lakeflow-connect-managed-ingestion)
49. [Databricks vs Snowflake: how do you choose?](#49-databricks-vs-snowflake-how-do-you-choose)
50. [Troubleshooting a slow Databricks job end to end](#50-troubleshooting-a-slow-databricks-job-end-to-end)

</details>

## Beginner Databricks interview questions (freshers)

### 1. Databricks architecture: control plane vs compute plane (classic and serverless)

<p class="qb-meta">Beginner · Very Common · Lakehouse Architecture</p>

<p class="qb-label">Short answer</p>

The control plane, run by Databricks, hosts the web app, notebooks, job scheduler, cluster manager, Unity Catalog services and APIs. The compute plane is where data is processed. In the classic model, clusters run in your own cloud account and VPC/VNet. In the serverless model, compute runs in Databricks' account, isolated per workspace. Data normally stays in your cloud object storage.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Control plane (Databricks-managed)**
- Workspace UI, notebooks and code repositories, the Lakeflow Jobs scheduler, cluster lifecycle management, query history, and the Unity Catalog metastore service.
- It holds metadata and configuration, not your table data. Notebook results and some workspace files are stored in workspace storage.

**Compute plane**
| Mode | Where compute runs | You manage | Good for |
|---|---|---|---|
| Classic | VMs in your cloud account/network | Instance types, networking (VPC injection), cluster policies | Strict network control, custom images, GPUs, long-running custom setups |
| Serverless | Databricks-managed account, workspace-isolated | Almost nothing; choose performance/budget policies | Fast start, no cluster tuning, SQL warehouses, jobs, notebooks, pipelines |

**Storage**
- Your data lives in cloud object storage (S3, ADLS, GCS) as Delta or Iceberg tables, governed by Unity Catalog through storage credentials and external locations (see [Unity Catalog: metastore and the three-level namespace](#5-unity-catalog-metastore-and-the-three-level-namespace), [Managed vs external tables, and volumes](#6-managed-vs-external-tables-and-volumes)).
- Managed tables sit in managed storage locations you configure, at metastore, catalog or schema level.

**Networking and security considerations**
- Classic: private connectivity (PrivateLink or Private Endpoints), secure cluster connectivity (no public IPs), and egress control in your network.
- Serverless: network connectivity configurations and serverless egress controls. Private connectivity to your resources is configured from the Databricks side.

**Why interviewers ask:** to check you understand where data and code run (security reviews), why serverless removes cluster start-up time, and how costs split. Classic means a DBU bill plus your own cloud VM bill. Serverless means a DBU bill that includes the infrastructure.

<p class="qb-label">Example</p>

```text
            +--------------------- Databricks account -----------------------+
            |  Control plane: Web UI, Jobs scheduler, Cluster mgr, Unity Catalog |
            |  Serverless compute plane (SQL warehouses, serverless jobs)        |
            +-------------------------------|-----------------------------------+
                                            | secure connection
            +--------------------- Your cloud account ------------------------+
            |  Classic compute plane (clusters in your VPC/VNet)                |
            |  Object storage: s3://acme-lakehouse/  (Delta/Iceberg tables)     |
            +-------------------------------------------------------------------+
```

<p class="qb-label">Real-world use case</p>

A bank keeps classic compute for jobs that must reach on-prem systems over private links. It moved BI to serverless SQL warehouses, which start in seconds, with Unity Catalog governing both.

<p class="qb-label">Common mistakes and red flags</p>

- Saying Databricks stores your tables in its own account by default.
- Not knowing serverless exists or how it changes billing.
- Ignoring network design in regulated environments.

<p class="qb-label">Follow-up questions</p>

- What is a workspace? — A deployment environment with its own users, notebooks, jobs and compute, attached to a Unity Catalog metastore.
- How many metastores per region? — Typically one Unity Catalog metastore per region per account, shared by workspaces.

</details>

### 2. What is Delta Lake, and how is it different from plain Parquet?

<p class="qb-meta">Beginner · Very Common · Delta Lake</p>

<p class="qb-label">Short answer</p>

Delta Lake is an open-source table format that stores data as Parquet files plus a transaction log (`_delta_log`). The log adds ACID transactions, schema enforcement and evolution, time travel, MERGE/UPDATE/DELETE, and file-level statistics for data skipping. Plain Parquet is just files, with no transactions or consistent table versioning.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

| Capability | Plain Parquet folder | Delta Lake table |
|---|---|---|
| Atomic writes | No (partial files visible on failure) | Yes (commit via log) |
| Concurrent writers | Unsafe | Optimistic concurrency control (see [Optimistic concurrency control and write conflicts in Delta](#39-optimistic-concurrency-control-and-write-conflicts-in-delta)) |
| UPDATE / DELETE / MERGE | Rewrite folders manually | Native SQL/API |
| Schema enforcement | None | Rejects mismatched writes by default (see [Schema enforcement vs schema evolution (mergeSchema)](#4-schema-enforcement-vs-schema-evolution-mergeschema)) |
| Time travel | No | `VERSION AS OF` / `TIMESTAMP AS OF` (see [Delta time travel and RESTORE](#3-delta-time-travel-and-restore)) |
| Data skipping | Parquet footers only | Per-file stats in the log (min/max/null counts) |
| Streaming source/sink | Limited | First-class (see [Structured Streaming with Delta as source and sink](#33-structured-streaming-with-delta-as-source-and-sink)) |
| Change feed | No | Change Data Feed (see [Change Data Feed](#22-change-data-feed)) |
| Layout optimisation | Manual | OPTIMIZE, Z-ORDER, Liquid Clustering (see [OPTIMIZE, optimized writes and auto compaction](#18-optimize-optimized-writes-and-auto-compaction)–[Liquid Clustering vs partitioning vs Z-ORDER](#40-liquid-clustering-vs-partitioning-vs-z-order)) |

**Internals in brief**
- Data files: ordinary Parquet.
- `_delta_log/`: JSON commit files listing added and removed files plus metadata, with periodic checkpoints (see [The Delta transaction log: JSON commits and checkpoints](#16-the-delta-transaction-log-json-commits-and-checkpoints)).
- Readers build the current file list from the log, then read only those Parquet files.

**Openness**
- Delta Lake is a Linux Foundation project, readable by Spark, Trino, Flink, DuckDB, Snowflake (through Iceberg or direct support) and more.
- **UniForm** writes Iceberg (and Hudi) compatible metadata, so Iceberg readers can query Delta tables (see [Delta Lake vs Apache Iceberg vs Apache Hudi](/career/data-engineering-interview-questions/#89-delta-lake-vs-apache-iceberg-vs-apache-hudi)).
- On Databricks, Delta is the default format for tables created with `CREATE TABLE`.

**Converting:** `CONVERT TO DELTA parquet.\`path\`` creates a log over existing Parquet files without rewriting them.

<p class="qb-label">Example</p>

```sql
-- Dialect: Databricks SQL
CREATE TABLE main.sales.orders (
  order_id BIGINT, customer_id BIGINT, amount DECIMAL(12,2), order_ts TIMESTAMP
) USING DELTA;

UPDATE main.sales.orders SET amount = amount * 0.9 WHERE order_id = 1001;
DESCRIBE HISTORY main.sales.orders;          -- each change is a versioned commit
CONVERT TO DELTA parquet.`s3://acme-lake/legacy/clicks/`;
```

<p class="qb-label">Real-world use case</p>

A team's nightly Parquet overwrite occasionally left half-written folders that broke morning reports. Converting to Delta made overwrites atomic. Readers see the old version until the new commit lands.

<p class="qb-label">Common mistakes and red flags</p>

- Saying Delta is a different file format from Parquet.
- Thinking Delta works only on Databricks.
- Deleting files directly in the storage folder of a Delta table.

<p class="qb-label">Follow-up questions</p>

- What happens if you delete a Parquet file manually? — The log still references it; reads fail until you repair (e.g. `FSCK REPAIR TABLE`) — never touch files directly.
- Delta vs Iceberg? — Comparable features; different metadata designs and ecosystems (see [Delta Lake vs Apache Iceberg vs Apache Hudi](/career/data-engineering-interview-questions/#89-delta-lake-vs-apache-iceberg-vs-apache-hudi)).

**In depth:** [What problems does Delta Lake solve?](/interview/delta-lake/what-delta-lake-solves/)

</details>

### 3. Delta time travel and RESTORE

<p class="qb-meta">Beginner · Very Common · Delta Lake</p>

<p class="qb-label">Short answer</p>

Every Delta commit creates a table version. You can query older versions with `VERSION AS OF n` or `TIMESTAMP AS OF '...'`, and roll the table back with `RESTORE TABLE ... TO VERSION AS OF n`. Time travel works only while both the log entries and the referenced data files still exist. VACUUM and log retention bound how far back you can go.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Querying history**
- SQL: `SELECT * FROM t VERSION AS OF 42`, `SELECT * FROM t TIMESTAMP AS OF '2026-03-01 08:00'`, or the `t@v42` shorthand.
- PySpark: `spark.read.option("versionAsOf", 42).table("t")` or `.option("timestampAsOf", "...")`.
- `DESCRIBE HISTORY t` lists versions, operations, users and metrics.

**RESTORE**
- `RESTORE TABLE t TO VERSION AS OF 42` (or `TIMESTAMP AS OF`) creates a **new commit** that makes the table look like version 42. History isn't erased, so a restore can itself be undone.
- It reports metrics (files removed or restored). It fails if needed data files were already vacuumed.

**Limits**
| Setting | Default | Effect |
|---|---|---|
| `delta.logRetentionDuration` | 30 days | How long log entries (versions) are kept |
| `delta.deletedFileRetentionDuration` | 7 days | Minimum age before VACUUM deletes unreferenced files |
| VACUUM run with `RETAIN n HOURS` | 168 hours default | Files older than this and no longer referenced are deleted |

With defaults, reliable time travel covers about **7 days** (the VACUUM window), even though log entries exist for 30 days.

**Uses**
- Recover from bad writes or deletes.
- Reproduce ML training data or reports exactly as of a version.
- Audit what changed (diff two versions with `EXCEPT`).
- Debug a pipeline by comparing before and after.

<p class="qb-label">Example</p>

```sql
-- Dialect: Databricks SQL
DESCRIBE HISTORY main.sales.orders;

-- What changed between versions 41 and 42?
SELECT * FROM main.sales.orders VERSION AS OF 42
EXCEPT
SELECT * FROM main.sales.orders VERSION AS OF 41;

-- Roll back an accidental DELETE
RESTORE TABLE main.sales.orders TO VERSION AS OF 41;
```

<p class="qb-label">Real-world use case</p>

A buggy job deleted all of yesterday's orders at 03:00. The on-call engineer ran `RESTORE ... TO VERSION AS OF` the version before the job's commit, verified the counts, and re-ran the fixed job. Dashboards were correct by 07:00.

<p class="qb-label">Common mistakes and red flags</p>

- Assuming 30 days of time travel after VACUUM has run with the default 7 days.
- Thinking RESTORE rewrites history.
- Using time travel as a long-term backup or archive strategy.

<p class="qb-label">Follow-up questions</p>

- Can you time travel after VACUUM? — Only to versions whose files weren't removed.
- How do you keep a long-term snapshot? — Deep clone it to a separate table (see [Shallow clone vs deep clone](#36-shallow-clone-vs-deep-clone)).

**In depth:** [What is Delta Lake time travel good for, and what are its limits?](/interview/delta-lake/time-travel-use-cases-limits/)

</details>

### 4. Schema enforcement vs schema evolution (mergeSchema)

<p class="qb-meta">Beginner · Very Common · Delta Lake</p>

<p class="qb-label">Short answer</p>

Schema enforcement means Delta rejects writes whose columns or types don't match the table schema, which protects tables from bad data. Schema evolution, enabled with `.option("mergeSchema", "true")`, `spark.databricks.delta.schema.autoMerge.enabled`, or `MERGE WITH SCHEMA EVOLUTION`, lets compatible changes such as new columns update the table schema automatically. Use evolution deliberately, mostly in bronze.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Enforcement rules (default)**
- Writes with extra columns not in the table fail.
- Incompatible types fail. A missing column in the write is allowed: it is filled with NULL if nullable.
- Column names are case-insensitive for matching, but can't differ only by case within one table.

**Evolution options**
| Method | Scope |
|---|---|
| `df.write.option("mergeSchema", "true")` | That write: add new columns, allowed type upcasts |
| `.option("overwriteSchema", "true")` with overwrite mode | Replace schema entirely (e.g. type change, dropped columns) |
| `spark.databricks.delta.schema.autoMerge.enabled = true` | Session-wide automatic evolution (including MERGE) |
| `MERGE WITH SCHEMA EVOLUTION INTO ...` | One MERGE statement |
| Auto Loader `cloudFiles.schemaEvolutionMode` | Ingestion-time evolution (see [Auto Loader schema inference, evolution and rescued data](#44-auto-loader-schema-inference-evolution-and-rescued-data)) |
| `ALTER TABLE ... ADD COLUMNS` | Explicit DDL |

**Other schema changes**
- Renaming and dropping columns need **column mapping** (`delta.columnMapping.mode = 'name'`), which raises the protocol version.
- **Type widening** (for example INT to BIGINT, or decimal precision increases) is supported as a table feature in newer runtimes.
- Schema changes are metadata commits, visible in `DESCRIBE HISTORY`.

**Layered guidance**
- **Bronze:** allow evolution (or land raw JSON in a string or VARIANT column) so ingestion doesn't break.
- **Silver and gold:** enforce strictly. Changes go through code review and contracts (see [What are data contracts, and how do you enforce them?](/career/data-engineering-interview-questions/#83-what-are-data-contracts-and-how-do-you-enforce-them)). Unexpected columns should fail loudly rather than silently appear in published tables.

<p class="qb-label">Example</p>

```python
# Runtime: PySpark 3.5 on Databricks
new_batch = spark.read.json("/Volumes/main/raw/landing/orders/2026-03-01/")

# Fails if new_batch has a column the table doesn't (enforcement)
# new_batch.write.mode("append").saveAsTable("main.bronze.orders")

# Allow new columns to be added to the bronze table
(new_batch.write
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable("main.bronze.orders"))
```

<p class="qb-label">Real-world use case</p>

A source added `promo_code`. Bronze accepted it through mergeSchema. The silver model, which selects explicit columns and enforces schema, was unaffected until an engineer intentionally added the column with tests.

<p class="qb-label">Common mistakes and red flags</p>

- Turning on session-wide autoMerge in production for all tables.
- Using `overwriteSchema` casually, which can drop columns consumers depend on.
- Not knowing that renames require column mapping.

<p class="qb-label">Follow-up questions</p>

- How do you change a column type? — Type widening if supported; otherwise rewrite with overwriteSchema or add a new column and migrate.
- What happens to old files after adding a column? — Nothing; the column reads as NULL for old rows.

**In depth:** [What is schema evolution and when is it safe?](/interview/delta-lake/schema-evolution-safety/)

</details>

### 5. Unity Catalog: metastore and the three-level namespace

<p class="qb-meta">Beginner · Very Common · Unity Catalog</p>

<p class="qb-label">Short answer</p>

Unity Catalog (UC) is Databricks' central governance layer for data and AI assets across workspaces. Objects are addressed as `catalog.schema.object` (tables, views, volumes, functions, models). A regional metastore is the top-level container attached to workspaces. UC provides access control, auditing, lineage, discovery and data sharing in one place.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Hierarchy**
```text
Metastore (per region, attached to many workspaces)
 └─ Catalog            e.g. prod, dev, sales
     └─ Schema         e.g. bronze, silver, gold
         ├─ Tables (managed / external), Views, Materialized views, Streaming tables
         ├─ Volumes (governed file storage)
         ├─ Functions (SQL/Python UDFs)
         └─ Models (MLflow registered models)
```
Plus securables outside the hierarchy: storage credentials, external locations, connections (Lakehouse Federation), shares and recipients (Delta Sharing), and service credentials.

**What UC provides**
- **Centralised access control:** ANSI-style `GRANT` on any securable, with inheritance down the hierarchy (see [Unity Catalog securables, privileges and inheritance](#23-unity-catalog-securables-privileges-and-inheritance)).
- **Fine-grained security:** row filters and column masks (see [Row filters and column masks in Unity Catalog](#43-row-filters-and-column-masks-in-unity-catalog)), plus ABAC policies driven by tags (newer).
- **Auditing:** system tables for audit logs, access and usage (see [Lineage and system tables](#24-lineage-and-system-tables)).
- **Lineage:** automatic table- and column-level lineage across notebooks, jobs, SQL and pipelines.
- **Discovery:** Catalog Explorer, comments, tags, and AI-generated descriptions.
- **Open interfaces:** UC exposes an Iceberg REST catalog API, and Unity Catalog OSS exists as an open-source project.
- **Sharing:** Delta Sharing for cross-organisation access (see [Delta Sharing](#15-delta-sharing)).

**Before UC:** each workspace had its own Hive metastore with table ACLs that didn't span workspaces. Migration tools (UCX, `SYNC` commands) help move Hive metastore tables to UC. `hive_metastore` appears as a legacy catalog in UC-enabled workspaces. In production code, always use fully qualified three-part names, so jobs behave the same regardless of the session's default catalog.

<p class="qb-label">Example</p>

```sql
-- Dialect: Databricks SQL
CREATE CATALOG IF NOT EXISTS prod;
CREATE SCHEMA IF NOT EXISTS prod.gold COMMENT 'Business-ready marts';

CREATE TABLE prod.gold.daily_revenue (
  revenue_date DATE, region STRING, revenue DECIMAL(18,2)
) COMMENT 'Daily revenue by region (net of refunds)';

GRANT USE CATALOG ON CATALOG prod TO `analysts`;
GRANT USE SCHEMA, SELECT ON SCHEMA prod.gold TO `analysts`;
```

<p class="qb-label">Real-world use case</p>

A company with 12 workspaces had duplicated, inconsistent table permissions in each Hive metastore. Moving to one UC metastore per region gave one place to grant access, with audit logs and lineage across all workspaces.

<p class="qb-label">Common mistakes and red flags</p>

- Using two-level names (`schema.table`) and relying on default catalogs in production code.
- Thinking UC only covers tables.
- Not knowing about `hive_metastore` as legacy.

<p class="qb-label">Follow-up questions</p>

- How do you isolate environments? — Separate catalogs (dev/test/prod), optionally bound to specific workspaces via workspace-catalog binding.
- What are volumes? — Governed storage for non-tabular files (see [Managed vs external tables, and volumes](#6-managed-vs-external-tables-and-volumes)).

</details>

### 6. Managed vs external tables, and volumes

<p class="qb-meta">Beginner · Very Common · Unity Catalog</p>

<p class="qb-label">Short answer</p>

Unity Catalog manages both the metadata and the data lifecycle of managed tables, which are stored in UC-managed storage. Dropping one deletes its data (after a recovery window), and it gets features such as predictive optimization. External tables register data at a path you control, and dropping them removes only metadata. Volumes are governed locations for non-tabular files such as CSVs, images, models and libraries.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

| Aspect | Managed table | External table |
|---|---|---|
| Data location | Managed storage (metastore/catalog/schema level) | Your path in an external location |
| DROP behaviour | Data deleted (managed tables recoverable with `UNDROP` within 7 days) | Metadata only; files remain |
| Formats | Delta (and managed Iceberg) | Delta, Parquet, CSV, JSON, Avro, ORC, text, Iceberg |
| Predictive optimization / auto features | Yes | No |
| Other engines writing directly to path | Not supported (go through UC) | Possible (with care) |
| Recommended | Default choice | When other systems must own the files or you need existing paths |

**Storage governance objects**
- **Storage credential:** a cloud identity (IAM role, managed identity, service account) UC uses to access storage.
- **External location:** a storage path plus a credential, with grants such as `CREATE EXTERNAL TABLE`, `READ FILES` and `WRITE FILES`.
- **Managed storage locations** can be set at catalog or schema level, so data for different domains sits in different buckets.

**Volumes**
- `CREATE VOLUME` (managed) or `CREATE EXTERNAL VOLUME ... LOCATION '...'`.
- Path format: `/Volumes/catalog/schema/volume/path/file.csv`, usable from Spark, Python, the shell and SQL `READ_FILES`.
- Governed with `READ VOLUME` and `WRITE VOLUME` grants.
- Used for landing files for Auto Loader, ML artifacts, libraries and init scripts. They replace DBFS mounts for governed access.

**DBFS mounts** are legacy and not governed by UC. Migrate them to external locations and volumes.

<p class="qb-label">Example</p>

```sql
-- Dialect: Databricks SQL
CREATE EXTERNAL LOCATION IF NOT EXISTS landing_loc
  URL 's3://acme-landing/'
  WITH (STORAGE CREDENTIAL acme_landing_cred);

CREATE EXTERNAL VOLUME main.raw.landing
  LOCATION 's3://acme-landing/vendor_files/';

CREATE TABLE main.silver.orders (order_id BIGINT, amount DECIMAL(12,2));          -- managed

CREATE TABLE main.ext.legacy_clicks USING PARQUET
  LOCATION 's3://acme-landing/legacy/clicks/';                                    -- external

SELECT * FROM READ_FILES('/Volumes/main/raw/landing/2026-03-01/', format => 'csv', header => true);
```

<p class="qb-label">Real-world use case</p>

A team moved from DBFS mounts, which had no audit trail and shared credentials, to external locations and volumes. Vendor files now land in a governed volume with per-team read grants, and Auto Loader reads from `/Volumes/...` paths.

<p class="qb-label">Common mistakes and red flags</p>

- Dropping a managed table expecting the files to remain.
- Pointing two external tables at overlapping paths.
- Still using DBFS mounts for governed data.

<p class="qb-label">Follow-up questions</p>

- Can you convert an external table to managed? — Databricks provides `ALTER TABLE ... SET MANAGED` for supported tables in newer releases; otherwise CTAS/clone into a managed table.
- Why does Databricks recommend managed tables? — Automatic optimisation, lifecycle management and better performance features.

</details>

### 7. Lakeflow Spark Declarative Pipelines (formerly DLT): streaming tables vs materialized views

<p class="qb-meta">Beginner · Very Common · Lakeflow Declarative Pipelines</p>

<p class="qb-label">Short answer</p>

Lakeflow Spark Declarative Pipelines (formerly Delta Live Tables, or DLT) let you declare datasets in SQL or Python. The framework builds the dependency graph, manages compute, retries, checkpoints and data quality expectations, and refreshes incrementally. Streaming tables process append-style data incrementally, each record once. Materialized views hold query results that are recomputed or incrementally refreshed to stay correct as sources change.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Dataset types**
| Type | Semantics | Use for |
|---|---|---|
| Streaming table | Each input row processed once (append/stream semantics); supports AUTO CDC targets | Ingestion (Auto Loader, Kafka), bronze/silver append flows, CDC targets |
| Materialized view | Result always reflects the query over current sources; incremental refresh when possible (Enzyme engine), else full recompute | Aggregations, joins, gold tables, sources with updates/deletes |
| View (temporary) | Not persisted; used within pipeline | Intermediate logic |
| Flows / sinks | Append flows into a target, or write to external sinks (Kafka, Delta) | Fan-in from multiple sources, external outputs |

**Why use pipelines**
- **Declarative:** you write SELECTs, and the system figures out order, parallelism and incremental processing.
- **Built-in quality:** expectations with warn, drop or fail actions (see [Pipeline expectations for data quality](#25-pipeline-expectations-for-data-quality)).
- **Operations:** automatic retries, an event log, lineage graph, and development vs production modes (see [Triggered vs continuous pipelines; development vs production mode](#26-triggered-vs-continuous-pipelines-development-vs-production-mode)).
- **Compute:** serverless pipelines, or classic clusters with enhanced autoscaling.
- **Open source:** Databricks contributed the declarative pipelines API to Apache Spark as **Spark Declarative Pipelines** (Spark 4.1 and later). The Databricks product is "Lakeflow Spark Declarative Pipelines", often shortened to Lakeflow pipelines.

**Naming changes to know**
- DLT became **Lakeflow Declarative Pipelines**, then **Lakeflow Spark Declarative Pipelines**.
- `APPLY CHANGES` became **AUTO CDC** (see [AUTO CDC (formerly APPLY CHANGES) for SCD Type 1/2 in pipelines](#42-auto-cdc-formerly-apply-changes-for-scd-type-12-in-pipelines)).
- The Python module `dlt` became **`pyspark.pipelines`** (imported as `dp`), with `dlt` still supported for compatibility.
- `CREATE LIVE TABLE` became `CREATE MATERIALIZED VIEW`, and `LIVE.` prefixes are no longer required.

<p class="qb-label">Example</p>

```sql
-- Dialect: Databricks SQL (Lakeflow Spark Declarative Pipelines)
CREATE OR REFRESH STREAMING TABLE bronze_orders
AS SELECT *, _metadata.file_path AS source_file
   FROM STREAM read_files('/Volumes/prod/raw/landing/orders/', format => 'json');

CREATE OR REFRESH STREAMING TABLE silver_orders (
  CONSTRAINT valid_amount EXPECT (amount >= 0) ON VIOLATION DROP ROW
)
AS SELECT CAST(order_id AS BIGINT) AS order_id, customer_id,
          CAST(amount AS DECIMAL(12,2)) AS amount, CAST(order_ts AS TIMESTAMP) AS order_ts
   FROM STREAM(bronze_orders);

CREATE OR REFRESH MATERIALIZED VIEW gold_daily_revenue
AS SELECT DATE(order_ts) AS order_date, SUM(amount) AS revenue
   FROM silver_orders GROUP BY DATE(order_ts);
```

<p class="qb-label">Real-world use case</p>

A team replaced 15 notebook jobs, each with hand-managed checkpoints and orchestration, with one pipeline of 15 datasets. Failures now show in a single event log, and quality metrics are tracked automatically per expectation.

<p class="qb-label">Common mistakes and red flags</p>

- Using a streaming table over a source that receives updates or deletes (use a materialized view, or CDF and AUTO CDC).
- Expecting arbitrary side effects inside dataset definitions.
- Not knowing the rename from DLT.

<p class="qb-label">Follow-up questions</p>

- When does a materialized view fully recompute? — When the query or source changes can't be handled incrementally; the event log shows the refresh technique.
- Can you write to tables outside the pipeline? — Through sinks/flows designed for that; datasets are owned by the pipeline.

</details>

### 8. Lakeflow Jobs (formerly Workflows): tasks, retries and repair runs

<p class="qb-meta">Beginner · Very Common · Orchestration</p>

<p class="qb-label">Short answer</p>

Lakeflow Jobs (formerly Databricks Workflows) is Databricks' built-in orchestrator. A job is a DAG of tasks (notebooks, Python scripts or wheels, SQL, dbt, pipelines, JARs, other jobs), with dependencies, schedules or triggers, retries, timeouts and notifications. If a run fails, a repair run re-executes only the failed and downstream tasks, not the whole job.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Task types:** notebook, Python script, Python wheel, JAR, Spark Submit, SQL (queries, files, dashboards and alerts), dbt, Lakeflow pipeline, run job (nested job), If/else condition, For each, and Clean Rooms notebooks.

**Triggers**
- **Scheduled** (cron, with a time zone).
- **File arrival** (new files in a UC external location or volume).
- **Table update** (when source tables change).
- **Continuous** (always rerun after completion).
- **Manual or API.**

**Compute per task**
- Job clusters (created for the run, then terminated, which is cheaper than all-purpose; see [All-purpose vs job compute vs SQL warehouse vs serverless](#9-all-purpose-vs-job-compute-vs-sql-warehouse-vs-serverless)).
- Serverless compute for jobs (see [Serverless compute for jobs and notebooks](#38-serverless-compute-for-jobs-and-notebooks)), or SQL warehouses for SQL tasks.
- Shared job clusters across tasks in the same run reduce start-up overhead.

**Reliability features**
- **Retries** per task (count, interval, retry on timeout).
- **Timeouts** and duration warning thresholds.
- **Repair run:** after fixing the cause, rerun failed tasks and their dependents with the same parameters.
- **Run if** conditions on dependencies: all succeeded, at least one succeeded, none failed, all done, at least one failed.
- **Notifications:** email, Slack, Teams, webhooks, PagerDuty on start, success, failure or duration overrun.
- **Max concurrent runs,** and queueing of runs.

**Deployment:** define jobs as code with Declarative Automation Bundles (YAML), Terraform or the REST API (see [Declarative Automation Bundles (formerly Databricks Asset Bundles) for CI/CD](#34-declarative-automation-bundles-formerly-databricks-asset-bundles-for-cicd)). Avoid hand-editing production jobs in the UI.

<p class="qb-label">Example</p>

```yaml
# Declarative Automation Bundle resource (databricks.yml excerpt)
resources:
  jobs:
    nightly_sales:
      name: nightly_sales
      schedule:
        quartz_cron_expression: "0 0 2 * * ?"
        timezone_id: "UTC"
      tasks:
        - task_key: ingest
          pipeline_task:
            pipeline_id: ${resources.pipelines.orders_pipeline.id}
        - task_key: build_marts
          depends_on: [{ task_key: ingest }]
          notebook_task:
            notebook_path: ../src/build_marts.py
          max_retries: 2
          min_retry_interval_millis: 300000
      email_notifications:
        on_failure: ["data-oncall@acme.example"]
```

<p class="qb-label">Real-world use case</p>

A 12-task nightly job failed at task 9 because of a missing upstream file. After the file arrived, the on-call engineer ran a repair run, and only tasks 9–12 re-executed, saving about 2 hours of recompute.

<p class="qb-label">Common mistakes and red flags</p>

- Running production jobs on all-purpose clusters.
- Rerunning the whole job instead of repairing.
- Hardcoding environment values instead of job parameters.

<p class="qb-label">Follow-up questions</p>

- How do tasks share small values? — Task values (`dbutils.jobs.taskValues.set/get`) (see [Job parameters, task values and conditional tasks](#29-job-parameters-task-values-and-conditional-tasks)).
- Lakeflow Jobs vs Airflow? — Native, low-ops for Databricks work; Airflow for broader cross-system orchestration (see [Airflow vs Dagster vs Prefect vs cloud-native orchestrators](/career/data-engineering-interview-questions/#51-airflow-vs-dagster-vs-prefect-vs-cloud-native-orchestrators)).

</details>

### 9. All-purpose vs job compute vs SQL warehouse vs serverless

<p class="qb-meta">Beginner · Very Common · Compute</p>

<p class="qb-label">Short answer</p>

All-purpose (interactive) compute is for notebooks and exploration, shared and long-lived, with a higher DBU rate. Job compute is created for a job run and terminated afterwards, at a lower DBU rate, and is the default for production. SQL warehouses are optimised for SQL and BI with Photon (classic, pro or serverless). Serverless compute for notebooks, jobs and pipelines starts in seconds and is managed entirely by Databricks.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

| Compute | Lifecycle | Workloads | Pricing notes | Strengths |
|---|---|---|---|---|
| All-purpose cluster | Manually started; auto-terminate after idle | Interactive notebooks, development | Highest DBU rate | Flexible, shared, libraries |
| Job compute (job cluster) | Per job run | Production notebooks, scripts, wheels | Lower DBU rate (Jobs Compute SKU) | Isolation, cheaper, reproducible |
| SQL warehouse | Starts on query; auto-stop | SQL, BI dashboards, dbt SQL | Classic/Pro/Serverless SKUs | Photon, query queuing, scaling for concurrency |
| Serverless notebooks/jobs | Seconds start, managed | Notebooks, jobs, pipelines | Serverless SKUs (include infra) | No cluster config, fast start |
| Lakeflow pipeline compute | Per pipeline update | Declarative pipelines | Pipeline SKUs (Core/Pro/Advanced) | Enhanced autoscaling |

**Access modes (classic compute with Unity Catalog)**
- **Standard** (formerly "shared"): multiple users with isolation, supporting fine-grained access controls.
- **Dedicated** (formerly "single user"): one user or group, full API support (for example RDD and some ML libraries).

**Choosing**
- Development: all-purpose or serverless notebooks, with auto-termination of 30–60 minutes.
- Production Spark jobs: job compute or serverless jobs. Never all-purpose (higher cost, noisy neighbours).
- BI and dbt on SQL: a SQL warehouse (serverless where available).
- Streaming 24/7: job compute sized for steady load, or pipelines with enhanced autoscaling.

**Cost:** DBU rates differ by SKU, edition or tier, and cloud. Classic compute also incurs your cloud VM costs, while serverless includes infrastructure in its DBU price (see [Controlling DBU cost on Databricks](#35-controlling-dbu-cost-on-databricks)).

<p class="qb-label">Example</p>

```json
{
  "job_clusters": [{
    "job_cluster_key": "etl_cluster",
    "new_cluster": {
      "spark_version": "15.4.x-scala2.12",
      "node_type_id": "i3.xlarge",
      "autoscale": { "min_workers": 2, "max_workers": 8 },
      "data_security_mode": "SINGLE_USER",
      "custom_tags": { "team": "sales-data", "env": "prod" }
    }
  }]
}
```
`data_security_mode` values in the API still use the older names (`SINGLE_USER` for dedicated, `USER_ISOLATION` for standard).

<p class="qb-label">Real-world use case</p>

A cost review found 60% of DBUs came from production jobs scheduled on an always-on all-purpose cluster. Moving them to job clusters and serverless jobs cut compute spend by about 40%.

<p class="qb-label">Common mistakes and red flags</p>

- Scheduling production on all-purpose clusters.
- No auto-termination on interactive clusters.
- Using a general cluster for BI instead of a SQL warehouse.

<p class="qb-label">Follow-up questions</p>

- What is a DBU? — Databricks Unit, a normalised unit of processing capability per hour, billed at SKU-specific rates.
- When choose dedicated access mode? — When you need features unsupported in standard mode (some ML libs, RDD APIs, GPU workloads).

</details>

### 10. What is Photon?

<p class="qb-meta">Beginner · Common · Performance</p>

<p class="qb-label">Short answer</p>

Photon is Databricks' native, vectorised query engine, written in C++. It replaces parts of Spark's JVM execution for SQL and DataFrame operations (scans, filters, joins, aggregations, writes, MERGE) and can make them much faster. It is on by default for SQL warehouses and serverless, and can be enabled on clusters (Photon-enabled compute has a higher DBU rate). Unsupported operations fall back to standard Spark transparently.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**How it works**
- Processes data in **columnar batches** with SIMD-friendly vectorised loops instead of row-at-a-time Java code.
- Integrates with Spark's Catalyst optimizer: the plan is built as usual, and supported operators run in Photon (they appear as `Photon...` nodes in the plan or query profile).
- **Transparent fallback:** unsupported expressions, Python UDFs and RDD operations run in regular Spark, possibly with transitions between engines that cost some performance.

**Where it helps most**
- SQL and DataFrame workloads with heavy scans, joins and aggregations over Delta or Parquet.
- Writes, MERGE, UPDATE and DELETE on Delta tables.
- High-cardinality GROUP BY, wide tables, and string-heavy processing.

**Where it helps less**
- Python UDFs, pandas UDFs and RDD code (not accelerated).
- Small or short queries dominated by overhead.
- Workloads dominated by external I/O or ML training.

**Cost considerations**
- Photon compute uses a higher DBU rate on classic clusters. Benefit comes when the speed-up exceeds the rate premium, which is common for SQL-heavy ETL. Benchmark representative jobs.
- On SQL warehouses and serverless it is included and usually can't be turned off.

**Checking usage:** in the query profile or Spark UI, look for Photon operators. Notes that an operator is "not supported by Photon" identify fallbacks to fix, for example by replacing a Python UDF with a built-in function.

<p class="qb-label">Example</p>

```json
{
  "cluster_name": "etl-photon",
  "spark_version": "15.4.x-scala2.12",
  "runtime_engine": "PHOTON",
  "node_type_id": "r6id.2xlarge",
  "autoscale": { "min_workers": 2, "max_workers": 8 }
}
```

<p class="qb-label">Real-world use case</p>

A nightly MERGE-heavy job took 70 minutes on standard runtime. With Photon it took 25 minutes. Despite the higher DBU rate, total cost dropped by about 35% because the cluster ran far fewer hours.

<p class="qb-label">Common mistakes and red flags</p>

- Expecting Photon to speed up Python UDFs.
- Enabling it without measuring the cost and benefit.
- Not checking for operator fallbacks.

<p class="qb-label">Follow-up questions</p>

- Does Photon change results? — No; it's semantically compatible with Spark SQL.
- Is Photon open source? — No; it's proprietary to Databricks (open alternatives like Velox/Gluten exist for Spark).

</details>

### 11. Databricks Runtime versions and LTS

<p class="qb-meta">Beginner · Occasional · Compute</p>

<p class="qb-label">Short answer</p>

The Databricks Runtime (DBR) is the versioned software image for clusters. It bundles a specific Apache Spark version, Delta Lake, Photon, Python and Scala versions, and preinstalled libraries. LTS (Long-Term Support) releases receive about 3 years of support and are recommended for production. ML runtimes add ML libraries. Serverless compute uses an automatically updated, versionless environment.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Runtime flavours**
- **Standard DBR:** Spark, Delta, Python, Scala, R and common libraries.
- **DBR ML:** adds TensorFlow, PyTorch, XGBoost, MLflow and GPU variants.
- **Photon:** selected through the runtime engine setting on supported versions.
- Version names look like `15.4.x-scala2.12`: major.minor, with patch updates automatic.

**Support model**
- **LTS** versions, roughly one or two per year, have a long support window, about 3 years from release. Use them for production stability.
- Non-LTS versions have short support windows and bring new features faster.
- Release notes list the Spark version (for example DBR 16.x with Spark 3.5.x, and DBR 17.x with Spark 4.0), library versions and behaviour changes.

**Upgrade practice**
1. Read the release notes for breaking changes: Spark behaviour, ANSI mode, Python version, removed libraries.
2. Test jobs on the new runtime in a staging environment, comparing results (data diffs) and performance.
3. Pin versions in job definitions or bundles, and upgrade deliberately per job rather than all at once.
4. Track end-of-support dates. Policies can block deprecated versions (see [Compute policies and instance pools](#31-compute-policies-and-instance-pools)).

**Serverless**
- Jobs and notebooks on serverless run on a Databricks-managed, continuously updated environment, selected through an **environment version** for client libraries. Databricks handles runtime upgrades with compatibility testing.

<p class="qb-label">Example</p>

```python
# Runtime: Databricks notebook (Python) — check versions in use
import sys
print("Spark:", spark.version)
print("Python:", sys.version.split()[0])
print("DBR:", spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion", "n/a"))
```

<p class="qb-label">Real-world use case</p>

A team upgraded 80 jobs from an end-of-support runtime by running each job's critical outputs on both old and new LTS runtimes in staging. Diffing the outputs found one changed behaviour (stricter ANSI casting) in two jobs before cutover.

<p class="qb-label">Common mistakes and red flags</p>

- Running production on non-LTS runtimes without a reason.
- Upgrading all jobs at once without testing.
- Ignoring end-of-support dates.

<p class="qb-label">Follow-up questions</p>

- What changes with Spark 4.0 runtimes? — ANSI SQL mode on by default and other behaviour changes (see [What's new in Spark 4.0 for data engineers?](/pyspark/pyspark-interview-questions/#50-whats-new-in-spark-40-for-data-engineers)).
- How do you install extra libraries? — Cluster libraries, notebook-scoped `%pip`, or job environment specs; prefer pinned versions.

</details>

### 12. Implementing medallion architecture on Databricks (See also Q32)

<p class="qb-meta">Beginner · Very Common · Lakehouse Architecture</p>

<p class="qb-label">Short answer</p>

On Databricks, map bronze, silver and gold to Unity Catalog catalogs or schemas. Ingest raw data into bronze streaming tables with Auto Loader or Lakeflow Connect. Clean, deduplicate and apply CDC into silver (expectations, AUTO CDC). Build gold materialized views and aggregates for BI and ML. Orchestrate with Lakeflow pipelines and Jobs, and govern access per layer with UC grants.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Layout in Unity Catalog** (one common pattern)
```text
prod (catalog)
 ├─ bronze  (schema) : raw streaming tables, append-only, ingestion metadata
 ├─ silver  (schema) : cleaned, conformed, deduplicated, SCD tables
 └─ gold    (schema) : marts, aggregates, feature tables
dev / test catalogs mirror the structure; workspace bindings restrict prod access
```
Some organisations use a catalog per layer or per domain (`sales_bronze`, `sales_gold`) instead. Choose based on grant boundaries and ownership.

**Implementation choices per layer**
| Layer | Typical Databricks building blocks |
|---|---|
| Bronze | Auto Loader / `read_files` streaming tables, Lakeflow Connect connectors, Kafka sources; keep `_metadata.file_path`, ingestion timestamp; schema evolution + rescued data |
| Silver | Streaming tables with expectations; AUTO CDC for SCD1/2; MERGE in jobs; CDF enabled for downstream incremental |
| Gold | Materialized views (incremental refresh), Delta tables built by dbt or jobs, metric views; liquid clustering on filter columns |
| Consumption | SQL warehouses for BI, AI/BI dashboards and Genie, Delta Sharing, feature serving |

**Cross-cutting concerns**
- **Quality:** expectations, plus dbt tests or Lakehouse Monitoring on gold.
- **Governance:** restrict bronze (raw PII) to engineers, and apply masks and row filters on silver and gold (see [Row filters and column masks in Unity Catalog](#43-row-filters-and-column-masks-in-unity-catalog)).
- **Optimisation:** predictive optimization on managed tables (see [Predictive optimization](#20-predictive-optimization)).
- **CI/CD:** Declarative Automation Bundles deploy pipelines and jobs per environment (see [Declarative Automation Bundles (formerly Databricks Asset Bundles) for CI/CD](#34-declarative-automation-bundles-formerly-databricks-asset-bundles-for-cicd)).
- **Lineage:** automatic through UC (see [Lineage and system tables](#24-lineage-and-system-tables)).

<p class="qb-label">Example</p>

```sql
-- Dialect: Databricks SQL (Lakeflow pipeline excerpt, gold layer)
CREATE OR REFRESH MATERIALIZED VIEW prod.gold.customer_ltv
CLUSTER BY (segment)
AS
SELECT c.customer_id, c.segment, c.country,
       COUNT(o.order_id)      AS lifetime_orders,
       SUM(o.amount)          AS lifetime_value,
       MAX(o.order_ts)        AS last_order_ts
FROM prod.silver.customers c
LEFT JOIN prod.silver.orders o ON o.customer_id = c.customer_id
GROUP BY c.customer_id, c.segment, c.country;
```

<p class="qb-label">Real-world use case</p>

A retailer standardised all domains on `prod.{bronze,silver,gold}` schemas with identical dev and test catalogs. New domains copy a bundle template for pipeline, jobs and grants, so onboarding a new source takes days instead of weeks.

<p class="qb-label">Common mistakes and red flags</p>

- Business logic in bronze.
- Analysts reading bronze directly.
- One giant pipeline for every domain, so a single failure blocks everything.

<p class="qb-label">Follow-up questions</p>

- Catalog per environment or per layer? — Commonly per environment, with schemas per layer; per-domain catalogs suit data-mesh ownership.
- Where does dbt fit? — Silver→gold SQL transformations on SQL warehouses, often alongside pipelines for ingestion.

</details>

### 13. Notebooks, Git folders, dbutils and widgets

<p class="qb-meta">Beginner · Common · Development</p>

<p class="qb-label">Short answer</p>

Databricks notebooks mix Python, SQL, Scala and R cells (with `%sql` and other magic commands), and support collaboration, scheduling and visualisations. Git folders (formerly Repos) sync notebooks and files with Git providers for version control. `dbutils` offers utilities for files, secrets, widgets, notebook chaining and job task values. Widgets parameterise notebooks for jobs and interactive use.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Notebooks**
- Default language per notebook, overridden per cell with `%python`, `%sql`, `%scala` or `%r`. `%pip install` installs notebook-scoped libraries. `%run ./other_notebook` includes another notebook's definitions.
- Saved in source format (`.py` with `# COMMAND ----------` separators) or `.ipynb` (supported, and the default for new notebooks in newer workspaces).
- Debugger, variable explorer and AI assistant features exist in current versions.

**Git folders (formerly Repos)**
- Clone a repository into the workspace, then branch, commit, pull and push through the UI or API.
- Production jobs should run from a **Git reference** (job source = Git) or from bundle deployments, not from personal workspace copies.
- Keep reusable logic in `.py` modules (importable) rather than duplicating it across notebooks.

**dbutils highlights**
| Module | Use |
|---|---|
| `dbutils.fs` | List/copy/move files (`/Volumes/...`, cloud paths) |
| `dbutils.secrets` | Read secrets from secret scopes (values redacted in output; see [Secret scopes and secrets management](#14-secret-scopes-and-secrets-management)) |
| `dbutils.widgets` | Define/read parameters (`text`, `dropdown`, `combobox`, `multiselect`) |
| `dbutils.notebook.run` / `exit` | Call notebooks with parameters and return values |
| `dbutils.jobs.taskValues` | Pass small values between job tasks (see [Job parameters, task values and conditional tasks](#29-job-parameters-task-values-and-conditional-tasks)) |

**Good practice for production**
- Notebooks are fine as entry points, but put core logic in tested Python modules or packages (wheels), with unit tests run in CI (see [The testing pyramid for data pipelines](/career/data-engineering-interview-questions/#67-the-testing-pyramid-for-data-pipelines)).
- Parameterise with widgets or job parameters, not hardcoded dates or paths.
- Avoid long chains of `%run`. Use proper imports.

<p class="qb-label">Example</p>

```python
# Runtime: Databricks notebook (Python)
dbutils.widgets.text("run_date", "2026-03-01", "Run date (YYYY-MM-DD)")
dbutils.widgets.dropdown("env", "dev", ["dev", "test", "prod"], "Environment")

from datetime import date
run_date = date.fromisoformat(dbutils.widgets.get("run_date"))   # validates format
env = dbutils.widgets.get("env")

display(dbutils.fs.ls(f"/Volumes/{env}/raw/landing/orders/{run_date.isoformat()}/"))
```

<p class="qb-label">Real-world use case</p>

A team had 30 notebooks copy-pasting the same cleaning functions. They moved the functions into a Python package in a Git folder with pytest tests. Notebooks import it, and a CI pipeline runs the tests on every pull request.

<p class="qb-label">Common mistakes and red flags</p>

- Running production jobs from personal notebook copies.
- Hardcoded parameters.
- All business logic in untested notebook cells.

<p class="qb-label">Follow-up questions</p>

- `%run` vs `dbutils.notebook.run`? — `%run` inlines another notebook's code in the same context; `dbutils.notebook.run` runs it as a separate ephemeral job with parameters and a return value.
- How do you test notebook code? — Move logic into modules and test with pytest locally or in CI (Databricks Connect helps run against a cluster).

</details>

### 14. Secret scopes and secrets management

<p class="qb-meta">Beginner · Common · Security</p>

<p class="qb-label">Short answer</p>

Secret scopes store credentials (API keys, database passwords) either in Databricks itself or backed by Azure Key Vault. Code reads them with `dbutils.secrets.get(scope, key)`, and their values are redacted as `[REDACTED]` in notebook output. Access is controlled with ACLs per scope. Prefer Unity Catalog storage credentials, service principals and service credentials over stored secrets wherever possible.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Scope types**
- **Databricks-backed:** secrets are stored encrypted by Databricks. Created and managed with the CLI or API.
- **Azure Key Vault-backed:** the scope points to a Key Vault, and secrets are managed there.

**Usage**
- `dbutils.secrets.get(scope="vendor-api", key="token")`.
- In cluster or Spark configuration: `{{secrets/scope/key}}` references.
- `dbutils.secrets.list(scope)` shows key names, not values.

**Access control**
- Scope ACLs grant MANAGE, WRITE or READ to users, groups or service principals.
- Workspace admins can manage all scopes. Restrict creation and reading to the right groups.

**Redaction is a guardrail, not a guarantee.** Printing a secret shows `[REDACTED]`, but code can still exfiltrate it (for example by writing it to a table or splitting the characters). Limit who can run code with access to sensitive scopes, and audit usage.

**Prefer identity over secrets** (see [How do you manage secrets in pipelines?](/career/data-engineering-interview-questions/#28-how-do-you-manage-secrets-in-pipelines))
- Storage access: **UC storage credentials and external locations** (no keys in code).
- Calling cloud services: **UC service credentials** (governed cloud identities for services).
- Database connections: **Lakehouse Federation connections** (credentials stored as a UC connection; see [Lakehouse Federation](#46-lakehouse-federation)).
- Job identity: **service principals** with OAuth M2M, not personal access tokens.

<p class="qb-label">Example</p>

```python
# Runtime: Databricks notebook (Python) — call a partner API with a stored token
import requests

token = dbutils.secrets.get(scope="partner-api", key="bearer-token")
resp = requests.get(
    "https://api.partner.example/v1/orders",
    headers={"Authorization": f"Bearer {token}"},
    params={"updated_since": "2026-03-01T00:00:00Z"},
    timeout=30,
)
resp.raise_for_status()
print(len(resp.json().get("data", [])))   # never print the token itself
```

```bash
# Databricks CLI: create scope and secret
databricks secrets create-scope partner-api
databricks secrets put-secret partner-api bearer-token --string-value "<value from vault>"
```

<p class="qb-label">Real-world use case</p>

An audit found a database password in plain text in 14 notebooks. The team moved it to a secret scope readable only by the ETL service principal, rotated the password, and later replaced the JDBC pattern with a Lakehouse Federation connection.

<p class="qb-label">Common mistakes and red flags</p>

- Hardcoding credentials in notebooks or job parameters.
- Believing redaction makes secrets fully safe.
- Using personal access tokens for production jobs.

<p class="qb-label">Follow-up questions</p>

- How do you rotate secrets? — Update the secret value in the scope or Key Vault; jobs read the new value at runtime.
- What are UC service credentials? — UC-governed cloud identities used by code to call external cloud services without stored keys.

</details>

### 15. Delta Sharing

<p class="qb-meta">Beginner · Common · Data Sharing</p>

<p class="qb-label">Short answer</p>

Delta Sharing is an open protocol for securely sharing live tables (and views, volumes, notebooks and models) without copying them. In Databricks-to-Databricks sharing, the recipient mounts the share as a catalog governed by their own Unity Catalog. In open sharing, any client (pandas, Spark, Power BI and others) reads with a credential file. The provider controls access, and data stays in the provider's storage.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Objects (Unity Catalog)**
- **Share:** a collection of tables, views, volumes, models and notebooks. You can share specific partitions or table history (for time travel or CDF on the recipient side).
- **Recipient:** an entity receiving shares, either Databricks-to-Databricks (identified by a metastore sharing identifier) or open (token-based, or OIDC federation).
- **Provider:** from the recipient's perspective, who shares with them.

**Modes**
| Mode | Recipient | Auth | Governance on recipient side |
|---|---|---|---|
| Databricks-to-Databricks | Another UC metastore (any cloud/region) | Built-in | Mount as catalog; grant in their UC |
| Open sharing | Any client implementing protocol | Bearer token (credential file) or OIDC federation | Client-side |

**Features**
- Live data: recipients see updates as the provider commits.
- **History sharing** enables time travel and streaming reads (`readStream`) on recipient side, including CDF if enabled.
- Views shared through Delta Sharing let you filter rows and columns. Row filters and masks have specific behaviours, so test them.
- Egress cost: cross-region or cross-cloud reads incur egress from the provider's storage. Consider replication or caching for heavy cross-region use.
- Audit: provider-side audit logs in `system.access.audit` record recipient access.
- **Marketplace** and **Clean Rooms** build on Delta Sharing.

<p class="qb-label">Example</p>

```sql
-- Dialect: Databricks SQL (provider)
CREATE SHARE IF NOT EXISTS partner_sales;
ALTER SHARE partner_sales ADD TABLE prod.gold.partner_daily_sales WITH HISTORY;
CREATE RECIPIENT IF NOT EXISTS retailer_x USING ID 'aws:us-west-2:<metastore-uuid>';
GRANT SELECT ON SHARE partner_sales TO RECIPIENT retailer_x;

-- Dialect: Databricks SQL (recipient)
CREATE CATALOG partner_data USING SHARE acme_provider.partner_sales;
SELECT * FROM partner_data.gold.partner_daily_sales WHERE sale_date = current_date() - 1;
```

<p class="qb-label">Real-world use case</p>

A manufacturer shares daily sell-through tables with 15 retailers. Some are on Databricks and get catalog mounts. Others use open sharing from pandas and Power BI. This replaced SFTP CSV drops and nightly reconciliation work.

<p class="qb-label">Common mistakes and red flags</p>

- Ignoring cross-cloud egress costs.
- Sharing raw tables with PII instead of curated views.
- Thinking recipients get a copy.

<p class="qb-label">Follow-up questions</p>

- How does it compare to Snowflake Secure Data Sharing? — Similar live, no-copy idea; Delta Sharing is an open protocol readable by non-Databricks clients (see [Secure Data Sharing and reader accounts](/snowflake/snowflake-interview-questions/#26-secure-data-sharing-and-reader-accounts)).
- How do you revoke access? — Revoke the grant or drop the recipient; access stops immediately.

</details>

## Intermediate Databricks interview questions

### 16. The Delta transaction log: JSON commits and checkpoints

<p class="qb-meta">Intermediate · Very Common · Delta Lake Internals</p>

<p class="qb-label">Short answer</p>

Every write to a Delta table creates a numbered JSON commit file in `_delta_log/`. It records actions: add file, remove file, metadata or schema change, protocol, and commit info. Periodically, Delta writes a Parquet checkpoint that summarises the table state, so readers don't replay thousands of JSON files. The current table equals the latest checkpoint plus the JSON commits after it.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Commit files**
- `00000000000000000000.json`, `...0001.json` and so on. Each one is a single atomic commit.
- **Actions** inside include:
  - `add` (path, size, partition values, statistics such as min/max/null counts, deletion vector info);
  - `remove` (logically deleted files, kept for time travel until VACUUM);
  - `metaData` (schema, partition columns, table properties);
  - `protocol` (minimum reader and writer versions, and table features);
  - `commitInfo` (operation, user, timestamp, metrics);
  - `txn` (application transaction IDs, used by streaming for idempotent writes).

**Checkpoints**
- Written by default every 10 commits (configurable). Newer **v2 checkpoints** and multi-part checkpoints scale better for huge tables.
- `_last_checkpoint` points readers to the latest checkpoint.
- Readers load the checkpoint, then apply later JSON commits. This is called **log replay**.

**Atomicity mechanism**
- A writer tries to create the next commit file (N+1). The storage layer must guarantee mutual exclusion ("put if absent"). On S3 this historically needed a coordination service, and S3 now supports conditional writes. If another writer created N+1 first, the writer checks for conflicts and retries (see [Optimistic concurrency control and write conflicts in Delta](#39-optimistic-concurrency-control-and-write-conflicts-in-delta)).

**Retention**
- Log entries are kept for `delta.logRetentionDuration` (default 30 days) and cleaned up after checkpoints.
- Removed data files are kept until `VACUUM` deletes those older than `delta.deletedFileRetentionDuration` (default 7 days; see [VACUUM: default retention and the risks](#17-vacuum-default-retention-and-the-risks)).
- Time travel needs both the log entries and the data files for the target version.

**Protocol and table features:** enabling features such as deletion vectors, column mapping or liquid clustering may raise the reader and writer protocol requirements. Older clients then can't read the table. Check compatibility before enabling.

<p class="qb-label">Example</p>

```sql
-- Dialect: Databricks SQL
DESCRIBE HISTORY main.sales.orders LIMIT 5;     -- version, timestamp, operation, metrics
DESCRIBE DETAIL main.sales.orders;              -- numFiles, sizeInBytes, location, properties

ALTER TABLE main.sales.orders SET TBLPROPERTIES (
  'delta.logRetentionDuration' = 'interval 30 days',
  'delta.deletedFileRetentionDuration' = 'interval 7 days'
);
```

<p class="qb-label">Real-world use case</p>

A streaming table that committed every few seconds had 200,000 log entries, and query planning slowed down. Checkpointing settings, plus fewer and larger commits from a longer trigger interval, restored fast table-state loading.

<p class="qb-label">Common mistakes and red flags</p>

- Thinking Delta locks the table during writes.
- Confusing log retention with data-file retention.
- Enabling new table features without checking reader compatibility.

<p class="qb-label">Follow-up questions</p>

- How does data skipping work? — Per-file min/max stats in `add` actions let the engine skip files that can't match filters.
- What is a table feature? — A named capability recorded in the protocol (e.g. deletionVectors) that readers/writers must support.

</details>

### 17. VACUUM: default retention and the risks

<p class="qb-meta">Intermediate · Very Common · Delta Lake Maintenance</p>

<p class="qb-label">Short answer</p>

VACUUM deletes data files that are no longer referenced by the current table version and are older than the retention threshold, which is 7 days (168 hours) by default. It reduces storage cost but limits time travel. Setting retention below the default can break long-running readers or streams that still use older files, so Delta blocks it unless you disable a safety check.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**What VACUUM removes**
- Files marked `remove` in the log (replaced by UPDATE, DELETE, MERGE, OPTIMIZE or overwrite) whose removal is older than the retention threshold.
- Files in the table directory that aren't tracked by the log at all (orphans from failed writes), subject to the same age rule.
- It does **not** delete log files. Log cleanup is separate and automatic.

**Settings**
- `VACUUM t` uses `delta.deletedFileRetentionDuration` (default 7 days).
- `VACUUM t RETAIN 240 HOURS` keeps 10 days.
- `VACUUM t DRY RUN` lists files that would be deleted.
- Below 7 days, Delta raises an error unless `spark.databricks.delta.retentionDurationCheck.enabled = false`. Do this only if you are sure no reader, stream or job needs older files.

**Risks of aggressive retention**
- Concurrent readers or long queries reading an older snapshot fail with missing files.
- Streaming queries that are behind can't read their next files.
- Time travel and RESTORE beyond the window become impossible (see [Delta time travel and RESTORE](#3-delta-time-travel-and-restore)).

**Operations**
- **Predictive optimization** in Unity Catalog can run VACUUM (and OPTIMIZE) automatically on managed tables (see [Predictive optimization](#20-predictive-optimization)).
- VACUUM can be expensive on huge tables because it lists files. Newer modes such as `VACUUM ... LITE` use the log instead of listing storage, in supported runtimes.
- For GDPR deletions, VACUUM is what physically removes the deleted rows' old files. Schedule it after erasure requests (see [How does GDPR "right to erasure" affect lake and warehouse design?](/career/data-engineering-interview-questions/#97-how-does-gdpr-right-to-erasure-affect-lake-and-warehouse-design)).

<p class="qb-label">Example</p>

```sql
-- Dialect: Databricks SQL
VACUUM main.sales.orders DRY RUN;          -- preview
VACUUM main.sales.orders;                  -- default 7-day retention
VACUUM main.sales.orders RETAIN 336 HOURS; -- keep 14 days of history files

ALTER TABLE main.sales.orders
SET TBLPROPERTIES ('delta.deletedFileRetentionDuration' = 'interval 14 days');
```

<p class="qb-label">Real-world use case</p>

A table rewritten nightly by MERGE accumulated 40 TB of unreferenced files because nobody ran VACUUM. A weekly VACUUM (later replaced by predictive optimization) brought storage back to the 6 TB live size plus 7 days of history.

<p class="qb-label">Common mistakes and red flags</p>

- `RETAIN 0 HOURS` in production.
- Never running VACUUM.
- Deleting files manually in the storage console.

<p class="qb-label">Follow-up questions</p>

- Does VACUUM affect performance of queries? — Not directly; it only deletes unreferenced files. OPTIMIZE affects performance.
- Why might VACUUM take hours? — Listing millions of files in object storage; use log-based modes or run more often.

**In depth:** [What does VACUUM do in Delta Lake, and what can go wrong?](/interview/delta-lake/vacuum-retention-risks/)

</details>

### 18. OPTIMIZE, optimized writes and auto compaction

<p class="qb-meta">Intermediate · Very Common · Delta Lake Maintenance</p>

<p class="qb-label">Short answer</p>

`OPTIMIZE` compacts many small files into fewer large ones (around 1 GB by default) and, with Z-ORDER or liquid clustering, also improves data layout. Optimized writes reduce the number of files at write time by shuffling data to fewer writers. Auto compaction runs a small OPTIMIZE after writes when there are many small files. Together they fix the small-files problem.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**OPTIMIZE**
- Rewrites small files into bigger ones (the target size is configurable, and tables can tune file size automatically based on size and workload).
- Idempotent: already-optimised files are skipped.
- Can target a subset: `OPTIMIZE t WHERE event_date >= current_date() - 7` (partition columns only, for partitioned tables).
- The old small files become unreferenced and are removed later by VACUUM.
- It's a commit like any other, so readers are unaffected.

**Optimized writes** (`delta.autoOptimize.optimizeWrite = true`)
- Adds an adaptive shuffle before writing, so each partition gets fewer, larger files. This costs a shuffle at write time.
- Enabled by default for some operations on recent Databricks runtimes (for example MERGE, UPDATE, DELETE and CTAS on partitioned tables). Check your runtime.

**Auto compaction** (`delta.autoOptimize.autoCompact = true`)
- After a successful write, if a partition or table has accumulated enough small files, it runs a lightweight synchronous compaction on that data.
- Great for streaming and frequent small batch writes.

**Predictive optimization** (Unity Catalog managed tables) automatically decides when to run OPTIMIZE and VACUUM based on usage (see [Predictive optimization](#20-predictive-optimization)).

**Liquid clustering:** `OPTIMIZE` on a liquid-clustered table incrementally clusters new data (see [Liquid Clustering vs partitioning vs Z-ORDER](#40-liquid-clustering-vs-partitioning-vs-z-order)).

<p class="qb-label">Example</p>

```sql
-- Dialect: Databricks SQL
ALTER TABLE main.events.page_views SET TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
);

OPTIMIZE main.events.page_views WHERE event_date >= current_date() - INTERVAL 3 DAYS;

DESCRIBE DETAIL main.events.page_views;   -- check numFiles and sizeInBytes before/after
```

<p class="qb-label">Real-world use case</p>

A streaming job writing every 30 seconds produced 2,880 files per day per partition. Enabling auto compaction and a nightly OPTIMIZE kept the average file size near 500 MB, and dashboard queries became 5 times faster.

<p class="qb-label">Common mistakes and red flags</p>

- Running OPTIMIZE on the entire 100 TB table every hour.
- Expecting OPTIMIZE to free storage immediately (VACUUM does that).
- Not knowing optimized writes add a shuffle cost.

<p class="qb-label">Follow-up questions</p>

- How often to run OPTIMIZE? — Based on write patterns; daily for recent partitions is common, or let predictive optimization decide.
- Does OPTIMIZE conflict with concurrent writes? — It can conflict with deletes/updates on the same files; appends are fine.

</details>

### 19. Z-ORDER

<p class="qb-meta">Intermediate · Very Common · Delta Lake Performance</p>

<p class="qb-label">Short answer</p>

`OPTIMIZE ... ZORDER BY (col1, col2)` rewrites files so rows with similar values in those columns are co-located, using a space-filling Z-order curve. That makes per-file min/max statistics selective, so filters on those columns skip more files. It is effective for high-cardinality filter columns, but costly to maintain because it rewrites data. Liquid clustering is now generally recommended instead for new tables.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**How it works**
- Interleaves the bits of several columns' values into one sort key (a Z-curve), so locality is preserved across multiple dimensions at once.
- After a Z-ORDER, each file covers a narrow range of the chosen columns, so data skipping works for filters on any of them, with diminishing returns as you add columns.

**Choosing columns**
- Columns frequently used in WHERE filters or join keys, with **high cardinality** (customer ID, device ID, user ID).
- Not the partition columns. Within each partition, Z-ORDER clusters by other columns.
- Usually 1–4 columns. Each extra column dilutes the effect on the others.
- Columns must have statistics collected. By default Delta collects stats on the first 32 columns, configurable with `delta.dataSkippingNumIndexedCols` or `delta.dataSkippingStatsColumns`.

**Limitations**
- **Not incremental:** new data isn't Z-ordered until you re-run OPTIMIZE ZORDER, and re-running rewrites more data than a simple compaction.
- Changing the Z-ORDER columns means a full rewrite.
- It interacts awkwardly with partitioning choices.

**Liquid clustering replaces it for new tables** on Databricks: incremental, keys can change without rewrites, and it works with row-level concurrency (see [Liquid Clustering vs partitioning vs Z-ORDER](#40-liquid-clustering-vs-partitioning-vs-z-order)). Z-ORDER remains common on existing tables and in OSS Delta.

<p class="qb-label">Example</p>

```sql
-- Dialect: Databricks SQL
OPTIMIZE main.events.page_views
WHERE event_date >= '2026-03-01'
ZORDER BY (user_id, page_id);

-- Check skipping effect: compare files read in the query profile / Spark UI
SELECT COUNT(*) FROM main.events.page_views
WHERE event_date = '2026-03-02' AND user_id = 884213;
```

<p class="qb-label">Real-world use case</p>

Support lookups by `user_id` on a date-partitioned events table read every file in a day's partition. Z-ordering recent partitions by `user_id` cut files read per lookup by over 95%. Later the table moved to liquid clustering on `(event_date, user_id)`.

<p class="qb-label">Common mistakes and red flags</p>

- Z-ordering on a partition column.
- Z-ordering on columns without collected statistics.
- Running ZORDER on the whole table daily.

<p class="qb-label">Follow-up questions</p>

- Z-ORDER vs sorting by one column? — Linear sort helps only the first column; Z-order balances locality across several.
- Can you Z-ORDER and liquid cluster the same table? — No; liquid clustering is incompatible with ZORDER and partitioning on that table.

</details>

### 20. Predictive optimization

<p class="qb-meta">Intermediate · Occasional · Delta Lake Maintenance</p>

<p class="qb-label">Short answer</p>

Predictive optimization is a Unity Catalog feature for managed tables. It automatically decides when to run maintenance (OPTIMIZE, VACUUM, statistics collection, and clustering for `CLUSTER BY AUTO`) based on table usage and how much benefit it expects. It runs on serverless compute and is billed separately, so teams don't need hand-written maintenance jobs.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**What it does**
- **OPTIMIZE:** compacts files and clusters liquid-clustered tables when write patterns warrant it.
- **VACUUM:** removes unreferenced files according to retention settings.
- **ANALYZE / statistics:** keeps statistics fresh for the query optimizer and data skipping.
- **Automatic liquid clustering key selection** for tables with `CLUSTER BY AUTO`, based on observed query predicates.

**How to enable**
- At account, catalog or schema level: `ALTER CATALOG main ENABLE PREDICTIVE OPTIMIZATION;`, with inheritance downwards and the option to disable for specific schemas.
- It applies only to **Unity Catalog managed tables**, not external tables. Databricks has been enabling it by default for newer accounts. Check your account's status.

**Observability**
- The `system.storage.predictive_optimization_operations_history` system table shows which operations ran, on which tables, and their cost and impact.
- Cost appears in billing system tables under a predictive-optimization SKU (see [Monitoring spend with system.billing tables](#45-monitoring-spend-with-systembilling-tables)).

**When to keep manual control**
- External tables (not covered).
- Strict maintenance windows, or tables where you want deterministic timing, such as running VACUUM right after GDPR deletes.
- Very large tables with specific rewrite budgets. Monitor the history and adjust.

**Why it matters in interviews:** it shows awareness that modern lakehouse operations are shifting from cron-scheduled OPTIMIZE and VACUUM scripts to platform-managed maintenance. It also shows you know managed tables gain this benefit (one reason Databricks recommends managed tables; see [Managed vs external tables, and volumes](#6-managed-vs-external-tables-and-volumes)).

<p class="qb-label">Example</p>

```sql
-- Dialect: Databricks SQL
ALTER CATALOG main ENABLE PREDICTIVE OPTIMIZATION;
ALTER SCHEMA main.scratch DISABLE PREDICTIVE OPTIMIZATION;

SELECT table_name, operation_type, operation_status, start_time, usage_quantity
FROM system.storage.predictive_optimization_operations_history
WHERE catalog_name = 'main' AND start_time >= current_date() - INTERVAL 7 DAYS
ORDER BY start_time DESC;
```

<p class="qb-label">Real-world use case</p>

A platform team deleted 120 nightly OPTIMIZE and VACUUM jobs after enabling predictive optimization on its managed catalog. Storage dropped because VACUUM now ran consistently, and query performance stayed the same or improved.

<p class="qb-label">Common mistakes and red flags</p>

- Expecting it to maintain external tables.
- Not monitoring its cost.
- Running duplicate manual maintenance on the same tables.

<p class="qb-label">Follow-up questions</p>

- Is it free? — No; it uses serverless compute billed to your account.
- Does it replace OPTIMIZE entirely? — For managed tables, largely; you can still run OPTIMIZE manually when needed.

</details>

### 21. MERGE INTO for upserts and its performance

<p class="qb-meta">Intermediate · Very Common · Delta Lake</p>

<p class="qb-label">Short answer</p>

Delta `MERGE INTO` upserts, updates and deletes rows in one atomic commit, with `WHEN MATCHED`, `WHEN NOT MATCHED` and `WHEN NOT MATCHED BY SOURCE` clauses. Its cost comes from joining the source to the target and rewriting every file that contains a matched row. Speed it up by deduplicating the source, adding partition or cluster predicates to the ON clause, clustering by the merge key, and using deletion vectors.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Syntax highlights (Databricks SQL)**
- `WHEN MATCHED [AND cond] THEN UPDATE SET ... | UPDATE SET * | DELETE`
- `WHEN NOT MATCHED [BY TARGET] [AND cond] THEN INSERT ... | INSERT *`
- `WHEN NOT MATCHED BY SOURCE [AND cond] THEN DELETE | UPDATE`: handles target rows absent from the source, which is useful for full-snapshot syncs.
- `MERGE WITH SCHEMA EVOLUTION` adds new source columns (see [Schema enforcement vs schema evolution (mergeSchema)](#4-schema-enforcement-vs-schema-evolution-mergeschema)).

**How it executes**
1. Join the source with the target on the merge condition to find the target files containing matches.
2. Rewrite those files with updated, deleted or kept rows (or write deletion vectors plus new files; see [Deletion vectors](#41-deletion-vectors)).
3. Commit.

The source must have **at most one row per target row**. Otherwise you get "multiple source rows matched", so deduplicate first (see [Delete duplicates but keep one row](/sql/sql-interview-questions/#22-delete-duplicates-but-keep-one-row)).

**Performance levers**
| Lever | Why it helps |
|---|---|
| Pruning predicates in ON (`t.event_date >= '2026-03-01'`) | Limits target files scanned |
| Liquid clustering / Z-ORDER on merge keys | Matches concentrate in fewer files |
| Deletion vectors | Avoid full file rewrites |
| Small, deduplicated source | Less join and less ambiguity |
| Broadcast small source | Avoid shuffling the target |
| Low-shuffle MERGE (default on Databricks) | Unmodified rows in touched files aren't reshuffled |
| Photon | Faster joins and writes (see [What is Photon?](#10-what-is-photon)) |

**Metrics:** `DESCRIBE HISTORY` operationMetrics show `numTargetFilesRemoved`, `numTargetRowsUpdated` and `numTargetBytesAdded`. Use them to see write amplification.

<p class="qb-label">Example</p>

```sql
-- Dialect: Databricks SQL
MERGE INTO main.silver.customers AS t
USING (
  SELECT * FROM main.bronze.customer_changes
  WHERE _ingested_at >= current_timestamp() - INTERVAL 2 HOURS
  QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY change_ts DESC) = 1
) AS s
ON t.customer_id = s.customer_id
WHEN MATCHED AND s.op = 'D' THEN DELETE
WHEN MATCHED AND s.change_ts > t.change_ts THEN UPDATE SET *
WHEN NOT MATCHED AND s.op <> 'D' THEN INSERT *;
```
`UPDATE SET *` and `INSERT *` require the source columns to match the target (here the source includes `op`, so the target must also have `op`, or you list the columns explicitly).

<p class="qb-label">Real-world use case</p>

A MERGE into a 2 TB customer table rewrote 1.8 TB every hour, because customers were randomly spread across files. Liquid clustering on `customer_id` plus deletion vectors cut rewritten bytes to about 20 GB per run.

<p class="qb-label">Common mistakes and red flags</p>

- No source dedup.
- No target pruning predicate on huge tables.
- Using `UPDATE SET *` when source and target columns differ.

<p class="qb-label">Follow-up questions</p>

- How does foreachBatch use MERGE in streaming? — Each micro-batch is deduped and merged; checkpoints make it exactly-once with idempotent logic (see [foreachBatch for upserts and multiple sinks](/pyspark/pyspark-interview-questions/#36-foreachbatch-for-upserts-and-multiple-sinks)).
- What is `WHEN NOT MATCHED BY SOURCE` used for? — Deleting or flagging target rows missing from a full source snapshot.

</details>

### 22. Change Data Feed

<p class="qb-meta">Intermediate · Common · Delta Lake CDC</p>

<p class="qb-label">Short answer</p>

Change Data Feed (CDF) records row-level changes (insert, update_preimage, update_postimage, delete) for each Delta commit, once you enable `delta.enableChangeDataFeed = true`. Downstream jobs read only what changed between versions or timestamps, with `table_changes()` in SQL or `readChangeFeed` in batch or streaming. Silver-to-gold pipelines can then process updates and deletes incrementally.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Why CDF**
- A normal streaming read of a Delta table only handles **appends**. Updates and deletes in the source break or are ignored, unless you use options such as `skipChangeCommits` (which skip them).
- CDF exposes the actual row changes, so downstream tables can apply MERGEs incrementally.

**Output columns**
| Column | Meaning |
|---|---|
| `_change_type` | `insert`, `update_preimage`, `update_postimage`, `delete` |
| `_commit_version` | Delta version of the change |
| `_commit_timestamp` | Commit time |

**Reading**
- SQL: `SELECT * FROM table_changes('main.silver.customers', 120, 135)`, or by timestamps.
- Batch: `spark.read.option("readChangeFeed", "true").option("startingVersion", 120).table(...)`.
- Streaming: `spark.readStream.option("readChangeFeed", "true").table(...)`, with a checkpoint tracking progress.

**Storage and retention**
- Changes are stored in `_change_data/` files for some operations, and derived from data files for others (inserts and appends).
- They are subject to the same retention and VACUUM as data files. Readers that fall behind the retention window can't read old changes.
- There is some write overhead. Enable CDF only on tables that need incremental downstream processing.

**Typical pattern:** silver customers (MERGE from CDC) → CDF → gold `dim_customer` SCD2 or aggregates updated incrementally. Lakeflow pipelines' AUTO CDC can consume CDF-like inputs (see [AUTO CDC (formerly APPLY CHANGES) for SCD Type 1/2 in pipelines](#42-auto-cdc-formerly-apply-changes-for-scd-type-12-in-pipelines)).

<p class="qb-label">Example</p>

```sql
-- Dialect: Databricks SQL
ALTER TABLE main.silver.customers SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

SELECT customer_id, segment, _change_type, _commit_version
FROM table_changes('main.silver.customers', 120)
WHERE _change_type IN ('insert', 'update_postimage', 'delete');
```

Sample: version 121 updates customer 7's segment from "SMB" to "MID". CDF returns two rows for that change: `update_preimage` (SMB) and `update_postimage` (MID).

<p class="qb-label">Real-world use case</p>

A gold revenue-by-segment table was rebuilt fully every hour, at 45 minutes per run. Using CDF from silver orders and customers, the job recomputed only the affected customers' aggregates, and runtime fell to 4 minutes.

<p class="qb-label">Common mistakes and red flags</p>

- Streaming from a Delta table that receives updates without CDF and expecting the updates to flow.
- Enabling CDF after the fact and expecting history before enablement.
- Ignoring retention and VACUUM for CDF consumers.

<p class="qb-label">Follow-up questions</p>

- How do you dedupe multiple changes per key in one batch? — Keep the latest `update_postimage`/`insert`/`delete` per key by `_commit_version` before merging.
- CDF vs CDC? — CDF is Delta's change log for a table; CDC usually refers to capturing changes from source databases.

</details>

### 23. Unity Catalog securables, privileges and inheritance

<p class="qb-meta">Intermediate · Common · Unity Catalog</p>

<p class="qb-label">Short answer</p>

Unity Catalog uses SQL GRANTs on securables (metastore, catalog, schema, table, view, volume, function, model, external location, credential, connection and others) to principals (users, groups, service principals). Privileges granted on a container are inherited by its children. To read a table, a principal needs `USE CATALOG` on the catalog, `USE SCHEMA` on the schema and `SELECT` on the table, granted directly or inherited.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Key privileges**
| Privilege | On | Allows |
|---|---|---|
| `USE CATALOG` / `USE SCHEMA` | Catalog / schema | Traverse into the container (required for any access inside) |
| `SELECT` | Table/view/MV (or inherited from schema/catalog) | Read data |
| `MODIFY` | Table | INSERT/UPDATE/DELETE/MERGE |
| `CREATE TABLE`, `CREATE SCHEMA`, `CREATE VOLUME`, `CREATE FUNCTION` | Schema/catalog | Create objects |
| `READ VOLUME` / `WRITE VOLUME` | Volume | File access |
| `EXECUTE` | Function/model | Run UDFs, use models |
| `READ FILES` / `WRITE FILES` / `CREATE EXTERNAL TABLE` | External location | Direct path access and external table creation |
| `MANAGE` | Object | Manage grants and properties without owning |
| `ALL PRIVILEGES` | Any | All applicable privileges (except some admin-only) |
| `BROWSE` | Catalog | See metadata (discover) without reading data |

**Inheritance:** `GRANT SELECT ON SCHEMA prod.gold TO analysts` applies to all current *and future* tables in that schema. There is no separate "future grants" concept, unlike Snowflake (see [Access-role vs functional-role design and future grants](/snowflake/snowflake-interview-questions/#27-access-role-vs-functional-role-design-and-future-grants)).

**Ownership:** each securable has an owner (a user, group or service principal) with full control. Best practice is to make **groups** or **service principals** the owners, not individuals.

**Admin roles:** account admins, metastore admins, and workspace admins (with limited UC powers). Grant metastore admin sparingly.

**Best practices**
- Grant to **groups** synced from your identity provider (SCIM), never to individuals.
- Grant at schema or catalog level for broad access, and table level for exceptions.
- Use separate catalogs per environment, and workspace bindings to restrict which workspaces see production.
- Manage grants as code (Terraform or bundles; see [Infrastructure as Code (Terraform) for data platforms](/career/data-engineering-interview-questions/#69-infrastructure-as-code-terraform-for-data-platforms), [Declarative Automation Bundles (formerly Databricks Asset Bundles) for CI/CD](#34-declarative-automation-bundles-formerly-databricks-asset-bundles-for-cicd)).

<p class="qb-label">Example</p>

```sql
-- Dialect: Databricks SQL
GRANT USE CATALOG ON CATALOG prod TO `data-analysts`;
GRANT USE SCHEMA, SELECT ON SCHEMA prod.gold TO `data-analysts`;     -- inherited by all tables
GRANT MODIFY, CREATE TABLE ON SCHEMA prod.silver TO `etl-service-principal`;
GRANT READ VOLUME ON VOLUME prod.raw.landing TO `ingestion-team`;
ALTER TABLE prod.gold.daily_revenue OWNER TO `finance-data-owners`;

SHOW GRANTS ON SCHEMA prod.gold;
```

<p class="qb-label">Real-world use case</p>

New gold tables were invisible to analysts until someone filed a ticket. Moving the SELECT grant from table level to schema level (inherited) meant every new table was readable by `data-analysts` immediately.

<p class="qb-label">Common mistakes and red flags</p>

- Granting SELECT on a table but not USE CATALOG/USE SCHEMA.
- Individuals owning production tables.
- Granting ALL PRIVILEGES broadly.

<p class="qb-label">Follow-up questions</p>

- How do service principals authenticate for jobs? — OAuth machine-to-machine (M2M) tokens; avoid personal access tokens for production.
- How do you audit grants? — `system.access.audit` and information_schema privilege views.

**In depth:** [What is Unity Catalog used for?](/interview/databricks/unity-catalog/)

</details>

### 24. Lineage and system tables

<p class="qb-meta">Intermediate · Common · Unity Catalog Observability</p>

<p class="qb-label">Short answer</p>

Unity Catalog captures table- and column-level lineage automatically for queries run on UC-enabled compute (notebooks, jobs, SQL, pipelines), and shows it in Catalog Explorer and the `system.access.table_lineage` / `column_lineage` tables. System tables in the `system` catalog expose audit logs, billing, compute, jobs, query history and more as Delta tables, so you can query them with SQL for governance, cost and operations.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Lineage**
- Captured when code reads and writes UC tables. No instrumentation needed.
- Shows upstream and downstream tables, columns, notebooks, jobs, dashboards and models.
- Retained for a rolling window (check current retention, typically about a year).
- Use it for impact analysis before schema changes, root-cause analysis, and compliance (see [What is data lineage, and how is it captured?](/career/data-engineering-interview-questions/#66-what-is-data-lineage-and-how-is-it-captured)).
- Limitations: lineage through some operations (certain UDFs, RDD code, external tools) may be incomplete. External lineage can be added through APIs.

**Key system tables (schemas)**
| Schema.table | Use |
|---|---|
| `system.access.audit` | Who did what (logins, grants, queries, downloads) |
| `system.access.table_lineage`, `column_lineage` | Lineage edges |
| `system.billing.usage`, `system.billing.list_prices` | DBU consumption and list prices (see [Monitoring spend with system.billing tables](#45-monitoring-spend-with-systembilling-tables)) |
| `system.compute.clusters`, `node_types`, `warehouse_events` | Compute configuration and events |
| `system.lakeflow.jobs`, `job_runs`, `job_task_runs` | Job metadata and run history |
| `system.query.history` | SQL warehouse / serverless query history |
| `system.storage.predictive_optimization_operations_history` | Maintenance operations (see [Predictive optimization](#20-predictive-optimization)) |
| `system.information_schema.*` | Metadata about catalogs, tables, columns, privileges |

Access to system tables must be granted explicitly. Some schemas need an account admin to enable them, and audit data is sensitive.

<p class="qb-label">Example</p>

```sql
-- Dialect: Databricks SQL
-- Downstream impact: what reads from silver.orders?
SELECT DISTINCT target_table_full_name, entity_type
FROM system.access.table_lineage
WHERE source_table_full_name = 'prod.silver.orders'
  AND event_date >= current_date() - INTERVAL 90 DAYS;

-- Who read a sensitive table recently?
SELECT event_time, user_identity.email AS user_email, action_name
FROM system.access.audit
WHERE request_params.full_name_arg = 'prod.gold.dim_customer'
  AND event_date >= current_date() - INTERVAL 30 DAYS
ORDER BY event_time DESC;
```

<p class="qb-label">Real-world use case</p>

Before dropping a deprecated column, an engineer queried `column_lineage` and found two dashboards and one ML feature table still using it. The team migrated them first and avoided a production break.

<p class="qb-label">Common mistakes and red flags</p>

- Building custom lineage parsers when UC already captures it.
- Giving broad access to audit system tables.
- Assuming lineage covers non-UC (Hive metastore) tables.

<p class="qb-label">Follow-up questions</p>

- How do you get lineage from external tools (dbt, Airflow)? — Through OpenLineage integrations or the external lineage API into UC.
- Are system tables real-time? — They have some latency; check per table.

</details>

### 25. Pipeline expectations for data quality

<p class="qb-meta">Intermediate · Very Common · Lakeflow Declarative Pipelines</p>

<p class="qb-label">Short answer</p>

Expectations are named constraints declared on pipeline datasets: `CONSTRAINT name EXPECT (condition)`. Each has a violation action. The default is warn: keep the row and record metrics. `DROP ROW` removes violating rows. `FAIL UPDATE` stops the pipeline update. Pass and fail counts per expectation are recorded in the pipeline event log, so quality is observable over time.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Actions**
| Syntax | Behaviour | Use for |
|---|---|---|
| `EXPECT (cond)` | Keep row, count violation | Monitoring soft rules |
| `EXPECT (cond) ON VIOLATION DROP ROW` | Drop invalid rows | Row-level validity in silver |
| `EXPECT (cond) ON VIOLATION FAIL UPDATE` | Fail the update (transaction rolled back for that flow) | Critical invariants (e.g. primary key not null) |

Python equivalents are the `@dp.expect`, `@dp.expect_or_drop` and `@dp.expect_or_fail` decorators, plus `expect_all*` variants that take a dictionary of rules.

**Quarantine pattern.** Expectations drop rows but don't store them. To keep bad rows for investigation:
- Define the rules once (a dictionary).
- Use `expect_all_or_drop(rules)` on the clean table.
- Build a separate quarantine table that selects rows where `NOT (rule1 AND rule2 ...)` (see [How do you handle bad records: quarantine tables and dead-letter queues](/career/data-engineering-interview-questions/#46-how-do-you-handle-bad-records-quarantine-tables-and-dead-letter-queues)).

**Metrics**
- The pipeline **event log** (queryable with the `event_log()` table-valued function, or the configured event log table) records `flow_progress` events with `data_quality.expectations`: name, dataset, passed and failed records.
- Build dashboards and alerts on violation rates.

**Limits**
- Expectations are row-level conditions. Cross-row checks (uniqueness, referential integrity, aggregates) need other patterns, such as a materialized view that computes duplicates with an expectation on its result, or separate tests.
- Expectations aren't a replacement for upstream contracts (see [What are data contracts, and how do you enforce them?](/career/data-engineering-interview-questions/#83-what-are-data-contracts-and-how-do-you-enforce-them)).

<p class="qb-label">Example</p>

```python
# Runtime: Lakeflow Spark Declarative Pipelines (Python, pyspark.pipelines)
from pyspark import pipelines as dp
from pyspark.sql import functions as F

rules = {
    "valid_order_id": "order_id IS NOT NULL",
    "non_negative_amount": "amount >= 0",
    "known_currency": "currency IN ('USD','EUR','INR')",
}

@dp.table(name="silver_orders")
@dp.expect_all_or_drop(rules)
def silver_orders():
    return spark.readStream.table("bronze_orders")

@dp.table(name="quarantine_orders")
def quarantine_orders():
    all_valid = " AND ".join(f"({c})" for c in rules.values())
    return (spark.readStream.table("bronze_orders")
            .filter(F.expr(f"NOT ({all_valid})")))
```
The quarantine condition is built from the rule definitions, which are constants in code, not user input. In a production pipeline, also handle rows where a rule evaluates to NULL, for example by wrapping each with `COALESCE(cond, false)`.

<p class="qb-label">Real-world use case</p>

A pipeline with `FAIL UPDATE` on `order_id IS NOT NULL` and `DROP ROW` on currency validity caught a vendor feed regression. The update failed before gold refreshed, and the event-log dashboard showed exactly which rule tripped.

<p class="qb-label">Common mistakes and red flags</p>

- Using FAIL UPDATE for minor issues, causing constant outages.
- Dropping rows without keeping a quarantine copy.
- Expecting uniqueness checks from row-level expectations.

<p class="qb-label">Follow-up questions</p>

- How do you alert on violations? — Query the event log on a schedule (Databricks SQL alert) and notify when failed records exceed a threshold.
- How do NULLs interact with expectations? — A condition such as `amount >= 0` evaluates to NULL when `amount` is NULL; write explicit `IS NOT NULL` checks so the rule's intent is unambiguous.

</details>

### 26. Triggered vs continuous pipelines; development vs production mode

<p class="qb-meta">Intermediate · Common · Lakeflow Declarative Pipelines</p>

<p class="qb-label">Short answer</p>

A triggered pipeline processes all available data once, then stops, run on a schedule or on demand, which is cost-efficient. A continuous pipeline keeps running and processes new data with low latency. Development mode reuses the cluster and doesn't retry, for fast iteration. Production mode restarts compute on failure and retries automatically, for reliability.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Triggered vs continuous**
| Aspect | Triggered | Continuous |
|---|---|---|
| Runs | On schedule/trigger, then stops | Always on |
| Latency | Schedule interval (minutes–hours) | Seconds to minutes |
| Cost | Pay only during runs | Pay continuously |
| Streaming tables | Process available data then stop (like `availableNow`) | Process continuously |
| Materialized views | Refreshed on each update | Refreshed as sources change (when supported) |
| Typical use | Most batch/near-real-time ETL | Low-latency operational pipelines |

Triggered runs are usually orchestrated by **Lakeflow Jobs** (a pipeline task), so they integrate with other tasks (see [Lakeflow Jobs (formerly Workflows): tasks, retries and repair runs](#8-lakeflow-jobs-formerly-workflows-tasks-retries-and-repair-runs)). Pipelines can also be scheduled directly.

**Development vs production mode**
| Aspect | Development | Production |
|---|---|---|
| Cluster reuse | Keeps cluster up between updates (faster iteration) | Restarts cluster for each update/recovery |
| Retries | No automatic retries (see errors immediately) | Automatic retries with escalating recovery |
| Purpose | Building and debugging | Scheduled, reliable runs |

**Other operational options**
- **Full refresh:** recompute datasets from scratch. This is needed after logic changes that affect history. It is dangerous for streaming tables whose sources no longer retain all data, so they can be protected with `pipelines.reset.allowed = false`.
- **Selective refresh:** refresh only chosen tables.
- **Serverless vs classic compute,** and enhanced autoscaling for streaming workloads.
- **Channels** (current and preview) control the runtime version.
- **Publishing:** pipelines publish datasets to a target catalog and schema in Unity Catalog. Datasets can now be published across multiple schemas.

<p class="qb-label">Example</p>

```json
{
  "name": "orders_pipeline",
  "catalog": "prod",
  "schema": "sales",
  "serverless": true,
  "continuous": false,
  "development": false,
  "channel": "CURRENT",
  "libraries": [{ "glob": { "include": "/Workspace/pipelines/orders/**" } }],
  "configuration": { "source_path": "/Volumes/prod/raw/landing/orders/" }
}
```

<p class="qb-label">Real-world use case</p>

A team ran a pipeline in continuous mode "just in case", but consumers only checked dashboards hourly. Switching to triggered mode, scheduled every 15 minutes in a Lakeflow Job, cut pipeline cost by about 70% with acceptable latency.

<p class="qb-label">Common mistakes and red flags</p>

- Running production in development mode (no retries).
- Continuous mode without a real latency need.
- Full refresh of a streaming table whose source data has expired.

<p class="qb-label">Follow-up questions</p>

- How do you parameterise a pipeline? — Configuration key-value pairs referenced in code (`spark.conf.get`) or `${var}` in SQL.
- How do you prevent accidental full refresh? — Table property `pipelines.reset.allowed = false` on critical streaming tables.

</details>

### 27. Auto Loader: directory listing vs file notification

<p class="qb-meta">Intermediate · Very Common · Ingestion</p>

<p class="qb-label">Short answer</p>

Auto Loader (`format("cloudFiles")`) incrementally ingests new files from cloud storage as a Structured Streaming source. It tracks processed files in a checkpoint (RocksDB), so each file is loaded exactly once. Directory listing mode discovers files by listing paths, which is simple and works anywhere. File notification mode uses cloud events and queues to discover files, which scales better for very large directories.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Core usage**
- `spark.readStream.format("cloudFiles").option("cloudFiles.format", "json").load(path)`.
- Write with a checkpoint location. Use `trigger(availableNow=True)` for batch-style incremental runs, or continuous processing.
- Supported formats: JSON, CSV, XML, Parquet, Avro, ORC, text and binaryFile.

**Discovery modes**
| Mode | How | Pros | Cons |
|---|---|---|---|
| Directory listing (default) | List input path; incremental listing optimisations for lexically ordered paths | No extra cloud setup; works with UC volumes | Listing cost/latency grows with huge directories |
| File notification | Cloud events → queue (SQS/SNS, Event Grid, Pub/Sub); Auto Loader reads queue | Scales to millions of files, low latency | Needs permissions to create/manage notification resources |
| Managed file events (newer) | Unity Catalog external locations with file events enabled; Auto Loader uses UC-managed notifications | Notification-mode scale without per-stream setup | Requires external location file events configuration |

**Key options**
- `cloudFiles.schemaLocation`: where the inferred schema and its evolution history are stored (see [Auto Loader schema inference, evolution and rescued data](#44-auto-loader-schema-inference-evolution-and-rescued-data)).
- `cloudFiles.maxFilesPerTrigger` / `cloudFiles.maxBytesPerTrigger`: rate limiting (see [What is backpressure in streaming systems?](/career/data-engineering-interview-questions/#94-what-is-backpressure-in-streaming-systems)).
- `cloudFiles.includeExistingFiles`: whether to process files already present at first start.
- `cloudFiles.backfillInterval`: periodic listing in notification mode to catch any missed events.
- `modifiedAfter` / `pathGlobFilter`: file filters.
- The `_metadata` column gives file path, size and modification time, which is useful for lineage.

**Auto Loader vs plain `readStream` on files:** Auto Loader scales better (RocksDB state rather than an in-memory file list), supports schema inference and evolution, and offers notification mode.

<p class="qb-label">Example</p>

```python
# Runtime: PySpark 3.5 on Databricks
from pyspark.sql import functions as F

raw = (spark.readStream
       .format("cloudFiles")
       .option("cloudFiles.format", "json")
       .option("cloudFiles.schemaLocation", "/Volumes/prod/raw/_schemas/orders")
       .option("cloudFiles.maxFilesPerTrigger", 1000)
       .load("/Volumes/prod/raw/landing/orders/"))

(raw.withColumn("source_file", F.col("_metadata.file_path"))
    .withColumn("ingested_at", F.current_timestamp())
    .writeStream
    .option("checkpointLocation", "/Volumes/prod/raw/_checkpoints/bronze_orders")
    .trigger(availableNow=True)
    .toTable("prod.bronze.orders"))
```

<p class="qb-label">Real-world use case</p>

A landing bucket received 2 million small files a day, and directory listing slowed to 20 minutes per run. Enabling file events on the UC external location and switching Auto Loader to use them cut discovery to seconds.

<p class="qb-label">Common mistakes and red flags</p>

- Sharing one checkpoint between two streams.
- Deleting the checkpoint to "fix" an issue, which reprocesses everything.
- No rate limits on huge backfills, which overwhelms the cluster.

<p class="qb-label">Follow-up questions</p>

- How do you reprocess one file? — Copy it to a new path or name; Auto Loader tracks by path, or use a separate batch read.
- Auto Loader vs COPY INTO? — See [COPY INTO vs Auto Loader](#28-copy-into-vs-auto-loader).

</details>

### 28. COPY INTO vs Auto Loader

<p class="qb-meta">Intermediate · Common · Ingestion</p>

<p class="qb-label">Short answer</p>

Both load files into Delta tables idempotently, skipping files already loaded. `COPY INTO` is a SQL command that is simple, retriable and good for thousands of files on a schedule or ad hoc. Auto Loader is a Structured Streaming source that scales to millions of files, supports file-notification discovery, schema inference and evolution with rescued data, and runs continuously or as `availableNow` batches. Databricks recommends Auto Loader for most incremental ingestion.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

| Aspect | COPY INTO | Auto Loader |
|---|---|---|
| Interface | SQL statement | `readStream.format("cloudFiles")` / `read_files` in streaming tables |
| State tracking | Load history in target table's metadata | Checkpoint (RocksDB) |
| Scale | Thousands of files per run comfortably | Millions of files; notification mode |
| Latency | Per run | Continuous or triggered |
| Schema | Explicit or inferred (`mergeSchema` option) | Inference, hints, evolution modes, `_rescued_data` |
| Typical use | SQL-centric teams, ad-hoc or modest scheduled loads, SQL warehouses | Production ingestion pipelines, Lakeflow pipelines |
| Reprocess files | `COPY_OPTIONS ('force' = 'true')` | New checkpoint or path tricks |

**COPY INTO notes**
- Works on SQL warehouses and clusters.
- Idempotent: files already loaded are skipped unless `force` is set.
- Supports `PATTERN`, `FILES`, `FORMAT_OPTIONS` and `COPY_OPTIONS` (`mergeSchema`).
- You can create an empty table with no schema and let COPY INTO infer and evolve it, for Delta targets.

**Choosing**
- Need SQL-only ingestion from a warehouse, small to moderate volumes, or one-off backfills: COPY INTO.
- Continuous or very high file counts, schema drift, or Lakeflow pipelines: Auto Loader.

<p class="qb-label">Example</p>

```sql
-- Dialect: Databricks SQL
CREATE TABLE IF NOT EXISTS prod.bronze.vendor_invoices;    -- schemaless; COPY INTO can infer

COPY INTO prod.bronze.vendor_invoices
FROM '/Volumes/prod/raw/landing/invoices/'
FILEFORMAT = CSV
PATTERN = '2026-03-*/*.csv'
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');
-- Re-running the same command skips files already loaded.
```

<p class="qb-label">Real-world use case</p>

A finance team loads about 50 vendor CSVs a day with COPY INTO from a SQL warehouse, which is simple and idempotent. The clickstream platform (500,000 files a day) uses Auto Loader with file notifications in a Lakeflow pipeline.

<p class="qb-label">Common mistakes and red flags</p>

- `force = true` on scheduled jobs, which duplicates data.
- Using COPY INTO for millions of tiny files continuously.
- Not knowing both are idempotent by design.

<p class="qb-label">Follow-up questions</p>

- What is `read_files`? — A table-valued function for reading files in SQL; with `STREAM` in pipelines it uses Auto Loader under the hood.
- Can COPY INTO load into external tables? — Into Delta tables (managed or external); check docs for formats.

</details>

### 29. Job parameters, task values and conditional tasks

<p class="qb-meta">Intermediate · Common · Orchestration</p>

<p class="qb-label">Short answer</p>

Job parameters are key-value inputs defined once for a job, overridable per run, and pushed to every task. Dynamic value references such as `{{job.start_time.iso_date}}` inject run context. Task values let one task publish small outputs (`dbutils.jobs.taskValues.set`) that downstream tasks read. If/else condition tasks and For each tasks add branching and looping inside a job.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Job parameters**
- Defined at job level and available to all tasks (as notebook widgets, script arguments, or SQL parameters).
- Overridden on "Run now with different parameters", API runs and backfills.
- Use them for environment, run date and source path, which makes jobs idempotent per date (see [How do you make Airflow tasks idempotent and safe to retry?](/career/data-engineering-interview-questions/#50-how-do-you-make-airflow-tasks-idempotent-and-safe-to-retry)).

**Dynamic value references**
- `{{job.id}}`, `{{job.run_id}}`, `{{job.start_time.iso_date}}`, `{{job.trigger.type}}`, `{{tasks.<task_key>.values.<key>}}` and others.
- Use them instead of `datetime.now()` in code, so runs are reproducible.

**Task values**
- `dbutils.jobs.taskValues.set(key="row_count", value=12345)` in an upstream task.
- `dbutils.jobs.taskValues.get(taskKey="ingest", key="row_count", default=0)` downstream, or `{{tasks.ingest.values.row_count}}` in task configuration.
- For small values only (counts, flags, file lists), not datasets. The same principle applies as Airflow XCom (see [What are XComs, and what are their limits?](/career/data-engineering-interview-questions/#48-what-are-xcoms-and-what-are-their-limits)).

**Control flow**
- **If/else condition task:** compare values (`{{tasks.ingest.values.row_count}} > 0`) and branch. Downstream tasks depend on the true or false outcome.
- **For each task:** loop a nested task over an input array (a list of tables or dates), with configurable concurrency.
- **Run job task:** call another job as a modular sub-workflow.
- **Run if dependencies:** run cleanup when "at least one failed".

<p class="qb-label">Example</p>

```python
# Runtime: Databricks notebook (Python) — upstream task "ingest"
run_date = dbutils.widgets.get("run_date")          # job parameter, e.g. {{job.start_time.iso_date}}
df = spark.read.table("prod.bronze.orders").filter(f"order_date = '{run_date}'")
row_count = df.count()
dbutils.jobs.taskValues.set(key="row_count", value=row_count)

# Downstream condition task (configured in job YAML/UI):
#   left: {{tasks.ingest.values.row_count}}  operator: GREATER_THAN  right: 0
# true branch -> build_marts ; false branch -> notify_no_data
```
`run_date` comes from a job parameter set by the orchestrator, not from end users. Still validate its format (for example with `datetime.date.fromisoformat`) before using it in a SQL string, or use parameterised `spark.sql(..., args=...)` (Spark 3.4+).

<p class="qb-label">Real-world use case</p>

A job processes 40 source tables. A For each task iterates over a table list produced by a config task, with concurrency 8. An If/else task skips mart builds when the ingestion task reports zero new rows, which saves compute on empty days.

<p class="qb-label">Common mistakes and red flags</p>

- Passing DataFrames through task values.
- Using `now()` inside tasks instead of run parameters.
- Building string SQL from unvalidated parameters.

<p class="qb-label">Follow-up questions</p>

- How do you backfill a date range? — Trigger runs with `run_date` overrides (or the job backfill feature) with controlled concurrency.
- Task-level vs job-level parameters? — Job parameters apply to all tasks; task parameters are specific and can reference job parameters.

</details>

### 30. Cluster sizing, autoscaling and spot instances

<p class="qb-meta">Intermediate · Common · Compute</p>

<p class="qb-label">Short answer</p>

Size clusters by workload. Use memory-optimised nodes for joins, aggregations and caching, compute-optimised nodes for CPU-heavy transforms, and storage-optimised nodes with local NVMe for heavy shuffle and Delta caching. Autoscaling between a minimum and maximum worker count adapts to load. Spot workers with on-demand fallback cut cost for fault-tolerant jobs, but keep the driver on-demand.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Sizing approach**
1. **Start from the data:** input size, shuffle volume (Spark UI), and the number of tasks.
2. **Cores determine parallelism:** total task slots equal workers × cores per worker. Aim for enough partitions to keep all cores busy (see [How Spark decides the number of input partitions](/pyspark/pyspark-interview-questions/#18-how-spark-decides-the-number-of-input-partitions)).
3. **Memory per core:** check for spill and GC pressure. Spill in the Spark UI suggests more memory per core or more partitions (see [Executor memory model: unified, execution, storage, overhead](/pyspark/pyspark-interview-questions/#42-executor-memory-model-unified-execution-storage-overhead)).
4. **Fewer large nodes vs many small:** large nodes reduce shuffle network overhead but increase GC and failure impact. Moderate sizes (8–16 cores) are a common middle ground.
5. **Driver:** larger if collecting results, broadcasting big tables or running many concurrent jobs. Usually the same type as workers.

**Autoscaling**
- Set `min_workers` and `max_workers`. Databricks adds workers when tasks are pending and removes idle ones.
- **Optimized autoscaling** (Premium and above) scales down more aggressively based on shuffle-file state.
- Streaming: prefer **enhanced autoscaling** in Lakeflow pipelines, or fixed sizes. Classic autoscaling reacts poorly to streaming workloads.
- Autoscaling doesn't fix skew. One huge task still runs on one core (see [How do you detect data skew?](/pyspark/pyspark-interview-questions/#21-how-do-you-detect-data-skew)).

**Spot / preemptible**
- `first_on_demand: 1` keeps the driver on-demand. `availability: SPOT_WITH_FALLBACK` falls back to on-demand when spot capacity isn't available (see [Spot/preemptible and reserved capacity for data workloads](/career/data-engineering-interview-questions/#73-spotpreemptible-and-reserved-capacity-for-data-workloads)).
- Good for batch ETL. Riskier for long jobs with expensive shuffles, because losing nodes recomputes stages.
- Decommissioning features help migrate shuffle data when spot nodes get preemption notices.

**Other levers**
- **Photon** for SQL and DataFrame-heavy workloads (see [What is Photon?](#10-what-is-photon)).
- **Instance pools** to reduce start-up time (see [Compute policies and instance pools](#31-compute-policies-and-instance-pools)).
- Latest **LTS runtimes** for performance improvements.
- **Serverless** removes most sizing decisions (see [Serverless compute for jobs and notebooks](#38-serverless-compute-for-jobs-and-notebooks)).

<p class="qb-label">Example</p>

```json
{
  "spark_version": "15.4.x-scala2.12",
  "node_type_id": "r6id.2xlarge",
  "driver_node_type_id": "r6id.2xlarge",
  "autoscale": { "min_workers": 2, "max_workers": 12 },
  "runtime_engine": "PHOTON",
  "aws_attributes": {
    "first_on_demand": 1,
    "availability": "SPOT_WITH_FALLBACK",
    "zone_id": "auto"
  },
  "autotermination_minutes": 30
}
```

<p class="qb-label">Real-world use case</p>

A join-heavy job on 20 compute-optimised workers spilled 2 TB to disk. Switching to 10 memory-optimised workers with local NVMe, plus AQE (see [Adaptive Query Execution: what does it do?](/pyspark/pyspark-interview-questions/#22-adaptive-query-execution-what-does-it-do)), removed most of the spill. It ran 35% faster at a lower cost.

<p class="qb-label">Common mistakes and red flags</p>

- Adding workers to fix skew.
- A spot driver.
- Huge min_workers that keep the cluster oversized all the time.

<p class="qb-label">Follow-up questions</p>

- How do you know a cluster is oversized? — Low CPU utilisation in metrics, many idle task slots, short tasks; compare against smaller runs.
- Why storage-optimised nodes? — Local NVMe speeds shuffle spill and the disk cache.

</details>

### 31. Compute policies and instance pools

<p class="qb-meta">Intermediate · Common · Compute Governance</p>

<p class="qb-label">Short answer</p>

Compute (cluster) policies are JSON rules that restrict or fix cluster settings: allowed node types, maximum workers, required tags, auto-termination, runtime versions and access mode. Users can only create compliant compute. Instance pools keep a set of idle, ready VMs, so clusters start and scale faster. You pay cloud costs for idle pool instances, but not DBUs.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Policies**
- Attribute rules of type `fixed`, `allowlist`, `blocklist`, `range`, `regex`, `unlimited` and `forbidden`, plus `defaultValue` and `isOptional`.
- Common uses:
  - Enforce `autotermination_minutes` ≤ 60 for interactive compute.
  - Cap `autoscale.max_workers`.
  - Require `custom_tags.team` and `custom_tags.cost_center` for chargeback (see [FinOps: tagging, chargeback and showback](/career/data-engineering-interview-questions/#72-finops-tagging-chargeback-and-showback)).
  - Allow only approved node families and the latest LTS runtimes.
  - Force `data_security_mode` to UC-compatible modes.
  - Set a spot policy.
- **Policy families** (Databricks-provided templates such as Personal Compute, Shared Compute and Job Compute) can be extended with overrides.
- Grant `CAN_USE` on policies to groups. Remove unrestricted cluster creation from most users.
- **Serverless budget policies** play a similar role for tagging serverless usage.

**Instance pools**
- Pre-provisioned idle instances (`min_idle_instances`) plus a maximum capacity.
- Clusters attached to a pool draw instances from it. On termination, instances return to the pool (until the idle timeout).
- Benefits: start-up drops from several minutes to under a minute or so, and autoscaling reacts faster.
- Costs: idle instances incur **cloud VM charges** (not DBUs). Keep `min_idle_instances` small.
- Spot pools or on-demand pools. The driver and workers can use different pools.
- Serverless compute often makes pools unnecessary for jobs and SQL.

<p class="qb-label">Example</p>

```json
{
  "spark_version": { "type": "regex", "pattern": "1[5-9]\\.[0-9]+\\.x-scala2\\.12" },
  "autotermination_minutes": { "type": "range", "maxValue": 60, "defaultValue": 30 },
  "autoscale.max_workers": { "type": "range", "maxValue": 10 },
  "node_type_id": { "type": "allowlist", "values": ["m6id.xlarge", "r6id.xlarge", "r6id.2xlarge"] },
  "custom_tags.team": { "type": "unlimited", "isOptional": false },
  "data_security_mode": { "type": "fixed", "value": "USER_ISOLATION" },
  "instance_pool_id": { "type": "forbidden", "hidden": true }
}
```

<p class="qb-label">Real-world use case</p>

After a few users created 100-node interactive clusters that ran for days, the platform team rolled out policies with max 10 workers, a 30-minute auto-termination and required team tags. Interactive compute spend fell by half, and every DBU became attributable.

<p class="qb-label">Common mistakes and red flags</p>

- Unrestricted cluster creation for everyone.
- Large `min_idle_instances` in pools, burning cloud cost.
- Tags not enforced, so chargeback is impossible.

<p class="qb-label">Follow-up questions</p>

- How do policies interact with jobs? — Job clusters can be required to use specific policies; bundles reference `policy_id`.
- Are pools useful with serverless? — Mostly not; serverless already starts quickly.

</details>

### 32. SQL warehouses: classic vs pro vs serverless

<p class="qb-meta">Intermediate · Common · Compute</p>

<p class="qb-label">Short answer</p>

Databricks SQL warehouses are compute endpoints for SQL and BI, with T-shirt sizes and multi-cluster scaling for concurrency. Classic warehouses run in your cloud account with basic features. Pro adds features such as Lakehouse Federation, workflows integration, Python UDFs and predictive I/O. Serverless runs in Databricks' account, starts in seconds, scales quickly with Intelligent Workload Management, and is the recommended default where available.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

| Aspect | Classic | Pro | Serverless |
|---|---|---|---|
| Compute location | Your account | Your account | Databricks account |
| Start-up time | Minutes | Minutes | Seconds |
| Photon | Yes | Yes | Yes |
| Predictive I/O, some advanced features | Limited | Yes | Yes |
| Lakehouse Federation, ML/AI SQL functions, Python UDFs (features vary) | No | Yes | Yes |
| Autoscaling for concurrency | Clusters added per queueing | Same | Intelligent Workload Management (fast, query-aware) |
| Billing | DBU + your VM costs | DBU + VM | DBU (infra included) |

Feature availability per warehouse type changes over time. Check current docs before quoting exact matrices.

**Sizing and scaling**
- **Size** (2X-Small to 4X-Large) sets the compute per cluster, for faster individual queries.
- **Scaling (min/max clusters)** handles concurrency. Extra clusters are added when queries queue.
- **Auto-stop** after idle minutes. Serverless can stop quickly (minutes) without long cold starts.
- **Query result cache** and the disk cache speed up repeated dashboard queries.

**Typical uses:** BI dashboards (Power BI, Tableau, Databricks AI/BI dashboards), ad-hoc SQL, dbt SQL models, and alerts.

**Monitoring**
- `system.query.history` for per-query statistics, and `system.compute.warehouse_events` for scaling events.
- Query profile for operator-level tuning (see [Troubleshooting a slow Databricks job end to end](#50-troubleshooting-a-slow-databricks-job-end-to-end)).

<p class="qb-label">Example</p>

```json
{
  "name": "bi-serverless",
  "cluster_size": "Medium",
  "min_num_clusters": 1,
  "max_num_clusters": 4,
  "auto_stop_mins": 10,
  "enable_serverless_compute": true,
  "warehouse_type": "PRO",
  "tags": { "custom_tags": [{ "key": "team", "value": "analytics" }] }
}
```

<p class="qb-label">Real-world use case</p>

A BI team on a classic warehouse kept it running all day because start-up took about 5 minutes. Switching to serverless with a 10-minute auto-stop let it shut down between bursts. Dashboard latency was unchanged, and cost fell substantially.

<p class="qb-label">Common mistakes and red flags</p>

- Upsizing to fix concurrency queuing (use max clusters instead).
- Running heavy Spark ETL on SQL warehouses when job compute fits better.
- Never auto-stopping classic warehouses.

<p class="qb-label">Follow-up questions</p>

- Can dbt use SQL warehouses? — Yes; dbt-databricks connects via HTTP path to a warehouse.
- How do you find slow queries? — Query history system table and query profile.

</details>

### 33. Structured Streaming with Delta as source and sink

<p class="qb-meta">Intermediate · Common · Streaming</p>

<p class="qb-label">Short answer</p>

A Delta table can be a streaming source: new commits are read incrementally, which by default handles appends only. It can also be a streaming sink, with exactly-once writes through checkpoints and Delta's transactional commits. Use `availableNow` triggers for incremental batch, CDF to stream updates and deletes, and `skipChangeCommits` to ignore non-append changes deliberately.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Delta as a source**
- `spark.readStream.table("prod.silver.orders")` reads new data files as they are committed.
- Rate limits: `maxFilesPerTrigger` (default 1000) and `maxBytesPerTrigger`.
- Starting point: `startingVersion` or `startingTimestamp` (otherwise it starts from the current state, then follows new commits).
- **Updates and deletes in the source** (from MERGE, UPDATE, DELETE or OPTIMIZE rewrites) cause errors by default, because the stream expects appends. Options:
  - `skipChangeCommits = true`: ignore commits that change existing data. Rows deleted or updated there won't be reflected.
  - **Change Data Feed** (`readChangeFeed = true`) to process updates and deletes properly (see [Change Data Feed](#22-change-data-feed)).
  - OPTIMIZE-only commits (`dataChange = false`) are ignored automatically.

**Delta as a sink**
- `writeStream.toTable("...")` (append) or `foreachBatch` for MERGE (see [foreachBatch for upserts and multiple sinks](/pyspark/pyspark-interview-questions/#36-foreachbatch-for-upserts-and-multiple-sinks)).
- Exactly-once: the checkpoint tracks source offsets, and Delta records a `txn` (query ID and batch ID), so a retried batch isn't committed twice.
- `outputMode("complete")` for aggregations that overwrite the result table.

**Triggers**
- `trigger(availableNow=True)`: process everything available in rate-limited batches, then stop. This is ideal for scheduled incremental jobs.
- `trigger(processingTime="1 minute")` for continuous micro-batches. See [Triggers: processingTime, availableNow, once, continuous](/pyspark/pyspark-interview-questions/#35-triggers-processingtime-availablenow-once-continuous).

**Operational tips**
- **One checkpoint per stream,** stored on reliable storage (a UC volume or cloud path). Never share or delete it casually.
- Changing the query logic may require a new checkpoint. Check compatibility rules.
- Monitor with `StreamingQueryListener`, the Spark UI streaming tab, or Lakeflow pipeline event logs.

<p class="qb-label">Example</p>

```python
# Runtime: PySpark 3.5 on Databricks — incremental silver -> gold aggregation as a scheduled job
from pyspark.sql import functions as F

(spark.readStream
    .option("maxFilesPerTrigger", 500)
    .table("prod.silver.page_views")
    .withWatermark("event_ts", "2 hours")
    .groupBy(F.window("event_ts", "1 hour"), "page_id")
    .agg(F.count("*").alias("views"))
    .writeStream
    .outputMode("append")
    .option("checkpointLocation", "/Volumes/prod/gold/_checkpoints/hourly_page_views")
    .trigger(availableNow=True)
    .toTable("prod.gold.hourly_page_views"))
```

<p class="qb-label">Real-world use case</p>

A streaming job from silver orders failed after a GDPR DELETE ran on the source table. The team switched the downstream to read the Change Data Feed, so deletions propagated to gold instead of breaking the stream.

<p class="qb-label">Common mistakes and red flags</p>

- Streaming from a table that receives updates without CDF or skip options.
- Sharing checkpoints.
- Using `processingTime` 24/7 when `availableNow` on a schedule is enough.

<p class="qb-label">Follow-up questions</p>

- What does `ignoreDeletes` do? — Older option that ignored deletes at partition boundaries; `skipChangeCommits` is the broader modern option.
- How do you restart from scratch? — New checkpoint location (and usually truncate/rebuild the sink).

</details>

### 34. Declarative Automation Bundles (formerly Databricks Asset Bundles) for CI/CD

<p class="qb-meta">Intermediate · Common · CI/CD</p>

<p class="qb-label">Short answer</p>

Declarative Automation Bundles (DABs, renamed from Databricks Asset Bundles in March 2026) package project code together with YAML definitions of Databricks resources: jobs, pipelines, dashboards, schemas, volumes, models and permissions. The Databricks CLI (`databricks bundle validate / deploy / run`) deploys the same project to dev, staging and prod targets. That gives repeatable, reviewable CI/CD.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Structure**
```text
my_project/
 ├─ databricks.yml          # bundle name, targets (dev/staging/prod), variables, includes
 ├─ resources/
 │   ├─ orders_job.yml      # jobs
 │   └─ orders_pipeline.yml # Lakeflow pipelines
 ├─ src/                    # notebooks, Python packages, SQL
 └─ tests/                  # unit tests
```

**Key concepts**
- **Targets:** `dev` (mode: development, which prefixes resource names with the user and pauses schedules), and `staging` and `prod` (mode: production, with stricter checks, run-as identity and permissions).
- **Variables:** per-target values (catalog names, warehouse IDs, node types).
- **Artifacts:** build Python wheels during deployment.
- **Permissions and run_as:** deploy production jobs to run as a service principal.
- **Templates:** `databricks bundle init` starts from standard templates (default-python, SQL, dbt).
- **Deployment engines:** the newer **direct deployment engine** (default for new bundles in recent CLI versions) calls Databricks APIs directly, and the older engine used Terraform under the hood. Existing bundles keep working, because the rename is non-breaking.
- `databricks bundle generate` / `deployment bind` bring existing UI-created jobs under bundle management.

**CI/CD flow**
1. Pull request: lint, unit tests, `databricks bundle validate -t staging`.
2. Merge to main: `databricks bundle deploy -t staging`, run integration tests (`bundle run`).
3. Release or approval: `databricks bundle deploy -t prod`.
4. Authenticate CI with OAuth for service principals, or workload identity federation (OIDC) from GitHub Actions or Azure DevOps. No PATs.

<p class="qb-label">Example</p>

```yaml
# databricks.yml
bundle:
  name: orders_platform

variables:
  catalog:
    default: dev

targets:
  dev:
    mode: development
    default: true
    workspace:
      host: https://adb-1111111111111111.11.azuredatabricks.net
  prod:
    mode: production
    variables:
      catalog: prod
    workspace:
      host: https://adb-2222222222222222.22.azuredatabricks.net
    run_as:
      service_principal_name: "6f1c0d2e-0000-4a6b-9c1d-orders-sp"

include:
  - resources/*.yml
```

```bash
databricks bundle validate -t prod
databricks bundle deploy -t prod
databricks bundle run -t prod nightly_sales
```

<p class="qb-label">Real-world use case</p>

A team used to copy jobs between workspaces by hand and regularly had config drift. After moving to bundles with dev, staging and prod targets, every change goes through a pull request. Production deploys are one CI step running as a service principal.

<p class="qb-label">Common mistakes and red flags</p>

- Editing production jobs in the UI after deploying them with bundles, which causes drift.
- Deploying production as a personal user.
- Hardcoding catalog names instead of target variables.

<p class="qb-label">Follow-up questions</p>

- Bundles vs Terraform? — Bundles focus on project-level code + workflows; Terraform suits platform infrastructure (workspaces, UC metastore, networking). Many teams use both.
- What does development mode do? — Prefixes names, pauses schedules, and isolates per developer to avoid clobbering shared resources.

</details>

### 35. Controlling DBU cost on Databricks

<p class="qb-meta">Intermediate · Very Common · Cost Optimisation</p>

<p class="qb-label">Short answer</p>

Run production on job compute or serverless, not all-purpose clusters. Enforce auto-termination and tagging with compute policies. Right-size clusters and use spot with fallback for batch. Use Photon where it pays off, and incremental processing (Auto Loader, CDF, materialized views) instead of full recomputes. Keep tables optimised (liquid clustering, predictive optimization). Then monitor spend with `system.billing` tables, budgets and alerts.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Where cost comes from**
- **DBUs** (Databricks billing), by SKU: all-purpose, jobs, SQL (classic, pro, serverless), pipelines, serverless jobs and notebooks, model serving, predictive optimization and others.
- **Cloud infrastructure** for classic compute (VMs, disks, network), billed by your cloud provider.
- **Storage** in your cloud (Delta files, old versions before VACUUM).

**Levers**
| Area | Actions |
|---|---|
| Compute type | Jobs → job compute/serverless; BI → serverless SQL warehouses; dev → small auto-terminating clusters |
| Governance | Compute policies (max size, auto-termination, required tags), budget policies for serverless (see [Compute policies and instance pools](#31-compute-policies-and-instance-pools)) |
| Sizing | Right-size from Spark UI metrics; autoscaling; spot with fallback; storage-optimised nodes for shuffle-heavy work (see [Cluster sizing, autoscaling and spot instances](#30-cluster-sizing-autoscaling-and-spot-instances)) |
| Engine | Photon for SQL/DataFrame-heavy jobs (benchmark; see [What is Photon?](#10-what-is-photon)) |
| Work reduction | Incremental ingestion (Auto Loader), CDF-driven updates, materialized views with incremental refresh, avoid full-refresh pipelines |
| Data layout | Liquid clustering, OPTIMIZE, predictive optimization → fewer files scanned (see [Liquid Clustering vs partitioning vs Z-ORDER](#40-liquid-clustering-vs-partitioning-vs-z-order), [Predictive optimization](#20-predictive-optimization)) |
| Scheduling | Triggered pipelines instead of continuous where latency allows (see [Triggered vs continuous pipelines; development vs production mode](#26-triggered-vs-continuous-pipelines-development-vs-production-mode)); consolidate tiny jobs |
| Storage | VACUUM regularly, lifecycle policies on raw landing zones |

**Visibility**
- `system.billing.usage` joined with `system.billing.list_prices` gives cost by SKU, workspace, job, warehouse, user and custom tags (see [Monitoring spend with system.billing tables](#45-monitoring-spend-with-systembilling-tables)).
- **Budgets** (account console) with alerts, and usage dashboards.
- Tag everything (team, project, env) for chargeback.

<p class="qb-label">Example</p>

```sql
-- Dialect: Databricks SQL — last 30 days DBUs by product and SKU
SELECT billing_origin_product,
       sku_name,
       SUM(usage_quantity) AS dbus
FROM system.billing.usage
WHERE usage_date >= current_date() - INTERVAL 30 DAYS
  AND usage_unit = 'DBU'
GROUP BY billing_origin_product, sku_name
ORDER BY dbus DESC;
```

<p class="qb-label">Real-world use case</p>

A monthly review showed 45% of DBUs on all-purpose compute, mostly scheduled notebooks, and a continuous pipeline that only needed hourly freshness. Moving the jobs to serverless jobs and the pipeline to a triggered schedule cut the bill by about 40%.

<p class="qb-label">Common mistakes and red flags</p>

- Ignoring the cloud VM bill for classic compute.
- No tags, so nobody owns the spend.
- Full-refresh pipelines for append-only data.

<p class="qb-label">Follow-up questions</p>

- Is serverless always cheaper? — Not always; it removes idle and start-up waste, but sustained heavy jobs may be cheaper on well-sized classic compute — measure.
- How do you attribute serverless cost? — Budget policies / custom tags on serverless usage in `system.billing.usage`.

</details>

### 36. Shallow clone vs deep clone

<p class="qb-meta">Intermediate · Common · Delta Lake</p>

<p class="qb-label">Short answer</p>

A deep clone copies a Delta table's data files and metadata into a new, fully independent table, which is useful for backups, migrations and archiving. A shallow clone copies only the metadata and references the source's data files. It is near-instant and cheap, which suits testing and experiments. But it depends on the source's files still existing, so VACUUM on the source can break it.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

| Aspect | Shallow clone | Deep clone |
|---|---|---|
| Copies data files | No (references source files) | Yes |
| Creation time/cost | Seconds, minimal storage | Proportional to data size |
| Independence | Writes are independent (new files go to clone), but reads of unchanged data depend on source files | Fully independent |
| Risk | Source VACUUM can remove files the clone needs | None from source |
| Incremental re-sync | Recreate | `CREATE OR REPLACE TABLE ... DEEP CLONE` copies only new/changed files |
| Typical use | Dev/test copies, trying a MERGE/OPTIMIZE safely, short-lived experiments | Backups, DR copies, archiving a point-in-time, migrating tables between locations |

**Details**
- Both can clone a specific version: `... CLONE src VERSION AS OF 42` or `TIMESTAMP AS OF`.
- Clones keep the schema, partitioning and clustering, constraints and most table properties. Clone history starts fresh, so the clone's time travel doesn't include the source's history.
- Deep clones are incremental when re-run into the same target. Only new or changed files are copied, which makes them efficient for scheduled backups.
- In Unity Catalog, shallow clones of managed tables have specific rules (supported in newer runtimes). Check the docs.
- Snowflake's zero-copy clone is closer to a shallow clone with copy-on-write, but it doesn't break on source cleanup, because Snowflake retains shared micro-partitions (see [Zero-copy cloning](/snowflake/snowflake-interview-questions/#7-zero-copy-cloning)).

<p class="qb-label">Example</p>

```sql
-- Dialect: Databricks SQL
-- Fast test copy to try a risky MERGE
CREATE OR REPLACE TABLE dev.sandbox.orders_test SHALLOW CLONE prod.silver.orders;

-- Nightly incremental backup into another catalog/location
CREATE OR REPLACE TABLE backup.silver.orders DEEP CLONE prod.silver.orders;

-- Freeze month-end state for audit
CREATE TABLE archive.finance.ledger_2026_03 DEEP CLONE prod.gold.ledger
  TIMESTAMP AS OF '2026-03-31 23:59:59';
```

<p class="qb-label">Real-world use case</p>

Before applying a schema migration to a 10 TB table, an engineer shallow-cloned it in seconds, ran and validated the migration on the clone, then applied it to production. A separate nightly deep clone to another region served as the DR copy.

<p class="qb-label">Common mistakes and red flags</p>

- Using shallow clones as backups.
- Keeping long-lived shallow clones while the source runs VACUUM.
- Expecting the clone to inherit the source's time-travel history.

<p class="qb-label">Follow-up questions</p>

- Does cloning copy permissions? — Grants are not copied by default; apply them on the clone.
- How do you check what a deep clone copied? — Clone operation metrics in `DESCRIBE HISTORY` (files and bytes copied).

</details>

### 37. Identity columns and constraints in Delta tables

<p class="qb-meta">Intermediate · Occasional · Delta Lake</p>

<p class="qb-label">Short answer</p>

Delta supports identity columns (`GENERATED ALWAYS AS IDENTITY` or `BY DEFAULT`), which produce unique but not necessarily consecutive values. It enforces `NOT NULL` and `CHECK` constraints on write. In Unity Catalog, primary and foreign key constraints are informational, not enforced, but help documentation and some query optimisations (with `RELY`). Generated columns compute values from other columns.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Identity columns**
- `id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1)`: values are assigned automatically, and users can't insert them (with `ALWAYS`).
- `GENERATED BY DEFAULT AS IDENTITY` allows explicit values, for example during migration.
- Values are **unique and increasing per writer, but may have gaps** and aren't strictly sequential across concurrent writers.
- Limitations: identity columns can't be added to existing tables via ALTER (create the table with them), and some concurrent-write patterns are restricted. Many teams prefer **hash-based surrogate keys** for deterministic, rerunnable pipelines (see [Surrogate keys vs natural keys](/career/data-engineering-interview-questions/#5-surrogate-keys-vs-natural-keys)).

**Constraints**
| Constraint | Enforced? | Notes |
|---|---|---|
| `NOT NULL` | Yes | Writes violating it fail |
| `CHECK (expr)` | Yes | `ALTER TABLE t ADD CONSTRAINT c CHECK (amount >= 0)`; existing data must satisfy it |
| `PRIMARY KEY` / `FOREIGN KEY` (UC) | No (informational) | Shown in catalog/ERDs; `RELY` lets optimizer use them for rewrites |
| `UNIQUE` | Not supported as enforced | Use tests/dedupe logic |

**Generated columns**
- `event_date DATE GENERATED ALWAYS AS (CAST(event_ts AS DATE))` is computed on write, and is useful for partition-like columns from timestamps.
- Delta can use generated-column relationships for partition pruning when filters target the source column.

**Practical guidance**
- Enforce critical invariants with CHECK and NOT NULL in silver and gold. They fail fast on bad writes.
- Uniqueness still needs pipeline logic (MERGE, dedupe) plus tests.
- Use informational PK/FK constraints to document the model, and enable BI tools' relationship detection.

<p class="qb-label">Example</p>

```sql
-- Dialect: Databricks SQL
CREATE TABLE prod.gold.dim_store (
  store_key   BIGINT GENERATED ALWAYS AS IDENTITY,
  store_id    STRING NOT NULL,
  store_name  STRING,
  region      STRING NOT NULL,
  opened_on   DATE,
  CONSTRAINT pk_dim_store PRIMARY KEY (store_key) RELY
);

ALTER TABLE prod.silver.orders ADD CONSTRAINT non_negative_amount CHECK (amount >= 0);

-- This write fails because it violates the CHECK constraint
-- INSERT INTO prod.silver.orders (order_id, amount) VALUES (1, -5);
```

<p class="qb-label">Real-world use case</p>

A refund bug started writing negative quantities into silver orders. A `CHECK (quantity >= 0)` constraint made the job fail immediately with a clear error, instead of corrupting gold metrics silently.

<p class="qb-label">Common mistakes and red flags</p>

- Assuming PK constraints prevent duplicates.
- Expecting identity values to be gap-free.
- Adding CHECK constraints when existing data violates them (the ALTER fails).

<p class="qb-label">Follow-up questions</p>

- Identity vs hash keys? — Identity is compact but non-deterministic across rebuilds; hash keys are reproducible.
- What does RELY do? — Tells the optimizer it can trust the constraint for query optimisations (e.g. join elimination).

</details>

### 38. Serverless compute for jobs and notebooks

<p class="qb-meta">Intermediate · Common · Compute</p>

<p class="qb-label">Short answer</p>

Serverless compute for notebooks, jobs and pipelines runs on Databricks-managed infrastructure. It starts in seconds and autoscales automatically, with no cluster configuration, instance types or runtime upgrades to manage. You pick a performance mode (standard or performance-optimised) and optionally a budget policy for cost attribution, and pay serverless DBU rates that include infrastructure. Some features, such as RDD APIs and custom Spark configurations, are limited.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Benefits**
- **Fast start:** no 3–7 minute cluster spin-up, which helps short and frequent jobs.
- **No tuning:** Databricks chooses and scales resources (including Photon).
- **Automatic upgrades:** a versionless runtime, with client library versions pinned through **environments**.
- **Security:** isolated per workspace, governed by Unity Catalog.
- **Cost attribution:** budget policies add tags to serverless usage.

**Modes**
- **Performance-optimised:** faster start and execution, at a higher cost.
- **Standard:** lower cost with slightly longer start-up, suited to scheduled batch jobs (check availability).

**Limitations to know** (vary by release)
- No RDD APIs, and limited Spark configuration (only supported confs).
- Libraries come through environment specs (`%pip` or job environments) rather than cluster init scripts.
- No GPUs for general serverless jobs (separate serverless GPU offerings exist).
- Networking to private resources needs network connectivity configurations and private endpoints configured on the Databricks side.
- Must use Unity Catalog. Legacy Hive metastore patterns, DBFS mounts and some data sources aren't supported.

**When to choose classic job compute instead**
- Long-running, steady, heavy workloads where tuned clusters with spot instances are cheaper.
- Unsupported APIs or libraries, custom init scripts, GPUs, or special networking.

<p class="qb-label">Example</p>

```yaml
# Declarative Automation Bundle: serverless job task with an environment
resources:
  jobs:
    hourly_enrichment:
      name: hourly_enrichment
      schedule:
        quartz_cron_expression: "0 0 * * * ?"
        timezone_id: "UTC"
      environments:
        - environment_key: default
          spec:
            client: "2"
            dependencies:
              - requests==2.32.3
      tasks:
        - task_key: enrich
          environment_key: default
          spark_python_task:
            python_file: ../src/enrich.py
```
No `new_cluster` or `job_cluster_key` is given, so the task runs on serverless compute. The `client` value selects the serverless environment version. Use the version your workspace supports.

<p class="qb-label">Real-world use case</p>

300 small hourly jobs each spent about 5 minutes starting job clusters for 2 minutes of work. Moving them to serverless cut end-to-end latency by about 70% and total cost by about 30%, because start-up time was no longer billed.

<p class="qb-label">Common mistakes and red flags</p>

- Moving RDD-based code to serverless without refactoring.
- Ignoring budget policies, so serverless spend is unattributed.
- Assuming serverless is always cheaper for 24/7 heavy jobs.

<p class="qb-label">Follow-up questions</p>

- How do you install libraries on serverless? — Environment dependencies or notebook `%pip`; no init scripts.
- Can serverless access on-prem databases? — Via configured private connectivity (network connectivity configs/private endpoints), subject to setup.

</details>

## Advanced Databricks interview questions (experienced)

### 39. Optimistic concurrency control and write conflicts in Delta

<p class="qb-meta">Advanced · Common · Delta Lake Internals</p>

<p class="qb-label">Short answer</p>

Delta writers don't lock the table. Each one reads a snapshot, writes new files, then tries to commit the next log version. If another commit landed in between, Delta checks whether the two transactions logically conflict, for example by touching the same files or partitions. Non-conflicting commits are retried automatically. Conflicting ones fail with exceptions such as `ConcurrentAppendException` or `ConcurrentDeleteReadException`.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Process**
1. Read the table at version N and record what was read (files and predicates).
2. Write new data files.
3. Attempt to commit N+1.
4. If N+1 already exists, read the intervening commits and check for conflicts against what this transaction read and wrote.
5. No conflict: rebase and commit as the next version. Conflict: throw an exception.

**Isolation levels**
- **WriteSerializable** (default): blind appends never conflict with other writes. Other operations are checked, giving serializable writes while reads see snapshots.
- **Serializable** (stricter): even reads in a transaction must be serializable with concurrent writes. More conflicts occur.

**Typical conflicts**
| Exception | Cause |
|---|---|
| `ConcurrentAppendException` | Another job added files to the partition/data this operation read (e.g. two MERGEs on the same partition) |
| `ConcurrentDeleteReadException` | Another job deleted files this operation read |
| `ConcurrentDeleteDeleteException` | Both jobs deleted the same file |
| `MetadataChangedException` | Concurrent schema/property change |
| `ProtocolChangedException` | Concurrent protocol upgrade |

**Reducing conflicts**
- Make operations touch **disjoint data**: partition or cluster by the key you write by, and include explicit predicates in MERGE conditions (`t.date = '2026-03-01'`) so Delta knows the scope.
- **Row-level concurrency** on Databricks (with deletion vectors and liquid clustering, in supported runtimes) detects conflicts at row level instead of file level, so concurrent MERGE, UPDATE and DELETE on different rows of the same files can succeed.
- Serialise writers to the same table through orchestration.
- Retry with backoff on transient conflicts.

<p class="qb-label">Example</p>

```python
# Runtime: PySpark 3.5 on Databricks (delta-spark API)
import time
from delta.exceptions import ConcurrentAppendException
from delta.tables import DeltaTable

def merge_with_retry(spark, updates_df, max_attempts=3):
    for attempt in range(1, max_attempts + 1):
        try:
            (DeltaTable.forName(spark, "main.sales.orders").alias("t")
                .merge(updates_df.alias("s"),
                       "t.order_id = s.order_id AND t.order_date = s.order_date")  # scope predicate
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute())
            return
        except ConcurrentAppendException:
            if attempt == max_attempts:
                raise
            time.sleep(2 ** attempt)
```

<p class="qb-label">Real-world use case</p>

Two hourly jobs merged into the same orders table and failed with `ConcurrentAppendException` several times a day. Adding a date predicate to both MERGE conditions (they touched different days) and enabling liquid clustering with row-level concurrency removed the conflicts.

<p class="qb-label">Common mistakes and red flags</p>

- Saying Delta uses table locks.
- Retrying blindly without scoping operations.
- Running many concurrent MERGEs on the same partitions by design.

<p class="qb-label">Follow-up questions</p>

- Do readers ever block? — No; readers use snapshots and never conflict with writers.
- Are appends safe in parallel? — Yes; blind appends don't conflict under WriteSerializable.

**In depth:** [How does Delta Lake MERGE work, and why do concurrent MERGEs fail?](/interview/delta-lake/delta-merge-internals-conflicts/)

</details>

### 40. Liquid Clustering vs partitioning vs Z-ORDER

<p class="qb-meta">Advanced · Very Common · Delta Lake Performance</p>

<p class="qb-label">Short answer</p>

Liquid clustering (`CLUSTER BY (cols)`) replaces Hive-style partitioning and Z-ORDER for Delta tables. It clusters data incrementally during OPTIMIZE, lets you change clustering keys without rewriting existing data, avoids small-file explosions from over-partitioning, and supports row-level concurrency. `CLUSTER BY AUTO` lets Databricks pick keys from query patterns. It is the recommended default layout for new tables.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

| Aspect | Partitioning | Z-ORDER | Liquid clustering |
|---|---|---|---|
| Defined at | Table creation (fixed) | Each OPTIMIZE run | Table property; changeable |
| Granularity | Directories per value | Within files/partitions | Within files, adaptive |
| Cardinality fit | Low (dates, region) | High | Low to high, multiple columns |
| Incremental | N/A | No (rewrites) | Yes (clusters new data on OPTIMIZE) |
| Change keys | Full rewrite | Full rewrite | `ALTER TABLE ... CLUSTER BY` — new data uses new keys; existing data reclustered over time |
| Small-file risk | High if over-partitioned | Low | Low |
| Concurrency | File-level conflicts | File-level | Row-level concurrency (with deletion vectors) |
| Works with each other | Partition + ZORDER | — | Not combinable with partitioning/ZORDER |

**When to use what**
- **New Delta tables on Databricks:** liquid clustering on the columns you most filter by, often a date plus one or two high-cardinality keys. Or use `CLUSTER BY AUTO` with predictive optimization.
- **Existing partitioned tables:** keep them if they work well. Migrate when partitions are skewed or over-granular, or when filter patterns change.
- **OSS or other engines:** check reader support. Liquid clustering is part of Delta Lake 3.x and later, but tooling support varies.

**Operational notes**
- Clustering happens when **OPTIMIZE** runs (manually, on schedule, or through predictive optimization). Some writes cluster on write when tables are small enough or in supported runtimes.
- Choose up to 4 clustering keys.
- Statistics must exist on the clustering columns. Databricks handles this for the keys.
- Iceberg tables in Unity Catalog support a similar clustering approach in newer releases.

<p class="qb-label">Example</p>

```sql
-- Dialect: Databricks SQL
CREATE TABLE main.events.page_views (
  event_id STRING, user_id BIGINT, page_id BIGINT, event_ts TIMESTAMP, event_date DATE
) CLUSTER BY (event_date, user_id);

-- Change keys later without rewriting history immediately
ALTER TABLE main.events.page_views CLUSTER BY (event_date, page_id);

-- Let Databricks choose keys based on workload (Unity Catalog managed tables)
ALTER TABLE main.events.sessions CLUSTER BY AUTO;

OPTIMIZE main.events.page_views;   -- incrementally clusters data
```

<p class="qb-label">Real-world use case</p>

A table partitioned by `(event_date, country)` produced millions of tiny files, because 200 countries × 3 years × hourly writes is a lot of folders. Recreating it with `CLUSTER BY (event_date, country)` cut the file count by 98% and improved most queries.

<p class="qb-label">Common mistakes and red flags</p>

- Combining liquid clustering with partitioning.
- Choosing too many clustering keys.
- Never running OPTIMIZE, so data never gets clustered.

<p class="qb-label">Follow-up questions</p>

- How do you migrate a partitioned table? — CTAS/`CREATE TABLE ... CLUSTER BY ... AS SELECT`, or `ALTER TABLE ... CLUSTER BY` on unpartitioned tables, then OPTIMIZE.
- Does liquid clustering help MERGE? — Yes; clustering on merge keys reduces files rewritten and, with row-level concurrency, conflicts.

**In depth:** [OPTIMIZE, Z-ORDER or liquid clustering: which would you use on a Delta table?](/interview/delta-lake/optimize-zorder-vs-liquid-clustering/)

</details>

### 41. Deletion vectors

<p class="qb-meta">Advanced · Occasional · Delta Lake Internals</p>

<p class="qb-label">Short answer</p>

Deletion vectors let Delta mark individual rows as deleted in a small side file (a bitmap), instead of rewriting the whole Parquet file on DELETE, UPDATE or MERGE. Writes become much faster (merge-on-read). Readers apply the bitmaps to filter deleted rows, and OPTIMIZE or REORG later rewrites files to purge them physically.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Without deletion vectors (copy-on-write)**
- Updating 1 row in a 1 GB file rewrites the entire file. Frequent small updates spread across many files cause huge write amplification.

**With deletion vectors**
- DELETE: write a bitmap marking row positions deleted in the affected files. The original files are untouched.
- UPDATE and MERGE: mark old rows deleted in the bitmap, and write the updated rows to new files.
- Readers combine the files and bitmaps, skipping deleted rows.
- Enabled with `delta.enableDeletionVectors = true`. On recent Databricks runtimes it is on by default for new tables (check workspace settings).

**Trade-offs**
| Aspect | Copy-on-write | Deletion vectors |
|---|---|---|
| Write speed for sparse updates | Slow | Fast |
| Read overhead | None | Small (apply bitmaps) |
| Storage until compaction | Old files kept for time travel | Old rows remain in files until rewrite |
| Physical removal | On rewrite | `OPTIMIZE`, `REORG TABLE ... APPLY (PURGE)`, then VACUUM |

**Implications**
- **Protocol:** enabling deletion vectors upgrades table features. Older readers or engines without support can't read the table. Check compatibility with external consumers (Delta Sharing recipients, other engines).
- **GDPR:** deleted rows still exist physically until files are rewritten and vacuumed. Run `REORG ... APPLY (PURGE)` and then VACUUM after erasures (see [How does GDPR "right to erasure" affect lake and warehouse design?](/career/data-engineering-interview-questions/#97-how-does-gdpr-right-to-erasure-affect-lake-and-warehouse-design)).
- **Row-level concurrency** builds on deletion vectors (see [Optimistic concurrency control and write conflicts in Delta](#39-optimistic-concurrency-control-and-write-conflicts-in-delta)).
- Iceberg v3 introduced a similar deletion-vector concept (see [Delta Lake vs Apache Iceberg vs Apache Hudi](/career/data-engineering-interview-questions/#89-delta-lake-vs-apache-iceberg-vs-apache-hudi)).

<p class="qb-label">Example</p>

```sql
-- Dialect: Databricks SQL
ALTER TABLE main.crm.contacts SET TBLPROPERTIES ('delta.enableDeletionVectors' = true);

DELETE FROM main.crm.contacts WHERE contact_id IN (SELECT contact_id FROM main.gov.erasure_requests);

-- Physically rewrite affected files to remove soft-deleted rows, then clean up old files
REORG TABLE main.crm.contacts APPLY (PURGE);
VACUUM main.crm.contacts;
```

<p class="qb-label">Real-world use case</p>

A CDC MERGE that updated about 0.5% of rows each hour rewrote around 300 GB per run because the updates were spread across files. With deletion vectors, each run wrote a few GB, and runtime dropped from 40 minutes to 6.

<p class="qb-label">Common mistakes and red flags</p>

- Assuming DELETE physically removes rows immediately.
- Enabling them without checking downstream reader compatibility.
- Never compacting, so bitmaps accumulate and reads slow down.

<p class="qb-label">Follow-up questions</p>

- What is merge-on-read? — Writes record changes cheaply; reads merge them with base data until compaction (see [Delta Lake vs Apache Iceberg vs Apache Hudi](/career/data-engineering-interview-questions/#89-delta-lake-vs-apache-iceberg-vs-apache-hudi)).
- How do you remove deletion vectors entirely? — `REORG TABLE ... APPLY (PURGE)`; disabling the feature may require dropping it via `ALTER TABLE ... DROP FEATURE`.

</details>

### 42. AUTO CDC (formerly APPLY CHANGES) for SCD Type 1/2 in pipelines

<p class="qb-meta">Advanced · Common · Lakeflow Declarative Pipelines</p>

<p class="qb-label">Short answer</p>

In Lakeflow Spark Declarative Pipelines (formerly DLT), `AUTO CDC` (previously called `APPLY CHANGES INTO`) applies a stream of change events to a target streaming table. You declare keys, a sequencing column, delete conditions and `STORED AS SCD TYPE 1` or `SCD TYPE 2`. The framework handles ordering, out-of-order events, deletes and SCD2 validity columns, so you don't hand-write MERGE logic. See [How do you design and load an SCD Type 2 dimension?](/career/data-engineering-interview-questions/#36-how-do-you-design-and-load-an-scd-type-2-dimension) for the underlying SCD2 design.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**What you declare**
- **Target:** a streaming table, created with `create_streaming_table()` or `CREATE STREAMING TABLE`.
- **Source:** a streaming source of change rows (CDC feed, CDF, Kafka).
- **KEYS:** the business key(s).
- **SEQUENCE BY:** an ordering column (commit timestamp or LSN). Late, out-of-order events are handled correctly.
- **APPLY AS DELETE WHEN:** the delete condition (`op = 'D'`).
- **APPLY AS TRUNCATE WHEN** (SCD1 only, optional).
- **COLUMNS / EXCEPT:** which columns to include.
- **STORED AS SCD TYPE 1 | 2**, with `TRACK HISTORY ON` columns for SCD2. Only changes in those columns create new versions.

**SCD2 output**
- Adds `__START_AT` and `__END_AT` columns based on the sequencing values. The current row has `__END_AT = NULL`.
- Late-arriving changes are slotted into the right place in history.

**Naming**
- Databricks renamed DLT to **Lakeflow Spark Declarative Pipelines** (2025) and `APPLY CHANGES` to **`AUTO CDC`**. Python API: `dp.create_auto_cdc_flow()` in the `pyspark.pipelines` module, with older `dlt.apply_changes()` still supported for compatibility. Expect both names in interviews.
- **AUTO CDC FROM SNAPSHOT** handles sources that provide periodic full snapshots instead of change events: it infers inserts, updates and deletes by comparing snapshots.

**Why it matters:** hand-written SCD2 MERGE logic is error-prone (ordering, deletes, late data). AUTO CDC makes it declarative and consistent across tables.

<p class="qb-label">Example</p>

```sql
-- Dialect: Databricks SQL (Lakeflow Spark Declarative Pipelines)
CREATE OR REFRESH STREAMING TABLE dim_customer_scd2;

CREATE FLOW customers_cdc_flow AS
AUTO CDC INTO dim_customer_scd2
FROM STREAM(main.bronze.customer_cdc)
KEYS (customer_id)
APPLY AS DELETE WHEN op = 'D'
SEQUENCE BY commit_ts
COLUMNS * EXCEPT (op, _ingested_at)
STORED AS SCD TYPE 2
TRACK HISTORY ON segment, city;
```
Sample: customer 9 has events at commit_ts 10:00 (segment SMB), 10:20 (city change only, tracked), and a late event at 10:10 (segment MID). The result is three versions ordered by commit_ts, with the 10:10 version inserted between the others.

<p class="qb-label">Real-world use case</p>

A team maintained 25 SCD2 dimensions with custom MERGE notebooks and kept hitting ordering bugs. Migrating to AUTO CDC flows in one pipeline removed about 3,000 lines of code, and late events were handled correctly.

<p class="qb-label">Common mistakes and red flags</p>

- No SEQUENCE BY, or a sequencing column that isn't monotonic per key.
- Tracking history on volatile columns, which explodes the version count.
- Using the old name without knowing the new one, or vice versa.

<p class="qb-label">Follow-up questions</p>

- How do you query current rows? — Filter `__END_AT IS NULL`.
- How does this compare to dbt snapshots? — Same SCD2 idea; dbt snapshots compare state on each run, AUTO CDC applies streamed change events (see [Snapshots: timestamp vs check strategy (See also Q12)](/etl-elt/dbt-interview-questions/#17-snapshots-timestamp-vs-check-strategy-see-also-q12)).

</details>

### 43. Row filters and column masks in Unity Catalog

<p class="qb-meta">Advanced · Common · Unity Catalog Security</p>

<p class="qb-label">Short answer</p>

A row filter is a SQL UDF returning BOOLEAN that is attached to a table. Rows where it returns FALSE are hidden from the querying user. A column mask is a UDF that returns the value to show, such as the real value or a redacted one. Both use functions like `is_account_group_member()`, so one table can serve many audiences securely. Newer ABAC policies apply filters and masks automatically through governed tags.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Row filters**
- `CREATE FUNCTION f(region STRING) RETURN is_account_group_member('global_sales') OR region = ...`
- `ALTER TABLE t SET ROW FILTER f ON (region);`
- Evaluated for every query. Users can't bypass them.
- Can reference mapping tables (user or group to allowed values) for dynamic rules.

**Column masks**
- `CREATE FUNCTION mask_email(email STRING) RETURN CASE WHEN is_account_group_member('pii_readers') THEN email ELSE regexp_replace(email, '^[^@]+', '***') END;`
- `ALTER TABLE t ALTER COLUMN email SET MASK mask_email;`
- Masks can take extra columns as arguments (`USING COLUMNS (country)`) for conditional masking.

**ABAC (attribute-based access control) policies**
- Define policies at catalog or schema level that apply row filters or column masks to any table or column with matching **governed tags** (for example `pii = email`). New tagged columns are protected automatically. This is in public preview or GA depending on release, so check current docs.

**Functions for identity**
- `current_user()`, `is_account_group_member('group')` (account-level groups, recommended), and `is_member()` (workspace-local groups, legacy).

**Considerations**
- Supported compute: Unity Catalog-enabled compute in standard (shared) or dedicated access mode with fine-grained access control, SQL warehouses, and serverless. Check your runtime.
- Performance: keep functions simple, and use mapping tables that are small and well clustered.
- Filters and masks also apply to Delta Sharing recipients and downstream views in specific ways. Test them.
- Dynamic views (views with CASE and `is_account_group_member`) are the older pattern and still work, but filters and masks protect the base table itself.

<p class="qb-label">Example</p>

```sql
-- Dialect: Databricks SQL
CREATE OR REPLACE FUNCTION prod.gov.region_filter(row_region STRING)
RETURN is_account_group_member('sales_global')
    OR EXISTS (SELECT 1 FROM prod.gov.user_regions ur
               WHERE ur.user_email = current_user() AND ur.region = row_region);

ALTER TABLE prod.gold.fct_orders SET ROW FILTER prod.gov.region_filter ON (region);

CREATE OR REPLACE FUNCTION prod.gov.mask_email(email STRING)
RETURN CASE WHEN is_account_group_member('pii_readers') THEN email
            ELSE regexp_replace(email, '^[^@]+', '***') END;

ALTER TABLE prod.gold.dim_customer ALTER COLUMN email SET MASK prod.gov.mask_email;
```

<p class="qb-label">Real-world use case</p>

A global retailer replaced 40 regional views with one row filter backed by a user-to-region mapping table, plus column masks for customer contact details. Access reviews now cover two functions instead of 40 views.

<p class="qb-label">Common mistakes and red flags</p>

- Using workspace-local `is_member()` in multi-workspace setups.
- Complex, slow filter functions.
- Assuming filters apply on unsupported compute modes.

<p class="qb-label">Follow-up questions</p>

- Can owners bypass row filters? — No special bypass; design the function to allow admin groups explicitly if needed.
- How do you test them? — Query as users in different groups, or impersonate via service principals in test environments.

</details>

### 44. Auto Loader schema inference, evolution and rescued data

<p class="qb-meta">Advanced · Common · Ingestion</p>

<p class="qb-label">Short answer</p>

Auto Loader samples files to infer a schema, stores it under `cloudFiles.schemaLocation`, and tracks changes over time. When new columns appear, the default mode `addNewColumns` stops the stream with an error, and on restart the column is added. Values that don't fit the schema (type mismatches, unexpected fields) can be kept in the `_rescued_data` column instead of being lost.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Inference**
- Samples up to a configurable number of files or bytes (`cloudFiles.schemaInference.sampleSize.numFiles` / `numBytes`).
- For JSON and CSV, all columns are inferred as **strings** by default unless `cloudFiles.inferColumnTypes = true`. This avoids type conflicts across files.
- `cloudFiles.schemaHints` fixes the types of specific columns: `"amount DECIMAL(12,2), order_ts TIMESTAMP"`.

**Evolution modes (`cloudFiles.schemaEvolutionMode`)**
| Mode | Behaviour on new columns |
|---|---|
| `addNewColumns` (default when no schema provided) | Stream fails with `UnknownFieldException`; schema location updated; restart picks up new column |
| `rescue` | Schema never evolves; new/unexpected fields go into `_rescued_data` |
| `failOnNewColumns` | Stream fails; manual schema update needed |
| `none` | Ignore new columns (not rescued unless rescuedDataColumn set) |

**Rescued data column**
- `_rescued_data` (a JSON string) holds fields not in the schema, values that failed type conversion, and case-mismatched columns, along with the source file path.
- It lets ingestion continue without silent loss. Monitor it: a growing count of non-null `_rescued_data` signals upstream drift.

**Operating evolution in production**
- Run the stream in a **job with retries** in `addNewColumns` mode. The first attempt fails on a new column, and the retry continues with the evolved schema. Lakeflow pipelines handle this restart automatically.
- Bronze tables should allow schema merges. Silver should select explicit columns, so changes are deliberate (see [Schema enforcement vs schema evolution (mergeSchema)](#4-schema-enforcement-vs-schema-evolution-mergeschema)).
- Type changes upstream (int to string) won't evolve automatically, and the values show up in rescued data.

<p class="qb-label">Example</p>

```python
# Runtime: PySpark 3.5 on Databricks
stream = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/Volumes/prod/raw/_schemas/events")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaHints", "amount DECIMAL(12,2), event_ts TIMESTAMP")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .load("/Volumes/prod/raw/landing/events/"))

(stream.writeStream
    .option("checkpointLocation", "/Volumes/prod/raw/_checkpoints/bronze_events")
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable("prod.bronze.events"))
```
Sample: a file arrives with `"amount": "N/A"`. The `amount` column becomes NULL for that row, and `_rescued_data` contains `{"amount":"N/A","_file_path":"..."}`.

<p class="qb-label">Real-world use case</p>

An upstream team started sending `user_id` as a string UUID instead of an integer. Instead of the stream crashing or silently nulling data, the values landed in `_rescued_data`. A daily monitor flagged the rise, and the team agreed a migration to a string column.

<p class="qb-label">Common mistakes and red flags</p>

- No retries around `addNewColumns`, so the job stays failed.
- Ignoring `_rescued_data`.
- Letting inference type everything as strings forever without hints or casts downstream.

<p class="qb-label">Follow-up questions</p>

- Where is the schema history? — In `_schemas` under `schemaLocation`, versioned.
- How do you force a known schema? — Provide `.schema(...)` explicitly; then evolution doesn't occur (use rescue for extras).

</details>

### 45. Monitoring spend with system.billing tables

<p class="qb-meta">Advanced · Occasional · Cost Optimisation</p>

<p class="qb-label">Short answer</p>

`system.billing.usage` records DBU usage per hour per SKU, with rich metadata: workspace, job ID, warehouse ID, cluster ID, pipeline ID, endpoint, user, custom tags and product. Join it to `system.billing.list_prices` to estimate list-price cost, and to `system.lakeflow` or `system.compute` tables for names. This is the foundation for chargeback, anomaly alerts and cost dashboards.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Key columns in `system.billing.usage`**
- `usage_date`, `usage_start_time`, `usage_end_time`, `workspace_id`, `sku_name`, `cloud`, `usage_quantity`, `usage_unit` (DBU and others).
- `usage_metadata`: a struct with `job_id`, `job_run_id`, `cluster_id`, `warehouse_id`, `dlt_pipeline_id`, `endpoint_name`, `notebook_id` and more.
- `identity_metadata`: `run_as` and `owned_by`, which identify the user or service principal.
- `custom_tags`: from compute policies, jobs or budget policies.
- `billing_origin_product`: JOBS, SQL, DLT/pipelines, ALL_PURPOSE, MODEL_SERVING, PREDICTIVE_OPTIMIZATION and so on.
- `product_features`: for example whether Photon or serverless was used.

**`system.billing.list_prices`:** SKU price history (`price_start_time`, `price_end_time`, `pricing.default`). Join on SKU and time range. List prices don't reflect negotiated discounts, so apply your contract rate separately.

**Useful analyses**
1. Cost per job, joined to `system.lakeflow.jobs` for names and owners. Find the top 20 jobs by monthly cost.
2. Cost per team from `custom_tags['team']`, which also reveals untagged spend.
3. Anomaly detection: day-over-day spikes per workspace or SKU, alerted through a Databricks SQL alert.
4. All-purpose vs jobs vs serverless mix over time.
5. Cost per pipeline run, as a unit-economics trend (see [FinOps: tagging, chargeback and showback](/career/data-engineering-interview-questions/#72-finops-tagging-chargeback-and-showback)).

**Access:** system tables must be enabled (the billing schema is typically available by default) and granted carefully. Billing data is account-wide.

<p class="qb-label">Example</p>

```sql
-- Dialect: Databricks SQL — top jobs by estimated list cost, last 30 days
WITH usage AS (
  SELECT u.usage_metadata.job_id AS job_id,
         u.sku_name, u.usage_quantity, u.usage_start_time, u.workspace_id
  FROM system.billing.usage u
  WHERE u.usage_date >= current_date() - INTERVAL 30 DAYS
    AND u.usage_metadata.job_id IS NOT NULL
),
priced AS (
  SELECT us.job_id, us.workspace_id,
         us.usage_quantity * lp.pricing.default AS est_list_cost
  FROM usage us
  JOIN system.billing.list_prices lp
    ON lp.sku_name = us.sku_name
   AND us.usage_start_time >= lp.price_start_time
   AND (lp.price_end_time IS NULL OR us.usage_start_time < lp.price_end_time)
)
SELECT p.job_id, j.name AS job_name, ROUND(SUM(p.est_list_cost), 2) AS est_cost_usd
FROM priced p
LEFT JOIN (
  SELECT workspace_id, job_id, name,
         ROW_NUMBER() OVER (PARTITION BY workspace_id, job_id ORDER BY change_time DESC) AS rn
  FROM system.lakeflow.jobs
) j ON j.job_id = p.job_id AND j.workspace_id = p.workspace_id AND j.rn = 1
GROUP BY p.job_id, j.name
ORDER BY est_cost_usd DESC
LIMIT 20;
```
`system.lakeflow.jobs` is an SCD-style table, so the ROW_NUMBER keeps the latest name per job.

<p class="qb-label">Real-world use case</p>

A daily cost-anomaly alert on `system.billing.usage` flagged a 4× spike in one workspace. It traced to a job whose `max_workers` had been raised to 200 by mistake, and the team reverted it the same morning.

<p class="qb-label">Common mistakes and red flags</p>

- Using list price as actual cost without the contract discount.
- Ignoring untagged usage.
- Joining job names without handling the SCD history in jobs tables.

<p class="qb-label">Follow-up questions</p>

- How do you include cloud VM costs? — From your cloud cost export (tags propagate to VMs on classic compute).
- How fresh is billing data? — Updated throughout the day with some latency; not real-time.

</details>

### 46. Lakehouse Federation

<p class="qb-meta">Advanced · Occasional · Unity Catalog</p>

<p class="qb-label">Short answer</p>

Lakehouse Federation lets Unity Catalog register external databases as foreign catalogs through a connection. Supported sources include PostgreSQL, MySQL, SQL Server, Oracle, Snowflake, Redshift, BigQuery and others. Users can query them with Databricks SQL, joined with lakehouse tables, under UC permissions and lineage, without first building ingestion pipelines. Catalog federation extends this to external catalogs such as Hive metastores and AWS Glue.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**How it works**
1. `CREATE CONNECTION` stores the host, port and credentials securely in UC.
2. `CREATE FOREIGN CATALOG ... USING CONNECTION ... OPTIONS (database '...')` mirrors the external database's schemas and tables.
3. Grant access with normal UC privileges. Queries push down filters, projections and some aggregations and joins to the source where supported.

**Good uses**
- Ad-hoc exploration of operational data before deciding to ingest it.
- Small reference or lookup tables joined to lakehouse data.
- Migration periods, querying the legacy warehouse alongside new tables.
- Proof-of-concept reports with low query volume.

**Not a replacement for ingestion when**
- Queries are frequent or heavy. Every query hits the source database's compute and network, which risks OLTP impact.
- You need history, time travel, or consistent snapshots across sources.
- Latency or availability must be independent of the source system.
- For those cases, ingest via Lakeflow Connect or CDC (see [Lakeflow Connect managed ingestion](#48-lakeflow-connect-managed-ingestion), [CDC from a PostgreSQL OLTP database into the warehouse](/career/scenario-based-data-engineering-interview-questions/#41-cdc-from-a-postgresql-oltp-database-into-the-warehouse)). Materialized views over federated tables can also cache results.

**Governance:** queries are audited in UC, lineage captures foreign tables, and access is centrally controlled. Connection credentials should use dedicated read-only users on the source.

<p class="qb-label">Example</p>

```sql
-- Dialect: Databricks SQL
CREATE CONNECTION pg_orders_conn TYPE postgresql
OPTIONS (
  host 'orders-db.internal.acme.example',
  port '5432',
  user secret('federation', 'pg_user'),
  password secret('federation', 'pg_password')
);

CREATE FOREIGN CATALOG pg_orders USING CONNECTION pg_orders_conn OPTIONS (database 'orders');

GRANT USE CATALOG, USE SCHEMA, SELECT ON CATALOG pg_orders TO `analysts`;

SELECT o.order_id, o.status, c.segment
FROM pg_orders.public.orders o
JOIN prod.gold.dim_customer c ON c.customer_id = o.customer_id
WHERE o.created_at >= current_date() - INTERVAL 1 DAY;
```

<p class="qb-label">Real-world use case</p>

During a migration from Redshift, analysts queried Redshift tables through a foreign catalog next to new Delta tables. The team then prioritised ingesting the federated tables that showed the heaviest query use in UC audit logs.

<p class="qb-label">Common mistakes and red flags</p>

- Pointing heavy dashboards at production OLTP databases through federation.
- Using high-privilege source accounts.
- Assuming every operation pushes down.

<p class="qb-label">Follow-up questions</p>

- How do you see what pushed down? — Query profile/EXPLAIN shows the remote query.
- Federation vs Delta Sharing? — Federation queries other databases; Delta Sharing shares Delta-based data between organisations.

</details>

### 47. Preventing small files from streaming and frequent writes

<p class="qb-meta">Advanced · Common · Delta Lake Performance</p>

<p class="qb-label">Short answer</p>

Streaming and frequent micro-batches write many small files per commit, especially with many partitions or high shuffle parallelism. Prevent this with optimized writes and auto compaction, longer trigger intervals or `availableNow` runs, fewer output partitions (liquid clustering instead of fine partitioning), and right-sized shuffle partitions. Clean up with scheduled OPTIMIZE or predictive optimization. See [What is the small-files problem, and how do you fix it?](/career/data-engineering-interview-questions/#55-what-is-the-small-files-problem-and-how-do-you-fix-it) for the general problem.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Root causes on Databricks**
- Every micro-batch is a commit with at least one file per writing task, per partition touched.
- `spark.sql.shuffle.partitions` set high (for example 200 or more) for small micro-batches gives hundreds of tiny files per batch.
- Hive-style partitioning by date and hour, or by a high-cardinality column, multiplies the file count.
- Many concurrent small appends from several jobs.

**Prevention**
| Technique | Effect |
|---|---|
| `delta.autoOptimize.optimizeWrite = true` | Shuffle to fewer, larger files at write time |
| `delta.autoOptimize.autoCompact = true` | Post-write compaction of small files |
| Longer trigger (`processingTime='5 minutes'`) or scheduled `availableNow` | Bigger micro-batches, fewer commits |
| Lower shuffle partitions for streaming aggregations (set before first run; stored in checkpoint) or AQE coalescing | Fewer output tasks |
| Liquid clustering instead of fine-grained partitions | Avoid per-partition file fan-out |
| `foreachBatch` with `coalesce(n)` before write | Explicit control of files per batch |
| Lakeflow pipelines on serverless | Auto-tuned file sizes and optimisation |

**Cleanup**
- Scheduled `OPTIMIZE` (recent data only, or the whole table with liquid clustering) or **predictive optimization** (see [Predictive optimization](#20-predictive-optimization)).
- `VACUUM` to delete the small files after compaction (see [VACUUM: default retention and the risks](#17-vacuum-default-retention-and-the-risks)).

**Streaming-specific gotcha:** for stateful streaming queries, the number of shuffle partitions is fixed in the checkpoint at first start. Changing `spark.sql.shuffle.partitions` later has no effect on that query unless you start a new checkpoint.

<p class="qb-label">Example</p>

```python
# Runtime: PySpark 3.5 on Databricks
spark.sql("""
  ALTER TABLE prod.bronze.device_events SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true')
""")

def write_batch(batch_df, batch_id):
    (batch_df.coalesce(8)                      # cap files per micro-batch
             .write.mode("append")
             .saveAsTable("prod.bronze.device_events"))

(spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/Volumes/prod/raw/_schemas/device_events")
    .load("/Volumes/prod/raw/landing/device_events/")
    .writeStream
    .foreachBatch(write_batch)
    .option("checkpointLocation", "/Volumes/prod/raw/_checkpoints/device_events")
    .trigger(processingTime="2 minutes")
    .start())
```
Appends in `foreachBatch` are not automatically idempotent across retries of the same batch. For exactly-once, use Delta's idempotent writes (`txnAppId`/`txnVersion` write options set from the batch ID) or MERGE on an event ID (see [foreachBatch for upserts and multiple sinks](/pyspark/pyspark-interview-questions/#36-foreachbatch-for-upserts-and-multiple-sinks)).

<p class="qb-label">Real-world use case</p>

An IoT bronze table written every 10 seconds, partitioned by `date` and `device_type` (300 types), produced about 2.5 million files a month. Moving to liquid clustering, a 2-minute trigger and auto compaction kept the file count stable at around 15,000.

<p class="qb-label">Common mistakes and red flags</p>

- Changing shuffle partitions mid-life for a stateful stream and expecting effect.
- No compaction strategy at all.
- `coalesce(1)` on large batches (bottleneck).

<p class="qb-label">Follow-up questions</p>

- How do you measure? — `DESCRIBE DETAIL` numFiles and average size; table history `numOutputFiles` per batch.
- Does Photon help with small files? — Somewhat on reads, but layout fixes are the real solution.

</details>

### 48. Lakeflow Connect managed ingestion

<p class="qb-meta">Advanced · Occasional · Ingestion</p>

<p class="qb-label">Short answer</p>

Lakeflow Connect is Databricks' family of managed ingestion connectors for SaaS applications (Salesforce, Workday, ServiceNow, Google Analytics and others) and databases (SQL Server, PostgreSQL, MySQL and others, using CDC). It creates serverless ingestion pipelines that land data incrementally into Unity Catalog streaming tables, with schema evolution, monitoring and governance built in. It reduces the need for third-party ELT tools or custom connectors.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Components**
- **Connection** (UC securable): credentials and connection details for the source.
- **Ingestion gateway** (for databases): captures changes from the source, using CDC or change tracking for SQL Server, logical replication for PostgreSQL, and so on, and stages them.
- **Ingestion pipeline:** a serverless Lakeflow pipeline that applies the changes to destination streaming tables (SCD Type 1 by default, with SCD Type 2 options for some connectors).
- Also: **standard connectors** (Auto Loader, Kafka, Kinesis and others) and **Zerobus**-style direct-write ingestion for event data (newer). Check current availability.

**Why use it**
- Fewer moving parts: no separate ELT vendor, and lineage and governance stay in UC.
- Incremental, CDC-based loads with deletes handled.
- Schema evolution handling, plus monitoring through pipeline event logs and system tables.
- Serverless compute, so no cluster management.

**Considerations**
- **Connector coverage and maturity vary.** Some connectors are GA and others in preview. Check limits (tables per pipeline, supported objects, history modes).
- **Source impact and prerequisites:** enabling CDC or change tracking on SQL Server, replication slots on PostgreSQL, API quotas on SaaS sources.
- **Cost:** serverless pipeline DBUs, plus gateway compute for database connectors.
- **Alternatives:** Fivetran, Airbyte, Debezium + Kafka (see [Design a log-based CDC pipeline with Debezium and Kafka](/career/data-engineering-interview-questions/#87-design-a-log-based-cdc-pipeline-with-debezium-and-kafka)), and cloud-native DMS-style tools. Choose based on coverage, cost and operational preferences.

<p class="qb-label">Example</p>

```yaml
# Declarative Automation Bundle excerpt: Lakeflow Connect ingestion pipeline (illustrative structure)
resources:
  pipelines:
    salesforce_ingest:
      name: salesforce_ingest
      catalog: prod
      schema: bronze_salesforce
      ingestion_definition:
        connection_name: salesforce_prod_conn
        objects:
          - table:
              source_schema: objects
              source_table: Account
              destination_catalog: prod
              destination_schema: bronze_salesforce
          - table:
              source_schema: objects
              source_table: Opportunity
              destination_catalog: prod
              destination_schema: bronze_salesforce
```
Field names follow the pipelines API `ingestion_definition` structure, which differs by connector type. Generate the exact spec from the UI or docs for your connector.

<p class="qb-label">Real-world use case</p>

A company replaced a third-party ELT tool for Salesforce and a nightly SQL Server dump with Lakeflow Connect. Salesforce objects and SQL Server CDC now land in governed bronze streaming tables with lineage, and the extra tool subscription was retired.

<p class="qb-label">Common mistakes and red flags</p>

- Assuming every SaaS object or API is supported.
- Not preparing source CDC prerequisites.
- Ignoring API quota impact on SaaS sources.

<p class="qb-label">Follow-up questions</p>

- How are deletes handled? — Via CDC delete events (databases) or connector-specific soft/hard delete handling (SaaS).
- How do you monitor freshness? — Pipeline event logs/system tables and alerts on last successful update.

</details>

### 49. Databricks vs Snowflake: how do you choose?

<p class="qb-meta">Advanced · Common · Platform Comparison</p>

<p class="qb-label">Short answer</p>

Both are lakehouse-style platforms with SQL warehousing, data engineering, governance, sharing and AI features, and both now support open table formats. Databricks is Spark-native and strongest for code-heavy data engineering, streaming and ML, with data in your own storage in open formats. Snowflake is SQL-first with a fully managed experience and simple operations, strong for warehousing, sharing and BI. Choose by workloads, team skills, openness needs, governance and total cost.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

| Dimension | Databricks | Snowflake |
|---|---|---|
| Core engine | Apache Spark + Photon; SQL warehouses | Proprietary MPP SQL engine; Snowpark for Python/Java/Scala |
| Storage | Your cloud storage, Delta/Iceberg (open) | Snowflake-managed (native) or Iceberg in your storage |
| Primary persona | Data engineers, data scientists, ML engineers (plus analysts via SQL) | Analysts, analytics engineers, data engineers (SQL-first) |
| Streaming | Structured Streaming, Lakeflow pipelines (mature) | Snowpipe Streaming, Dynamic Tables (minutes latency) |
| ML/AI | MLflow, Feature Store, Model Serving, Mosaic AI, vector search | Cortex AI functions, Snowflake ML, Cortex Search/Analyst |
| Governance | Unity Catalog (also OSS) | Horizon (RBAC, policies, tagging, lineage) |
| Sharing | Delta Sharing (open protocol), Marketplace, Clean Rooms | Secure Data Sharing, Listings/Marketplace, Clean Rooms |
| Ops model | More knobs (compute types, clusters) — serverless reducing this | Minimal ops; warehouses + serverless features |
| Cost model | DBUs (+ cloud VMs for classic) | Credits (compute) + storage |

**How I'd decide in an interview**
1. **Workload mix:** mostly SQL BI and ELT? Snowflake is often simpler. Heavy Python, Spark, streaming and ML? Databricks often fits better.
2. **Team skills:** a SQL-centric analytics team or a software-engineering-heavy data team.
3. **Openness and lock-in:** a requirement to keep data in open formats in your own buckets for multiple engines. Both support Iceberg now, so compare maturity for your use.
4. **Existing ecosystem:** cloud provider, BI tools, existing contracts and skills.
5. **Governance and compliance:** both strong. Check specific needs (private networking, CMK, regions).
6. **Total cost:** benchmark representative workloads on both, including people and operations cost.

**Coexistence is common:** for example, Databricks for ingestion, streaming and ML feature pipelines, and Snowflake for enterprise BI, connected through Iceberg tables or sharing. Avoid duplicating data and logic without a clear boundary.

<p class="qb-label">Real-world use case</p>

A retailer kept Snowflake for finance and BI (SQL-heavy, many analysts) and adopted Databricks for real-time inventory streaming and demand-forecasting ML. Shared gold datasets are exposed as Iceberg tables both platforms can read.

<p class="qb-label">Common mistakes and red flags</p>

- Brand loyalty answers ("X is always better").
- Outdated claims ("Snowflake can't do Python", "Databricks can't do BI").
- Ignoring operations and people costs.

<p class="qb-label">Follow-up questions</p>

- How would you run a fair benchmark? — Same data, representative queries/jobs, warm and cold runs, measure cost and time, include concurrency.
- How do you avoid lock-in? — Open table formats, portable transformation code (dbt/SQL), and governance catalogs with open APIs.

</details>

### 50. Troubleshooting a slow Databricks job end to end

<p class="qb-meta">Advanced · Common · Performance Troubleshooting</p>

<p class="qb-label">Short answer</p>

Compare the slow run with a good run: data volume, code changes, cluster size, runtime version. Then use the Spark UI or query profile to find the slowest stage and its cause: data skew (one long task), spill (memory), excessive shuffle, too many small files or poor data skipping, wrong join strategy, Python UDFs, or cluster issues (autoscaling waits, spot loss). Fix the biggest cause first, and re-measure.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Step 1: establish the baseline**
- Run history in Lakeflow Jobs shows duration trends. When did it slow down? Correlate with deployments, data growth and runtime upgrades.
- Check input sizes (rows and bytes read) between a fast and a slow run.

**Step 2: locate the bottleneck**
- **Spark UI → Jobs/Stages:** find the longest stage. Compare task duration percentiles (min, median, max).
- **SQL/DataFrame tab or query profile:** operator-level time, rows, spill, files read vs pruned.
- **Executors tab:** GC time, memory, failed tasks, lost executors.
- **Cluster event log:** autoscaling delays, spot terminations, driver restarts.

**Step 3: diagnose by symptom**
| Symptom | Likely cause | Fix |
|---|---|---|
| One/few tasks take far longer than median | Skew | AQE skew join, salting, broadcast, filter hot keys (see [How do you detect data skew?](/pyspark/pyspark-interview-questions/#21-how-do-you-detect-data-skew), [Fix skew with salting (with code)](/pyspark/pyspark-interview-questions/#39-fix-skew-with-salting-with-code)) |
| Large "spill (memory/disk)" | Insufficient memory per task | More/larger partitions, memory-optimised nodes, reduce data earlier (see [Executor memory model: unified, execution, storage, overhead](/pyspark/pyspark-interview-questions/#42-executor-memory-model-unified-execution-storage-overhead)) |
| Huge shuffle read/write | Wide transformations, wrong join strategy | Broadcast small side, pre-aggregate, avoid repeated shuffles (see [What happens during a shuffle, and why is it expensive?](/pyspark/pyspark-interview-questions/#16-what-happens-during-a-shuffle-and-why-is-it-expensive), [Join strategies: broadcast hash, shuffle hash, sort-merge](/pyspark/pyspark-interview-questions/#19-join-strategies-broadcast-hash-shuffle-hash-sort-merge)) |
| Scan reads most files | Poor clustering, no pruning, small files | Liquid clustering/Z-ORDER, OPTIMIZE, sargable filters (see [Liquid Clustering vs partitioning vs Z-ORDER](#40-liquid-clustering-vs-partitioning-vs-z-order), [What is the small-files problem, and how do you fix it?](/career/data-engineering-interview-questions/#55-what-is-the-small-files-problem-and-how-do-you-fix-it)) |
| Many tiny tasks | Too many small files/partitions | Compaction, AQE coalesce, `maxPartitionBytes` (see [How Spark decides the number of input partitions](/pyspark/pyspark-interview-questions/#18-how-spark-decides-the-number-of-input-partitions)) |
| Python UDF stages slow | Row-at-a-time Python | Built-ins or pandas UDFs (see [Python UDF vs pandas UDF vs built-in functions](/pyspark/pyspark-interview-questions/#24-python-udf-vs-pandas-udf-vs-built-in-functions)) |
| Driver busy, executors idle | `collect()`, driver-side loops, huge broadcast | Push work to executors (see [Driver OOM vs executor OOM: causes and fixes](/pyspark/pyspark-interview-questions/#43-driver-oom-vs-executor-oom-causes-and-fixes)) |
| Waiting for workers | Autoscaling/pool limits, spot shortages | Pools, min workers, fallback to on-demand |

**Step 4: fix, measure, prevent**
- Change one thing at a time, and compare duration and cost.
- Add monitoring: duration thresholds on jobs (warnings), and alerts on data volume anomalies.

<p class="qb-label">Example</p>

```python
# Runtime: PySpark 3.5 on Databricks — quick skew check on a join key before tuning
from pyspark.sql import functions as F

orders = spark.read.table("prod.silver.orders")
(orders.groupBy("customer_id")
       .count()
       .orderBy(F.desc("count"))
       .limit(10)
       .show())

orders.join(spark.read.table("prod.silver.customers"), "customer_id").explain("formatted")
```

<p class="qb-label">Real-world use case</p>

A nightly job grew from 50 minutes to 3 hours. The Spark UI showed one task running 2.5 hours in a join stage. A new marketplace seller accounted for 30% of orders, so the data was skewed. Enabling AQE skew-join handling and broadcasting the small seller dimension brought the job back to 45 minutes.

<p class="qb-label">Common mistakes and red flags</p>

- Adding workers before identifying the bottleneck.
- Looking only at total job time, not stage and task distributions.
- Changing many settings at once.

<p class="qb-label">Follow-up questions</p>

- What is the first metric you look at? — Task duration distribution in the slowest stage — it immediately separates skew from general undersizing.
- How do you prevent regressions? — Duration warnings on jobs, data volume monitors, and performance tests for major changes.

</details>
