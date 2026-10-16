---
title: "PySpark and Spark interview questions for Data Engineers"
seoTitle: "PySpark and Spark Interview Questions and Answers"
description: "50 PySpark and Apache Spark interview questions with answers: execution model, shuffles, joins, skew, caching, memory tuning and Structured Streaming."
hub: "pyspark"
label: "PySpark and Spark"
section: 5
publishedDate: "2026-10-16"
updatedDate: "2026-10-16"
levels: {"Beginner": 15, "Intermediate": 23, "Advanced": 12}
questions: [{"num": 251, "id": "SPK-01", "title": "Spark architecture: driver, executors and cluster manager", "topic": "Architecture", "level": "Beginner", "frequency": "Very Common"}, {"num": 252, "id": "SPK-02", "title": "Jobs, stages, tasks and the DAG", "topic": "Execution Model", "level": "Beginner", "frequency": "Very Common"}, {"num": 253, "id": "SPK-03", "title": "Lazy evaluation", "topic": "Execution Model", "level": "Beginner", "frequency": "Very Common"}, {"num": 254, "id": "SPK-04", "title": "Transformations vs actions", "topic": "Execution Model", "level": "Beginner", "frequency": "Very Common"}, {"num": 255, "id": "SPK-05", "title": "Narrow vs wide transformations", "topic": "Execution Model", "level": "Beginner", "frequency": "Very Common"}, {"num": 256, "id": "SPK-06", "title": "What happens during a shuffle, and why is it expensive?", "topic": "Shuffle", "level": "Intermediate", "frequency": "Very Common"}, {"num": 257, "id": "SPK-07", "title": "RDD vs DataFrame vs Dataset", "topic": "APIs", "level": "Beginner", "frequency": "Very Common"}, {"num": 258, "id": "SPK-08", "title": "repartition vs coalesce", "topic": "Partitioning", "level": "Beginner", "frequency": "Very Common"}, {"num": 259, "id": "SPK-09", "title": "spark.sql.shuffle.partitions: default and tuning", "topic": "Configuration", "level": "Intermediate", "frequency": "Very Common"}, {"num": 260, "id": "SPK-10", "title": "How Spark decides the number of input partitions", "topic": "Partitioning", "level": "Intermediate", "frequency": "Common"}, {"num": 261, "id": "SPK-11", "title": "Join strategies: broadcast hash, shuffle hash, sort-merge", "topic": "Joins", "level": "Intermediate", "frequency": "Very Common"}, {"num": 262, "id": "SPK-12", "title": "Broadcast joins: threshold, hints and limits", "topic": "Joins", "level": "Intermediate", "frequency": "Very Common"}, {"num": 263, "id": "SPK-13", "title": "How do you detect data skew?", "topic": "Performance", "level": "Intermediate", "frequency": "Very Common"}, {"num": 264, "id": "SPK-14", "title": "Fix skew with salting (with code)", "topic": "Performance", "level": "Advanced", "frequency": "Very Common"}, {"num": 265, "id": "SPK-15", "title": "Adaptive Query Execution: what does it do?", "topic": "Optimisation", "level": "Intermediate", "frequency": "Very Common"}, {"num": 266, "id": "SPK-16", "title": "cache() vs persist() and storage levels", "topic": "Caching", "level": "Beginner", "frequency": "Very Common"}, {"num": 267, "id": "SPK-17", "title": "When caching hurts, and how to unpersist", "topic": "Caching", "level": "Intermediate", "frequency": "Common"}, {"num": 268, "id": "SPK-18", "title": "Catalyst optimizer phases", "topic": "Optimisation Internals", "level": "Advanced", "frequency": "Common"}, {"num": 269, "id": "SPK-19", "title": "Project Tungsten and whole-stage code generation", "topic": "Optimisation Internals", "level": "Advanced", "frequency": "Common"}, {"num": 270, "id": "SPK-20", "title": "Python UDF vs pandas UDF vs built-in functions", "topic": "PySpark Performance", "level": "Intermediate", "frequency": "Very Common"}, {"num": 271, "id": "SPK-21", "title": "Executor memory model: unified, execution, storage, overhead", "topic": "Memory", "level": "Advanced", "frequency": "Very Common"}, {"num": 272, "id": "SPK-22", "title": "Driver OOM vs executor OOM: causes and fixes", "topic": "Memory", "level": "Advanced", "frequency": "Very Common"}, {"num": 273, "id": "SPK-23", "title": "How do you size executors (cores and memory)?", "topic": "Resource Tuning", "level": "Advanced", "frequency": "Common"}, {"num": 274, "id": "SPK-24", "title": "Dynamic allocation", "topic": "Resource Tuning", "level": "Intermediate", "frequency": "Common"}, {"num": 275, "id": "SPK-25", "title": "Reading the Spark UI", "topic": "Monitoring", "level": "Intermediate", "frequency": "Common"}, {"num": 276, "id": "SPK-26", "title": "Using explain() to read physical plans", "topic": "Optimisation", "level": "Intermediate", "frequency": "Common"}, {"num": 277, "id": "SPK-27", "title": "Predicate pushdown and partition pruning", "topic": "Optimisation", "level": "Intermediate", "frequency": "Very Common"}, {"num": 278, "id": "SPK-28", "title": "Dynamic partition pruning", "topic": "Optimisation", "level": "Advanced", "frequency": "Occasional"}, {"num": 279, "id": "SPK-29", "title": "partitionBy vs bucketBy when writing", "topic": "Data Layout", "level": "Intermediate", "frequency": "Common"}, {"num": 280, "id": "SPK-30", "title": "Save modes and dynamic partition overwrite", "topic": "Writes", "level": "Intermediate", "frequency": "Common"}, {"num": 281, "id": "SPK-31", "title": "Handling nulls in PySpark", "topic": "Data Cleaning", "level": "Beginner", "frequency": "Common"}, {"num": 282, "id": "SPK-32", "title": "Window functions in PySpark", "topic": "Window Functions", "level": "Intermediate", "frequency": "Very Common"}, {"num": 283, "id": "SPK-33", "title": "explode, flattening nested JSON and arrays", "topic": "Semi-Structured Data", "level": "Intermediate", "frequency": "Very Common"}, {"num": 284, "id": "SPK-34", "title": "Reading and writing with an explicit schema", "topic": "I/O", "level": "Beginner", "frequency": "Common"}, {"num": 285, "id": "SPK-35", "title": "groupByKey vs reduceByKey (RDD API)", "topic": "RDD API", "level": "Intermediate", "frequency": "Common"}, {"num": 286, "id": "SPK-36", "title": "Broadcast variables and accumulators", "topic": "Shared Variables", "level": "Beginner", "frequency": "Common"}, {"num": 287, "id": "SPK-37", "title": "Checkpointing vs caching (lineage truncation)", "topic": "Fault Tolerance", "level": "Advanced", "frequency": "Occasional"}, {"num": 288, "id": "SPK-38", "title": "Structured Streaming programming model", "topic": "Structured Streaming", "level": "Intermediate", "frequency": "Very Common"}, {"num": 289, "id": "SPK-39", "title": "Triggers: processingTime, availableNow, once, continuous", "topic": "Structured Streaming", "level": "Intermediate", "frequency": "Common"}, {"num": 290, "id": "SPK-40", "title": "Watermarks and late data in Structured Streaming", "topic": "Structured Streaming", "level": "Advanced", "frequency": "Very Common"}, {"num": 291, "id": "SPK-41", "title": "Checkpoints and exactly-once in Structured Streaming", "topic": "Structured Streaming", "level": "Advanced", "frequency": "Common"}, {"num": 292, "id": "SPK-42", "title": "Output modes: append, update, complete", "topic": "Structured Streaming", "level": "Beginner", "frequency": "Common"}, {"num": 293, "id": "SPK-43", "title": "Stream-stream joins", "topic": "Structured Streaming", "level": "Advanced", "frequency": "Occasional"}, {"num": 294, "id": "SPK-44", "title": "foreachBatch for upserts and multiple sinks", "topic": "Structured Streaming", "level": "Intermediate", "frequency": "Common"}, {"num": 295, "id": "SPK-45", "title": "Coding: deduplicate to keep the latest record per key", "topic": "Coding Patterns", "level": "Beginner", "frequency": "Very Common"}, {"num": 296, "id": "SPK-46", "title": "Coding: top-N per group and running totals", "topic": "Coding Patterns", "level": "Intermediate", "frequency": "Very Common"}, {"num": 297, "id": "SPK-47", "title": "Coding: word count / frequency count", "topic": "Coding Patterns", "level": "Beginner", "frequency": "Common"}, {"num": 298, "id": "SPK-48", "title": "What's new in Spark 4.0 for data engineers?", "topic": "Versions", "level": "Advanced", "frequency": "Occasional"}, {"num": 299, "id": "SPK-49", "title": "Spark Connect (client-server)", "topic": "Architecture", "level": "Beginner", "frequency": "Occasional"}, {"num": 300, "id": "SPK-50", "title": "Spark vs pandas vs Polars: when to use each", "topic": "Tool Selection", "level": "Intermediate", "frequency": "Common"}]
---

<details class="qb-item" id="q251" data-level="Beginner">
<summary><span class="qb-num">Q251</span><h3 class="qb-title">Spark architecture: driver, executors and cluster manager</h3><span class="qb-meta">Beginner · Very Common · Architecture</span></summary>

**Short answer**

A Spark application has one driver and many executors. The driver runs your main program, builds the logical and physical plans, splits work into stages and tasks, and schedules them. Executors are JVM processes on worker nodes that run tasks, cache data and write shuffle files. A cluster manager (standalone, YARN, Kubernetes, or Databricks' own) allocates resources for them.

**Detailed explanation**

**Driver**
- Hosts the `SparkSession` / `SparkContext`.
- Turns DataFrame and SQL code into a plan (Catalyst; see [Q268](/interview/pyspark/#q268)), then into a DAG of stages and tasks (see [Q252](/interview/pyspark/#q252)).
- The **DAG scheduler** splits work into stages at shuffle boundaries. The **task scheduler** sends tasks to executors, tracks progress and retries failures.
- Holds results of actions like `collect()` and broadcast variables before distribution. A driver OOM usually means too much data was pulled to it (see [Q272](/interview/pyspark/#q272)).

**Executors**
- Launched at application start (or dynamically; see [Q274](/interview/pyspark/#q274)). Each has a number of **cores**, which are task slots, and a memory allocation.
- Run tasks: one task processes one partition of data.
- Store cached partitions (see [Q266](/interview/pyspark/#q266)) and write and serve shuffle files.
- Report status and metrics back to the driver.

**Cluster manager**
| Manager | Notes |
|---|---|
| Standalone | Spark's built-in simple manager |
| YARN | Hadoop clusters (EMR, on-prem) |
| Kubernetes | Containers; driver and executors as pods |
| Databricks / managed services | Platform-managed clusters or serverless |

**Deploy modes**
- **Client mode:** the driver runs where you submit (a laptop or edge node).
- **Cluster mode:** the driver runs inside the cluster, which is more robust for production.

**Spark Connect** (Spark 3.4+) decouples the client from the driver. A thin client sends unresolved plans to a remote Spark server (see [Q299](/interview/pyspark/#q299)).

**Example**

```python
# Runtime: PySpark 3.5
from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName("orders-etl")
         .config("spark.executor.instances", "10")
         .config("spark.executor.cores", "4")
         .config("spark.executor.memory", "16g")
         .config("spark.driver.memory", "8g")
         .getOrCreate())

sc = spark.sparkContext
print(sc.master, sc.applicationId, sc.defaultParallelism)
```
On managed platforms such as Databricks or EMR Serverless, executor settings are usually defined by the cluster configuration rather than in code.

**Real-world use case**

A job failed with "Total size of serialized results is bigger than spark.driver.maxResultSize". The code was calling `collect()` on 50 million rows to write a CSV from the driver. Rewriting it to write directly from the executors with `df.write` fixed it.

**Common mistakes and red flags**

- Thinking the driver processes the data.
- Not knowing a task maps to a partition.
- Confusing executors with worker nodes. One node can host several executors.

**Follow-up questions**

- What happens if an executor dies? — Its tasks are retried on other executors; lost cached/shuffle data is recomputed from lineage.
- What happens if the driver dies? — The application fails (unless the platform restarts it, e.g. supervised cluster mode or job retries).

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q252" data-level="Beginner">
<summary><span class="qb-num">Q252</span><h3 class="qb-title">Jobs, stages, tasks and the DAG</h3><span class="qb-meta">Beginner · Very Common · Execution Model</span></summary>

**Short answer**

Each action (`count`, `write`, `collect`) triggers a job. The DAG scheduler splits each job into stages at shuffle boundaries, which are wide dependencies. Each stage consists of tasks, one per partition, that run the same code in parallel. The DAG (directed acyclic graph) records the dependencies between stages, so Spark can schedule them and recompute lost data.

**Detailed explanation**

**Hierarchy:** Application → Jobs (per action) → Stages (split at shuffles) → Tasks (per partition).

**Stages**
- **Narrow transformations** (`filter`, `select`, `withColumn`, `map`) are **pipelined** within one stage. No data moves between executors.
- **Wide transformations** (`groupBy`, `join` without broadcast, `distinct`, `repartition`, `orderBy`) need a **shuffle** and create a new stage (see [Q255](/interview/pyspark/#q255), [Q256](/interview/pyspark/#q256)).
- **ShuffleMapStages** write shuffle files. The **ResultStage** produces the action's output.

**Tasks**
- The number of tasks in a stage equals its number of partitions.
  - Reading files: input splits (see [Q260](/interview/pyspark/#q260)).
  - After a shuffle: `spark.sql.shuffle.partitions` (default 200), possibly coalesced by AQE (see [Q259](/interview/pyspark/#q259), [Q265](/interview/pyspark/#q265)).
- Tasks run on executor cores. With 40 cores and 400 tasks, there are about 10 "waves".

**Fault tolerance through lineage:** if a task fails, Spark retries it (by default up to 4 attempts). If shuffle output is lost with a dead executor, Spark recomputes the parent stage's affected partitions from lineage.

**Reading it in the Spark UI**
- **Jobs tab:** one entry per action. **Stages tab:** durations, shuffle read and write, spill, and the task time distribution.
- The **DAG visualisation** shows stage boundaries (exchanges).
- With AQE, the final plan can differ from the initial one (see [Q265](/interview/pyspark/#q265)).

**Example**

```python
# Runtime: PySpark 3.5
from pyspark.sql import functions as F

orders = spark.read.parquet("s3://lake/silver/orders/")         # stage 1 (scan)
paid = orders.filter(F.col("status") == "paid")                  # narrow, same stage
by_region = paid.groupBy("region").agg(F.sum("amount").alias("revenue"))  # shuffle -> stage 2
by_region.write.mode("overwrite").parquet("s3://lake/gold/revenue_by_region/")  # action -> job
```
The write action triggers one job with two stages: scan, filter and partial aggregation; then shuffle, final aggregation and write.

**Real-world use case**

A job had 15 stages where 4 were expected. The DAG showed repeated scans and shuffles because the same DataFrame was reused in several actions without caching. Caching it once (see [Q266](/interview/pyspark/#q266)) and combining two actions into one write cut runtime by half.

**Common mistakes and red flags**

- Saying "each transformation is a stage".
- Not linking stage boundaries to shuffles.
- Not knowing that each action triggers recomputation from the source unless cached.

**Follow-up questions**

- How many jobs does `df.count(); df.write...` create? — At least two (one per action), recomputing the plan twice unless cached.
- What is speculative execution? — Launching duplicate copies of slow tasks (`spark.speculation`) and taking the first to finish.

**In depth:** [Explain Spark jobs, stages and tasks.](/interview/spark/jobs-stages-tasks/)

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q253" data-level="Beginner">
<summary><span class="qb-num">Q253</span><h3 class="qb-title">Lazy evaluation</h3><span class="qb-meta">Beginner · Very Common · Execution Model</span></summary>

**Short answer**

Spark doesn't execute transformations when you call them. It records them in a logical plan, and only an action (such as `count`, `show`, `collect` or `write`) triggers execution. That lets Spark's optimizer see the whole pipeline: it can push filters down, prune columns, combine steps and pick join strategies before any data is read.

**Detailed explanation**

**How it works**
- `df.filter(...).select(...).join(...)` returns new DataFrames immediately. Each one is just a plan.
- On an action, Catalyst analyses, optimises and physically plans the entire chain (see [Q268](/interview/pyspark/#q268)), then runs it as jobs, stages and tasks.

**Benefits**
- **Optimisation across steps:** a filter written after a join can be pushed below the join or into the file scan (predicate pushdown; see [Q277](/interview/pyspark/#q277)). Unused columns are never read (column pruning).
- **Less I/O:** only the necessary data is read.
- **Pipelining:** narrow operations fuse into one stage, with no intermediate materialisation.
- **Fault tolerance:** the plan (lineage) lets Spark recompute lost partitions.

**Consequences to remember**
- **Errors appear late.** A bad cast or a missing file may only fail at the action, far from the line that caused it.
- **Recomputation.** Every action recomputes the plan from the source unless you cache or persist (see [Q266](/interview/pyspark/#q266)). Calling `count()` "for debugging" before a `write` doubles the work.
- **Non-deterministic sources** (reading a changing table, using `rand()` or the current time) can give different results across actions if the data changes between them.
- `explain()` shows the plan without running it (see [Q276](/interview/pyspark/#q276)). `show()` runs a limited job, which may not exercise the whole dataset.

**Example**

```python
# Runtime: PySpark 3.5
from pyspark.sql import functions as F

df = spark.read.parquet("s3://lake/silver/events/")              # nothing read yet
recent = df.filter(F.col("event_date") >= "2026-03-01")           # still lazy
small = recent.select("user_id", "event_type")                     # still lazy

small.explain()            # shows pushed filters and pruned columns; no job runs
print(small.count())       # action: job runs now, reading only 2 columns of matching files
```

**Real-world use case**

A notebook had `print(df.count())` after every step to "check progress", which caused 12 full recomputations of a 2 TB pipeline. Removing them (or caching at one checkpoint) reduced runtime from 3 hours to 25 minutes.

**Common mistakes and red flags**

- Believing each line executes immediately.
- Calling multiple actions on an uncached DataFrame.
- Being surprised that errors surface later.

**Follow-up questions**

- Is reading a file lazy? — Mostly; schema inference may trigger an early job, which an explicit schema avoids (see [Q284](/interview/pyspark/#q284)).
- Is `cache()` an action? — No; it marks the DataFrame for caching, which happens on the next action.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q254" data-level="Beginner">
<summary><span class="qb-num">Q254</span><h3 class="qb-title">Transformations vs actions</h3><span class="qb-meta">Beginner · Very Common · Execution Model</span></summary>

**Short answer**

Transformations (such as `select`, `filter`, `withColumn`, `join`, `groupBy().agg()` and `union`) define a new DataFrame from an existing one and are lazy. Actions (such as `count`, `collect`, `show`, `take`, `write` and `foreach`) trigger execution and either return results to the driver or write data out. Every action starts at least one job.

**Detailed explanation**

| Category | Examples | Effect |
|---|---|---|
| Narrow transformations | `select`, `filter`/`where`, `withColumn`, `drop`, `map` (RDD), `union`, `coalesce` | No shuffle; pipelined in one stage |
| Wide transformations | `groupBy().agg()`, `join` (non-broadcast), `distinct`, `orderBy`, `repartition`, window functions with PARTITION BY | Shuffle; new stage |
| Actions returning to driver | `collect`, `take(n)`, `first`, `head`, `count`, `show`, `toPandas` | Results come to driver (watch memory) |
| Actions writing out | `write.save/saveAsTable/parquet/insertInto`, `foreach`, `foreachPartition` | Data written by executors |
| Streaming | `writeStream.start()` | Starts a continuous/incremental query |

**Subtleties**
- `cache()` and `persist()` are lazy markers, not actions.
- `df.rdd.getNumPartitions()` doesn't trigger a job. `df.count()` does.
- Some "transformations" trigger eager work: schema inference on read, `df.schema` with inference, `checkpoint()` (eager by default), `localCheckpoint()`, and in some cases `sort` sampling jobs to compute range boundaries.
- `show()` runs a small job and may read only a few partitions, so it isn't a full validation.

**Driver-safety guidance**
- Prefer `take(n)` or `limit(n).toPandas()` to `collect()` / `toPandas()` on large data.
- Write large outputs with `df.write` rather than collecting them to the driver.

**Example**

```python
# Runtime: PySpark 3.5
from pyspark.sql import functions as F

clicks = spark.read.parquet("s3://lake/silver/clicks/")
daily = (clicks
         .withColumn("click_date", F.to_date("click_ts"))      # transformation (narrow)
         .groupBy("click_date", "campaign_id")                 # transformation (wide)
         .agg(F.count("*").alias("clicks")))

daily.show(5)                                                   # action (small job)
daily.write.mode("overwrite").partitionBy("click_date") \
     .parquet("s3://lake/gold/daily_campaign_clicks/")          # action (full job)
```

**Real-world use case**

An engineer used `df.collect()` in a loop to send each row to an API, and the driver ran out of memory at 3 million rows. Replacing it with `foreachPartition` (batched API calls from executors) scaled the job, with rate limiting per partition.

**Common mistakes and red flags**

- Calling `cache()` an action.
- Using `collect()` on large data.
- Not realising every action re-executes uncached lineage.

**Follow-up questions**

- Is `printSchema()` an action? — No job for known schemas; may trigger inference on schemaless reads.
- `foreach` vs `foreachPartition`? — The latter lets you open one connection per partition instead of per row.

**In depth:** [What is the difference between a transformation and an action in Spark?](/interview/spark/transformation-vs-action/)

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q255" data-level="Beginner">
<summary><span class="qb-num">Q255</span><h3 class="qb-title">Narrow vs wide transformations</h3><span class="qb-meta">Beginner · Very Common · Execution Model</span></summary>

**Short answer**

In a narrow transformation, each output partition depends on at most one input partition (`filter`, `select`, `withColumn`), so no data moves across the network. In a wide transformation, an output partition needs data from many input partitions (`groupBy`, non-broadcast `join`, `distinct`, `orderBy`, `repartition`), which forces a shuffle and a new stage. Minimising wide transformations, and the data they move, is central to Spark performance.

**Detailed explanation**

| Aspect | Narrow | Wide |
|---|---|---|
| Dependency | One-to-one (or few-to-one, e.g. `coalesce`) | Many-to-many |
| Data movement | None (within executor) | Shuffle across network |
| Stage boundary | No | Yes |
| Failure recovery | Recompute one parent partition | May need to recompute many parent partitions |
| Examples | `select`, `filter`, `withColumn`, `cast`, `union`, `coalesce`, broadcast hash join (large side) | `groupBy().agg()`, `join` (sort-merge/shuffle hash), `distinct`, `dropDuplicates`, `orderBy`, `repartition`, window `PARTITION BY` |

**Notes**
- **Broadcast joins** avoid shuffling the large side. The small side is sent to every executor, so the join itself is narrow for the big table (see [Q262](/interview/pyspark/#q262)).
- **Aggregations** do a **partial (map-side) aggregation** before the shuffle, so only per-partition partial results move. That greatly reduces the shuffle size for `sum` and `count`.
- `coalesce(n)` reducing partitions is narrow. `repartition(n)` is wide (see [Q258](/interview/pyspark/#q258)).
- **Pre-partitioned or bucketed data** can let Spark skip a shuffle for joins and aggregations on the bucket keys (see [Q279](/interview/pyspark/#q279)).

**How to reduce wide work**
- Filter and project before joins and aggregations.
- Broadcast small dimensions.
- Combine several aggregations into one `groupBy`.
- Avoid unnecessary `distinct` or `orderBy` in intermediate steps.
- Reuse the same partitioning for consecutive operations on the same key.

**Example**

```python
# Runtime: PySpark 3.5
from pyspark.sql import functions as F

orders = spark.read.table("silver.orders")
stores = spark.read.table("silver.stores")          # small dimension

result = (orders
          .filter(F.col("order_date") >= "2026-03-01")          # narrow
          .join(F.broadcast(stores), "store_id")                 # broadcast: no shuffle of orders
          .groupBy("region")                                     # wide: shuffle (after partial agg)
          .agg(F.sum("amount").alias("revenue"),
               F.countDistinct("customer_id").alias("customers")))
result.explain()     # look for 'Exchange' nodes = shuffles
```

**Real-world use case**

A pipeline ran `distinct()` after every join "to be safe", which added four extra shuffles of 500 GB each. Removing the redundant distincts (the keys were already unique) halved the runtime.

**Common mistakes and red flags**

- Calling `coalesce` wide.
- Not knowing broadcast joins avoid a shuffle of the large table.
- Ignoring map-side partial aggregation.

**Follow-up questions**

- How do you see shuffles in a plan? — `Exchange` operators in `explain()` / SQL tab.
- Is `union` wide? — No; it just concatenates partitions.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q256" data-level="Intermediate">
<summary><span class="qb-num">Q256</span><h3 class="qb-title">What happens during a shuffle, and why is it expensive?</h3><span class="qb-meta">Intermediate · Very Common · Shuffle</span></summary>

**Short answer**

A shuffle redistributes data across executors by key. Map-side tasks partition, often sort, serialise and write their output to local shuffle files. Reduce-side tasks then fetch their partition's blocks from every map task over the network. It is expensive because of disk I/O, network transfer, serialisation, sorting and memory pressure, and skew can make one reducer far slower than the rest.

**Detailed explanation**

**Mechanics (sort-based shuffle, the default)**
1. **Map side (ShuffleMapStage):** each task computes the target partition for each record (hash of the key modulo the number of partitions, or a range partitioner for `orderBy`). It buffers and sorts records by partition ID, spilling to disk if memory fills, and writes one data file plus an index file per task.
2. **Shuffle service:** an external shuffle service, or Kubernetes and Databricks equivalents, can serve shuffle files even if the executor that wrote them is gone. This matters for dynamic allocation (see [Q274](/interview/pyspark/#q274)).
3. **Reduce side:** each task fetches its blocks from all map outputs (`spark.reducer.maxSizeInFlight` controls the fetch parallelism), then merges and aggregates or joins them.

**Why it costs so much**
- **Disk:** writes on the map side and reads on the reduce side, plus spill during sorts.
- **Network:** all-to-all data transfer.
- **CPU:** serialisation and deserialisation, compression, sorting.
- **Memory:** buffers on both sides. Insufficient memory causes spill (see [Q271](/interview/pyspark/#q271)).
- **Skew:** a hot key sends a huge share of the data to one reducer (see [Q263](/interview/pyspark/#q263)).
- **Fan-out:** many map tasks × many reduce partitions means many small blocks and lots of overhead.

**Metrics to read (Spark UI, Stages tab)**
- Shuffle Write Size and Records (map side), Shuffle Read Size and Records (reduce side).
- Spill (Memory and Disk), Fetch Wait Time.
- Task time distribution: the max far above the median means skew.

**Reducing shuffle cost**
- Shuffle less data: filter, project and pre-aggregate before it.
- Avoid shuffles altogether: broadcast joins (see [Q262](/interview/pyspark/#q262)), bucketing (see [Q279](/interview/pyspark/#q279)), or reusing an existing partitioning.
- Right-size the partition count (see [Q259](/interview/pyspark/#q259)) and let AQE coalesce and handle skew (see [Q265](/interview/pyspark/#q265)).
- Use efficient serialisation (DataFrames use Tungsten's binary format; see [Q269](/interview/pyspark/#q269)) and compression (`spark.shuffle.compress`, on by default).
- Storage-optimised nodes with local SSDs for shuffle-heavy work (see [Q230](/interview/databricks/#q230)).

**Example**

```python
# Runtime: PySpark 3.5
from pyspark.sql import functions as F

events = spark.read.table("silver.events")

# Two separate shuffles on the same key...
a = events.groupBy("user_id").agg(F.count("*").alias("events"))
b = events.groupBy("user_id").agg(F.max("event_ts").alias("last_seen"))
two_shuffles = a.join(b, "user_id")

# ...vs one shuffle with both aggregations
one_shuffle = events.groupBy("user_id").agg(F.count("*").alias("events"),
                                            F.max("event_ts").alias("last_seen"))
one_shuffle.explain()   # single Exchange hashpartitioning(user_id)
```

**Real-world use case**

A job wrote 3 TB of shuffle data to compute per-user metrics in five separate groupBys joined back together. Collapsing them into one `groupBy().agg(...)` cut shuffle volume to 600 GB and runtime by 60%.

**Common mistakes and red flags**

- "Shuffles are in memory only."
- Ignoring spill and fetch-wait metrics.
- Multiple shuffles on the same key that could be combined.

**Follow-up questions**

- What is push-based shuffle? — An optimisation (Spark 3.2+ on YARN) where map outputs are merged on remote nodes to reduce small-block fetches.
- Why do shuffle files survive executor loss with an external shuffle service? — The service, not the executor process, serves them.

**In depth:** [What causes a shuffle in Spark?](/interview/spark/what-causes-a-shuffle/)

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q257" data-level="Beginner">
<summary><span class="qb-num">Q257</span><h3 class="qb-title">RDD vs DataFrame vs Dataset</h3><span class="qb-meta">Beginner · Very Common · APIs</span></summary>

**Short answer**

An RDD is Spark's low-level distributed collection of objects. You get full control, but no schema-aware optimisation. A DataFrame is a distributed table with named columns and a schema, optimised by Catalyst and executed by Tungsten, and it is the standard API in Python, SQL, Scala and R. A Dataset (Scala and Java only) is a typed DataFrame with compile-time type safety. In PySpark you use DataFrames.

**Detailed explanation**

| Aspect | RDD | DataFrame | Dataset (Scala/Java) |
|---|---|---|---|
| Abstraction | Distributed collection of objects | Distributed table (rows + schema) | Typed objects with schema |
| Optimisation | None (you control execution) | Catalyst + Tungsten | Catalyst + Tungsten (lambdas less optimisable) |
| Type safety | Compile-time (Scala) | Runtime (column names checked at analysis) | Compile-time |
| Languages | Scala, Java, Python | All, including SQL | Scala, Java |
| Serialization | Java/Kryo objects (Python: pickled) | Compact binary (UnsafeRow) | Encoders to binary |
| Python performance | Slow (Python worker round-trips) | Fast (work stays in JVM/Photon) | N/A |
| Typical use today | Legacy code, low-level control, unstructured processing | Almost everything | Typed Scala pipelines |

**Why DataFrames win for data engineering**
- The optimizer can reorder, push down and prune because it understands the schema and the operations (see [Q268](/interview/pyspark/#q268)).
- Columnar in-memory caching and whole-stage code generation (see [Q269](/interview/pyspark/#q269)).
- PySpark DataFrame operations run in the JVM. Only Python UDFs cross into Python workers (see [Q270](/interview/pyspark/#q270)).
- Unified batch and streaming APIs (Structured Streaming).

**When you still see RDDs**
- Older code using `map`/`reduceByKey`, `mapPartitions` for custom per-partition logic (DataFrames also have `mapInPandas` / `mapInArrow`), or very low-level partition control.
- Not available on some platforms' shared or serverless compute (for example Databricks standard access mode and serverless), which is another reason to avoid them.

**Example**

```python
# Runtime: PySpark 3.5
from pyspark.sql import functions as F

# RDD style (legacy): word count
lines_rdd = spark.sparkContext.textFile("s3://lake/raw/logs/2026-03-01/*.log")
counts_rdd = (lines_rdd.flatMap(lambda line: line.split())
                       .map(lambda w: (w.lower(), 1))
                       .reduceByKey(lambda a, b: a + b))

# DataFrame style (optimised)
lines_df = spark.read.text("s3://lake/raw/logs/2026-03-01/*.log")
counts_df = (lines_df.select(F.explode(F.split(F.lower("value"), r"\s+")).alias("word"))
                     .filter(F.col("word") != "")
                     .groupBy("word").count())
```

**Real-world use case**

A legacy PySpark job used RDD `map` lambdas to parse JSON and aggregate. Rewriting it with `from_json` and DataFrame aggregations made it about 6 times faster, mainly by eliminating Python serialisation between the JVM and Python workers.

**Common mistakes and red flags**

- Saying Datasets are available in PySpark.
- Writing new pipelines with RDDs without a reason.
- Thinking DataFrames are "just pandas on Spark".

**Follow-up questions**

- What is the pandas API on Spark? — `pyspark.pandas`, a pandas-like API backed by Spark DataFrames (formerly Koalas).
- Can you convert between them? — `df.rdd` and `spark.createDataFrame(rdd, schema)`; conversions cost serialization.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q258" data-level="Beginner">
<summary><span class="qb-num">Q258</span><h3 class="qb-title">repartition vs coalesce</h3><span class="qb-meta">Beginner · Very Common · Partitioning</span></summary>

**Short answer**

`repartition(n)` or `repartition(n, cols)` performs a full shuffle to produce n roughly even partitions, optionally hash-partitioned by columns. It can increase or decrease the partition count. `coalesce(n)` only decreases the count by merging existing partitions without a shuffle, which is cheaper but can produce uneven partitions. It can also reduce upstream parallelism if Spark pushes the coalesce back into earlier stages.

**Detailed explanation**

| Aspect | `repartition` | `coalesce` |
|---|---|---|
| Shuffle | Yes (full) | No (narrow) |
| Increase partitions | Yes | No (cannot increase without `shuffle=True` in RDD API) |
| Decrease partitions | Yes | Yes |
| Balance | Even sizes (round-robin or hash) | Possibly uneven (merges neighbours) |
| By column | `repartition(n, "col")` hash-partitions by key | No |
| Cost | Higher | Lower |
| Typical use | Fix skewed/too few partitions, prepare for partitioned writes, increase parallelism | Reduce output file count at the end of a job |

**The coalesce gotcha.** Because coalesce is narrow, Spark can pipeline it into the preceding stage. `df.<heavy transforms>.coalesce(1).write` may run the *heavy transforms* with only 1 task, which is extremely slow. If upstream work is heavy, use `repartition(n)` before writing (accepting one shuffle), or make sure a shuffle boundary already exists before the coalesce.

**Related functions**
- `repartitionByRange(n, col)`: range partitioning, which gives sorted, non-overlapping ranges. Useful before writes that benefit from ordered files.
- `df.write.partitionBy("col")`: directory partitioning at write time, a different concept (see [Q279](/interview/pyspark/#q279)).
- **AQE** can automatically coalesce small shuffle partitions after a shuffle (see [Q265](/interview/pyspark/#q265)), which often removes the need for a manual `coalesce`.
- SQL hints: `/*+ REPARTITION(200, col) */`, `/*+ COALESCE(10) */`, `/*+ REBALANCE */` (AQE-aware rebalancing that avoids small files).

**Example**

```python
# Runtime: PySpark 3.5
from pyspark.sql import functions as F

big = spark.read.table("silver.events").filter(F.col("event_date") == "2026-03-01")

# Write ~16 files per date partition, keyed by user for locality
(big.repartition(16, "user_id")
    .write.mode("overwrite")
    .parquet("s3://lake/gold/events_by_user/event_date=2026-03-01/"))

# Small final result: reduce files cheaply (upstream already has a shuffle from groupBy)
summary = big.groupBy("event_type").count()
summary.coalesce(1).write.mode("overwrite").json("s3://lake/reports/event_type_counts/")
```

**Real-world use case**

A job ended with `coalesce(1)` to produce a single CSV, and the entire 300 GB pipeline ran on one core for 4 hours. Moving to `repartition(1)` after the aggregation (a small result) brought it to 12 minutes.

**Common mistakes and red flags**

- `coalesce` to increase partitions.
- `coalesce(1)` directly after heavy narrow transformations.
- `repartition` everywhere without need, adding shuffles.

**Follow-up questions**

- How do you check partition count? — `df.rdd.getNumPartitions()`.
- What does `repartition("col")` without n do? — Uses `spark.sql.shuffle.partitions` as the count.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q259" data-level="Intermediate">
<summary><span class="qb-num">Q259</span><h3 class="qb-title">spark.sql.shuffle.partitions: default and tuning</h3><span class="qb-meta">Intermediate · Very Common · Configuration</span></summary>

**Short answer**

`spark.sql.shuffle.partitions` sets the number of partitions produced by DataFrame and SQL shuffles (joins, aggregations). The default is 200. Too few partitions makes each one huge, which causes spill and OOM. Too many makes them tiny, with scheduling overhead and small files. A common target is roughly 100–200 MB per shuffle partition. With AQE enabled (the default since Spark 3.2), Spark coalesces partitions at runtime, so you can start high and let AQE shrink them.

**Detailed explanation**

**Why 200 is often wrong**
- 10 GB of shuffle data at 200 partitions is about 50 MB each, which is fine.
- 2 TB at 200 partitions is about 10 GB each: spill, OOM and long straggling tasks.
- 50 MB at 200 partitions is 250 KB each: scheduling overhead and 200 tiny output files.

**Sizing approach**
1. Estimate the shuffle size from the Spark UI (Shuffle Write in the previous stage).
2. Divide by the target partition size (about 128–256 MB). For example, 1 TB / 200 MB ≈ 5,000 partitions.
3. Make it a multiple of total executor cores for even waves (with 400 cores, use 4,000 or 4,800).

**With AQE (Spark 3.x)**
- `spark.sql.adaptive.enabled = true` (default since 3.2).
- `spark.sql.adaptive.coalescePartitions.enabled = true`: merges small post-shuffle partitions toward `spark.sql.adaptive.advisoryPartitionSizeInBytes` (default 64 MB).
- `spark.sql.adaptive.coalescePartitions.initialPartitionNum`: start with a high count and let AQE coalesce.
- Databricks also offers **auto-optimised shuffle** (`spark.sql.shuffle.partitions = auto`).

**Streaming caveat:** for stateful Structured Streaming queries, the number of shuffle partitions is fixed in the checkpoint at first start. Changing the config later has no effect without a new checkpoint (see [Q245](/interview/databricks/#q245)).

**Not to confuse with**
- `spark.default.parallelism`: the RDD default (usually total cores).
- `spark.sql.files.maxPartitionBytes`: input split size when reading files (see [Q260](/interview/pyspark/#q260)).

**Example**

```python
# Runtime: PySpark 3.5
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", str(128 * 1024 * 1024))
spark.conf.set("spark.sql.shuffle.partitions", "2000")   # upper bound; AQE coalesces down

result = (spark.read.table("silver.orders")
          .groupBy("customer_id")
          .agg({"amount": "sum"}))
result.write.mode("overwrite").saveAsTable("gold.customer_spend")
```

**Real-world use case**

A 1.5 TB join with default 200 shuffle partitions spilled 4 TB to disk and took 2 hours. Setting 3,000 partitions with AQE coalescing removed most of the spill, and the job finished in 35 minutes.

**Common mistakes and red flags**

- Never changing the default for very large or very small jobs.
- Setting a huge count without AQE, creating thousands of tiny output files.
- Changing it for a running stateful stream and expecting an effect.

**Follow-up questions**

- What's the default value? — 200.
- How does AQE pick the final number? — It merges adjacent small partitions based on actual shuffle statistics toward the advisory size.

**In depth:** [How does partition count affect Spark performance?](/interview/spark/partition-count-performance/)

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q260" data-level="Intermediate">
<summary><span class="qb-num">Q260</span><h3 class="qb-title">How Spark decides the number of input partitions</h3><span class="qb-meta">Intermediate · Common · Partitioning</span></summary>

**Short answer**

For file sources, Spark packs files and file splits into input partitions of up to `spark.sql.files.maxPartitionBytes` (default 128 MB). It accounts for `spark.sql.files.openCostInBytes` (default 4 MB) per file, and respects splittability: Parquet splits by row groups, while gzipped CSV can't be split. The result is roughly the total size divided by about 128 MB, adjusted for parallelism, file count and file sizes.

**Detailed explanation**

**For DataFrame file reads (Parquet, ORC, JSON, CSV)**
1. Compute `maxSplitBytes = min(maxPartitionBytes, max(openCostInBytes, bytesPerCore))`, where `bytesPerCore = totalBytes / defaultParallelism` (including per-file open costs).
2. Split large splittable files into chunks of `maxSplitBytes`.
3. Pack small files together into partitions up to `maxSplitBytes`, treating each file as at least `openCostInBytes`.

**Consequences**
- **Large splittable files** (Parquet with many row groups) parallelise well.
- **Many tiny files** each cost at least the open-cost estimate, plus real per-file I/O overhead (see [Q57](/interview/data-engineering/#q57)).
- **Non-splittable files** (one big `.csv.gz`) produce a single partition, so a single task reads them (see [Q58](/interview/data-engineering/#q58)).
- **Small datasets on a large cluster:** `bytesPerCore` lowers the split size, so all cores get work.

**Other sources**
- **JDBC:** one partition unless you set `partitionColumn`, `lowerBound`, `upperBound` and `numPartitions`, or provide predicates.
- **Kafka:** one Spark partition per Kafka topic partition by default (`minPartitions` can split further).
- **Delta and Iceberg:** follow file-based logic over the table's data files after pruning.
- **RDD `textFile`:** a `minPartitions` argument, and Hadoop input splits.

**Tuning knobs**
| Setting | Default | Effect |
|---|---|---|
| `spark.sql.files.maxPartitionBytes` | 128 MB | Larger → fewer, bigger input tasks |
| `spark.sql.files.openCostInBytes` | 4 MB | Higher → fewer small files packed per partition |
| `spark.sql.files.minPartitionNum` | (defaults to parallelism) | Minimum target partitions |

**Example**

```python
# Runtime: PySpark 3.5
spark.conf.set("spark.sql.files.maxPartitionBytes", str(256 * 1024 * 1024))

df = spark.read.parquet("s3://lake/silver/events/event_date=2026-03-01/")
print("input partitions:", df.rdd.getNumPartitions())

# Parallel JDBC read: 16 partitions over an integer key range
orders = (spark.read.format("jdbc")
          .option("url", "jdbc:postgresql://orders-db.internal:5432/orders")
          .option("dbtable", "public.orders")
          .option("partitionColumn", "order_id")
          .option("lowerBound", "1")
          .option("upperBound", "50000000")
          .option("numPartitions", "16")
          .option("user", db_user).option("password", db_password)
          .load())
```
`db_user` and `db_password` should come from a secrets manager (see [Q86](/interview/data-engineering/#q86)).

**Real-world use case**

A JDBC extract of 50 million rows ran in a single task for 90 minutes because no partitioning options were set. Adding `partitionColumn` with 16 partitions brought it to 8 minutes, within the source database's connection limits.

**Common mistakes and red flags**

- JDBC reads without partition options.
- Expecting parallelism from one huge gzip file.
- Confusing input partitions with shuffle partitions.

**Follow-up questions**

- Do bounds filter rows in JDBC reads? — No; they only define partition strides; rows outside bounds go into the first/last partitions.
- How do you reduce tasks on many tiny files? — Compact files, or raise `maxPartitionBytes`/`openCostInBytes` to pack more per task.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q261" data-level="Intermediate">
<summary><span class="qb-num">Q261</span><h3 class="qb-title">Join strategies: broadcast hash, shuffle hash, sort-merge</h3><span class="qb-meta">Intermediate · Very Common · Joins</span></summary>

**Short answer**

Spark picks a join strategy from the join type, the condition and the table sizes. A broadcast hash join sends the small side to every executor, with no shuffle of the large side. A sort-merge join shuffles and sorts both sides by key, then merges them. It is the default for large equi-joins. A shuffle hash join shuffles both sides and builds a hash table per partition. Non-equi joins fall back to broadcast nested loop or Cartesian joins, which are slow.

**Detailed explanation**

| Strategy | Requirements | Shuffle | Best when | Risk |
|---|---|---|---|---|
| Broadcast hash join (BHJ) | Equi-join; one side small enough (≤ `autoBroadcastJoinThreshold` or hinted) | No (large side) | Fact × small dimension | Broadcasting too-large tables → driver/executor OOM |
| Shuffle hash join (SHJ) | Equi-join; one side much smaller per partition | Both sides | Medium-sized build side fits in memory per partition | OOM if build partitions too large |
| Sort-merge join (SMJ) | Equi-join; sortable keys | Both sides (+ sort) | Large × large | Shuffle/sort cost; skew |
| Broadcast nested loop join (BNLJ) | Any condition; one side broadcastable | No | Small non-equi joins | Very slow on big data |
| Cartesian product | No join condition / cross join | Yes | Rarely intended | Explodes rows |

**Selection logic (simplified)**
1. Hints first: `BROADCAST`, `MERGE`, `SHUFFLE_HASH`, `SHUFFLE_REPLICATE_NL`.
2. For equi-joins, broadcast if one side's estimated size is under the threshold (default 10 MB) and the join type allows it. For a left outer join, the right side can be broadcast.
3. Otherwise sort-merge join (`spark.sql.join.preferSortMergeJoin = true`), unless conditions favour shuffle hash.
4. **AQE** re-plans at runtime: it can convert a sort-merge join to a broadcast join once actual sizes are known, switch to shuffle hash join with local maps, and split skewed partitions (see [Q265](/interview/pyspark/#q265)).

**Reading the plan:** look for `BroadcastHashJoin`, `SortMergeJoin`, `ShuffledHashJoin` and `BroadcastNestedLoopJoin` nodes, and their `Exchange` children (see [Q276](/interview/pyspark/#q276)).

**Example**

```python
# Runtime: PySpark 3.5
from pyspark.sql import functions as F

orders = spark.read.table("silver.orders")        # large
stores = spark.read.table("silver.stores")        # small
returns = spark.read.table("silver.returns")      # large

bhj = orders.join(F.broadcast(stores), "store_id")             # BroadcastHashJoin
smj = orders.join(returns, "order_id", "left")                 # SortMergeJoin (large x large)
shj = orders.hint("shuffle_hash").join(returns, "order_id")    # ShuffledHashJoin if feasible

bhj.explain()
smj.explain()
```

**Real-world use case**

A join between a 2 TB fact and a 40 MB dimension ran as a sort-merge join because statistics were missing and the estimate exceeded the 10 MB threshold. Adding a broadcast hint (and later collecting table statistics) removed the 2 TB shuffle, and the stage went from 25 minutes to 3.

**Common mistakes and red flags**

- Broadcasting a multi-GB table.
- Non-equi joins on large tables without adding an equality key.
- Not checking the physical plan.

**Follow-up questions**

- Which sides can be broadcast in outer joins? — Left outer: right side; right outer: left side; full outer: generally not broadcast (BNLJ only).
- Why does sort-merge need sorting? — To merge both sides in one pass per partition, which scales beyond memory.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q262" data-level="Intermediate">
<summary><span class="qb-num">Q262</span><h3 class="qb-title">Broadcast joins: threshold, hints and limits</h3><span class="qb-meta">Intermediate · Very Common · Joins</span></summary>

**Short answer**

Spark broadcasts a table automatically if its estimated size is below `spark.sql.autoBroadcastJoinThreshold` (default 10 MB, set to -1 to disable). `F.broadcast(df)` or `/*+ BROADCAST(t) */` forces it. With AQE, `spark.sql.adaptive.autoBroadcastJoinThreshold` is applied to actual runtime sizes. Broadcasting avoids shuffling the big side, but the broadcast table must fit in driver and executor memory, and there is a hard limit of 8 GB per broadcast.

**Detailed explanation**

**How a broadcast works**
1. The driver collects the small table (or executors build it, depending on the implementation), and builds a hash relation.
2. It is sent to every executor (BitTorrent-like distribution) and kept in memory.
3. Each task of the big table probes the hash table locally, with no shuffle.

**Thresholds and settings**
| Setting | Default | Notes |
|---|---|---|
| `spark.sql.autoBroadcastJoinThreshold` | 10 MB | Static planning based on estimated size; -1 disables |
| `spark.sql.adaptive.autoBroadcastJoinThreshold` | (defaults to the above) | Used by AQE with actual post-shuffle sizes |
| `spark.sql.broadcastTimeout` | 300 s | Time allowed to build/broadcast |
| Max broadcast table size | 8 GB (hard limit) | Practical limits much lower — memory per executor |

**Estimates matter.** Size estimates come from file sizes or statistics. After filters, Spark may overestimate (no statistics) or underestimate (compressed Parquet expands several-fold in memory). Run `ANALYZE TABLE ... COMPUTE STATISTICS`, or rely on AQE runtime decisions.

**When to force a broadcast**
- A small dimension (even 50–500 MB in memory, if executors have room) joined with a huge fact.
- A filtered side known to be small that the optimizer can't estimate.

**When not to**
- The "small" side is large or grows over time. Hints are static and silently become dangerous.
- Driver memory is small: building broadcasts may pass through the driver.
- Full outer joins, where broadcast hash join doesn't apply.

**Example**

```python
# Runtime: PySpark 3.5
from pyspark.sql import functions as F

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(50 * 1024 * 1024))   # 50 MB

clicks = spark.read.table("silver.clicks")                       # 3 TB
campaigns = (spark.read.table("silver.campaigns")
             .filter(F.col("is_active")))                         # small after filter

joined = clicks.join(F.broadcast(campaigns), "campaign_id", "inner")
joined.explain()   # expect BroadcastHashJoin and BroadcastExchange
```

```sql
-- Dialect: Spark SQL
SELECT /*+ BROADCAST(c) */ k.click_id, c.campaign_name
FROM silver.clicks k
JOIN silver.campaigns c ON k.campaign_id = c.campaign_id;
```

**Real-world use case**

A hard-coded broadcast hint on a "small" customer table worked for a year. Then the table grew to 6 GB, executors started dying with OOM, and the job failed nightly. Removing the hint and relying on AQE with statistics restored stability.

**Common mistakes and red flags**

- Permanent broadcast hints on growing tables.
- Setting the threshold to several GB globally.
- Forgetting that compressed size isn't in-memory size.

**Follow-up questions**

- What is the default threshold? — 10 MB.
- What is a broadcast variable? — A read-only value shipped once per executor for use in UDFs/RDD code (see [Q286](/interview/pyspark/#q286)); different from broadcast joins but same mechanism.

**In depth:** [When would you use a broadcast join in Spark?](/interview/pyspark/broadcast-join/)

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q263" data-level="Intermediate">
<summary><span class="qb-num">Q263</span><h3 class="qb-title">How do you detect data skew?</h3><span class="qb-meta">Intermediate · Very Common · Performance</span></summary>

**Short answer**

Skew is when a few keys or partitions hold far more data than the rest. In the Spark UI, a stage where most tasks finish quickly but a few run much longer, with much larger shuffle read size or spill, is the giveaway. Confirm it by counting rows per join or group key, including NULL and placeholder values.

**Detailed explanation**

**Symptoms**
- **Task time distribution:** the median is seconds, and the max is tens of minutes.
- **Shuffle Read Size per task:** a few tasks read gigabytes while the rest read megabytes.
- **Spill and OOM** concentrated in the same few tasks.
- The stage "hangs at 199/200 tasks".
- Executors sit idle while one task runs.

**Confirming**
1. Find the operation in that stage (join, groupBy, window) and its key.
2. Count per key: `df.groupBy(key).count().orderBy(desc("count"))`.
3. Check for **NULLs or sentinel values** ("unknown", "-1", empty strings), which often cause skew in joins and aggregations.
4. Check **distribution after filters**. Skew may appear only for a particular date or segment.

**Common causes**
| Cause | Example |
|---|---|
| Natural popularity | One mega-merchant, celebrity user, default product |
| NULL/placeholder keys | Guest orders with NULL `customer_id` |
| Data quality issues | Bots generating millions of events for one ID |
| Partitioning on low-cardinality column | `repartition("country")` with one dominant country |
| Explosion | `explode` on arrays with a few huge arrays |

**What to do next** (see [Q264](/interview/pyspark/#q264), [Q265](/interview/pyspark/#q265))
- Let **AQE skew join** handle it (Spark 3.x, for sort-merge and shuffle hash joins).
- **Broadcast** the smaller side if feasible.
- **Filter or separately process** NULLs and hot keys.
- **Salting** for aggregations and joins AQE doesn't fix.

**Example**

```python
# Runtime: PySpark 3.5
from pyspark.sql import functions as F

orders = spark.read.table("silver.orders")

key_stats = (orders.groupBy("merchant_id").count()
             .agg(F.max("count").alias("max_rows"),
                  F.expr("percentile_approx(count, 0.5)").alias("median_rows"),
                  F.sum(F.when(F.col("merchant_id").isNull(), F.col("count"))).alias("null_key_rows")))
key_stats.show()

orders.groupBy("merchant_id").count().orderBy(F.desc("count")).show(10)
```
If `max_rows` is orders of magnitude larger than `median_rows`, or `null_key_rows` is large, the key is skewed.

**Real-world use case**

A join stage stuck at 399/400 tasks was traced to 35% of orders having `customer_id = NULL` (guest checkout). Filtering NULL keys out of the join and union-ing them back afterwards removed the straggler.

**Common mistakes and red flags**

- Adding executors to fix skew.
- Ignoring NULL keys.
- Diagnosing without looking at task-level metrics.

**Follow-up questions**

- Does AQE fix skew in aggregations? — AQE skew handling targets joins; aggregations rely on partial aggregation and may still need salting.
- How do NULL keys behave in joins? — They never match on `=`, but still shuffle to the same partition, causing skew.

**In depth:** [What is data skew and how can you mitigate it?](/interview/spark/data-skew/)

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q264" data-level="Advanced">
<summary><span class="qb-num">Q264</span><h3 class="qb-title">Fix skew with salting (with code)</h3><span class="qb-meta">Advanced · Very Common · Performance</span></summary>

**Short answer**

Salting spreads a hot key across N partitions by adding a random salt (0 to N−1) to the skewed side's key. For joins, the other side is replicated N times, once per salt value, so every salted key still finds its match. For aggregations, aggregate by (key, salt) first, then aggregate again by key. Check whether AQE skew-join handling already solves it before salting manually.

**Detailed explanation**

**Join salting**
1. On the large, skewed side: `salt = floor(rand() * N)`.
2. On the other side: cross join with `range(N)` to create N copies, one per salt.
3. Join on `(key, salt)`.
4. Cost: the other side grows N times. Salt only hot keys to limit that (selective salting).

**Aggregation salting (two-stage)**
1. `groupBy(key, salt).agg(partial)`: work spreads across N tasks per hot key.
2. `groupBy(key).agg(combine partials)`.
3. This works for decomposable aggregates (sum, count, min, max). For averages, sum and count separately. For distinct counts, use approximate sketches or two-step distinct logic.

**Choosing N:** roughly the hot key's size divided by the target partition size. For example, a 20 GB hot key with 200 MB targets needs N ≈ 100.

**Alternatives before salting**
- **AQE skew join** (`spark.sql.adaptive.skewJoin.enabled = true`, default): splits skewed partitions automatically for sort-merge and shuffle hash joins (see [Q265](/interview/pyspark/#q265)).
- **Broadcast** the smaller side (see [Q262](/interview/pyspark/#q262)).
- **Separate the hot keys:** process them with a broadcast join, process the rest normally, then union.
- **Fix the data:** filter NULL or placeholder keys (see [Q263](/interview/pyspark/#q263)).

**Example**

```python
# Runtime: PySpark 3.5 — selective salting for a skewed join
from pyspark.sql import functions as F

N = 32
orders = spark.read.table("silver.orders")            # skewed on merchant_id
merchants = spark.read.table("silver.merchants")      # too large to broadcast

hot_keys = [r.merchant_id for r in
            orders.groupBy("merchant_id").count().orderBy(F.desc("count")).limit(5).collect()]

orders_salted = orders.withColumn(
    "salt",
    F.when(F.col("merchant_id").isin(hot_keys), F.floor(F.rand(seed=7) * N)).otherwise(F.lit(0))
).withColumn("salt", F.col("salt").cast("int"))

salts = spark.range(N).withColumnRenamed("id", "salt").withColumn("salt", F.col("salt").cast("int"))
merchants_salted = (merchants.filter(F.col("merchant_id").isin(hot_keys)).crossJoin(salts)
                    .unionByName(merchants.filter(~F.col("merchant_id").isin(hot_keys))
                                 .withColumn("salt", F.lit(0))))

joined = orders_salted.join(merchants_salted, ["merchant_id", "salt"]).drop("salt")

# Two-stage salted aggregation
revenue = (orders_salted.groupBy("merchant_id", "salt").agg(F.sum("amount").alias("part_sum"))
           .groupBy("merchant_id").agg(F.sum("part_sum").alias("revenue")))
```
The hot-key list is collected to the driver, but it is tiny (5 values), so `collect()` is safe here. `isin` with a NULL in the list never matches, which is fine because NULL keys should be handled separately (see [Q263](/interview/pyspark/#q263)).

**Real-world use case**

A marketplace job's join on `merchant_id` had one merchant with 40% of orders, and the single task ran for 2 hours. Selective salting of the top 5 merchants with N = 32 cut the stage to 9 minutes. After a Spark 3 upgrade, AQE skew join handled it without salting.

**Common mistakes and red flags**

- Salting without replicating the other side, which loses matches.
- Salting all keys, needlessly multiplying the other side.
- Not checking AQE first.

**Follow-up questions**

- Why use a seed in `rand`? — For reproducibility across retries; non-deterministic salts can cause inconsistent results if a stage is recomputed.
- How do you salt a window function? — Usually you can't directly; pre-aggregate or partition by a finer key where semantics allow.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q265" data-level="Intermediate">
<summary><span class="qb-num">Q265</span><h3 class="qb-title">Adaptive Query Execution: what does it do?</h3><span class="qb-meta">Intermediate · Very Common · Optimisation</span></summary>

**Short answer**

Adaptive Query Execution (AQE), on by default since Spark 3.2, re-optimises the physical plan at runtime using actual statistics from completed shuffle stages. It does three main things: it coalesces small shuffle partitions, switches join strategies (for example sort-merge to broadcast once a side turns out small), and splits skewed partitions in joins. That removes much manual tuning of partition counts and join hints.

**Detailed explanation**

**How it works:** Spark executes the query stage by stage. At each shuffle or broadcast boundary (a "query stage"), it collects real output sizes and re-plans the remaining stages.

**Main features**
| Feature | Config (default) | What it does |
|---|---|---|
| Coalesce shuffle partitions | `spark.sql.adaptive.coalescePartitions.enabled` (true) | Merges small adjacent partitions toward `advisoryPartitionSizeInBytes` (64 MB) |
| Switch join strategy | `spark.sql.adaptive.autoBroadcastJoinThreshold` | Converts SMJ to BHJ when a side is small at runtime; can use local shuffle readers to avoid extra shuffle |
| Skew join optimisation | `spark.sql.adaptive.skewJoin.enabled` (true) | Splits partitions larger than `skewedPartitionFactor` (5) × median and above `skewedPartitionThresholdInBytes` (256 MB) into sub-partitions, replicating the other side's matching partition |
| Shuffled hash join preference | `spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold` | Picks SHJ when partitions are small enough |
| Empty relation propagation | (enabled) | Short-circuits joins with empty sides |
| Rebalance partitions | `REBALANCE` hint | Evens out partitions (useful before writes) |

**Limits**
- It needs a shuffle or exchange to gather statistics, so it can't fix the first scan stage.
- Skew handling applies to joins (sort-merge and shuffle hash), not to aggregations or window functions in general.
- It doesn't apply to streaming queries' stateful operators in the same way (micro-batch planning is limited).
- In the Spark UI, the plan shows `AdaptiveSparkPlan isFinalPlan=true` once execution finishes. Look at the final plan, not just the initial one.

**Practical defaults:** keep AQE on, set `spark.sql.shuffle.partitions` reasonably high (or use `initialPartitionNum`), and let AQE coalesce. Tune the advisory size for your workloads, typically 64–256 MB.

**Example**

```python
# Runtime: PySpark 3.5 (AQE settings shown explicitly; most are defaults)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", str(128 * 1024 * 1024))
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", str(256 * 1024 * 1024))

result = spark.table("silver.orders").join(spark.table("silver.payments"), "order_id")
result.explain()            # initial plan: AdaptiveSparkPlan isFinalPlan=false
result.write.mode("overwrite").saveAsTable("gold.order_payments")
# After run, the SQL tab shows the final plan with coalesced partitions / skew splits
```

**Real-world use case**

After upgrading from Spark 2.4 to 3.x with AQE on, a team removed custom salting code and hand-tuned partition counts from 30 jobs. Runtimes stayed the same or improved, and the code became much simpler.

**Common mistakes and red flags**

- Disabling AQE "for predictability" without evidence.
- Expecting AQE to fix aggregation skew.
- Reading only the initial plan.

**Follow-up questions**

- When was AQE enabled by default? — Spark 3.2.
- Does AQE work with Photon/Databricks? — Yes; Databricks extends AQE with additional runtime optimisations.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q266" data-level="Beginner">
<summary><span class="qb-num">Q266</span><h3 class="qb-title">cache() vs persist() and storage levels</h3><span class="qb-meta">Beginner · Very Common · Caching</span></summary>

**Short answer**

`cache()` and `persist()` both keep a DataFrame's computed partitions so later actions reuse them instead of recomputing from source. For DataFrames, `cache()` is `persist()` with the default level `MEMORY_AND_DISK` (memory-and-disk deserialised in newer versions). `persist(level)` lets you choose `MEMORY_ONLY`, `DISK_ONLY`, `OFF_HEAP`, replicated variants and others. Both are lazy and materialise on the next action. Call `unpersist()` to release them.

**Detailed explanation**

**Storage levels (common ones)**
| Level | Memory | Disk | Serialized | Replication | Notes |
|---|---|---|---|---|---|
| `MEMORY_ONLY` | ✓ | | (deserialized for RDD) | 1 | Recompute partitions that don't fit |
| `MEMORY_AND_DISK` (DataFrame default for `cache()`) | ✓ | ✓ | DataFrames use columnar compressed format | 1 | Spill to disk if memory is full |
| `MEMORY_ONLY_SER` / `MEMORY_AND_DISK_SER` (Scala/Java RDD) | ✓ | (✓) | ✓ | 1 | Less memory, more CPU |
| `DISK_ONLY` | | ✓ | ✓ | 1 | When memory is scarce |
| `*_2` variants | | | | 2 | Replicated for faster recovery |
| `OFF_HEAP` | off-heap | | ✓ | 1 | Requires off-heap config |

For DataFrames, cached data is stored in an **in-memory columnar format** (compressed), which is efficient for scans.

**When caching helps**
- A DataFrame is reused in **multiple actions** or branches (written to two outputs, used in several joins).
- Iterative algorithms (ML training loops).
- An expensive computation (heavy joins) feeding several downstream steps.

**How to use it correctly**
1. `df.cache()` (lazy).
2. Trigger materialisation with the first real action. A `count()` forces a full materialisation, which costs time. Often the first downstream write does the job.
3. Reuse `df`.
4. `df.unpersist()` when done (memory is otherwise held until evicted or the session ends).

**Check it:** the Spark UI Storage tab shows the fraction cached, memory and disk use. `df.storageLevel` shows the level.

**Platform note:** on Databricks, the **disk cache** (formerly Delta cache) automatically caches remote Parquet and Delta data on local SSDs, which is separate from Spark caching. Serverless and some shared compute modes restrict `cache()` or `persist()`.

**Example**

```python
# Runtime: PySpark 3.5
from pyspark import StorageLevel
from pyspark.sql import functions as F

enriched = (spark.read.table("silver.orders")
            .join(spark.read.table("silver.customers"), "customer_id")
            .withColumn("order_month", F.date_trunc("month", "order_ts")))

enriched.persist(StorageLevel.MEMORY_AND_DISK)

(enriched.groupBy("order_month", "segment").agg(F.sum("amount").alias("revenue"))
         .write.mode("overwrite").saveAsTable("gold.revenue_by_segment"))     # materialises cache
(enriched.groupBy("order_month", "country").agg(F.countDistinct("customer_id").alias("buyers"))
         .write.mode("overwrite").saveAsTable("gold.buyers_by_country"))      # reuses cache

enriched.unpersist()
```

**Real-world use case**

A job wrote five gold tables from the same expensive enriched DataFrame and recomputed a 2 TB join five times. Persisting the enriched DataFrame once cut runtime from 2.5 hours to 50 minutes.

**Common mistakes and red flags**

- Caching everything "just in case" (see [Q267](/interview/pyspark/#q267)).
- Forgetting `unpersist()` in long-running sessions.
- Thinking `cache()` executes immediately.

**Follow-up questions**

- How do you cache a table in SQL? — `CACHE TABLE t` (eager by default) / `CACHE LAZY TABLE t`; `UNCACHE TABLE t`.
- What happens if cached partitions are evicted? — They're recomputed from lineage (or read from disk if the level includes disk).

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q267" data-level="Intermediate">
<summary><span class="qb-num">Q267</span><h3 class="qb-title">When caching hurts, and how to unpersist</h3><span class="qb-meta">Intermediate · Common · Caching</span></summary>

**Short answer**

Caching hurts when the data is used only once, so materialisation is pure overhead. It also hurts when the cached data competes with execution memory (more spill and GC), when it is bigger than memory and keeps getting evicted and recomputed, or when it holds stale results while the source changes. Cache only reused, expensive intermediates. Release them with `unpersist()` or `spark.catalog.clearCache()`.

**Detailed explanation**

**Costs of caching**
- **Materialisation work:** computing and storing all partitions, sometimes forced with `count()`.
- **Memory pressure:** cached blocks share the unified memory pool with execution (see [Q271](/interview/pyspark/#q271)). Heavy caching can squeeze joins and aggregations, causing spill and GC pauses.
- **Eviction churn:** data that doesn't fit gets evicted (LRU) and recomputed. With `MEMORY_ONLY` you can end up paying for both caching and recomputation.
- **Plan interference:** a cached DataFrame becomes an `InMemoryRelation` in plans. Filters may no longer push down to the source files, so caching a whole table and then filtering it can be slower than reading the filtered data directly.
- **Staleness:** a cache holds a snapshot. If the underlying Delta table changes, the cached data doesn't (Spark invalidates some caches on writes through the same session, but not on external changes).

**Anti-patterns**
- `df.cache()` on every DataFrame "for speed".
- Caching right after reading a file that is used once.
- Caching huge raw tables, then filtering.
- Calling `count()` after every cache just to materialise it.
- Never unpersisting in notebooks or long sessions, so memory slowly fills.

**Better alternatives**
- Write an intermediate Delta or Parquet table if it's reused across jobs, or if lineage is very long (also see checkpointing in Q287).
- Rely on platform disk caches (Databricks disk cache) for repeated reads of the same files.
- Restructure code so one pipeline writes multiple outputs in one pass where possible.

**Releasing memory**
- `df.unpersist()` (asynchronous by default, `blocking=True` to wait).
- `spark.catalog.uncacheTable("t")` and `spark.catalog.clearCache()`.
- Check the Spark UI Storage tab to make sure nothing unexpected is cached.

**Example**

```python
# Runtime: PySpark 3.5
from pyspark.sql import functions as F

raw = spark.read.table("bronze.events")

# Anti-pattern: cache huge raw table, then filter (loses file-level pruning, eats memory)
# raw.cache(); raw.count(); march = raw.filter(F.col("event_date") >= "2026-03-01")

# Better: filter first so pruning applies; cache only the reused, smaller intermediate
march = raw.filter(F.col("event_date") >= "2026-03-01").select("user_id", "event_type", "event_ts")
march.cache()
march.groupBy("event_type").count().write.mode("overwrite").saveAsTable("gold.march_event_counts")
march.groupBy("user_id").agg(F.max("event_ts").alias("last_seen")) \
     .write.mode("overwrite").saveAsTable("gold.march_last_seen")
march.unpersist()
```

**Real-world use case**

A shared notebook cluster kept slowing down through the day. The Storage tab showed 400 GB of forgotten cached DataFrames from many users' sessions. A policy of clearing caches at the end of notebooks, and fewer blanket `.cache()` calls, fixed the slowdowns.

**Common mistakes and red flags**

- Treating caching as free.
- Caching before filtering.
- Ignoring the Storage tab.

**Follow-up questions**

- Cache vs writing a temp table? — Cache lives only in that application's memory/disk; a table persists, survives failures, and is shareable.
- Does caching truncate lineage? — No; checkpointing does (see [Q287](/interview/pyspark/#q287)).

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q268" data-level="Advanced">
<summary><span class="qb-num">Q268</span><h3 class="qb-title">Catalyst optimizer phases</h3><span class="qb-meta">Advanced · Common · Optimisation Internals</span></summary>

**Short answer**

Catalyst turns DataFrame and SQL code into an optimised execution plan in four phases. Analysis resolves names and types against the catalog. Logical optimisation applies rule-based rewrites such as predicate pushdown, constant folding and column pruning. Physical planning generates physical plans and picks one, choosing join strategies and using cost and statistics. Code generation (Tungsten) compiles the chosen plan into efficient JVM bytecode.

**Detailed explanation**

**1. Analysis (unresolved → resolved logical plan)**
- Resolves table and column names, data types and functions through the catalog.
- Errors like "cannot resolve column" appear here, at plan time, not at runtime.

**2. Logical optimisation (rule-based)**
- **Predicate pushdown:** move filters closer to the source, through joins and projections, and into scans (see [Q277](/interview/pyspark/#q277)).
- **Column pruning:** read only the needed columns.
- **Constant folding**, null propagation and boolean simplification.
- **Combining filters and projections**, removing redundant operations.
- **Join reordering** (with CBO statistics; `spark.sql.cbo.enabled`, plus join reorder settings).
- **Subquery decorrelation** (see [Q124](/interview/sql/#q124)).

**3. Physical planning**
- Generates one or more physical plans: join strategy (broadcast, sort-merge, shuffle hash), aggregation strategy (hash vs sort), and exchange (shuffle) placement.
- Uses size estimates and statistics (`ANALYZE TABLE ... COMPUTE STATISTICS [FOR COLUMNS ...]`) to choose.
- **AQE** can re-plan later stages at runtime (see [Q265](/interview/pyspark/#q265)).

**4. Code generation**
- **Whole-stage code generation** fuses chains of operators into single Java functions, avoiding virtual calls and intermediate objects (see [Q269](/interview/pyspark/#q269)). The operators show up with `*` and codegen IDs in `explain()` output.

**What blocks Catalyst**
- **Python UDFs** are black boxes. No pushdown through them, and data is serialised to Python (see [Q270](/interview/pyspark/#q270)).
- RDD operations (outside Catalyst).
- Non-deterministic expressions limit some rewrites.
- Missing statistics lead to poor join choices.

**Extending it:** Catalyst is rule-based and extensible. Libraries can add rules through `SparkSessionExtensions`, which Delta Lake uses for its commands.

**Example**

```python
# Runtime: PySpark 3.5
from pyspark.sql import functions as F

q = (spark.read.table("silver.orders")
     .join(spark.read.table("silver.customers"), "customer_id")
     .filter((F.col("country") == "IN") & (F.col("order_date") >= "2026-03-01"))
     .select("order_id", "amount", "segment"))

q.explain(mode="extended")   # parsed -> analyzed -> optimized logical -> physical plans
```
In the optimised logical plan, the filters on `country` and `order_date` are pushed below the join to each side, and only the needed columns are read from each table.

**Real-world use case**

A query filtered on a column computed by a Python UDF, and every row was sent to Python before filtering. Replacing the UDF with built-in functions let Catalyst push the filter into the Parquet scan, and runtime dropped 10-fold.

**Common mistakes and red flags**

- Not knowing what predicate pushdown or column pruning are.
- Thinking Catalyst optimises Python UDF internals.
- Never computing table statistics.

**Follow-up questions**

- What is CBO? — Cost-based optimisation using table/column statistics for join reordering and strategy selection.
- Where do you see the optimised plan? — `explain("extended")` or the SQL tab in the Spark UI.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q269" data-level="Advanced">
<summary><span class="qb-num">Q269</span><h3 class="qb-title">Project Tungsten and whole-stage code generation</h3><span class="qb-meta">Advanced · Common · Optimisation Internals</span></summary>

**Short answer**

Project Tungsten is Spark's execution-engine work for CPU and memory efficiency. It has three parts: off-heap and binary memory management (compact UnsafeRow format, less GC), cache-aware algorithms and data structures, and whole-stage code generation. Codegen fuses many operators into a single generated Java function per stage, which avoids per-row virtual calls and object creation.

**Detailed explanation**

**1. Memory management and binary format**
- Rows are stored as **UnsafeRow**, a compact binary layout, instead of Java objects. That means less memory, less garbage collection, and fast comparisons and hashing on raw bytes.
- Optional **off-heap memory** (`spark.memory.offHeap.enabled`) for some data structures.
- Shuffle and sort operate on serialised binary data.

**2. Cache-aware computation**
- Sorting and hashing structures are designed for CPU cache locality (for example sorting on key prefixes).

**3. Whole-stage code generation (WSCG)**
- The classic "Volcano" model calls `next()` per row per operator, which costs a lot of virtual calls.
- WSCG generates one tight loop for a pipeline (scan → filter → project → partial aggregate), compiled at runtime by Janino.
- In `explain()` output, operators prefixed with `*(n)` share codegen stage n.
- Falls back to interpreted execution when generated code would be too large (`spark.sql.codegen.hugeMethodLimit`) or for unsupported expressions.

**Vectorised readers:** Parquet and ORC vectorised readers decode columns in batches (`spark.sql.parquet.enableVectorizedReader`), feeding efficient columnar processing.

**Beyond Tungsten:** native engines (**Photon** on Databricks; **Gluten/Velox** and **Comet** in open source) push further with vectorised C++ or Rust execution (see [Q232](/interview/databricks/#q232)).

**Why it matters for engineers**
- Built-in functions benefit fully. Python UDFs break the generated pipeline and serialise data (see [Q270](/interview/pyspark/#q270)).
- Very wide rows or extremely complex expressions can disable codegen. Splitting complex expressions can help.
- Codegen issues sometimes appear as warnings in logs ("Generated method too long"), which hint at fallback to slower paths.

**Example**

```python
# Runtime: PySpark 3.5
from pyspark.sql import functions as F

df = (spark.range(0, 10_000_000)
      .withColumn("bucket", F.col("id") % 100)
      .filter(F.col("bucket") < 10)
      .groupBy("bucket").agg(F.sum("id").alias("total")))

df.explain(mode="formatted")
# Look for "WholeStageCodegen (1)" / "*(1)" groupings around Range, Filter, Project, HashAggregate
```

**Real-world use case**

A transformation with 400 chained `withColumn` CASE expressions produced "generated code too large" warnings and fell back to interpreted execution. Consolidating the expressions with `select` and splitting logic into two stages restored codegen and halved the runtime.

**Common mistakes and red flags**

- Not knowing what UnsafeRow or WSCG are at a high level.
- Thinking Tungsten is a separate product to install.
- Chaining hundreds of `withColumn` calls (plan bloat).

**Follow-up questions**

- Why is `select` preferred to many `withColumn` calls? — Each `withColumn` adds a projection to the plan; huge chains slow analysis and may hurt codegen.
- How do you disable WSCG for debugging? — `spark.sql.codegen.wholeStage=false` (not for production).

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q270" data-level="Intermediate">
<summary><span class="qb-num">Q270</span><h3 class="qb-title">Python UDF vs pandas UDF vs built-in functions</h3><span class="qb-meta">Intermediate · Very Common · PySpark Performance</span></summary>

**Short answer**

Built-in functions (`pyspark.sql.functions`) run inside the JVM or a native engine with full Catalyst and codegen optimisation, so they are the fastest. Plain Python UDFs process rows one at a time in Python worker processes, with serialisation between the JVM and Python, and are the slowest. pandas (vectorised) UDFs use Apache Arrow to send column batches to Python and run vectorised pandas or NumPy code, which is often 10–100 times faster than row UDFs.

**Detailed explanation**

| Option | Execution | Optimizer visibility | Speed | Use for |
|---|---|---|---|---|
| Built-in functions / SQL expressions | JVM (codegen) / Photon | Full (pushdown, pruning) | Fastest | Almost everything: string, date, JSON, arrays, math, regex |
| Python UDF (`@udf`) | Row-by-row in Python workers | Black box | Slowest | Truly custom logic with no built-in alternative, small data |
| Arrow-optimised Python UDF (Spark 3.5+, `useArrow=True`) | Batches via Arrow, row logic | Black box | Faster than classic UDF | Existing row UDFs, quick improvement |
| pandas UDF (`@pandas_udf`) | Batches as pandas Series/DataFrames via Arrow | Black box | Fast | Vectorised numeric/ML logic, library calls (scikit-learn, scipy) |
| `mapInPandas` / `applyInPandas` / `mapInArrow` | Batches per partition/group | Black box | Fast | Per-group models, complex per-partition processing |
| Scala/Java UDF | JVM | Mostly black box | Fast | Performance-critical custom logic |

**Why Python UDFs are slow**
- Data is serialised from JVM rows to Python objects (pickle) and back.
- Row-at-a-time function calls in Python.
- Catalyst can't see inside, so no predicate pushdown or constant folding through the UDF.
- NULL handling and type conversion overhead.

**Guidelines**
1. Search for a built-in first: `regexp_extract`, `from_json`, `transform` (higher-order array functions), `date_format`, `when`/`otherwise`, `aggregate`.
2. Use **pandas UDFs** when custom Python is necessary.
3. Declare **return types** precisely.
4. Apply UDFs **after** filtering, so fewer rows go through Python.
5. Check the plan for `BatchEvalPython` (row UDF) or `ArrowEvalPython` (pandas UDF) nodes.

**Example**

```python
# Runtime: PySpark 3.5 (pandas + pyarrow installed)
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

orders = spark.read.table("silver.orders")

# 1) Built-in (preferred): tiered discount
with_builtin = orders.withColumn(
    "discount",
    F.when(F.col("amount") >= 500, F.col("amount") * 0.10)
     .when(F.col("amount") >= 100, F.col("amount") * 0.05)
     .otherwise(F.lit(0.0)))

# 2) Row-at-a-time Python UDF (slow)
@F.udf(returnType=DoubleType())
def discount_udf(amount):
    if amount is None:
        return None
    return amount * 0.10 if amount >= 500 else amount * 0.05 if amount >= 100 else 0.0

# 3) Vectorised pandas UDF (fast when Python is required)
@F.pandas_udf(DoubleType())
def discount_pudf(amount: pd.Series) -> pd.Series:
    rate = pd.Series(0.0, index=amount.index)
    rate[amount >= 100] = 0.05
    rate[amount >= 500] = 0.10
    return amount * rate

with_pudf = orders.withColumn("discount", discount_pudf(F.col("amount").cast("double")))
```
All three give the same results for non-NULL amounts. The pandas UDF returns NaN/NULL for NULL inputs automatically.

**Real-world use case**

A user-agent parsing UDF processed 3 billion rows in 4 hours. Swapping it for a pandas UDF calling the same library on batches cut that to 35 minutes. Replacing the simple fields with `regexp_extract` built-ins cut it further, to 12 minutes.

**Common mistakes and red flags**

- Writing UDFs for things built-ins already do.
- Applying UDFs before filters.
- Wrong return types, producing NULLs silently.

**Follow-up questions**

- What does Arrow do here? — Columnar in-memory format enabling zero-copy-ish batch transfer between JVM and Python.
- Can UDFs be used in SQL? — Yes, register with `spark.udf.register("name", fn)` (or create SQL functions in Unity Catalog).

**In depth:** [When should you avoid Python UDFs in PySpark?](/interview/pyspark/avoid-python-udfs/)

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q271" data-level="Advanced">
<summary><span class="qb-num">Q271</span><h3 class="qb-title">Executor memory model: unified, execution, storage, overhead</h3><span class="qb-meta">Advanced · Very Common · Memory</span></summary>

**Short answer**

An executor's JVM heap (`spark.executor.memory`) contains a reserved 300 MB plus a unified region (`spark.memory.fraction`, default 0.6 of the remaining heap) shared by execution memory (shuffles, joins, sorts, aggregations) and storage memory (cache, broadcasts). The rest is user memory. Outside the heap, `spark.executor.memoryOverhead` (default the larger of 10% or 384 MB) covers off-heap needs, including PySpark worker processes. Exceed the container limit and the cluster manager kills the executor.

**Detailed explanation**

**Layout (per executor)**
```text
Container memory  =  spark.executor.memory (heap)  +  memoryOverhead  (+ offHeap.size if enabled, + pyspark memory if set)

Heap:
 ├─ Reserved memory: 300 MB
 ├─ Unified memory: (heap - 300MB) * spark.memory.fraction (0.6)
 │    ├─ Storage (cache, broadcast)  ─┐  boundary moves:
 │    └─ Execution (shuffle, join,    ┘  execution can evict storage down to
 │        sort, aggregation buffers)     spark.memory.storageFraction (0.5) of unified
 └─ User memory: remaining 40% (your data structures, UDF objects, metadata)
```

**Unified memory rules**
- Execution and storage borrow from each other.
- Execution can **evict cached blocks** (down to the protected `storageFraction`). Storage can't evict execution memory.
- When execution memory runs out, operators **spill** to disk (sort and aggregation buffers), which is slower but safe.

**Memory overhead**
- Off-heap allocations: JVM internals, thread stacks, NIO buffers, Netty for shuffle, and **Python worker processes** for UDFs and pandas UDFs. With `spark.executor.pyspark.memory` unset, Python memory comes from the overhead.
- Container killed messages such as "Container killed by YARN for exceeding memory limits" or Kubernetes `OOMKilled` usually mean the **overhead** is too small (heavy Python UDFs, Arrow batches, many cores per executor). They are not necessarily heap problems.

**Per-task memory:** execution memory is shared across the concurrently running tasks on an executor, so each task gets roughly 1/N of it when N tasks are active. More cores per executor means less memory per task.

**Symptoms and levers**
| Symptom | Lever |
|---|---|
| Large spill (memory/disk) in stages | More partitions (smaller tasks), more memory per core, fewer cores per executor |
| `java.lang.OutOfMemoryError: Java heap space` | Reduce per-task data (skew, partitions), increase executor memory, avoid huge broadcasts |
| Container killed for exceeding memory limits | Increase `memoryOverhead`, limit Python batch sizes (`spark.sql.execution.arrow.maxRecordsPerBatch`) |
| Long GC pauses | Fewer cores per executor, smaller heaps, avoid caching huge deserialized objects |

**Example**

```python
# Runtime: PySpark 3.5 (typically set at cluster/session creation, not mid-session)
from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName("memory-tuned-job")
         .config("spark.executor.memory", "24g")
         .config("spark.executor.memoryOverhead", "4g")     # room for Python workers / Arrow
         .config("spark.executor.cores", "4")
         .config("spark.memory.fraction", "0.6")
         .config("spark.sql.execution.arrow.maxRecordsPerBatch", "5000")
         .getOrCreate())
```

**Real-world use case**

A pandas-UDF-heavy job kept losing executors with "exceeding memory limits" despite 32 GB heaps. The Python workers were using memory outside the heap. Raising `memoryOverhead` to 6 GB and lowering the Arrow batch size fixed it, while the heap was actually reduced.

**Common mistakes and red flags**

- Raising `executor.memory` for container-killed errors that are overhead problems.
- Not knowing spill is the safety valve.
- Many cores per executor with little memory.

**Follow-up questions**

- What is spill? — Writing in-memory buffers to disk when execution memory is insufficient; visible as Spill (Memory/Disk) in the UI.
- What's `spark.memory.storageFraction`? — The portion of unified memory protected for storage from eviction by execution (default 0.5).

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q272" data-level="Advanced">
<summary><span class="qb-num">Q272</span><h3 class="qb-title">Driver OOM vs executor OOM: causes and fixes</h3><span class="qb-meta">Advanced · Very Common · Memory</span></summary>

**Short answer**

Driver OOM usually comes from pulling data to the driver (`collect`, `toPandas`, large broadcasts built on the driver), huge query plans, or many concurrent jobs and metadata. Executor OOM comes from too much data per task (skew, too few partitions), memory-hungry operations (huge explode, big windows, collect_list on hot keys), oversized broadcasts, or Python worker memory exceeding the overhead. Fix the data flow first, then memory settings.

**Detailed explanation**

**Driver OOM**
| Cause | Fix |
|---|---|
| `collect()` / `toPandas()` / `take(huge)` | Write results with `df.write`; aggregate before collecting; `limit` |
| Broadcasting large tables (driver may build broadcast) | Lower/disable broadcast threshold; let AQE decide; don't force hints on big tables |
| `spark.driver.maxResultSize` exceeded (default 1 GB) | Avoid large results; raise cautiously only if truly needed |
| Giant plans (thousands of `withColumn`, loops unioning hundreds of DataFrames) | Restructure: `select` with many expressions, build lists then one `unionByName`, checkpoint long lineage |
| Listing millions of files / huge metadata | Compact files; table formats with metadata pruning |
| Many streaming queries or threads on one driver | Split into separate jobs |
| Python driver code holding large objects (pandas) | Keep data in Spark; process in executors |

**Executor OOM**
| Cause | Fix |
|---|---|
| Skewed key → one giant partition | AQE skew join, salting, filter NULL keys (see [Q263](/interview/pyspark/#q263), [Q264](/interview/pyspark/#q264)) |
| Too few shuffle partitions | Increase partitions; AQE coalesce (see [Q259](/interview/pyspark/#q259)) |
| `collect_list`/`collect_set` on hot keys, huge arrays | Limit, pre-aggregate, or redesign |
| `explode` of large arrays | Filter first, process in chunks |
| Wide window over huge partitions | Narrower partition key, pre-aggregation |
| Broadcast too large for executors | Disable broadcast for that join |
| Container killed (overhead) | Raise `memoryOverhead`; reduce Arrow batch size (see [Q271](/interview/pyspark/#q271)) |
| Too many cores per executor | Fewer cores → more memory per task |

**Diagnosis**
- Read the stack trace and where it happened: driver logs vs executor logs.
- **Spark UI:** failed stage, the tasks that failed, shuffle read size of those tasks (skew), and spill.
- **Executors tab:** memory, GC time, failed task counts.
- Reproduce on a sample with the same skew pattern, if possible.

**Example**

```python
# Runtime: PySpark 3.5
from pyspark.sql import functions as F

events = spark.read.table("silver.events")

# Driver OOM risk: pulling 200M rows to pandas
# pdf = events.toPandas()

# Instead: aggregate in Spark, bring only the small result
daily = events.groupBy(F.to_date("event_ts").alias("d")).count()
pdf = daily.toPandas()                       # a few hundred rows

# Executor OOM risk: collecting all events per user into one array on a skewed key
# per_user = events.groupBy("user_id").agg(F.collect_list("event_type"))

# Instead: compute what you need without materialising giant arrays
per_user = events.groupBy("user_id", "event_type").count()
```

**Real-world use case**

A nightly job died with executor OOM after a bot generated 80 million events for one `user_id`. A `collect_list` per user exploded on that key. Filtering known bot IDs upstream and replacing `collect_list` with counts fixed it permanently.

**Common mistakes and red flags**

- Raising memory without finding the cause.
- Setting `maxResultSize = 0` (unlimited) to "fix" collect issues.
- Confusing heap OOM with container overhead kills.

**Follow-up questions**

- Where do you see driver memory issues? — Driver logs, GC messages, and the Spark UI becoming unresponsive.
- How do you avoid big plans in loops? — Accumulate DataFrames in a list and `reduce(DataFrame.unionByName, dfs)` once, or checkpoint periodically.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q273" data-level="Advanced">
<summary><span class="qb-num">Q273</span><h3 class="qb-title">How do you size executors (cores and memory)?</h3><span class="qb-meta">Advanced · Common · Resource Tuning</span></summary>

**Short answer**

A common starting point is about 4–5 cores per executor (good throughput without too much GC or HDFS client contention), with memory sized so each core gets roughly 4–8 GB of heap. Leave room for memory overhead and OS or daemons on each node, and leave 1 core and some memory for node services. Then tune from Spark UI metrics such as spill, GC time and task duration.

**Detailed explanation**

**Worked example (self-managed YARN-style node)**
- Node: 16 cores, 128 GB RAM.
- Reserve 1 core and about 8 GB for the OS and node manager, leaving 15 cores and about 120 GB.
- 5 cores per executor gives 3 executors per node.
- About 40 GB per executor container. Subtract overhead (max of 10% or 384 MB, about 4 GB), so `spark.executor.memory` ≈ 36 GB and `memoryOverhead` ≈ 4 GB.
- On 10 nodes: 30 executors × 5 cores = 150 task slots. Leave one executor's worth for the YARN Application Master if the driver runs in cluster mode.

**Trade-offs**
| Few large executors (many cores) | Many small executors (few cores) |
|---|---|
| Share broadcast variables and cache across more tasks | Less GC pressure per JVM |
| Fewer JVMs → less overhead | More JVMs → more overhead, more broadcast copies |
| Long GC pauses; HDFS throughput issues with >5 concurrent tasks | Smaller heaps can't handle big partitions |
| Single failure loses more work | Better fault isolation |

"Tiny" executors (1 core each) lose the benefits of sharing within the JVM. "Fat" executors (all cores of a node) suffer GC and I/O contention. The middle ground usually wins.

**Managed platforms**
- **Databricks:** typically one executor per worker node, using all of its cores. You size by choosing **node types** (memory- vs compute- vs storage-optimised) and worker counts (see [Q230](/interview/databricks/#q230)). Serverless sizes itself.
- **EMR:** `maximizeResourceAllocation` or instance-fleet defaults. Still validate them.
- **Kubernetes:** set pod CPU and memory requests and limits to match executor cores, memory and overhead.

**Tuning from metrics**
- **Spill or OOM:** more memory per core (fewer cores per executor, or bigger nodes), or more partitions.
- **High GC time (>10% of task time):** smaller heaps per executor or fewer cores, and less caching of deserialised objects.
- **Idle cores:** too few partitions or tasks, or skew.
- **Short tasks (<100 ms):** too many partitions, so reduce them.

**Example**

```bash
# spark-submit on YARN for the worked example above
spark-submit \
  --master yarn --deploy-mode cluster \
  --num-executors 29 \
  --executor-cores 5 \
  --executor-memory 36g \
  --conf spark.executor.memoryOverhead=4g \
  --driver-memory 8g \
  --conf spark.sql.shuffle.partitions=600 \
  orders_etl.py
```

**Real-world use case**

A team ran 1 executor with 15 cores and a 110 GB heap per node and saw 30% GC time. Switching to 3 executors × 5 cores × 36 GB per node cut GC time to 5%, and job runtime fell by 25% on the same hardware.

**Common mistakes and red flags**

- Using all node memory for the heap, with no overhead.
- One-core executors everywhere.
- Copying sizing formulas onto Databricks without considering node types.

**Follow-up questions**

- How many partitions for 150 cores? — Typically 2–4× cores (300–600) for good load balancing, adjusted by data size.
- Why ~5 cores? — Empirical sweet spot for throughput vs GC/HDFS contention; not a law.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q274" data-level="Intermediate">
<summary><span class="qb-num">Q274</span><h3 class="qb-title">Dynamic allocation</h3><span class="qb-meta">Intermediate · Common · Resource Tuning</span></summary>

**Short answer**

Dynamic allocation (`spark.dynamicAllocation.enabled`) lets Spark add executors when tasks are pending and remove them when idle, within min and max bounds. Removing executors safely requires their shuffle files to stay available. That comes from an external shuffle service, or from shuffle tracking (`spark.dynamicAllocation.shuffleTracking.enabled`, for example on Kubernetes), which keeps executors holding needed shuffle data alive.

**Detailed explanation**

**Key settings**
| Setting | Purpose |
|---|---|
| `spark.dynamicAllocation.enabled` | Turn on |
| `minExecutors` / `maxExecutors` / `initialExecutors` | Bounds and start size |
| `schedulerBacklogTimeout` (1 s) / `sustainedSchedulerBacklogTimeout` | How quickly to request more executors when tasks queue (requests grow exponentially) |
| `executorIdleTimeout` (60 s) | Remove executors idle this long |
| `cachedExecutorIdleTimeout` (infinite) | Idle timeout for executors holding cached data |
| `shuffleTracking.enabled` | Keep executors with active shuffle data (no external service needed) |
| `spark.shuffle.service.enabled` | Use external shuffle service (YARN/standalone) |

**Benefits**
- Shared clusters use resources efficiently, because jobs grab capacity when needed and release it afterwards.
- Long-running applications (notebooks, Thrift servers) don't hold idle executors.

**Caveats**
- **Executor start-up latency:** scaling up takes time (container or VM allocation). Short, bursty stages may finish before new executors arrive.
- **Cached data** pins executors (with the infinite cached idle timeout), or is lost when they're removed.
- **Streaming:** classic dynamic allocation isn't well suited to Structured Streaming. Use fixed sizing, or platform features (Databricks enhanced autoscaling for pipelines).
- **Max executors** must be capped on shared clusters, otherwise one job can starve others.

**Relationship to cluster autoscaling:** dynamic allocation scales **executors within available resources**. Cluster autoscaling (Databricks, EMR managed scaling, Kubernetes cluster autoscaler) scales **nodes**. They work together: pending executors trigger node scale-up. On Databricks, cluster autoscaling is the main mechanism (see [Q230](/interview/databricks/#q230)).

**Example**

```python
# Runtime: PySpark 3.5 on Kubernetes (settings applied at session creation)
from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName("adhoc-analytics")
         .config("spark.dynamicAllocation.enabled", "true")
         .config("spark.dynamicAllocation.shuffleTracking.enabled", "true")
         .config("spark.dynamicAllocation.minExecutors", "2")
         .config("spark.dynamicAllocation.maxExecutors", "40")
         .config("spark.dynamicAllocation.executorIdleTimeout", "120s")
         .getOrCreate())
```

**Real-world use case**

A shared YARN cluster ran 30 analyst notebooks, each holding 20 static executors all day. Enabling dynamic allocation (min 1, max 30, 2-minute idle timeout) with the external shuffle service let the same cluster serve twice as many users.

**Common mistakes and red flags**

- Enabling it without a shuffle service or shuffle tracking, which loses shuffle data.
- No `maxExecutors` on shared clusters.
- Expecting instant scale-up for short stages.

**Follow-up questions**

- What is decommissioning? — Graceful executor removal that migrates shuffle/cache blocks before shutdown (Spark 3.1+), useful with spot instances.
- Dynamic allocation vs AQE? — Allocation scales resources; AQE changes the plan.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q275" data-level="Intermediate">
<summary><span class="qb-num">Q275</span><h3 class="qb-title">Reading the Spark UI</h3><span class="qb-meta">Intermediate · Common · Monitoring</span></summary>

**Short answer**

The Spark UI shows Jobs (one per action), Stages (durations, input, shuffle read and write, spill, the task time distribution), Storage (cached data), Environment (configuration), Executors (memory, GC, failed tasks) and the SQL/DataFrame tab (physical plan with per-operator metrics). Use it to find the slowest stage and why: skew, spill, too much shuffle, too many tasks, or GC pressure.

**Detailed explanation**

**Tabs and what to look for**
| Tab | Look for |
|---|---|
| Jobs | Which action is slow; failed jobs; event timeline (executor add/remove) |
| Stages | Duration; Input/Output; Shuffle Read/Write; Spill (Memory/Disk); **Summary Metrics for tasks** (min/25th/median/75th/max) |
| Stage detail → Tasks | Individual slow tasks, their host, shuffle read size, GC time, errors |
| SQL / DataFrame | Query DAG with operator metrics: rows output, time, data size, number of files read/pruned, broadcast sizes; AQE final plan |
| Storage | Cached RDDs/DataFrames, fraction cached, memory vs disk |
| Executors | Active/dead executors, memory used, GC time, task failures, shuffle totals, thread dumps |
| Environment | Effective Spark configs (verify AQE, shuffle partitions, etc.) |
| Structured Streaming | Input rate vs processing rate, batch duration, state size |

**Diagnostic patterns**
1. **Skew:** the max task time and shuffle read are far above the median (see [Q263](/interview/pyspark/#q263)).
2. **Spill:** non-zero spill disk or memory, so tasks are too large for memory (see [Q271](/interview/pyspark/#q271)).
3. **Too many tiny tasks:** thousands of tasks with median duration under 100 ms (see [Q260](/interview/pyspark/#q260)).
4. **Too few tasks:** fewer tasks than cores, so the cluster is underutilised.
5. **Scan heavy:** large input, with "files pruned" small. Check partitioning, clustering and filters (see [Q277](/interview/pyspark/#q277)).
6. **GC:** GC time above about 10% of task time (see [Q273](/interview/pyspark/#q273)).
7. **Failed or retried tasks:** fetch failures (lost shuffle files, executor loss), OOM.
8. **Streaming lag:** processing rate below input rate, with growing batch duration (see [Q66](/interview/data-engineering/#q66)).

**History server:** the Spark History Server, or the platform's equivalent, keeps UIs for finished applications. Enable event logging (`spark.eventLog.enabled`) for post-mortems.

**Example**

```python
# Runtime: PySpark 3.5 — label jobs so they're easy to find in the UI
sc = spark.sparkContext
sc.setJobGroup("daily_orders", "Build gold.daily_orders")
sc.setJobDescription("Aggregate paid orders by day and region")

(spark.read.table("silver.orders")
     .filter("status = 'paid'")
     .groupBy("order_date", "region").sum("amount")
     .write.mode("overwrite").saveAsTable("gold.daily_orders"))

print(sc.uiWebUrl)   # link to the live Spark UI (where accessible)
```

**Real-world use case**

A job's Stages tab showed a stage with median task time of 4 seconds and a max of 38 minutes, with 30 GB of shuffle read on that one task. That identified a skewed join key within minutes, avoiding a week of guessing at cluster settings.

**Common mistakes and red flags**

- Only looking at total job duration.
- Ignoring the SQL tab's operator metrics.
- Not checking effective configs in the Environment tab.

**Follow-up questions**

- What is a fetch failure? — A reduce task couldn't fetch shuffle blocks (executor lost/disk issue); Spark recomputes the map stage.
- How do you analyse a job that already finished? — History server with event logs, or platform job run UI.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q276" data-level="Intermediate">
<summary><span class="qb-num">Q276</span><h3 class="qb-title">Using explain() to read physical plans</h3><span class="qb-meta">Intermediate · Common · Optimisation</span></summary>

**Short answer**

`df.explain(mode)` prints the query plan without running it. Modes are `simple` (physical only), `extended` (parsed, analysed and optimised logical plans plus physical), `formatted` (an outline with node details), `cost` and `codegen`. Read the plan bottom-up from the scans. Check pushed filters and read columns, `Exchange` nodes (shuffles), join types, broadcasts and Python UDF nodes.

**Detailed explanation**

**What to check, in order**
1. **Scan nodes** (`FileScan parquet` / `Scan delta`):
   - `PushedFilters: [IsNotNull(order_date), GreaterThanOrEqual(order_date, 2026-03-01)]` shows filter pushdown worked.
   - `PartitionFilters: [...]` shows partition pruning (see [Q277](/interview/pyspark/#q277)).
   - `ReadSchema` shows only the needed columns (column pruning).
2. **Exchange nodes:**
   - `Exchange hashpartitioning(customer_id, 200)` is a shuffle by key.
   - `BroadcastExchange` builds a broadcast for a join.
   - `Exchange rangepartitioning` is a shuffle for a global sort.
   - Count them, because each is a shuffle (see [Q256](/interview/pyspark/#q256)).
3. **Joins:** `BroadcastHashJoin`, `SortMergeJoin`, `ShuffledHashJoin`, `BroadcastNestedLoopJoin` (slow), `CartesianProduct` (danger) (see [Q261](/interview/pyspark/#q261)).
4. **Aggregations:** `HashAggregate (partial)` → `Exchange` → `HashAggregate (final)` is the normal two-phase aggregation. `SortAggregate` can appear for some types.
5. **Python:** `BatchEvalPython` (row UDF), `ArrowEvalPython` (pandas UDF), `FlatMapGroupsInPandas` (see [Q270](/interview/pyspark/#q270)).
6. **Codegen:** `*(n)` prefixes show whole-stage codegen groups (see [Q269](/interview/pyspark/#q269)).
7. **AQE:** `AdaptiveSparkPlan isFinalPlan=false` before execution. After execution, the SQL tab shows the final plan (see [Q265](/interview/pyspark/#q265)).

**SQL equivalent:** `EXPLAIN [EXTENDED | FORMATTED | COST | CODEGEN] <query>`.

**Habit worth building:** run `explain()` before shipping any non-trivial transformation, and again after tuning changes. Comparing the two plans (number of Exchanges, join types, pushed filters) confirms the change did what you intended, before you spend cluster time measuring it.

**Example**

```python
# Runtime: PySpark 3.5
from pyspark.sql import functions as F

q = (spark.read.table("silver.orders")
     .filter(F.col("order_date") >= "2026-03-01")
     .join(F.broadcast(spark.read.table("silver.stores")), "store_id")
     .groupBy("region").agg(F.sum("amount").alias("revenue")))

q.explain(mode="formatted")
```
Abbreviated expected shape:
```text
AdaptiveSparkPlan
+- HashAggregate(keys=[region], functions=[sum(amount)])            <- final agg
   +- Exchange hashpartitioning(region, 200)                         <- shuffle
      +- HashAggregate(keys=[region], functions=[partial_sum(amount)])
         +- Project [amount, region]
            +- BroadcastHashJoin [store_id], [store_id], Inner, BuildRight
               :- Filter (isnotnull(order_date) AND (order_date >= 2026-03-01))
               :  +- FileScan ... PushedFilters: [IsNotNull(order_date), GreaterThanOrEqual(order_date,2026-03-01)]
               +- BroadcastExchange HashedRelationBroadcastMode
                  +- FileScan ... silver.stores [store_id, region]
```

**Real-world use case**

A query was slow even though it filtered on `event_date`. `explain()` showed `PartitionFilters: []` because the filter compared `to_date(event_ts)` instead of the partition column `event_date`. Rewriting the filter enabled partition pruning.

**Common mistakes and red flags**

- Not knowing what `Exchange` means.
- Reading only the top of the plan.
- Ignoring PushedFilters and ReadSchema.

**Follow-up questions**

- Does explain execute the query? — No (except that AQE final plans only appear after execution).
- What is `BuildRight`? — The right side is the hash-table build side in a hash join.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q277" data-level="Intermediate">
<summary><span class="qb-num">Q277</span><h3 class="qb-title">Predicate pushdown and partition pruning</h3><span class="qb-meta">Intermediate · Very Common · Optimisation</span></summary>

**Short answer**

Partition pruning skips whole directories, or partitions in a table format, when filters match the partition columns. Predicate pushdown passes filters to the data source, so Parquet, ORC, Delta and Iceberg readers can skip row groups or files using min/max statistics, and JDBC sources filter in the database. Both only work when filters are on raw columns in a pushable form, and they cut I/O dramatically.

**Detailed explanation**

**Partition pruning**
- With `event_date=2026-03-01/` directories, `WHERE event_date = '2026-03-01'` reads one folder.
- Delta and Iceberg prune using partition values in metadata. Iceberg's **hidden partitioning** prunes on `event_ts` filters through transforms (see [Q55](/interview/data-engineering/#q55)).
- **Dynamic partition pruning** prunes at runtime based on a join's filtered dimension (see [Q278](/interview/pyspark/#q278)).

**Predicate pushdown**
- **File formats:** filters become source filters (`PushedFilters` in the plan). Readers skip row groups (Parquet) or stripes (ORC) whose statistics don't match (see [Q53](/interview/data-engineering/#q53)). Delta uses per-file stats in the transaction log for **data skipping** before opening files.
- **JDBC:** WHERE clauses are sent to the database. Aggregate and limit pushdown are available in newer versions for some sources.
- **Kafka:** limited (offset ranges).

**What breaks it**
| Pattern | Problem | Fix |
|---|---|---|
| `F.to_date("event_ts") == "2026-03-01"` when partitioned by `event_date` | Filter not on partition column | Filter `event_date` directly or use generated columns |
| `F.year("order_date") == 2026` | Function on column, not pushable as range | `order_date >= '2026-01-01' AND < '2027-01-01'` |
| Filter after a Python UDF | UDF blocks pushdown | Filter before UDF |
| Casting the column (`col.cast("string") == "42"`) | Type mismatch | Compare with correct type |
| Unsorted data | Stats ranges overlap → little skipping | Cluster/Z-order/sort (see [Q208](/interview/databricks/#q208), [Q209](/interview/databricks/#q209)) |
| Stats not collected on that column (Delta first 32 columns) | No skipping | Reorder columns or configure stats columns |

**Verifying:** look in `explain()` for `PartitionFilters` and `PushedFilters` (see [Q276](/interview/pyspark/#q276)). In the SQL tab or query profile, compare files and bytes read with and without the filter.

**Example**

```python
# Runtime: PySpark 3.5
from pyspark.sql import functions as F

events = spark.read.table("silver.events")      # partitioned by event_date (DATE)

bad = events.filter(F.to_date("event_ts") == F.lit("2026-03-01"))       # scans all partitions
good = events.filter(F.col("event_date") == F.lit("2026-03-01").cast("date"))

bad.explain()    # PartitionFilters: []
good.explain()   # PartitionFilters: [isnotnull(event_date), (event_date = 2026-03-01)]
```

**Real-world use case**

A dashboard query on a 30 TB events table took 6 minutes because analysts filtered on `event_ts` while the table was partitioned by `event_date`. Adding an `event_date` filter alongside (or switching to Iceberg hidden partitioning) cut the scan to one day, about 30 GB.

**Common mistakes and red flags**

- Assuming any filter prunes partitions.
- Wrapping columns in functions.
- Not knowing about Delta's data skipping stats limit.

**Follow-up questions**

- Does pushdown work for nested fields? — Yes for many formats/versions (nested schema pruning and filter pushdown), check engine support.
- What is aggregate pushdown? — Pushing MIN/MAX/COUNT to sources that can answer from metadata (Parquet stats, JDBC).

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q278" data-level="Advanced">
<summary><span class="qb-num">Q278</span><h3 class="qb-title">Dynamic partition pruning</h3><span class="qb-meta">Advanced · Occasional · Optimisation</span></summary>

**Short answer**

Dynamic partition pruning (DPP, Spark 3.0+) prunes partitions of a large partitioned fact table at runtime, using the filter results from the other side of a join. A typical case is a fact partitioned by `date_key` joined to a date dimension filtered to "last week". Spark computes the qualifying keys from the dimension, often reusing a broadcast, and injects them as a partition filter into the fact scan.

**Detailed explanation**

**The problem**
```text
SELECT ... FROM fact_sales f JOIN dim_date d ON f.date_key = d.date_key
WHERE d.fiscal_week = '2026-W10'
```
The filter is on the **dimension**, not the fact's partition column. Static planning can't know which `date_key` values qualify, so without DPP it scans every fact partition.

**How DPP works**
1. Spark sees that the fact is partitioned on the join key and that the other side has a selective filter.
2. It builds a subquery (or reuses the broadcast of the filtered dimension) that collects the distinct join keys.
3. It adds a **dynamic pruning filter** to the fact scan: `date_key IN (keys from dim)`.
4. Only matching partitions are read.

**Conditions**
- `spark.sql.optimizer.dynamicPartitionPruning.enabled = true` (default).
- The fact table is partitioned on the join key (directory partitions or table-format partitions).
- An equi-join, with a selective filter on the other side.
- Most effective when the dimension side is broadcast, because the broadcast result is reused (`reuseBroadcastOnly` default true).

**In plans:** look for `dynamicpruningexpression(...)` in the fact's `PartitionFilters`, plus a `SubqueryBroadcast` node.

**Related:** Databricks and Delta also apply **dynamic file pruning** (DFP), which skips *files* (not just partitions) using join-key statistics. That helps clustered, unpartitioned tables too.

**Example**

```python
# Runtime: PySpark 3.5
from pyspark.sql import functions as F

fact = spark.read.table("gold.fact_sales")       # partitioned by date_key
dim_date = spark.read.table("gold.dim_date")

week_sales = (fact.join(dim_date.filter(F.col("fiscal_week") == "2026-W10"), "date_key")
                   .groupBy("store_id").agg(F.sum("net_amount").alias("revenue")))

week_sales.explain()
# Expect in fact scan: PartitionFilters: [isnotnull(date_key), dynamicpruningexpression(date_key IN dynamicpruning#...)]
```

**Real-world use case**

Weekly reports joining a 5-year sales fact to the date dimension scanned all 1,800 date partitions on Spark 2.4. After upgrading to Spark 3 with DPP, they read 7 partitions, and report time dropped from 12 minutes to 20 seconds.

**Common mistakes and red flags**

- Not partitioning the fact on the join key and expecting DPP.
- Applying functions to the join key, which blocks pruning.
- Confusing DPP with static partition pruning.

**Follow-up questions**

- Does DPP work without broadcast? — It can run a separate subquery if enabled, but by default favours reusing broadcasts.
- How does this relate to Snowflake/warehouses? — Similar runtime pruning ideas (join filters, bloom filters) exist in many MPP engines.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q279" data-level="Intermediate">
<summary><span class="qb-num">Q279</span><h3 class="qb-title">partitionBy vs bucketBy when writing</h3><span class="qb-meta">Intermediate · Common · Data Layout</span></summary>

**Short answer**

`write.partitionBy("col")` creates a directory per value (`col=value/`), which enables partition pruning on filters. Use it for low-cardinality columns such as dates. `bucketBy(n, "col")` hash-distributes rows into a fixed number of files per partition, and requires `saveAsTable` with metastore tables. Joins and aggregations on the bucket key between tables bucketed the same way can then skip shuffles. Bucketing is less used with Delta, where liquid clustering is preferred.

**Detailed explanation**

| Aspect | `partitionBy` | `bucketBy` |
|---|---|---|
| Physical layout | Directories per distinct value | Fixed N files per (partition) by hash of key |
| Cardinality fit | Low (dates, regions) | High (customer_id, user_id) |
| Main benefit | Partition pruning on filters | Shuffle-free joins/aggregations on bucket key (both sides bucketed compatibly) |
| Requirements | Any file-based write | `saveAsTable` (Hive-compatible metastore); not supported for Delta tables |
| Risk | Over-partitioning → small files (see [Q56](/interview/data-engineering/#q56)) | Wrong bucket count, mismatched bucketing across tables, files per bucket per task |
| Sorting | — | Optional `sortBy` within buckets |

**partitionBy tips**
- Partition by the column used in most filters, with a moderate number of values (hundreds to low thousands).
- Repartition by the same columns before writing, to avoid many files per partition: `df.repartition("event_date").write.partitionBy("event_date")`.
- For Delta, use **dynamic partition overwrite** or `replaceWhere` to overwrite specific partitions safely (see [Q280](/interview/pyspark/#q280)).

**bucketBy tips**
- Both join sides must be bucketed on the join key with the same (or compatible) bucket counts to avoid shuffles. Spark 3 can join tables with bucket counts that are multiples of each other with configuration (`spark.sql.bucketing.coalesceBucketsInJoin.enabled`).
- Writing with many tasks creates up to tasks × buckets files. Repartition to the bucket count first.
- Bucketing is mostly used with Hive/Parquet tables in metastore-managed environments. For Delta and Iceberg, prefer liquid clustering (Delta; see [Q209](/interview/databricks/#q209)) or Iceberg `bucket(N, col)` partition transforms (see [Q55](/interview/data-engineering/#q55)).

**Example**

```python
# Runtime: PySpark 3.5 (Hive-compatible metastore tables for bucketing)
from pyspark.sql import functions as F

events = spark.read.table("bronze.events")

# Directory partitioning by date, ~8 files per day
(events.repartition(8, "event_date")
       .write.mode("overwrite")
       .partitionBy("event_date")
       .format("parquet")
       .saveAsTable("silver.events_by_date"))

# Bucketing two large tables on customer_id for shuffle-free joins later
(spark.read.table("bronze.orders").repartition(64, "customer_id")
      .write.mode("overwrite").bucketBy(64, "customer_id").sortBy("customer_id")
      .format("parquet").saveAsTable("silver.orders_bucketed"))
(spark.read.table("bronze.customers").repartition(64, "customer_id")
      .write.mode("overwrite").bucketBy(64, "customer_id").sortBy("customer_id")
      .format("parquet").saveAsTable("silver.customers_bucketed"))

joined = spark.table("silver.orders_bucketed").join(spark.table("silver.customers_bucketed"), "customer_id")
joined.explain()   # expect SortMergeJoin without Exchange on either side
```

**Real-world use case**

On an on-prem Hive/Spark platform, two 3 TB tables were joined nightly on `account_id`. Bucketing both by `account_id` into 512 buckets removed a 6 TB shuffle and cut the job from 90 to 30 minutes.

**Common mistakes and red flags**

- `partitionBy` on high-cardinality IDs.
- Bucketing only one side.
- Trying `bucketBy` with Delta tables.

**Follow-up questions**

- Why repartition before partitionBy? — Each write task writes one file per partition it holds; repartitioning by the partition column limits files per partition.
- What is the modern alternative to bucketing in lakehouses? — Liquid clustering (Delta) or bucket transforms (Iceberg).

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q280" data-level="Intermediate">
<summary><span class="qb-num">Q280</span><h3 class="qb-title">Save modes and dynamic partition overwrite</h3><span class="qb-meta">Intermediate · Common · Writes</span></summary>

**Short answer**

Save modes control writes to existing data. `append` adds rows, `overwrite` replaces data, `errorifexists` (the default) fails if the target exists, and `ignore` skips silently. With `partitionOverwriteMode = dynamic`, an overwrite replaces only the partitions present in the DataFrame, not the whole table. On Delta, `replaceWhere` overwrites exactly the rows matching a predicate. These are the tools for idempotent partition reloads.

**Detailed explanation**

| Mode | Behaviour | Risk |
|---|---|---|
| `append` | Add rows | Duplicates on rerun (not idempotent) |
| `overwrite` (static, default) | Replace the **entire** table/path | Wipes other partitions if you meant to replace one |
| `overwrite` + dynamic partition overwrite | Replace only partitions that appear in the incoming data | Missing partitions in input are left untouched (intended) |
| `errorifexists` / `error` | Fail if exists | — |
| `ignore` | Do nothing if exists | Silent skips |

**Dynamic partition overwrite**
- `spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")`, or the per-write option `.option("partitionOverwriteMode", "dynamic")`.
- Works for partitioned file tables. Delta also supports it (recent Delta and DBR versions).
- This is idempotent per partition: rerunning a day replaces that day only (see [Q61](/interview/data-engineering/#q61)).

**Delta `replaceWhere`**
- `.option("replaceWhere", "event_date >= '2026-03-01' AND event_date < '2026-03-08'")` overwrites exactly the rows matching the predicate, atomically. Delta validates that the written data satisfies the predicate (`spark.databricks.delta.replaceWhere.constraintCheck.enabled`).
- It works on any columns (not just partitions) in newer Delta versions.
- It is more explicit and safer than dynamic mode, because the replaced range is declared even if the input happens to be missing a day.

**SQL equivalents**
- `INSERT OVERWRITE TABLE t PARTITION (event_date = '2026-03-01') SELECT ...` (static partition).
- `INSERT OVERWRITE t SELECT ...` with dynamic mode enabled.
- Delta: `INSERT INTO t REPLACE WHERE event_date = '2026-03-01' SELECT ...`.

**Example**

```python
# Runtime: PySpark 3.5 with Delta Lake
from pyspark.sql import functions as F

day = "2026-03-01"
daily = (spark.read.table("silver.orders")
         .filter(F.col("order_date") == day)
         .groupBy("order_date", "store_id").agg(F.sum("amount").alias("revenue")))

# Delta: atomically replace exactly that day's rows (safe to rerun)
(daily.write.format("delta")
      .mode("overwrite")
      .option("replaceWhere", f"order_date = '{day}'")
      .saveAsTable("gold.daily_store_revenue"))

# Parquet/partitioned tables: dynamic partition overwrite
(daily.write.mode("overwrite")
      .option("partitionOverwriteMode", "dynamic")
      .partitionBy("order_date")
      .parquet("s3://lake/gold/daily_store_revenue_parquet/"))
```
`day` is set by the orchestrator (for example from a run parameter) and validated as a date before use, not taken from user input.

**Real-world use case**

A backfill job meant to rewrite one day used plain `mode("overwrite")` on a partitioned Parquet table, and wiped three years of data. Recovery took a day of reprocessing. The fix was `replaceWhere` on Delta (and dynamic partition overwrite elsewhere) plus a code-review checklist item.

**Common mistakes and red flags**

- Static overwrite on partitioned tables for single-partition reloads.
- `append` in rerunnable jobs.
- `replaceWhere` predicates that don't match the written data.

**Follow-up questions**

- What does `overwriteSchema` do? — Allows replacing the table schema during overwrite (see [Q212](/interview/databricks/#q212)).
- Is overwrite atomic on Delta? — Yes; readers see old or new version, never partial.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q281" data-level="Beginner">
<summary><span class="qb-num">Q281</span><h3 class="qb-title">Handling nulls in PySpark</h3><span class="qb-meta">Beginner · Common · Data Cleaning</span></summary>

**Short answer**

Detect nulls with `isNull()` / `isNotNull()`, not `== None`. Replace them with `fillna` / `na.fill` (per column), `F.coalesce(col, default)`, or `when/otherwise`. Drop rows with `dropna` / `na.drop` (`how`, `thresh`, `subset`). Use null-safe equality `eqNullSafe` (`<=>` in SQL) when NULL should match NULL. Remember that NULLs never match in normal joins and are ignored by most aggregates.

**Detailed explanation**

**Key behaviours (three-valued logic; see [Q107](/interview/sql/#q107))**
- `F.col("x") == None` yields NULL, not True, so filters drop every row. Use `F.col("x").isNull()`.
- Comparisons with NULL give NULL, and `filter` keeps only True.
- Aggregates (`sum`, `avg`, `count(col)`) ignore NULLs. `count("*")` counts rows.
- Joins on `=` never match NULL keys (see [Q263](/interview/pyspark/#q263) for skew effects).
- `groupBy` puts all NULLs into one group.
- Sorting: `asc_nulls_first` / `asc_nulls_last` / `desc_nulls_*` give explicit control.

**Tools**
| Need | API |
|---|---|
| Find nulls per column | `df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns])` |
| Replace with constants | `df.fillna({"country": "UNKNOWN", "discount": 0})` |
| Replace with another column | `F.coalesce("ship_date", "order_date")` |
| Conditional | `F.when(F.col("x").isNull(), ...).otherwise(...)` |
| Drop rows | `df.dropna(subset=["order_id"])`, `df.dropna(how="all")`, `df.dropna(thresh=3)` |
| Null-safe equality | `df1.join(df2, df1.k.eqNullSafe(df2.k))` |
| Empty strings → NULL | `F.when(F.trim("x") == "", None).otherwise(F.col("x"))` or `F.nullif` (Spark 3.5+) |
| NaN (floats) | `F.isnan`, `df.na.fill` handles NaN for numeric; `nanvl` |

**Notes**
- `fillna` with a dict only applies to columns whose type matches the value's type. A string default won't fill integer columns.
- NaN isn't NULL for floats. Handle both in numeric data from pandas or CSV sources.
- Decide on null semantics per business column, because "unknown" and "not applicable" are different. Document them in contracts (see [Q38](/interview/data-engineering/#q38)).

**Example**

```python
# Runtime: PySpark 3.5
from pyspark.sql import functions as F

customers = spark.createDataFrame(
    [(1, "IN", None, " "), (2, None, 5.0, "a@x.com"), (3, "US", float("nan"), None)],
    "customer_id INT, country STRING, discount DOUBLE, email STRING")

null_counts = customers.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in customers.columns])
null_counts.show()

clean = (customers
         .withColumn("email", F.when(F.trim("email") == "", None).otherwise(F.col("email")))
         .withColumn("discount", F.nanvl("discount", F.lit(None).cast("double")))
         .fillna({"country": "UNKNOWN", "discount": 0.0}))
clean.show()
```
Result: customer 1 has email NULL and discount 0.0. Customer 2 has country UNKNOWN. Customer 3 has its NaN discount turned into 0.0.

**Real-world use case**

A churn report undercounted active customers because `filter(F.col("cancel_date") == None)` returned nothing. Switching to `isNull()` fixed the count, and a lint rule now flags `== None` in PySpark code.

**Common mistakes and red flags**

- `== None` comparisons.
- Ignoring NaN vs NULL.
- Filling NULLs with 0 when 0 is a valid, different value.

**Follow-up questions**

- How do NULLs affect `countDistinct`? — They're excluded.
- How do you join where NULL should match NULL? — `eqNullSafe` / `<=>`.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q282" data-level="Intermediate">
<summary><span class="qb-num">Q282</span><h3 class="qb-title">Window functions in PySpark</h3><span class="qb-meta">Intermediate · Very Common · Window Functions</span></summary>

**Short answer**

Define a window with `Window.partitionBy(...).orderBy(...)` and optionally a frame (`rowsBetween` / `rangeBetween`). Then apply functions with `.over(w)`: ranking (`row_number`, `rank`, `dense_rank`, `ntile`, `percent_rank`), offsets (`lag`, `lead`), and aggregates (`sum`, `avg`, `count`, `max`, `first`, `last`). Each window's partition is shuffled to one task, so partition keys with huge groups cause skew and memory pressure.

**Detailed explanation**

**Building blocks**
- `from pyspark.sql.window import Window`
- `w = Window.partitionBy("customer_id").orderBy("order_ts")`
- Frames:
  - `rowsBetween(Window.unboundedPreceding, Window.currentRow)` for running totals (row-based).
  - `rangeBetween(-6, 0)` over a numeric order column for value-based ranges. Order by days-since-epoch for date ranges.
- Default frame when `orderBy` is present: `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` (peers with equal order values are included). Specify `rowsBetween` for exact row semantics (see [Q111](/interview/sql/#q111)).

**Common patterns**
| Task | Code |
|---|---|
| Latest record per key | `row_number().over(w_desc) == 1` (see [Q295](/interview/pyspark/#q295)) |
| Previous value/delta | `F.lag("amount").over(w)` |
| Running total | `F.sum("amount").over(w.rowsBetween(Window.unboundedPreceding, 0))` |
| 7-row moving average | `F.avg("x").over(w.rowsBetween(-6, 0))` |
| Rank within group | `F.dense_rank().over(Window.partitionBy("cat").orderBy(F.desc("sales")))` |
| Share of group total | `F.col("x") / F.sum("x").over(Window.partitionBy("grp"))` |
| Sessionization | `lag` + flag + cumulative sum (see [Q112](/interview/sql/#q112)) |

**Performance**
- A window with `partitionBy` triggers a shuffle by the partition keys, plus a sort within partitions.
- **No `partitionBy`** moves all data to one partition, a single task. Spark warns about it. Avoid it for big data.
- Huge partitions (hot keys) can OOM or spill. Pre-aggregate, or narrow the partition key.
- Several windows with the **same** partitionBy/orderBy share one shuffle and sort. Different specs add more.

**Example**

```python
# Runtime: PySpark 3.5
from pyspark.sql import functions as F
from pyspark.sql.window import Window

orders = spark.read.table("silver.orders")

w = Window.partitionBy("customer_id").orderBy("order_ts")
w_run = w.rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (orders
    .withColumn("order_seq", F.row_number().over(w))
    .withColumn("prev_amount", F.lag("amount").over(w))
    .withColumn("days_since_prev",
                F.datediff("order_ts", F.lag("order_ts").over(w)))
    .withColumn("running_spend", F.sum("amount").over(w_run)))
```
Sample for one customer: orders of 100, 50 and 30 give `order_seq` 1, 2, 3; `prev_amount` NULL, 100, 50; and `running_spend` 100, 150, 180.

**Real-world use case**

A churn feature pipeline computed "days since previous order" and "orders in the last 90 days" with windows partitioned by `customer_id`. Replacing a self-join approach cut runtime from 50 to 8 minutes.

**Common mistakes and red flags**

- Windows without `partitionBy` on large data.
- Relying on the default RANGE frame when there are ties.
- Using `rank` when exactly one row per group is needed (use `row_number`).

**Follow-up questions**

- `rowsBetween` vs `rangeBetween`? — Row offsets vs value ranges on the order column.
- How do you dedupe deterministically? — `row_number` with a full tiebreaker in orderBy.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q283" data-level="Intermediate">
<summary><span class="qb-num">Q283</span><h3 class="qb-title">explode, flattening nested JSON and arrays</h3><span class="qb-meta">Intermediate · Very Common · Semi-Structured Data</span></summary>

**Short answer**

Parse JSON strings with `from_json(col, schema)`, or read JSON files with an explicit schema. Access struct fields with dot notation (`col("payload.customer.id")`) or `select("struct.*")`. Turn arrays into rows with `explode` (drops empty and NULL arrays), `explode_outer` (keeps them), or `posexplode` (adds the position). For arrays of structs, explode first, then select the struct fields.

**Detailed explanation**

**Toolkit**
| Function | Purpose |
|---|---|
| `from_json(col, schema)` | String → struct/array/map |
| `to_json(col)` | Struct → JSON string |
| `get_json_object(col, '$.a.b')` / `json_tuple` | Extract fields from JSON strings without full schema (slower, string results) |
| `schema_of_json(sample)` | Infer a schema from a sample string |
| `col("a.b")`, `select("a.*")` | Struct field access / flatten one level |
| `explode` / `explode_outer` / `posexplode(_outer)` | Arrays/maps → rows |
| `inline` / `inline_outer` | Array of structs → rows with struct columns flattened |
| `transform`, `filter`, `aggregate`, `exists` | Higher-order functions on arrays without exploding |
| `VARIANT` type + `parse_json` (Spark 4.0+) | Semi-structured column with efficient storage/access (see [Q298](/interview/pyspark/#q298)) |

**Design considerations**
- **Explode multiplies rows.** Exploding two independent arrays in sequence creates a Cartesian product per row. Explode one, or use `arrays_zip` and then explode.
- **Empty arrays disappear with `explode`.** Use `explode_outer` if the parent row must survive (for example orders with no items).
- **Schema drift:** an explicit schema plus `_rescued_data` or a corrupt-record column (`columnNameOfCorruptRecord`) handles unexpected shapes (see [Q225](/interview/databricks/#q225)).
- **Prefer higher-order functions** for per-element transforms when you don't need rows, which avoids the explosion cost.
- **Model output:** usually a parent table (one row per order) and a child table (one row per item), keyed by the parent ID (see [Q133](/interview/sql/#q133) for the SQL view).

**Example**

```python
# Runtime: PySpark 3.5
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType, DoubleType

raw = spark.createDataFrame([
    ('{"order_id":"o1","customer":{"id":"c9","tier":"gold"},'
     '"items":[{"sku":"A","qty":2,"price":5.0},{"sku":"B","qty":1,"price":12.5}]}',),
    ('{"order_id":"o2","customer":{"id":"c3","tier":"basic"},"items":[]}',)
], "payload STRING")

schema = StructType([
    StructField("order_id", StringType()),
    StructField("customer", StructType([StructField("id", StringType()),
                                        StructField("tier", StringType())])),
    StructField("items", ArrayType(StructType([StructField("sku", StringType()),
                                               StructField("qty", IntegerType()),
                                               StructField("price", DoubleType())]))),
])

orders = raw.select(F.from_json("payload", schema).alias("o")).select("o.*")

order_items = (orders
    .select("order_id", F.col("customer.id").alias("customer_id"),
            F.explode_outer("items").alias("item"))
    .select("order_id", "customer_id", "item.sku", "item.qty", "item.price",
            (F.col("item.qty") * F.col("item.price")).alias("line_total")))
order_items.show()
```
Result: o1 produces 2 rows (A, line total 10.0; B, line total 12.5). o2 produces 1 row with NULL item fields, because of `explode_outer`.

**Real-world use case**

An e-commerce event stream nested cart items inside each checkout event. The silver layer produces `checkout_events` (one row per event) and `checkout_items` (exploded). That made basket analysis a simple SQL join instead of repeated JSON parsing.

**Common mistakes and red flags**

- Using `explode` and losing parents with empty arrays.
- Exploding multiple arrays sequentially, creating an accidental Cartesian product.
- `get_json_object` everywhere instead of parsing once with a schema.

**Follow-up questions**

- How do you flatten deeply nested schemas generically? — Recursively walk `df.schema`, selecting `struct.field` aliases, and explode arrays deliberately.
- What does `arrays_zip` do? — Combines arrays element-wise into an array of structs, so you can explode them together.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q284" data-level="Beginner">
<summary><span class="qb-num">Q284</span><h3 class="qb-title">Reading and writing with an explicit schema</h3><span class="qb-meta">Beginner · Common · I/O</span></summary>

**Short answer**

Provide a schema with `StructType` or a DDL string (`"id INT, name STRING"`) when reading CSV or JSON. That avoids a costly inference pass, guarantees types, and makes bad data visible instead of silently turning columns into strings. Combine it with a parse mode (`PERMISSIVE` with a corrupt-record column, `DROPMALFORMED`, `FAILFAST`) to control how malformed rows are handled.

**Detailed explanation**

**Why an explicit schema**
- **Performance:** `inferSchema=true` (CSV) or default JSON inference reads data an extra time, which is expensive on large inputs.
- **Correctness:** inference can pick the wrong types (IDs with leading zeros become ints, mixed formats become strings) or vary between files and days.
- **Contracts:** the schema documents what you expect (see [Q38](/interview/data-engineering/#q38)).

**Ways to define it**
- `StructType([StructField("order_id", LongType(), False), ...])`.
- DDL string: `"order_id BIGINT, amount DECIMAL(12,2), order_ts TIMESTAMP"`.
- Reuse an existing table's schema: `spark.table("silver.orders").schema`.

**Parse modes (CSV/JSON)**
| Mode | Malformed rows |
|---|---|
| `PERMISSIVE` (default) | Keep row; fields NULL; raw text in `columnNameOfCorruptRecord` (must be in schema as STRING) |
| `DROPMALFORMED` | Drop silently (risky — data loss) |
| `FAILFAST` | Throw on first bad record |

**Useful CSV options:** `header`, `sep`, `quote`, `escape`, `multiLine`, `nullValue`, `emptyValue`, `dateFormat`, `timestampFormat`, `encoding`, `ignoreLeadingWhiteSpace`.

**Writing**
- Format choice: Parquet or Delta for analytics (see [Q52](/interview/data-engineering/#q52)), CSV or JSON only for interchange.
- Writes preserve the DataFrame schema. Cast explicitly before writing to get the intended target types, such as decimals for money.
- Platform alternatives: Databricks `badRecordsPath`, or Auto Loader `_rescued_data` (see [Q225](/interview/databricks/#q225)).

**Example**

```python
# Runtime: PySpark 3.5
from pyspark.sql import functions as F

schema = """
  order_id BIGINT,
  customer_id STRING,
  amount DECIMAL(12,2),
  order_ts TIMESTAMP,
  _corrupt_record STRING
"""

orders = (spark.read
          .schema(schema)
          .option("header", "true")
          .option("mode", "PERMISSIVE")
          .option("columnNameOfCorruptRecord", "_corrupt_record")
          .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
          .csv("s3://lake/landing/orders/2026-03-01/"))

orders = orders.cache()        # required before filtering only on the corrupt-record column
bad = orders.filter(F.col("_corrupt_record").isNotNull())
good = orders.filter(F.col("_corrupt_record").isNull()).drop("_corrupt_record")

bad.write.mode("append").json("s3://lake/quarantine/orders/2026-03-01/")
good.write.mode("append").format("delta").saveAsTable("bronze.orders")
orders.unpersist()
```
Spark disallows queries that reference only the internal corrupt-record column on raw CSV/JSON scans. Caching (or otherwise materialising) the parsed result first is the documented workaround.

**Real-world use case**

A daily CSV load inferred `zip_code` as an integer, which dropped leading zeros for 8% of US addresses. An explicit schema with `zip_code STRING` and a quarantine path for malformed rows fixed both the data and visibility into bad rows.

**Common mistakes and red flags**

- `inferSchema=true` in production.
- `DROPMALFORMED` without monitoring.
- Storing money as DOUBLE.

**Follow-up questions**

- How do you enforce schema on JSON with extra fields? — Explicit schema ignores extras; use rescued data (Auto Loader) to capture them.
- How do you get the schema as DDL? — `df.schema.simpleString()` or `df.schema.json()`; `StructType.fromDDL` in newer versions.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q285" data-level="Intermediate">
<summary><span class="qb-num">Q285</span><h3 class="qb-title">groupByKey vs reduceByKey (RDD API)</h3><span class="qb-meta">Intermediate · Common · RDD API</span></summary>

**Short answer**

`reduceByKey(func)` combines values per key on the map side before shuffling, like a combiner, and then again after the shuffle. Only partial results cross the network. `groupByKey()` shuffles every value for each key, then groups them, which uses more network and memory and risks OOM on hot keys. Prefer `reduceByKey`, `aggregateByKey` or `combineByKey`, and in modern code use DataFrame `groupBy().agg()`, which does partial aggregation automatically.

**Detailed explanation**

| Aspect | `groupByKey` | `reduceByKey` | `aggregateByKey` / `combineByKey` |
|---|---|---|---|
| Map-side combine | No | Yes | Yes |
| Shuffle volume | All values | One partial value per key per partition | Partial accumulators |
| Output | `(key, Iterable[values])` | `(key, reduced value)` | `(key, accumulator)` of different type |
| Memory risk | High (all values per key in memory on reducer) | Low | Low–moderate |
| Function requirement | — | Associative & commutative | Seq + combine functions |

**Illustration**
- Word count with `reduceByKey(lambda a, b: a + b)`: each partition sends `(word, partial_count)` once per distinct word.
- With `groupByKey().mapValues(sum)`: each partition sends every `(word, 1)` pair, which can be millions of tiny records.

**When `groupByKey` is acceptable:** when you genuinely need all values per key (for example ordering events per session in custom logic) and groups are bounded in size. Even then, DataFrame windows or `applyInPandas` are usually better.

**DataFrame equivalent**
- `df.groupBy("key").agg(F.sum("v"))` already does `partial_sum` → shuffle → `final sum` (see the plan in Q276).
- `collect_list` in a DataFrame is the "groupByKey" equivalent, with the same memory risks on hot keys (see [Q272](/interview/pyspark/#q272)).

**Interview angle:** this classic question tests whether you understand **map-side combining and shuffle volume**, which still applies to DataFrame aggregations and to distributed systems generally (MapReduce combiners).

**Example**

```python
# Runtime: PySpark 3.5 (RDD API)
pairs = spark.sparkContext.parallelize(
    [("IN", 120.0), ("US", 80.0), ("IN", 30.0), ("UK", 15.0), ("US", 20.0)], numSlices=2)

revenue_reduce = pairs.reduceByKey(lambda a, b: a + b)              # map-side combine
revenue_group = pairs.groupByKey().mapValues(lambda vals: sum(vals))  # ships all values

# aggregateByKey: (sum, count) to compute average per key
zero = (0.0, 0)
avg_by_country = (pairs.aggregateByKey(zero,
                                       lambda acc, v: (acc[0] + v, acc[1] + 1),
                                       lambda a, b: (a[0] + b[0], a[1] + b[1]))
                       .mapValues(lambda s: s[0] / s[1]))
print(sorted(revenue_reduce.collect()), sorted(avg_by_country.collect()))
```
Result: IN 150.0, UK 15.0, US 100.0. Averages: IN 75.0, UK 15.0, US 50.0.

**Real-world use case**

A legacy RDD job used `groupByKey` to sum clicks per URL across 10 billion events, shuffling 1.2 TB. Switching to `reduceByKey` shuffled about 8 GB, and a later DataFrame rewrite simplified it further.

**Common mistakes and red flags**

- Using `groupByKey` for sums or counts.
- Non-associative functions in `reduceByKey`.
- Thinking DataFrame `groupBy` behaves like `groupByKey` (it doesn't).

**Follow-up questions**

- What is a combiner? — Map-side pre-aggregation reducing data before shuffle.
- Why must reduce functions be associative and commutative? — Partial results are combined in arbitrary order across partitions.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q286" data-level="Beginner">
<summary><span class="qb-num">Q286</span><h3 class="qb-title">Broadcast variables and accumulators</h3><span class="qb-meta">Beginner · Common · Shared Variables</span></summary>

**Short answer**

A broadcast variable sends a read-only value, such as a lookup dictionary or a small model, to each executor once, instead of with every task. Accumulators are write-only (from tasks) counters or sums that the driver reads, for metrics like "bad records seen". Accumulator updates inside transformations can be double-counted on task retries or recomputation. Only updates inside actions are guaranteed to be applied exactly once.

**Detailed explanation**

**Broadcast variables**
- `bc = spark.sparkContext.broadcast(lookup_dict)`, then `bc.value` inside UDFs or RDD functions.
- Sent once per executor and cached there, which avoids serialising a big object into every task closure.
- Use them for small reference data in UDFs, ML model weights, or configuration maps.
- Clean up with `bc.unpersist()` (executors) or `bc.destroy()` (everywhere).
- For **DataFrame joins**, use broadcast *joins* (`F.broadcast(df)`; see [Q262](/interview/pyspark/#q262)), not broadcast variables. Joins let Catalyst optimise.

**Accumulators**
- `acc = spark.sparkContext.accumulator(0)`, then `acc.add(1)` in tasks and `acc.value` on the driver after an action.
- Custom accumulators through `AccumulatorParam` (Python) or `AccumulatorV2` (Scala).
- **Guarantees:**
  - Updates in **actions** (`foreach`, `foreachPartition`) are applied once per task, even with retries.
  - Updates in **transformations** (`map`, inside UDFs) may be applied **multiple times** if tasks are retried, stages recomputed, or the DataFrame is evaluated by multiple actions. Treat them as approximate.
- They show up in the Spark UI for named accumulators (Scala). In PySpark they're mostly for driver-side reads.

**Better alternatives for metrics**
- Compute counts with DataFrame aggregations (`count` of bad rows) as part of the pipeline, which is exact and lineage-safe.
- Use the observable metrics API: `df.observe("metrics", F.count(...).alias(...))` (Spark 3.3+ in PySpark) gives exact metrics per action or streaming batch.

**Example**

```python
# Runtime: PySpark 3.5
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

country_names = {"IN": "India", "US": "United States", "UK": "United Kingdom"}
bc_names = spark.sparkContext.broadcast(country_names)

@F.udf(StringType())
def country_name(code):
    return bc_names.value.get(code, "Unknown")

orders = spark.read.table("silver.orders")
named = orders.withColumn("country_name", country_name("country_code"))

# Exact metric via observe (preferred over accumulators inside transformations)
observed = named.observe("quality", F.sum(F.when(F.col("country_name") == "Unknown", 1).otherwise(0))
                                      .alias("unknown_countries"))
observed.write.mode("overwrite").saveAsTable("gold.orders_named")
```
For a tiny map like this, a broadcast join with a small DataFrame, or `F.create_map` with literals, avoids the UDF entirely and is faster. The UDF is shown to illustrate broadcast variables.

**Real-world use case**

A job counted malformed rows with an accumulator in a `map`, and reported 2.3 times the true number because the DataFrame was evaluated by three actions. Replacing it with an explicit aggregate count made the metric exact.

**Common mistakes and red flags**

- Trusting accumulator values updated in transformations.
- Broadcasting huge objects.
- Using broadcast variables where a broadcast join would let the optimizer work.

**Follow-up questions**

- How do you read `df.observe` results? — Via a `QueryExecutionListener`/`Observation` object (`pyspark.sql.Observation`) after the action, or in streaming progress.
- What's the size limit for broadcast variables? — Practical memory limits per executor; keep them small (MBs to low hundreds of MBs).

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q287" data-level="Advanced">
<summary><span class="qb-num">Q287</span><h3 class="qb-title">Checkpointing vs caching (lineage truncation)</h3><span class="qb-meta">Advanced · Occasional · Fault Tolerance</span></summary>

**Short answer**

Caching keeps computed data in memory or local disk but keeps the full lineage. If a cached partition is lost, Spark recomputes it from the original plan. `checkpoint()` writes the data to reliable storage (`setCheckpointDir`) and truncates the lineage, so the plan restarts from the checkpoint files. `localCheckpoint()` truncates using executor storage, which is faster but not fault-tolerant. Use checkpoints to cut very long or iterative plans.

**Detailed explanation**

| Aspect | `cache()` / `persist()` | `checkpoint()` (reliable) | `localCheckpoint()` |
|---|---|---|---|
| Storage | Executor memory/disk | Reliable storage (HDFS/S3/ADLS/volumes) | Executor storage |
| Lineage | Kept | Truncated | Truncated |
| Survives executor loss | Recompute from lineage | Yes (re-read files) | No (job fails if lost) |
| Eager? | Lazy | Eager by default (`eager=True`) — runs a job | Eager by default |
| Cost | Memory | Write to storage (extra job) | Local write |
| Use | Reuse within job | Long/iterative lineage, very complex plans | Speed-focused lineage cut in stable clusters |

**Why lineage length matters**
- **Iterative algorithms** (graph processing, iterative joins in loops, or a loop that unions or updates a DataFrame 100 times) build huge logical plans. Catalyst analysis and optimisation time grows, plans can hit stack overflows, and recomputation after failure can go all the way back to the start.
- A checkpoint every N iterations keeps plans small and bounds recomputation.

**Alternatives**
- Write an intermediate Delta or Parquet table and read it back. This is explicit, inspectable, reusable across jobs, and often clearer in production pipelines.
- Restructure loops into set-based operations (one `unionByName` of a list, or a single join).

**Streaming checkpoints are different.** Structured Streaming checkpoints store offsets, commit logs and state for exactly-once recovery (see [Q291](/interview/pyspark/#q291)). The name is the same, but the purpose differs.

**Example**

```python
# Runtime: PySpark 3.5 — iterative label propagation with periodic lineage truncation
from pyspark.sql import functions as F

spark.sparkContext.setCheckpointDir("s3://lake/_spark_checkpoints/label_propagation/")

edges = spark.read.table("silver.account_links")              # src, dst
labels = spark.read.table("silver.accounts").select(F.col("account_id").alias("node"),
                                                     F.col("account_id").alias("label"))
for i in range(20):
    propagated = (edges.join(labels, edges.src == labels.node)
                       .groupBy(F.col("dst").alias("node"))
                       .agg(F.min("label").alias("new_label")))
    labels = (labels.join(propagated, "node", "left")
                    .select("node", F.least("label", F.coalesce("new_label", "label")).alias("label")))
    if i % 5 == 4:
        labels = labels.checkpoint()      # truncate lineage every 5 iterations
labels.write.mode("overwrite").saveAsTable("gold.account_clusters")
```

**Real-world use case**

A fraud-ring detection job ran 30 iterations of joins. By iteration 20, planning alone took 10 minutes and the driver hit stack overflows. Checkpointing every 5 iterations kept planning under a few seconds, and the job became reliable.

**Common mistakes and red flags**

- Confusing streaming checkpoints with DataFrame checkpoints.
- Not setting a checkpoint directory.
- Using `localCheckpoint` on spot-heavy clusters.

**Follow-up questions**

- Is checkpoint data cleaned up automatically? — Not by default; set `spark.cleaner.referenceTracking.cleanCheckpoints` or manage the directory.
- Should you cache before checkpoint? — Often yes for RDDs (avoids computing twice); DataFrame eager checkpoint computes once and writes.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q288" data-level="Intermediate">
<summary><span class="qb-num">Q288</span><h3 class="qb-title">Structured Streaming programming model</h3><span class="qb-meta">Intermediate · Very Common · Structured Streaming</span></summary>

**Short answer**

Structured Streaming treats a stream as an unbounded table that keeps growing. You write the same DataFrame or SQL query you would for batch, and Spark runs it incrementally, by default as micro-batches. Each trigger processes the new data, updates any state (aggregations, joins, deduplication), and writes results to a sink, with checkpoints providing fault tolerance and exactly-once sinks where supported.

**Detailed explanation**

**Core concepts**
- **Source:** Kafka, files or Auto Loader, Delta tables, Kinesis, the rate source (testing), sockets (testing).
- **Query:** normal DataFrame operations. Stateful operators keep state across batches: aggregations, `dropDuplicates`, stream-stream joins, and `transformWithState` (Spark 4.0) or `applyInPandasWithState`.
- **Trigger:** when to run the next batch (see [Q289](/interview/pyspark/#q289)).
- **Output mode:** append, update or complete (see [Q292](/interview/pyspark/#q292)).
- **Sink:** Delta or files, Kafka, `foreachBatch` (arbitrary batch logic; see [Q294](/interview/pyspark/#q294)), `foreach`, console or memory (testing).
- **Checkpoint:** offsets, commit log and state store, for recovery (see [Q291](/interview/pyspark/#q291)).

**Execution modes**
| Mode | Latency | Guarantees | Notes |
|---|---|---|---|
| Micro-batch (default) | ~100 ms to minutes | Exactly-once with replayable sources + idempotent sinks | Most common |
| Continuous processing (experimental) | ~1 ms | At-least-once | Limited operators |
| Real-time mode (newer, platform-specific; e.g. Databricks) | Sub-second | Varies | Check availability |

**Event time and state**
- Group by event-time windows (`window("event_ts", "10 minutes")`).
- **Watermarks** bound how late data can be and when state can be dropped (see [Q290](/interview/pyspark/#q290)). Without them, state grows forever for aggregations or deduplication on unbounded keys.

**Operational aspects**
- Monitor with `query.lastProgress` / `recentProgress` (input rows per second, processing rate, batch duration, state rows), or a `StreamingQueryListener`.
- Back-pressure and rate limits come through source options (`maxOffsetsPerTrigger` for Kafka, `maxFilesPerTrigger` for files) (see [Q66](/interview/data-engineering/#q66)).
- Changing a query may break checkpoint compatibility (state schema, operators). Plan upgrades.

**Example**

```python
# Runtime: PySpark 3.5 — Kafka -> parsed events -> 5-minute counts per page into Delta
from pyspark.sql import functions as F

events = (spark.readStream.format("kafka")
          .option("kafka.bootstrap.servers", "broker1:9092,broker2:9092")
          .option("subscribe", "page_views")
          .option("startingOffsets", "latest")
          .option("maxOffsetsPerTrigger", 500000)
          .load()
          .select(F.from_json(F.col("value").cast("string"),
                              "page_id STRING, user_id STRING, event_ts TIMESTAMP").alias("e"))
          .select("e.*"))

counts = (events.withWatermark("event_ts", "15 minutes")
          .groupBy(F.window("event_ts", "5 minutes"), "page_id")
          .count())

query = (counts.writeStream
         .outputMode("append")
         .format("delta")
         .option("checkpointLocation", "s3://lake/_checkpoints/page_view_counts/")
         .trigger(processingTime="1 minute")
         .toTable("gold.page_view_counts_5m"))
```

**Real-world use case**

A media site computes 5-minute page-view counts from Kafka into Delta, with a 15-minute watermark. Editorial dashboards read the Delta table with about 2 minutes of end-to-end latency, using the same code style as the batch backfill job.

**Common mistakes and red flags**

- Stateful aggregations without watermarks.
- No checkpoint location.
- Expecting complete-mode output for huge results.

**Follow-up questions**

- How does Structured Streaming differ from DStreams? — DStreams is the legacy RDD-based API (deprecated); Structured Streaming is DataFrame-based with event-time and exactly-once semantics.
- How do you test streaming code? — Rate/memory sources, `availableNow` triggers on sample files, and asserting sink contents.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q289" data-level="Intermediate">
<summary><span class="qb-num">Q289</span><h3 class="qb-title">Triggers: processingTime, availableNow, once, continuous</h3><span class="qb-meta">Intermediate · Common · Structured Streaming</span></summary>

**Short answer**

Triggers control when micro-batches run. The default runs the next batch as soon as the previous one finishes. `processingTime="1 minute"` runs at fixed intervals. `availableNow=True` processes all data available at start in multiple rate-limited batches, then stops. `once=True` (deprecated) did the same in a single batch. `continuous="1 second"` is experimental low-latency mode. Use `availableNow` for scheduled incremental jobs.

**Detailed explanation**

| Trigger | Behaviour | Typical use | Notes |
|---|---|---|---|
| Default (unspecified) | Next micro-batch starts immediately after previous completes | Low-latency always-on pipelines | Can burn compute when data is sparse |
| `processingTime="N unit"` | Batch every N; if a batch takes longer, next starts immediately after | Steady near-real-time with controlled cost | Bigger intervals → fewer, larger commits (fewer small files) |
| `availableNow=True` (Spark 3.3+) | Process all available data (respecting `maxFilesPerTrigger`/`maxOffsetsPerTrigger`) in multiple batches, then stop | Scheduled incremental jobs (hourly/daily) using streaming semantics | Replaces `once` |
| `once=True` | Single batch of all available data, then stop | Legacy | Deprecated; ignores rate limits → risk of huge batch |
| `continuous="1 second"` | Continuous processing with checkpoint interval | Millisecond latency experiments | Experimental, at-least-once, limited operators |

**Why `availableNow` is popular**
- You get streaming's **incremental bookkeeping** (checkpointed offsets, exactly-once, state) with **batch-like cost**: the cluster starts, catches up, and shuts down.
- Rate limits are respected, so a big backlog is processed in manageable batches.
- It fits orchestration. Schedule it every 15 minutes in Airflow or Lakeflow Jobs.

**Choosing intervals**
- The latency SLA sets the upper bound (see [Q35](/interview/data-engineering/#q35)).
- Longer intervals produce fewer commits, fewer small files and less overhead (see [Q245](/interview/databricks/#q245)).
- Batch duration should stay below the interval. If it doesn't, you fall behind: watch `processingRate` against `inputRate`.

**Example**

```python
# Runtime: PySpark 3.5
from pyspark.sql import functions as F

source = (spark.readStream.format("delta")
          .option("maxFilesPerTrigger", 200)
          .table("bronze.orders"))

cleaned = source.filter(F.col("order_id").isNotNull())

# Scheduled incremental run: catch up, then stop
q = (cleaned.writeStream
     .option("checkpointLocation", "s3://lake/_checkpoints/silver_orders/")
     .trigger(availableNow=True)
     .toTable("silver.orders_clean"))
q.awaitTermination()

# Always-on alternative: every 30 seconds
# cleaned.writeStream.option("checkpointLocation", "...").trigger(processingTime="30 seconds").toTable("...")
```

**Real-world use case**

A team kept a 24/7 cluster running a stream that received data only during business hours. Switching to `availableNow` triggered every 10 minutes by the scheduler cut compute cost by about 75%, with the same 10-minute freshness SLA.

**Common mistakes and red flags**

- Using `once=True` for large backlogs.
- Default trigger on sparse data, which wastes compute.
- Processing time intervals shorter than batch duration.

**Follow-up questions**

- Does `availableNow` support stateful queries and watermarks? — Yes; state is checkpointed between runs.
- What's the difference between triggered streaming and batch? — Streaming tracks what was processed (offsets/state); batch needs your own watermarking logic.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q290" data-level="Advanced">
<summary><span class="qb-num">Q290</span><h3 class="qb-title">Watermarks and late data in Structured Streaming</h3><span class="qb-meta">Advanced · Very Common · Structured Streaming</span></summary>

**Short answer**

`withWatermark("event_ts", "15 minutes")` tells Spark to track the maximum event time seen, minus 15 minutes, as the watermark. Windows ending before the watermark are finalised. In append mode they are emitted, and their state is dropped. Events later than the watermark are discarded for stateful operations. Watermarks bound state size and define how much lateness you tolerate (see [Q36](/interview/data-engineering/#q36)).

**Detailed explanation**

**Mechanics**
- Watermark = `max(event_time seen) − delay`, computed per micro-batch. It only moves forward.
- For windowed aggregations:
  - **Append mode:** a window's result is emitted **once**, after the watermark passes the window's end. State for that window is then removed.
  - **Update mode:** results are emitted as they change, and state is still cleaned once the watermark passes.
- Late events (event time below the watermark) are **dropped** from stateful operators. They are not guaranteed to be dropped immediately: Spark guarantees that data within the delay is kept, but data beyond it may or may not be processed.

**Where watermarks are required or important**
| Operation | Role |
|---|---|
| Windowed aggregation (append mode) | Required to emit results |
| `dropDuplicates` / `dropDuplicatesWithinWatermark` (Spark 3.5+) | Bounds dedup state |
| Stream-stream joins | Needed (with time-range join conditions) to clean state (see [Q293](/interview/pyspark/#q293)) |
| `transformWithState` / arbitrary stateful ops | Timers and state eviction |

**Multiple watermarks:** with several inputs (joins, unions), the global watermark defaults to the **minimum** across inputs (`spark.sql.streaming.multipleWatermarkPolicy = min`), which is conservative. `max` is possible but drops more data.

**Choosing the delay**
- Measure actual lateness, as a distribution of `ingest_time − event_time`. Choose a delay covering, say, p99.9, balanced against latency and state size.
- Larger delays mean results are later in append mode, and state is larger.
- Data arriving later than the watermark needs a separate correction path: batch reprocessing of late partitions, or a "late events" table (see [Q36](/interview/data-engineering/#q36)).

**Example**

```python
# Runtime: PySpark 3.5
from pyspark.sql import functions as F

events = spark.readStream.table("bronze.sensor_readings")    # device_id, reading, event_ts

per_10min = (events
    .withWatermark("event_ts", "20 minutes")
    .groupBy(F.window("event_ts", "10 minutes"), "device_id")
    .agg(F.avg("reading").alias("avg_reading"), F.count("*").alias("n")))

(per_10min.writeStream
    .outputMode("append")                         # emit each window once, after watermark passes
    .option("checkpointLocation", "s3://lake/_checkpoints/sensor_10min/")
    .trigger(processingTime="1 minute")
    .toTable("silver.sensor_10min"))
```
Walkthrough: the max event time seen is 10:45, so the watermark is 10:25. The 10:00–10:10 and 10:10–10:20 windows are final and emitted. An event stamped 10:05 arriving now is dropped. One stamped 10:28 still updates the 10:20–10:30 window.

**Real-world use case**

A streaming dedup without a watermark kept every event ID ever seen, and state grew to 600 GB over a month until the job died. Switching to `dropDuplicatesWithinWatermark` with a 2-hour watermark kept state at about 5 GB.

**Common mistakes and red flags**

- No watermark on stateful queries.
- Watermark on a processing-time column instead of event time.
- Expecting late data to be updated in append mode.

**Follow-up questions**

- How do you monitor dropped late rows? — Streaming progress metrics include `numRowsDroppedByWatermark` for stateful operators.
- Does the watermark affect stateless queries? — No; it only matters for stateful operations.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q291" data-level="Advanced">
<summary><span class="qb-num">Q291</span><h3 class="qb-title">Checkpoints and exactly-once in Structured Streaming</h3><span class="qb-meta">Advanced · Common · Structured Streaming</span></summary>

**Short answer**

A streaming checkpoint directory stores the offsets planned for each batch (write-ahead log), the commit log of completed batches, the state store, and metadata. On restart, Spark replays any planned but uncommitted batch from the same offsets. With a replayable source (Kafka, files, Delta) and an idempotent or transactional sink (Delta, file sink), that gives end-to-end exactly-once results. Other sinks give at-least-once unless you make writes idempotent.

**Detailed explanation**

**Checkpoint contents**
```text
checkpointLocation/
 ├─ metadata          # query id
 ├─ offsets/          # batch N: source offsets planned (written BEFORE processing)
 ├─ commits/          # batch N: marked complete (written AFTER sink commit)
 ├─ state/            # state store files per operator/partition (HDFS-backed or RocksDB)
 └─ sources/          # source-specific metadata (e.g. file source seen files)
```

**Recovery flow**
1. On restart, read the last offsets entry (batch N) and the last commit (batch N−1).
2. If batch N wasn't committed, **re-run it with exactly the same offsets**.
3. The sink must handle that rerun without duplicates:
   - **Delta sink:** records `(queryId, batchId)` as a transaction. A replayed batch ID is skipped.
   - **File sink:** a `_spark_metadata` log of committed files. Readers that use it ignore uncommitted files.
   - **Kafka sink:** at-least-once (possible duplicates on retry), unless consumers dedupe or you use idempotent producer patterns at application level.
   - **`foreachBatch`:** your code must be idempotent. Use MERGE on keys, or Delta `txnAppId`/`txnVersion` options with the `batchId` (see [Q294](/interview/pyspark/#q294)).

**State store**
- The default HDFS-backed store keeps state in executor memory with checkpointed deltas.
- The **RocksDB state store** (`spark.sql.streaming.stateStore.providerClass`) handles large state better (off-heap, on disk), and changelog checkpointing reduces checkpoint overhead.

**Rules**
- **One checkpoint per query.** Never share one between queries.
- Store it on durable storage (S3, ADLS, GCS, UC volumes), not local disk.
- **Query changes:** changing stateful operators, grouping keys or output schema may be incompatible with the existing checkpoint. Spark 4.0 adds a state data source reader to inspect state. Plan a new checkpoint plus a backfill when you must.
- Deleting a checkpoint means starting over: reprocessing from `startingOffsets`, which can produce duplicates downstream.

**Example**

```python
# Runtime: PySpark 3.5 — exactly-once from Kafka to Delta with RocksDB state
from pyspark.sql import functions as F

spark.conf.set("spark.sql.streaming.stateStore.providerClass",
               "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")

payments = (spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", "broker1:9092")
            .option("subscribe", "payments")
            .option("startingOffsets", "earliest")
            .load()
            .select(F.from_json(F.col("value").cast("string"),
                                "payment_id STRING, amount DECIMAL(12,2), event_ts TIMESTAMP").alias("p"))
            .select("p.*")
            .withWatermark("event_ts", "1 hour")
            .dropDuplicatesWithinWatermark(["payment_id"]))

(payments.writeStream
    .format("delta")
    .option("checkpointLocation", "s3://lake/_checkpoints/silver_payments/")
    .toTable("silver.payments"))
```

**Real-world use case**

An engineer deleted a streaming checkpoint to "fix" a stuck job. The stream restarted from `earliest` and re-inserted 3 days of payments into a Delta table with an append sink, creating duplicates. The runbook now forbids deleting checkpoints without a rebuild plan. The sink was also changed to MERGE on `payment_id`.

**Common mistakes and red flags**

- Assuming exactly-once with any sink.
- Sharing or deleting checkpoints casually.
- Changing stateful query logic without a migration plan.

**Follow-up questions**

- Where is the Kafka offset stored — Kafka consumer groups or the checkpoint? — In Spark's checkpoint (Spark doesn't rely on committed consumer group offsets for recovery).
- What is changelog checkpointing? — RocksDB mode that uploads only changes per batch, reducing checkpoint latency.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q292" data-level="Beginner">
<summary><span class="qb-num">Q292</span><h3 class="qb-title">Output modes: append, update, complete</h3><span class="qb-meta">Beginner · Common · Structured Streaming</span></summary>

**Short answer**

The output mode decides what each micro-batch writes to the sink. Append writes only new rows that will never change. It is the default, and windowed aggregations need a watermark to use it. Update writes only rows that changed since the last trigger. Complete rewrites the entire result table on every trigger, which only suits small aggregation results. Not every mode is valid for every query or sink.

**Detailed explanation**

| Mode | Writes per batch | Valid for | Sink considerations |
|---|---|---|---|
| Append | New final rows only | Stateless queries; aggregations with watermark (emitted once finalised); stream-stream joins | Works with Delta/file/Kafka; simplest |
| Update | Rows that changed (new or updated aggregates) | Aggregations (with/without watermark) | Sink must handle upserts — use `foreachBatch` + MERGE for Delta |
| Complete | Entire result table | Aggregations only | Full rewrite each batch — keep result small |

**Rules of thumb**
- **Stateless ETL** (parse, filter, enrich): append.
- **Windowed metrics with tolerable delay:** append plus a watermark, which emits each window once (see [Q290](/interview/pyspark/#q290)).
- **Running totals or live counters** that change continuously: update, plus `foreachBatch` MERGE into Delta.
- **Small dashboards** (top 100 pages, per-country counts): complete into a small table.
- Sorting a streaming DataFrame is only allowed after an aggregation in complete mode.

**Common errors**
- "Append output mode not supported when there are streaming aggregations on streaming DataFrames without watermark." Add a watermark, or use update or complete.
- Using update mode directly with a Delta sink appends the changed rows (duplicates by key). Handle it with MERGE in `foreachBatch`.

**Example**

```python
# Runtime: PySpark 3.5 — update mode with MERGE into Delta via foreachBatch
from pyspark.sql import functions as F
from delta.tables import DeltaTable

orders = spark.readStream.table("silver.orders_stream")
totals = orders.groupBy("customer_id").agg(F.sum("amount").alias("lifetime_spend"))

def upsert_totals(batch_df, batch_id):
    target = DeltaTable.forName(spark, "gold.customer_lifetime_spend")
    (target.alias("t")
        .merge(batch_df.alias("s"), "t.customer_id = s.customer_id")
        .whenMatchedUpdate(set={"lifetime_spend": "s.lifetime_spend"})
        .whenNotMatchedInsertAll()
        .execute())

(totals.writeStream
    .outputMode("update")
    .foreachBatch(upsert_totals)
    .option("checkpointLocation", "s3://lake/_checkpoints/customer_lifetime_spend/")
    .start())
```
This aggregation without a watermark keeps state for every customer forever. That is acceptable for a bounded key space (customers), but not for unbounded keys such as event IDs.

**Real-world use case**

A team used complete mode to maintain per-user counts for 50 million users, and every batch rewrote a 4 GB table. Switching to update mode with a MERGE wrote only the few thousand changed users per batch.

**Common mistakes and red flags**

- Complete mode on large results.
- Update mode into an append-only sink, creating duplicates.
- Not knowing append needs a watermark for aggregations.

**Follow-up questions**

- Which mode does a stateless query use? — Append (update behaves the same for stateless queries).
- Can you sort in streaming? — Only after aggregation in complete mode.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q293" data-level="Advanced">
<summary><span class="qb-num">Q293</span><h3 class="qb-title">Stream-stream joins</h3><span class="qb-meta">Advanced · Occasional · Structured Streaming</span></summary>

**Short answer**

Structured Streaming can join two streams, for example ad impressions with clicks. Spark buffers past rows of both sides in state so later rows can match. To keep that state bounded, define watermarks on both sides and a time-range condition in the join, such as a click within 1 hour after the impression. Outer joins also require watermarks and emit unmatched rows only after the watermark passes.

**Detailed explanation**

**Join types**
| Join | Requirements | Output timing |
|---|---|---|
| Inner | Optional watermarks + time constraint (strongly recommended for bounded state) | As soon as matches occur |
| Left/right outer | Watermark on the opposite side + event-time range condition | Matches immediately; non-matches (NULLs) after watermark guarantees no future match |
| Full outer, left semi | Supported in Spark 3.1+ with same constraints | Similar |

**State cleanup logic**
- With `impression_ts` and `click_ts` watermarked and the condition `click_ts BETWEEN impression_ts AND impression_ts + INTERVAL 1 HOUR`:
  - Spark knows an impression can't match clicks older than its own time, or newer than one hour after it.
  - Once the click-side watermark passes `impression_ts + 1 hour`, that impression's state is removed.
- Without the range condition, state grows without bound even with watermarks.

**Practical notes**
- Join keys must be equality conditions (`impression_id`), plus the time range.
- Output mode is **append** only.
- State size is roughly (input rate × retention window), so monitor `stateOperators` metrics in the progress output.
- **Stream-static joins** (a stream joined to a Delta dimension table) are simpler and stateless. The static side is re-read each batch (Delta picks up the latest version). Use those for enrichment with dimensions.
- For complex event correlation (sessions, multi-step funnels), `transformWithState` (Spark 4.0) or `applyInPandasWithState` gives custom state logic with timers.

**Example**

```python
# Runtime: PySpark 3.5 — impressions joined to clicks within 1 hour
from pyspark.sql import functions as F

impressions = (spark.readStream.table("bronze.ad_impressions")
               .select("impression_id", "ad_id", F.col("event_ts").alias("impression_ts"))
               .withWatermark("impression_ts", "2 hours"))

clicks = (spark.readStream.table("bronze.ad_clicks")
          .select(F.col("impression_id").alias("click_impression_id"), F.col("event_ts").alias("click_ts"))
          .withWatermark("click_ts", "3 hours"))

attributed = impressions.join(
    clicks,
    F.expr("""
        click_impression_id = impression_id AND
        click_ts >= impression_ts AND
        click_ts <= impression_ts + interval 1 hour
    """),
    "leftOuter")

(attributed.writeStream
    .outputMode("append")
    .option("checkpointLocation", "s3://lake/_checkpoints/ad_attribution/")
    .toTable("silver.ad_attribution"))
```
Impressions with no click within an hour are emitted with NULL click columns. That happens after the click watermark passes `impression_ts + 1 hour`, roughly 4 hours after the impression given the 3-hour watermark.

**Real-world use case**

An ad-tech team computed click-through attribution in real time. An earlier version without the time-range condition hit 1 TB of state in a week. Adding the 1-hour constraint stabilised state at about 40 GB.

**Common mistakes and red flags**

- No time-range condition.
- Expecting outer-join NULL rows immediately.
- Using a stream-stream join where a stream-static join would do.

**Follow-up questions**

- How is a stream-static join different? — No state; each micro-batch joins with the current static table snapshot.
- How do you handle very late clicks? — Accept loss beyond the watermark, or run a batch correction job.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q294" data-level="Intermediate">
<summary><span class="qb-num">Q294</span><h3 class="qb-title">foreachBatch for upserts and multiple sinks</h3><span class="qb-meta">Intermediate · Common · Structured Streaming</span></summary>

**Short answer**

`foreachBatch(func)` gives you each micro-batch as a normal DataFrame plus a batch ID, so you can use any batch API. Typical uses are MERGE into Delta, writing to several sinks, calling JDBC, or custom dedupe. It is at-least-once by default: a failed batch is retried with the same data and batch ID. Make the function idempotent, for example with MERGE on keys or Delta's `txnAppId`/`txnVersion` options.

**Detailed explanation**

**Why use it**
- Sinks without native streaming support (JDBC databases, some APIs).
- **Upserts and CDC application** with MERGE (see [Q213](/interview/databricks/#q213)).
- **Multiple outputs** from one stream (write to Delta and publish a summary). Cache the batch DataFrame if you write it more than once.
- Batch-only operations unavailable on streaming DataFrames.

**Idempotency patterns**
| Pattern | How |
|---|---|
| MERGE on business key with dedupe | Re-applying the same batch yields same final state |
| Delta idempotent writes | `.option("txnAppId", app_id).option("txnVersion", batch_id)` — Delta skips if already committed |
| Track batch_id in a control table | Skip if batch already processed (for non-Delta sinks) |
| Overwrite partitions derived from batch | `replaceWhere` for deterministic partitions |

**Dedupe within the batch:** a micro-batch may contain several changes for the same key. Keep the latest per key (by sequence or event time) before MERGE. Otherwise MERGE fails with "multiple source rows matched".

**Performance**
- The function runs on the driver, but the DataFrame operations inside it run distributed. Don't `collect()` inside.
- If you write the batch to two sinks, `batch_df.persist()` first and `unpersist()` at the end.
- Long MERGEs into big tables slow every batch. Cluster the target by the merge key and use deletion vectors (see [Q210](/interview/databricks/#q210), [Q213](/interview/databricks/#q213)).

**Example**

```python
# Runtime: PySpark 3.5 with Delta Lake — CDC upsert, idempotent and deduped
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

cdc = spark.readStream.table("bronze.customer_cdc")    # customer_id, op, change_seq, name, segment

def apply_cdc(batch_df, batch_id):
    latest = (batch_df
              .withColumn("rn", F.row_number().over(
                  Window.partitionBy("customer_id").orderBy(F.desc("change_seq"))))
              .filter("rn = 1").drop("rn"))
    (DeltaTable.forName(spark, "silver.customers").alias("t")
        .merge(latest.alias("s"), "t.customer_id = s.customer_id")
        .whenMatchedDelete(condition="s.op = 'D'")
        .whenMatchedUpdate(condition="s.change_seq > t.change_seq",
                           set={"name": "s.name", "segment": "s.segment", "change_seq": "s.change_seq"})
        .whenNotMatchedInsert(condition="s.op <> 'D'",
                              values={"customer_id": "s.customer_id", "name": "s.name",
                                      "segment": "s.segment", "change_seq": "s.change_seq"})
        .execute())

(cdc.writeStream
    .foreachBatch(apply_cdc)
    .option("checkpointLocation", "s3://lake/_checkpoints/silver_customers_cdc/")
    .trigger(availableNow=True)
    .start())
```
The `change_seq > t.change_seq` guard makes a replayed batch a no-op for rows already applied, which keeps the function idempotent.

**Real-world use case**

A streaming CDC job appended changes with `foreachBatch` and no idempotency. After a cluster restart, one batch was applied twice, producing duplicate rows. Switching to a deduped MERGE with a sequence guard made retries harmless.

**Common mistakes and red flags**

- Assuming `foreachBatch` is exactly-once.
- No dedupe within the batch.
- `collect()` inside the function.

**Follow-up questions**

- Can you use `foreachBatch` with continuous processing? — No; micro-batch only.
- `foreach` vs `foreachBatch`? — `foreach` processes each row with an open/process/close writer; `foreachBatch` gives the batch DataFrame.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q295" data-level="Beginner">
<summary><span class="qb-num">Q295</span><h3 class="qb-title">Coding: deduplicate to keep the latest record per key</h3><span class="qb-meta">Beginner · Very Common · Coding Patterns</span></summary>

**Short answer**

Use `row_number()` over a window partitioned by the key, ordered by the recency column descending, with a deterministic tiebreaker, and keep `rn = 1`. `dropDuplicates(["key"])` keeps an arbitrary row, so it is wrong when you need the latest. An alternative is `max_by` (Spark 3.3+), or a max-struct trick in `groupBy`, to avoid a full sort.

**Detailed explanation**

**Approach 1: `row_number` (most common and clearest)**
- `Window.partitionBy("customer_id").orderBy(F.desc("updated_at"), F.desc("ingested_at"))`.
- Filter `rn == 1`. The tiebreaker ensures determinism when `updated_at` ties.
- NULL handling: `desc` puts NULLs last by default, which is usually what you want.

**Approach 2: `groupBy` + `max_by` / struct max**
- `F.max_by("col", "updated_at")` per column (Spark 3.3+).
- Or `F.max(F.struct("updated_at", "ingested_at", *other_cols))` and then expand the struct. The struct comparison orders by its fields left to right.
- Often cheaper than a window, because there is no sort within groups (hash aggregation), but it is less readable.

**Approach 3: `dropDuplicates` (only for exact duplicates or "any one row")**
- `df.dropDuplicates(["customer_id"])` keeps one arbitrary row per key. That is fine for exact-duplicate removal, not for "latest".

**Streaming variant:** `dropDuplicatesWithinWatermark` for event-ID dedupe with bounded state (see [Q290](/interview/pyspark/#q290)).

**Comparison**
| Method | Deterministic latest? | Cost | Readability |
|---|---|---|---|
| `row_number` window | Yes (with tiebreaker) | Shuffle + sort per key | High |
| `max_by` / struct max | Yes | Shuffle + hash aggregate | Medium |
| `dropDuplicates` | No | Shuffle + hash aggregate | High (but wrong for latest) |

**Example**

```python
# Runtime: PySpark 3.5
from pyspark.sql import functions as F
from pyspark.sql.window import Window

updates = spark.createDataFrame([
    (1, "Asha",  "SMB", "2026-03-01 10:00:00", "2026-03-01 10:05:00"),
    (1, "Asha",  "MID", "2026-03-02 09:00:00", "2026-03-02 09:01:00"),
    (2, "Ben",   "ENT", "2026-03-01 11:00:00", "2026-03-01 11:00:30"),
    (2, "Ben",   "MID", "2026-03-01 11:00:00", "2026-03-01 11:02:00"),   # tie on updated_at
], "customer_id INT, name STRING, segment STRING, updated_at STRING, ingested_at STRING") \
  .withColumn("updated_at", F.to_timestamp("updated_at")) \
  .withColumn("ingested_at", F.to_timestamp("ingested_at"))

w = Window.partitionBy("customer_id").orderBy(F.desc("updated_at"), F.desc("ingested_at"))
latest = updates.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")
latest.show()

latest_alt = (updates.groupBy("customer_id")
              .agg(F.max(F.struct("updated_at", "ingested_at", "name", "segment")).alias("r"))
              .select("customer_id", "r.name", "r.segment", "r.updated_at"))
```
Result: customer 1 → MID (newer `updated_at`). Customer 2 → MID (same `updated_at`, but newer `ingested_at` wins the tie).

**Real-world use case**

A silver customer table used `dropDuplicates(["customer_id"])` and occasionally showed stale addresses, because the kept row was arbitrary. Replacing it with `row_number` ordered by `updated_at` and `ingested_at` fixed the mismatches reported by the CRM team.

**Common mistakes and red flags**

- `dropDuplicates` for "latest".
- No tiebreaker.
- `orderBy` on the whole DataFrame followed by `dropDuplicates` (order isn't guaranteed to be preserved).

**Follow-up questions**

- How would you do it in SQL? — `QUALIFY ROW_NUMBER() OVER (...) = 1` (see [Q115](/interview/sql/#q115)).
- How do you handle deletes in the latest record? — Keep the latest change including delete flags, then filter or apply them in MERGE.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q296" data-level="Intermediate">
<summary><span class="qb-num">Q296</span><h3 class="qb-title">Coding: top-N per group and running totals</h3><span class="qb-meta">Intermediate · Very Common · Coding Patterns</span></summary>

**Short answer**

For top-N per group, rank rows with `row_number` (exactly N) or `dense_rank` (ties included) over `partitionBy(group).orderBy(desc(metric))`, and filter rank ≤ N. For running totals, use `F.sum(metric).over(partitionBy(group).orderBy(time).rowsBetween(Window.unboundedPreceding, Window.currentRow))`. Pre-aggregate to the needed grain first, so windows run over fewer rows.

**Detailed explanation**

**Top-N per group**
- Pre-aggregate first (for example sales per product per category per month), then rank.
- Choose a ranking function deliberately:
  - `row_number`: exactly N rows. Ties are broken arbitrarily unless you add a tiebreaker.
  - `rank`: ties share a rank, with gaps (1, 1, 3). Filtering ≤ N can return more than N.
  - `dense_rank`: ties share a rank, no gaps (1, 1, 2). Returns all items in the top N distinct values.

**Running totals**
- A row frame from the start of the partition to the current row.
- Specify `rowsBetween`. The default frame with `orderBy` is RANGE, which includes ties in the order column and can surprise you (see [Q111](/interview/sql/#q111)).
- Running totals reset per partition, so partition by the reset grain (customer, or customer and year).

**Related variants**
- **Running total within each month:** `partitionBy("customer_id", "month")`.
- **Rolling 7-day sums:** `rangeBetween` over a numeric day index (days since epoch), or a 7-row frame if data is dense and daily.
- **Percent of total:** `F.col("x") / F.sum("x").over(Window.partitionBy("grp"))`.

**Performance:** one shuffle per distinct partition spec. Combine operations with the same window spec in one `select`.

**Example**

```python
# Runtime: PySpark 3.5
from pyspark.sql import functions as F
from pyspark.sql.window import Window

sales = spark.read.table("silver.order_items")   # category, product_id, order_date, amount

# Top 3 products per category by revenue (ties included via dense_rank)
prod_rev = sales.groupBy("category", "product_id").agg(F.sum("amount").alias("revenue"))
w_rank = Window.partitionBy("category").orderBy(F.desc("revenue"))
top3 = (prod_rev.withColumn("rnk", F.dense_rank().over(w_rank))
                .filter(F.col("rnk") <= 3))

# Daily revenue running total per category
daily = sales.groupBy("category", "order_date").agg(F.sum("amount").alias("daily_revenue"))
w_run = (Window.partitionBy("category").orderBy("order_date")
               .rowsBetween(Window.unboundedPreceding, Window.currentRow))
running = daily.withColumn("running_revenue", F.sum("daily_revenue").over(w_run))
```
Sample: category "Toys" with daily revenue 100, 40 and 60 on consecutive days gives running revenue of 100, 140 and 200.

**Real-world use case**

A merchandising report needed the top 10 SKUs per store per week, plus cumulative weekly sales. Pre-aggregating to store-SKU-week before windowing cut the windowed row count from 2 billion transactions to 30 million.

**Common mistakes and red flags**

- Ranking raw transactions instead of aggregates.
- `rank` when exactly N rows are required.
- Default RANGE frame with duplicate dates.

**Follow-up questions**

- How do you get exactly N with ties broken by name? — `row_number` with `orderBy(desc("revenue"), "product_name")`.
- How do you do this in SQL? — `QUALIFY DENSE_RANK() OVER (...) <= 3` and `SUM() OVER (... ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)` (see [Q109](/interview/sql/#q109), [Q111](/interview/sql/#q111)).

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q297" data-level="Beginner">
<summary><span class="qb-num">Q297</span><h3 class="qb-title">Coding: word count / frequency count</h3><span class="qb-meta">Beginner · Common · Coding Patterns</span></summary>

**Short answer**

Read the text into a DataFrame (`spark.read.text`). Lower-case it, split on non-word characters, `explode` into one word per row, filter out empty strings (and optionally stopwords), then `groupBy("word").count()` and order by count descending. It is Spark's classic hello world, and it shows narrow transformations, a shuffle (groupBy) and map-side partial aggregation.

**Detailed explanation**

**Steps and what they show**
1. `spark.read.text(path)`: one row per line, in a column named `value`. Input partitions are based on file splits (see [Q260](/interview/pyspark/#q260)).
2. `lower` + `regexp_replace` / `split(value, r"[^a-z0-9']+")`: narrow, per-row transformations.
3. `explode`: one row per token, still narrow, but the row count grows.
4. `filter(word != "")`: removes empty tokens created by leading punctuation.
5. `groupBy("word").count()`: a wide transformation. Spark does `partial_count` per partition, a shuffle by word, then the final count (see [Q285](/interview/pyspark/#q285)).
6. `orderBy(desc("count"))`: another shuffle (range partitioning) for a global sort. Use `limit` for top-K.

**Variations interviewers ask for**
- **Stopwords:** a broadcast small DataFrame with a left anti join, or `isin` with a small list.
- **Top-K only:** `orderBy(...).limit(k)` (Spark optimises this to a TakeOrderedAndProject, avoiding a full sort).
- **Per-document counts:** `groupBy("doc_id", "word")`.
- **Streaming word count:** the same code on `readStream`, with complete or update output mode.
- **RDD version:** `flatMap` → `map((w, 1))` → `reduceByKey` (see [Q257](/interview/pyspark/#q257)).

**Example**

```python
# Runtime: PySpark 3.5
from pyspark.sql import functions as F

lines = spark.createDataFrame(
    [("The quick brown fox.",), ("The lazy dog, the end!",), ("",)], "value STRING")

stopwords = spark.createDataFrame([("the",), ("a",), ("an",)], "word STRING")

word_counts = (lines
    .select(F.explode(F.split(F.lower("value"), r"[^a-z0-9']+")).alias("word"))
    .filter(F.col("word") != "")
    .join(F.broadcast(stopwords), "word", "left_anti")
    .groupBy("word").count()
    .orderBy(F.desc("count"), "word"))

word_counts.show()
```
Result (with stopwords removed): brown 1, dog 1, end 1, fox 1, lazy 1, quick 1. Without the stopword filter, "the" would have a count of 3.

**Real-world use case**

A support team used the same pattern on 20 million ticket descriptions to find the most frequent error terms per product each week. The output fed a dashboard that flagged spikes in terms like "timeout" after a release.

**Common mistakes and red flags**

- Splitting only on spaces (punctuation stays attached).
- Forgetting to filter empty strings.
- Using `collect()` and Python `Counter` on the driver.

**Follow-up questions**

- How many shuffles does this have? — Typically two (groupBy and global orderBy), plus a broadcast for stopwords.
- How would you get the top 10 only? — `orderBy(...).limit(10)` — optimised to avoid full sort.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q298" data-level="Advanced">
<summary><span class="qb-num">Q298</span><h3 class="qb-title">What&#x27;s new in Spark 4.0 for data engineers?</h3><span class="qb-meta">Advanced · Occasional · Versions</span></summary>

**Short answer**

Spark 4.0 (released in 2025) turns ANSI SQL mode on by default, which brings stricter casting and overflow errors. It adds the VARIANT type for semi-structured data, SQL scripting and pipe syntax, string collations, the Python Data Source API, and `transformWithState` for advanced stateful streaming. Spark Connect is improved, there is a lightweight Python client, and Java 17 is the default. Upgrades need testing because ANSI changes can break existing jobs.

**Detailed explanation**

**Key changes**
| Area | Change | Impact |
|---|---|---|
| ANSI mode default (`spark.sql.ansi.enabled=true`) | Invalid casts, overflow, divide-by-zero raise errors instead of returning NULL/wrapping | Existing pipelines may fail; use `try_cast`, `try_divide`, `try_*` functions |
| VARIANT type | Efficient storage and querying of semi-structured data (`parse_json`, `variant_get`, `:` path syntax in some engines) | Faster than JSON strings for nested data |
| SQL scripting | Multi-statement scripts with variables, IF/WHILE/FOR, exception handlers | Procedural SQL inside Spark |
| SQL pipe syntax | `FROM t \|> WHERE ... \|> AGGREGATE ...` | Readable step-by-step queries |
| Collations | Case/accent-insensitive comparisons via `COLLATE` | Fewer `lower()` workarounds |
| Python Data Source API | Write custom batch/stream sources and sinks in Python | Easier connectors |
| Python UDTFs (since 3.5) and improvements | Table-valued Python functions | Custom row-generating logic |
| Structured Streaming `transformWithState` | Arbitrary stateful processing with timers, TTL, multiple state variables | Replaces many `flatMapGroupsWithState` uses |
| State data source reader | Read/inspect streaming state as a DataFrame | Debugging stateful jobs |
| Spark Connect enhancements + `pyspark-client` | Thin Python client package, closer API parity | Remote clients, IDE development (see [Q299](/interview/pyspark/#q299)) |
| Java 17 default, Scala 2.13 | Platform updates | Rebuild JVM libraries |
| Observability/logging | Structured JSON logging | Easier log analysis |

**Upgrade checklist**
1. Run test suites with ANSI on. Fix casts (`try_cast`) and arithmetic edge cases.
2. Check removed or deprecated APIs and configurations, and the library compatibility of connectors (Delta, Kafka, cloud connectors).
3. Rebuild Scala and Java dependencies for 2.13 and Java 17.
4. Diff outputs between versions on representative data (see [Q234](/interview/databricks/#q234)).
5. Review the release notes of your platform's runtime that ships Spark 4.x (for example Databricks Runtime 17.x).

Spark 4.1 followed with further additions, such as Spark Declarative Pipelines (see [Q221](/interview/databricks/#q221)). Check the release notes for your version.

**Example**

```sql
-- Dialect: Spark SQL 4.0
SET spark.sql.ansi.enabled = true;

SELECT try_cast('12a' AS INT) AS safe_cast,          -- NULL instead of error
       try_divide(10, 0)       AS safe_div;          -- NULL instead of error

SELECT variant_get(parse_json('{"user":{"id":42,"tier":"gold"}}'), '$.user.tier', 'string') AS tier;

FROM silver.orders
|> WHERE order_date >= DATE'2026-03-01'
|> AGGREGATE SUM(amount) AS revenue GROUP BY region
|> ORDER BY revenue DESC;
```

**Real-world use case**

During a Spark 4 upgrade test, 6 of 120 jobs failed because string-to-int casts on dirty data now raised errors. The team replaced `cast` with `try_cast` plus explicit data-quality counts, which surfaced bad data that had previously turned into NULLs silently.

**Common mistakes and red flags**

- Upgrading without testing ANSI behaviour.
- Assuming VARIANT works identically across engines.
- Not knowing Spark 4 exists.

**Follow-up questions**

- Can you disable ANSI mode? — Yes (`spark.sql.ansi.enabled=false`), but better to fix code; ANSI catches silent data issues.
- What is `transformWithState`? — A new stateful streaming operator with richer state types, timers and TTL.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q299" data-level="Beginner">
<summary><span class="qb-num">Q299</span><h3 class="qb-title">Spark Connect (client-server)</h3><span class="qb-meta">Beginner · Occasional · Architecture</span></summary>

**Short answer**

Spark Connect (Spark 3.4+) separates the client from the Spark driver. A thin client, such as the PySpark client from any IDE, builds unresolved logical plans and sends them over gRPC to a remote Spark Connect server, which runs the driver and executes the work. Clients become lightweight and stable across upgrades. One cluster can safely serve many clients, and Spark can be embedded in more applications and languages.

**Detailed explanation**

**Architecture**
```text
Client (Python/Scala/Go/Rust/...)          Spark Connect server (driver)
  DataFrame API → unresolved plan  --gRPC-->  analyse/optimise/execute
  results  <-- Arrow batches ----------------  executors process data
```

**Benefits**
- **Thin clients:** no JVM needed locally for the Python client (`pyspark-client` package in Spark 4.0). Use it from laptops, notebooks, web apps or IDEs.
- **Stability:** client and server upgrade more independently. Client crashes don't kill the driver, and driver issues don't crash the client process.
- **Multi-tenancy:** many sessions on one server, with isolation.
- **Debugging:** run code locally in an IDE against a remote cluster.

**Limitations (vary by version)**
- No direct **RDD** API or `SparkContext` access from Connect clients.
- Some APIs and features differ or arrived later (certain ML APIs, some streaming features). Parity improves with each release.
- UDFs still execute on the server side (Python UDFs need matching Python environments on the server).

**Where you see it**
- **Databricks Connect** (v13+) is built on Spark Connect, connecting local IDEs to Databricks clusters or serverless.
- Databricks **serverless and standard access mode** compute use Spark Connect-style architecture, which is why RDD APIs are unavailable there (see [Q247](/interview/databricks/#q247), [Q257](/interview/pyspark/#q257)).
- OSS: start with `sbin/start-connect-server.sh` and connect using `SparkSession.builder.remote("sc://host:15002")`.

**Example**

```python
# Runtime: PySpark 3.5+ client (pip install "pyspark[connect]") against a Spark Connect server
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.remote("sc://spark-connect.internal.acme.example:15002").getOrCreate()

orders = spark.read.table("silver.orders")
summary = (orders.filter(F.col("order_date") >= "2026-03-01")
                 .groupBy("region").agg(F.sum("amount").alias("revenue")))
print(summary.toPandas())        # small result returned as Arrow batches
```

**Real-world use case**

A team built an internal FastAPI service that runs parameterised aggregations on the data lake. With Spark Connect, the web service runs a lightweight Python client, and the shared Spark cluster does the heavy work. A service crash no longer takes down the driver.

**Common mistakes and red flags**

- Expecting RDD code to work over Connect.
- Assuming results stream unlimited to the client (still return small results).
- Confusing it with the Thrift/JDBC server.

**Follow-up questions**

- Spark Connect vs Thrift server? — Thrift serves SQL over JDBC/ODBC; Connect serves the full DataFrame API (plans) over gRPC.
- What's Databricks Connect? — Databricks' client library based on Spark Connect for running code from IDEs against Databricks compute.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q300" data-level="Intermediate">
<summary><span class="qb-num">Q300</span><h3 class="qb-title">Spark vs pandas vs Polars: when to use each</h3><span class="qb-meta">Intermediate · Common · Tool Selection</span></summary>

**Short answer**

Use pandas for small to medium data that fits comfortably in one machine's memory (up to a few GB), for quick analysis and ecosystem compatibility. Use Polars for single-machine workloads that need speed and memory efficiency (multi-threaded, lazy, Arrow-based, tens to hundreds of GB on large machines, with streaming execution). Use Spark when data or compute exceeds one machine, or you need distributed processing, lakehouse integration, streaming and fault tolerance.

**Detailed explanation**

| Dimension | pandas | Polars | Spark (PySpark) |
|---|---|---|---|
| Execution | Single-threaded mostly, eager | Multi-threaded, lazy + eager, Rust engine | Distributed, lazy, JVM/Photon |
| Data scale | Fits in RAM (rule of thumb: data × 5–10 ≤ RAM) | Larger-than-RAM possible via streaming engine on one machine | TBs–PBs across cluster |
| Startup/overhead | Minimal | Minimal | Cluster/session overhead (seconds–minutes) |
| API | Mature, huge ecosystem | Expression-based, consistent, fast | DataFrame/SQL, rich connectors |
| Memory format | NumPy (Arrow-backed options in 2.x) | Apache Arrow | Tungsten binary / Arrow for Python interchange |
| Fault tolerance | None | None | Task retries, lineage |
| Lakehouse integration | Via libraries (deltalake, pyiceberg) | Via libraries (deltalake, pyiceberg) | Native (Delta, Iceberg, UC/catalogs) |
| Streaming | No | Limited | Structured Streaming |
| Best for | EDA, small ETL, ML feature prep on samples | Fast single-node ETL, local data apps, CI tests | Big data ETL, streaming, shared platforms |

**Decision heuristics**
- **Data under 1 GB and simple logic:** pandas or Polars. Spark's overhead dominates.
- **1–100 GB on one strong machine:** Polars (or DuckDB for SQL) is often faster and cheaper than a cluster.
- **Above roughly 100 GB, growing data, many concurrent jobs, or streaming:** Spark.
- **Already on a Spark platform** with governance (Unity Catalog), lineage and orchestration: Spark keeps operations consistent, even for medium data. Serverless reduces the overhead.
- **pandas API on Spark** (`pyspark.pandas`) helps migrate pandas code to scale, but performance differs. Test it.

**Interop**
- Spark → pandas: `df.toPandas()` (Arrow-accelerated with `spark.sql.execution.arrow.pyspark.enabled`). Small results only.
- Polars ↔ Arrow ↔ Spark: `pl.from_arrow`, and Spark `createDataFrame` from pandas or Arrow.
- pandas UDFs bring pandas logic into Spark at scale (see [Q270](/interview/pyspark/#q270)).

**Example**

```python
# Runtime: Python 3.12 with polars 1.x, pandas 2.x; PySpark 3.5
import polars as pl
import pandas as pd

# Polars: lazy, multi-threaded single-node aggregation over Parquet files
top_regions_pl = (pl.scan_parquet("data/orders/*.parquet")
                    .filter(pl.col("order_date") >= pl.date(2026, 3, 1))
                    .group_by("region")
                    .agg(pl.col("amount").sum().alias("revenue"))
                    .sort("revenue", descending=True)
                    .collect())

# pandas: fine for a small extract
small = pd.read_parquet("data/orders/2026-03-01.parquet")
top_regions_pd = small.groupby("region", as_index=False)["amount"].sum().sort_values("amount", ascending=False)

# Spark: same logic at cluster scale
from pyspark.sql import functions as F
top_regions_spark = (spark.read.parquet("s3://lake/silver/orders/")
                     .filter(F.col("order_date") >= "2026-03-01")
                     .groupBy("region").agg(F.sum("amount").alias("revenue"))
                     .orderBy(F.desc("revenue")))
```

**Real-world use case**

A team ran 40 small daily Spark jobs (each under 2 GB) on a cluster, paying for 5-minute start-ups each time. Moving them to Polars on a single container cut runtimes to under a minute each and reduced cost by about 80%. Large fact-table jobs stayed on Spark.

**Common mistakes and red flags**

- Spark for tiny datasets.
- pandas on data bigger than memory.
- Treating `toPandas()` as a scaling solution.

**Follow-up questions**

- Where does DuckDB fit? — Single-node SQL engine, excellent for local analytics on Parquet/Delta/Iceberg, similar niche to Polars.
- How do you test Spark logic quickly? — Small local Spark session with pytest fixtures and tiny DataFrames (see [Q88](/interview/data-engineering/#q88)).

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>
