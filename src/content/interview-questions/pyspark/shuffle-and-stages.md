---
publishedDate: "2026-10-15"
updatedDate: "2026-10-15"
reviewedDate: "2026-10-15"
title: "What causes a shuffle in Spark, and why is it expensive?"
seoTitle: "Spark Shuffle: PySpark Interview Answer"
description: "A Spark shuffle redistributes data across partitions, adding network, disk and serialization costs; identify it in plans and Spark UI stages."
technology: ["pyspark", "spark"]
topic: ["execution", "shuffles"]
difficulty: "Medium"
questionType: ["conceptual", "optimization"]
estimatedMinutes: 7
interviewRelevance: "High"
shortAnswer: "A shuffle redistributes records across partitions, commonly for a join, `groupBy`, `distinct` or repartition by key. Executors may need to serialize data, write intermediate blocks, transfer them across the network and read them in a later stage. That makes shuffles a frequent cost and skew risk, though they are sometimes necessary for correct distributed aggregation or joins. I look at the physical plan and Spark UI before trying to remove or reduce one."
followUps: ["What separates Spark stages?", "How does a broadcast join avoid a shuffle?", "Why can a groupBy spill to disk?", "What metrics reveal skew?"]
related: ["articles:spark/partitions-shuffles-skew", "interview-questions:pyspark/broadcast-join"]
sources:
  - { label: "Apache Spark documentation: Performance tuning", url: "https://spark.apache.org/docs/latest/sql-performance-tuning.html" }
versionContext: "Planner choices and shuffle behavior are Spark-version- and query-dependent."
---

## Narrow versus wide work

A filter can usually process each input partition independently, so it is narrow. A `groupBy('customer_id')` must bring rows for the same key together, so it normally causes a wide dependency and a shuffle. A stage boundary often appears where Spark must materialize shuffle output before the next stage can proceed.

The Spark UI can show shuffle read/write bytes, fetch wait, spill, task duration and partition sizes. A few unusually long tasks often suggest skew; uniformly large shuffle volume may suggest too much data or a join strategy problem. Increasing the cluster size without reading those metrics can leave the underlying exchange unchanged.

## Common mistakes

1. Calling every shuffle a bug; repartitioning is sometimes required.
2. Increasing shuffle partitions without checking data size and task overhead.
3. Confusing a slow task caused by skew with a uniformly expensive shuffle.
