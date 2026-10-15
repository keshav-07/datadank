---
publishedDate: "2026-10-15"
updatedDate: "2026-10-15"
reviewedDate: "2026-10-15"
title: "What does Adaptive Query Execution do in Spark?"
seoTitle: "Spark Adaptive Query Execution: Interview Answer"
description: "Spark AQE uses runtime statistics to adjust supported query plans, including shuffle partition coalescing and selected join or skew optimizations."
technology: ["pyspark", "spark"]
topic: ["query-planning", "optimization"]
difficulty: "Medium"
questionType: ["conceptual", "optimization"]
estimatedMinutes: 7
interviewRelevance: "High"
shortAnswer: "Adaptive Query Execution (AQE) can revise supported parts of a Spark SQL physical plan using runtime statistics collected during execution. Documented optimizations include coalescing post-shuffle partitions, handling skewed joins, and converting some join strategies when runtime sizes permit. AQE reduces the need to guess every setting in advance, but it does not remove the need to inspect data quality, query plans and runtime metrics; behavior depends on Spark version, configuration and query shape."
followUps: ["Why can AQE coalesce shuffle partitions?", "Can AQE turn every join into a broadcast join?", "How do you tell whether AQE changed a plan?"]
related: ["interview-questions:pyspark/shuffle-and-stages", "interview-questions:pyspark/data-skew"]
sources:
  - { label: "Apache Spark documentation: Adaptive Query Execution", url: "https://spark.apache.org/docs/latest/sql-performance-tuning.html#adaptive-query-execution" }
versionContext: "AQE defaults and optimizations vary by Spark distribution and version."
---

## Why runtime information helps

Before execution, Spark uses available statistics to select a plan. Some real sizes are only known after a stage completes. AQE can use those runtime statistics to adjust supported downstream decisions—for example, combine many small shuffle partitions or apply skew-aware handling to a join.

This can make a workload less sensitive to imperfect initial estimates, but the adaptive plan still has limits. It cannot make a huge many-to-many join cheap or correct a bad join condition. Inspect the final adaptive plan in the Spark UI or explain output after execution, then compare input, shuffle and task metrics.

## Common mistakes

1. Assuming AQE is enabled and configured identically on every cluster.
2. Treating it as a substitute for understanding cardinality and data skew.
3. Judging plan changes from only the initial, pre-execution plan.
