---
publishedDate: "2026-10-15"
updatedDate: "2026-10-15"
reviewedDate: "2026-10-15"
title: "How do you diagnose and handle data skew in PySpark?"
seoTitle: "PySpark Data Skew: Interview Answer"
description: "Diagnose skew from partition and task metrics, then choose a targeted fix such as salting, pre-aggregation or adaptive skew handling."
technology: ["pyspark", "spark"]
topic: ["joins", "performance", "skew"]
difficulty: "Hard"
questionType: ["debugging", "optimization", "scenario"]
estimatedMinutes: 9
interviewRelevance: "High"
shortAnswer: "I confirm skew by comparing task durations, input rows and shuffle sizes: a small number of much larger or slower tasks is a common signature. Then I inspect the key distribution and query plan. Possible fixes include pre-aggregating, filtering unnecessary rows, broadcasting a genuinely small side, salting hot keys when semantics allow it, or using Spark's adaptive skew handling. The right fix depends on whether the skewed key is legitimate and whether changing partitioning preserves the join or aggregation result."
followUps: ["How does salting work?", "When does broadcast help?", "How could salting break a distinct count?", "What can AQE change at runtime?"]
related: ["articles:spark/partitions-shuffles-skew", "interview-questions:pyspark/broadcast-join"]
sources:
  - { label: "Apache Spark documentation: Skew join optimization", url: "https://spark.apache.org/docs/latest/sql-performance-tuning.html#optimizing-skew-join" }
versionContext: "Adaptive skew handling depends on Spark version and configuration; inspect the executed plan and metrics."
---

## A targeted response

Start with evidence: compare the slowest tasks with the median, examine shuffle read and spill, and find whether one or a few join keys dominate. If the skew comes from a hot key, salting can split its work across partitions and replicate matching rows on the other side. That is safe only when the final aggregation removes the salt correctly and the additional replication is affordable.

Before salting, test simpler changes. Filtering unused rows and columns reduces data volume. Pre-aggregation can shrink one side if the operation is algebraically valid. A broadcast join can avoid shuffling the large side when the other side is genuinely small enough to fit in executor memory. Adaptive Query Execution can split skewed partitions for supported plans, but should be verified in the executed plan.

## Common mistakes

1. Increasing partition count without addressing a single hot key.
2. Broadcasting a table based only on its compressed file size.
3. Salting without removing the salt correctly from downstream grouping.
