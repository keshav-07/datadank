---
publishedDate: "2026-10-15"
updatedDate: "2026-10-15"
reviewedDate: "2026-10-15"
title: "When should you use repartition() versus coalesce()?"
seoTitle: "PySpark repartition vs coalesce: Interview"
description: "Use repartition to redistribute data with a shuffle; use coalesce mainly to reduce partitions with less data movement, while checking balance."
technology: ["pyspark", "spark"]
topic: ["partitions", "performance"]
difficulty: "Medium"
questionType: ["conceptual", "optimization"]
estimatedMinutes: 7
interviewRelevance: "High"
shortAnswer: "`repartition()` redistributes records and normally triggers a shuffle, which can increase or decrease partition count and can repartition by columns. `coalesce()` is commonly used to reduce partitions while avoiding a full shuffle, but can leave uneven partitions or limit parallelism. Use repartition when you need a more balanced distribution or key-based partitioning; use coalesce when reducing output partitions after filtering and the existing distribution is acceptable. Measure task sizes and output file sizes."
followUps: ["Can coalesce increase the number of partitions?", "Why might one output partition become a bottleneck?", "How do partition counts affect output files?"]
related: ["articles:spark/partitions-shuffles-skew", "interview-questions:pyspark/shuffle-and-stages"]
sources:
  - { label: "Apache Spark documentation: DataFrame repartition API", url: "https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.repartition.html" }
  - { label: "Apache Spark documentation: DataFrame coalesce API", url: "https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.coalesce.html" }
versionContext: "DataFrame API behavior described in Apache Spark; RDD coalesce has related but distinct details."
---

## Practical choices

```python
# Spread work by a join key; expect redistribution.
balanced = events.repartition(200, "customer_id")

# Reduce output parallelism after filtering if current partitions are sufficiently balanced.
smaller_output = filtered.coalesce(20)
```

Repartitioning can improve balance but adds network and disk work. Coalescing can reduce small output files, but a large input collapsed into too few partitions can create long tasks, driver pressure at collection time, or a single oversized file. Output file count is also affected by writer behavior and partition columns.

## Common mistakes

1. Coalescing a large dataset to one partition just to get one file.
2. Repartitioning without a reason, adding a costly shuffle to a small workload.
3. Assuming equal partition counts mean equal amounts of data.
