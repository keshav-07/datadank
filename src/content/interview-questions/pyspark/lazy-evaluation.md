---
publishedDate: "2026-10-15"
updatedDate: "2026-10-15"
reviewedDate: "2026-10-15"
title: "What does lazy evaluation mean in PySpark?"
seoTitle: "PySpark Lazy Evaluation: Interview Answer"
description: "PySpark transformations build a logical plan that Spark optimizes; actions trigger execution, so inspect plans and avoid repeated actions."
technology: ["pyspark", "spark"]
topic: ["execution", "fundamentals"]
difficulty: "Easy"
questionType: ["conceptual"]
estimatedMinutes: 6
interviewRelevance: "High"
shortAnswer: "PySpark DataFrame transformations usually describe a computation and return a new DataFrame without immediately reading all data. Spark builds and optimizes a plan, then an action such as `count()`, `collect()` or a write triggers execution. Laziness allows Spark SQL to combine operations and choose a physical plan, but it also means errors and cost often appear at the action. Repeated actions can recompute the same lineage unless the result is persisted or materialized."
followUps: ["Which operations are actions?", "Why can collect() be dangerous?", "How can you inspect the plan before execution?"]
related: ["articles:pyspark/transformations-vs-actions", "articles:spark/apache-spark-architecture"]
sources:
  - { label: "Apache Spark documentation: DataFrame API", url: "https://spark.apache.org/docs/latest/sql-programming-guide.html" }
versionContext: "Examples describe the DataFrame API in current Apache Spark; optimizer decisions depend on version, source and configuration."
---

## Example

```python
recent = events.filter("event_date >= DATE '2026-01-01'")
by_day = recent.groupBy("event_date").count()

by_day.explain()            # inspect a plan; does not collect all rows
by_day.write.mode("overwrite").parquet(output_path)  # action: executes the plan
```

The filter and aggregation are transformations. The write is an action and starts the job. A `collect()` action also triggers execution, but transfers every returned row to the driver; it is appropriate only when the result is known to be small.

## Debugging implications

An exception may surface at an action even when the problematic transformation was defined much earlier. Use `explain()` to inspect the plan and the Spark UI to inspect stages, tasks, shuffle and spill. If several actions reuse an expensive intermediate result, persistence may help, but benchmark the storage and recomputation trade-off.

## Common mistakes

1. Assuming a DataFrame transformation has already read or validated the data.
2. Calling `count()` repeatedly for logging and then running the same computation again.
3. Using `collect()` for a large production dataset.
