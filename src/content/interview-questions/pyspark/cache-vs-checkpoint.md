---
publishedDate: "2026-10-15"
updatedDate: "2026-10-15"
reviewedDate: "2026-10-15"
title: "When should you cache or checkpoint a PySpark DataFrame?"
seoTitle: "PySpark Cache vs Checkpoint: Interview Answer"
description: "Cache reused computations when recomputation costs more than storage; checkpoint to truncate lineage or provide a durable recovery boundary."
technology: ["pyspark", "spark"]
topic: ["reliability", "performance"]
difficulty: "Medium"
questionType: ["conceptual", "optimization"]
estimatedMinutes: 7
interviewRelevance: "Medium"
shortAnswer: "Cache or persist a DataFrame when several actions reuse an expensive computation and the saved recomputation is worth the storage and memory pressure. A checkpoint materializes data and truncates the lineage graph, which can help with very long lineage or streaming recovery patterns; checkpoint semantics and storage depend on the API and mode. Neither is automatically faster: cache is lost with executor or application loss, and checkpointing adds I/O. Unpersist data when it is no longer needed."
followUps: ["What is the difference between local and reliable checkpointing?", "Why can caching slow a job down?", "Does cache trigger execution immediately?"]
related: ["articles:spark/caching-broadcast-accumulators", "interview-questions:pyspark/lazy-evaluation"]
sources:
  - { label: "Apache Spark documentation: DataFrame persist API", url: "https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.persist.html" }
  - { label: "Apache Spark documentation: DataFrame checkpoint API", url: "https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.checkpoint.html" }
versionContext: "Persistence defaults and checkpoint behavior are API- and runtime-dependent; set a checkpoint directory for reliable checkpointing where required."
---

## Decision example

If an expensive parsed dataset feeds three separate aggregations, persistence may avoid evaluating the same upstream parse three times:

```python
parsed = raw.selectExpr("... as event_id", "... as event_time").persist()
parsed.count()  # materialize only if the planned reuse justifies it
daily = parsed.groupBy("event_time").count()
by_id = parsed.groupBy("event_id").count()
parsed.unpersist()
```

The `count()` itself costs a job, so caching is useful only if later work reuses the materialized data enough to offset that cost. If a chain of iterative transformations creates extremely long lineage, checkpointing can cut it, but it requires storage and a recovery decision.

## Common mistakes

1. Caching every intermediate DataFrame and exhausting executor memory.
2. Assuming `cache()` immediately computes the data.
3. Using cache as a durable recovery mechanism.
