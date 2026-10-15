---
publishedDate: "2026-10-15"
updatedDate: "2026-10-15"
reviewedDate: "2026-10-15"
title: "What does a Structured Streaming checkpoint store?"
seoTitle: "Spark Streaming Checkpoints: Interview Answer"
description: "Streaming checkpoints persist query progress and state for recovery; stable checkpoint paths and compatible query changes are essential."
technology: ["pyspark", "spark"]
topic: ["structured-streaming", "recovery"]
difficulty: "Medium"
questionType: ["conceptual", "architecture"]
estimatedMinutes: 8
interviewRelevance: "High"
shortAnswer: "A Structured Streaming checkpoint stores progress and state information Spark needs to recover a streaming query, including committed source offsets and state for stateful operations. The checkpoint location must be stable and unique to the query. Reusing or deleting it changes recovery behavior, while changing the query in ways incompatible with stored state can prevent a restart. A checkpoint does not by itself make an external sink idempotent or guarantee end-to-end exactly-once behavior; sink semantics matter too."
followUps: ["Can two queries share a checkpoint directory?", "What is the role of a watermark?", "How do sink semantics affect delivery guarantees?", "What happens when state schema changes?"]
related: ["system-designs:streaming-etl-with-kafka-spark", "interview-questions:data-engineering/exactly-once-vs-effectively-once"]
sources:
  - { label: "Apache Spark documentation: Structured Streaming", url: "https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html" }
versionContext: "Checkpoint compatibility depends on query changes, source, sink and Spark version; test recovery before production changes."
---

## Recovery and state

A checkpoint lets Spark resume progress instead of treating every restart as a new query. For stateful operations such as aggregations or deduplication, it also holds state needed to continue processing. Keep each production query's checkpoint location stable across routine restarts, and never point separate logical queries at the same path.

A watermark bounds how long the query retains state for handling late event-time data; it is not a universal guarantee that all later events will be accepted. Choose its delay from the lateness contract and state-size budget. Checkpointing coordinates query recovery, while the sink must still handle retries or duplicate effects correctly for the desired end-to-end guarantee.

## Common mistakes

1. Deleting a checkpoint to clear an error without considering replay and duplicate writes.
2. Assuming a checkpoint protects the downstream database from duplicate side effects.
3. Changing stateful query logic without validating checkpoint compatibility.
