---
publishedDate: "2026-10-15"
updatedDate: "2026-10-15"
reviewedDate: "2026-10-15"
title: "How do Snowflake streams track table changes?"
seoTitle: "Snowflake Streams: Interview Answer"
description: "A Snowflake stream records a change-tracking offset over a source object; consuming transactions advance it, so retention and task design matter."
technology: ["snowflake", "data-engineering"]
topic: ["streams", "change-data-capture"]
difficulty: "Medium"
questionType: ["conceptual", "architecture"]
estimatedMinutes: 8
interviewRelevance: "High"
shortAnswer: "A Snowflake stream is a change-tracking object that exposes row-level changes to a source table between an offset and the current table version. It is metadata over change tracking, not a separate copy of the changed rows. A consuming DML transaction advances the stream offset when that transaction commits, which allows a task to process a window of changes. Retention, stale-stream risk, transactional consumption and idempotent downstream writes must be part of the design."
followUps: ["Does selecting from a stream advance the offset?", "What happens when a stream becomes stale?", "How do streams and tasks fit together?", "How are deletes represented?"]
related: ["interview-questions:snowflake/streams-tasks-incremental-pipeline", "articles:snowflake/streams-and-tasks"]
sources:
  - { label: "Snowflake documentation: Introduction to streams", url: "https://docs.snowflake.com/en/user-guide/streams-intro" }
versionContext: "Stream behavior depends on source support, retention and transaction boundaries; confirm current platform limits."
---

## Offset behavior

A stream exposes changes from its current offset to a newer table version. Merely querying the stream does not consume it. When a DML transaction uses the stream and commits, the offset advances to the transaction's start point, so a subsequent consumer sees the next interval. A rollback does not commit that consumption.

This makes transaction design important: the change rows and target update should be processed in a controlled transaction where supported, and the target operation should tolerate retries. If consumers do not advance the stream before its change-tracking history expires, the stream can become stale and need recreation or a full reconciliation.

## Common mistakes

1. Treating a stream as a durable queue that retains changes forever.
2. Assuming a plain read consumes the stream.
3. Advancing a stream without ensuring the downstream write succeeded.
