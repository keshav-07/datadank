---
publishedDate: "2026-10-15"
updatedDate: "2026-10-15"
reviewedDate: "2026-10-15"
title: "When would you use Auto Loader in Databricks?"
seoTitle: "Databricks Auto Loader: Interview Answer"
description: "Use Auto Loader for incremental cloud-file ingestion at scale, with checkpointed discovery, schema handling and explicit malformed-data policy."
technology: ["databricks", "spark"]
topic: ["ingestion", "cloud-files"]
difficulty: "Medium"
questionType: ["conceptual", "scenario"]
estimatedMinutes: 7
interviewRelevance: "High"
shortAnswer: "I would consider Auto Loader when a Databricks pipeline must incrementally discover and ingest files arriving in cloud storage, especially when file counts make repeated directory listing expensive. It tracks progress with a checkpoint and supports schema inference and evolution modes. I would still define the file contract, checkpoint lifecycle, malformed-record behavior and replay plan; Auto Loader does not make upstream files valid or guarantee that every downstream business rule is correct."
followUps: ["What is the checkpoint used for?", "How do you handle a new column?", "What happens if a file is corrected in place?", "How is Auto Loader different from batch listing?"]
related: ["interview-questions:data-engineering/design-cdc-pipeline", "articles:databricks/auto-loader-declarative-pipelines"]
sources:
  - { label: "Databricks documentation: What is Auto Loader?", url: "https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/" }
versionContext: "Cloud provider, notification mode, schema mode and runtime determine exact configuration and supported behavior."
---

## What to discuss in a design

Auto Loader is a Structured Streaming source for files in supported cloud object storage. A checkpoint records stream progress so a restarted query can continue, while schema tracking records observed schema information. The ingestion design still needs an explicit policy for unknown fields, corrupt files, duplicate business records and replay.

For example, keep the source file path and arrival timestamp in the bronze data, route malformed records to a quarantine location, and use a stable checkpoint location for the production stream. Do not delete or reuse a checkpoint casually: it represents progress state, and changing it can cause data to be reprocessed or skipped depending on the source and query.

## Common mistakes

1. Assuming file discovery means exactly-once business processing in downstream tables.
2. Changing the checkpoint path to force a restart without assessing replay effects.
3. Enabling schema evolution without deciding which changes are safe to accept.
