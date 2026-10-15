---
publishedDate: "2026-10-15"
updatedDate: "2026-10-15"
reviewedDate: "2026-10-15"
title: "How do you monitor source freshness in dbt?"
seoTitle: "dbt Source Freshness: Interview Answer"
description: "Use dbt source freshness to compare source timestamps with defined thresholds, then schedule, alert and assign ownership for stale inputs."
technology: ["dbt", "data-engineering"]
topic: ["freshness", "observability"]
difficulty: "Medium"
questionType: ["conceptual", "scenario"]
estimatedMinutes: 6
interviewRelevance: "High"
shortAnswer: "Declare a reliable loaded-at field and freshness thresholds on the source, run the freshness check on a schedule, and alert when the latest observed data exceeds the warning or error age. Freshness checks detect stale inputs; they do not prove that all expected records arrived or that the data is correct. I would keep the monitor independent enough to alert when the transformation job fails, define an owner and response target, and track freshness history against the consumer SLA."
followUps: ["What timestamp should be used?", "How do you choose the threshold?", "How is freshness different from a row-count test?"]
related: ["interview-questions:data-engineering/monitor-pipeline-freshness-slas", "cheat-sheets:dbt"]
sources:
  - { label: "dbt documentation: Source freshness", url: "https://docs.getdbt.com/docs/deploy/source-freshness" }
versionContext: "Source freshness command options and configuration depend on dbt version and adapter support."
---

## What the check tells you

Given a configured loaded-at expression and thresholds, dbt compares the newest observed timestamp with the current time. A warning threshold can signal degradation before an error threshold breaches the data contract. For example, a table expected hourly might warn after 90 minutes and error after 3 hours; choose limits from the actual arrival pattern and consumer need rather than copying these example values.

Freshness is only as trustworthy as the timestamp. A source-generated event time can be old even when ingestion is healthy; an ingestion timestamp says when the platform received the row but may miss a totally empty load. Pair freshness with volume, completeness and reconciliation checks where needed.

## Common mistakes

1. Measuring event time when the requirement is ingestion arrival time.
2. Treating a freshness pass as proof of complete or valid data.
3. Running checks only inside the pipeline whose failure they are meant to detect.
