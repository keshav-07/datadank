---
publishedDate: "2026-10-15"
updatedDate: "2026-10-15"
reviewedDate: "2026-10-15"
title: "How do dbt materializations differ?"
seoTitle: "dbt Materializations: Interview Answer"
description: "Compare dbt view, table, incremental and ephemeral materializations, including their warehouse cost, freshness and dependency trade-offs."
technology: ["dbt", "sql"]
topic: ["models", "materializations"]
difficulty: "Medium"
questionType: ["conceptual", "scenario"]
estimatedMinutes: 7
interviewRelevance: "High"
shortAnswer: "A view stores a query definition and computes results when queried; a table stores the built result; an incremental model updates a previously built relation using a defined change strategy; and an ephemeral model is compiled into downstream SQL rather than created as a warehouse relation. Choose based on data volume, query cost, freshness, reuse and platform behavior. Materialization is a physical implementation choice, so validate its semantics with the project adapter and warehouse."
followUps: ["When can a view be cheaper than a table?", "What does an ephemeral model do to compiled SQL?", "What happens when an incremental model's logic changes?"]
related: ["cheat-sheets:dbt", "interview-questions:dbt/incremental-models"]
sources:
  - { label: "dbt documentation: Materializations", url: "https://docs.getdbt.com/docs/build/materializations" }
versionContext: "Built-in materialization behavior is adapter-dependent; custom materializations can change these details."
---

## Choosing a materialization

| Materialization | Stored relation? | Typical fit | Main trade-off |
|---|---|---|---|
| View | Yes, as a query definition | Lightweight logic and fresh source reads | Recomputes work when queried; nested views can become expensive |
| Table | Yes, as stored results | Reused or expensive transformations | Rebuild cost and data staleness between runs |
| Incremental | Yes, updated from prior state | Large datasets where only a slice changes | More state, key, late-data and correction logic to own |
| Ephemeral | No separate relation | Small reusable SQL fragments | Inlines into dependents, which can create large SQL |

For a slowly changing but small dimension, a table may simplify consumers. For a large event fact table, incremental processing may reduce repeated compute if the change signal and merge semantics are reliable. A view can be appropriate when the source is small and must be current. These are workload decisions, not universal rules.

## Common mistakes

1. Making every model incremental without defining how corrections and deletes arrive.
2. Choosing a table for freshness without scheduling a rebuild often enough.
3. Using ephemeral models for large, repeatedly expanded transformations.
