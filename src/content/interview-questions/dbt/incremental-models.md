---
publishedDate: "2026-10-15"
updatedDate: "2026-10-15"
reviewedDate: "2026-10-15"
title: "How do you build a reliable incremental model in dbt?"
seoTitle: "dbt Incremental Models: Interview Answer"
description: "Design reliable dbt incremental models with a stable change signal, unique key, bounded lookback, late-data handling and full-refresh plan."
technology: ["dbt", "sql"]
topic: ["incremental-models", "reliability"]
difficulty: "Hard"
questionType: ["architecture", "coding", "scenario"]
estimatedMinutes: 10
interviewRelevance: "High"
shortAnswer: "I first define how changes and deletes are identified, then make the incremental filter deterministic and safe to rerun. I choose a unique key and a platform-supported incremental strategy, deduplicate incoming records before the merge, and include a lookback or CDC mechanism for late changes. I compare incremental output with a full refresh on a sample, test key and freshness assumptions, and document when to use `--full-refresh`. Incremental syntax alone does not guarantee correctness."
followUps: ["How would you handle hard deletes?", "What does unique_key guarantee?", "How do you test an incremental model?", "When is a full refresh required?"]
related: ["interview-questions:data-engineering/design-incremental-load", "articles:etl-elt/incremental-loading-watermarks-backfills", "cheat-sheets:dbt"]
sources:
  - { label: "dbt documentation: Incremental models", url: "https://docs.getdbt.com/docs/build/incremental-models" }
versionContext: "`unique_key` and incremental strategies are adapter-specific in implementation; this example is illustrative, not warehouse-executed."
---

## Example pattern

```sql
{{ config(materialized='incremental', unique_key='order_id') }}

select order_id, customer_id, updated_at, status
from {{ source('app', 'orders') }}
{% if is_incremental() %}
where updated_at >= (
  select coalesce(dateadd(day, -2, max(updated_at)), '1900-01-01')
  from {{ this }}
)
{% endif %}
```

The SQL dialect shown is illustrative and must match the adapter. The overlap window intentionally rereads recent records so that late arrivals and corrections can be observed. The `unique_key` allows a supported strategy to match records, but it does not deduplicate multiple source rows with the same key; the model should select one deterministic latest row per key before materialization.

## Reliability checklist

- Does the change column update for every correction?
- How are hard deletes represented or reconciled?
- Can rerunning the same interval change the result?
- Does the merge strategy handle duplicate source keys?
- How far back can data arrive late?
- Can a full refresh rebuild the expected result?

## Common mistakes

1. Treating `unique_key` as a database uniqueness constraint.
2. Filtering only records newer than the target maximum without a late-data plan.
3. Forgetting that a changed model expression may require rebuilding old rows.
