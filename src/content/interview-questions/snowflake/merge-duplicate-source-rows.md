---
publishedDate: "2026-10-15"
updatedDate: "2026-10-15"
reviewedDate: "2026-10-15"
title: "What can go wrong when MERGE source keys are duplicated in Snowflake?"
seoTitle: "Snowflake MERGE Duplicate Rows: Interview Answer"
description: "Deduplicate a Snowflake MERGE source to one deterministic row per key; multiple matches can be nondeterministic or cause duplicate inserts."
technology: ["snowflake", "sql"]
topic: ["merge", "deduplication"]
difficulty: "Hard"
questionType: ["debugging", "coding"]
estimatedMinutes: 8
interviewRelevance: "High"
shortAnswer: "Before a Snowflake `MERGE`, I ensure the source has at most one deterministic row for each target key. Multiple source rows matching one target row can make updates ambiguous or fail, depending on the matched clauses and session settings; unmatched duplicate source rows can also insert duplicate target keys. I rank source versions by a stable timestamp and tie-breaker, keep one row per key, and test the key uniqueness assumption before executing the merge."
followUps: ["How do you choose a deterministic tie-breaker?", "What does ERROR_ON_NONDETERMINISTIC_MERGE control?", "Does a unique_key declaration enforce uniqueness?"]
related: ["interview-questions:sql/remove-duplicate-records", "interview-questions:sql/scd-type-2-merge-sql"]
sources:
  - { label: "Snowflake documentation: MERGE", url: "https://docs.snowflake.com/en/sql-reference/sql/merge" }
versionContext: "Nondeterministic MERGE behavior and session parameters should be checked against the account's Snowflake documentation and settings."
---

## Make the source unique first

```sql
with ranked_source as (
  select s.*,
         row_number() over (
           partition by order_id
           order by source_updated_at desc, ingest_sequence desc
         ) as rn
  from staging_orders s
), one_row_per_order as (
  select order_id, status, source_updated_at
  from ranked_source
  where rn = 1
)
merge into target_orders t
using one_row_per_order s
  on t.order_id = s.order_id
when matched and s.source_updated_at > t.source_updated_at then
  update set status = s.status, source_updated_at = s.source_updated_at
when not matched then
  insert (order_id, status, source_updated_at)
  values (s.order_id, s.status, s.source_updated_at);
```

The tie-breaker must be stable and meaningful; otherwise two rows with the same timestamp can produce an arbitrary winner. Add a pre-merge assertion that the source key count equals its distinct key count. Also decide how deletes and out-of-order updates are represented.

## Common mistakes

1. Assuming `MERGE` automatically collapses repeated source keys.
2. Ranking only by a timestamp that is not unique.
3. Relying on a session parameter instead of fixing ambiguous source data.
