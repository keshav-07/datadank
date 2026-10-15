---
publishedDate: "2026-10-15"
updatedDate: "2026-10-15"
reviewedDate: "2026-10-15"
title: "When would you use a dbt snapshot?"
seoTitle: "dbt Snapshots: Interview Answer"
description: "dbt snapshots preserve observed changes to mutable source rows; learn when they fit, how timestamp and check strategies differ, and their limits."
technology: ["dbt", "data-warehousing"]
topic: ["snapshots", "history"]
difficulty: "Medium"
questionType: ["conceptual", "scenario"]
estimatedMinutes: 7
interviewRelevance: "High"
shortAnswer: "Use a dbt snapshot when you need to preserve historical versions of rows from a mutable source that does not already retain history. A timestamp strategy detects changes from a reliable `updated_at` column; a check strategy compares selected columns when no dependable update timestamp exists. Snapshots record what the job observed at run time, so they can miss intermediate states between runs and do not automatically provide source-log CDC semantics. For exact change history or deletes, evaluate CDC or source-side history."
followUps: ["How do timestamp and check strategies differ?", "Can a snapshot recover a change that happened and was reverted between runs?", "How do snapshots relate to SCD Type 2?"]
related: ["articles:data-warehousing/slowly-changing-dimensions", "system-designs:slowly-changing-dimension-framework", "cheat-sheets:dbt"]
sources:
  - { label: "dbt documentation: Snapshots", url: "https://docs.getdbt.com/docs/build/snapshots" }
versionContext: "Snapshot configuration syntax has changed across dbt releases; verify the YAML form supported by the project's version."
---

## What a snapshot records

A snapshot compares the current source relation with the prior snapshot state and stores changed row versions with validity metadata. That can support a Type 2 style history for dimensions such as customer address or account status.

| Strategy | Detection signal | Main assumption |
|---|---|---|
| Timestamp | Source `updated_at` | Every relevant change updates the timestamp reliably |
| Check | Comparison of selected columns | The checked columns include every change that matters |

Snapshots are scheduled observations. If a source value changes from A to B and back to A between two snapshot runs, a snapshot may never observe B. If every transition must be retained, capture changes at the source with CDC or an audit log.

## Common mistakes

1. Calling a snapshot a complete transaction log.
2. Choosing timestamp strategy when the source timestamp is not consistently maintained.
3. Treating snapshot history as a substitute for defining deletion and retention policies.
