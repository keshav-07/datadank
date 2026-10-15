---
publishedDate: "2026-10-15"
updatedDate: "2026-10-15"
reviewedDate: "2026-10-15"
title: "What do OPTIMIZE and VACUUM do for a Delta table?"
seoTitle: "Delta Lake OPTIMIZE vs VACUUM: Interview Answer"
description: "OPTIMIZE rewrites data files for layout and file-size efficiency; VACUUM removes eligible unreferenced files and can limit historical recovery."
technology: ["databricks", "delta-lake"]
topic: ["performance", "maintenance"]
difficulty: "Medium"
questionType: ["conceptual", "optimization"]
estimatedMinutes: 7
interviewRelevance: "High"
shortAnswer: "`OPTIMIZE` rewrites a Delta table's data files to improve file layout and reduce small-file overhead; on supported configurations it can also apply clustering or data layout choices. `VACUUM` deletes eligible data files that are no longer referenced by the table after the retention threshold. Optimize is mainly about read efficiency; vacuum is about reclaiming storage and removing old files, with a direct trade-off against time-travel availability."
followUps: ["Does OPTIMIZE change logical table contents?", "What can VACUUM make unavailable?", "How would you decide whether optimization helped?"]
related: ["interview-questions:databricks/delta-lake-transactions", "interview-questions:databricks/unity-catalog"]
sources:
  - { label: "Databricks documentation: Optimize data file layout", url: "https://docs.databricks.com/aws/en/delta/optimize" }
  - { label: "Databricks documentation: VACUUM", url: "https://docs.databricks.com/aws/en/tables/operations/vacuum" }
versionContext: "Available layout commands and predictive optimization depend on table type, runtime and workspace configuration."
---

## Compare the operations

| Command | Main action | Why run it | Important consequence |
|---|---|---|---|
| `OPTIMIZE` | Rewrites files into a more useful physical layout | Reduce small-file overhead or improve data skipping | Consumes compute and writes a new table version |
| `VACUUM` | Deletes old files no longer needed by retained versions | Reduce storage and remove expired files | Older versions may no longer be queryable |

Measure whether a layout operation reduces scanned bytes, file counts or query latency for the actual workload. A maintenance job that runs on every table without workload evidence can cost more than it saves. Before reducing retention, check the maximum duration of readers and writers, recovery requirements, and any streaming consumers.

## Common mistakes

1. Treating `VACUUM` as a way to compact small files.
2. Treating `OPTIMIZE` as a backup or a guarantee of faster queries.
3. Disabling retention safety checks without validating long-running jobs and recovery needs.
