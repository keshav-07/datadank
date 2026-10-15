---
publishedDate: "2026-10-15"
updatedDate: "2026-10-15"
reviewedDate: "2026-10-15"
title: "What is Photon, and when can it improve a Databricks workload?"
seoTitle: "Databricks Photon Engine: Interview Answer"
description: "Photon is Databricks' vectorized query engine for supported workloads; assess operator coverage, runtime settings and end-to-end cost."
technology: ["databricks", "spark"]
topic: ["compute", "performance"]
difficulty: "Medium"
questionType: ["conceptual", "optimization"]
estimatedMinutes: 6
interviewRelevance: "Medium"
shortAnswer: "Photon is Databricks' native vectorized query engine that accelerates supported SQL and DataFrame operations on Databricks compute. It can improve throughput for compatible workloads, but it is not a guarantee that every Python operation, custom UDF or bottleneck becomes faster. I would inspect the query profile and operator support, compare runtime and cost on representative data, and first address avoidable scans, skew and excessive shuffles."
followUps: ["Why can a Python UDF limit acceleration?", "How would you measure whether Photon helped?", "Does it replace Spark's optimizer?"]
related: ["interview-questions:pyspark/avoid-python-udfs", "interview-questions:databricks/optimize-vacuum"]
sources:
  - { label: "Databricks documentation: Photon overview", url: "https://docs.databricks.com/aws/en/compute/photon" }
versionContext: "Photon availability and supported operations depend on compute type, runtime and workspace configuration."
---

## A sensible evaluation

Use a representative query and dataset. Record the baseline runtime, bytes scanned, shuffle volume, spill, and compute cost. Then compare the same workload with Photon on compatible compute. Confirm that the query plan is using the expected engine and that the improvement persists across more than one run; cache state and concurrent workload can distort a single timing.

An engine change will not repair poor data layout or a skewed join automatically. A query dominated by Python-side work may see limited benefit. Explain the measured result and cost trade-off rather than claiming a blanket speedup.

## Common mistakes

1. Attributing every runtime difference to the execution engine without controlling for cache and workload.
2. Assuming all Spark APIs and UDFs have identical acceleration support.
3. Benchmarking on a small sample that does not reproduce the production bottleneck.
