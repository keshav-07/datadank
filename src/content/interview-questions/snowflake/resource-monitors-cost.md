---
publishedDate: "2026-10-15"
updatedDate: "2026-10-15"
reviewedDate: "2026-10-15"
title: "How do you control and investigate Snowflake compute cost?"
seoTitle: "Snowflake Compute Cost Control: Interview Answer"
description: "Control Snowflake compute spend with workload separation, auto-suspend, resource monitors and query history, while protecting critical workloads."
technology: ["snowflake", "data-engineering"]
topic: ["cost", "warehouses"]
difficulty: "Medium"
questionType: ["scenario", "optimization"]
estimatedMinutes: 8
interviewRelevance: "High"
shortAnswer: "I start by attributing spend to warehouses and workloads, then reduce unnecessary runtime and inefficient scans: enable suitable auto-suspend, size for the actual query bottleneck, separate competing workload classes and inspect query history. Resource monitors can track warehouse credit use and alert or suspend according to configured thresholds, but they are not a substitute for budgets, account-level billing review or workload ownership. I validate changes against latency and queueing objectives so cost controls do not silently break critical refreshes."
followUps: ["When can a larger warehouse reduce cost?", "What does auto-suspend affect?", "How would you set a resource monitor threshold?", "How do you find a runaway query?"]
related: ["interview-questions:snowflake/virtual-warehouses", "articles:snowflake/cost-optimization"]
sources:
  - { label: "Snowflake documentation: Resource monitors", url: "https://docs.snowflake.com/en/user-guide/resource-monitors" }
  - { label: "Snowflake documentation: Query history", url: "https://docs.snowflake.com/en/user-guide/ui-history" }
versionContext: "Resource monitor scope, supported actions and billing behavior depend on account configuration and Snowflake edition."
---

## Investigate before tuning

Group usage by warehouse and time period, then identify idle runtime, repeated scans, spill, queuing and unusually expensive statements. A query slowed by spilling may benefit from a larger warehouse or query rewrite; a queue caused by concurrency may call for a multi-cluster policy or workload separation. Increasing size for every problem can increase credits without reducing the actual bottleneck.

Auto-suspend and auto-resume reduce idle compute but can affect cache reuse and the first query after resume. Resource monitors provide thresholds and alert or suspend actions for warehouses; they do not cover every account charge, so pair them with budget and usage reporting. Put an owner and escalation path on each limit.

## Common mistakes

1. Suspending a production warehouse without understanding scheduled refresh windows.
2. Assuming a resource monitor is a complete account budget control.
3. Cutting warehouse size without monitoring SLA impact and queuing.
