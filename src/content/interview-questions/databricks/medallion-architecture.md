---
publishedDate: "2026-10-15"
updatedDate: "2026-10-15"
reviewedDate: "2026-10-15"
title: "How would you explain the medallion architecture in Databricks?"
seoTitle: "Databricks Medallion Architecture: Interview Answer"
description: "Explain bronze, silver and gold as progressively refined data layers, including quality ownership, replayability and why the pattern is not mandatory."
technology: ["databricks", "data-engineering"]
topic: ["architecture", "medallion"]
difficulty: "Easy"
questionType: ["conceptual", "architecture"]
estimatedMinutes: 6
interviewRelevance: "High"
shortAnswer: "Medallion architecture organizes data into layers with increasing refinement: bronze preserves ingested source data and ingestion context, silver cleans and conforms records across sources, and gold presents curated datasets for business use. It is a design pattern, not a required three-table product feature. The useful interview discussion is what each layer guarantees, how bad records are handled, and whether retaining raw inputs makes replay and audit possible."
followUps: ["What belongs in bronze versus silver?", "Would you always create all three layers?", "How do you handle quarantined records?"]
related: ["system-designs:scalable-lakehouse", "articles:databricks/databricks-for-data-engineers"]
sources:
  - { label: "Databricks documentation: Medallion architecture", url: "https://docs.databricks.com/aws/en/lakehouse/medallion" }
  - { label: "Databricks documentation: Delta Lake", url: "https://docs.databricks.com/aws/en/delta/" }
versionContext: "Medallion is an architectural pattern; layer names and guarantees are design choices for each platform."
---

## Layer contracts

| Layer | Typical responsibility | Useful contract |
|---|---|---|
| Bronze | Capture source records with ingestion metadata | Preserve source payload and arrival context; make reprocessing possible |
| Silver | Validate, deduplicate and standardize | Define keys, timestamps, schema and invalid-row handling |
| Gold | Publish domain or use-case datasets | Define business grain, measures, freshness and consumer access |

The boundaries should make failure and ownership easier to reason about. If the source is already clean and the workload is simple, additional layers may add latency and storage without adding value. If bronze is retained, define privacy controls and retention so replayability does not become uncontrolled duplication of sensitive data.

## Common mistakes

1. Treating the layer names as a substitute for data contracts.
2. Putting business metrics in bronze or silently changing their meaning between layers.
3. Retaining sensitive raw data indefinitely because it might be useful later.
