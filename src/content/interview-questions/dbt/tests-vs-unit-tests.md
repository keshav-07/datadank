---
publishedDate: "2026-10-15"
updatedDate: "2026-10-15"
reviewedDate: "2026-10-15"
title: "What is the difference between dbt data tests and unit tests?"
seoTitle: "dbt Data Tests vs Unit Tests: Interview Answer"
description: "Explain dbt data tests and unit tests: what each validates, when they run, and how they complement model-level and production-data checks."
technology: ["dbt", "data-engineering"]
topic: ["testing", "data-quality"]
difficulty: "Medium"
questionType: ["conceptual", "scenario"]
estimatedMinutes: 7
interviewRelevance: "High"
shortAnswer: "A dbt data test checks properties of built data, such as uniqueness, non-null values or a relationship between keys; a failing test returns offending rows. A unit test checks a model's SQL logic against small, declared input rows and expected output rows, which is useful for branches and edge cases without scanning production-sized inputs. They answer different questions: unit tests exercise transformation logic, while data tests check actual output data. Neither replaces source freshness, reconciliation or end-to-end operational monitoring."
followUps: ["When would you use a singular test?", "How do you test a late-arriving record rule?", "Do tests run automatically after every model?", "How would you test warehouse-specific SQL?"]
related: ["interview-questions:data-engineering/test-a-data-pipeline", "interview-questions:data-engineering/data-quality-checks", "cheat-sheets:dbt"]
sources:
  - { label: "dbt documentation: Data tests", url: "https://docs.getdbt.com/docs/build/data-tests" }
  - { label: "dbt documentation: Unit tests", url: "https://docs.getdbt.com/docs/build/unit-tests" }
versionContext: "dbt unit-test availability and supported SQL patterns vary by dbt version and adapter; confirm against the deployed version."
---

## Comparison

| Check | Example | Detects |
|---|---|---|
| Data test | `unique` on `order_id` | Duplicate keys in the built relation |
| Data test | `relationships` from order customer to customer key | Orphaned references |
| Unit test | Feed null and valid amounts to a revenue calculation | Incorrect branching or expression logic |
| Source freshness | Compare the latest loaded timestamp with a threshold | A source that has stopped arriving on time |

In dbt, a data test conventionally returns failing records; no rows means the assertion passed. A unit test supplies fixtures and an expected result for a model's logic. The latter can catch a bug before a full model build, while the former checks the data that was actually produced.

## Practical approach

Use unit tests for transformation branches that are easy to get wrong, such as null handling or status mapping. Use data tests for durable invariants such as key uniqueness and accepted values. Choose severity and failure behavior based on whether publishing incorrect data is worse than delaying a dataset. Add a response owner for failures so a red test is actionable.

## Common mistakes

1. Assuming a passing unit test proves production data is fresh or complete.
2. Writing only tests that restate the SQL implementation.
3. Treating every anomaly as a hard failure without considering consumer impact.
