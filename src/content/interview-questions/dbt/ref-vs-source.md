---
publishedDate: "2026-10-15"
updatedDate: "2026-10-15"
reviewedDate: "2026-10-15"
title: "What is the difference between ref() and source() in dbt?"
seoTitle: "dbt ref() vs source(): Interview Answer"
description: "Explain dbt ref() and source(): how ref creates model dependencies and environment-aware relations, while source declares upstream data."
technology: ["dbt", "sql"]
topic: ["dependencies", "sources"]
difficulty: "Easy"
questionType: ["conceptual", "coding"]
estimatedMinutes: 5
interviewRelevance: "High"
shortAnswer: "Use `ref()` to reference another dbt-managed model or seed; it both resolves the relation name for the active target and creates an edge in the project DAG. Use `source()` to reference an upstream relation declared as a source in YAML. Sources make ownership and freshness checks explicit and also resolve to the configured database, schema and identifier. Both are preferable to hard-coded relation names because dbt can understand dependencies and environment-specific naming."
followUps: ["What happens if you hard-code a table name?", "How do source freshness checks work?", "Can a source depend on a dbt model?"]
related: ["cheat-sheets:dbt", "articles:data-warehousing/data-warehousing-fundamentals"]
sources:
  - { label: "dbt documentation: ref function", url: "https://docs.getdbt.com/reference/dbt-jinja-functions/ref" }
  - { label: "dbt documentation: source function", url: "https://docs.getdbt.com/reference/dbt-jinja-functions/source" }
versionContext: "Core `ref()` and `source()` concepts; syntax should be checked against the installed dbt version."
---

## Example

Declare an upstream table in YAML:

```yaml
sources:
  - name: app
    schema: raw_app
    tables:
      - name: orders
```

Reference it from a staging model, then reference that model downstream:

```sql
-- models/staging/stg_orders.sql
select order_id, customer_id, ordered_at
from {{ source('app', 'orders') }}
```

```sql
-- models/marts/fct_orders.sql
select order_id, customer_id, ordered_at
from {{ ref('stg_orders') }}
```

The `ref()` call tells dbt that `fct_orders` depends on `stg_orders`; the source declaration gives dbt a named entry point to data loaded outside the project. A hard-coded `raw_app.orders` can run in one environment while silently bypassing dependency metadata and environment-specific configuration.

## Common mistakes

1. Using `source()` for a model built by the same dbt project.
2. Using `ref()` to represent an external raw table that has not been declared.
3. Assuming a source declaration itself ingests or validates the data.
