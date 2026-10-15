---
publishedDate: "2026-10-15"
updatedDate: "2026-10-15"
reviewedDate: "2026-10-15"
title: "How do dbt macros and Jinja work?"
seoTitle: "dbt Macros and Jinja: Interview Answer"
description: "Understand dbt macros and Jinja: compile-time SQL generation, reuse, adapter dispatch, and how to avoid abstractions that hide business logic."
technology: ["dbt", "sql"]
topic: ["jinja", "macros"]
difficulty: "Medium"
questionType: ["conceptual", "coding"]
estimatedMinutes: 6
interviewRelevance: "Medium"
shortAnswer: "dbt uses Jinja to render project files before SQL is sent to the warehouse. Macros are reusable Jinja functions that can generate SQL, such as a consistent date spine or safe identifier logic. They are useful when the same non-business-specific pattern is repeated, but a macro that hides important business rules can make compiled SQL difficult to review. Inspect compiled output, keep inputs explicit, and use adapter-aware dispatch when SQL syntax differs between platforms."
followUps: ["What is the difference between a macro and a model?", "When should a macro use adapter dispatch?", "How do you debug a compiled model?"]
related: ["cheat-sheets:dbt", "interview-questions:sql/optimize-slow-query"]
sources:
  - { label: "dbt documentation: Jinja and macros", url: "https://docs.getdbt.com/docs/build/jinja-macros" }
  - { label: "dbt documentation: Adapter dispatch", url: "https://docs.getdbt.com/reference/dbt-jinja-functions/dispatch" }
versionContext: "Macro SQL is adapter- and runtime-sensitive; example is conceptual and not executed."
---

## Example

```sql
{% macro cents_to_currency(column_name, scale=2) %}
  ({{ column_name }} / 100.0)::numeric(16, {{ scale }})
{% endmacro %}
```

Calling `{{ cents_to_currency('amount_cents') }}` expands into SQL before execution. The cast shown is PostgreSQL-style; a cross-platform project should not assume it works unchanged on every adapter. Adapter dispatch can route a macro call to platform-specific implementations while keeping the model call stable.

## Good abstraction boundaries

- Extract repeated technical patterns that have one well-understood meaning.
- Keep business rules visible in model SQL or clearly named macros.
- Test generated SQL on each supported adapter.
- Review `target/compiled` output when debugging template behavior.

## Common mistakes

1. Adding a macro for a pattern used once, making the model harder to read.
2. Generating identifiers from unvalidated user input.
3. Assuming Jinja runs inside the warehouse; it renders before the SQL is submitted.
