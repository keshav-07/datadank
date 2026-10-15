---
publishedDate: "2026-10-15"
updatedDate: "2026-10-15"
reviewedDate: "2026-10-15"
title: "How would you design role-based access control in Snowflake?"
seoTitle: "Snowflake RBAC: Interview Answer"
description: "Design Snowflake RBAC with least-privilege functional roles, ownership boundaries, role inheritance and grants to managed identities."
technology: ["snowflake", "data-engineering"]
topic: ["security", "access-control"]
difficulty: "Medium"
questionType: ["architecture", "scenario"]
estimatedMinutes: 8
interviewRelevance: "High"
shortAnswer: "I separate job functions into roles, grant only the required database, schema, warehouse and object privileges to those roles, and assign roles to users or service identities through an approved hierarchy. Object ownership should be controlled and distinct from routine read or write access. I use managed roles rather than direct user grants where possible, review inheritance for accidental privilege expansion, and test access with the same role the workload will use. Masking and row policies may be needed in addition to object grants."
followUps: ["How does role inheritance work?", "What does a future grant cover?", "How do you keep a service user least-privileged?", "When are masking policies needed?"]
related: ["interview-questions:databricks/unity-catalog", "articles:snowflake/security-access-control"]
sources:
  - { label: "Snowflake documentation: Access control overview", url: "https://docs.snowflake.com/en/user-guide/security-access-control-overview" }
versionContext: "Snowflake role and grant behavior is account-configuration dependent; validate the hierarchy and managed access settings in the target account."
---

## A practical role hierarchy

```text
functional role (ANALYST_READ) -> object privileges
user or service role          -> inherits the functional role
```

Separate the ability to use a warehouse from privileges on databases and tables. A role may need `USAGE` on a warehouse, database and schema plus `SELECT` on a table. Grant only the required actions, and make ownership or grant-management privileges explicit. For service workloads, use non-human identities and rotate credentials through the approved secret process.

Row access and column masking solve a different problem from table-level `SELECT`: they control which rows or values are visible after access is granted. Add those controls when the data classification and consumer policy require them.

## Common mistakes

1. Giving broad administrator roles to day-to-day users or jobs.
2. Confusing warehouse `USAGE` with permission to query a table.
3. Granting directly to many users, making audits and revocation difficult.
