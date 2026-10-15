---
publishedDate: "2026-10-15"
updatedDate: "2026-10-15"
reviewedDate: "2026-10-15"
title: "What is the difference between dbt run and dbt build?"
seoTitle: "dbt run vs dbt build: Interview Answer"
description: "Compare dbt run and dbt build, including how build sequences models, tests, seeds and snapshots for an integrated validation workflow."
technology: ["dbt"]
topic: ["commands", "deployment"]
difficulty: "Easy"
questionType: ["conceptual"]
estimatedMinutes: 5
interviewRelevance: "Medium"
shortAnswer: "`dbt run` builds selected models but does not by itself run the full set of associated tests and other resource types. `dbt build` is a broader DAG-aware command that can build and test selected resources, and includes supported seeds and snapshots in dependency order. I use `build` for an integrated validation/deployment selection when I want upstream tests to gate downstream resources; I use `run` when I specifically intend to execute model nodes. Exact command behavior should be checked for the installed dbt version."
followUps: ["Does dbt build run tests before or after models?", "How do selectors affect either command?", "When would you run dbt test separately?"]
related: ["cheat-sheets:dbt", "interview-questions:data-engineering/cicd-for-data-pipelines"]
sources:
  - { label: "dbt documentation: dbt build", url: "https://docs.getdbt.com/reference/commands/build" }
  - { label: "dbt documentation: dbt run", url: "https://docs.getdbt.com/reference/commands/run" }
versionContext: "Command resource support and ordering should be verified against the version and runner used by the project."
---

## Why this distinction matters

The command affects whether tests are part of the selected execution workflow. A model build can succeed while its output violates a data assertion. Running a build that includes the relevant tests allows failures to be surfaced in the same DAG-aware invocation and can prevent dependent resources from proceeding according to the command's execution rules.

Before relying on a CI command, inspect the selection syntax and confirm that it includes the intended models, tests and upstream dependencies. A command name alone is not a test strategy.

## Common mistakes

1. Assuming `dbt run` also executes every data test.
2. Assuming `dbt build` selects every project resource regardless of selector.
3. Not checking how a failure affects downstream nodes in the chosen command and version.
