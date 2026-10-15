---
publishedDate: "2026-10-15"
updatedDate: "2026-10-15"
reviewedDate: "2026-10-15"
title: "How would you make dbt CI faster without missing affected models?"
seoTitle: "dbt CI State Selection: Interview Answer"
description: "Speed up dbt CI with a production manifest, state-aware selection, downstream expansion, isolated schemas and tests that gate deployment."
technology: ["dbt", "data-engineering"]
topic: ["ci-cd", "state-selection"]
difficulty: "Hard"
questionType: ["architecture", "scenario"]
estimatedMinutes: 9
interviewRelevance: "High"
shortAnswer: "Build a production manifest as the comparison state, then select modified resources and the downstream models affected by them, commonly with a state-aware selector such as `state:modified+`. Build in an isolated schema and use deferral to read unchanged parents from the trusted production state when supported. Run relevant tests, publish results to the pull request, and periodically verify the selection against a broader build. Pin the dbt version and ensure artifacts correspond to the same project revision and target."
followUps: ["Why include downstream nodes?", "What does defer do?", "How do you handle a change to a macro or source definition?", "What is a safe fallback if artifacts are stale?"]
related: ["interview-questions:data-engineering/cicd-for-data-pipelines", "cheat-sheets:dbt"]
sources:
  - { label: "dbt documentation: State selection", url: "https://docs.getdbt.com/reference/node-selection/methods" }
versionContext: "Selector behavior and deferral capabilities differ across dbt versions and runners; check the project's manifest and release track."
---

## A practical pull-request flow

1. Generate or retrieve a manifest from the deployed project revision.
2. Compare the pull request graph against that state and select modified nodes plus descendants that need rebuilding.
3. Build into a unique development schema so parallel pull requests cannot overwrite one another.
4. Defer unchanged upstream relations to the selected state when supported, instead of rebuilding the entire project.
5. Run the tests that protect changed outputs and show the selection and results to the reviewer.
6. Clean up the isolated schema after the check.

Fast selection is a risk trade-off: an incorrect or stale state artifact may omit work. Keep selector logic version-controlled, test graph-changing cases, and maintain a periodic full or broad integration build. Never run pull-request code with production write credentials.

## Common mistakes

1. Selecting modified models but forgetting dependent descendants.
2. Using a manifest from an unrelated commit, target or dbt version.
3. Building all pull requests into the same shared schema.
