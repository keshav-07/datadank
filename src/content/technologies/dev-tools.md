---
title: "Developer Tools for Data Engineers"
shortName: "Dev tools"
description: "The command line, shell scripts, Git, Python environments, Docker and repo automation: the day-one toolkit every data pipeline job assumes you already know."
group: foundations
order: 14
keyFacts: ["Pipes, grep, awk and jq answer most questions about a file before you write any Python", "Git history, pull requests and a clean .gitignore keep data and secrets out of the repository", "Pinned dependencies and container images make a pipeline run the same on a laptop, in CI and in production"]
whatToLearnFirst: ["articles:dev-tools/dev-tools-for-data-engineers", "articles:dev-tools/command-line-for-data-work", "articles:dev-tools/git-for-data-engineers"]
relatedTechnologies: ["python", "sql", "airflow"]
monogram: "CLI"
lessons: ["articles:dev-tools/dev-tools-for-data-engineers", "articles:dev-tools/command-line-for-data-work", "articles:dev-tools/git-for-data-engineers", "articles:dev-tools/shell-scripting-for-pipelines", "articles:dev-tools/python-environments-packaging", "articles:dev-tools/docker-for-data-engineers", "articles:dev-tools/makefiles-pre-commit-data-repos"]
updatedDate: 2026-10-16
---

Before SQL or Spark, a Data Engineer needs a working toolkit: a terminal to inspect and move files, Git to version and review code, a reproducible Python environment, and containers to run the same pipeline everywhere. Interviewers rarely ask about these directly, but take-home tasks, pairing sessions and the first week of any job assume them.

This course starts with the command line and Git, then builds up to robust shell scripts, Python packaging, Docker and Docker Compose, and finishes with Makefiles and pre-commit hooks that tie a data repository together. Every example was run in a terminal and shows its real output.
