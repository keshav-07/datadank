---
title: "dbt for Data Engineers"
shortName: "dbt"
description: "dbt turns SQL select statements into tested, documented warehouse models. Learn models, materializations, incremental models, tests, snapshots and CI."
group: processing
order: 14
keyFacts: ["A model is a select statement; dbt writes the DDL and runs models in DAG order","ref() and source() build the lineage graph, so never hard-code table names","Tests, contracts and slim CI catch bad data and breaking changes before production"]
whatToLearnFirst: ["articles:dbt/dbt-for-data-engineers"]
relatedTechnologies: ["sql","etl-elt","data-warehousing","airflow"]
cheatSheet: "cheat-sheets:dbt"
monogram: "dbt"
lessons: ["articles:dbt/dbt-for-data-engineers", "articles:dbt/models-ref-and-sources", "articles:dbt/materializations", "articles:dbt/incremental-models"]
updatedDate: 2026-10-17
---

dbt (data build tool) is the transformation layer of an ELT pipeline. Raw data is already in the warehouse; you write each transformation as a SQL select statement, and dbt compiles the Jinja in it, works out the dependency graph from `ref()` and `source()`, creates the views and tables in order, and runs the tests you declare next to the models.

This course builds one small project end to end and runs every command for real: sources and layered models, materializations, incremental models and late data, tests and contracts, snapshots for SCD Type 2, Jinja macros and packages, and finally deployment with environments, selectors and slim CI. The examples run on dbt Core v1 with DuckDB, and each lesson notes where cloud warehouses or the newer v2 engine differ.
