---
title: "Top 50 dbt Interview Questions and Answers"
seoTitle: "Top 50 dbt Interview Questions and Answers"
description: "Top 50 dbt interview questions with answers: models and materializations, incremental models, tests, snapshots, Jinja and macros, and CI/CD."
technology: ["dbt", "etl-elt", "data-engineering"]
topic: ["interview-questions"]
difficulty: "Intermediate"
section: "learn"
publishedDate: "2026-10-16"
updatedDate: "2026-10-16"
reviewedDate: "2026-10-16"
related: ["interview-questions:data-engineering/late-arriving-data"]
versionContext: "Product names and defaults as of October 2026; code snippets state their dialect or runtime"
---

These 50 dbt interview questions cover models and materializations, incremental strategies, tests, snapshots, sources, Jinja and macros, project design, CI/CD and governance. They are grouped from beginner (freshers) to advanced (experienced). Each one starts with the answer you would give in the first 20 seconds; open the full answer for the detailed explanation, an example, how it shows up in real work, common mistakes and the follow-up questions interviewers ask.

## Question list

<details class="qb-index" open>
<summary>All 50 questions</summary>

1. [What is dbt, and where does it fit in ELT?](#1-what-is-dbt-and-where-does-it-fit-in-elt)
2. [dbt project structure: dbt_project.yml and profiles.yml](#2-dbt-project-structure-dbt_projectyml-and-profilesyml)
3. [ref() vs source()](#3-ref-vs-source)
4. [Materializations: view vs table vs incremental vs ephemeral vs materialized_view](#4-materializations-view-vs-table-vs-incremental-vs-ephemeral-vs-materialized_view)
5. [`--full-refresh`: when and why](#5---full-refresh-when-and-why)
6. [Seeds](#6-seeds)
7. [The four built-in generic data tests](#7-the-four-built-in-generic-data-tests)
8. [Singular data tests](#8-singular-data-tests)
9. [Jinja basics in dbt](#9-jinja-basics-in-dbt)
10. [Project layering: staging, intermediate and marts](#10-project-layering-staging-intermediate-and-marts)
11. [Naming and style conventions](#11-naming-and-style-conventions)
12. [dbt run vs build vs test vs compile](#12-dbt-run-vs-build-vs-test-vs-compile)
13. [Generating documentation and the lineage graph](#13-generating-documentation-and-the-lineage-graph)
14. [The analyses folder and dbt show](#14-the-analyses-folder-and-dbt-show)
15. [Incremental models and is_incremental()](#15-incremental-models-and-is_incremental)
16. [unique_key and on_schema_change](#16-unique_key-and-on_schema_change)
17. [Snapshots: timestamp vs check strategy (See also Q12)](#17-snapshots-timestamp-vs-check-strategy-see-also-q12)
18. [Writing custom generic tests](#18-writing-custom-generic-tests)
19. [Unit tests](#19-unit-tests)
20. [Test severity, warn_if/error_if and store_failures](#20-test-severity-warn_iferror_if-and-store_failures)
21. [Source freshness](#21-source-freshness)
22. [Writing macros](#22-writing-macros)
23. [var() and env_var()](#23-var-and-env_var)
24. [Packages: dbt_utils, dbt_expectations and packages.yml](#24-packages-dbt_utils-dbt_expectations-and-packagesyml)
25. [Hooks: pre-hook, post-hook, on-run-start/end](#25-hooks-pre-hook-post-hook-on-run-startend)
26. [Exposures](#26-exposures)
27. [Node selection syntax (+, @, tags, paths, state)](#27-node-selection-syntax---tags-paths-state)
28. [Targets, environments and deployment](#28-targets-environments-and-deployment)
29. [dbt Core vs dbt platform (Cloud) vs Fusion engine / dbt v2](#29-dbt-core-vs-dbt-platform-cloud-vs-fusion-engine--dbt-v2)
30. [Debugging with compiled SQL, logs and the target folder](#30-debugging-with-compiled-sql-logs-and-the-target-folder)
31. [Custom schemas and generate_schema_name](#31-custom-schemas-and-generate_schema_name)
32. [grants and persist_docs configs](#32-grants-and-persist_docs-configs)
33. [Orchestrating dbt (Airflow/Cosmos, dbt platform jobs, warehouse-native dbt projects)](#33-orchestrating-dbt-airflowcosmos-dbt-platform-jobs-warehouse-native-dbt-projects)
34. [dbt artifacts: manifest.json and run_results.json](#34-dbt-artifacts-manifestjson-and-run_resultsjson)
35. [Designing a test strategy across layers](#35-designing-a-test-strategy-across-layers)
36. [Refactoring a legacy SQL script into dbt models](#36-refactoring-a-legacy-sql-script-into-dbt-models)
37. [Common dbt anti-patterns](#37-common-dbt-anti-patterns)
38. [Incremental strategies by adapter (append, merge, delete+insert, insert_overwrite, microbatch)](#38-incremental-strategies-by-adapter-append-merge-deleteinsert-insert_overwrite-microbatch)
39. [The microbatch incremental strategy](#39-the-microbatch-incremental-strategy)
40. [YAML-defined snapshots and hard-delete handling](#40-yaml-defined-snapshots-and-hard-delete-handling)
41. [Model contracts](#41-model-contracts)
42. [Model versions, groups and access levels](#42-model-versions-groups-and-access-levels)
43. [dbt Semantic Layer and MetricFlow (See also Q99)](#43-dbt-semantic-layer-and-metricflow-see-also-q99)
44. [Slim CI with state:modified and `--defer`](#44-slim-ci-with-statemodified-and---defer)
45. [Speeding up slow dbt runs](#45-speeding-up-slow-dbt-runs)
46. [Python models](#46-python-models)
47. [Late-arriving data in incremental models (lookback windows)](#47-late-arriving-data-in-incremental-models-lookback-windows)
48. [dbt Mesh: cross-project refs](#48-dbt-mesh-cross-project-refs)
49. [Cross-database macros and adapter.dispatch](#49-cross-database-macros-and-adapterdispatch)
50. [dbt clone and zero-copy CI environments](#50-dbt-clone-and-zero-copy-ci-environments)

</details>

## Beginner dbt interview questions (freshers)

### 1. What is dbt, and where does it fit in ELT?

<p class="qb-meta">Beginner · Very Common · Fundamentals</p>

<p class="qb-label">Short answer</p>

dbt (data build tool) is the "T" in ELT. You write transformations as SELECT statements (and optionally Python models), and dbt compiles them, works out dependencies from `ref()`, and materialises them in the warehouse as views, tables or incremental tables. It adds testing, documentation, lineage, environments and version control on top of SQL. It doesn't extract or load data; it runs inside your warehouse or lakehouse.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Where it sits**
```text
Sources --(Fivetran / Airbyte / Openflow / Lakeflow Connect / custom EL)--> raw tables in warehouse
raw --(dbt: staging -> intermediate -> marts)--> analytics-ready tables --> BI / ML / reverse ETL
```

**What dbt gives you**
- **Modularity:** each model is one SELECT in a `.sql` file. `ref('model')` creates dependencies, so dbt builds a DAG and runs models in the right order, in parallel where possible.
- **Materialisations:** declare *how* a model is persisted (view, table, incremental, ephemeral, materialized view) without writing DDL (see [Materializations: view vs table vs incremental vs ephemeral vs materialized_view](#4-materializations-view-vs-table-vs-incremental-vs-ephemeral-vs-materialized_view)).
- **Testing:** data tests (unique, not_null, relationships, accepted_values, custom ones) and unit tests (see [The four built-in generic data tests](#7-the-four-built-in-generic-data-tests)–[Unit tests](#19-unit-tests)).
- **Documentation and lineage:** YAML descriptions plus auto-generated docs and DAG (see [Generating documentation and the lineage graph](#13-generating-documentation-and-the-lineage-graph)).
- **Jinja and macros:** reusable, parameterised SQL (see [Jinja basics in dbt](#9-jinja-basics-in-dbt), [Writing macros](#22-writing-macros)).
- **Environments:** the same code deploys to dev, CI and prod through targets (see [Targets, environments and deployment](#28-targets-environments-and-deployment)).
- **Software engineering practices:** Git, code review, CI/CD and slim CI (see [Slim CI with state:modified and `--defer`](#44-slim-ci-with-statemodified-and---defer)).

**Flavours**
- **dbt Core:** open-source CLI.
- **dbt platform** (formerly dbt Cloud): a managed service with IDE, scheduler, CI, Semantic Layer and Catalog.
- **dbt Fusion engine:** a newer Rust-based engine with fast parsing and SQL comprehension.
- **Warehouse-native options:** for example dbt projects deployed as objects in Snowflake, or dbt tasks in Databricks Lakeflow Jobs (see [dbt Core vs dbt platform (Cloud) vs Fusion engine / dbt v2](#29-dbt-core-vs-dbt-platform-cloud-vs-fusion-engine--dbt-v2), [Orchestrating dbt (Airflow/Cosmos, dbt platform jobs, warehouse-native dbt projects)](#33-orchestrating-dbt-airflowcosmos-dbt-platform-jobs-warehouse-native-dbt-projects)).

**What it isn't:** an ingestion tool, a scheduler on its own (Core needs orchestration), or a compute engine. All compute happens in the warehouse.

<p class="qb-label">Example</p>

```sql
-- models/marts/fct_orders.sql  (Dialect: Snowflake SQL via dbt)
select
    o.order_id,
    o.customer_id,
    o.order_date,
    p.amount_usd
from {{ ref('stg_shop__orders') }} as o
left join {{ ref('int_order_payments') }} as p
  on o.order_id = p.order_id
```
`dbt build --select fct_orders+` compiles this to fully qualified table names, builds it after its upstream models, then runs its tests and its downstream models.

<p class="qb-label">Real-world use case</p>

An analytics team had 300 stored procedures with hidden dependencies and no tests. After moving to dbt, the lineage graph exposed the dependency chain, tests caught duplicate orders before dashboards did, and new analysts could contribute with pull requests.

<p class="qb-label">Common mistakes and red flags</p>

- Calling dbt an ETL tool that moves data.
- Hard-coding table names instead of using `ref()`.
- Thinking dbt has its own compute.

<p class="qb-label">Follow-up questions</p>

- What does dbt compile to? — Plain SQL (DDL/DML wrappers around your SELECT) executed by the adapter on the warehouse.
- What is an adapter? — A plugin (dbt-snowflake, dbt-databricks, dbt-bigquery...) translating dbt operations to a platform's SQL.

</details>

### 2. dbt project structure: dbt_project.yml and profiles.yml

<p class="qb-meta">Beginner · Very Common · Project Setup</p>

<p class="qb-label">Short answer</p>

`dbt_project.yml` defines the project: its name, profile to use, folder paths, and default configs for folders of models, seeds, snapshots and tests. `profiles.yml` (usually in `~/.dbt/`, or supplied by the platform) holds connection details per target: account, user, auth method, role, warehouse, database and schema. Keep secrets out of the repo by using `env_var()` in profiles.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Typical layout**
```text
my_dbt_project/
 ├─ dbt_project.yml
 ├─ packages.yml            # external packages (see Q322)
 ├─ models/
 │   ├─ staging/            # stg_ models + _sources.yml
 │   ├─ intermediate/
 │   └─ marts/
 ├─ seeds/                  # small CSVs (see Q310)
 ├─ snapshots/
 ├─ tests/                  # singular tests (see Q314)
 ├─ macros/
 ├─ analyses/
 └─ target/                 # compiled output, artifacts (git-ignored)
```

**`dbt_project.yml` essentials**
- `name`, `version`, `profile` (which profile in profiles.yml), `model-paths`, `macro-paths` and others.
- `models:` block with **folder-level configs**, using `+` prefixes for configs: `+materialized`, `+schema`, `+tags`, `+persist_docs` and so on.
- `vars:` for project variables (see [var() and env_var()](#23-var-and-env_var)).
- `on-run-start` / `on-run-end` hooks (see [Hooks: pre-hook, post-hook, on-run-start/end](#25-hooks-pre-hook-post-hook-on-run-startend)).
- Config precedence, from most specific: in-model `config()` > properties YAML > `dbt_project.yml` folder config.

**`profiles.yml` essentials**
- A profile name, then `target` (the default) and `outputs` with one entry per target (dev, ci, prod).
- Adapter-specific keys. For Snowflake: `account`, `user`, `authenticator` or `private_key_path`, `role`, `warehouse`, `database`, `schema`, `threads`.
- Secrets come from `{{ env_var('DBT_SNOWFLAKE_PRIVATE_KEY_PATH') }}`. Never commit them.
- The dbt platform manages connections and credentials in its UI, so there is no local profiles.yml. Newer dbt versions also support a project-level `profiles.yml` or `--profiles-dir`.

**Why the split matters:** `dbt_project.yml` is shared and committed, so it describes *what* to build and *how*. `profiles.yml` is personal or per-environment, so it describes *where* and *as whom*. Keeping them separate lets the same project run unchanged on a laptop, in CI and in production, with only the target and credentials differing. Threads (parallel model builds) also live in the profile, because the right value depends on the warehouse each target uses.

<p class="qb-label">Example</p>

```yaml
# dbt_project.yml
name: acme_analytics
version: "1.0.0"
profile: acme_snowflake

models:
  acme_analytics:
    staging:
      +materialized: view
      +schema: staging
    intermediate:
      +materialized: ephemeral
    marts:
      +materialized: table
      +schema: marts
      finance:
        +tags: ["finance", "daily"]
```

```yaml
# ~/.dbt/profiles.yml
acme_snowflake:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: "{{ env_var('DBT_SF_ACCOUNT') }}"
      user: "{{ env_var('DBT_SF_USER') }}"
      private_key_path: "{{ env_var('DBT_SF_KEY_PATH') }}"
      role: TRANSFORMER_DEV
      warehouse: TRANSFORM_XS
      database: ANALYTICS_DEV
      schema: "dbt_{{ env_var('DBT_USER_SUFFIX') }}"
      threads: 8
```

<p class="qb-label">Real-world use case</p>

A team set `+materialized: view` for staging and `table` for marts once in `dbt_project.yml`. That removed hundreds of per-model config lines and made materialisation policy reviewable in one file.

<p class="qb-label">Common mistakes and red flags</p>

- Committing passwords in profiles.yml.
- Repeating configs in every model.
- Not knowing the config precedence order.

<p class="qb-label">Follow-up questions</p>

- What does `dbt debug` do? — Validates profile, connection and project setup.
- Why are `+` prefixes used? — To distinguish configs from folder names in the YAML tree.

</details>

### 3. ref() vs source()

<p class="qb-meta">Beginner · Very Common · Fundamentals</p>

<p class="qb-label">Short answer</p>

`{{ source('shop', 'orders') }}` points to raw tables loaded outside dbt, which are declared in a sources YAML file. `{{ ref('stg_shop__orders') }}` points to another dbt model, seed or snapshot. Both resolve to fully qualified relation names and build the DAG. `ref()` is environment-aware: it resolves to the dev, CI or prod schema of the target being built, and it controls build order.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

| Aspect | `source()` | `ref()` |
|---|---|---|
| Points to | Raw/external tables (not built by dbt) | dbt nodes: models, seeds, snapshots |
| Declared in | `sources:` YAML (database, schema, tables, freshness, tests) | Implicit (the model file itself) |
| Environment resolution | Same raw location for all targets (unless you template it) | Resolves to current target's schema (or deferred prod; see [Slim CI with state:modified and `--defer`](#44-slim-ci-with-statemodified-and---defer)) |
| DAG | Root nodes (green in lineage) | Edges between models |
| Extra features | Source freshness (see [Source freshness](#21-source-freshness)), source tests, `loaded_at_field` | Versioned refs `ref('m', v=2)`, cross-project refs (see [Model versions, groups and access levels](#42-model-versions-groups-and-access-levels), [dbt Mesh: cross-project refs](#48-dbt-mesh-cross-project-refs)) |

**Why never hard-code table names**
- dbt can't see the dependency, so it may build models in the wrong order.
- Environments break. Dev models would read production tables, or the reverse.
- Lineage, docs and selection (`+model`) miss the edge.
- Renaming or moving schemas means editing SQL everywhere.

**Best practices**
- Reference each source in **exactly one staging model** (`stg_<source>__<entity>`). Everything downstream refs staging models, never sources directly (see [Project layering: staging, intermediate and marts](#10-project-layering-staging-intermediate-and-marts)).
- Put source definitions in `_<source>__sources.yml` next to the staging models.
- Add tests and freshness on sources to catch upstream issues early.
- Use `identifier:` when the raw table name is ugly, and keep a clean logical name.

<p class="qb-label">Example</p>

```yaml
# models/staging/shop/_shop__sources.yml
version: 2
sources:
  - name: shop
    database: RAW
    schema: SHOP
    loaded_at_field: _loaded_at
    freshness:
      warn_after: {count: 6, period: hour}
      error_after: {count: 24, period: hour}
    tables:
      - name: orders
        identifier: ORDERS_V2
      - name: customers
```

```sql
-- models/staging/shop/stg_shop__orders.sql
select
    id::number            as order_id,
    customer_id::number   as customer_id,
    status                as order_status,
    created_at::timestamp as ordered_at
from {{ source('shop', 'orders') }}
```

<p class="qb-label">Real-world use case</p>

A model selected from `ANALYTICS.MARTS.DIM_CUSTOMER` directly. In CI it read production data instead of the PR's version and passed tests incorrectly. Switching to `ref('dim_customer')` made CI test the modified model.

<p class="qb-label">Common mistakes and red flags</p>

- Hard-coded relation names.
- Sources referenced from many models.
- Missing source declarations, so there is no freshness or lineage.

<p class="qb-label">Follow-up questions</p>

- Can sources point to different databases per environment? — Yes, by templating `database:` with `target.name` or `var()`.
- What does `ref` compile to? — `database.schema.identifier` for the relation in the active target.

</details>

### 4. Materializations: view vs table vs incremental vs ephemeral vs materialized_view

<p class="qb-meta">Beginner · Very Common · Materializations</p>

<p class="qb-label">Short answer</p>

A view stores only the SQL, so every query recomputes it: cheap to build, slower to read. A table is rebuilt fully on each run, which is fast to read and costly to rebuild. Incremental inserts or merges only new or changed rows into an existing table. Ephemeral models are inlined as CTEs into downstream models and never created. Materialized views (or dynamic tables on Snowflake) are maintained by the warehouse itself.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

| Materialization | Build cost | Query speed | Freshness | Use for |
|---|---|---|---|---|
| `view` | Near zero | Recomputed each query | Always current | Staging, light transforms, low-traffic models |
| `table` | Full rebuild each run | Fast | As of last run | Marts and dims of moderate size, heavy logic queried often |
| `incremental` | Only new/changed data | Fast | As of last run | Large event/fact tables (see [Incremental models and is_incremental()](#15-incremental-models-and-is_incremental), [Incremental strategies by adapter (append, merge, delete+insert, insert_overwrite, microbatch)](#38-incremental-strategies-by-adapter-append-merge-deleteinsert-insert_overwrite-microbatch)) |
| `ephemeral` | None (inlined CTE) | Depends on parent | N/A | Small reusable intermediate logic, not queried directly |
| `materialized_view` | Warehouse-managed refresh | Fast | Near-real-time (platform-dependent) | Simple aggregations on platforms with good MV support |
| `dynamic_table` (dbt-snowflake) | Snowflake-managed incremental/full refresh by target lag | Fast | Target lag | Declarative pipelines on Snowflake (see [Snowpipe vs Snowpipe Streaming vs COPY INTO](/snowflake/snowflake-interview-questions/#39-snowpipe-vs-snowpipe-streaming-vs-copy-into)) |

Snapshots (SCD2) and seeds are separate node types (see [Seeds](#6-seeds), [Snapshots: timestamp vs check strategy (See also Q12)](#17-snapshots-timestamp-vs-check-strategy-see-also-q12)).

**Choosing**
1. Start with **views** for staging.
2. Use **tables** for marts until build time or cost becomes a problem.
3. Move to **incremental** when full rebuilds are too slow or expensive, typically large append-heavy facts.
4. Use **ephemeral** sparingly. They can't be queried or tested on their own, and deep chains create huge compiled SQL that is hard to debug.
5. Use **materialized views or dynamic tables** when platform-managed freshness fits, and check the platform's limitations (supported SQL, refresh cost).

**Configuring:** in `dbt_project.yml` by folder, in properties YAML, or in-model with `{{ config(materialized='incremental') }}`.

<p class="qb-label">Example</p>

```sql
-- models/marts/fct_page_views.sql
{{ config(
    materialized = 'incremental',
    unique_key = 'page_view_id',
    incremental_strategy = 'merge',
    cluster_by = ['view_date']
) }}

select page_view_id, user_id, page_id, viewed_at, viewed_at::date as view_date
from {{ ref('stg_web__page_views') }}
{% if is_incremental() %}
where viewed_at > (select dateadd('hour', -3, max(viewed_at)) from {{ this }})
{% endif %}
```

```sql
-- models/marts/agg_daily_signups.sql  (dbt-snowflake dynamic table)
{{ config(materialized='dynamic_table', target_lag='30 minutes', snowflake_warehouse='TRANSFORM_S') }}
select signup_date, count(*) as signups
from {{ ref('stg_app__users') }}
group by signup_date
```

<p class="qb-label">Real-world use case</p>

A 4-billion-row page-views table rebuilt as a `table` took 50 minutes each hour. Converting it to incremental with a 3-hour lookback cut the run to 4 minutes, and the small dimension models stayed as tables.

<p class="qb-label">Common mistakes and red flags</p>

- Making everything incremental from day one.
- Long chains of ephemeral models.
- Heavy views queried by dashboards all day.

<p class="qb-label">Follow-up questions</p>

- Can you test an ephemeral model? — Not directly as a relation; test the downstream model instead.
- What's a custom materialization? — A macro implementing your own build logic via `{% materialization %}` blocks.

</details>

### 5. `--full-refresh`: when and why

<p class="qb-meta">Beginner · Common · Operations</p>

<p class="qb-label">Short answer</p>

`dbt run --full-refresh` (or `build`) drops and rebuilds incremental models (and seeds) from scratch, ignoring `is_incremental()`. Use it after logic changes that must apply to history, incompatible schema changes, corrupted or duplicated data, or changes to the incremental key. For very large tables it's expensive and slow, so you can protect models with `full_refresh: false` and backfill in ranges instead.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**When you need it**
- **Business logic changed** for existing rows (new calculation, bug fix), and history must reflect it.
- **Incompatible schema changes:** type changes, renamed columns, or a changed `unique_key`.
- **Data corruption:** duplicates from a bad strategy, or missing data from a broken filter.
- **Seeds** whose column types changed.
- **Snapshots aren't affected** by `--full-refresh`. Don't rebuild them; that would lose history.

**Risks and costs**
- Recomputes everything, which can take hours and cost a lot of credits on billion-row tables.
- May not be possible if **source data has expired** (retention-limited raw data, Kafka topics), because rebuilding loses history.
- Downstream consumers may see empty or partial tables mid-build. dbt typically builds into a temporary relation and swaps, depending on adapter and materialization, but long builds still matter.

**Safeguards**
- `{{ config(full_refresh = false) }}` on critical large models makes dbt ignore `--full-refresh` for them. Rebuilds then need a deliberate config change.
- Run full refreshes on a **larger warehouse** or off-peak.
- For time-series models, prefer **range backfills**: microbatch `--event-time-start/end` (see [The microbatch incremental strategy](#39-the-microbatch-incremental-strategy)), or a variable-driven date range.
- Use **zero-copy clones** to test a full refresh first (see [dbt clone and zero-copy CI environments](#50-dbt-clone-and-zero-copy-ci-environments)).

<p class="qb-label">Example</p>

```sql
-- models/marts/fct_events.sql
{{ config(
    materialized = 'incremental',
    unique_key = 'event_id',
    full_refresh = false        -- protect: --full-refresh is ignored for this model
) }}
select * from {{ ref('stg_app__events') }}
{% if is_incremental() %}
where event_ts > (select max(event_ts) from {{ this }})
{% endif %}
```

```bash
# Rebuild one fixed model and everything downstream
dbt build --select fct_orders+ --full-refresh --target prod
```

<p class="qb-label">Real-world use case</p>

An engineer ran `dbt run --full-refresh` on the whole project in production. A 3-year events model was rebuilt from raw data that only had 13 months of retention, and 23 months of history were lost. Restoring it from Time Travel saved the day, and `full_refresh: false` is now mandatory on such models.

<p class="qb-label">Common mistakes and red flags</p>

- Project-wide full refresh in production.
- Not knowing source retention limits.
- Using full refresh as a routine fix for bad incremental logic.

<p class="qb-label">Follow-up questions</p>

- Does --full-refresh affect views? — Views are recreated every run anyway.
- How do you full-refresh just one model? — `--select model_name --full-refresh`.

</details>

### 6. Seeds

<p class="qb-meta">Beginner · Common · Seeds</p>

<p class="qb-label">Short answer</p>

Seeds are small CSV files in the `seeds/` folder that `dbt seed` loads as tables, version-controlled with the project. Downstream models `ref()` them like any model. Use them for small, slowly changing reference data that analysts own, such as country mappings, holiday calendars, account mappings or test fixtures. Don't use them for large or frequently changing data.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Good uses**
- Mapping tables: status code to label, country code to region, GL account to reporting line.
- Static calendars and lists (holidays, fiscal periods), if not generated with SQL.
- Small lookup data needed for tests or demos.

**Avoid them for**
- Large data. Seeds load through INSERT statements batched by the adapter, which is slow beyond a few thousand rows, and they bloat the repo.
- Data owned or updated by other systems. Ingest those as sources.
- Sensitive data (PII, secrets), which would end up in Git history.

**Configuring**
- `column_types` to fix types (otherwise they are inferred), for example `zip_code: varchar` to keep leading zeros.
- `quote_columns`, `delimiter`, `+schema`, `+enabled`.
- Tests and descriptions in YAML, like models.
- Changing column types or removing columns may require `dbt seed --full-refresh`.

**Commands**
- `dbt seed` loads all seeds. `dbt seed --select country_regions` loads one.
- `dbt build` includes seeds in DAG order.

**Ownership tip:** treat each seed like a small dataset with an owner. Name the owning team in the YAML description or `meta`, review changes through pull requests, and add tests for keys and allowed values. That turns ad-hoc spreadsheet mappings into governed reference data.

<p class="qb-label">Example</p>

```csv
country_code,country_name,region,is_eu
DE,Germany,EMEA,true
FR,France,EMEA,true
IN,India,APAC,false
US,United States,AMER,false
```

```yaml
# seeds/_seeds.yml
seeds:
  - name: country_regions
    description: "Owned by Finance Ops; reviewed quarterly."
    config:
      column_types:
        country_code: varchar(2)
        is_eu: boolean
    columns:
      - name: country_code
        data_tests: [unique, not_null]
```

```sql
-- models/marts/dim_customer.sql
select c.customer_id, c.country_code, r.region, r.is_eu
from {{ ref('stg_crm__customers') }} c
left join {{ ref('country_regions') }} r using (country_code)
```

<p class="qb-label">Real-world use case</p>

Finance kept a spreadsheet mapping 300 GL accounts to P&L lines, emailed monthly. Moving it into a seed with pull-request review gave version history and tests (unique accounts, accepted P&L lines), and the P&L mart now refs it directly.

<p class="qb-label">Common mistakes and red flags</p>

- 100,000-row seeds.
- Seeds with PII.
- Relying on type inference for codes with leading zeros.

<p class="qb-label">Follow-up questions</p>

- Where would larger reference data go? — Ingest as a source (stage + COPY, or EL tool).
- Are seeds rebuilt every run? — `dbt seed` truncates and reloads by default.

</details>

### 7. The four built-in generic data tests

<p class="qb-meta">Beginner · Very Common · Testing</p>

<p class="qb-label">Short answer</p>

dbt ships four generic data tests that you attach to columns in YAML. `unique` means no duplicate values. `not_null` means no NULLs. `accepted_values` means values come from a list. `relationships` means every value exists in another model's column, which checks referential integrity. Each compiles to a query returning failing rows, and the test fails if any rows come back.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

| Test | Checks | Typical use |
|---|---|---|
| `unique` | Column has no duplicate values | Primary/surrogate keys in every model |
| `not_null` | No NULLs | Keys, required business fields |
| `accepted_values` | Values ∈ `values:` list (optional `quote: false`) | Status, type, currency codes |
| `relationships` | Each value exists in `to:` model's `field:` | FK from fact to dimension |

**How tests work**
- Each test compiles to a SELECT that returns **failing rows**. For example, `unique` groups by the column and keeps `count(*) > 1`.
- `dbt test` (or `dbt build`) runs them. Zero rows means pass. Thresholds can be configured (see [Test severity, warn_if/error_if and store_failures](#20-test-severity-warn_iferror_if-and-store_failures)).
- Since dbt 1.8, the YAML key is **`data_tests:`** (the older `tests:` key still works), which distinguishes them from **unit tests** (see [Unit tests](#19-unit-tests)).

**Useful configs**
- `where:` to test a subset (`where: "order_date >= current_date - 30"`) for big tables.
- `severity: warn` for soft rules.
- `config: {store_failures: true}` to keep failing rows in a table for debugging.
- `name:` to give tests readable names in logs.

**Coverage guidance**
- **Every model:** primary key `unique` and `not_null`. This is the minimum standard many teams enforce in CI.
- Foreign keys: `relationships` (or a `dbt_utils` or `dbt_expectations` equivalent with `where` filters for late-arriving dimensions).
- Enumerations: `accepted_values`.
- Add packages for richer tests: `dbt_utils`, `dbt_expectations`, `elementary` (see [Packages: dbt_utils, dbt_expectations and packages.yml](#24-packages-dbt_utils-dbt_expectations-and-packagesyml)).

<p class="qb-label">Example</p>

```yaml
# models/marts/_marts__models.yml
models:
  - name: fct_orders
    columns:
      - name: order_id
        data_tests: [unique, not_null]
      - name: customer_id
        data_tests:
          - not_null
          - relationships:
              arguments:
                to: ref('dim_customer')
                field: customer_id
      - name: order_status
        data_tests:
          - accepted_values:
              arguments:
                values: ['placed', 'shipped', 'delivered', 'returned', 'cancelled']
```
Newer dbt versions nest test parameters under `arguments:`. Older versions put `to:`, `field:` and `values:` directly under the test name. Use the form your dbt version expects.

<p class="qb-label">Real-world use case</p>

A new status, "partially_shipped", appeared in the source. The `accepted_values` test failed in the nightly build before the dashboard refreshed, and the team added the value deliberately, with a mapping to reporting categories.

<p class="qb-label">Common mistakes and red flags</p>

- Models without primary-key tests.
- `relationships` failing on legitimate late-arriving dimensions without `where` or warn configs.
- Not knowing tests return failing rows.

<p class="qb-label">Follow-up questions</p>

- How do you test a composite key? — `dbt_utils.unique_combination_of_columns`.
- How do you see failing rows? — `store_failures`, or run the compiled test SQL from `target/`.

</details>

### 8. Singular data tests

<p class="qb-meta">Beginner · Common · Testing</p>

<p class="qb-label">Short answer</p>

A singular test is a SQL file in `tests/` that selects rows violating a rule. If the query returns any rows, the test fails. Use singular tests for one-off business rules that don't fit a reusable generic test, such as "refund amount can't exceed the original payment" or "daily totals reconcile with the source". They can `ref()` models and use Jinja.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Characteristics**
- Lives in `tests/` (or the configured `test-paths`), as one SELECT per file.
- Attached to every model it `ref`s, so `dbt test --select fct_payments` runs it.
- Supports `{{ config(severity='warn', store_failures=true, tags=[...]) }}` at the top of the file.
- Results appear like other tests in logs and artifacts.

**Good candidates**
- **Cross-column rules:** `ship_date >= order_date`.
- **Cross-model reconciliation:** total revenue in the mart equals the sum in staging for the same day.
- **Business invariants:** one active subscription per customer.
- **Anomaly guards:** today's row count isn't below 50% of the 7-day average (or use packages or observability tools).

**Singular vs generic**
| Singular | Generic |
|---|---|
| One SQL file, specific to a case | Parameterised macro reused across columns/models |
| Quick to write | Needs a `{% test %}` macro (see [Writing custom generic tests](#18-writing-custom-generic-tests)) |
| Harder to reuse | Applied in YAML anywhere |

If you write the same singular test three times with different tables, convert it to a generic test.

**Writing them well**
- Return identifying columns and the values that broke the rule, which makes debugging easy.
- Filter to relevant windows on big tables, and use `where` logic for incremental data.
- Name files descriptively: `assert_refunds_not_exceed_payments.sql`.

<p class="qb-label">Example</p>

```sql
-- tests/assert_refunds_not_exceed_payments.sql  (Dialect: Snowflake SQL via dbt)
{{ config(severity = 'error', store_failures = true) }}

with refunds as (
    select payment_id, sum(refund_amount) as total_refunded
    from {{ ref('fct_refunds') }}
    group by payment_id
)
select p.payment_id, p.amount as paid_amount, r.total_refunded
from {{ ref('fct_payments') }} p
join refunds r on r.payment_id = p.payment_id
where r.total_refunded > p.amount
```
Sample: payment P1 was 100.00 and has refunds totalling 120.00. The test returns that row and fails, and the row is stored in the failures table for review.

<p class="qb-label">Real-world use case</p>

A singular reconciliation test comparing daily revenue in `fct_orders` against the payments provider's settlement totals (as a source) caught a currency conversion bug on day one. It had affected 2% of orders.

<p class="qb-label">Common mistakes and red flags</p>

- Tests that return all rows, or no rows, because of inverted logic.
- Unbounded scans on huge tables.
- Copy-pasted singular tests that should be generic.

<p class="qb-label">Follow-up questions</p>

- How do you make a test warn instead of fail? — `config(severity='warn')`.
- Can singular tests be run alone? — `dbt test --select test_name` (file name without .sql).

</details>

### 9. Jinja basics in dbt

<p class="qb-meta">Beginner · Very Common · Jinja</p>

<p class="qb-label">Short answer</p>

dbt uses Jinja templating to generate SQL before execution. `{{ ... }}` outputs expressions (`ref()`, `source()`, variables, macro calls). `{% ... %}` runs statements: `if`, `for`, `set`, and macro definitions. `{# ... #}` is a comment. Jinja runs at compile time, so it produces SQL text and doesn't run per row in the warehouse.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Syntax**
| Syntax | Purpose | Example |
|---|---|---|
| `{{ expr }}` | Print value | `{{ ref('dim_customer') }}`, `{{ var('start_date') }}` |
| `{% stmt %}` | Logic | `{% if target.name == 'prod' %}`, `{% for c in cols %}` |
| `{# note #}` | Comment (not rendered) | `{# remove after migration #}` |
| `{%- -%}` | Whitespace control | Trim newlines in generated SQL |
| Filters | Transform values | `{{ cols \| join(', ') }}`, `{{ name \| upper }}` |

**Useful dbt context**
- `target.name`, `target.schema`, `target.database`, `target.type` (adapter).
- `this` (the current relation), `model.name`, `config.get('materialized')`.
- `is_incremental()`, `execute` (true during execution, false during parsing).
- `run_query()` to query the warehouse at compile time, for example to fetch distinct values. Guard it with `{% if execute %}`.
- `log()`, `exceptions.raise_compiler_error()`.

**Good uses**
- Loops for repetitive SQL, such as pivoting known values or generating columns from a list.
- Environment-specific logic (sampling in dev).
- Macros for reusable expressions (see [Writing macros](#22-writing-macros)).

**Overuse warnings**
- Heavy Jinja makes models hard to read and debug. Check the compiled SQL in `target/compiled/` (see [Debugging with compiled SQL, logs and the target folder](#30-debugging-with-compiled-sql-logs-and-the-target-folder)).
- `run_query` at compile time adds warehouse queries and slows parsing and compilation.
- Prefer SQL features (window functions, `PIVOT`) where the warehouse supports them.

<p class="qb-label">Example</p>

```sql
-- models/marts/fct_payments_pivoted.sql  (Dialect: Snowflake SQL via dbt)
{% set methods = ['card', 'bank_transfer', 'wallet', 'gift_card'] %}

select
    order_id,
    {% for m in methods -%}
    sum(case when payment_method = '{{ m }}' then amount else 0 end) as {{ m }}_amount
    {%- if not loop.last %},{% endif %}
    {% endfor %}
from {{ ref('stg_payments') }}
{% if target.name == 'dev' %}
where created_at >= dateadd('day', -7, current_date)   {# keep dev builds small #}
{% endif %}
group by order_id
```
This compiles to four `sum(case ...)` columns: `card_amount`, `bank_transfer_amount`, `wallet_amount` and `gift_card_amount`. In dev, a 7-day filter is added.

<p class="qb-label">Real-world use case</p>

A finance model had 48 nearly identical hand-written CASE columns for monthly buckets. Replacing them with a Jinja loop over a list cut the file from 400 lines to 40, and adding a new bucket became a one-line change.

<p class="qb-label">Common mistakes and red flags</p>

- Thinking Jinja runs per row.
- `run_query` without an `execute` guard.
- Unreadable Jinja-heavy models with no compiled-SQL review.

<p class="qb-label">Follow-up questions</p>

- What does `execute` guard against? — During parsing, queries aren't run; `run_query` results would be empty or fail.
- How do you see rendered SQL? — `dbt compile` and check `target/compiled/...`.

</details>

### 10. Project layering: staging, intermediate and marts

<p class="qb-meta">Beginner · Very Common · Project Design</p>

<p class="qb-label">Short answer</p>

Staging models are one per source table. They rename, cast, apply light cleaning and dedupe, with no joins or business logic, and are usually views. Intermediate models combine and reshape staging models into reusable building blocks (joins, aggregations, pivots). Marts are business-facing facts and dimensions organised by domain, materialised as tables or incremental. Each layer only refs the layer before it.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Layer responsibilities**
| Layer | Naming | Contents | Materialization | Refs |
|---|---|---|---|---|
| Staging | `stg_<source>__<entity>` | Rename, cast, standardise units/timezones, basic dedupe, one model per source table | View (or table if heavy) | `source()` only |
| Intermediate | `int_<entity>_<verb>` (e.g. `int_orders_pivoted`) | Joins, re-graining, complex logic shared by marts | Ephemeral/view/table | `ref()` staging/intermediate |
| Marts | `fct_<process>`, `dim_<entity>`, or business names | Kimball-style facts/dims or wide tables, by domain folder (finance, marketing) | Table/incremental | `ref()` intermediate/staging |

**Folder structure**
```text
models/
 ├─ staging/
 │   ├─ shop/      _shop__sources.yml, _shop__models.yml, stg_shop__orders.sql, ...
 │   └─ stripe/    ...
 ├─ intermediate/
 │   └─ finance/   int_payments_pivoted_to_orders.sql
 └─ marts/
     ├─ finance/   fct_orders.sql, dim_customers.sql
     └─ marketing/ fct_campaign_performance.sql
```

**Why layer**
- **Single source of truth per source table.** Cleaning happens once, so fixes propagate everywhere.
- **Reusability.** Intermediate logic is shared, not duplicated across marts.
- **Clear ownership and testing.** Staging tests check source assumptions, and mart tests check business rules (see [Designing a test strategy across layers](#35-designing-a-test-strategy-across-layers)).
- **Readable DAG.** Lineage flows left to right without spaghetti.

**Relation to medallion:** staging is roughly bronze-to-silver cleaning, intermediate is silver logic, and marts are gold. Many teams map them directly (see [What is the medallion (bronze/silver/gold) architecture?](/career/data-engineering-interview-questions/#14-what-is-the-medallion-bronzesilvergold-architecture)).

**Anti-patterns to avoid:** marts that ref sources directly, staging with joins, and the same entity cleaned in several places (see [Common dbt anti-patterns](#37-common-dbt-anti-patterns)).

<p class="qb-label">Example</p>

```sql
-- models/staging/shop/stg_shop__orders.sql
with source as (select * from {{ source('shop', 'orders') }}),
renamed as (
    select
        id::number                       as order_id,
        customer_id::number              as customer_id,
        lower(trim(status))              as order_status,
        convert_timezone('UTC', created_at)::timestamp_ntz as ordered_at,
        total_cents / 100.0              as order_total
    from source
)
select * from renamed
qualify row_number() over (partition by order_id order by ordered_at desc) = 1
```

```sql
-- models/marts/finance/fct_orders.sql
select o.order_id, o.customer_id, o.ordered_at, o.order_total, p.amount_paid
from {{ ref('stg_shop__orders') }} o
left join {{ ref('int_payments_pivoted_to_orders') }} p using (order_id)
```

<p class="qb-label">Real-world use case</p>

In a project of 900 models without layering, the same Salesforce account cleaning appeared in 14 places, with different results. Refactoring into one staging model per source table, plus shared intermediate models, removed the inconsistencies and cut the model count by a third.

<p class="qb-label">Common mistakes and red flags</p>

- Business logic in staging.
- Marts reading sources directly.
- Huge intermediate models that are really marts.

<p class="qb-label">Follow-up questions</p>

- Should staging dedupe? — Light, technical dedupe (exact duplicates from loaders) is common; business dedupe often belongs later.
- Where do you put SCD2 snapshots? — Snapshot raw sources; then stage/model from the snapshot.

</details>

### 11. Naming and style conventions

<p class="qb-meta">Beginner · Occasional · Project Design</p>

<p class="qb-label">Short answer</p>

Use consistent prefixes (`stg_`, `int_`, `fct_`, `dim_`), the double-underscore source separator (`stg_stripe__charges`), snake_case names, singular or plural nouns used consistently, and standard column suffixes (`_id`, `_at` for timestamps, `_date`, `is_`/`has_` for booleans, `_amount` or a currency suffix). Write CTE-based models with an import → logic → final select pattern. Enforce it with SQLFluff (or dbt Fusion's linting) and code review.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Model naming**
| Pattern | Meaning |
|---|---|
| `stg_<source>__<entity>` | Staging per source table (`stg_salesforce__accounts`) |
| `int_<entity>_<verb>` | Intermediate (`int_orders_joined_payments`) |
| `fct_<event/process>` | Facts (`fct_orders`, `fct_page_views`) |
| `dim_<entity>` | Dimensions (`dim_customers`) |
| `snap_<source>__<entity>` | Snapshots |
| `_<folder>__models.yml` / `_<source>__sources.yml` | Property files sorted to top of folder |

**Column conventions**
- Primary key: `<entity>_id` (natural) and `<entity>_key` or `_sk` (surrogate). Be consistent.
- Timestamps `_at` (`created_at`, in UTC), dates `_date`, booleans `is_`/`has_`.
- Money: `_amount` plus a currency column, or a currency suffix (`_usd`). Never ambiguous units.
- No reserved words, no abbreviations nobody understands, and lower_snake_case.

**SQL style**
- **Import CTEs** at the top (`with orders as (select * from {{ ref('stg_shop__orders') }})`), then **logical CTEs**, then a **final** `select * from final`.
- Explicit column lists in marts, so contracts and diffs are clear.
- Leading or trailing commas used consistently, lowercase keywords (or uppercase, consistently), and four-space indentation.
- Use `using` or explicit `on` consistently, and fully alias joined columns.
- Comments explain *why*, not *what*.

**Tooling**
- **SQLFluff** with the `dbt` templater (or `jinja`), run in pre-commit and CI.
- `dbt_project_evaluator` for structural rules (marts referencing sources, missing tests or docs).
- **dbt Fusion** offers built-in SQL comprehension and linting capabilities in its tooling (check the current feature set).

<p class="qb-label">Example</p>

```sql
-- models/marts/finance/fct_refunds.sql  (Dialect: Snowflake SQL via dbt)
with

refunds as (
    select * from {{ ref('stg_stripe__refunds') }}
),

charges as (
    select * from {{ ref('stg_stripe__charges') }}
),

final as (
    select
        refunds.refund_id,
        refunds.charge_id,
        charges.order_id,
        refunds.refund_amount_usd,
        refunds.refunded_at,
        refunds.refund_amount_usd = charges.charge_amount_usd as is_full_refund
    from refunds
    left join charges
        on refunds.charge_id = charges.charge_id
)

select * from final
```

```yaml
# .sqlfluff (excerpt)
# [sqlfluff]
# templater = dbt
# dialect = snowflake
# [sqlfluff:rules:capitalisation.keywords]
# capitalisation_policy = lower
```

<p class="qb-label">Real-world use case</p>

A team had `created`, `create_ts`, `creation_date` and `created_at` across models, plus mixed timezones. Adopting `_at` for UTC timestamps, enforced by SQLFluff rules and review, reduced analyst confusion and wrong-timezone bugs.

<p class="qb-label">Common mistakes and red flags</p>

- Inconsistent prefixes.
- Ambiguous money units.
- Style debates resolved in every PR instead of by tooling.

<p class="qb-label">Follow-up questions</p>

- Why the double underscore? — Separates source name from entity when either contains underscores.
- Should marts use `select *`? — Final `select * from final` is fine; explicit column lists inside `final` keep outputs deliberate.

</details>

### 12. dbt run vs build vs test vs compile

<p class="qb-meta">Beginner · Very Common · Commands</p>

<p class="qb-label">Short answer</p>

`dbt run` builds models only. `dbt test` runs data tests and unit tests on built objects. `dbt build` runs models, tests, seeds and snapshots together in DAG order, testing each node right after it builds and skipping downstream nodes when a test fails. `dbt compile` renders Jinja to SQL without executing the models (it may still run introspective queries). `build` is the usual choice for production and CI.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

| Command | Does | Use when |
|---|---|---|
| `dbt run` | Materialize models | Quick iteration on models; legacy pipelines |
| `dbt test` | Run tests (data + unit) | After a run, or to test without rebuilding |
| `dbt build` | Seeds → snapshots → models → tests in DAG order; failed test skips children | Production and CI (safest) |
| `dbt compile` | Render SQL into `target/compiled` | Debugging Jinja, reviewing generated SQL |
| `dbt seed` | Load seeds | Seed changes |
| `dbt snapshot` | Run snapshots | Scheduled SCD2 capture |
| `dbt show` | Run a model/query preview, return rows | Quick inspection (see [The analyses folder and dbt show](#14-the-analyses-folder-and-dbt-show)) |
| `dbt docs generate` | Build catalog and docs artifacts | Documentation (see [Generating documentation and the lineage graph](#13-generating-documentation-and-the-lineage-graph)) |
| `dbt source freshness` | Check source freshness | Before builds (see [Source freshness](#21-source-freshness)) |
| `dbt ls` | List selected resources | Verify selectors |
| `dbt retry` | Re-run failed/skipped nodes from last run | After fixing a failure |
| `dbt clone` | Clone relations from another state | CI/dev setup (see [dbt clone and zero-copy CI environments](#50-dbt-clone-and-zero-copy-ci-environments)) |
| `dbt parse` / `dbt debug` / `dbt deps` / `dbt clean` | Parse project / check connection / install packages / remove target dirs | Setup and maintenance |

**Why `build` is preferred**
- Tests run **immediately after** each model, so a failing primary-key test on `stg_orders` stops `fct_orders` from building on bad data. That is a circuit breaker.
- One command, one set of artifacts, and correct ordering of seeds and snapshots.
- Selection works the same way (`dbt build --select state:modified+`).

**Practical notes**
- `--fail-fast` (`-x`) stops at the first failure.
- `--threads` sets parallelism.
- `--exclude-resource-type unit_test` in production if unit tests ran in CI.
- `dbt run` followed by `dbt test` lets bad data reach downstream tables before tests run.

<p class="qb-label">Example</p>

```bash
# Local iteration
dbt run --select stg_shop__orders
dbt compile --select fct_orders            # inspect target/compiled/.../fct_orders.sql

# CI and production
dbt build --select state:modified+ --defer --state ./prod-artifacts --target ci
dbt build --target prod --exclude-resource-type unit_test

# Fix and continue after a failure
dbt retry
```

<p class="qb-label">Real-world use case</p>

A nightly pipeline ran `dbt run` then `dbt test`. A duplicate-key bug in staging propagated into 40 downstream tables before the tests failed. Switching to `dbt build` meant the failing test skipped the dependants, so production kept yesterday's correct data.

<p class="qb-label">Common mistakes and red flags</p>

- Using `run` and `test` separately in production.
- Thinking `compile` never queries the warehouse.
- Not knowing about `dbt retry`.

<p class="qb-label">Follow-up questions</p>

- What happens to downstream nodes when a model fails in build? — They're skipped.
- Does `dbt build` run source freshness? — No; run `dbt source freshness` separately.

</details>

### 13. Generating documentation and the lineage graph

<p class="qb-meta">Beginner · Common · Documentation & Lineage</p>

<p class="qb-label">Short answer</p>

Describe models, columns, sources and macros in YAML (and in `{% docs %}` blocks for long text). `dbt docs generate` compiles the project and queries the warehouse for column types and statistics, writing `manifest.json` and `catalog.json`. `dbt docs serve` hosts a static site with documentation and an interactive DAG. dbt platform provides Catalog, formerly dbt Explorer, as the managed equivalent.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**What goes into docs**
- `description` for models, columns, sources, seeds, snapshots, macros and exposures.
- `{% docs block_name %} ... {% enddocs %}` in `.md` files, reused with `{{ doc('block_name') }}`. Good for shared definitions such as "net revenue".
- `meta:` fields (owner, tier, PII flags) show in docs and can drive automation.
- Tests, contracts, materialisation, tags and lineage appear automatically.

**Generation**
- `dbt docs generate` produces `target/manifest.json` (project graph and metadata), `target/catalog.json` (warehouse metadata: columns, types, stats), and `index.html`.
- `--static` produces a single self-contained HTML file for easy hosting.
- `--no-compile` and `--select` speed it up on big projects.
- Host the static files on internal object storage or a web server behind SSO, or use dbt platform Catalog.

**Persisting docs to the warehouse:** `persist_docs` writes descriptions into table and column comments, so catalogs (Snowflake Horizon, Unity Catalog, BI tools) show them (see [grants and persist_docs configs](#32-grants-and-persist_docs-configs)).

**Lineage graph**
- Built from `ref` and `source` edges plus exposures. Sources are green, models blue, exposures orange in dbt docs.
- Interactive selection uses the same syntax as the CLI (`+fct_orders+`).
- Column-level lineage is available in dbt platform Catalog and Fusion-powered tooling, but not in the classic static docs.

**Keeping docs alive**
- Require descriptions for marts and public models in CI (`dbt_project_evaluator`, or a custom check over the manifest).
- Use codegen to scaffold YAML.
- Document **why** and **business meaning**, not just column names.

<p class="qb-label">Example</p>

```markdown
<!-- models/docs/finance_docs.md -->
{% docs net_revenue %}
Order total minus discounts, refunds and taxes, in USD at the daily FX rate on `ordered_at`.
Excludes cancelled orders. Owned by Finance Data (finance-data@acme.example).
{% enddocs %}
```

```yaml
models:
  - name: fct_orders
    description: "One row per order. Grain: order_id."
    meta:
      owner: finance-data
      tier: 1
    columns:
      - name: net_revenue_usd
        description: "{{ doc('net_revenue') }}"
```

```bash
dbt docs generate --target prod
dbt docs serve --port 8081
```

<p class="qb-label">Real-world use case</p>

New analysts spent weeks asking which revenue column to use. Reusable doc blocks for 15 core metrics, plus persisted docs, made the definitions visible in dbt docs and in the BI tool's field descriptions, and onboarding questions dropped noticeably.

<p class="qb-label">Common mistakes and red flags</p>

- Docs generated but never hosted or updated.
- Descriptions that repeat the column name.
- Not knowing what `catalog.json` contains.

<p class="qb-label">Follow-up questions</p>

- Does docs generate run models? — No; it compiles and queries metadata.
- How do you get column-level lineage? — dbt platform Catalog/Fusion, or external lineage tools parsing SQL.

</details>

### 14. The analyses folder and dbt show

<p class="qb-meta">Beginner · Occasional · Development Workflow</p>

<p class="qb-label">Short answer</p>

The `analyses/` folder holds SQL files that use `ref()`, `source()` and Jinja but are only compiled, never materialised. They are good for version-controlled ad-hoc queries, audit queries and one-off reports. `dbt show --select model` (or `--inline "select ..."`) runs a model's SQL or an inline query and prints a preview of the rows, without building anything.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**analyses**
- `dbt compile` renders them into `target/compiled/.../analyses/`, ready to paste into a SQL editor or BI tool.
- They benefit from environment-aware refs. The same analysis works in dev or prod targets.
- Good uses:
  - Reconciliation queries used during migrations (with `audit_helper`; see [Refactoring a legacy SQL script into dbt models](#36-refactoring-a-legacy-sql-script-into-dbt-models)).
  - Saved investigation queries ("why did revenue drop on 2026-03-02?").
  - Queries for BI tools that can't use dbt directly.
  - Documented, reviewed SQL that isn't part of the DAG.
- They appear in docs and lineage as analysis nodes.

**dbt show**
- `dbt show --select fct_orders --limit 20` compiles and runs the model's SELECT with a limit and prints the rows.
- `dbt show --inline "select count(*) from {{ ref('fct_orders') }} where order_status = 'returned'"` runs ad-hoc Jinja SQL.
- `--output json` for scripts.
- Used in the dbt platform IDE as "preview", and the Fusion VS Code extension also offers previews.
- It runs a real query, so warehouse cost and data access apply. Limits are pushed into the query on most adapters, but complex models may still compute a lot.

**When to promote an analysis to a model:** when it is used repeatedly, needs tests, or feeds other work. Then make it a proper model with docs and tests.

<p class="qb-label">Example</p>

```sql
-- analyses/revenue_drop_2026_03_02.sql
select
    o.order_status,
    count(*)           as orders,
    sum(o.order_total) as revenue
from {{ ref('fct_orders') }} o
where o.ordered_at::date in ('2026-03-01', '2026-03-02')
group by 1
order by 3 desc
```

```bash
dbt compile --select revenue_drop_2026_03_02
dbt show --select fct_orders --limit 5
dbt show --inline "select order_status, count(*) from {{ ref('fct_orders') }} group by 1"
```

<p class="qb-label">Real-world use case</p>

During an incident review, the team stored the investigation queries as analyses in the PR. That gave a reviewable record of how the root cause was found, and a ready-made query when a similar alert fired a month later.

<p class="qb-label">Common mistakes and red flags</p>

- Expecting analyses to create tables.
- Using `dbt show` on heavy models without considering cost.
- Leaving important recurring logic as analyses without tests.

<p class="qb-label">Follow-up questions</p>

- Do analyses run in `dbt build`? — No; they're compiled only.
- Can `dbt show` preview a CTE? — Use `--inline` with the CTE logic or compile and run pieces manually.

</details>

## Intermediate dbt interview questions

### 15. Incremental models and is_incremental()

<p class="qb-meta">Intermediate · Very Common · Incremental Models</p>

<p class="qb-label">Short answer</p>

An incremental model builds the full table on the first run, or with `--full-refresh`. On later runs it processes only new or changed rows and adds them to the existing table using the configured strategy. `is_incremental()` is true only when the target table exists, the model is incremental, and it isn't a full refresh. Inside that block you filter the source using `{{ this }}`, the existing target table.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Lifecycle**
1. **First run:** the target doesn't exist, so `is_incremental()` is false. dbt runs the full SELECT and creates the table.
2. **Subsequent runs:** `is_incremental()` is true. The filter limits rows, for example `updated_at > max(updated_at) in {{ this }}`. dbt writes the results to a temp relation, then appends, merges or deletes and inserts into the target, depending on the strategy (see [Incremental strategies by adapter (append, merge, delete+insert, insert_overwrite, microbatch)](#38-incremental-strategies-by-adapter-append-merge-deleteinsert-insert_overwrite-microbatch)).
3. **`--full-refresh`:** dbt rebuilds from scratch (see [`--full-refresh`: when and why](#5---full-refresh-when-and-why)).

**Designing the filter**
- Use a **reliable, monotonic column**: an `updated_at` set by the source or loader, an ingestion timestamp, or an increasing ID.
- Add a **lookback window** (`max(updated_at) - interval '3 hours'`) to catch late-arriving or clock-skewed rows. Combine it with `unique_key` so reprocessed rows merge instead of duplicating (see [Late-arriving data in incremental models (lookback windows)](#47-late-arriving-data-in-incremental-models-lookback-windows)).
- Use a **subquery on `{{ this }}`** or a pre-computed variable. On some warehouses, push the filter so partition pruning applies.
- The filter belongs to the **source** side. Avoid filters that depend on joins to the target.

**Pitfalls**
- **No `unique_key` with a lookback** creates duplicates under the append strategy.
- **Changing the model logic** doesn't rewrite historical rows. You need a full refresh or a backfill.
- **Schema changes:** new columns don't appear without `on_schema_change` handling (see [unique_key and on_schema_change](#16-unique_key-and-on_schema_change)).
- **Deletes in the source** aren't reflected unless you model them (soft-delete flags, or delete+insert per partition).
- **Window functions** over only the new rows give wrong results for metrics that need full history (running totals). Recompute affected keys, or keep those models as full tables.

<p class="qb-label">Example</p>

```sql
-- models/marts/fct_orders.sql  (Dialect: Snowflake SQL via dbt)
{{ config(materialized='incremental', unique_key='order_id', incremental_strategy='merge') }}

with orders as (
    select * from {{ ref('stg_shop__orders') }}
    {% if is_incremental() %}
      where updated_at >= (select dateadd('hour', -6, max(updated_at)) from {{ this }})
    {% endif %}
)
select order_id, customer_id, order_status, order_total, ordered_at, updated_at
from orders
```
Walkthrough: if the target's max `updated_at` is 2026-03-02 10:00, this run reads source rows updated since 04:00 and merges them on `order_id`. A row whose status changed at 09:30 updates in place.

<p class="qb-label">Real-world use case</p>

An incremental orders model used `>` against `max(updated_at)` with no lookback, and missed rows the loader committed late with earlier timestamps. Daily revenue was 0.3% low. Adding a 6-hour lookback with merge on `order_id` fixed it.

<p class="qb-label">Common mistakes and red flags</p>

- Using `current_date` instead of the target's state.
- No lookback.
- Expecting logic changes to apply to old rows automatically.

<p class="qb-label">Follow-up questions</p>

- What is `{{ this }}`? — The relation of the current model in the target (the existing table).
- When does `is_incremental()` return false? — First build, full refresh, or when materialization isn't incremental.

</details>

### 16. unique_key and on_schema_change

<p class="qb-meta">Intermediate · Common · Incremental Models</p>

<p class="qb-label">Short answer</p>

`unique_key` tells merge and delete+insert strategies which column(s) identify a row, so reprocessed rows update instead of duplicating. It must truly be unique in the model output. `on_schema_change` controls what happens when an incremental model's columns change: `ignore` (default), `fail`, `append_new_columns`, or `sync_all_columns`, which adds and removes columns.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**unique_key**
- A single column (`'order_id'`) or a list (`['order_id', 'line_number']`).
- With **duplicates in the batch**, MERGE errors ("duplicate row detected" on Snowflake with ERROR_ON_NONDETERMINISTIC_MERGE, or nondeterministic results). Dedupe in the model with `qualify row_number() ... = 1`.
- **NULLs in key columns** never match in MERGE, so they insert duplicates each run. Ensure keys are non-null, and test them with `not_null`.
- Add `unique` and `not_null` tests on the key, or `dbt_utils.unique_combination_of_columns` for composite keys.

**on_schema_change options**
| Option | Behaviour on new/removed columns |
|---|---|
| `ignore` (default) | New columns not added; removed columns cause NULLs or errors depending on SQL |
| `fail` | Error, forcing you to decide (full refresh or manual change) |
| `append_new_columns` | Add new columns (no backfill of old rows); never drop |
| `sync_all_columns` | Add new and drop removed columns; handles some type changes (adapter-dependent) |

**Notes**
- Newly added columns are **NULL for historical rows**. Backfill with a full refresh or targeted update if history matters.
- Column type changes are adapter-specific. Many require a full refresh.
- **Model contracts** (see [Model contracts](#41-model-contracts)) make schema changes explicit and fail fast in CI.
- Snapshots handle schema changes differently: new columns are added, and removed columns stay.

<p class="qb-label">Example</p>

```sql
-- Dialect: Snowflake SQL via dbt
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = ['order_id', 'line_number'],
    on_schema_change = 'append_new_columns'
) }}

select order_id, line_number, sku, quantity, unit_price, discount_pct, updated_at
from {{ ref('stg_shop__order_lines') }}
{% if is_incremental() %}
where updated_at >= (select dateadd('day', -1, max(updated_at)) from {{ this }})
{% endif %}
qualify row_number() over (partition by order_id, line_number order by updated_at desc) = 1
```

```yaml
models:
  - name: fct_order_lines
    data_tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns: [order_id, line_number]
```

<p class="qb-label">Real-world use case</p>

After a source added `discount_pct`, an incremental model with `on_schema_change: ignore` silently never exposed it, and analysts thought discounts weren't tracked. Switching to `append_new_columns` plus a one-time backfill of 90 days solved it.

<p class="qb-label">Common mistakes and red flags</p>

- `unique_key` that isn't unique.
- NULLable keys.
- Expecting new columns to be backfilled automatically.

<p class="qb-label">Follow-up questions</p>

- Does unique_key enforce uniqueness? — No; it's used for matching. Tests enforce it.
- What happens with `sync_all_columns` when a column is removed? — It's dropped from the target table (data lost for that column).

</details>

### 17. Snapshots: timestamp vs check strategy (See also Q12)

<p class="qb-meta">Intermediate · Very Common · Snapshots</p>

<p class="qb-label">Short answer</p>

Snapshots implement SCD Type 2 on mutable source tables. Each `dbt snapshot` run compares the current source to the snapshot table and closes changed rows by setting `dbt_valid_to`, then inserts new versions. The timestamp strategy detects changes using an `updated_at` column, which is reliable and cheap. The check strategy compares listed columns (or all of them), and suits sources without a trustworthy updated timestamp.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Columns dbt adds**
- `dbt_scd_id`: a hash identifying the version.
- `dbt_updated_at`: the timestamp used for the version.
- `dbt_valid_from` / `dbt_valid_to`: the validity window. The current row has `dbt_valid_to` NULL, or a configured far-future value with `dbt_valid_to_current`.
- Column names can be customised with `snapshot_meta_column_names` (dbt 1.9+).

**Strategies**
| Strategy | Detects change by | Pros | Cons |
|---|---|---|---|
| `timestamp` | `updated_at` greater than the snapshot's last value | Efficient, simple | Requires a reliable updated timestamp that changes on every update |
| `check` | Comparing `check_cols` (list or `all`) | Works without timestamps | Heavier comparisons; can miss changes if columns not listed; `all` captures noise columns |

**Behaviour**
- Snapshots capture state **only when they run**. Changes between runs (A → B → A within a day) are invisible. For complete history, use CDC (see [Airflow executors: Local vs Celery vs Kubernetes](/career/data-engineering-interview-questions/#84-airflow-executors-local-vs-celery-vs-kubernetes)), or AUTO CDC on Databricks (see [AUTO CDC (formerly APPLY CHANGES) for SCD Type 1/2 in pipelines](/databricks/databricks-interview-questions/#42-auto-cdc-formerly-apply-changes-for-scd-type-12-in-pipelines)).
- **Hard deletes** in the source aren't captured unless configured (see [YAML-defined snapshots and hard-delete handling](#40-yaml-defined-snapshots-and-hard-delete-handling)).
- Snapshot SQL should select from a **source** (or a light staging model), not a heavily transformed model. Keep it as raw as possible and transform downstream.
- `--full-refresh` doesn't rebuild snapshots, because history is precious. Protect snapshot tables from accidental drops (grants, separate schema).

<p class="qb-label">Example</p>

```yaml
# snapshots/snap_crm__customers.yml  (dbt 1.9+ YAML snapshot)
snapshots:
  - name: snap_crm__customers
    relation: source('crm', 'customers')
    config:
      schema: snapshots
      unique_key: customer_id
      strategy: timestamp
      updated_at: updated_at
      dbt_valid_to_current: "to_date('9999-12-31')"
```

```sql
-- Current state and point-in-time lookup (Dialect: Snowflake SQL)
select * from {{ ref('snap_crm__customers') }} where dbt_valid_to = to_date('9999-12-31');

select * from {{ ref('snap_crm__customers') }}
where '2026-02-15'::date >= dbt_valid_from and '2026-02-15'::date < dbt_valid_to;
```
Walkthrough: customer 7 moves from segment SMB to MID on 2026-03-01. After the next snapshot run, the SMB row gets `dbt_valid_to = 2026-03-01 …`, and a new MID row starts from that timestamp with `dbt_valid_to = 9999-12-31`.

<p class="qb-label">Real-world use case</p>

Sales commissions depended on each account's tier at the time of sale. A daily snapshot of CRM accounts with the timestamp strategy let finance join each order to the tier valid at `ordered_at`, replacing manual spreadsheet lookups.

<p class="qb-label">Common mistakes and red flags</p>

- Snapshotting transformed models.
- Check strategy with `all` including volatile columns such as `last_login`.
- Assuming snapshots catch every intermediate change.

<p class="qb-label">Follow-up questions</p>

- How often should snapshots run? — As often as you need history granularity, typically each load cycle.
- Can you backfill a snapshot? — Not natively; you'd need historical extracts or CDC data to reconstruct.

</details>

### 18. Writing custom generic tests

<p class="qb-meta">Intermediate · Common · Testing</p>

<p class="qb-label">Short answer</p>

A custom generic test is a `{% test name(model, column_name, ...) %}` Jinja block, usually in `tests/generic/` or `macros/`, that returns failing rows for any model and column passed to it. Once defined, you apply it in YAML exactly like `unique` or `not_null`, with your own parameters. This turns repeated business rules into one tested, reusable definition.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Anatomy**
- Signature: `{% test test_name(model, column_name, arg1, arg2=default) %}`. `model` and `column_name` are injected by dbt. Model-level tests omit `column_name`.
- Body: a SELECT returning failing rows.
- YAML usage: `- test_name: {arg1: ..., arg2: ...}`, nested under `arguments:` in newer versions.

**Design tips**
- Keep tests **adapter-neutral** using cross-database macros (`dbt.dateadd`, `dbt.datediff`; see [Cross-database macros and adapter.dispatch](#49-cross-database-macros-and-adapterdispatch)).
- Support an optional **`where`** through the standard config rather than custom arguments. dbt applies the `where` config to the `model` relation automatically.
- Return useful columns: the offending value and a key, for debugging.
- Document them in a YAML `macros:` block, and unit-test complex ones with small fixtures.
- Check **packages** before writing your own. `dbt_utils` (`expression_is_true`, `accepted_range`, `not_constant`, `recency`) and `dbt_expectations` cover many cases (see [Packages: dbt_utils, dbt_expectations and packages.yml](#24-packages-dbt_utils-dbt_expectations-and-packagesyml)).

**Common custom tests**
- Values within a range (`min_value`, `max_value`, inclusive flags).
- Positive or non-negative amounts.
- Column matching a regex (email, IBAN formats).
- Date not in the future.
- Row count within X% of another model.

**Sharing them:** put organisation-wide tests in an internal package, so every project uses the same definitions and tolerances, and changes to a rule happen in one reviewed place.

<p class="qb-label">Example</p>

```sql
-- tests/generic/not_in_future.sql
{% test not_in_future(model, column_name, grace_minutes=5) %}
select {{ column_name }} as offending_value, count(*) as n
from {{ model }}
where {{ column_name }} > {{ dbt.dateadd('minute', grace_minutes, dbt.current_timestamp()) }}
group by 1
{% endtest %}
```

```yaml
# models/marts/_marts__models.yml
models:
  - name: fct_orders
    columns:
      - name: ordered_at
        data_tests:
          - not_in_future:
              arguments:
                grace_minutes: 10
              config:
                severity: warn
```
Sample: one order with `ordered_at` two days in the future (a timezone bug) returns one failing group, and the test warns.

<p class="qb-label">Real-world use case</p>

Five teams had each written their own singular "no future timestamps" tests with inconsistent tolerances. A shared `not_in_future` generic test in the internal package standardised the rule across 140 timestamp columns.

<p class="qb-label">Common mistakes and red flags</p>

- Hard-coding table names inside generic tests.
- Adapter-specific SQL in shared tests.
- Reinventing tests packages already provide.

<p class="qb-label">Follow-up questions</p>

- Where can generic tests live? — `tests/generic/` or `macros/` (or a package).
- How do you override a built-in test? — Define a test with the same name in your project; project definitions take precedence.

</details>

### 19. Unit tests

<p class="qb-meta">Intermediate · Common · Testing</p>

<p class="qb-label">Short answer</p>

Unit tests (dbt 1.8+) check a model's transformation logic on small, hand-crafted input rows (`given`), comparing the output to `expect`. They run before the model is built, against mocked inputs, not production data. Data tests check the data that was actually built. Use unit tests for tricky SQL: CASE logic, window functions, date math, regex parsing and incremental logic.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Structure (YAML `unit_tests:`)**
- `model`: the model under test.
- `given`: a list of inputs (`ref('...')` or `source('...')`), each with `rows`, given inline as dicts, CSV, or SQL.
- `expect`: the expected output rows. Only the columns you list are compared.
- `overrides`: mock `vars`, `env_vars` or macros (for example `is_incremental: true`) to test incremental branches.

**Unit vs data tests**
| Aspect | Unit test | Data test |
|---|---|---|
| Input | Fixed fixtures | Real built data |
| When | Before/without building model (in CI, dev) | After building |
| Checks | Logic correctness | Data quality/invariants |
| Speed/cost | Small, cheap queries (still run on warehouse) | Can scan large tables |

**Running**
- `dbt test --select test_type:unit`, or as part of `dbt build` (unit tests run before the model is materialised).
- In CI, run unit tests for modified models (`--select state:modified,test_type:unit`; see [Slim CI with state:modified and `--defer`](#44-slim-ci-with-statemodified-and---defer)).
- Production runs often exclude them (`--exclude-resource-type unit_test`) to save time, since logic was already verified in CI.

**Limitations**
- They still need a warehouse connection, because fixtures are compiled into SQL and executed there.
- Only direct inputs are mocked. You test one model at a time.
- Some adapter-specific types or functions need care in fixtures. Introspective macros that query the database may need overrides.

<p class="qb-label">Example</p>

```yaml
# models/marts/_fct_orders__unit_tests.yml
unit_tests:
  - name: order_status_bucketing_and_net_amount
    model: fct_orders
    given:
      - input: ref('stg_shop__orders')
        rows:
          - {order_id: 1, customer_id: 10, order_status: "delivered", order_total: 100.00, discount: 10.00}
          - {order_id: 2, customer_id: 11, order_status: "returned",  order_total: 50.00,  discount: 0.00}
          - {order_id: 3, customer_id: 12, order_status: "cancelled", order_total: 20.00,  discount: 0.00}
    expect:
      rows:
        - {order_id: 1, status_bucket: "completed", net_amount: 90.00}
        - {order_id: 2, status_bucket: "reversed",  net_amount: 0.00}
        - {order_id: 3, status_bucket: "reversed",  net_amount: 0.00}

  - name: incremental_branch_filters_old_rows
    model: fct_orders
    overrides:
      macros:
        is_incremental: true
    given:
      - input: this
        rows: [{order_id: 1, updated_at: "2026-03-01 10:00:00"}]
      - input: ref('stg_shop__orders')
        rows:
          - {order_id: 1, updated_at: "2026-03-01 09:00:00"}
          - {order_id: 5, updated_at: "2026-03-02 08:00:00"}
    expect:
      rows:
        - {order_id: 5}
```
The second test assumes the model's incremental filter has no lookback window, so the older row is excluded. Adjust the fixtures to match your model's actual lookback.

<p class="qb-label">Real-world use case</p>

A refactor of revenue bucketing logic broke the "returned" handling, but data tests passed because the data was valid, just wrong. A unit test with three fixture rows would have caught it, so the team now requires unit tests for any mart with CASE-heavy business logic.

<p class="qb-label">Common mistakes and red flags</p>

- Confusing unit tests with data tests.
- Huge fixtures copied from production.
- Running unit tests in every production run unnecessarily.

<p class="qb-label">Follow-up questions</p>

- Can you test ephemeral inputs? — Mock the direct refs of the model; ephemeral models referenced are mocked like refs.
- What does `input: this` mean? — Mocks the current state of the incremental model's existing table.

</details>

### 20. Test severity, warn_if/error_if and store_failures

<p class="qb-meta">Intermediate · Common · Testing</p>

<p class="qb-label">Short answer</p>

`severity: error` (default) fails the run when a test fails. `severity: warn` only logs a warning. `warn_if` and `error_if` set thresholds on the failing-row count (for example `">10"`), so a few known bad rows warn while many fail. `store_failures: true` saves failing rows to a table in the warehouse (or `store_failures_as: view`) so you can investigate after the run.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**How thresholds evaluate**
- dbt counts failing rows (`fail_calc`, default `count(*)`).
- With `severity: error`, it first checks `error_if` (default `!= 0`). If that is true, the test errors. Otherwise it checks `warn_if` (default `!= 0`) and warns if true.
- With `severity: warn`, it only checks `warn_if`.
- `fail_calc` can be customised, for example `sum(case when ... then 1 else 0 end)`.

**Typical policies**
| Situation | Config |
|---|---|
| Primary key uniqueness | `severity: error` (default) |
| Known legacy dirty data, tolerate up to N | `error_if: ">100"`, `warn_if: ">0"` |
| Soft business rule / monitoring only | `severity: warn` |
| Non-critical source checks | `severity: warn` + alerting |

**store_failures**
- Writes failing rows to `<target schema>_dbt_test__audit.<test_name>` by default (configurable `schema`).
- Overwritten each run. Snapshot or copy it if you need history.
- Watch for **PII exposure**: failure tables contain real rows, so restrict access to the audit schema.
- `store_failures_as: ephemeral | table | view`.

**Behaviour in builds**
- In `dbt build`, a failing **error** test **skips downstream models** of the tested model, which prevents bad data from propagating (a circuit breaker). Warnings don't skip.
- `--warn-error` and `--warn-error-options` can promote warnings to errors in CI.

<p class="qb-label">Example</p>

```yaml
models:
  - name: stg_crm__contacts
    columns:
      - name: email
        data_tests:
          - not_null:
              config:
                severity: error
                warn_if: ">0"
                error_if: ">500"
                store_failures: true
                where: "created_at >= dateadd('day', -30, current_date)"
      - name: phone_country_code
        data_tests:
          - accepted_values:
              arguments:
                values: ['+1', '+44', '+91', '+49']
              config:
                severity: warn
```

<p class="qb-label">Real-world use case</p>

A CRM source had 120 historical contacts with NULL emails that couldn't be fixed. The `not_null` test was set to warn above 0 and error above 500. That kept the nightly run green while still alerting the team if a new bug caused a spike.

<p class="qb-label">Common mistakes and red flags</p>

- Everything on warn, so tests are ignored.
- `store_failures` exposing PII broadly.
- Not knowing that error tests skip downstream nodes in build.

<p class="qb-label">Follow-up questions</p>

- How do you alert on warnings? — Parse `run_results.json` (see [dbt artifacts: manifest.json and run_results.json](#34-dbt-artifacts-manifestjson-and-run_resultsjson)) or use observability packages/platform notifications.
- What's `limit` on tests? — Caps returned failing rows for performance.

</details>

### 21. Source freshness

<p class="qb-meta">Intermediate · Common · Sources</p>

<p class="qb-label">Short answer</p>

`dbt source freshness` checks how recently each source table was loaded. It compares `max(loaded_at_field)` (or warehouse metadata, where the adapter supports it) against `warn_after` and `error_after` thresholds defined in the sources YAML. Results go to `sources.json`. Running it before transformations lets you stop or flag a pipeline when upstream data is stale, instead of publishing stale dashboards.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Configuration**
- `loaded_at_field`: a timestamp column set by the loader (for example `_fivetran_synced`, `_loaded_at`). Prefer load time over business event time.
- `freshness: {warn_after: {count, period}, error_after: {count, period}}`, set at source or table level (tables inherit).
- `filter:` limits the scan on huge tables (`filter: "_loaded_at >= dateadd(day, -3, current_date)"`).
- Without `loaded_at_field`, some adapters (Snowflake, BigQuery, Redshift, Databricks) can use **table metadata** (last altered time). Check adapter support.
- `freshness: null` disables freshness for specific tables, such as static ones.

**Running it**
- `dbt source freshness` (all), or `--select "source:shop"`.
- Results: pass, warn or error per table, with the max loaded time and age.
- In dbt platform jobs, a "run source freshness" step. In orchestrators, a task before `dbt build`.
- Selection by freshness: `dbt build --select "source_status:fresher+"` builds only models downstream of sources that got new data since the last check (needs previous state artifacts).

**Using results**
- Fail fast: an error on a critical source stops downstream builds, or alerts.
- Publish freshness to dashboards ("data as of"), and to the data catalog.
- Feed SLA monitoring (see [What are the dimensions of data quality?](/career/data-engineering-interview-questions/#15-what-are-the-dimensions-of-data-quality)).

<p class="qb-label">Example</p>

```yaml
# models/staging/payments/_payments__sources.yml
sources:
  - name: payments
    database: RAW
    schema: STRIPE
    loaded_at_field: _loaded_at
    freshness:
      warn_after: {count: 2, period: hour}
      error_after: {count: 6, period: hour}
    tables:
      - name: charges
        freshness:
          filter: "_loaded_at >= dateadd('day', -2, current_timestamp())"
      - name: refunds
      - name: currency_codes
        freshness: null
```

```bash
dbt source freshness --select "source:payments"
dbt build --select "source_status:fresher+" --state ./prod-artifacts
```

<p class="qb-label">Real-world use case</p>

An EL connector silently stopped syncing on a Friday night, and the Monday revenue dashboard showed a flat weekend. After adding source freshness with `error_after: 6 hours`, as a gating step that pages on-call, the next connector failure was caught within hours.

<p class="qb-label">Common mistakes and red flags</p>

- Using business event time as `loaded_at_field` (late events look stale).
- Freshness configured but never run.
- Full scans of huge tables without `filter`.

<p class="qb-label">Follow-up questions</p>

- Does freshness block `dbt build` automatically? — No; you orchestrate it as a separate step and decide behaviour.
- Freshness for models (not sources)? — Newer dbt versions add model freshness configs; otherwise use `dbt_utils.recency` tests.

</details>

### 22. Writing macros

<p class="qb-meta">Intermediate · Very Common · Jinja</p>

<p class="qb-label">Short answer</p>

Macros are reusable Jinja functions defined with `{% macro name(args) %} ... {% endmacro %}` in the `macros/` folder. You call them in models as `{{ name(args) }}`. Use them for repeated SQL snippets (currency conversion, surrogate keys, standard audit columns), environment logic, and operations run with `dbt run-operation`. Keep them small, documented and tested.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Kinds of macros**
| Type | Example |
|---|---|
| Expression snippets | `cents_to_dollars('amount_cents')` |
| Column/list generators | Standard audit columns, generate column lists from `adapter.get_columns_in_relation` |
| Overrides of dbt behaviour | `generate_schema_name` (see [Custom schemas and generate_schema_name](#31-custom-schemas-and-generate_schema_name)), `generate_alias_name` |
| Operations | `dbt run-operation grant_select --args '{role: analyst}'` (DDL/admin tasks) |
| Hooks | Called from pre/post-hooks (see [Hooks: pre-hook, post-hook, on-run-start/end](#25-hooks-pre-hook-post-hook-on-run-startend)) |
| Cross-database | `adapter.dispatch` implementations (see [Cross-database macros and adapter.dispatch](#49-cross-database-macros-and-adapterdispatch)) |

**Best practices**
- **Single purpose and clear arguments,** with defaults where sensible.
- **Document** in YAML (`macros:` with `description` and `arguments`).
- **Return SQL fragments,** not whole models, so they stay composable.
- **Whitespace control** (`{%-`) for clean compiled SQL.
- **Test** through unit tests on models that use them, or dedicated test models.
- **Prefer packages** (`dbt_utils.generate_surrogate_key`, `dbt.dateadd`) over writing your own.
- Avoid deep macro nesting, which makes debugging painful.

**`return()` vs output:** macros normally render text. `{{ return(value) }}` returns a Python object (list or dict) to the caller, which is useful for helper macros that compute values.

**Security:** macros that build SQL from `var()` or arguments should only be used with trusted inputs (project config or orchestrator). Avoid passing raw user input into identifiers. Use `adapter.quote()` for identifiers where needed.

<p class="qb-label">Example</p>

```sql
-- macros/money.sql
{% macro cents_to_currency(column_name, scale=2) -%}
    round(({{ column_name }})::number(38, 4) / 100, {{ scale }})
{%- endmacro %}

-- macros/audit_columns.sql
{% macro audit_columns() -%}
    current_timestamp()             as dbt_loaded_at,
    '{{ invocation_id }}'           as dbt_invocation_id
{%- endmacro %}
```

```sql
-- models/staging/stripe/stg_stripe__charges.sql
select
    id                                   as charge_id,
    {{ cents_to_currency('amount') }}    as amount,
    currency,
    {{ audit_columns() }}
from {{ source('stripe', 'charges') }}
```
Compiled output: `round((amount)::number(38, 4) / 100, 2) as amount`, plus the two audit columns with the run's invocation ID.

<p class="qb-label">Real-world use case</p>

Currency conversion was copy-pasted into 30 models with slightly different rounding, which caused reconciliation differences of a few cents. A single macro made the logic consistent, and a later rounding change took one line.

<p class="qb-label">Common mistakes and red flags</p>

- Macros that hide whole business logic models.
- No documentation.
- Unquoted dynamic identifiers from untrusted input.

<p class="qb-label">Follow-up questions</p>

- How do you run a macro standalone? — `dbt run-operation macro_name --args '{...}'`.
- What's `invocation_id`? — A unique ID for the dbt run, useful for audit columns and lineage.

</details>

### 23. var() and env_var()

<p class="qb-meta">Intermediate · Common · Configuration</p>

<p class="qb-label">Short answer</p>

`{{ var('name', default) }}` reads project variables. They are defined in `dbt_project.yml` under `vars:` and overridden at runtime with `--vars '{"name": value}'`, which suits parameters such as start dates or feature flags. `{{ env_var('NAME', default) }}` reads environment variables, which suit secrets and environment-specific settings such as credentials and account names. In dbt platform, variables prefixed `DBT_ENV_SECRET_` are masked in logs.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**var()**
- Defined in `dbt_project.yml`, globally or scoped to a package or project:
  ```yaml
  vars:
    start_date: "2024-01-01"
    acme_analytics:
      enable_sampling: false
  ```
- Overridden on the CLI: `dbt build --vars '{"start_date": "2026-01-01"}'`.
- Use it for **business parameters** (lookback days, backfill ranges, currency lists) and package configuration (packages often read vars).
- Missing vars without defaults raise compile errors, which is good for required parameters.

**env_var()**
- Reads OS environment variables at parse or compile time.
- Use it for **credentials** in profiles.yml, account identifiers, and environment-specific database names.
- Secrets: in dbt platform, `DBT_ENV_SECRET_*` values are masked and only usable in profiles and Git config. Locally, rely on your secret manager to export them.
- Changing env vars can trigger a **full re-parse** in some cases, since parsing depends on them.

**Choosing**
| Use | Prefer |
|---|---|
| Passwords, keys, tokens | `env_var` (secrets manager) |
| Target-specific connection info | `env_var` or profiles targets |
| Backfill dates, thresholds, flags | `var` |
| Logic depending on environment name | `target.name` (no var needed) |

**Pitfalls**
- Putting secrets in `vars` (they appear in `manifest.json` and logs).
- String vs number types from the CLI. `--vars` is YAML-parsed, so quote dates.
- Overusing vars until models become unreadable configuration puzzles.
- Vars in SQL must be **trusted input**. Don't pass end-user input into vars that are rendered into SQL.

<p class="qb-label">Example</p>

```sql
-- models/marts/fct_daily_kpis.sql
{% set lookback_days = var('kpi_lookback_days', 3) %}

select kpi_date, sum(revenue) as revenue, count(distinct customer_id) as buyers
from {{ ref('fct_orders') }}
where kpi_date >= dateadd('day', -{{ lookback_days | int }}, current_date)
group by kpi_date
```

```bash
export DBT_SF_ACCOUNT="acme-xy12345"               # read via env_var in profiles.yml
dbt build --select fct_daily_kpis --vars '{"kpi_lookback_days": 30}'
```
The `| int` filter forces the var to an integer, so a malformed value fails at compile time instead of injecting arbitrary text into the SQL.

<p class="qb-label">Real-world use case</p>

A backfill used to need code edits to change hard-coded dates. Introducing `var('backfill_start')` and `var('backfill_end')` with safe defaults let on-call engineers run controlled backfills from the orchestrator without code changes.

<p class="qb-label">Common mistakes and red flags</p>

- Secrets in vars or Git.
- Unquoted dates in `--vars`.
- Vars used to switch whole model logic everywhere.

<p class="qb-label">Follow-up questions</p>

- Where are vars visible? — In compiled SQL, manifest and logs — never for secrets.
- What's the precedence? — CLI `--vars` override project `vars:`; package-scoped vars apply within that package.

</details>

### 24. Packages: dbt_utils, dbt_expectations and packages.yml

<p class="qb-meta">Intermediate · Common · Packages</p>

<p class="qb-label">Short answer</p>

Packages are dbt projects you install as dependencies. List them in `packages.yml` (or `dependencies.yml`) with version ranges, then run `dbt deps`. `dbt_utils` adds core helper macros and tests (surrogate keys, date spines, `unique_combination_of_columns`, `expression_is_true`). `dbt_expectations` brings Great Expectations-style tests. Others cover specific sources (Fivetran packages), observability (elementary), audit (`audit_helper`) and codegen.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Installing**
- `packages.yml`: hub packages (`package: dbt-labs/dbt_utils`, `version: [">=1.3.0", "<2.0.0"]`), Git packages (`git:` URL plus `revision:` tag or commit), local packages (`local: ../shared`), and private packages (`private:`, for dbt platform).
- `dbt deps` installs into `dbt_packages/` (git-ignored) and writes `package-lock.yml` for reproducible installs. Commit the lock file.

**Common packages**
| Package | Highlights |
|---|---|
| `dbt_utils` | `generate_surrogate_key`, `date_spine`, `star`, `pivot`, `union_relations`, `deduplicate`, tests: `unique_combination_of_columns`, `expression_is_true`, `accepted_range`, `recency`, `equal_rowcount` |
| `dbt_expectations` (community fork maintained by Metaplane) | `expect_column_values_to_be_between`, `..._to_match_regex`, row count expectations, distribution checks |
| `audit_helper` | Compare old vs new model outputs (refactoring validation; see [Refactoring a legacy SQL script into dbt models](#36-refactoring-a-legacy-sql-script-into-dbt-models)) |
| `codegen` | Generate source YAML and base models |
| `elementary` | Observability: test results, anomaly detection, reports |
| `dbt_project_evaluator` | Lints project structure against best practices |
| Source-specific packages | Pre-built staging/marts for SaaS sources |

**Governance**
- **Pin version ranges.** Unpinned Git branches break builds unexpectedly.
- Review the package code. It runs SQL in your warehouse with your role.
- Check adapter support: some packages implement macros only for certain warehouses.
- Watch the maintenance status. Some packages have moved or been forked, and dbt Fusion compatibility varies by package.

<p class="qb-label">Example</p>

```yaml
# packages.yml
packages:
  - package: dbt-labs/dbt_utils
    version: [">=1.3.0", "<2.0.0"]
  - package: metaplane/dbt_expectations
    version: [">=0.10.0", "<0.11.0"]
  - package: dbt-labs/audit_helper
    version: [">=0.12.0", "<1.0.0"]
```

```sql
-- models/marts/dim_customer.sql
select
    {{ dbt_utils.generate_surrogate_key(['source_system', 'customer_id']) }} as customer_key,
    source_system, customer_id, customer_name, segment
from {{ ref('int_customers_unioned') }}
```

```yaml
models:
  - name: fct_orders
    data_tests:
      - dbt_utils.expression_is_true:
          arguments:
            expression: "net_amount <= gross_amount"
    columns:
      - name: net_amount
        data_tests:
          - dbt_expectations.expect_column_values_to_be_between:
              arguments:
                min_value: 0
                max_value: 1000000
```

<p class="qb-label">Real-world use case</p>

A team wrote its own surrogate-key macro with inconsistent NULL handling. Switching to `dbt_utils.generate_surrogate_key` (which handles NULLs consistently and is cross-database) removed a class of join mismatches.

<p class="qb-label">Common mistakes and red flags</p>

- Unpinned package versions.
- Not committing the lock file.
- Installing packages without reviewing what they run.

<p class="qb-label">Follow-up questions</p>

- How do you use a macro from a package? — Prefix with the package name, e.g. `dbt_utils.star(...)`.
- How do you share internal macros across projects? — Internal Git/private package, or dbt Mesh for models (see [dbt Mesh: cross-project refs](#48-dbt-mesh-cross-project-refs)).

</details>

### 25. Hooks: pre-hook, post-hook, on-run-start/end

<p class="qb-meta">Intermediate · Common · Hooks</p>

<p class="qb-label">Short answer</p>

Hooks run SQL around dbt operations. `pre-hook` and `post-hook` run before and after a specific model, seed or snapshot is built, for example setting session parameters or applying grants. `on-run-start` and `on-run-end` run once at the beginning and end of an invocation, for example creating audit tables or logging results. They can call macros and use context such as `this` and `results`.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Where they're defined**
- Per model: `{{ config(pre_hook=[...], post_hook=[...]) }}`, or in YAML or `dbt_project.yml` folder configs (`+post-hook`).
- Project level: `on-run-start:` and `on-run-end:` in `dbt_project.yml`.
- Hooks from folder configs and model configs are combined. Order matters, and they run in the model's transaction context where applicable.

**Typical uses**
| Hook | Use |
|---|---|
| `pre-hook` | `alter session set ...` (timezone, query tag), temporary setup |
| `post-hook` | Legacy grants, `ANALYZE`, clustering/optimize statements, data masking application, audit inserts |
| `on-run-start` | Create audit schema/table, set run-level tags |
| `on-run-end` | Log `results` (status, timing per node) to an audit table; send summaries; vacuum/optimize maintenance |

**Prefer native configs where available**
- **Grants:** use the `grants` config (see [grants and persist_docs configs](#32-grants-and-persist_docs-configs)) instead of post-hook GRANT statements.
- **Query tags (Snowflake):** the `query_tag` config.
- **Docs to warehouse comments:** `persist_docs`.
- **Clustering:** adapter configs (`cluster_by`, `liquid_clustered_by`).

**Cautions**
- Hooks run with dbt's role, so make sure they're safe and idempotent.
- Heavy post-hooks (OPTIMIZE on big tables each run) can dominate runtime.
- Hooks fail the model if they error, which may be surprising.
- `on-run-end` runs even on failures. Use `results` to branch.

<p class="qb-label">Example</p>

```yaml
# dbt_project.yml (excerpt)
on-run-start:
  - "create table if not exists {{ target.database }}.audit.dbt_run_log (invocation_id string, node string, status string, execution_time float, logged_at timestamp)"

on-run-end:
  - "{{ log_run_results(results) }}"

models:
  acme_analytics:
    marts:
      +pre-hook: "alter session set timezone = 'UTC'"
```

```sql
-- macros/log_run_results.sql
{% macro log_run_results(results) %}
  {% if execute and results | length > 0 %}
    insert into {{ target.database }}.audit.dbt_run_log
    values
    {% for r in results -%}
      ('{{ invocation_id }}', '{{ r.node.unique_id }}', '{{ r.status }}', {{ r.execution_time }}, current_timestamp())
      {%- if not loop.last %},{% endif %}
    {% endfor %}
  {% else %}
    select 1
  {% endif %}
{% endmacro %}
```
Node IDs and statuses come from dbt itself, not user input. Still, keep logged values to safe fields like these, and avoid logging free-text error messages unescaped.

<p class="qb-label">Real-world use case</p>

An `on-run-end` hook writing per-model status and timing to an audit table fed a dashboard of the slowest and most-failing models. That guided a performance sprint that cut nightly runtime by 35%.

<p class="qb-label">Common mistakes and red flags</p>

- Using hooks for grants when the `grants` config exists.
- Expensive maintenance in every post-hook.
- Non-idempotent hooks.

<p class="qb-label">Follow-up questions</p>

- Do hooks run in `dbt compile`? — No; they run during execution commands (run/build/seed/snapshot).
- Can you disable inherited hooks for one model? — Override the config for that model (e.g. set to an empty list).

</details>

### 26. Exposures

<p class="qb-meta">Intermediate · Occasional · Documentation & Lineage</p>

<p class="qb-label">Short answer</p>

Exposures declare downstream uses of dbt models (dashboards, notebooks, ML models, applications, reverse-ETL syncs) in YAML, with `depends_on`, owner, type, maturity and URL. They appear in the docs lineage graph and can be selected (`dbt build --select +exposure:weekly_revenue_dashboard`). Before changing a model you can see who would be affected, and you can build everything a critical dashboard needs.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Fields**
- `name`, `label`, `type` (`dashboard`, `notebook`, `analysis`, `ml`, `application`).
- `owner` (name and email, required), `maturity` (low, medium, high), `url`, `description`.
- `depends_on`: a list of `ref()`, `source()` or `metric()` entries.
- `tags` and `meta` (for example a business-critical tier or SLA).

**Uses**
- **Impact analysis:** `dbt ls --select +exposure:executive_kpis` lists the upstream models. Conversely, before deprecating `fct_orders`, check which exposures depend on it.
- **Targeted builds:** `dbt build --select +exposure:executive_kpis` builds and tests everything feeding a critical dashboard, for example in a priority job that runs first.
- **Ownership and communication:** owners get notified of breaking changes, and deprecations are planned with them.
- **Documentation:** the docs site shows the exposure node at the right edge of the lineage graph.

**Keeping them current**
- Manually maintained exposures go stale. Some BI integrations (dbt platform with Tableau or Power BI, or catalog tools) can auto-generate them. Otherwise, add exposure updates to the dashboard release checklist.
- Lineage in catalogs (Unity Catalog, Snowflake Horizon, OpenLineage tools) can complement dbt exposures (see [What is data lineage, and how is it captured?](/career/data-engineering-interview-questions/#66-what-is-data-lineage-and-how-is-it-captured)).

<p class="qb-label">Example</p>

```yaml
# models/exposures.yml
exposures:
  - name: executive_kpis
    label: Executive KPI Dashboard
    type: dashboard
    maturity: high
    url: https://bi.acme.example/dashboards/exec-kpis
    description: "Daily revenue, orders, active customers for the leadership team."
    owner:
      name: BI Platform Team
      email: bi-platform@acme.example
    depends_on:
      - ref('fct_orders')
      - ref('dim_customers')
      - ref('agg_daily_kpis')
    meta:
      tier: 1
      sla: "07:00 UTC"
```

```bash
dbt build --select +exposure:executive_kpis --target prod
dbt ls --select +exposure:executive_kpis --resource-type model
```

<p class="qb-label">Real-world use case</p>

Before renaming columns in `dim_customers`, an engineer checked exposures, found 3 tier-1 dashboards depending on it, and coordinated changes with their owners. A separate early-morning job builds `+exposure:executive_kpis` first, to protect the 07:00 SLA.

<p class="qb-label">Common mistakes and red flags</p>

- Exposures with no owner.
- Stale `depends_on`.
- Thinking exposures are executable objects (they're metadata).

<p class="qb-label">Follow-up questions</p>

- Do exposures run anything? — No; they're nodes for lineage and selection.
- Can exposures depend on metrics? — Yes, `metric('...')` references where Semantic Layer metrics are defined.

</details>

### 27. Node selection syntax (+, @, tags, paths, state)

<p class="qb-meta">Intermediate · Very Common · Commands</p>

<p class="qb-label">Short answer</p>

`--select` and `--exclude` choose which nodes run. `model+` means the model and its descendants, `+model` the model and its ancestors, and `2+model` limits ancestors to depth 2. `@model` means the model, its descendants, and all ancestors of those descendants. Methods include `tag:`, `path:`, `source:`, `exposure:`, `resource_type:`, `config.materialized:`, `test_type:`, `state:modified`, and `result:error`. Spaces mean union, and commas mean intersection.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Graph operators**
| Syntax | Selects |
|---|---|
| `fct_orders` | Just the node |
| `fct_orders+` / `+fct_orders` | Node + all descendants / ancestors |
| `fct_orders+1` / `2+fct_orders` | Depth-limited |
| `@fct_orders` | Node, descendants, and ancestors of descendants (everything needed to build descendants) |
| `+exposure:exec_kpis` | Everything upstream of an exposure |

**Methods**
| Method | Example |
|---|---|
| `tag:` | `tag:finance` |
| `path:` / folder | `path:models/marts/finance` or `models/marts/finance` |
| `source:` | `source:stripe+` (everything downstream of the source) |
| `resource_type:` | `resource_type:snapshot` |
| `config.<key>:` | `config.materialized:incremental` |
| `test_type:` | `test_type:unit`, `test_type:data` |
| `state:` | `state:modified`, `state:new`, `state:modified.body` (needs `--state`) |
| `result:` | `result:error+` (needs prior run_results in `--state`) |
| `source_status:` | `source_status:fresher+` (see [Source freshness](#21-source-freshness)) |
| `group:` / `access:` / `version:` | Governance-based selection |
| `package:` | `package:dbt_utils` |
| Wildcards | `stg_stripe__*` |

**Set operations**
- **Union:** space-separated, `--select "tag:finance tag:marketing"`.
- **Intersection:** comma, `--select "tag:daily,config.materialized:incremental"`.
- **Exclusion:** `--exclude "tag:deprecated"`.

**YAML selectors** (`selectors.yml`): named, reusable selection definitions such as `--selector nightly_finance`, with `union`, `intersection` and `exclude` blocks. They keep orchestration commands short and reviewable.

**Indirect selection:** when selecting models, attached tests are selected too (eager by default). `--indirect-selection cautious` only includes tests whose parents are all selected, and `buildable` and `empty` modes also exist.

<p class="qb-label">Example</p>

```bash
# Everything downstream of changed models, deferring unchanged parents to prod
dbt build --select "state:modified+" --defer --state ./prod-artifacts

# Re-run only what failed last time, plus its children
dbt build --select "result:error+ result:fail+" --state ./target

# Finance incremental models only
dbt run --select "tag:finance,config.materialized:incremental"

# All ancestors needed for an exposure, excluding heavy backfill models
dbt build --select "+exposure:executive_kpis" --exclude "tag:heavy_backfill"
```

```yaml
# selectors.yml
selectors:
  - name: nightly_finance
    definition:
      union:
        - method: tag
          value: finance
          children: true
        - exclude:
            - method: tag
              value: deprecated
```

<p class="qb-label">Real-world use case</p>

A monolithic nightly `dbt build` took 3 hours. Splitting it into YAML selectors per domain, with the critical `+exposure:executive_kpis` running first, let tier-1 dashboards refresh by 06:00 while lower-priority marts ran afterwards.

<p class="qb-label">Common mistakes and red flags</p>

- Confusing comma (intersection) with space (union).
- `state:` selection without `--state`.
- Not knowing indirect test selection.

<p class="qb-label">Follow-up questions</p>

- How do you preview a selection? — `dbt ls --select ...`.
- What does `@` add over `+`? — Ancestors of descendants, so descendants can actually build.

</details>

### 28. Targets, environments and deployment

<p class="qb-meta">Intermediate · Common · Deployment</p>

<p class="qb-label">Short answer</p>

A target is a named connection configuration in a profile (dev, ci, prod), with its own database, schema, role, warehouse and threads. `--target prod` selects it at runtime. Use `target.name` in Jinja for environment-specific behaviour. Typical setup: developers build in personal dev schemas, CI builds in per-PR schemas, and production builds through an orchestrator using a service account.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Typical environment design**
| Environment | Where | Who/what runs | Data |
|---|---|---|---|
| Dev | `ANALYTICS_DEV.dbt_<user>` | Developer locally/IDE | Sampled or deferred to prod |
| CI | `ANALYTICS_CI.dbt_ci_pr_<n>` | CI on pull request | Slim CI with defer (see [Slim CI with state:modified and `--defer`](#44-slim-ci-with-statemodified-and---defer)) |
| Staging/QA (optional) | `ANALYTICS_STAGING` | Release branch | Full or near-full |
| Prod | `ANALYTICS.<schemas>` | Orchestrator, service user/role | Full |

**Practices**
- **Isolation:** separate roles per environment (dev roles can't write production schemas), and separate warehouses for cost control.
- **Same code, different targets.** Avoid branching logic on environment except for safe things: dev sampling, smaller warehouses, disabling expensive exports.
- **Service accounts** with key-pair or OAuth authentication for prod and CI. No personal credentials.
- **Schema naming:** customise `generate_schema_name` so prod uses clean schema names while dev prefixes them per user (see [Custom schemas and generate_schema_name](#31-custom-schemas-and-generate_schema_name)).
- **Promotion:** merge to `main`, then deploy (the orchestrator pulls the new code or image), then run. Use tags or releases for rollback.
- **Artifacts:** save prod `manifest.json` and `run_results.json` for Slim CI and monitoring (see [dbt artifacts: manifest.json and run_results.json](#34-dbt-artifacts-manifestjson-and-run_resultsjson)).

**dbt platform specifics:** environments (development, staging, production) are configured in the UI with credentials and deferral settings. Jobs run in deployment environments, and CI jobs trigger on PRs automatically.

<p class="qb-label">Example</p>

```sql
-- models/staging/app/stg_app__events.sql
select event_id, user_id, event_type, event_ts
from {{ source('app', 'events') }}
{% if target.name == 'dev' %}
where event_ts >= dateadd('day', -3, current_date)      -- small dev builds
{% endif %}
```

```yaml
# profiles.yml (excerpt) — prod output
    prod:
      type: snowflake
      account: "{{ env_var('DBT_SF_ACCOUNT') }}"
      user: SVC_DBT_PROD
      private_key_path: "{{ env_var('DBT_SF_PROD_KEY_PATH') }}"
      role: TRANSFORMER_PROD
      warehouse: TRANSFORM_M
      database: ANALYTICS
      schema: core
      threads: 16
```

<p class="qb-label">Real-world use case</p>

A developer accidentally ran `dbt run` against production with personal credentials, overwriting a mart with a half-finished change. Separating roles (dev roles have no write access to `ANALYTICS`) and running production only through the orchestrator's service account made it impossible to repeat.

<p class="qb-label">Common mistakes and red flags</p>

- Developers with production write access.
- Different SQL logic per environment.
- Personal credentials in prod jobs.

<p class="qb-label">Follow-up questions</p>

- How do you handle dev data volumes? — Sampling with `target.name`, `--empty`, or deferral to prod for upstream models.
- How do you roll back a bad deploy? — Revert the commit and rerun; restore data via Time Travel/clone if needed.

</details>

### 29. dbt Core vs dbt platform (Cloud) vs Fusion engine / dbt v2

<p class="qb-meta">Intermediate · Very Common · Ecosystem</p>

<p class="qb-label">Short answer</p>

dbt Core is the open-source Python CLI. You bring your own orchestration, CI and docs hosting. The dbt platform (formerly dbt Cloud) is the managed SaaS: IDE, Studio, scheduler, CI jobs, Semantic Layer APIs, Catalog, Canvas and Copilot features. The dbt Fusion engine is a Rust-based rewrite of the dbt engine. It parses projects much faster, understands SQL (column-level lineage, compile-time errors) and powers the VS Code extension and dbt v2 tooling.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

| Aspect | dbt Core | dbt platform | dbt Fusion engine |
|---|---|---|---|
| Form | Python package + CLI (Apache 2.0) | Managed SaaS | New engine (Rust); CLI, VS Code/LSP, used in dbt platform |
| Orchestration | Bring your own (Airflow, Dagster, cron, warehouse-native) | Built-in scheduler, job chaining, CI on PR | Same as host (CLI or platform) |
| IDE | Any editor | Studio IDE, Canvas (visual) | VS Code extension with live errors, lineage, previews |
| Semantic Layer | MetricFlow locally | Hosted APIs for BI/AI | Supported per release |
| SQL understanding | Templating only (Jinja → text) | Same + platform features | Parses SQL: column-level lineage, type/compile checks before running |
| Speed | Parse can be slow on large projects | Same engine historically | Much faster parse/compile |
| Licence | Apache 2.0 | Commercial | Source-available / proprietary components, with free tier usage terms |
| Compatibility | Baseline | Baseline | Most projects; some Jinja patterns, packages and adapters may need updates |

**dbt v2 / naming notes**
- dbt Labs has been aligning the product around **dbt (the language and framework)**, the **dbt platform**, and **Fusion**. Expect "dbt Cloud" to be referred to as the dbt platform, and new major-version tooling to be Fusion-based.
- Deprecations: projects are encouraged to fix deprecation warnings (for example `tests:` → `data_tests:`, and test arguments nested under `arguments:`) to be Fusion-ready.

**Other ways to run dbt**
- **Warehouse-native:** Snowflake **dbt Projects on Snowflake** (deploy and run dbt as Snowflake objects, scheduled with tasks), and Databricks **dbt tasks** in Lakeflow Jobs (see [Orchestrating dbt (Airflow/Cosmos, dbt platform jobs, warehouse-native dbt projects)](#33-orchestrating-dbt-airflowcosmos-dbt-platform-jobs-warehouse-native-dbt-projects)).
- Orchestrators: Airflow with Astronomer **Cosmos**, Dagster's dbt integration, Prefect.

**Choosing**
- **Core:** cost-sensitive, strong platform team, an existing orchestrator, full control.
- **Platform:** faster setup, managed CI and scheduling, Semantic Layer APIs, and a less technical contributor base.
- **Fusion:** evaluate it for speed and developer experience. Test package and adapter compatibility first.

<p class="qb-label">Example</p>

```bash
# dbt Core
pip install dbt-core dbt-snowflake
dbt --version
dbt build --target prod

# Fusion CLI (installed separately; command name per current docs)
dbtf --version
dbtf build --target dev
```
The Fusion binary name, `dbtf` here, and its install steps may change between releases. Follow the current dbt docs.

<p class="qb-label">Real-world use case</p>

A 3,000-model project took about 4 minutes just to parse on dbt Core, which slowed every developer iteration. A Fusion pilot parsed it in seconds and surfaced column-level errors before any warehouse query. Two community packages had to be upgraded first.

<p class="qb-label">Common mistakes and red flags</p>

- Calling dbt Cloud the only way to schedule dbt.
- Assuming Fusion is a drop-in for every project without testing.
- Outdated naming without awareness of current terms.

<p class="qb-label">Follow-up questions</p>

- Can Core and platform coexist? — Yes; same project code, different execution environments.
- What's column-level lineage good for? — Impact analysis of renaming/removing columns (see [What is data lineage, and how is it captured?](/career/data-engineering-interview-questions/#66-what-is-data-lineage-and-how-is-it-captured)).

</details>

### 30. Debugging with compiled SQL, logs and the target folder

<p class="qb-meta">Intermediate · Common · Debugging</p>

<p class="qb-label">Short answer</p>

When a model fails, look at `target/compiled/` (rendered SELECT) and `target/run/` (the full DDL/DML dbt actually executed). Run that SQL in the warehouse to reproduce the error. Check `logs/dbt.log` (debug-level details, adapter queries) and use `--debug` for verbose output. `dbt compile --select model` and `dbt show` let you inspect logic without materialising.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Where things are**
| Path | Contents |
|---|---|
| `target/compiled/<project>/models/...sql` | Jinja-rendered SELECT |
| `target/run/<project>/models/...sql` | Executed statements (CREATE/MERGE/INSERT wrappers) |
| `target/manifest.json`, `run_results.json` | Graph and results (see [dbt artifacts: manifest.json and run_results.json](#34-dbt-artifacts-manifestjson-and-run_resultsjson)) |
| `logs/dbt.log` | Detailed logs including adapter-issued queries |

**Workflow**
1. **Read the error message.** dbt shows the node and the database error (for example "invalid identifier", "duplicate row detected during DML action").
2. **Open the compiled or run SQL**, paste it into a SQL editor, and execute pieces of it (individual CTEs) to isolate the issue.
3. **Jinja problems:** `dbt compile --select model` with no warehouse writes. Add `{{ log(var_value, info=True) }}` to print values during compile. Check `execute` guards.
4. **Dependency or selection problems:** `dbt ls --select ...` and `dbt list --resource-type model --output json`.
5. **Incremental logic:** check `is_incremental()` behaviour. The run SQL shows the temp table and MERGE. Reproduce with the existing target state.
6. **Connection or auth:** `dbt debug`.
7. **Performance:** use the warehouse query profile for the specific query ID (Snowflake query tags help find dbt queries; see [MERGE in Snowflake and the nondeterministic-merge error](/snowflake/snowflake-interview-questions/#35-merge-in-snowflake-and-the-nondeterministic-merge-error)).

**Useful flags**
- `--debug` gives verbose logs to the console. `--log-level debug`.
- `--log-format json` for machine-readable logs (shipping to observability tools).
- `--fail-fast` stops early when debugging cascades.
- `--empty` runs a schema-only validation with zero rows.
- `dbt show --select model --limit 10` previews output (see [The analyses folder and dbt show](#14-the-analyses-folder-and-dbt-show)).

**Typical errors and causes**
| Error | Likely cause |
|---|---|
| Compilation Error: model not found | Typo in `ref`, disabled model, wrong package name |
| Duplicate row detected during DML (Snowflake MERGE) | Non-unique `unique_key` in batch (see [unique_key and on_schema_change](#16-unique_key-and-on_schema_change)) |
| Object does not exist or not authorized | Missing grants, wrong target database/role, upstream not built (defer issue) |
| Cycle detected | Two models ref each other; break with intermediate model |

<p class="qb-label">Example</p>

```sql
-- Inspect a value at compile time
{% set cutoff = var('cutoff_date', '2026-01-01') %}
{{ log("fct_orders cutoff_date = " ~ cutoff, info=True) }}

select * from {{ ref('stg_shop__orders') }}
where ordered_at >= '{{ cutoff }}'
```

```bash
dbt compile --select fct_orders
cat target/compiled/acme_analytics/models/marts/finance/fct_orders.sql
dbt build --select fct_orders --debug --fail-fast
```

<p class="qb-label">Real-world use case</p>

An incremental model failed with "duplicate row detected" every few days. The run SQL showed the MERGE source, and running that SELECT revealed two rows per `order_id` when an order had two payment methods. The fix was aggregating payments before the join.

<p class="qb-label">Common mistakes and red flags</p>

- Debugging only the model file without the compiled SQL.
- Not knowing `target/run` exists.
- Changing code randomly instead of isolating CTEs.

<p class="qb-label">Follow-up questions</p>

- How do you tag dbt queries in Snowflake? — `query_tag` config per model/project.
- How do you debug a macro? — `dbt run-operation` with `log()` statements, or compile a model that calls it.

</details>

### 31. Custom schemas and generate_schema_name

<p class="qb-meta">Intermediate · Common · Configuration</p>

<p class="qb-label">Short answer</p>

By default, a model with `+schema: marts` builds into `<target_schema>_marts`, for example `dbt_kumar_marts` in dev or `core_marts` in prod. That concatenation prevents developers from colliding with production. To get clean schema names in prod (just `marts`) and prefixed ones in dev, override the `generate_schema_name` macro so its logic depends on `target.name`.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Default behaviour**
- No custom schema: the model goes into `target.schema`.
- With a custom schema: `target.schema ~ '_' ~ custom_schema`. In dev, `dbt_kumar` plus `marts` gives `dbt_kumar_marts`.
- Why: a custom schema shouldn't let a developer write into the shared production schema.

**Common override**
- **Prod:** use the custom schema name as is (`marts`, `staging`, `finance`).
- **Dev and CI:** keep the prefix (`dbt_kumar_marts`), or put everything in the dev schema.

**Related macros**
- `generate_database_name`: similar logic for custom databases (`+database`).
- `generate_alias_name`: controls table names, for example adding version suffixes or stripping prefixes.

**Cautions**
- Changing the macro on an existing project moves where models are built. Old relations remain, so clean them up, and update grants and BI connections.
- Make sure CI schemas stay isolated (see [Slim CI with state:modified and `--defer`](#44-slim-ci-with-statemodified-and---defer)).
- Avoid logic that lets non-prod targets write to production schemas.
- Document the convention. It's a common source of "where did my table go?" confusion.

**Interview angle:** this question checks whether you understand that dbt's defaults are designed for safety in multi-developer environments, and that you can customise behaviour by overriding built-in macros. The same override pattern applies to many other dbt behaviours.

<p class="qb-label">Example</p>

```sql
-- macros/generate_schema_name.sql
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- elif target.name == 'prod' -%}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {{ default_schema }}_{{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
```
| Target | target.schema | `+schema` | Result |
|---|---|---|---|
| dev | `dbt_kumar` | `marts` | `dbt_kumar_marts` |
| ci | `dbt_ci_pr_42` | `marts` | `dbt_ci_pr_42_marts` |
| prod | `core` | `marts` | `marts` |
| prod | `core` | (none) | `core` |

<p class="qb-label">Real-world use case</p>

Production BI tools pointed to `ANALYTICS.MARTS`, but dbt kept building `ANALYTICS.CORE_MARTS` because of the default concatenation, so teams created manual views to bridge the gap. Overriding `generate_schema_name` for prod removed the bridge views.

<p class="qb-label">Common mistakes and red flags</p>

- Not knowing about the default concatenation.
- Overrides that let dev write to prod schemas.
- Changing the macro without migration and cleanup.

<p class="qb-label">Follow-up questions</p>

- Is there a built-in alternative? — dbt ships `generate_schema_name_for_env` you can call from your override.
- How do you put all dev models in one schema? — Return `target.schema` for non-prod targets regardless of custom schema.

</details>

### 32. grants and persist_docs configs

<p class="qb-meta">Intermediate · Occasional · Governance</p>

<p class="qb-label">Short answer</p>

The `grants` config (for example `+grants: {select: ['reporter_role']}`) makes dbt apply and maintain privileges on each relation it builds. It also revokes privileges not listed, for the privilege types it manages. `persist_docs: {relation: true, columns: true}` writes model and column descriptions into the warehouse as table and column comments, so catalogs and BI tools show the documentation.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**grants**
- Configure in `dbt_project.yml` (folder level), YAML, or model config. Lists merge or override according to config inheritance. Use a `+` prefix on the privilege key (for example `+select`) to add to inherited grants rather than replace them.
- dbt compares current grants with the desired ones after building, and issues GRANT and REVOKE statements. On Snowflake, `copy_grants` can also preserve grants on recreated objects.
- Privilege names follow the adapter: `select` on Snowflake, BigQuery `roles/bigquery.dataViewer`, Databricks `select`.
- Prefer it to hand-written post-hook GRANTs, because it is declarative and idempotent (see [Hooks: pre-hook, post-hook, on-run-start/end](#25-hooks-pre-hook-post-hook-on-run-startend)).
- Platform-level access patterns, such as Snowflake future grants (see [Access-role vs functional-role design and future grants](/snowflake/snowflake-interview-questions/#27-access-role-vs-functional-role-design-and-future-grants)) or Unity Catalog schema-level grants (see [Unity Catalog securables, privileges and inheritance](/databricks/databricks-interview-questions/#23-unity-catalog-securables-privileges-and-inheritance)), may cover the need without per-object grants. Choose one approach to avoid fights between dbt and the platform.

**persist_docs**
- `relation: true` gives a table or view comment. `columns: true` gives column comments.
- Adapter support varies. For example, column comments on Snowflake views have limitations, depending on version.
- Makes documentation visible in Snowsight, Unity Catalog, and BI tools that read comments.
- Write meaningful descriptions (see [Generating documentation and the lineage graph](#13-generating-documentation-and-the-lineage-graph)). Persisting empty docs adds nothing.

**Other related configs**
- `query_tag` (Snowflake): tag dbt queries for cost attribution.
- `tags` and `meta`: governance metadata, for example `meta: {contains_pii: true}`, which macros can use to apply masking policies (Snowflake tag or masking automation).
- Adapter configs for policies, such as dbt-databricks column masks or row filters in newer versions. Check the adapter docs.

<p class="qb-label">Example</p>

```yaml
# dbt_project.yml (excerpt)
models:
  acme_analytics:
    +persist_docs:
      relation: true
      columns: true
    marts:
      +grants:
        select: ["REPORTER", "DATA_SCIENTIST"]
      finance:
        +grants:
          +select: ["FINANCE_ANALYST"]      # '+' adds to inherited list instead of replacing
    staging:
      +grants:
        select: []                          # no consumer access to staging
```

<p class="qb-label">Real-world use case</p>

After each full rebuild, BI users lost access to recreated tables because grants were applied manually. Moving to the `grants` config made access part of the code, and every rebuild reapplied the right roles automatically.

<p class="qb-label">Common mistakes and red flags</p>

- Mixing manual grants with dbt-managed grants (dbt revokes the extras).
- Granting to users instead of roles.
- Relying on persist_docs without writing descriptions.

<p class="qb-label">Follow-up questions</p>

- What happens to grants not in config? — dbt revokes them for the privilege types it manages on that relation.
- How do you apply masking from dbt? — Post-hooks/macros applying masking policies, or tag-based policies in the warehouse driven by `meta`.

</details>

### 33. Orchestrating dbt (Airflow/Cosmos, dbt platform jobs, warehouse-native dbt projects)

<p class="qb-meta">Intermediate · Common · Orchestration</p>

<p class="qb-label">Short answer</p>

There are four main options. dbt platform jobs (scheduled, triggered by API or another job's completion). Airflow, often with Astronomer Cosmos, which renders each dbt model as an Airflow task. Other orchestrators such as Dagster or Prefect. Warehouse-native execution: dbt Projects on Snowflake scheduled with tasks, or dbt tasks in Databricks Lakeflow Jobs. Choose based on where the rest of your pipeline is orchestrated and how much per-model visibility you need.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

| Option | How it runs dbt | Strengths | Trade-offs |
|---|---|---|---|
| dbt platform jobs | Managed runners; schedule, API, job-completion triggers | Low ops, CI integration, artifacts, Catalog | Cross-system orchestration needs API calls from your orchestrator |
| Airflow + `BashOperator`/`DbtCoreOperator` | One task runs `dbt build` | Simple | Whole project as one task — coarse retries/visibility |
| Airflow + Cosmos | Parses project; one Airflow task (or task group) per model/test | Per-model retries, lineage in Airflow UI, mixes with other tasks | Large DAGs in Airflow; parse overhead; environment setup |
| Dagster | Assets mapped from dbt models | Asset-based lineage, partitions, freshness policies | Different paradigm to learn |
| Snowflake dbt Projects | dbt project deployed as a Snowflake object; `EXECUTE DBT PROJECT` scheduled by tasks | No external runtime; governed in Snowflake | Snowflake-only; feature parity evolving |
| Databricks dbt task | dbt task in Lakeflow Jobs on job/serverless compute + SQL warehouse | Native with other Databricks tasks | Databricks-only |

**Design principles**
- **Upstream dependencies:** trigger dbt when the ingestion finishes (sensor, dataset or asset event, job-completion trigger), or gate on source freshness (see [Source freshness](#21-source-freshness)). Avoid "run at 2am and hope".
- **Granularity:** per-domain selectors or per-model tasks for retries and SLAs. Don't make 2,000 Airflow tasks without need.
- **Idempotency:** dbt runs are naturally rerunnable. Pass date vars for backfills (see [How do you make Airflow tasks idempotent and safe to retry?](/career/data-engineering-interview-questions/#50-how-do-you-make-airflow-tasks-idempotent-and-safe-to-retry)).
- **Artifacts:** persist `manifest.json` and `run_results.json` for CI deferral and monitoring (see [dbt artifacts: manifest.json and run_results.json](#34-dbt-artifacts-manifestjson-and-run_resultsjson)).
- **Secrets:** inject them through the orchestrator's secret backend into env vars (see [How do you manage secrets in pipelines?](/career/data-engineering-interview-questions/#28-how-do-you-manage-secrets-in-pipelines)).

<p class="qb-label">Example</p>

```python
# Runtime: Airflow 3.x + astronomer-cosmos
from datetime import datetime
from airflow.sdk import dag
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, RenderConfig
from cosmos.profiles import SnowflakePrivateKeyPemProfileMapping

profile_config = ProfileConfig(
    profile_name="acme_snowflake",
    target_name="prod",
    profile_mapping=SnowflakePrivateKeyPemProfileMapping(conn_id="snowflake_dbt_prod"),
)

@dag(schedule="0 3 * * *", start_date=datetime(2026, 1, 1), catchup=False, tags=["dbt"])
def dbt_finance_nightly():
    DbtTaskGroup(
        group_id="finance",
        project_config=ProjectConfig("/opt/airflow/dbt/acme_analytics"),
        profile_config=profile_config,
        render_config=RenderConfig(select=["tag:finance"]),
        default_args={"retries": 2},
    )

dbt_finance_nightly()
```
Import paths (`airflow.sdk` in Airflow 3, Cosmos profile mapping class names) depend on the installed versions. Check them against your Airflow and Cosmos releases.

<p class="qb-label">Real-world use case</p>

A team ran `dbt build` as one Airflow task, and any failure meant rerunning everything for 2 hours. With Cosmos, each model became a task. Failed models retried individually, and an upstream ingestion task gated the finance task group.

<p class="qb-label">Common mistakes and red flags</p>

- Time-based schedules without upstream dependencies.
- Per-model tasks for huge projects without considering scheduler load.
- Credentials baked into images.

<p class="qb-label">Follow-up questions</p>

- How do you trigger a dbt platform job from Airflow? — dbt Cloud provider operators/API call with job ID, then poll for completion.
- How do you backfill with dbt in Airflow? — Parameterise vars by logical date and run backfills (see [How do you make Airflow tasks idempotent and safe to retry?](/career/data-engineering-interview-questions/#50-how-do-you-make-airflow-tasks-idempotent-and-safe-to-retry)).

</details>

### 34. dbt artifacts: manifest.json and run_results.json

<p class="qb-meta">Intermediate · Common · Metadata</p>

<p class="qb-label">Short answer</p>

dbt writes JSON artifacts to `target/`. `manifest.json` describes the whole project: nodes, configs, dependencies, compiled SQL, docs and tests. `run_results.json` has per-node status, timing, messages and failure counts for the last command. `catalog.json` (from docs generate) has warehouse metadata, and `sources.json` has freshness results. They power Slim CI, observability dashboards, lineage tools and custom governance checks.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

| Artifact | Produced by | Key contents | Uses |
|---|---|---|---|
| `manifest.json` | Most commands (parse/compile/run/build) | Nodes (models, tests, seeds, snapshots, sources, exposures, metrics), `depends_on`, configs, tags, meta, descriptions, compiled code, checksums | State comparison for Slim CI (see [Slim CI with state:modified and `--defer`](#44-slim-ci-with-statemodified-and---defer)), lineage export, linting/governance checks |
| `run_results.json` | run, build, test, seed, snapshot, compile | Per-node `status` (success/error/fail/warn/skipped), `execution_time`, `failures`, `message`, adapter response (rows affected, query id) | Monitoring, alerting, performance tracking (see [Speeding up slow dbt runs](#45-speeding-up-slow-dbt-runs)) |
| `catalog.json` | `docs generate` | Columns, data types, table stats from warehouse | Docs site, schema comparison |
| `sources.json` | `source freshness` | Max loaded_at, age, status per source | Freshness dashboards, `source_status` selection |
| `semantic_manifest.json` | Parse (with semantic layer) | Semantic models and metrics | MetricFlow |

**Practical patterns**
- **Upload after every production run** to object storage or a warehouse table (an `on-run-end` hook or orchestrator step). Keep history for trends.
- **Observability:** load `run_results` into a table, then dashboard failure rates, slowest models and test failures by owner. Packages like elementary automate this.
- **Governance checks in CI:** parse the manifest to enforce "every public model has a description, an owner in meta, and PK tests".
- **Lineage integrations:** OpenLineage and catalog tools ingest the manifest.
- **Schema versions:** artifacts carry `metadata.dbt_schema_version`. Write parsers that tolerate new fields across dbt versions.

<p class="qb-label">Example</p>

```python
# Runtime: Python 3.12 — CI governance check over manifest.json
import json
import sys
from pathlib import Path

manifest = json.loads(Path("target/manifest.json").read_text())
nodes = manifest["nodes"]

tested_columns = {
    (n.get("attached_node"), n.get("column_name"))
    for n in nodes.values()
    if n["resource_type"] == "test" and n.get("test_metadata", {}).get("name") in {"unique", "not_null"}
}

problems = []
for uid, node in nodes.items():
    if node["resource_type"] != "model" or not node["path"].startswith("marts/"):
        continue
    if not node.get("description"):
        problems.append(f"{uid}: missing description")
    if not node.get("meta", {}).get("owner"):
        problems.append(f"{uid}: missing meta.owner")
    if not any(attached == uid for attached, _ in tested_columns):
        problems.append(f"{uid}: no unique/not_null tests")

print("\n".join(problems) or "All mart models pass governance checks")
sys.exit(1 if problems else 0)
```

<p class="qb-label">Real-world use case</p>

A platform team loads `run_results.json` from every production run into a table. A weekly report lists each domain's slowest models and most-failing tests by owner, which made performance and quality work visible to every domain team.

<p class="qb-label">Common mistakes and red flags</p>

- Not persisting artifacts, so Slim CI and history are impossible.
- Brittle parsers broken by new dbt versions.
- Ignoring `skipped` statuses in monitoring.

<p class="qb-label">Follow-up questions</p>

- Where is the compiled SQL in the manifest? — `compiled_code` per node (after compile/run).
- How do you get query IDs per model? — `adapter_response` in `run_results.json` (adapter-dependent, e.g. Snowflake query_id).

</details>

### 35. Designing a test strategy across layers

<p class="qb-meta">Intermediate · Common · Testing</p>

<p class="qb-label">Short answer</p>

Test where each assumption enters. Sources get freshness, plus key and accepted-value checks on raw data. Staging gets primary-key unique and not_null tests, type and format checks, and dedupe. Intermediate gets grain checks after joins (no fan-out). Marts get business rules, relationships, reconciliation against sources or finance totals, and contracts. Add unit tests for complex logic. Tune severity so failures are actionable.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**By layer**
| Layer | Tests | Why |
|---|---|---|
| Sources | Freshness; `unique`/`not_null` on natural keys; accepted values on status codes | Catch upstream breakage early, attribute blame correctly |
| Staging | PK `unique` + `not_null` on every model; type/format (regex, ranges) | Guarantee clean foundation |
| Intermediate | Grain tests (`unique_combination_of_columns`) after joins; row count expectations vs inputs | Prevent fan-out/duplication |
| Marts | Relationships to dims; business invariants (`net <= gross`); reconciliation singular tests; contracts on public models; recency | Protect consumers and KPIs |
| Logic | Unit tests for CASE/window/incremental logic (see [Unit tests](#19-unit-tests)) | Correctness independent of data |
| Exposures | Build/test `+exposure:` first in critical jobs | Protect tier-1 assets |

**Making tests effective**
- **Every model gets a PK test.** This is the highest value per effort.
- **Severity policy:** errors for invariants that must stop the pipeline, warnings for monitoring. Review warnings weekly, or they become noise (see [Test severity, warn_if/error_if and store_failures](#20-test-severity-warn_iferror_if-and-store_failures)).
- **Ownership:** route test failures to the owners (in `meta` or group owners), with alerts in team channels.
- **Cost control:** `where` filters on big tables (last N days), sampling, and running expensive reconciliation tests less often (tags).
- **Avoid test bloat:** don't test the same rule on every layer. Test once where the rule originates, and test outputs where consumers depend on them.
- **Observability:** anomaly detection (volume, freshness, distribution) through packages like elementary, or warehouse-native monitors, complements deterministic tests (see [Data observability vs data quality testing](/career/data-engineering-interview-questions/#45-data-observability-vs-data-quality-testing)).

**CI vs production**
- CI runs unit tests, plus data tests on modified models (Slim CI).
- Production runs data tests in `dbt build` (circuit breaker), plus freshness before the build.

<p class="qb-label">Example</p>

```yaml
# models/intermediate/_int__models.yml
models:
  - name: int_orders_joined_payments
    description: "Grain: one row per order_id (payments aggregated)."
    data_tests:
      - dbt_utils.unique_combination_of_columns:
          arguments:
            combination_of_columns: [order_id]
      - dbt_utils.equal_rowcount:
          arguments:
            compare_model: ref('stg_shop__orders')
```

```sql
-- tests/assert_fct_orders_reconciles_with_payments.sql
{{ config(severity='error', tags=['reconciliation']) }}
select o.ordered_at::date as day, sum(o.amount_paid) as mart_paid, max(p.paid) as source_paid
from {{ ref('fct_orders') }} o
join (select paid_at::date as day, sum(amount) as paid
      from {{ ref('stg_stripe__charges') }} where status = 'succeeded' group by 1) p
  on p.day = o.ordered_at::date
where o.ordered_at >= dateadd('day', -7, current_date)
group by 1
having abs(sum(o.amount_paid) - max(p.paid)) > 0.01
```
The reconciliation test assumes orders and payments fall on the same day. Adjust the join for your settlement timing, or compare cumulative totals over a window.

<p class="qb-label">Real-world use case</p>

A team had 2,000 tests but still missed a fan-out bug that doubled revenue, because no test checked grain after a join. Adding grain tests to every intermediate model, and cutting 600 redundant not_null tests, improved both coverage and runtime.

<p class="qb-label">Common mistakes and red flags</p>

- Lots of not_null tests but no grain tests.
- Everything at warn severity.
- Testing only marts, so failures can't be traced to sources.

<p class="qb-label">Follow-up questions</p>

- How do you measure coverage? — Share of models with PK tests, public models with contracts, and critical columns with rules (manifest analysis; see [dbt artifacts: manifest.json and run_results.json](#34-dbt-artifacts-manifestjson-and-run_resultsjson)).
- Tests vs observability tools? — Tests check known rules; observability detects unknown anomalies.

</details>

### 36. Refactoring a legacy SQL script into dbt models

<p class="qb-meta">Intermediate · Common · Migration</p>

<p class="qb-label">Short answer</p>

First get the legacy script running as one dbt model whose output matches the original. Then replace hard-coded tables with `source()` and `ref()`, and split the logic into staging models (one per source), intermediate CTEs or models, and a final mart. After each step, validate with row counts and `audit_helper` comparisons against the legacy output. Add tests and docs, then cut consumers over.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Step by step**
1. **Lift and shift.** Copy the legacy SQL into a model (`legacy_revenue_report.sql`) unchanged, except for wrapping it as a SELECT. Build it, and confirm it matches the legacy table.
2. **Declare sources.** Replace raw table names with `{{ source(...) }}`, and add source YAML with freshness.
3. **Create staging models.** For each raw table, a `stg_` model with renames, casts and cleaning. Point the legacy model's CTEs at them.
4. **Split the logic.** Turn large CTEs or temp-table steps into intermediate models where they're reusable or testable. Keep small CTEs inline.
5. **Final mart.** Clean naming, explicit columns, a materialisation choice (table, incremental if large).
6. **Validate at each step**, comparing the refactored output with the legacy output:
   - `audit_helper.compare_relations` (matching, missing or extra rows).
   - `compare_column_values` per column to find which column differs.
   - Row counts, sums of key measures, distinct keys.
7. **Add tests:** PK unique and not_null, relationships, business rules, plus unit tests for tricky CASE logic.
8. **Document and cut over:** descriptions and exposures, then point BI to the new model, run both in parallel for a period, and retire the legacy job.

**Handling legacy quirks**
- **Temp tables and procedural steps** become models or CTEs. Row-by-row cursor logic becomes set-based SQL (window functions).
- **UPDATE statements** applied after insert become CASE expressions or later models.
- **Hidden business rules** (magic numbers, hard-coded IDs): extract them into seeds or vars with documentation (see [Seeds](#6-seeds)).
- **Known legacy bugs:** decide explicitly whether to replicate them during validation (bug-for-bug), then fix them in a separate, documented change.

<p class="qb-label">Example</p>

```sql
-- analyses/compare_revenue_report.sql  (uses audit_helper)
{% set old_relation = adapter.get_relation(database='LEGACY', schema='REPORTS', identifier='REVENUE_REPORT') %}
{% set new_relation = ref('fct_revenue_report') %}

{{ audit_helper.compare_relations(
    a_relation = old_relation,
    b_relation = new_relation,
    primary_key = 'order_id'
) }}
```
Expected result summary: `in_a`, `in_b` and counts by match status. 100% `in_a and in_b` for the full row comparison means the refactor is faithful.

<p class="qb-label">Real-world use case</p>

A 1,800-line stored procedure, with 14 temp tables and 6 UPDATE passes, produced the monthly revenue report. Refactoring it into 9 staging, 4 intermediate and 1 mart model, validated with `audit_helper` at each step, surfaced two long-standing bugs. Finance approved the fixes separately after the faithful port was verified.

<p class="qb-label">Common mistakes and red flags</p>

- Rewriting everything at once without validation.
- Fixing bugs during the port without telling stakeholders.
- Leaving hard-coded table names.

<p class="qb-label">Follow-up questions</p>

- How do you migrate stored procedures with loops? — Replace with set-based SQL; for genuinely procedural needs, Python models or keep a procedure called by a hook temporarily.
- How long do you run old and new in parallel? — Through at least one full business cycle (e.g. month-end close).

</details>

### 37. Common dbt anti-patterns

<p class="qb-meta">Intermediate · Common · Best Practices</p>

<p class="qb-label">Short answer</p>

Common anti-patterns: hard-coded table names instead of `ref()`/`source()`; sources referenced from many models; business logic in staging; models without primary-key tests; giant 1,000-line models or deep ephemeral chains; incremental models without a `unique_key` or lookback; heavy views referenced everywhere; untested macros full of business logic; secrets in code; and running `run` then `test` instead of `build`.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

| Anti-pattern | Why it hurts | Fix |
|---|---|---|
| Hard-coded relations | Breaks DAG, environments, lineage | `ref()` / `source()` (see [ref() vs source()](#3-ref-vs-source)) |
| Same source in many models | Inconsistent cleaning, hard to change | One staging model per source table (see [Project layering: staging, intermediate and marts](#10-project-layering-staging-intermediate-and-marts)) |
| Marts referencing sources | Skips cleaning layer | Route through staging |
| No PK tests | Duplicates go unnoticed | `unique` + `not_null` everywhere (see [The four built-in generic data tests](#7-the-four-built-in-generic-data-tests)) |
| Monster models | Unreadable, untestable, slow to debug | Split into intermediate models; CTE structure |
| Deep ephemeral chains | Huge compiled SQL; can't debug pieces | Views/tables for important steps (see [Materializations: view vs table vs incremental vs ephemeral vs materialized_view](#4-materializations-view-vs-table-vs-incremental-vs-ephemeral-vs-materialized_view)) |
| Incremental without unique_key/lookback | Duplicates or missed late data | Merge + lookback (see [Incremental models and is_incremental()](#15-incremental-models-and-is_incremental), [Late-arriving data in incremental models (lookback windows)](#47-late-arriving-data-in-incremental-models-lookback-windows)) |
| Everything incremental | Complexity without need | Tables until scale demands otherwise |
| Heavy views reused widely | Repeated computation, slow BI | Materialise as tables (see [Speeding up slow dbt runs](#45-speeding-up-slow-dbt-runs)) |
| Jinja-heavy business logic in macros | Hidden logic, hard review | Keep logic in models; macros for small reusable snippets |
| `select *` in marts | Accidental schema changes reach consumers | Explicit columns, contracts for public models (see [Model contracts](#41-model-contracts)) |
| Secrets in profiles/vars committed | Security risk | `env_var` + secret manager (see [var() and env_var()](#23-var-and-env_var)) |
| `run` then `test` in prod | Bad data propagates before tests run | `dbt build` (see [dbt run vs build vs test vs compile](#12-dbt-run-vs-build-vs-test-vs-compile)) |
| Full refresh as routine fix | Expensive; risky with limited source retention | Fix logic; targeted backfills; `full_refresh: false` (see [`--full-refresh`: when and why](#5---full-refresh-when-and-why)) |
| No docs/owners | Nobody knows what models mean | Descriptions + `meta.owner` + CI checks (see [dbt artifacts: manifest.json and run_results.json](#34-dbt-artifacts-manifestjson-and-run_resultsjson)) |
| Circular logic via post-hooks writing to upstream tables | Hidden dependencies | Model dependencies explicitly |
| Dead models | Cost and confusion | Lineage + usage analysis; delete unused |

**Automated detection**
- `dbt_project_evaluator` flags structural issues: direct source references in marts, missing PK tests, model fanout, undocumented models.
- SQLFluff for style.
- Manifest-based CI checks for team-specific rules.
- Warehouse usage data (query history, access history) to find unused models.

**Cultural fixes:** a PR template checklist (tests, docs, materialisation rationale), code owners per domain, and periodic cleanup sprints.

<p class="qb-label">Example</p>

```sql
-- Before (anti-patterns: hard-coded table, raw source in mart, select *, no dedupe)
select *
from RAW.SHOP.ORDERS o
join ANALYTICS.MARTS.DIM_CUSTOMER c on o.customer_id = c.customer_id

-- After (staging via ref, explicit columns, deterministic dedupe upstream)
select
    o.order_id,
    o.customer_id,
    c.customer_segment,
    o.ordered_at,
    o.order_total
from {{ ref('stg_shop__orders') }} o
left join {{ ref('dim_customers') }} c
  on o.customer_id = c.customer_id
```

<p class="qb-label">Real-world use case</p>

Running `dbt_project_evaluator` on a 1,400-model project flagged 230 direct source references in marts, 310 models without PK tests and 90 unused models. A quarter-long cleanup cut runtime by 25% and incident counts by about half.

<p class="qb-label">Common mistakes and red flags</p>

- Can't name more than one anti-pattern.
- Defends hard-coded table names.
- No strategy for enforcing standards beyond hope.

<p class="qb-label">Follow-up questions</p>

- How do you find unused models? — Warehouse access history + exposures + BI usage metadata.
- What's model fanout? — One model feeding very many direct children, often a sign intermediate models are missing.

</details>

## Advanced dbt interview questions (experienced)

### 38. Incremental strategies by adapter (append, merge, delete+insert, insert_overwrite, microbatch)

<p class="qb-meta">Advanced · Very Common · Incremental Models</p>

<p class="qb-label">Short answer</p>

The incremental strategy decides how new rows land. `append` just inserts. `merge` upserts on a `unique_key`. `delete+insert` deletes target rows matching the new batch's keys, then inserts. `insert_overwrite` replaces whole partitions. `microbatch` processes time-sliced batches by `event_time`. Supported strategies and defaults vary by adapter, so pick based on keys, partitioning, volume and the cost profile of the warehouse.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

| Strategy | How | Needs | Good for | Watch out |
|---|---|---|---|---|
| `append` | INSERT new rows | — | Immutable events, no reprocessing | Duplicates if lookback/retries |
| `merge` | MERGE on `unique_key` | `unique_key` | Upserts, late updates | Full target scans if unpruned; cost on huge tables |
| `delete+insert` | DELETE matching keys, INSERT batch | `unique_key` | Large batch replacements; adapters where MERGE is slow | Two statements; key must cover all affected rows |
| `insert_overwrite` | Replace partitions present in batch | Partitioned target (BigQuery, Spark/Databricks) | Date-partitioned facts with full-day recompute | Partitions missing from batch are untouched |
| `replace_where` (dbt-databricks) | Delta `REPLACE WHERE` predicate | `incremental_predicates` | Deterministic ranges | Predicate must match data |
| `microbatch` (dbt 1.9+) | Independent batches by `event_time` and `batch_size`, using the adapter's replace mechanism per batch | `event_time`, `begin`, `batch_size` | Large time-series, backfills, retries per batch (see [The microbatch incremental strategy](#39-the-microbatch-incremental-strategy)) | Requires clean event_time column |

**Adapter notes** (defaults can change, so check your adapter's docs)
- **Snowflake:** `merge` is the default. `append`, `delete+insert`, `insert_overwrite` (full-table overwrite on Snowflake) and `microbatch` are supported.
- **Databricks:** `merge` is the default for Delta. `append`, `insert_overwrite`, `replace_where` and `microbatch` are supported.
- **BigQuery:** `merge` is the default. `insert_overwrite` with partitions (static or dynamic) is often the cheapest.
- **Postgres and Redshift:** `append`, `delete+insert` and `merge` (newer versions).

**Performance lever: `incremental_predicates`.** These add conditions to the MERGE or DELETE so the warehouse prunes the target, for example `DBT_INTERNAL_DEST.order_date >= dateadd(day, -7, current_date)`. Without them, MERGE may scan the whole target to find matches.

<p class="qb-label">Example</p>

```sql
-- Dialect: Snowflake SQL via dbt
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'event_id',
    incremental_predicates = [
      "DBT_INTERNAL_DEST.event_date >= dateadd(day, -3, current_date)"
    ],
    cluster_by = ['event_date']
) }}

select event_id, user_id, event_type, event_ts, event_ts::date as event_date
from {{ ref('stg_app__events') }}
{% if is_incremental() %}
where event_ts >= (select dateadd(day, -2, max(event_ts)) from {{ this }})
{% endif %}
```
The predicate window (3 days) must be at least as wide as the source lookback (2 days). Otherwise rows reprocessed from the lookback can't find their existing matches, and they get inserted again as duplicates.

<p class="qb-label">Real-world use case</p>

A merge into a 20-billion-row events table took 40 minutes because each run scanned the whole target. Adding `incremental_predicates` limited to the last 3 days (matched to the lookback) cut it to 3 minutes on clustered data.

<p class="qb-label">Common mistakes and red flags</p>

- `append` with lookbacks.
- `insert_overwrite` assuming partitions not in the batch get deleted.
- Predicates narrower than the lookback.

<p class="qb-label">Follow-up questions</p>

- What's `DBT_INTERNAL_DEST`? — The alias dbt uses for the target table in generated MERGE/DELETE statements.
- Can unique_key be composite? — Yes, a list of columns.

</details>

### 39. The microbatch incremental strategy

<p class="qb-meta">Advanced · Common · Incremental Models</p>

<p class="qb-label">Short answer</p>

`incremental_strategy='microbatch'` (dbt 1.9+) splits processing into independent time batches (hour, day, month or year) based on an `event_time` column. dbt filters the model's inputs to each batch automatically, so you don't write `is_incremental()` logic. It can rerun, retry or backfill specific batches with `--event-time-start` / `--event-time-end`, and use `lookback` for late data.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Key configs**
| Config | Meaning |
|---|---|
| `event_time` | Column on this model (and upstream models/sources) used to slice data |
| `batch_size` | `hour`, `day`, `month`, `year` |
| `begin` | Earliest timestamp for initial/full builds |
| `lookback` | Number of prior batches to reprocess each run (default 1) |
| `concurrent_batches` | Run batches in parallel when the adapter/model allows |
| `unique_key` (some adapters) | Used where the adapter implements batch replace via merge |

**How filtering works**
- Upstream models and sources that also declare `event_time` are **auto-filtered** to the batch window when `ref`'d or `source`'d in a microbatch model.
- Upstreams without `event_time` are read in full (for example dimensions). Use `ref('dim').render()`-style options or set `event_time` on upstreams deliberately.
- Each batch is written by the adapter's mechanism for replacing that time range: delete+insert or insert_overwrite by time range, depending on the adapter. Reruns are therefore idempotent per batch.

**Operations**
- Normal run: processes the current batch plus `lookback` previous batches.
- **Backfill:** `dbt run --select fct_events --event-time-start "2026-01-01" --event-time-end "2026-02-01"`.
- **Retry:** `dbt retry` reruns only failed batches.
- Logs show per-batch success or failure.

**When to prefer it over classic incremental**
- Large time-series facts, where predictable time-sliced work and easy backfills matter.
- Teams tired of hand-written `is_incremental()` filters and lookback math.
- It is less suitable when the natural key isn't time-based or updates arrive without reliable event time (CDC upserts on keys are better served by `merge`).

<p class="qb-label">Example</p>

```sql
-- models/marts/fct_sessions.sql  (Dialect: Snowflake SQL via dbt 1.9+)
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'microbatch',
    event_time = 'session_start',
    batch_size = 'day',
    begin = '2025-01-01',
    lookback = 2
) }}

select session_id, user_id, session_start, session_end, page_count
from {{ ref('int_web__sessions') }}      -- auto-filtered per day if it declares event_time
```

```yaml
# models/intermediate/_int__models.yml
models:
  - name: int_web__sessions
    config:
      event_time: session_start
```

<p class="qb-label">Real-world use case</p>

A team spent hours backfilling a hand-written incremental model after a logic fix. With microbatch they ran a one-month backfill using `--event-time-start/--event-time-end`. Two failed days were retried with `dbt retry`, without touching the other 28.

<p class="qb-label">Common mistakes and red flags</p>

- Missing `event_time` on upstreams, which makes dbt read everything.
- Wrong `batch_size` for the data volume.
- Using microbatch for key-based CDC upserts.

<p class="qb-label">Follow-up questions</p>

- Do you need `is_incremental()` with microbatch? — No; filtering is automatic.
- How is late data handled? — Via `lookback` batches; very late data needs targeted backfills.

</details>

### 40. YAML-defined snapshots and hard-delete handling

<p class="qb-meta">Advanced · Common · Snapshots</p>

<p class="qb-label">Short answer</p>

Since dbt 1.9, snapshots can be defined entirely in YAML (`relation:` plus `config:`), instead of the older Jinja `{% snapshot %}` blocks. Hard deletes are handled with `hard_deletes`. `ignore` (default) does nothing. `invalidate` closes the current row by setting `dbt_valid_to`. `new_record` inserts a new row flagged `dbt_is_deleted = true`, so deletions appear explicitly in history. The older `invalidate_hard_deletes: true` corresponds to `invalidate`.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Why YAML snapshots**
- Less boilerplate: no SQL file is needed when snapshotting a source or model directly.
- Configs live alongside other properties, such as descriptions and tests.
- If you need light transformations, point `relation:` to an ephemeral or staging model, or keep the legacy SQL block style (still supported).

**Hard-delete modes**
| `hard_deletes` | What happens when a key disappears from source | History shows |
|---|---|---|
| `ignore` | Nothing; last version stays "current" forever | Deleted rows look active (misleading) |
| `invalidate` | `dbt_valid_to` set to snapshot time | Row ends; no explicit delete marker |
| `new_record` | Close current row and insert a new row with `dbt_is_deleted = true` | Explicit deletion event; reinsertion later creates another version |

**Considerations**
- Detecting deletes requires comparing the **full** source key set to current snapshot rows, which is more expensive on big tables. The source must be complete each run, not an incremental extract. Otherwise missing rows look like deletes.
- If the source is an **incremental extract** (only changed rows), hard-delete detection gives false deletes. Use CDC delete events instead.
- Downstream "current" logic: filter `dbt_valid_to` is NULL (or the far-future date) **and** `coalesce(dbt_is_deleted, false) = false` when using `new_record`.
- Other 1.9+ options: `snapshot_meta_column_names` to rename metadata columns, and `dbt_valid_to_current` to use a far-future date instead of NULL, which simplifies BETWEEN joins.

<p class="qb-label">Example</p>

```yaml
# snapshots/snap_billing__subscriptions.yml
snapshots:
  - name: snap_billing__subscriptions
    relation: source('billing', 'subscriptions')
    description: "SCD2 history of subscriptions; deletions recorded explicitly."
    config:
      schema: snapshots
      unique_key: subscription_id
      strategy: check
      check_cols: [plan_code, status, seats]
      hard_deletes: new_record
      dbt_valid_to_current: "to_timestamp('9999-12-31')"
```

```sql
-- models/marts/dim_subscription_current.sql
select *
from {{ ref('snap_billing__subscriptions') }}
where dbt_valid_to = to_timestamp('9999-12-31')
  and coalesce(dbt_is_deleted, false) = false
```

<p class="qb-label">Real-world use case</p>

Cancelled subscriptions were hard-deleted in the billing database, and a snapshot with default settings kept them "active" forever, inflating MRR. Switching to `hard_deletes: new_record` made cancellations explicit, and the MRR model excluded rows marked deleted.

<p class="qb-label">Common mistakes and red flags</p>

- Hard-delete detection on incremental extracts.
- Forgetting the deleted flag in "current" filters.
- Assuming YAML snapshots require dbt 1.8.

<p class="qb-label">Follow-up questions</p>

- What if a deleted key comes back? — With `new_record`, a new active version is inserted.
- Can you convert legacy snapshot blocks to YAML? — Yes; move configs to YAML and point `relation` to the source/model.

</details>

### 41. Model contracts

<p class="qb-meta">Advanced · Common · Governance</p>

<p class="qb-label">Short answer</p>

A model contract (`contract: {enforced: true}`) declares the exact columns and data types a model must produce. dbt checks the compiled output against them before building and fails if they don't match. On supported platforms it can also apply constraints in the DDL (`not_null`, `primary_key`, `check`, `foreign_key`), with enforcement depending on the platform. Use contracts on public, consumer-facing models to stop accidental breaking changes.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**What enforcement does**
- Before building, dbt compares the model's output columns (names, order-independent, and types) with the YAML definition. A mismatch is an error.
- It builds the table with **explicit DDL** (column types) instead of `CREATE TABLE AS SELECT` type inference, on supported materialisations (table, incremental). Views get the type check only.
- **Constraints:**
  - `not_null` is enforced on most warehouses.
  - `primary_key`, `unique` and `foreign_key` are often **informational** (Snowflake, BigQuery, Databricks), so use data tests for real enforcement.
  - `check` is enforced on Postgres and Databricks (Delta CHECK constraints; see [Identity columns and constraints in Delta tables](/databricks/databricks-interview-questions/#37-identity-columns-and-constraints-in-delta-tables)), and not on Snowflake.

**Contracts in a governance setup**
- Combine them with **access levels** (public, protected, private), **groups** and **model versions** (see [Model versions, groups and access levels](#42-model-versions-groups-and-access-levels)). Breaking changes to a public contracted model require a new version, and dbt detects breaking contract changes against the previous state in CI (with `state:modified`).
- Contracts are the producer side of a **data contract** (see [What are data contracts, and how do you enforce them?](/career/data-engineering-interview-questions/#83-what-are-data-contracts-and-how-do-you-enforce-them)). Consumers rely on stable columns and types.

**Trade-offs**
- More YAML to maintain, because every column needs a type. Use codegen to generate it.
- Type names are adapter-specific (`number(38,2)` vs `decimal(38,2)`). Use `data_type` values valid for your warehouse.
- Use contracts for **stable, shared** models, not every staging model.

<p class="qb-label">Example</p>

```yaml
# models/marts/finance/_finance__models.yml
models:
  - name: fct_orders
    access: public
    config:
      contract:
        enforced: true
      materialized: table
    columns:
      - name: order_id
        data_type: number(38,0)
        constraints:
          - type: not_null
          - type: primary_key
      - name: customer_id
        data_type: number(38,0)
        constraints:
          - type: not_null
      - name: ordered_at
        data_type: timestamp_ntz
      - name: order_total
        data_type: number(18,2)
    data_tests:
      - dbt_utils.unique_combination_of_columns:
          arguments:
            combination_of_columns: [order_id]
```
If someone changes `order_total` to return a float, or drops `customer_id`, `dbt build` fails with a contract error before the table is replaced.

<p class="qb-label">Real-world use case</p>

A finance mart's `amount` column changed from `number(18,2)` to float during a refactor, causing rounding issues in a downstream reconciliation system. After contracts were enforced on all public finance models, a similar change was caught in CI two months later.

<p class="qb-label">Common mistakes and red flags</p>

- Assuming primary key constraints are enforced on Snowflake.
- Contracts on every model (maintenance burden).
- Not pairing contracts with versioning for breaking changes.

<p class="qb-label">Follow-up questions</p>

- What happens to views with contracts? — Column/type check only; constraints not applied.
- How do you handle an intentional breaking change? — Create a new model version and deprecate the old one (see [Model versions, groups and access levels](#42-model-versions-groups-and-access-levels)).

</details>

### 42. Model versions, groups and access levels

<p class="qb-meta">Advanced · Occasional · Governance</p>

<p class="qb-label">Short answer</p>

Groups assign models to an owning team. Access levels control who can `ref` a model: `private` (same group only), `protected` (same project, the default), `public` (other projects through dbt Mesh). Model versions (`versions:` in YAML) let you publish a breaking change as v2 while v1 keeps working for consumers until its `deprecation_date`. Consumers use `ref('model', v=1)`, or the latest version by default.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Groups**
- `groups:` in YAML, each with a name and owner (name or email). Models set `group: finance`.
- They enable **private** access: only models in the same group can ref the model.

**Access levels**
| Level | Who can `ref` |
|---|---|
| `private` | Models in the same group |
| `protected` (default) | Any model in the same project (and packages installed in it) |
| `public` | Any project, including downstream projects via dbt Mesh cross-project refs (see [dbt Mesh: cross-project refs](#48-dbt-mesh-cross-project-refs)) |

Violations fail at parse time, so the rules are enforced, not just advised.

**Versions**
- Define `versions:` with `v:` numbers. Each version can include or exclude columns relative to the base definition, or point to a different SQL file (`defined_in`).
- `latest_version:` sets what an unpinned `ref('fct_orders')` resolves to.
- Relations are named `fct_orders_v1` and `fct_orders_v2` by default. You can create a view or alias for the latest version.
- `deprecation_date:` makes dbt warn consumers who still ref old versions.
- Combine with **contracts**: when a public contracted model changes in a breaking way, dbt in CI (state comparison) flags it unless you bump the version.

**Lifecycle of a breaking change**
1. Add v2 with the new schema. Keep v1, and set `latest_version: 1` initially if you want consumers to opt in.
2. Communicate, and let consumers migrate to `ref('fct_orders', v=2)`.
3. Switch `latest_version` to 2.
4. After the deprecation date, remove v1.

<p class="qb-label">Example</p>

```yaml
groups:
  - name: finance
    owner:
      name: Finance Data Team
      email: finance-data@acme.example

models:
  - name: fct_orders
    group: finance
    access: public
    latest_version: 2
    config:
      contract: {enforced: true}
    columns:
      - name: order_id
        data_type: number(38,0)
      - name: order_total
        data_type: number(18,2)
      - name: currency
        data_type: varchar
    versions:
      - v: 2
      - v: 1
        deprecation_date: 2026-06-30
        columns:
          - include: all
            exclude: [currency]
```

```sql
-- Downstream model in a consuming project still on v1
select order_id, order_total from {{ ref('finance_project', 'fct_orders', v=1) }}
```

<p class="qb-label">Real-world use case</p>

The finance team needed to add a mandatory currency column and change amount semantics in `fct_orders`. They published v2, kept v1 for 90 days with a deprecation date, and three consuming teams migrated on their own schedules without broken dashboards.

<p class="qb-label">Common mistakes and red flags</p>

- Making everything public.
- Versioning without contracts.
- Never removing deprecated versions.

<p class="qb-label">Follow-up questions</p>

- Does private access affect the warehouse grants? — No; it's a dbt ref-time rule. Warehouse access uses grants.
- How is `latest_version` resolved? — An unpinned `ref` resolves to it.

</details>

### 43. dbt Semantic Layer and MetricFlow (See also Q99)

<p class="qb-meta">Advanced · Common · Semantic Layer</p>

<p class="qb-label">Short answer</p>

The dbt Semantic Layer defines metrics once in YAML: semantic models with entities, dimensions and measures, plus metrics built on them. MetricFlow generates the correct SQL (joins, grains, filters) for any requested combination of metrics and dimensions. BI tools, notebooks and AI agents query metrics through the dbt platform's Semantic Layer APIs, so "revenue" means the same thing everywhere.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Building blocks**
| Concept | Meaning |
|---|---|
| Semantic model | Maps a dbt model to entities (keys), dimensions (categorical, time), measures (aggregations) |
| Entity | Join key (`primary`, `foreign`, `unique`, `natural`) — MetricFlow uses entities to build joins |
| Dimension | Attributes to group/filter by; time dimensions with granularity |
| Measure | Aggregation of a column (`sum`, `count`, `count_distinct`, `average`, `percentile`…) |
| Metric | `simple`, `ratio`, `derived`, `cumulative`, `conversion` metrics built on measures |
| Saved query / export | Predefined metric queries, optionally materialised as tables |

**How queries work**
- A consumer requests `metrics=[revenue, orders], group_by=[metric_time__month, customer__segment]`.
- MetricFlow finds the semantic models, joins through entities (avoiding fan-out errors), applies filters, and generates SQL for the warehouse.
- Interfaces include the dbt platform Semantic Layer APIs (JDBC/ADBC, GraphQL), integrations (Tableau, Power BI, Google Sheets, Hex and others), and the MetricFlow CLI in development (`mf query`, `dbt sl query`).

**Open-source and ecosystem notes**
- MetricFlow is open source (Apache 2.0, relicensed in 2025). The hosted query APIs are part of the dbt platform.
- dbt Labs is part of the **Open Semantic Interchange (OSI)** initiative, which aims at portable semantic definitions across tools. Warehouse-native semantic layers also exist, such as Snowflake semantic views and Databricks metric views (see [What is a semantic/metrics layer?](/career/data-engineering-interview-questions/#75-what-is-a-semanticmetrics-layer)).
- **YAML spec changes:** newer dbt versions have been simplifying the semantic layer YAML (for example defining metrics closer to models). Check your version's spec.

**Design tips**
- Build semantic models on **clean marts** at clear grains.
- Define measures with **explicit aggregations** and descriptions. Use `agg_time_dimension`.
- Prefer **ratio and derived metrics** over pre-computed ratios, which avoids averaging averages.
- Test metric outputs against known reports during migration.

<p class="qb-label">Example</p>

```yaml
# models/marts/finance/_finance__semantic.yml
semantic_models:
  - name: orders
    model: ref('fct_orders')
    defaults:
      agg_time_dimension: ordered_at
    entities:
      - name: order
        type: primary
        expr: order_id
      - name: customer
        type: foreign
        expr: customer_id
    dimensions:
      - name: ordered_at
        type: time
        type_params:
          time_granularity: day
      - name: order_status
        type: categorical
    measures:
      - name: order_total
        agg: sum
      - name: order_count
        expr: order_id
        agg: count_distinct

metrics:
  - name: revenue
    label: Revenue
    type: simple
    type_params:
      measure: order_total
  - name: average_order_value
    label: Average Order Value
    type: ratio
    type_params:
      numerator: revenue
      denominator: order_count
```

```bash
dbt sl query --metrics revenue,average_order_value --group-by metric_time__month,order__order_status
```

<p class="qb-label">Real-world use case</p>

Three BI tools computed "average order value" differently: a mean of daily averages in one, excluding cancellations in another. Defining it once as a ratio metric in the Semantic Layer and connecting the tools to it removed a long-running dispute between finance and marketing.

<p class="qb-label">Common mistakes and red flags</p>

- Defining metrics on messy staging models.
- Pre-aggregated averages.
- Treating the semantic layer as only a BI feature, without governance.

<p class="qb-label">Follow-up questions</p>

- How does MetricFlow avoid fan-out? — It uses entity relationships and aggregates measures at their semantic model grain before joining.
- Semantic layer vs a wide mart table? — Wide tables fix one grain/filter set; the semantic layer answers flexible combinations consistently.

</details>

### 44. Slim CI with state:modified and `--defer`

<p class="qb-meta">Advanced · Very Common · CI/CD</p>

<p class="qb-label">Short answer</p>

Slim CI builds and tests only what a pull request changed, plus its descendants, using `--select state:modified+` with `--state` pointing to the production `manifest.json`. `--defer` makes refs to unchanged, unbuilt parents resolve to production relations instead of the CI schema. CI then validates changes in minutes without rebuilding the whole project.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**How it works**
1. Production runs save artifacts (`manifest.json`). CI downloads the latest production manifest. In dbt platform, deferral to a production environment is built in.
2. dbt compares the PR's manifest with production's. `state:modified` finds nodes whose SQL, configs, macros they depend on, or YAML properties changed. `state:new` finds added nodes.
3. `dbt build --select state:modified+ --defer --state prod_artifacts/ --target ci` then:
   - builds the modified models and their descendants into a **CI schema** (often PR-specific, such as `dbt_ci_pr_123`);
   - lets unchanged parents that aren't in the selection **defer** to production relations, so they don't need building;
   - runs tests on what was built.
4. Clean up CI schemas after merge or on a schedule.

**Refinements**
- **Sub-selectors:** `state:modified.body`, `.configs`, `.relation`, `.persisted_descriptions`, `.macros`, `.contract`, for finer control.
- **`--favor-state`:** prefer production relations even if a CI relation exists.
- **`dbt clone`:** zero-copy clone production relations of modified incremental models into CI first, so incremental logic runs against realistic existing data (see [dbt clone and zero-copy CI environments](#50-dbt-clone-and-zero-copy-ci-environments)).
- **Empty builds:** `--empty` (dbt 1.8+) builds with zero rows (`limit 0`) to validate SQL and schema cheaply.
- **Contract checks:** breaking changes to contracted public models are flagged when compared with state (see [Model contracts](#41-model-contracts)).
- **Unit tests:** run them for modified models in CI (`test_type:unit`).

**Why it matters**
- Full CI builds of large projects take hours and cost a lot. Slim CI often takes minutes.
- Developers get fast feedback, and reviewers see test results on the PR.

<p class="qb-label">Example</p>

```yaml
# .github/workflows/dbt_ci.yml (excerpt)
jobs:
  dbt-ci:
    runs-on: ubuntu-latest
    env:
      DBT_SF_ACCOUNT: ${{ secrets.DBT_SF_ACCOUNT }}
      DBT_SF_USER: ${{ secrets.DBT_SF_CI_USER }}
      DBT_SF_KEY_PATH: /tmp/ci_key.p8
      DBT_CI_SCHEMA: dbt_ci_pr_${{ github.event.number }}
    steps:
      - uses: actions/checkout@v4
      - run: pip install dbt-snowflake
      - run: echo "${{ secrets.DBT_SF_CI_PRIVATE_KEY }}" > /tmp/ci_key.p8
      - run: ./scripts/download_prod_manifest.sh prod-artifacts/   # from object storage
      - run: dbt deps
      - run: dbt build --select state:modified+ --defer --state prod-artifacts --target ci
```
The CI profile's `schema` reads `env_var('DBT_CI_SCHEMA')`, so each PR builds into its own schema. The private key is written to a temporary file on the ephemeral runner and is never logged.

<p class="qb-label">Real-world use case</p>

A 1,200-model project's CI rebuilt everything in 2.5 hours. Slim CI with deferral cut typical PR checks to 6–10 minutes, and the warehouse cost of CI dropped by over 90%.

<p class="qb-label">Common mistakes and red flags</p>

- A stale production manifest, which gives wrong modified detection.
- No `--defer`, so CI tries to build every parent.
- Never cleaning CI schemas.

<p class="qb-label">Follow-up questions</p>

- Where does the prod manifest come from? — Saved after each prod run (object storage, artifact store) or provided by dbt platform.
- What if a macro changes? — `state:modified` includes nodes that depend on changed macros.

</details>

### 45. Speeding up slow dbt runs

<p class="qb-meta">Advanced · Common · Performance</p>

<p class="qb-label">Short answer</p>

First find the bottleneck. Compare model timings from `run_results.json` and the critical path in the DAG. Then convert slow full-table rebuilds to incremental (with pruning predicates), tune expensive SQL and data layout (clustering or partitioning), raise threads to use DAG parallelism, right-size or split warehouses, reduce unnecessary models and tests, and parse faster with partial parsing or Fusion.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Diagnose**
- `run_results.json` gives execution time per node. Sort to find the top 20. Track trends over runs (see [dbt artifacts: manifest.json and run_results.json](#34-dbt-artifacts-manifestjson-and-run_resultsjson)).
- Warehouse query history and query profiles show spill, scans, joins and queueing (see [MERGE in Snowflake and the nondeterministic-merge error](/snowflake/snowflake-interview-questions/#35-merge-in-snowflake-and-the-nondeterministic-merge-error), [Troubleshooting a slow Databricks job end to end](/databricks/databricks-interview-questions/#50-troubleshooting-a-slow-databricks-job-end-to-end)).
- **Critical path:** total runtime is bounded by the longest dependency chain, not total model time. Optimise models on that chain first.
- Parse and compile time: is dbt itself slow before queries start?

**Levers**
| Area | Actions |
|---|---|
| Materializations | Incremental for large facts; `incremental_predicates`; microbatch for time-series (see [Incremental models and is_incremental()](#15-incremental-models-and-is_incremental)–[The microbatch incremental strategy](#39-the-microbatch-incremental-strategy)) |
| SQL | Filter early, avoid `select *` on wide tables, remove repeated heavy CTEs, avoid exploding joins, use QUALIFY instead of self-joins |
| Data layout | Clustering keys (Snowflake), partition/cluster (BigQuery), liquid clustering (Databricks) on filter/join columns |
| Parallelism | `threads` (8–32 typical) — DAG width limits benefit; warehouse concurrency must match |
| Compute | Bigger warehouse for heavy models via `snowflake_warehouse` config; separate warehouses per workload; Gen2/adaptive options |
| DAG design | Break long chains; avoid huge views queried repeatedly downstream (materialize as table); remove dead models |
| Tests | `where` filters on big tables, sample-based tests, run expensive tests less frequently |
| Selection | Split nightly runs by domain/priority; `source_status:fresher+` to skip unchanged |
| Parsing | Partial parsing (default), avoid `run_query` at parse, Fusion engine |

**Views as a hidden cost:** a view referenced by 10 downstream models recomputes 10 times. If it is heavy, materialise it as a table.

<p class="qb-label">Example</p>

```sql
-- Heavy model routed to a larger warehouse only for itself (dbt-snowflake)
{{ config(
    materialized = 'incremental',
    unique_key = 'event_id',
    snowflake_warehouse = 'TRANSFORM_L',
    cluster_by = ['event_date'],
    incremental_predicates = ["DBT_INTERNAL_DEST.event_date >= dateadd(day, -3, current_date)"]
) }}
select * from {{ ref('int_events_enriched') }}
{% if is_incremental() %}
where event_date >= dateadd(day, -2, current_date)
{% endif %}
```

```python
# Runtime: Python 3.12 — top 10 slowest nodes from run_results.json
import json
from pathlib import Path

results = json.loads(Path("target/run_results.json").read_text())["results"]
slowest = sorted(results, key=lambda r: r.get("execution_time", 0), reverse=True)[:10]
for r in slowest:
    print(f'{r["execution_time"]:8.1f}s  {r["status"]:8}  {r["unique_id"]}')
```

<p class="qb-label">Real-world use case</p>

A 4-hour nightly run had a critical path through a heavy view referenced by 12 models. Materialising that view as an incremental table, raising threads from 4 to 16, and giving three heavy models a larger warehouse cut the run to 70 minutes.

<p class="qb-label">Common mistakes and red flags</p>

- Upsizing the warehouse for everything.
- Raising threads beyond warehouse capacity, which causes queueing.
- Ignoring the critical path.

<p class="qb-label">Follow-up questions</p>

- How many threads? — Enough to use DAG width without queueing; tune with warehouse size/multi-cluster.
- What is partial parsing? — dbt re-parses only changed files using `partial_parse.msgpack`.

</details>

### 46. Python models

<p class="qb-meta">Advanced · Occasional · Python Models</p>

<p class="qb-label">Short answer</p>

A dbt Python model is a `.py` file defining `def model(dbt, session)` that returns a DataFrame. It runs on the warehouse's Python runtime: Snowpark on Snowflake, PySpark on Databricks, BigQuery DataFrames or Dataproc on BigQuery. It participates in the DAG with `dbt.ref()` and `dbt.source()`. Use it for logic SQL handles poorly, such as ML scoring, complex parsing or statistical functions. Keep most transformations in SQL.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Structure**
- `dbt.config(materialized="table", packages=[...])` sets configs (packages are platform-specific).
- `dbt.ref("model")` and `dbt.source("src", "table")` return platform DataFrames.
- Return a DataFrame. dbt writes it with the chosen materialisation (table or incremental, with `dbt.is_incremental` and `dbt.this`).
- Python models can't use Jinja, and macros don't apply inside them.

**Platform execution**
| Platform | Runtime | Notes |
|---|---|---|
| Snowflake | Snowpark stored procedure | Packages from Snowflake Anaconda channel/artifact repository; warehouse compute (Snowpark-optimized for memory-heavy) |
| Databricks | PySpark on cluster/serverless | `submission_method` (all-purpose, job cluster, serverless); Python dependencies configured per platform |
| BigQuery | BigQuery DataFrames or Dataproc Serverless | Configuration via adapter docs |

**Good uses**
- Applying a pre-trained ML model to score rows (with UDFs or vectorised functions).
- Complex text, parsing or NLP with Python libraries.
- Statistical routines unavailable in SQL.
- Prototyping logic with pandas-like APIs (Snowpark pandas, PySpark pandas API).

**Cautions**
- **Cost and speed:** Python execution can be slower and more expensive than SQL. Pulling data to pandas (`to_pandas()`) on large tables risks memory problems.
- **Testing:** unit tests for Python models are limited compared with SQL. Rely on data tests and modular Python functions tested separately.
- **Debugging and observability** are harder: logs live in the platform's procedure or job logs.
- Team skills and code review standards must cover both languages.

<p class="qb-label">Example</p>

```python
# models/marts/ml/fct_churn_scores.py  (dbt-snowflake Python model, Snowpark)
import snowflake.snowpark.functions as F

def model(dbt, session):
    dbt.config(materialized="table", packages=["scikit-learn==1.5.2"])

    features = dbt.ref("int_customer_churn_features")
    # Simple rules-based score as a stand-in for a registered model call
    scored = features.with_column(
        "churn_score",
        F.least(F.lit(1.0),
                F.col("days_since_last_order") / F.lit(180.0) * F.lit(0.6)
                + F.col("support_tickets_90d") / F.lit(10.0) * F.lit(0.4)),
    ).select("customer_id", "churn_score", F.current_timestamp().alias("scored_at"))
    return scored
```
In practice you would call a model registered in the platform's model registry, for example as a UDF or a registry inference method. The score formula here is illustrative. Check the package pin against the versions available in your Snowflake channel.

<p class="qb-label">Real-world use case</p>

A team scored 20 million customers nightly with a registered churn model. A dbt Python model calls the model's inference function on Snowpark and writes `fct_churn_scores`. Tests check score ranges, and an exposure links the CRM sync.

<p class="qb-label">Common mistakes and red flags</p>

- Using Python models for simple joins.
- `to_pandas()` on large tables.
- Expecting Jinja inside Python models.

<p class="qb-label">Follow-up questions</p>

- Can Python models be incremental? — Yes on supported adapters, using `dbt.is_incremental` and `dbt.this`.
- Can SQL models ref Python models? — Yes; they're normal DAG nodes.

</details>

### 47. Late-arriving data in incremental models (lookback windows)

<p class="qb-meta">Advanced · Common · Incremental Models</p>

<p class="qb-label">Short answer</p>

Rows can arrive after the model has already processed their time window: delayed loaders, offline devices, back-dated corrections. Use a lookback window in the incremental filter that reprocesses recent days, combined with merge on `unique_key` (or partition replacement), so reprocessed rows replace rather than duplicate. Filter on load time instead of event time where possible, and run periodic wider backfills for very late data.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Filter column choice**
| Filter on | Pros | Cons |
|---|---|---|
| Ingestion/load timestamp (`_loaded_at`) | Catches all newly arrived rows regardless of event time | Needs a reliable loader timestamp; target partitions by event date get touched across many days |
| Event timestamp (`event_ts`) | Aligns with partitions/clustering | Late rows with old event_ts are missed without lookback |
| Both: load-time filter + event-date predicates for pruning | Accurate and efficient | Slightly more complex |

**Lookback design**
- Pick a window covering the observed lateness, for example the p99.9 of arrival delay (see [How do you build data quality checks into a pipeline?](/career/data-engineering-interview-questions/#44-how-do-you-build-data-quality-checks-into-a-pipeline)).
- `where event_ts >= (select max(event_ts) from {{ this }}) - interval '3 days'`.
- Pair it with **merge on `unique_key`** (see [Incremental strategies by adapter (append, merge, delete+insert, insert_overwrite, microbatch)](#38-incremental-strategies-by-adapter-append-merge-deleteinsert-insert_overwrite-microbatch)). With **append**, a lookback creates duplicates.
- Make `incremental_predicates` at least as wide as the lookback, to keep MERGE pruning correct.
- **Microbatch:** `lookback: N` batches (see [The microbatch incremental strategy](#39-the-microbatch-incremental-strategy)).

**Aggregated models**
- For daily aggregates, recompute whole affected days. Use `insert_overwrite` or `delete+insert` by date, or a merge on `(date, dimension keys)`.
- Find the affected days from the newly arrived rows (`select distinct event_date from new_rows`), and recompute only those days.

**Very late data**
- A scheduled wider backfill (weekly, last 30 days) with a var-driven lookback.
- Monitoring that counts rows arriving beyond the lookback, by comparing load time with event time. Alert when it's non-zero.
- Document the guarantee ("numbers final after 3 days").

<p class="qb-label">Example</p>

```sql
-- models/marts/agg_daily_device_metrics.sql  (Dialect: Snowflake SQL via dbt)
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['metric_date', 'device_id']
) }}

{% set lookback_days = var('device_lookback_days', 3) | int %}

with new_rows as (
    select * from {{ ref('stg_iot__readings') }}
    {% if is_incremental() %}
    where _loaded_at >= (select dateadd('day', -{{ lookback_days }}, max(max_loaded_at)) from {{ this }})
    {% endif %}
),
affected_days as (
    select distinct reading_ts::date as metric_date from new_rows
)
select
    r.reading_ts::date      as metric_date,
    r.device_id,
    avg(r.temperature_c)    as avg_temperature_c,
    count(*)                as readings,
    max(r._loaded_at)       as max_loaded_at
from {{ ref('stg_iot__readings') }} r
join affected_days d on r.reading_ts::date = d.metric_date
group by 1, 2
```
Rows recently loaded define which days are affected. Each of those days is recomputed from all its readings, so late readings for 5 days ago update that day's averages correctly.

<p class="qb-label">Real-world use case</p>

Field devices uploaded readings up to 2 days late when offline. Daily averages computed from on-time data were wrong for about 8% of device-days. Switching to load-time filtering, plus full recomputation of affected days, made the daily metrics match the full rebuild.

<p class="qb-label">Common mistakes and red flags</p>

- Append plus lookback, creating duplicates.
- Event-time filter only.
- Merging partial aggregates instead of recomputing affected days.

<p class="qb-label">Follow-up questions</p>

- How do you choose the lookback? — Measure arrival delay distribution; balance with cost.
- What if lateness exceeds the window? — Periodic backfills and monitoring of beyond-window arrivals.

**In depth:** [How do you handle late-arriving data?](/interview/data-engineering/late-arriving-data/)

</details>

### 48. dbt Mesh: cross-project refs

<p class="qb-meta">Advanced · Occasional · Multi-Project Architecture</p>

<p class="qb-label">Short answer</p>

dbt Mesh splits a monolithic dbt project into several domain-owned projects. Downstream projects reference upstream public models with `{{ ref('finance', 'fct_orders') }}`, declared in `dependencies.yml`. That relies on public access, contracts and versions (see [Model contracts](#41-model-contracts), [Model versions, groups and access levels](#42-model-versions-groups-and-access-levels)), and in dbt platform on cross-project metadata (Catalog). Teams then own, deploy and govern their domains independently while sharing stable interfaces.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Why split**
- Monoliths with thousands of models suffer slow parsing and CI, unclear ownership, merge conflicts, and every change risking everything.
- Domain teams want autonomy over their code, schedules and reviews, which matches data mesh principles (see [Data engineering for ML and GenAI: feature stores, embeddings, vector stores](/career/data-engineering-interview-questions/#100-data-engineering-for-ml-and-genai-feature-stores-embeddings-vector-stores)).

**Mechanics**
- `dependencies.yml` in the consumer project lists `projects: [{name: finance}]`.
- Consumers `ref('finance', 'fct_orders')` (optionally with `v=2`). Only **public** models are referencable.
- Resolution: dbt platform resolves the upstream project's production relations through metadata, with no need to rebuild them. For dbt Core, cross-project refs need a way to supply upstream metadata. Common Core alternatives are installing the upstream as a **package** (which brings its code), using **sources** pointing at the upstream's published tables, or community plugins. Check current support.
- Lineage spans projects in dbt platform Catalog.

**Design guidance**
- Expose a **small set of public, contracted, versioned** models per domain, as its "data products".
- Keep internals `protected` or `private`.
- Agree on **SLAs and freshness** for public models (exposures or contracts plus scheduling dependencies).
- Avoid **circular dependencies** between projects. The project DAG must be acyclic.
- Orchestrate consumers after producers: job-completion triggers or freshness gating.

**Alternatives**
- One monorepo project with groups and access levels, which gives most governance benefits without splitting.
- Packages for shared macros (not models).

<p class="qb-label">Example</p>

```yaml
# marketing_project/dependencies.yml
projects:
  - name: finance
```

```sql
-- marketing_project/models/marts/fct_campaign_revenue.sql
select c.campaign_id, c.channel, sum(o.order_total) as attributed_revenue
from {{ ref('stg_ads__campaign_touches') }} c
join {{ ref('finance', 'fct_orders', v=2) }} o
  on o.order_id = c.order_id
group by 1, 2
```

<p class="qb-label">Real-world use case</p>

A 2,500-model monolith with 60 contributors was split into five domain projects. Finance publishes eight public contracted models. Marketing and product reference them cross-project, CI per project dropped from 40 minutes to under 10, and ownership became clear.

<p class="qb-label">Common mistakes and red flags</p>

- Splitting projects without contracts, which leads to breaking changes downstream.
- Making most models public.
- Cyclic project dependencies.

<p class="qb-label">Follow-up questions</p>

- How do you prevent breaking downstream projects? — Contracts + versions + deprecation dates on public models.
- When not to use Mesh? — Small teams or projects where a single project with groups is simpler.

</details>

### 49. Cross-database macros and adapter.dispatch

<p class="qb-meta">Advanced · Occasional · Macros</p>

<p class="qb-label">Short answer</p>

`adapter.dispatch('macro_name', 'namespace')` picks the implementation of a macro for the current adapter. It looks for `snowflake__macro_name`, `databricks__macro_name` and so on, and falls back to `default__macro_name`. dbt ships cross-database macros (`dbt.dateadd`, `dbt.datediff`, `dbt.concat`, `dbt.type_string`, `dbt.safe_cast`, `dbt.split_part` and more), so packages and multi-warehouse projects can write portable SQL.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**How dispatch works**
1. A generic macro calls `{{ return(adapter.dispatch('my_macro', 'my_package')(args)) }}`.
2. dbt searches the namespaces, in order of the `dispatch` config in `dbt_project.yml` (so your project can override package implementations), for `<adapter>__my_macro`, then any parent adapter (for example `redshift__` falls back to `postgres__`), then `default__my_macro`.
3. The matching implementation renders SQL for that warehouse.

**Built-in cross-database macros** (in the `dbt` namespace)
- Date and time: `dateadd`, `datediff`, `date_trunc`, `last_day`, `current_timestamp`.
- Strings: `concat`, `split_part`, `length`, `replace`, `position`, `right`, `escape_single_quotes`.
- Types: `type_string`, `type_timestamp`, `type_int`, `type_numeric`, `cast`, `safe_cast`.
- Aggregation and sets: `listagg`, `bool_or`, `any_value`, `array_construct`, `array_append`.
- Others: `hash`, `cast_bool_to_text`, `except`, `intersect`.

**When to use**
- **Packages** meant for many warehouses.
- **Migrations** between warehouses: write once, then run on both during cutover.
- **Multi-cloud setups** (some business units on Snowflake, others on Databricks).
- For a single-warehouse project, native SQL is usually clearer. Don't over-abstract.

**Overriding package behaviour**
```yaml
dispatch:
  - macro_namespace: dbt_utils
    search_order: ['acme_analytics', 'dbt_utils']
```
With that, your project's `snowflake__<macro>` overrides the package's version. That is useful when a package's implementation is slow or wrong for your warehouse, without forking the package.

<p class="qb-label">Example</p>

```sql
-- macros/week_start.sql
{% macro week_start(date_expr) -%}
    {{ return(adapter.dispatch('week_start', 'acme_analytics')(date_expr)) }}
{%- endmacro %}

{% macro default__week_start(date_expr) -%}
    date_trunc('week', {{ date_expr }})
{%- endmacro %}

{% macro snowflake__week_start(date_expr) -%}
    date_trunc('week', {{ date_expr }})::date          -- respects WEEK_START session parameter
{%- endmacro %}

{% macro bigquery__week_start(date_expr) -%}
    date_trunc({{ date_expr }}, week(monday))
{%- endmacro %}
```

```sql
-- models/marts/agg_weekly_orders.sql
select {{ week_start('ordered_at') }} as week_start, count(*) as orders,
       {{ dbt.datediff("min(ordered_at)", "max(ordered_at)", "day") }} as active_span_days
from {{ ref('fct_orders') }}
group by 1
```

<p class="qb-label">Real-world use case</p>

During a Redshift-to-Snowflake migration, a team replaced dialect-specific date functions in 200 models with `dbt.dateadd`, `dbt.datediff` and a few dispatched macros. The same project ran on both warehouses for parallel validation until cutover.

<p class="qb-label">Common mistakes and red flags</p>

- Abstracting every function in a single-warehouse project.
- Forgetting the `default__` implementation.
- Not configuring dispatch search order when overriding package macros.

<p class="qb-label">Follow-up questions</p>

- What's the dispatch fallback chain? — Adapter-specific → parent adapter → default.
- Where are built-in cross-db macros documented? — In dbt's cross-database macros reference (dbt namespace).

</details>

### 50. dbt clone and zero-copy CI environments

<p class="qb-meta">Advanced · Occasional · CI/CD</p>

<p class="qb-label">Short answer</p>

`dbt clone --select ... --state prod_artifacts` creates copies of selected production relations in the current target schema. It uses zero-copy clones where the warehouse supports them (Snowflake, Databricks Delta shallow clones, BigQuery table clones) and views otherwise. CI or dev then gets realistic, prod-like tables instantly, for example to test incremental models against existing data or to give blue-green deployments a starting point.

<details class="qb-more">
<summary>Full answer: explanation, example and follow-ups</summary>

<p class="qb-label">Detailed explanation</p>

**Why clone**
- **Incremental models in CI:** with only `--defer`, a modified incremental model in CI has no existing table, so it runs the full-build branch, not the incremental logic. Cloning the production table first makes `is_incremental()` true and tests the real incremental path.
- **Dev environments:** developers start with production-shaped data without rebuilding upstream models.
- **Blue-green deployments:** clone production into a staging schema, build changes there, validate, then swap (see [Dev/test/prod environments and promoting data code](/career/data-engineering-interview-questions/#68-devtestprod-environments-and-promoting-data-code)).
- **Fast rollback points:** a clone snapshot before risky changes.

**How it works**
- Requires `--state` (a production manifest), so dbt knows where production relations live.
- For each selected node, dbt creates a clone if the adapter supports it, otherwise a view pointing to the production relation (`create view as select * from prod`).
- **Snowflake:** `CREATE TABLE ... CLONE`, zero-copy, near-instant.
- **Databricks:** shallow clone for Delta (see [Shallow clone vs deep clone](/databricks/databricks-interview-questions/#36-shallow-clone-vs-deep-clone)). Watch VACUUM on the source.
- **BigQuery:** table clones.
- Use `--full-refresh` to recreate existing clones.

**Typical CI sequence**
```bash
dbt clone --select state:modified+,config.materialized:incremental --state prod-artifacts --target ci
dbt build --select state:modified+ --defer --state prod-artifacts --target ci
```

**Cautions**
- Clones carry **production data** into CI and dev, so apply the same access controls, masking and PII policies. Consider masked clones or subsets for dev.
- Storage costs grow as clones diverge from the source (copy-on-write). Drop CI schemas after merge.
- Shallow clones on Databricks can break if the source is vacuumed. Keep CI schemas short-lived.

<p class="qb-label">Example</p>

```bash
# 1) Clone prod versions of modified incremental models into the PR schema
dbt clone \
  --select "state:modified+,config.materialized:incremental" \
  --state ./prod-artifacts \
  --target ci

# 2) Build and test modified models; unchanged parents defer to prod
dbt build --select "state:modified+" --defer --state ./prod-artifacts --target ci

# 3) Cleanup (e.g. scheduled job or PR close hook)
dbt run-operation drop_ci_schema --args '{schema_name: dbt_ci_pr_42}' --target ci
```
`drop_ci_schema` is a project macro that should refuse any schema name not matching the CI prefix pattern, so it can never drop production schemas.

<p class="qb-label">Real-world use case</p>

An incremental MERGE bug only appeared when the target already had data. CI never caught it because CI always built from scratch. Adding `dbt clone` for modified incremental models made CI exercise the incremental branch, and the next similar bug failed in the PR.

<p class="qb-label">Common mistakes and red flags</p>

- Assuming `--defer` alone tests incremental logic.
- Cloning PII into loosely controlled dev schemas.
- Never cleaning up clones.

<p class="qb-label">Follow-up questions</p>

- What does dbt clone do on adapters without cloning? — Creates views referencing production relations.
- Does cloning copy grants? — Depends on platform/config; apply `grants` config in the target.

</details>
