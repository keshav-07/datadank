---
title: "Data Engineering interview questions and answers"
seoTitle: "Data Engineering Interview Questions and Answers"
description: "100 data engineering interview questions with answers: modeling, ETL vs ELT, CDC, Kafka, Airflow, file formats, data quality, governance and cost."
hub: "data-engineering"
label: "Data Engineering"
section: 1
publishedDate: "2026-10-16"
updatedDate: "2026-10-16"
levels: {"Beginner": 30, "Intermediate": 45, "Advanced": 25}
questions: [{"num": 1, "id": "DE-01", "title": "What does a data engineer do, and how is the role different from data scientist and analytics engineer?", "topic": "Role Fundamentals", "level": "Beginner", "frequency": "Very Common"}, {"num": 2, "id": "DE-02", "title": "What is a data warehouse, and how is it different from an operational database?", "topic": "Data Warehousing", "level": "Beginner", "frequency": "Very Common"}, {"num": 3, "id": "DE-03", "title": "OLTP vs OLAP: what are the design differences?", "topic": "Data Warehousing", "level": "Beginner", "frequency": "Very Common"}, {"num": 4, "id": "DE-04", "title": "Kimball vs Inmon: compare the two warehouse methodologies", "topic": "Data Warehousing", "level": "Intermediate", "frequency": "Very Common"}, {"num": 5, "id": "DE-05", "title": "Star schema vs snowflake schema: when would you pick each?", "topic": "Data Modeling", "level": "Beginner", "frequency": "Very Common"}, {"num": 6, "id": "DE-06", "title": "Explain transactional, periodic snapshot and accumulating snapshot fact tables", "topic": "Data Modeling", "level": "Intermediate", "frequency": "Common"}, {"num": 7, "id": "DE-07", "title": "What are additive, semi-additive and non-additive measures?", "topic": "Data Modeling", "level": "Intermediate", "frequency": "Common"}, {"num": 8, "id": "DE-08", "title": "What is a conformed dimension, and why does it matter for an enterprise bus matrix?", "topic": "Data Modeling", "level": "Intermediate", "frequency": "Common"}, {"num": 9, "id": "DE-09", "title": "What is the grain of a fact table, and how do you declare it?", "topic": "Data Modeling", "level": "Intermediate", "frequency": "Very Common"}, {"num": 10, "id": "DE-10", "title": "Surrogate keys vs natural keys", "topic": "Data Modeling", "level": "Beginner", "frequency": "Very Common"}, {"num": 11, "id": "DE-11", "title": "What are Slowly Changing Dimensions? Explain Types 0, 1, 2 and 3", "topic": "Data Warehousing (SCD)", "level": "Beginner", "frequency": "Very Common"}, {"num": 12, "id": "DE-12", "title": "How do you design and load an SCD Type 2 dimension?", "topic": "Data Warehousing (SCD)", "level": "Intermediate", "frequency": "Very Common"}, {"num": 13, "id": "DE-13", "title": "SCD Types 4, 5 and 6 (hybrid): when are they used?", "topic": "Data Warehousing (SCD)", "level": "Advanced", "frequency": "Occasional"}, {"num": 14, "id": "DE-14", "title": "Junk, degenerate and role-playing dimensions", "topic": "Data Modeling", "level": "Intermediate", "frequency": "Common"}, {"num": 15, "id": "DE-15", "title": "What is a factless fact table?", "topic": "Data Modeling", "level": "Intermediate", "frequency": "Occasional"}, {"num": 16, "id": "DE-16", "title": "How do you model many-to-many relationships with bridge tables?", "topic": "Data Modeling", "level": "Advanced", "frequency": "Occasional"}, {"num": 17, "id": "DE-17", "title": "Normalisation (1NF–3NF), and when to denormalise", "topic": "Data Modeling", "level": "Beginner", "frequency": "Very Common"}, {"num": 18, "id": "DE-18", "title": "Data Vault 2.0: hubs, links and satellites", "topic": "Data Modeling", "level": "Advanced", "frequency": "Common"}, {"num": 19, "id": "DE-19", "title": "One Big Table (wide denormalised) vs dimensional model", "topic": "Data Modeling", "level": "Intermediate", "frequency": "Common"}, {"num": 20, "id": "DE-20", "title": "Conceptual, logical and physical data models", "topic": "Data Modeling", "level": "Beginner", "frequency": "Common"}, {"num": 21, "id": "DE-21", "title": "Walk me through modelling a new business domain from raw requirements", "topic": "Data Modeling", "level": "Advanced", "frequency": "Common"}, {"num": 22, "id": "DE-22", "title": "How do you model event or clickstream data?", "topic": "Data Modeling", "level": "Intermediate", "frequency": "Common"}, {"num": 23, "id": "DE-23", "title": "ETL vs ELT", "topic": "ETL vs ELT", "level": "Beginner", "frequency": "Very Common"}, {"num": 24, "id": "DE-24", "title": "What are the components of a data pipeline?", "topic": "ETL vs ELT", "level": "Beginner", "frequency": "Very Common"}, {"num": 25, "id": "DE-25", "title": "Full load vs incremental load", "topic": "ETL vs ELT", "level": "Beginner", "frequency": "Very Common"}, {"num": 26, "id": "DE-26", "title": "High-watermark incremental loading and its pitfalls", "topic": "ETL vs ELT", "level": "Intermediate", "frequency": "Very Common"}, {"num": 27, "id": "DE-27", "title": "Batch vs stream processing", "topic": "Batch vs Streaming", "level": "Beginner", "frequency": "Very Common"}, {"num": 28, "id": "DE-28", "title": "Lambda vs Kappa architecture", "topic": "Batch vs Streaming", "level": "Advanced", "frequency": "Common"}, {"num": 29, "id": "DE-29", "title": "Event time vs processing time vs ingestion time", "topic": "Batch vs Streaming", "level": "Intermediate", "frequency": "Common"}, {"num": 30, "id": "DE-30", "title": "Tumbling, sliding and session windows", "topic": "Batch vs Streaming", "level": "Advanced", "frequency": "Common"}, {"num": 31, "id": "DE-31", "title": "Data lake vs data warehouse vs lakehouse", "topic": "Data Lake / Lakehouse", "level": "Beginner", "frequency": "Very Common"}, {"num": 32, "id": "DE-32", "title": "What is the medallion (bronze/silver/gold) architecture?", "topic": "Data Lake / Lakehouse", "level": "Beginner", "frequency": "Very Common"}, {"num": 33, "id": "DE-33", "title": "How do open table formats make ACID possible on object storage?", "topic": "Data Lake / Lakehouse", "level": "Advanced", "frequency": "Common"}, {"num": 34, "id": "DE-34", "title": "What is a data swamp, and how do you prevent one?", "topic": "Data Lake / Lakehouse", "level": "Intermediate", "frequency": "Occasional"}, {"num": 35, "id": "DE-35", "title": "What are the dimensions of data quality?", "topic": "Data Quality & Observability", "level": "Beginner", "frequency": "Very Common"}, {"num": 36, "id": "DE-36", "title": "How do you build data quality checks into a pipeline?", "topic": "Data Quality & Observability", "level": "Intermediate", "frequency": "Very Common"}, {"num": 37, "id": "DE-37", "title": "Data observability vs data quality testing", "topic": "Data Quality & Observability", "level": "Intermediate", "frequency": "Common"}, {"num": 38, "id": "DE-38", "title": "What are data contracts, and how do you enforce them?", "topic": "Data Quality & Observability", "level": "Advanced", "frequency": "Common"}, {"num": 39, "id": "DE-39", "title": "How do you handle bad records: quarantine tables and dead-letter queues", "topic": "Data Quality & Observability", "level": "Intermediate", "frequency": "Common"}, {"num": 40, "id": "DE-40", "title": "What is Apache Airflow? Explain DAGs, tasks and operators", "topic": "Orchestration (Airflow)", "level": "Beginner", "frequency": "Very Common"}, {"num": 41, "id": "DE-41", "title": "Airflow architecture: scheduler, executor, workers, metadata DB, API server", "topic": "Orchestration (Airflow)", "level": "Intermediate", "frequency": "Common"}, {"num": 42, "id": "DE-42", "title": "Airflow executors: Local vs Celery vs Kubernetes", "topic": "Orchestration (Airflow)", "level": "Advanced", "frequency": "Common"}, {"num": 43, "id": "DE-43", "title": "What are XComs, and what are their limits?", "topic": "Orchestration (Airflow)", "level": "Intermediate", "frequency": "Common"}, {"num": 44, "id": "DE-44", "title": "Sensors vs deferrable operators", "topic": "Orchestration (Airflow)", "level": "Advanced", "frequency": "Common"}, {"num": 45, "id": "DE-45", "title": "Airflow catchup, backfill, start_date and logical date", "topic": "Orchestration (Airflow)", "level": "Intermediate", "frequency": "Very Common"}, {"num": 46, "id": "DE-46", "title": "How do you make Airflow tasks idempotent and safe to retry?", "topic": "Orchestration (Airflow)", "level": "Intermediate", "frequency": "Common"}, {"num": 47, "id": "DE-47", "title": "What changed in Airflow 3 (Assets, DAG versioning, Task SDK)?", "topic": "Orchestration (Airflow)", "level": "Advanced", "frequency": "Occasional"}, {"num": 48, "id": "DE-48", "title": "Airflow vs Dagster vs Prefect vs cloud-native orchestrators", "topic": "Orchestration (Airflow)", "level": "Intermediate", "frequency": "Occasional"}, {"num": 49, "id": "DE-49", "title": "What is Change Data Capture? Log-based vs query-based vs trigger-based", "topic": "CDC", "level": "Intermediate", "frequency": "Very Common"}, {"num": 50, "id": "DE-50", "title": "Design a log-based CDC pipeline with Debezium and Kafka", "topic": "CDC", "level": "Advanced", "frequency": "Common"}, {"num": 51, "id": "DE-51", "title": "How do you handle deletes in CDC: soft vs hard deletes", "topic": "CDC", "level": "Intermediate", "frequency": "Common"}, {"num": 52, "id": "DE-52", "title": "Row-oriented vs columnar file formats", "topic": "File Formats", "level": "Beginner", "frequency": "Very Common"}, {"num": 53, "id": "DE-53", "title": "Parquet internals: row groups, column chunks, pages and statistics", "topic": "File Formats", "level": "Advanced", "frequency": "Common"}, {"num": 54, "id": "DE-54", "title": "Parquet vs ORC vs Avro", "topic": "File Formats", "level": "Beginner", "frequency": "Very Common"}, {"num": 55, "id": "DE-55", "title": "Delta Lake vs Apache Iceberg vs Apache Hudi", "topic": "File Formats (Table Formats)", "level": "Advanced", "frequency": "Very Common"}, {"num": 56, "id": "DE-56", "title": "How do you choose a partition key, and what is over-partitioning?", "topic": "Partitioning", "level": "Intermediate", "frequency": "Very Common"}, {"num": 57, "id": "DE-57", "title": "What is the small-files problem, and how do you fix it?", "topic": "Partitioning & File Formats", "level": "Intermediate", "frequency": "Very Common"}, {"num": 58, "id": "DE-58", "title": "Compression codecs: Snappy vs ZSTD vs Gzip", "topic": "File Formats", "level": "Intermediate", "frequency": "Occasional"}, {"num": 59, "id": "DE-59", "title": "How does schema evolution work across Avro, Parquet and table formats?", "topic": "File Formats", "level": "Advanced", "frequency": "Common"}, {"num": 60, "id": "DE-60", "title": "Explain the CAP theorem (and PACELC) for data engineers", "topic": "Distributed Systems", "level": "Intermediate", "frequency": "Common"}, {"num": 61, "id": "DE-61", "title": "What is idempotency, and why is it critical in pipelines?", "topic": "Distributed Systems", "level": "Beginner", "frequency": "Very Common"}, {"num": 62, "id": "DE-62", "title": "At-most-once vs at-least-once vs exactly-once delivery", "topic": "Distributed Systems", "level": "Advanced", "frequency": "Very Common"}, {"num": 63, "id": "DE-63", "title": "Horizontal vs vertical scaling", "topic": "Distributed Systems", "level": "Beginner", "frequency": "Common"}, {"num": 64, "id": "DE-64", "title": "Partitioning vs sharding vs replication", "topic": "Distributed Systems", "level": "Advanced", "frequency": "Common"}, {"num": 65, "id": "DE-65", "title": "What is consistent hashing?", "topic": "Distributed Systems", "level": "Advanced", "frequency": "Occasional"}, {"num": 66, "id": "DE-66", "title": "What is backpressure in streaming systems?", "topic": "Distributed Systems", "level": "Advanced", "frequency": "Occasional"}, {"num": 67, "id": "DE-67", "title": "Kafka core concepts: broker, topic, partition, offset, replication", "topic": "Kafka", "level": "Beginner", "frequency": "Very Common"}, {"num": 68, "id": "DE-68", "title": "Kafka consumer groups and rebalancing", "topic": "Kafka", "level": "Intermediate", "frequency": "Very Common"}, {"num": 69, "id": "DE-69", "title": "What ordering guarantees does Kafka provide?", "topic": "Kafka", "level": "Intermediate", "frequency": "Common"}, {"num": 70, "id": "DE-70", "title": "Kafka idempotent producers and transactions", "topic": "Kafka", "level": "Advanced", "frequency": "Common"}, {"num": 71, "id": "DE-71", "title": "Kafka retention vs log compaction", "topic": "Kafka", "level": "Advanced", "frequency": "Common"}, {"num": 72, "id": "DE-72", "title": "What is a schema registry, and what are compatibility modes?", "topic": "Kafka", "level": "Intermediate", "frequency": "Common"}, {"num": 73, "id": "DE-73", "title": "Kafka vs Kinesis vs Pub/Sub vs Event Hubs", "topic": "Kafka", "level": "Intermediate", "frequency": "Occasional"}, {"num": 74, "id": "DE-74", "title": "Map the core AWS data services (S3, Glue, EMR, Redshift, Kinesis, Athena)", "topic": "Cloud Data Services", "level": "Beginner", "frequency": "Very Common"}, {"num": 75, "id": "DE-75", "title": "Map the core Azure data services (ADLS Gen2, ADF, Synapse/Fabric, Event Hubs)", "topic": "Cloud Data Services", "level": "Beginner", "frequency": "Common"}, {"num": 76, "id": "DE-76", "title": "Map the core GCP data services (GCS, BigQuery, Dataflow, Dataproc, Pub/Sub, Composer)", "topic": "Cloud Data Services", "level": "Beginner", "frequency": "Common"}, {"num": 77, "id": "DE-77", "title": "Object vs block vs file storage", "topic": "Cloud Data Services", "level": "Beginner", "frequency": "Common"}, {"num": 78, "id": "DE-78", "title": "Serverless vs provisioned compute for data workloads", "topic": "Cloud Data Services", "level": "Intermediate", "frequency": "Common"}, {"num": 79, "id": "DE-79", "title": "AWS Glue: crawlers, Data Catalog and Glue jobs", "topic": "Cloud Data Services", "level": "Intermediate", "frequency": "Common"}, {"num": 80, "id": "DE-80", "title": "What is data governance? Catalog, ownership, stewardship", "topic": "Governance & Security", "level": "Beginner", "frequency": "Common"}, {"num": 81, "id": "DE-81", "title": "RBAC vs ABAC", "topic": "Governance & Security", "level": "Intermediate", "frequency": "Common"}, {"num": 82, "id": "DE-82", "title": "How do you protect PII: masking, tokenisation, hashing, encryption", "topic": "Governance & Security", "level": "Intermediate", "frequency": "Very Common"}, {"num": 83, "id": "DE-83", "title": "Encryption at rest and in transit, and key management", "topic": "Governance & Security", "level": "Beginner", "frequency": "Common"}, {"num": 84, "id": "DE-84", "title": "What is data lineage, and how is it captured?", "topic": "Governance & Security", "level": "Intermediate", "frequency": "Common"}, {"num": 85, "id": "DE-85", "title": "How does GDPR \"right to erasure\" affect lake and warehouse design?", "topic": "Governance & Security", "level": "Advanced", "frequency": "Common"}, {"num": 86, "id": "DE-86", "title": "How do you manage secrets in pipelines?", "topic": "Governance & Security", "level": "Beginner", "frequency": "Common"}, {"num": 87, "id": "DE-87", "title": "What does CI/CD mean for data pipelines?", "topic": "CI/CD for Data", "level": "Beginner", "frequency": "Common"}, {"num": 88, "id": "DE-88", "title": "The testing pyramid for data pipelines", "topic": "CI/CD for Data", "level": "Intermediate", "frequency": "Common"}, {"num": 89, "id": "DE-89", "title": "Dev/test/prod environments and promoting data code", "topic": "CI/CD for Data", "level": "Intermediate", "frequency": "Common"}, {"num": 90, "id": "DE-90", "title": "Infrastructure as Code (Terraform) for data platforms", "topic": "CI/CD for Data", "level": "Intermediate", "frequency": "Common"}, {"num": 91, "id": "DE-91", "title": "Write-Audit-Publish and blue-green deployments for data", "topic": "CI/CD for Data", "level": "Advanced", "frequency": "Common"}, {"num": 92, "id": "DE-92", "title": "Git branching strategies for data teams", "topic": "CI/CD for Data", "level": "Beginner", "frequency": "Occasional"}, {"num": 93, "id": "DE-93", "title": "General principles of cloud data cost optimisation", "topic": "Cost Optimisation", "level": "Intermediate", "frequency": "Very Common"}, {"num": 94, "id": "DE-94", "title": "Storage tiering and lifecycle policies", "topic": "Cost Optimisation", "level": "Intermediate", "frequency": "Common"}, {"num": 95, "id": "DE-95", "title": "FinOps: tagging, chargeback and showback", "topic": "Cost Optimisation", "level": "Intermediate", "frequency": "Occasional"}, {"num": 96, "id": "DE-96", "title": "Spot/preemptible and reserved capacity for data workloads", "topic": "Cost Optimisation", "level": "Intermediate", "frequency": "Occasional"}, {"num": 97, "id": "DE-97", "title": "What is reverse ETL?", "topic": "Serving & Activation", "level": "Intermediate", "frequency": "Occasional"}, {"num": 98, "id": "DE-98", "title": "What is data mesh, and when does it fail?", "topic": "Architecture & Organisation", "level": "Advanced", "frequency": "Common"}, {"num": 99, "id": "DE-99", "title": "What is a semantic/metrics layer?", "topic": "Serving & Activation", "level": "Intermediate", "frequency": "Occasional"}, {"num": 100, "id": "DE-100", "title": "Data engineering for ML and GenAI: feature stores, embeddings, vector stores", "topic": "ML & AI Data", "level": "Advanced", "frequency": "Occasional"}]
---

<details class="qb-item" id="q1" data-level="Beginner">
<summary><span class="qb-num">Q1</span><h3 class="qb-title">What does a data engineer do, and how is the role different from data scientist and analytics engineer?</h3><span class="qb-meta">Beginner · Very Common · Role Fundamentals</span></summary>

**Short answer**

A data engineer builds and runs the systems that move, store and prepare data so it is reliable and available. Data scientists use that data to build models and run experiments. Analytics engineers sit between the two and turn clean data into tested, business-ready models.

**Detailed explanation**

Think of the data platform as a supply chain. The data engineer owns the plumbing:

- **Ingestion:** pulling data from databases, APIs, files and event streams.
- **Storage:** designing lake and warehouse layouts, file formats and partitioning.
- **Processing:** batch and streaming jobs that clean, join and reshape data.
- **Reliability:** orchestration, retries, monitoring, data quality checks and SLAs.
- **Platform concerns:** security, access control, cost and performance.

A data scientist consumes this output to answer questions with statistics and machine learning. Their success depends on whether the features they need exist, are fresh and are correct, which is why the two roles work closely together.

The analytics engineer is a newer title that grew with tools like dbt. They write SQL transformations, define business logic (what counts as an "active customer"), add tests and documentation, and publish marts that analysts and BI tools query.

| Role | Main output | Typical tools | Success measure |
|---|---|---|---|
| Data engineer | Pipelines, platform, raw-to-clean data | Spark, Kafka, Airflow, cloud services, Python | Data is fresh, correct, cheap, available |
| Analytics engineer | Business-ready models and metrics | SQL, dbt, warehouse | Trusted, documented metrics |
| Data scientist | Models, experiments, insights | Python, notebooks, ML libraries | Model accuracy, business impact |

In smaller companies one person often covers all three. In larger ones the boundaries are sharper and data engineers lean further toward software and infrastructure work.

**Real-world use case**

In an e-commerce company, the data engineer streams order events from the application database into the lakehouse. The analytics engineer builds a `fct_orders` model with a tested revenue definition. The data scientist uses it to train a demand-forecasting model.

**Common mistakes and red flags**

- Describing the job as "writing SQL queries" with no mention of reliability, scale or ownership.
- Saying data engineers "don't need to understand the business".
- Not knowing where their own pipelines' data ends up or who uses it.

**Follow-up questions**

- Which part of the role do you enjoy most? — Pick one area and back it with a concrete project.
- How do you work with data scientists? — Agree feature definitions and freshness SLAs up front, and give them production-grade tables, not ad-hoc extracts.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q2" data-level="Beginner">
<summary><span class="qb-num">Q2</span><h3 class="qb-title">What is a data warehouse, and how is it different from an operational database?</h3><span class="qb-meta">Beginner · Very Common · Data Warehousing</span></summary>

**Short answer**

A data warehouse is a central store built for analysis. It holds integrated, historical data from many systems and is organised for fast reads over large volumes. An operational database runs the day-to-day application and is optimised for many small, fast writes and lookups.

**Detailed explanation**

Bill Inmon's classic definition still holds: a warehouse is **subject-oriented, integrated, time-variant and non-volatile**. In plain terms:

- **Subject-oriented:** organised around business areas (sales, customers, inventory), not around one application's screens.
- **Integrated:** customer IDs, currencies and date formats from different sources are reconciled into one consistent view.
- **Time-variant:** it keeps history, so you can ask what the numbers looked like last quarter.
- **Non-volatile:** data is loaded and appended rather than constantly overwritten by users.

An operational database (Postgres behind a checkout service, for example) has different goals. It must handle thousands of concurrent inserts and updates, enforce constraints and respond in milliseconds for single-row operations. It is usually highly normalised to avoid update anomalies and often keeps only current state.

Running heavy analytical queries on the operational database competes with the application for resources and can slow down real customers. That is the main practical reason the two are separated.

| Aspect | Operational database | Data warehouse |
|---|---|---|
| Workload | Many short reads/writes | Fewer, large scans and aggregations |
| Data scope | One application, mostly current | Many sources, full history |
| Schema | Normalised (3NF) | Dimensional or denormalised |
| Storage layout | Usually row-oriented | Usually columnar |
| Users | Applications | Analysts, BI, data science |

Modern cloud warehouses (Snowflake, BigQuery, Redshift, Databricks SQL) separate storage from compute, so analytical scale no longer means buying one huge server.

**Real-world use case**

A bank's core system processes card payments in real time. Every night (or continuously, through CDC) those transactions land in the warehouse. There, risk analysts join them with customer, merchant and fraud-label data covering several years.

**Common mistakes and red flags**

- Saying a warehouse is "just a bigger database".
- Thinking a read replica of the OLTP database is a warehouse. It removes load but provides no integration or history.
- Not knowing why columnar storage helps analytics.

**Follow-up questions**

- What is a data mart? — A subject-specific subset of the warehouse built for one team or domain.
- Can a lakehouse replace a warehouse? — Often yes for analytics, if it provides ACID tables, governance and good SQL performance (see [Q31](/interview/data-engineering/#q31)).

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q3" data-level="Beginner">
<summary><span class="qb-num">Q3</span><h3 class="qb-title">OLTP vs OLAP: what are the design differences?</h3><span class="qb-meta">Beginner · Very Common · Data Warehousing</span></summary>

**Short answer**

OLTP (Online Transaction Processing) systems handle many small, concurrent transactions with strict consistency. OLAP (Online Analytical Processing) systems answer large aggregate queries over history. Their schema, storage layout, indexing and hardware choices are optimised in opposite directions.

**Detailed explanation**

The differences follow from the access pattern.

An **OLTP** query typically touches one or a few rows by key: "insert this order", "update this account balance". So OLTP systems use:
- **Row-oriented storage**, so a whole record is read or written in one I/O.
- **B-tree indexes** on primary and foreign keys for fast point lookups.
- **Normalised schemas** (3NF) so each fact is stored once and updates stay consistent.
- **Strong transactional guarantees** (ACID, row-level locking or MVCC) because many users write at once.

An **OLAP** query typically scans millions of rows but only a few columns: "revenue by region by month for three years". So OLAP systems use:
- **Columnar storage**, so only the needed columns are read and similar values compress well.
- **Partition and file pruning** using min/max metadata instead of many secondary indexes.
- **Denormalised or dimensional schemas** (star schema) to reduce joins.
- **Massively parallel processing (MPP)** that splits a scan across many nodes.

| Dimension | OLTP | OLAP |
|---|---|---|
| Typical query | Point read/write by key | Scan + aggregate |
| Rows touched | 1–100 | Millions to billions |
| Concurrency | Thousands of users | Tens to hundreds of queries |
| Schema | Normalised | Star/snowflake, wide tables |
| Storage | Row store | Column store |
| Latency target | Milliseconds | Seconds to minutes |
| Examples | PostgreSQL, MySQL, Oracle, SQL Server | Snowflake, BigQuery, Redshift, Databricks SQL |

Some products now blur the line. HTAP (hybrid transactional/analytical processing) databases and features like Snowflake hybrid tables try to serve both. In interviews, still explain the classic split first.

**Real-world use case**

A ride-hailing app writes each trip to an OLTP database as it happens. Pricing analysts query an OLAP warehouse to compare surge multipliers across cities and weeks without touching the production database.

**Common mistakes and red flags**

- Claiming OLAP systems "don't support updates". They do, but updates are costlier because files are immutable or columnar.
- Suggesting more indexes to speed up warehouse scans.
- Mixing up OLAP the workload with "OLAP cubes" the older technology.

**Follow-up questions**

- Why is columnar storage better for aggregates? — It reads only the referenced columns and compresses similar values, which cuts I/O dramatically.
- What is MVCC? — Multi-version concurrency control: readers see a consistent snapshot while writers create new row versions, so reads don't block writes.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q4" data-level="Intermediate">
<summary><span class="qb-num">Q4</span><h3 class="qb-title">Kimball vs Inmon: compare the two warehouse methodologies</h3><span class="qb-meta">Intermediate · Very Common · Data Warehousing</span></summary>

**Short answer**

Inmon is top-down: first build a normalised, enterprise-wide warehouse, then derive dimensional data marts from it. Kimball is bottom-up: build dimensional star schemas per business process, linked by conformed dimensions so together they form the warehouse.

**Detailed explanation**

**Inmon (Corporate Information Factory).** Source data is integrated into a central enterprise data warehouse in third normal form. That layer is the single, consistent version of the truth. Departmental data marts, usually dimensional, are built downstream for reporting. The approach is strong on integration and consistency, but it needs substantial upfront modelling, and the business waits longer for its first reports.

**Kimball (Dimensional Bus Architecture).** You pick one business process (orders, shipments, payments) and build a fact table at a declared grain, surrounded by dimension tables. Each new process adds another star. Shared dimensions such as `dim_customer` and `dim_date` are **conformed**: built once and reused, so metrics from different stars can be compared. The **bus matrix** (processes × dimensions) is the planning tool. You get faster delivery and business-friendly models, but you need discipline to keep dimensions conformed.

| Aspect | Inmon | Kimball |
|---|---|---|
| Direction | Top-down | Bottom-up |
| Core layer | Normalised EDW (3NF) | Dimensional star schemas |
| Marts | Derived from EDW | The marts are the warehouse |
| Time to first value | Longer | Shorter |
| Integration mechanism | Central normalised model | Conformed dimensions |
| Change handling | Change the EDW, then the marts | Add or modify stars |

Most modern stacks are a hybrid. A raw or staging layer lands source data, an integration layer (sometimes Data Vault, see [Q18](/interview/data-engineering/#q18)) cleans and conforms it, and a Kimball-style presentation layer serves BI. The medallion architecture (bronze/silver/gold) maps to the same idea.

**Real-world use case**

A retailer starts with a Kimball sales star to unblock finance reporting in six weeks. It then adds inventory and returns stars that reuse `dim_product` and `dim_store`, so margin-after-returns can be calculated across processes.

**Common mistakes and red flags**

- Saying "Kimball means no normalisation anywhere". Staging layers are often normalised.
- Not mentioning conformed dimensions when describing Kimball.
- Treating the choice as all-or-nothing instead of describing a layered hybrid.

**Follow-up questions**

- What is a bus matrix? — A grid of business processes against the dimensions they share. It guides which dimensions must be conformed.
- Which do you use today? — Usually a layered hybrid: integrated silver layer, dimensional gold layer.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q5" data-level="Beginner">
<summary><span class="qb-num">Q5</span><h3 class="qb-title">Star schema vs snowflake schema: when would you pick each?</h3><span class="qb-meta">Beginner · Very Common · Data Modeling</span></summary>

**Short answer**

A star schema has a central fact table joined directly to flat, denormalised dimension tables. A snowflake schema normalises dimensions into sub-tables (product → category → department). Star is the default for analytics because it is simpler and needs fewer joins. Snowflake helps when dimension hierarchies are huge or shared.

**Detailed explanation**

In a **star schema**, `fct_sales` holds foreign keys and numeric measures, and each dimension is one wide table. `dim_product` contains product name, brand, category and department all in one row. BI tools and analysts find it intuitive, and query engines handle the single-hop joins well.

In a **snowflake schema**, the dimension is split: `dim_product` references `dim_category`, which references `dim_department`. This removes repeated text (the department name is stored once rather than on every product row) and makes hierarchy changes a one-row update.

| Aspect | Star | Snowflake |
|---|---|---|
| Dimension structure | Denormalised, one table per dimension | Normalised into multiple tables |
| Joins per query | Fewer | More |
| Storage | Slightly more (repeated attributes) | Slightly less |
| Query simplicity | High | Lower |
| Maintenance of hierarchies | Update many rows | Update one row |
| BI tool friendliness | Very good | Good, but needs more modelling |

On columnar cloud warehouses, the storage argument for snowflaking is weak: repeated strings compress very well. The performance argument usually favours stars. Snowflaking still makes sense when:
- a sub-dimension is large and shared by several dimensions (for example a geography hierarchy used by customer, store and supplier);
- a source system already delivers the hierarchy normalised and you need it for governance;
- you need different SCD behaviour on different levels of a hierarchy.

**Example**

```sql
-- Dialect: ANSI SQL
-- Star: product attributes flattened into one dimension
SELECT d.department_name,
       SUM(f.net_amount) AS revenue
FROM fct_sales f
JOIN dim_product d ON f.product_key = d.product_key
GROUP BY d.department_name;

-- Snowflake: same question needs two extra joins
SELECT dep.department_name,
       SUM(f.net_amount) AS revenue
FROM fct_sales f
JOIN dim_product p     ON f.product_key = p.product_key
JOIN dim_category c    ON p.category_key = c.category_key
JOIN dim_department dep ON c.department_key = dep.department_key
GROUP BY dep.department_name;
```

**Real-world use case**

A marketplace keeps a star schema for its sales dashboards. It maintains a separate normalised `dim_geography` that the customer, seller and warehouse dimensions all reference, so region definitions are managed in one place.

**Common mistakes and red flags**

- Confusing "snowflake schema" with the Snowflake product.
- Saying snowflake schema is always faster because it is "normalised".
- Putting measures in dimension tables.

**Follow-up questions**

- What is a galaxy (fact constellation) schema? — Multiple fact tables sharing conformed dimensions.
- Would you snowflake on Snowflake or BigQuery? — Rarely; compression makes the storage saving small, and extra joins add complexity.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q6" data-level="Intermediate">
<summary><span class="qb-num">Q6</span><h3 class="qb-title">Explain transactional, periodic snapshot and accumulating snapshot fact tables</h3><span class="qb-meta">Intermediate · Common · Data Modeling</span></summary>

**Short answer**

A transactional fact stores one row per event (an order line). A periodic snapshot stores the state of something at regular intervals (daily account balance). An accumulating snapshot stores one row per process instance (an order) that is updated as the process moves through milestones.

**Detailed explanation**

**Transactional facts** are the most common. Each row is an atomic event at a fixed moment: a payment, a click, an order line. Rows are inserted and rarely updated. They are the most granular and flexible type, because any time-based rollup can be computed from them.

**Periodic snapshot facts** capture a measurement at a regular cadence, whether or not anything happened. Examples: end-of-day inventory per product per warehouse, or month-end account balance. They answer "what was the level?" questions cheaply. Rebuilding a balance from millions of transactions every time is expensive. Their measures are often **semi-additive**: you can sum across warehouses but not across days (see [Q7](/interview/data-engineering/#q7)).

**Accumulating snapshot facts** model a process with a defined start and end and several milestones, such as order placed → paid → shipped → delivered. There is one row per order, with a date key per milestone and lag measures like `days_to_ship`. The row is **updated** as each milestone happens, which makes this the only fact type that is routinely updated in place.

| Type | Grain | Row behaviour | Typical measures | Example |
|---|---|---|---|---|
| Transactional | One event | Insert only | Amount, quantity | Order lines |
| Periodic snapshot | Entity × period | Insert one row per period | Balance, stock level | Daily inventory |
| Accumulating snapshot | One process instance | Updated at each milestone | Lags, counts per stage | Order fulfilment pipeline |

**Example**

```sql
-- Dialect: ANSI SQL
-- Accumulating snapshot: one row per order, milestone dates filled in over time
CREATE TABLE fct_order_fulfilment (
    order_key          BIGINT       NOT NULL,
    customer_key       BIGINT       NOT NULL,
    placed_date_key    INT          NOT NULL,
    paid_date_key      INT,
    shipped_date_key   INT,
    delivered_date_key INT,
    days_placed_to_ship INT,
    days_ship_to_deliver INT,
    order_amount       DECIMAL(12,2) NOT NULL
);
```

**Real-world use case**

A logistics team uses an accumulating snapshot to track where shipments stall. Finance uses a periodic snapshot of accounts receivable each month-end. Both are derived from the same transactional event tables.

**Common mistakes and red flags**

- Summing a balance from a periodic snapshot across days.
- Building an accumulating snapshot for a process with no clear end.
- Not knowing that accumulating snapshots require updates or MERGE, which affects load design.

**Follow-up questions**

- How do you load an accumulating snapshot? — MERGE on the process key, filling milestone columns as events arrive.
- Can you derive a periodic snapshot from transactions? — Yes, with a running sum per entity over a date spine, but storing it saves repeated compute.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q7" data-level="Intermediate">
<summary><span class="qb-num">Q7</span><h3 class="qb-title">What are additive, semi-additive and non-additive measures?</h3><span class="qb-meta">Intermediate · Common · Data Modeling</span></summary>

**Short answer**

An additive measure can be summed across every dimension (sales amount). A semi-additive measure can be summed across some dimensions but not time (account balance). A non-additive measure cannot be summed at all (ratios, percentages, distinct counts). You recompute it from its components instead.

**Detailed explanation**

Classifying measures correctly prevents some of the most common BI bugs.

- **Additive:** quantity, revenue, cost. Summing them by day, product, region or any combination gives a correct answer. Transactional fact tables mostly hold additive measures.
- **Semi-additive:** stock on hand, account balance, headcount. Summing across warehouses for one day is valid. Summing across days is not: three days of a 100-unit balance is not 300 units. Over time you use the last value in the period (closing balance), an average or a minimum or maximum instead.
- **Non-additive:** margin percentage, unit price, conversion rate, `COUNT(DISTINCT customer_id)`. Adding two daily conversion rates gives nonsense. Two days' distinct customers can overlap, so their sum overcounts.

The safe pattern for non-additive measures is to **store the additive components** and compute the ratio at query time. Store `revenue` and `cost`, then compute `(revenue - cost) / revenue` after aggregating. Never average pre-computed percentages unless the weights are equal.

**Example**

```sql
-- Dialect: ANSI SQL
-- Wrong: averaging daily margin percentages weights small days the same as big ones
SELECT AVG(margin_pct) AS margin_pct_wrong
FROM daily_store_margin;

-- Right: aggregate components first, then divide
SELECT (SUM(revenue) - SUM(cost)) / NULLIF(SUM(revenue), 0) AS margin_pct
FROM daily_store_margin;
```

Sample: Day 1 revenue 100, cost 50 (50%); Day 2 revenue 1,000, cost 900 (10%).
Wrong answer = 30%. Correct answer = (1,100 − 950) / 1,100 = 13.6%.

**Real-world use case**

A subscription business reports monthly active users. Data engineers store the per-user activity facts and compute distinct counts per period. Summing daily active users into a monthly figure would inflate it.

**Common mistakes and red flags**

- Summing balances or inventory over time.
- Pre-computing ratios in the fact table and letting BI tools sum or average them.
- Not mentioning `NULLIF` or divide-by-zero handling when computing ratios.

**Follow-up questions**

- How do semantic layers handle this? — Metrics are defined as ratios of aggregated components, so tools compute them correctly at any grain (see [Q99](/interview/data-engineering/#q99)).
- How do you combine distinct counts across partitions cheaply? — Use mergeable sketches such as HyperLogLog (see [Q150](/interview/sql/#q150)).

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q8" data-level="Intermediate">
<summary><span class="qb-num">Q8</span><h3 class="qb-title">What is a conformed dimension, and why does it matter for an enterprise bus matrix?</h3><span class="qb-meta">Intermediate · Common · Data Modeling</span></summary>

**Short answer**

A conformed dimension is a single, shared dimension, with the same keys, attributes and meaning, used by multiple fact tables. It lets you combine and compare metrics across business processes ("drill across"). The bus matrix is the plan that shows which facts share which conformed dimensions.

**Detailed explanation**

Imagine the sales team and the support team each build their own customer table. Sales calls a customer "active" if they ordered in 90 days. Support uses "has an open contract". The surrogate keys differ too. Now nobody can reliably answer "revenue per support ticket by customer segment", because the two stars cannot be joined on a common customer.

A conformed dimension fixes this. `dim_customer` is built once, owned by one team, and reused by `fct_orders`, `fct_tickets` and `fct_subscriptions`. Conformance can be:

- **Identical:** the same physical table referenced by every fact.
- **Shrunken (rolled-up):** a subset of rows or a higher-grain version, such as `dim_month` derived from `dim_date`, with attribute names and values kept consistent.

The **bus matrix** lists business processes as rows and dimensions as columns, with a tick where they intersect. It shows you:
- which dimensions are reused most and should be built and conformed first;
- the integration scope before any tables exist;
- where drill-across reports will be possible.

| Process | Date | Customer | Product | Store | Agent |
|---|---|---|---|---|---|
| Orders | ✓ | ✓ | ✓ | ✓ | |
| Returns | ✓ | ✓ | ✓ | ✓ | |
| Support tickets | ✓ | ✓ | ✓ | | ✓ |

**Example**

```sql
-- Dialect: ANSI SQL
-- Drill across two facts using the conformed customer dimension
WITH rev AS (
    SELECT customer_key, SUM(net_amount) AS revenue
    FROM fct_orders GROUP BY customer_key
),
tix AS (
    SELECT customer_key, COUNT(*) AS ticket_count
    FROM fct_tickets GROUP BY customer_key
)
SELECT c.segment,
       SUM(rev.revenue)                        AS revenue,
       SUM(COALESCE(tix.ticket_count, 0))      AS tickets
FROM dim_customer c
LEFT JOIN rev ON rev.customer_key = c.customer_key
LEFT JOIN tix ON tix.customer_key = c.customer_key
GROUP BY c.segment;
```
Aggregating each fact separately before joining avoids the fan-out that a direct fact-to-fact join causes (see [Q127](/interview/sql/#q127)).

**Real-world use case**

A telecom company conforms `dim_customer`, `dim_date` and `dim_plan` across billing, network usage and churn facts. Leadership can then see revenue, usage and churn risk per plan in one report.

**Common mistakes and red flags**

- Joining two fact tables directly instead of through conformed dimensions.
- Letting each team build its own copy of a core dimension.
- Not knowing what a shrunken dimension is.

**Follow-up questions**

- Who should own a conformed dimension? — A single team or data product owner with change control, because many facts depend on it.
- How do you conform IDs from different source systems? — Build a mapping or master-data step that assigns one surrogate key per real-world entity.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q9" data-level="Intermediate">
<summary><span class="qb-num">Q9</span><h3 class="qb-title">What is the grain of a fact table, and how do you declare it?</h3><span class="qb-meta">Intermediate · Very Common · Data Modeling</span></summary>

**Short answer**

The grain is the exact business meaning of one row in a fact table, for example "one row per order line per shipment". You declare it in plain language before choosing dimensions or measures. Every column must be true at that grain.

**Detailed explanation**

Kimball's four-step design process is: choose the business process, **declare the grain**, identify the dimensions, identify the facts. The grain comes second for a reason. It decides which dimensions are valid and which measures can live in the table.

A good grain statement is specific and testable:
- Vague: "sales data".
- Precise: "one row per product scanned on a receipt at a store register".

Once declared:
- **Dimensions** must have exactly one value per row at that grain. A receipt line has one product, one store, one date and one cashier. "Promotion" may need a bridge if a line can have several.
- **Measures** must be true at that grain. Line quantity and line amount fit. Receipt-level tax or order-level shipping cost does not, unless you allocate it down to lines.
- **The primary key** is usually the combination of dimension keys plus a degenerate identifier (receipt number and line number). You test that combination for uniqueness.

Prefer the **most atomic grain** the source supports. Atomic data can always be rolled up. Aggregated data cannot be broken back down, and new questions ("which basket combinations sell together?") will need the detail.

Mixed grains are the classic failure: order headers and order lines in the same table, with header-level freight repeated on each line. Summing freight then overcounts it by the number of lines.

**Example**

```sql
-- Dialect: ANSI SQL
-- Test that the declared grain (order_id, line_number) is actually unique
SELECT order_id, line_number, COUNT(*) AS row_count
FROM fct_order_lines
GROUP BY order_id, line_number
HAVING COUNT(*) > 1;
-- Any rows returned mean the grain is violated (duplicates or mixed grain)
```

**Real-world use case**

An airline declares its booking fact as "one row per passenger per flight segment". Fare per segment is a valid measure. Baggage fees, charged per booking, go to a separate booking-level fact so they are not repeated across segments.

**Common mistakes and red flags**

- Picking dimensions first and discovering the grain later.
- Storing header-level measures on line-level rows.
- Not having a uniqueness test on the grain columns.

**Follow-up questions**

- What if the business only needs daily totals? — Still keep an atomic fact if it is affordable, and build the daily aggregate on top of it.
- How do you handle header-level charges? — Allocate them to lines with a documented rule, or keep a separate fact at header grain.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q10" data-level="Beginner">
<summary><span class="qb-num">Q10</span><h3 class="qb-title">Surrogate keys vs natural keys</h3><span class="qb-meta">Beginner · Very Common · Data Modeling</span></summary>

**Short answer**

A natural key comes from the business or source system (customer email, SKU, employee number). A surrogate key is a meaningless identifier generated in the warehouse, such as an integer sequence or a hash. Dimensions use surrogate keys so they can keep history, combine sources and stay stable when source IDs change.

**Detailed explanation**

Natural keys are tempting because they already exist, but they cause problems in a warehouse:

- **They change.** People change email addresses. Companies renumber products after a migration.
- **They collide across sources.** Customer `1001` in the CRM and customer `1001` in the billing system may be different people.
- **They cannot represent history.** In an SCD Type 2 dimension (see [Q12](/interview/data-engineering/#q12)), one customer has several rows over time, so the natural key is no longer unique. Each version needs its own surrogate key.
- **They can be wide or composite**, which makes joins slower and fact tables larger.

Surrogate keys solve these by giving each dimension row its own stable identifier. Facts store the surrogate key of the dimension version that was valid when the event happened.

Common ways to generate them:

| Method | Pros | Cons |
|---|---|---|
| Sequence / identity column | Compact integers, fast joins | Needs a central generator; values differ between environments |
| Hash of natural key (+ effective date for SCD2) | Deterministic, parallel-friendly, same across reruns | Larger (string/binary); tiny collision risk |
| UUID | Unique without coordination | Large, random, not deterministic |

In modern ELT, hash-based keys (for example an MD5 over the business key columns) are popular because a rerun produces the same key, which helps idempotency (see [Q61](/interview/data-engineering/#q61)). Integer sequences remain common in classic warehouses.

Keep the natural key as a regular column in the dimension. It is needed for lookups during loading and for tracing data back to the source.

**Example**

```sql
-- Dialect: Snowflake SQL
-- Deterministic surrogate key from source system + business key
SELECT
    MD5(source_system || '|' || customer_id) AS customer_key,
    source_system,
    customer_id,      -- natural key kept for lineage
    customer_name,
    email
FROM stg_customers;
```

**Real-world use case**

After merging two retail brands, both had a product numbered `A-100`. Surrogate keys built from `brand || '|' || sku` kept them distinct in `dim_product`, and no fact rows were mis-attributed.

**Common mistakes and red flags**

- Using email or phone number as the primary key of a customer dimension.
- Dropping the natural key from the dimension.
- Hashing columns without a delimiter, so `('ab','c')` and `('a','bc')` produce the same key.

**Follow-up questions**

- Why add a delimiter before hashing? — To avoid two different combinations producing the same concatenated string.
- How do you handle facts that arrive before their dimension row? — Insert an "inferred" placeholder dimension row with the natural key and update it when the real row arrives.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q11" data-level="Beginner">
<summary><span class="qb-num">Q11</span><h3 class="qb-title">What are Slowly Changing Dimensions? Explain Types 0, 1, 2 and 3</h3><span class="qb-meta">Beginner · Very Common · Data Warehousing (SCD)</span></summary>

**Short answer**

Slowly Changing Dimensions (SCDs) are techniques for handling dimension attributes that change over time. Type 0 never changes. Type 1 overwrites the old value. Type 2 adds a new row per change to keep full history. Type 3 adds a column holding the previous value.

**Detailed explanation**

Dimension attributes such as a customer's city or a product's category do change, just not often. The question is whether reports should show facts against the value at the time of the event or the current value. Each SCD type answers that differently, and you can choose per attribute.

- **Type 0 (retain original):** the value is fixed at first load and never updated. Good for "original signup channel" or "date of birth".
- **Type 1 (overwrite):** replace the old value. No history is kept, so past facts now report under the new value. Good for corrections (a typo in a name) or attributes nobody analyses historically.
- **Type 2 (add new row):** close the current row (set an end date and clear the current flag) and insert a new row with a new surrogate key. Facts loaded earlier still point to the old row, so history is preserved exactly. This is the most common answer for attributes that drive analysis, such as region, segment or price tier.
- **Type 3 (add new column):** keep `current_region` and `previous_region` in the same row. It holds only one prior value, and is useful for "before and after" comparisons such as a sales territory realignment.

| Type | History kept | Row count grows? | Typical use |
|---|---|---|---|
| 0 | None (value frozen) | No | Original attributes |
| 1 | None (overwritten) | No | Corrections, non-analytical fields |
| 2 | Full | Yes | Region, segment, tier |
| 3 | One previous value | No (extra column) | Planned one-time reorganisations |

Types 4, 5 and 6 are covered in [Q13](/interview/data-engineering/#q13). The mechanics of loading Type 2 are in [Q12](/interview/data-engineering/#q12).

**Example**

```sql
-- Dialect: ANSI SQL
-- Type 1: simple overwrite of a corrected attribute
UPDATE dim_customer
SET customer_name = 'Priya Sharma'
WHERE customer_id = 'C-2041';
```

**Real-world use case**

A sales organisation tracks account manager on `dim_account` as Type 2, so commissions are attributed to whoever owned the account at the time of each deal. It treats typo fixes in the account name as Type 1.

**Common mistakes and red flags**

- Applying Type 2 to every column, including ones that change constantly. That explodes the row count (see [Q13](/interview/data-engineering/#q13) for mini-dimensions).
- Saying Type 1 "keeps history in the logs".
- Not realising one table can mix types across columns.

**Follow-up questions**

- Which type for a customer's credit score that changes monthly? — Often a mini-dimension (Type 4) or a periodic snapshot fact, not Type 2 on the main dimension.
- How do facts join to a Type 2 dimension? — Through the surrogate key valid at the event time, looked up by effective date range.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q12" data-level="Intermediate">
<summary><span class="qb-num">Q12</span><h3 class="qb-title">How do you design and load an SCD Type 2 dimension?</h3><span class="qb-meta">Intermediate · Very Common · Data Warehousing (SCD)</span></summary>

**Short answer**

Add a surrogate key, the natural key, `valid_from`, `valid_to` and an `is_current` flag. On each load, compare incoming rows to current rows on the tracked attributes. Close the changed rows by setting `valid_to` and clearing `is_current`, then insert new versions. Brand-new keys are simply inserted.

**Detailed explanation**

**Table design**
- `customer_key`: surrogate key, unique per version.
- `customer_id`: natural key.
- Tracked attributes, for example `city` and `segment`.
- `valid_from` / `valid_to`: the effective range. Use a far-future date such as `9999-12-31` for the open row rather than NULL, so range joins are simpler.
- `is_current`: a convenience flag for "latest version" queries.
- Optional `row_hash`: a hash of the tracked attributes for cheap change detection.

**Load logic (per batch)**
1. Stage the incoming snapshot or the changed rows and compute `row_hash`.
2. **Expire:** for natural keys where the hash differs from the current row, set `valid_to` to the change timestamp and `is_current = FALSE`.
3. **Insert:** add a row for each new natural key and each changed key, with `valid_from` set to the change timestamp, the open-ended `valid_to` and `is_current = TRUE`.
4. Run steps 2 and 3 in one transaction (or one MERGE) so readers never see a key with zero or two current rows.

**Decisions to make explicitly**
- **Effective timestamp source:** use the source's change time when available. The load time is a fallback and distorts history when loads are late.
- **Deletes:** close the row, and optionally insert a tombstone version with `is_deleted = TRUE`.
- **Late or out-of-order changes:** a change older than the current version means splitting an existing range. This is rarer and needs a reprocessing path (see [Q411](/interview/scenario-based/#q411)).

**Example**

```sql
-- Dialect: Snowflake SQL
-- Step 1: expire current rows whose tracked attributes changed
UPDATE dim_customer d
SET valid_to = s.changed_at,
    is_current = FALSE
FROM stg_customer_changes s
WHERE d.customer_id = s.customer_id
  AND d.is_current = TRUE
  AND d.row_hash <> s.row_hash;

-- Step 2: insert new versions (changed keys) and brand-new keys
INSERT INTO dim_customer
    (customer_key, customer_id, city, segment, row_hash, valid_from, valid_to, is_current)
SELECT MD5(s.customer_id || '|' || TO_VARCHAR(s.changed_at)),
       s.customer_id, s.city, s.segment, s.row_hash,
       s.changed_at, '9999-12-31'::TIMESTAMP_NTZ, TRUE
FROM stg_customer_changes s
LEFT JOIN dim_customer d
       ON d.customer_id = s.customer_id
      AND d.is_current = TRUE
WHERE d.customer_id IS NULL;   -- no current row: new key, or just expired in step 1
```
Wrap both statements in `BEGIN ... COMMIT` so they apply atomically.

Sample: `C-1` is current in Pune. A change arrives moving it to Mumbai at `2026-03-01`. After the load there are two rows: Pune (`valid_to = 2026-03-01`, `is_current = FALSE`) and Mumbai (`valid_from = 2026-03-01`, `is_current = TRUE`).

**Real-world use case**

An insurer keeps policyholder risk tier as SCD2. Claims facts look up the tier valid on the claim date, so loss ratios by tier reflect the tier customers actually had at the time.

**Common mistakes and red flags**

- Running expire and insert as separate, non-atomic jobs.
- Using load time as `valid_from` when the source provides a change timestamp.
- Creating a new version when nothing tracked changed (no hash comparison).
- Overlapping or gapped validity ranges for the same natural key.

**Follow-up questions**

- How do you test an SCD2 table? — Exactly one current row per natural key, no overlapping ranges, and `valid_from < valid_to`.
- How does dbt do this? — Snapshots implement the same pattern (see [Q311](/interview/dbt/#q311)). On Databricks, AUTO CDC can maintain it (see [Q215](/interview/databricks/#q215)).

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q13" data-level="Advanced">
<summary><span class="qb-num">Q13</span><h3 class="qb-title">SCD Types 4, 5 and 6 (hybrid): when are they used?</h3><span class="qb-meta">Advanced · Occasional · Data Warehousing (SCD)</span></summary>

**Short answer**

Type 4 moves rapidly changing attributes into a separate mini-dimension (or history table) so the main dimension stays small. Type 5 adds a Type 1 reference to the current mini-dimension row on the base dimension. Type 6 combines Types 1, 2 and 3: SCD2 rows that also carry a current-value column overwritten on every version.

**Detailed explanation**

These types exist because Type 2 alone has limits.

**Type 4: mini-dimension / history table.** Attributes such as age band, income band or credit score band change often. Tracking them as Type 2 on a 50-million-row customer dimension would multiply its size. Instead, the bands go into a small `dim_customer_profile` with one row per *combination* of band values, and the fact table carries both `customer_key` and `profile_key`. Some teams use "Type 4" more loosely to mean "keep current values in one table and full history in a separate history table". Interviewers accept either if you explain it.

**Type 5 (Type 4 + Type 1 outrigger).** The base customer dimension also carries a Type 1 `current_profile_key`, overwritten whenever the profile changes. Analysts can then report facts by the customer's *current* profile without going through the fact table.

**Type 6 (1 + 2 + 3).** Each SCD2 row has both `historical_region` (the value when that version was valid) and `current_region` (overwritten on all versions of the key whenever it changes). One table answers both "as it was" and "as it is now".

| Type | Mechanism | Solves |
|---|---|---|
| 4 | Mini-dimension (or separate history table) | Fast-changing attributes bloating SCD2 |
| 5 | Type 4 + Type 1 pointer to current mini-dim row | Current-profile reporting without the fact table |
| 6 | SCD2 row + current-value columns overwritten | As-was and as-is reporting from one dimension |

**Example**

```sql
-- Dialect: ANSI SQL
-- Type 6: after a region change, refresh the current_region on all versions of the key
UPDATE dim_customer
SET current_region = 'WEST'
WHERE customer_id = 'C-77';
-- historical_region stays as it was on each version row
```

**Real-world use case**

A lender keeps a profile mini-dimension of credit score band × income band × tenure band (a few hundred rows). Every loan fact references the profile valid at origination, while the main customer dimension stays a manageable size.

**Common mistakes and red flags**

- Putting fast-changing numeric values (exact balance) into any SCD instead of a fact or snapshot.
- Claiming Type 6 means "six columns".
- Not explaining why a mini-dimension stores band combinations rather than one row per customer.

**Follow-up questions**

- Why bands instead of exact scores? — Bands keep the mini-dimension small and are what analysts actually group by.
- Is Type 6 worth the update cost? — Only when as-is reporting on history is a frequent need; otherwise join to the current row at query time.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q14" data-level="Intermediate">
<summary><span class="qb-num">Q14</span><h3 class="qb-title">Junk, degenerate and role-playing dimensions</h3><span class="qb-meta">Intermediate · Common · Data Modeling</span></summary>

**Short answer**

A junk dimension groups low-cardinality flags and indicators into one small table instead of many tiny ones. A degenerate dimension is an identifier (an order number) kept on the fact table with no dimension table of its own. A role-playing dimension is one physical dimension, usually date, used several times in a fact under different roles.

**Detailed explanation**

**Junk dimension.** Transactions often carry yes/no flags and small codes: `is_gift`, `payment_type`, `is_expedited`. Adding each as a fact column clutters the fact table. Creating a dimension for each is overkill. A junk dimension holds every valid combination (2 × 4 × 2 = 16 rows here), and the fact stores one `order_flags_key`.

**Degenerate dimension.** Order number, invoice number or ticket ID identify a group of fact rows, but every attribute about them already lives in other dimensions. Keeping the number on the fact lets you group lines into orders and trace back to the source, with no join needed.

**Role-playing dimension.** An order fact may have `order_date_key`, `ship_date_key` and `delivery_date_key`, all referencing `dim_date`. You do not copy the table three times. You join it three times with aliases, or expose role-specific views (`dim_ship_date`) so BI users see clear column names.

| Concept | Lives where | Why |
|---|---|---|
| Junk | Small separate dimension | Declutter many flags |
| Degenerate | Column on the fact | Grouping/traceability ID with no attributes |
| Role-playing | One dimension, many FKs | Same entity, different meanings |

**Example**

```sql
-- Dialect: ANSI SQL
-- Role-playing date dimension joined twice with aliases
SELECT od.calendar_month AS order_month,
       sd.calendar_month AS ship_month,
       COUNT(DISTINCT f.order_number) AS orders   -- order_number is degenerate
FROM fct_order_lines f
JOIN dim_date od ON f.order_date_key = od.date_key
JOIN dim_date sd ON f.ship_date_key  = sd.date_key
GROUP BY od.calendar_month, sd.calendar_month;
```

**Real-world use case**

A food-delivery company has 12 boolean flags per order (contactless, scheduled, promo applied and so on). A junk dimension of the observed combinations, a few hundred rows, replaced 12 columns on a billion-row fact, and BI filters stayed simple.

**Common mistakes and red flags**

- Building a dimension table for order number that just repeats the ID.
- Physically duplicating the date dimension for every role.
- Generating every theoretical flag combination when only a fraction ever occur. Either approach can work, but explain the choice.

**Follow-up questions**

- How do you populate a junk dimension? — Pre-generate all combinations, or insert new combinations as they first appear during loads.
- Do degenerate dimensions break star-schema rules? — No; Kimball explicitly allows them.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q15" data-level="Intermediate">
<summary><span class="qb-num">Q15</span><h3 class="qb-title">What is a factless fact table?</h3><span class="qb-meta">Intermediate · Occasional · Data Modeling</span></summary>

**Short answer**

A factless fact table has dimension keys but no numeric measures. It records that something happened (event tracking) or that something was possible (coverage). You analyse it by counting rows.

**Detailed explanation**

Not every business process produces a number. Two common patterns:

1. **Event-tracking factless facts** record occurrences. Examples: student attendance (student, class, date), website logins, or a patient visit with no billed amount. The question is "how many?", answered with `COUNT(*)`.

2. **Coverage factless facts** record what *could* have happened, so you can find what *didn't*. Example: a promotion coverage table listing every product × store × day under promotion. Comparing it with the sales fact shows promoted products that sold nothing, which you cannot see from sales data alone, because missing sales leave no rows.

Some designers add a constant `event_count = 1` column so BI tools have something to sum. That is a convenience; the table is still factless in spirit.

**Design notes**
- Declare the grain as carefully as for any fact ("one row per student per class session attended"), and test the key combination for uniqueness.
- Coverage tables can be generated from planning data (promotion calendars, class schedules) rather than from transactions, which is exactly why they reveal non-events.
- Keep coverage tables scoped. Generating every product × store × day combination "just in case" creates billions of rows nobody queries.

**Example**

```sql
-- Dialect: ANSI SQL
-- Promoted products that had zero sales: coverage fact anti-joined to sales fact
SELECT pc.date_key, pc.store_key, pc.product_key
FROM fct_promo_coverage pc
LEFT JOIN fct_sales s
       ON s.date_key    = pc.date_key
      AND s.store_key   = pc.store_key
      AND s.product_key = pc.product_key
WHERE s.product_key IS NULL;
```

**Real-world use case**

A university uses an enrolment factless fact (student × course × term) to count enrolments by department. It also uses a course-offering coverage fact to find courses that were offered but had no enrolments.

**Common mistakes and red flags**

- Saying "a fact table without facts is a dimension".
- Trying to find non-events (zero sales) from the sales fact alone.
- Not knowing the coverage pattern.

**Follow-up questions**

- How big can coverage tables get? — Very large (products × stores × days), so restrict them to the relevant scope, for example only promoted items.
- How do you measure attendance rate? — Divide attendance-event counts by a coverage count of scheduled sessions.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q16" data-level="Advanced">
<summary><span class="qb-num">Q16</span><h3 class="qb-title">How do you model many-to-many relationships with bridge tables?</h3><span class="qb-meta">Advanced · Occasional · Data Modeling</span></summary>

**Short answer**

When one fact row relates to several values of a dimension (a bank account with multiple holders, a patient visit with several diagnoses), you insert a bridge table between the fact and the dimension. The fact points to a group key, and the bridge maps that group to each member, often with a weighting factor so totals are not overcounted.

**Detailed explanation**

A star schema assumes each fact row has exactly one value per dimension. Many-to-many breaks that. The options are:

1. **Change the grain** so the fact is per member (one row per account per holder). This duplicates measures and needs allocation.
2. **Pick a primary member** (the first diagnosis only). Simple, but it loses information.
3. **Use a bridge table**, the general solution.

**Bridge design:**
- The fact carries `diagnosis_group_key`.
- `bridge_diagnosis_group (diagnosis_group_key, diagnosis_key, weighting_factor)` has one row per group member.
- `dim_diagnosis` is unchanged.

**Weighting factor:** for a group of three diagnoses, each gets 0.333 (or a business-defined split). Multiplying measures by the factor gives **allocated** totals that still add up to the fact total. Leaving it out gives **impact** totals ("how much cost involved diabetes?"), which overcount on purpose. Both are valid, but the report must say which one it shows.

Bridges can also carry effective dates when group membership changes over time, such as account holders added or removed.

**Example**

```sql
-- Dialect: ANSI SQL
-- Allocated cost by diagnosis: totals reconcile to the fact table
SELECT d.diagnosis_name,
       SUM(f.visit_cost * b.weighting_factor) AS allocated_cost
FROM fct_patient_visit f
JOIN bridge_diagnosis_group b ON f.diagnosis_group_key = b.diagnosis_group_key
JOIN dim_diagnosis d          ON b.diagnosis_key       = d.diagnosis_key
GROUP BY d.diagnosis_name;
```
Sample: a visit costing 300 with diagnoses A, B and C (weights of 1/3 each) contributes 100 to each. The allocated total is 300. The unweighted "impact" total would be 900.

**Real-world use case**

A bank's joint accounts have two or three holders. A bridge from `account_holder_group_key` to `dim_customer` lets relationship managers see balances per customer, both allocated and as total exposure.

**Common mistakes and red flags**

- Joining through the bridge and summing without the weighting factor, then calling the result revenue.
- Weighting factors per group that don't sum to 1.
- Not telling report users whether figures are allocated or impact totals.

**Follow-up questions**

- How do you test a bridge? — Check that weighting factors sum to 1 per group and that allocated totals equal fact totals.
- Can the group be reused? — Yes; identical member sets share one group key, which keeps the bridge small.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q17" data-level="Beginner">
<summary><span class="qb-num">Q17</span><h3 class="qb-title">Normalisation (1NF–3NF), and when to denormalise</h3><span class="qb-meta">Beginner · Very Common · Data Modeling</span></summary>

**Short answer**

Normalisation organises tables so each fact is stored once. 1NF means atomic values and no repeating groups. 2NF removes columns that depend on only part of a composite key. 3NF removes columns that depend on other non-key columns. You denormalise for read performance and simplicity in analytics, accepting some redundancy.

**Detailed explanation**

Normalisation prevents **update anomalies**: changing a fact in one place but not another, being unable to insert data without unrelated data, or losing information when a row is deleted.

- **1NF:** every column holds one atomic value, and there are no repeating column groups (`phone1`, `phone2`, `phone3`). Each row is uniquely identifiable.
- **2NF:** 1NF, plus every non-key column depends on the *whole* primary key. In `order_lines(order_id, product_id, quantity, product_name)`, `product_name` depends only on `product_id`, so it moves to a `products` table.
- **3NF:** 2NF, plus no **transitive** dependencies: non-key columns must not depend on other non-key columns. In `customers(customer_id, zip_code, city)`, city depends on zip code, so it moves to a `zip_codes` table.
- **BCNF** tightens 3NF for rare cases with overlapping candidate keys. It is worth naming, but seldom tested in depth.

**When to denormalise**
- **Analytical workloads:** star schemas deliberately flatten dimensions, so scans need fewer joins (see [Q5](/interview/data-engineering/#q5)).
- **Read-heavy serving tables:** API or dashboard tables that must answer in milliseconds.
- **Columnar engines:** redundancy compresses well, so the storage cost is small.
- **Pre-joined wide tables** for machine-learning features or the One Big Table pattern (see [Q19](/interview/data-engineering/#q19)).

The trade-off: denormalised tables are faster and simpler to query, but harder to keep consistent. Updates must touch many rows, and the pipeline becomes responsible for consistency instead of the schema.

**Example**

```sql
-- Dialect: ANSI SQL
-- 3NF design for an orders system
CREATE TABLE customers   (customer_id INT PRIMARY KEY, customer_name VARCHAR(100), zip_code VARCHAR(10));
CREATE TABLE zip_codes   (zip_code VARCHAR(10) PRIMARY KEY, city VARCHAR(60), state VARCHAR(30));
CREATE TABLE products    (product_id INT PRIMARY KEY, product_name VARCHAR(100), list_price DECIMAL(10,2));
CREATE TABLE orders      (order_id INT PRIMARY KEY, customer_id INT REFERENCES customers, order_ts TIMESTAMP);
CREATE TABLE order_lines (order_id INT REFERENCES orders, product_id INT REFERENCES products,
                          quantity INT, unit_price DECIMAL(10,2),
                          PRIMARY KEY (order_id, product_id));
```
`unit_price` on `order_lines` is not redundant with `list_price`. It records the price actually charged, which is a different fact.

**Real-world use case**

The application database stays in 3NF for transactional safety. The warehouse gold layer denormalises customer, city and state into `dim_customer`, so a revenue-by-state dashboard needs one join.

**Common mistakes and red flags**

- Saying normalisation is "bad for performance" without naming the workload.
- Confusing 2NF and 3NF.
- Treating historical prices on order lines as redundancy.

**Follow-up questions**

- Is a star schema in 3NF? — The fact table usually is; dimensions are deliberately denormalised (often 2NF).
- What anomaly does 3NF prevent? — Updating a city for one zip code but not another row with the same zip code.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q18" data-level="Advanced">
<summary><span class="qb-num">Q18</span><h3 class="qb-title">Data Vault 2.0: hubs, links and satellites</h3><span class="qb-meta">Advanced · Common · Data Modeling</span></summary>

**Short answer**

Data Vault is an integration-layer modelling method built from three table types. Hubs hold unique business keys. Links hold relationships between hubs. Satellites hold descriptive attributes with full history. It is insert-only, auditable and easy to extend with new sources, and you build dimensional marts on top of it for reporting.

**Detailed explanation**

Data Vault 2.0 targets a specific problem: integrating many changing source systems while keeping a complete, auditable history and loading in parallel.

- **Hub:** one row per unique business key (customer number, VIN). Columns: hash key, business key, load date, record source. Hubs rarely change.
- **Link:** one row per unique relationship between two or more hubs (customer *placed* order). Columns: link hash key, the participating hub hash keys, load date, record source. All relationships are modelled as many-to-many, so a change in cardinality does not force a redesign.
- **Satellite:** descriptive attributes for a hub or link, versioned over time. Columns: parent hash key, load date, hash diff (a hash of the attributes, used for change detection), the attributes, record source. You usually split satellites by source system or by rate of change.

Supporting structures include **PIT (point-in-time)** tables, which pre-compute which satellite row is valid at each snapshot date, and **bridge** tables that speed up multi-hop joins.

**Why teams choose it**
- New sources add new satellites without touching existing tables.
- Hash keys allow hubs, links and satellites to load in parallel with no lookups.
- Insert-only with load metadata gives a full audit trail, which regulated industries need.

**Why teams avoid it**
- Many more tables and joins. A raw vault is not query-friendly, so you still need a presentation layer.
- It takes skill and automation (templating or dbt packages) to build consistently.

**Example**

```sql
-- Dialect: ANSI SQL
CREATE TABLE hub_customer (
    customer_hk    CHAR(32)     PRIMARY KEY,  -- MD5 of business key
    customer_bk    VARCHAR(50)  NOT NULL,
    load_ts        TIMESTAMP    NOT NULL,
    record_source  VARCHAR(50)  NOT NULL
);
CREATE TABLE sat_customer_crm (
    customer_hk    CHAR(32)     NOT NULL,
    load_ts        TIMESTAMP    NOT NULL,
    hash_diff      CHAR(32)     NOT NULL,     -- MD5 of all attributes
    full_name      VARCHAR(100),
    email          VARCHAR(150),
    record_source  VARCHAR(50)  NOT NULL,
    PRIMARY KEY (customer_hk, load_ts)
);
```

**Real-world use case**

A bank integrating five core-banking systems after acquisitions models customers in one hub, with one satellite per source system. Regulators can trace any reported value back to the source row and load time.

**Common mistakes and red flags**

- Pointing BI tools straight at a raw vault.
- Putting descriptive attributes into hubs.
- Calling Data Vault a replacement for dimensional modelling. It is an integration layer beneath it.

**Follow-up questions**

- What is a business vault? — A layer of derived, rule-applied satellites and links built on the raw vault.
- When would you not use Data Vault? — For a small team with few, stable sources, where a staging → dimensional model is simpler.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q19" data-level="Intermediate">
<summary><span class="qb-num">Q19</span><h3 class="qb-title">One Big Table (wide denormalised) vs dimensional model</h3><span class="qb-meta">Intermediate · Common · Data Modeling</span></summary>

**Short answer**

One Big Table (OBT) pre-joins facts with all their dimension attributes into a single wide table, so consumers never write joins. A dimensional model keeps facts and dimensions separate. OBT gives fast, simple querying on columnar engines. Dimensional models give reuse, consistent dimensions and cheaper history handling.

**Detailed explanation**

Columnar warehouses read only the columns a query touches and compress repeated values well, so wide tables with hundreds of columns are practical. That made OBT popular for BI extracts, ML feature tables and self-service analytics.

| Aspect | One Big Table | Dimensional model |
|---|---|---|
| Query simplicity | Highest (no joins) | Needs joins |
| Performance | Excellent on columnar engines | Very good; joins add some cost |
| Dimension change (e.g. rename a segment) | Rewrite many rows | Update one dimension row |
| Reuse across facts | Each OBT copies the attributes | Conformed dimensions shared |
| History (SCD2) | Baked in at build time | Flexible as-was / as-is |
| Storage | More (compresses well) | Less |
| Governance | Many copies of the same attribute | Single definition |

A pragmatic pattern: **keep the dimensional model as the source of truth and generate OBTs from it** for specific consumers (a dashboard, a feature store). The OBT is then a derived, disposable artefact. Business rules live in the dimensions, so consistency is preserved.

Watch the grain. An OBT is still at a single grain (for example order line). Adding customer-level aggregates like "lifetime value" is fine if they are documented as repeated per row and not meant to be summed.

**Example**

```sql
-- Dialect: Snowflake SQL
-- OBT built from the dimensional model, not from raw sources
CREATE OR REPLACE TABLE obt_order_lines AS
SELECT f.order_number, f.line_number, f.quantity, f.net_amount,
       d.calendar_date, d.fiscal_quarter,
       c.customer_id, c.segment, c.country,
       p.sku, p.brand, p.category
FROM fct_order_lines f
JOIN dim_date d     ON f.order_date_key = d.date_key
JOIN dim_customer c ON f.customer_key   = c.customer_key
JOIN dim_product p  ON f.product_key    = p.product_key;
```

**Real-world use case**

A marketing team uses a self-service BI tool with weak join support. Data engineering publishes a nightly `obt_campaign_performance`, derived from conformed dimensions, so the team can filter and pivot freely without misjoining tables.

**Common mistakes and red flags**

- Building OBTs directly from raw data with business logic duplicated in each one.
- Summing repeated customer-level metrics in an order-line OBT.
- Claiming OBT eliminates the need for modelling.

**Follow-up questions**

- How do you keep OBTs in sync with dimension changes? — Rebuild them from the dimensional layer on each run, or incrementally on changed keys.
- Is OBT good for ML? — Yes, as a feature table, but point-in-time correctness matters (see [Q437](/interview/scenario-based/#q437)).

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q20" data-level="Beginner">
<summary><span class="qb-num">Q20</span><h3 class="qb-title">Conceptual, logical and physical data models</h3><span class="qb-meta">Beginner · Common · Data Modeling</span></summary>

**Short answer**

A conceptual model names the main business entities and how they relate, without technical detail. A logical model adds attributes, keys and relationships independent of any database. A physical model is the actual implementation: tables, data types, partitioning, clustering and constraints for a specific platform.

**Detailed explanation**

The three levels move from "what the business means" to "how the database stores it".

- **Conceptual:** for business stakeholders. "A Customer places Orders. An Order contains Products." Boxes and lines, no columns. It answers scope and vocabulary questions: is a "customer" a person or an account?
- **Logical:** for analysts and engineers. Every entity has attributes, a primary key and relationships with cardinality (one-to-many, many-to-many). It is normalised and platform-independent. Here you decide that Order Line is its own entity and that a Customer can have many Addresses.
- **Physical:** for the database. Exact table and column names, data types (`NUMBER(12,2)` vs `FLOAT`), nullability, surrogate key strategy, indexes or clustering keys, partitioning, and platform features (Snowflake clustering keys, Delta liquid clustering).

| Level | Audience | Contains | Changes when |
|---|---|---|---|
| Conceptual | Business | Entities, relationships | Business scope changes |
| Logical | Data team | Attributes, keys, cardinality | Requirements change |
| Physical | Engineers, DBAs | Tables, types, storage options | Platform or performance needs change |

In practice, modern teams often skip formal diagrams for the first two levels and capture them as documentation in dbt YAML or a catalog. Interviewers still want to hear that you think at all three levels, especially that you settle definitions before writing DDL.

**Example**

```sql
-- Dialect: Snowflake SQL
-- Physical model detail: types, nullability and a clustering choice
CREATE TABLE fct_order_lines (
    order_line_key  NUMBER(38,0)  NOT NULL,
    order_date      DATE          NOT NULL,
    customer_key    NUMBER(38,0)  NOT NULL,
    product_key     NUMBER(38,0)  NOT NULL,
    quantity        NUMBER(10,0)  NOT NULL,
    net_amount      NUMBER(12,2)  NOT NULL
)
CLUSTER BY (order_date);
```

**Real-world use case**

Before migrating a CRM, the team agrees a conceptual model with sales operations: Account, Contact, Opportunity. That surfaces that "customer" means Account to finance but Contact to marketing, before any pipeline is built.

**Common mistakes and red flags**

- Jumping to DDL without agreeing entity definitions.
- Putting platform-specific details in a logical model.
- Not knowing what cardinality means.

**Follow-up questions**

- Which tools do you use? — ER diagram tools for design, and dbt docs or a catalog to keep the model documented next to the code.
- How do you keep models in sync with reality? — Generate documentation from the code and enforce contracts (see [Q326](/interview/dbt/#q326)).

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q21" data-level="Advanced">
<summary><span class="qb-num">Q21</span><h3 class="qb-title">Walk me through modelling a new business domain from raw requirements</h3><span class="qb-meta">Advanced · Common · Data Modeling</span></summary>

**Short answer**

Start from the business questions and the processes that generate the data, not from the source tables. Declare the grain for each process. Identify conformed dimensions and measures. Profile the sources against that design. Then build the model in layers, with tests and documentation, and iterate with stakeholders.

**Detailed explanation**

A structured approach I use:

1. **Gather questions and decisions.** Ask stakeholders which decisions they make and which metrics drive them. "Which campaigns produce customers who stay longer than six months?" tells you far more than "we need a marketing table".
2. **Map business processes.** Each process (lead created, campaign sent, order placed, subscription cancelled) usually becomes a fact table.
3. **Declare the grain** for each fact in plain language (see [Q9](/interview/data-engineering/#q9)).
4. **Identify dimensions and conform them.** Draft a bus matrix (see [Q8](/interview/data-engineering/#q8)). Decide SCD behaviour per attribute (see [Q11](/interview/data-engineering/#q11)).
5. **Define measures and metrics precisely.** Write down exact formulas, filters and edge cases: is a refunded order revenue? Note additivity (see [Q7](/interview/data-engineering/#q7)).
6. **Profile the sources.** Check row counts, key uniqueness, null rates, value distributions and how history is represented. Profiling often changes the design: for example, the source overwrites status, so you need CDC or snapshots.
7. **Design the layers.** Raw/bronze lands the data as-is. Staging/silver cleans and conforms it. Marts/gold apply the dimensional model.
8. **Build iteratively.** Deliver one process end to end first, with tests on grain, keys and relationships, then expand.
9. **Validate with stakeholders.** Reconcile key numbers against the systems they trust today. Document known differences.
10. **Plan for change.** Owners, contracts with upstream teams, and how new attributes will be added.

Interviewers care less about the exact steps than about hearing **requirements before tables**, **grain before columns** and **reconciliation before launch**.

**Example**

```yaml
# Lightweight design artefact captured alongside the code (dbt-style YAML)
models:
  - name: fct_subscription_events
    description: "One row per subscription status change (created, upgraded, downgraded, cancelled)."
    columns:
      - name: subscription_event_id
        tests: [unique, not_null]
      - name: customer_key
        tests:
          - relationships:
              to: ref('dim_customer')
              field: customer_key
```

**Real-world use case**

For a new customer-success domain, I ran a one-hour workshop that produced 12 business questions. They mapped to three facts (tickets, health scores, renewals) and four conformed dimensions. We shipped tickets first in two weeks and reconciled ticket counts with the support tool before adding the rest.

**Common mistakes and red flags**

- Starting with "I'd look at the source tables and copy them".
- Not defining metrics precisely, so the dashboard and finance disagree later.
- Skipping reconciliation against a trusted source.

**Follow-up questions**

- What if stakeholders can't articulate requirements? — Start from existing reports and spreadsheets they use, and model the processes behind them.
- How do you handle conflicting definitions between teams? — Escalate to a data owner, document both if needed, and name them differently (`gross_revenue` vs `net_revenue`).

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q22" data-level="Intermediate">
<summary><span class="qb-num">Q22</span><h3 class="qb-title">How do you model event or clickstream data?</h3><span class="qb-meta">Intermediate · Common · Data Modeling</span></summary>

**Short answer**

Land raw events append-only with a stable event ID, event time, user or session identifiers, event name and a semi-structured payload. Then build typed, deduplicated event tables per important event type, plus derived models such as sessions and funnels. Partition or cluster by event date, and keep the raw layer so you can reprocess.

**Detailed explanation**

Event data is high-volume, append-only, often late or duplicated, and its schema evolves as product teams add properties.

**Raw layer (bronze)**
- Store each event as received: `event_id`, `event_name`, `event_ts` (client time), `received_ts` (server or ingestion time), `user_id`, `anonymous_id`, `session_id`, `properties` (JSON/VARIANT), `context` (device, app version).
- Never mutate it. It is your replay source.

**Clean layer (silver)**
- **Deduplicate** on `event_id`. Clients retry, so duplicates are normal (see [Q115](/interview/sql/#q115)).
- **Type and flatten** the important properties into columns, for example `order_id` and `amount` for `checkout_completed`. Keep less important properties in the semi-structured column.
- **Identity stitching:** map `anonymous_id` to `user_id` after login, usually through an identity mapping table.
- Split very different event types into their own tables, or use one narrow event table with a typed column per common property. Both work. Choose based on how consumers query.

**Derived layer (gold)**
- **Sessions:** group events per user with an inactivity gap (see [Q132](/interview/sql/#q132)).
- **Funnels and conversion facts**, and **daily user activity snapshots** for DAU/MAU.

**Physical design**
- Partition or cluster by event date. High-cardinality IDs are poor partition keys (see [Q56](/interview/data-engineering/#q56)).
- Use event time for business logic and ingestion time for incremental loading (see [Q29](/interview/data-engineering/#q29)).
- Govern schema with a tracking plan or data contract (see [Q38](/interview/data-engineering/#q38)).

**Example**

```sql
-- Dialect: Snowflake SQL
-- Silver: dedupe and type the checkout events from the raw VARIANT payload
CREATE OR REPLACE TABLE slv_checkout_completed AS
SELECT event_id,
       user_id,
       event_ts,
       properties:order_id::STRING        AS order_id,
       properties:amount::NUMBER(12,2)    AS amount,
       properties:currency::STRING        AS currency
FROM raw_events
WHERE event_name = 'checkout_completed'
QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY received_ts) = 1;
```

**Real-world use case**

A mobile app sends 300 event types. The team types the 25 that drive KPIs into silver tables and keeps the rest queryable through the raw VARIANT column. That kept pipeline maintenance small while still supporting ad-hoc analysis.

**Common mistakes and red flags**

- Partitioning by `user_id`.
- Using ingestion time for business metrics such as daily revenue.
- No deduplication, assuming the SDK sends each event exactly once.

**Follow-up questions**

- How do you handle a renamed property? — Map old and new names in silver with `COALESCE`, and enforce the tracking plan going forward.
- How do you deal with client clock skew? — Keep both client and server timestamps, and use server time for ordering when client time is clearly invalid.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q23" data-level="Beginner">
<summary><span class="qb-num">Q23</span><h3 class="qb-title">ETL vs ELT</h3><span class="qb-meta">Beginner · Very Common · ETL vs ELT</span></summary>

**Short answer**

In ETL, data is extracted, transformed on a separate processing engine, then loaded into the target already shaped. In ELT, raw data is loaded into the warehouse or lakehouse first and transformed there with its own compute, usually in SQL. Cloud warehouses made ELT the default because storage is cheap and compute scales on demand.

**Detailed explanation**

**ETL (Extract → Transform → Load)** grew up when warehouse compute was expensive and fixed. Tools like Informatica, SSIS or DataStage pulled data, transformed it on their own servers, and loaded only the cleaned result. Benefits: the warehouse stays small, and sensitive fields can be removed before they ever land. Drawbacks: raw data is often discarded, so reprocessing means re-extracting. Transformations live in a proprietary tool and are harder to version and test.

**ELT (Extract → Load → Transform)** lands raw data first (often with tools like Fivetran, Airbyte or native connectors), then transforms it inside the platform with SQL, frequently managed by dbt. Benefits: raw history is kept, so logic can be rerun. Transformations are code in Git with tests. The warehouse's elastic compute does the heavy lifting. Drawbacks: raw sensitive data lands in the platform and needs governance (masking, access control). Messy raw data can tempt teams to skip modelling.

| Aspect | ETL | ELT |
|---|---|---|
| Where transforms run | Separate engine | Target platform |
| Raw data kept | Often not | Yes |
| Typical tools | Informatica, SSIS, Talend, Spark jobs | Fivetran/Airbyte + dbt, Snowflake, Databricks SQL |
| Reprocessing | Re-extract from source | Rerun transforms on raw |
| PII handling | Strip before load | Govern after load |
| Best fit | Strict pre-load compliance, constrained targets | Cloud warehouses and lakehouses |

Real pipelines are often hybrids. Light transformation happens in flight, such as PII hashing or format conversion in Spark, and the business modelling happens in-warehouse. Streaming pipelines also transform before landing.

**Real-world use case**

A healthcare company hashes patient identifiers in a Spark job before landing data (the ETL part, for compliance). All downstream modelling then runs as ELT in the warehouse with dbt.

**Common mistakes and red flags**

- Saying ELT means "no transformation".
- Claiming ETL is obsolete. It still fits compliance-driven or edge cases.
- Ignoring the governance burden of landing raw PII under ELT.

**Follow-up questions**

- What is EtLT? — Extract, a light transform (cleansing, PII masking), load, then the main transform in the warehouse.
- Where does Spark fit? — It can do either: transform before loading to a warehouse, or act as the in-platform engine on a lakehouse.

**In depth:** [ETL vs ELT: what factors decide the choice?](/interview/data-engineering/etl-vs-elt-factors/)

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q24" data-level="Beginner">
<summary><span class="qb-num">Q24</span><h3 class="qb-title">What are the components of a data pipeline?</h3><span class="qb-meta">Beginner · Very Common · ETL vs ELT</span></summary>

**Short answer**

A pipeline has sources, ingestion, storage, transformation, serving and the operational layer around them. The operational layer covers orchestration, monitoring, data quality, metadata and lineage, and security. A good answer covers both the data path and the operational path.

**Detailed explanation**

**Data path**
1. **Sources:** OLTP databases, SaaS APIs, files, event streams, IoT devices.
2. **Ingestion:** batch extracts, CDC (see [Q49](/interview/data-engineering/#q49)), streaming consumers, managed connectors. Here you decide full vs incremental load (see [Q25](/interview/data-engineering/#q25)).
3. **Storage:** raw landing zone (object storage or warehouse raw schema), then curated layers. Choices include file and table formats (see [Q52](/interview/data-engineering/#q52)–[Q55](/interview/data-engineering/#q55)) and partitioning.
4. **Transformation:** cleaning, deduplication, conforming, modelling and aggregation, in SQL, Spark or Python.
5. **Serving:** warehouse marts for BI, feature tables for ML, APIs or reverse ETL into operational tools (see [Q97](/interview/data-engineering/#q97)).

**Operational path**
- **Orchestration:** scheduling, dependencies, retries, backfills (Airflow, Lakeflow Jobs, dbt jobs). See [Q40](/interview/data-engineering/#q40).
- **Data quality:** checks on freshness, volume, schema, uniqueness and business rules. See [Q36](/interview/data-engineering/#q36).
- **Observability and alerting:** run status, duration trends, data anomalies, on-call routing.
- **Metadata, catalog and lineage:** so people can find data and understand impact. See [Q84](/interview/data-engineering/#q84).
- **Security and governance:** access control, masking, encryption, audit. See [Q80](/interview/data-engineering/#q80)–[Q83](/interview/data-engineering/#q83).
- **CI/CD and environments:** tests, code review and safe deployment. See [Q87](/interview/data-engineering/#q87).
- **Cost management:** tagging, right-sizing, monitoring. See [Q93](/interview/data-engineering/#q93).

**Example**

```text
[Postgres] --CDC--> [Kafka] --> [Object storage: bronze] --Spark/dbt--> [silver] --> [gold marts] --> [BI / ML / Reverse ETL]
                         |                                                  |
                   [Orchestrator: schedules, retries]        [DQ checks, lineage, alerts, access control]
```

**Real-world use case**

A fintech's payments pipeline uses CDC from the ledger database into Kafka, lands it in Delta bronze tables, transforms it in Spark to silver, and builds dbt gold models for finance. Airflow orchestrates the batch steps, and data quality checks block publishing if totals don't reconcile with the ledger.

**Common mistakes and red flags**

- Listing only "extract, transform, load" and nothing operational.
- Not mentioning how failures are detected and recovered.
- Forgetting the consumers and their SLAs.

**Follow-up questions**

- Which component fails most often in your experience? — Usually ingestion, because of upstream changes, credentials and API limits, which is why contracts and alerts matter.
- How do you decide the SLA? — Work back from when consumers need the data, then budget time per stage with buffer for retries.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q25" data-level="Beginner">
<summary><span class="qb-num">Q25</span><h3 class="qb-title">Full load vs incremental load</h3><span class="qb-meta">Beginner · Very Common · ETL vs ELT</span></summary>

**Short answer**

A full load reads and replaces the entire dataset every run. An incremental load processes only the new or changed records since the last run. Full loads are simple and self-healing but get expensive as data grows. Incremental loads are efficient but need reliable change detection and careful handling of updates, deletes and late data.

**Detailed explanation**

**Full load (truncate-and-reload or create-or-replace)**
- Simple logic, and every run produces a complete, consistent copy.
- Self-healing: a bug fixed today corrects all history on the next run.
- Cost and time grow with total volume, not change volume.
- Good for small tables (dimensions under a few million rows), reference data, and sources with no change tracking.

**Incremental load**
- Picks up changes using a **high-watermark** column such as `updated_at` (see [Q26](/interview/data-engineering/#q26)), a CDC log (see [Q49](/interview/data-engineering/#q49)), file arrival, or a source-provided change feed.
- Writes with **append** (immutable events) or **MERGE/upsert** (mutable entities).
- Much cheaper at scale, and often the only way to meet tight SLAs on large tables.
- Risks: missed changes if the watermark is unreliable, deletes not captured, late-arriving records skipped, and drift from the source over time.

A common compromise: **incremental daily, with a periodic full refresh** (weekly or monthly) or a reconciliation job comparing counts and checksums against the source. Another is a **lookback window**: reprocess the last N days on each run to catch late updates (see [Q342](/interview/dbt/#q342)).

| Aspect | Full load | Incremental load |
|---|---|---|
| Logic complexity | Low | Medium–high |
| Cost at scale | High | Low |
| Captures deletes | Yes (implicitly) | Only with CDC or soft-delete flags |
| Recovers from bugs | Next run | Needs backfill |
| Typical use | Small/reference tables | Large facts, events, CDC |

**Example**

```sql
-- Dialect: Snowflake SQL
-- Incremental upsert of changed customers since the last watermark
MERGE INTO dim_customer_current t
USING (
    SELECT * FROM stg_customers
    WHERE updated_at > (SELECT COALESCE(MAX(updated_at), '1900-01-01'::TIMESTAMP_NTZ)
                        FROM dim_customer_current)
) s
ON t.customer_id = s.customer_id
WHEN MATCHED THEN UPDATE SET t.customer_name = s.customer_name,
                             t.email         = s.email,
                             t.updated_at    = s.updated_at
WHEN NOT MATCHED THEN INSERT (customer_id, customer_name, email, updated_at)
                      VALUES (s.customer_id, s.customer_name, s.email, s.updated_at);
```

**Real-world use case**

A 3-billion-row orders table loads incrementally every hour through CDC. A 20,000-row product catalog is fully reloaded nightly because it is cheap and guarantees deletes are reflected.

**Common mistakes and red flags**

- Making everything incremental, even tiny tables.
- Not explaining how deletes reach the target.
- Using `>=` vs `>` on the watermark without thinking about duplicates or misses at the boundary.

**Follow-up questions**

- How do you capture hard deletes incrementally? — Log-based CDC, source soft-delete flags, or a periodic key comparison against the source.
- When would you switch an incremental table back to full? — When it is small enough, or when correctness issues outweigh the cost.

**In depth:** [How would you design an incremental load for a large table?](/interview/data-engineering/design-incremental-load/)

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q26" data-level="Intermediate">
<summary><span class="qb-num">Q26</span><h3 class="qb-title">High-watermark incremental loading and its pitfalls</h3><span class="qb-meta">Intermediate · Very Common · ETL vs ELT</span></summary>

**Short answer**

A high-watermark load stores the maximum value of a monotonically increasing column, such as `updated_at` or an ID, from the last successful run, and next time extracts only rows above it. It is simple, but it misses rows when the column isn't truly monotonic. Typical causes: long-running transactions, clock skew, late commits, or updates that don't touch the column.

**Detailed explanation**

**How it works**
1. Read the stored watermark, for example from a control table.
2. Extract `WHERE updated_at > :last_watermark` (and optionally `<= :current_upper_bound`).
3. Load the batch into the target.
4. Only after a successful load, save the new watermark as the max value actually extracted.

**Pitfalls and fixes**
- **Late commits:** a transaction stamps `updated_at = 10:00:00` but commits at `10:05`. If your extract ran at `10:02` and saved `10:01` as the watermark, that row is skipped forever. *Fix:* subtract a safety overlap (re-read the last few minutes) and dedupe on load, or switch to log-based CDC.
- **Columns not updated by every change:** some application paths or bulk scripts update rows without touching `updated_at`. *Fix:* database triggers, CDC, or periodic reconciliation.
- **Deletes:** a deleted row has no `updated_at` to see. *Fix:* soft deletes or CDC.
- **Clock skew and time zones:** mixed server clocks or naive timestamps break ordering. *Fix:* use database-generated UTC timestamps.
- **Equal timestamps at the boundary:** with `>`, rows sharing the boundary timestamp that commit after the extract are lost. With `>=` you get duplicates. *Fix:* use `>=` plus idempotent MERGE or dedupe.
- **Saving the watermark before the load succeeds:** a failure then skips data. *Fix:* commit the watermark in the same transaction as the load, or only after it.
- **Using `now()` as the new watermark** instead of the max extracted value, which skips rows written during extraction.

**Example**

```sql
-- Dialect: Snowflake SQL
-- Extract with a 10-minute overlap; the target MERGE is idempotent so re-reads are safe
SET last_wm = (SELECT last_watermark FROM etl_control WHERE table_name = 'orders');

CREATE OR REPLACE TEMPORARY TABLE orders_batch AS
SELECT *
FROM src_orders
WHERE updated_at >= DATEADD(minute, -10, $last_wm::TIMESTAMP_NTZ);

-- ... MERGE orders_batch into target on order_id ...

UPDATE etl_control
SET last_watermark = (SELECT MAX(updated_at) FROM orders_batch)
WHERE table_name = 'orders'
  AND EXISTS (SELECT 1 FROM orders_batch);
```

**Real-world use case**

A team noticed about 0.1% of orders missing in the warehouse. The cause was long-running checkout transactions committing minutes after their `updated_at`. A 15-minute overlap with an idempotent MERGE closed the gap. The longer-term fix was moving to log-based CDC.

**Common mistakes and red flags**

- Assuming `updated_at` is always reliable.
- Updating the watermark before confirming the load.
- No reconciliation to detect silent misses (see [Q426](/interview/scenario-based/#q426)).

**Follow-up questions**

- Can an auto-increment ID be a watermark? — For insert-only tables, yes, but IDs can commit out of order too, and it never captures updates.
- How do you detect missed rows? — Daily count and checksum comparisons between source and target per partition.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q27" data-level="Beginner">
<summary><span class="qb-num">Q27</span><h3 class="qb-title">Batch vs stream processing</h3><span class="qb-meta">Beginner · Very Common · Batch vs Streaming</span></summary>

**Short answer**

Batch processing handles bounded chunks of data on a schedule, such as hourly or nightly. Stream processing handles unbounded data continuously as events arrive, with latency from seconds down to milliseconds. Batch is simpler and cheaper. Streaming is worth it when the business acts on data within minutes.

**Detailed explanation**

| Aspect | Batch | Streaming |
|---|---|---|
| Data | Bounded (a day, a file set) | Unbounded (continuous events) |
| Latency | Minutes to hours | Sub-second to minutes |
| Complexity | Lower: reruns, backfills are straightforward | Higher: state, ordering, late data, exactly-once |
| Cost | Pay per run; idle in between | Always-on compute (or frequent micro-batches) |
| Failure recovery | Rerun the batch | Restore from checkpoint and replay offsets |
| Tools | Spark, dbt, SQL, Airflow | Kafka, Flink, Spark Structured Streaming, Kinesis |

**Choose streaming when** the value of data decays quickly: fraud detection, real-time pricing, operational alerting, user-facing personalisation, IoT monitoring.

**Choose batch when** decisions are made daily or weekly, sources only deliver in batches anyway, or complex historical joins and reprocessing dominate.

The line is blurring:
- **Micro-batching** (Spark Structured Streaming default) processes small batches every few seconds.
- **Incremental batch** (run every 5 minutes, process only new data) often gives "near real-time" at batch simplicity. Spark's `availableNow` trigger and warehouse features like Snowflake Dynamic Tables or Lakeflow pipelines fit here.
- **Unified engines** let the same code run in batch or streaming mode.

A senior answer asks *"what latency does the business actually need, and what is it worth?"* before choosing. "Real-time" requests often turn out to mean "every 15 minutes".

**Real-world use case**

A card issuer scores transactions for fraud in a streaming pipeline within 200 ms. The same events are aggregated nightly in batch for regulatory reporting and model retraining.

**Common mistakes and red flags**

- Choosing streaming because it sounds modern.
- Ignoring the operational cost of always-on streaming jobs.
- Not knowing that micro-batch is still streaming semantics.

**Follow-up questions**

- How would you convert a nightly batch to near real-time? — Make it incremental, then schedule it more frequently or run it as a streaming job with an incremental trigger.
- What is the hardest part of streaming? — Correctness under late, duplicated and out-of-order data while keeping state bounded (see [Q290](/interview/pyspark/#q290)).

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q28" data-level="Advanced">
<summary><span class="qb-num">Q28</span><h3 class="qb-title">Lambda vs Kappa architecture</h3><span class="qb-meta">Advanced · Common · Batch vs Streaming</span></summary>

**Short answer**

Lambda runs two parallel paths: a batch layer that recomputes accurate results from all historical data, and a speed layer that serves fast approximate results. A serving layer merges them. Kappa uses one streaming path for everything and reprocesses history by replaying the log, so there is one codebase instead of two.

**Detailed explanation**

**Lambda architecture**
- *Batch layer:* stores the immutable master dataset and periodically recomputes views (for example nightly Spark jobs).
- *Speed layer:* processes recent events in real time to fill the gap since the last batch.
- *Serving layer:* answers queries by combining batch views and real-time views.
- **Pros:** the batch path corrects any streaming inaccuracies, and historical recomputation is easy.
- **Cons:** the same logic is implemented twice (batch and streaming), often in different frameworks, and the two drift apart. Operationally heavy.

**Kappa architecture**
- One streaming pipeline consumes from a durable, replayable log (Kafka with long retention, or a table format used as a stream).
- To fix a bug or change logic, you deploy a new version of the job that replays from the beginning or from a chosen offset into a new output, then switch consumers over.
- **Pros:** one codebase and consistent semantics.
- **Cons:** requires long log retention or tiered storage, replay of large histories can be slow, and some heavy historical computations are awkward in pure streaming.

| Aspect | Lambda | Kappa |
|---|---|---|
| Code paths | Two (batch + speed) | One (stream) |
| Reprocessing | Batch recompute | Replay the log |
| Consistency risk | Logic drift between paths | Lower |
| Storage need | Master dataset | Long-retention log |

**Today's practice** is mostly a "lakehouse-flavoured Kappa". Streams land into table formats (Delta, Iceberg). The same engine (Spark, Flink) processes them incrementally, and reprocessing reads the table history. Pure Lambda with separate frameworks is now rare in new designs.

**Real-world use case**

An ad-tech company retired its Lambda setup, which ran separate Storm and Hadoop jobs for click counts. It moved to Kafka with Spark Structured Streaming writing Delta tables, and backfills became replays from Delta history using the same code.

**Common mistakes and red flags**

- Saying Kappa "has no batch". It can still run bounded replays.
- Not mentioning the duplicate-logic problem of Lambda.
- Ignoring log retention cost in Kappa.

**Follow-up questions**

- How do you switch consumers after a Kappa replay? — Write the replay to a new table or version, validate it, then repoint views or consumers atomically.
- Is the medallion architecture Lambda or Kappa? — Neither by itself; it is a layering pattern that can be fed by batch or streams.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q29" data-level="Intermediate">
<summary><span class="qb-num">Q29</span><h3 class="qb-title">Event time vs processing time vs ingestion time</h3><span class="qb-meta">Intermediate · Common · Batch vs Streaming</span></summary>

**Short answer**

Event time is when something actually happened at the source. Ingestion time is when the platform received it. Processing time is when your job processed it. Business metrics should use event time. Incremental loading and monitoring usually use ingestion or processing time.

**Detailed explanation**

The three can differ by seconds or by days. A phone that was offline on a flight uploads yesterday's events this morning.

- **Event time:** stamped by the producer. It is the truth for "how many orders happened on Monday?" It can arrive late and out of order, and client clocks can be wrong.
- **Ingestion time:** stamped by the broker or landing system (Kafka record timestamp in `LogAppendTime` mode, or a `_loaded_at` column). Roughly monotonic, so it is ideal for incremental pickup ("give me everything that landed since my last run").
- **Processing time:** the clock of the processing engine. Deterministic results are impossible with it because rerunning gives different times. It is useful only for operational metrics and timeouts.

**Why it matters**
- Windowed aggregations by processing time change when you replay or when the system lags. By event time they are reproducible.
- Event-time processing needs a rule for **how long to wait for late data**. In streaming this is the **watermark** (see [Q290](/interview/pyspark/#q290)). In batch it is a **lookback window** (see [Q342](/interview/dbt/#q342)).
- Partitioning raw data by ingestion date and modelling by event date is a common, robust combination.

| Time | Set by | Use for | Pitfall |
|---|---|---|---|
| Event time | Source | Business metrics, windows | Late/out-of-order, clock skew |
| Ingestion time | Platform | Incremental loads, landing partitions | Not the business date |
| Processing time | Engine | Ops metrics, timeouts | Non-deterministic |

**Example**

```sql
-- Dialect: Snowflake SQL
-- Incremental pickup by ingestion time; aggregate by event time with a 3-day lookback
SELECT DATE_TRUNC('day', event_ts) AS event_date,
       COUNT(*)                    AS orders
FROM raw_order_events
WHERE _loaded_at > (SELECT last_loaded_at FROM etl_control WHERE table_name = 'order_events')
   OR DATE_TRUNC('day', event_ts) >= DATEADD(day, -3, CURRENT_DATE)
GROUP BY 1;
```
In practice you recompute only the affected event dates and MERGE them into the aggregate.

**Real-world use case**

A delivery app's "orders per hour" dashboard used processing time and showed spikes every time the pipeline caught up after an outage. Switching to event time with a two-hour late-data allowance removed the false spikes.

**Common mistakes and red flags**

- Reporting business KPIs by load date.
- Assuming event time is always trustworthy, with no handling of future or very old timestamps.
- Not knowing what a watermark is.

**Follow-up questions**

- How do you handle events with timestamps in the future? — Cap or flag them, fall back to server time, and monitor the rate.
- What does Kafka store as the timestamp? — Either `CreateTime` (producer-set) or `LogAppendTime` (broker-set), depending on topic configuration.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q30" data-level="Advanced">
<summary><span class="qb-num">Q30</span><h3 class="qb-title">Tumbling, sliding and session windows</h3><span class="qb-meta">Advanced · Common · Batch vs Streaming</span></summary>

**Short answer**

Tumbling windows are fixed-size and non-overlapping, such as every 5 minutes. Sliding (hopping) windows are fixed-size but overlap, such as a 10-minute window every 1 minute, so one event belongs to several windows. Session windows are dynamic: they group events per key until a gap of inactivity closes the session.

**Detailed explanation**

Windows turn an unbounded stream into finite groups you can aggregate.

- **Tumbling:** `[00:00–00:05)`, `[00:05–00:10)` and so on. Each event belongs to exactly one window. Use for periodic counts and per-minute metrics.
- **Sliding / hopping:** window length L, slide S < L. With L = 10 min and S = 1 min, each event lands in 10 windows. Use for moving averages and "errors in the last 10 minutes, updated every minute". Some engines distinguish *hopping* (fixed slide) from *sliding* (evaluated on each event). Clarify which is meant.
- **Session:** per key, a window starts at the first event and extends while events keep arriving within the gap (for example 30 minutes). Lengths vary per user. Use for user sessions and device activity bursts.

**Operational considerations**
- All three need **event-time** semantics and a **watermark** to decide when a window is complete and its state can be dropped (see [Q290](/interview/pyspark/#q290)).
- Sliding windows multiply state and output by L/S.
- Session windows can merge when a late event bridges two sessions, and engines must handle that.

**Example**

```python
# Runtime: PySpark 3.5 (Structured Streaming)
from pyspark.sql import functions as F

# events_stream: columns user_id STRING, event_ts TIMESTAMP, page STRING
tumbling = (events_stream
    .withWatermark("event_ts", "10 minutes")
    .groupBy(F.window("event_ts", "5 minutes"), "page")
    .count())

sliding = (events_stream
    .withWatermark("event_ts", "10 minutes")
    .groupBy(F.window("event_ts", "10 minutes", "1 minute"), "page")
    .count())

sessions = (events_stream
    .withWatermark("event_ts", "30 minutes")
    .groupBy(F.session_window("event_ts", "30 minutes"), "user_id")
    .agg(F.count("*").alias("events_in_session")))
```

**Real-world use case**

A streaming platform uses tumbling 1-minute windows for concurrent-viewer counts, a sliding 15-minute window for buffering-error alerts, and 30-minute session windows to measure watch sessions per user.

**Common mistakes and red flags**

- Using sliding windows without realising each event is counted multiple times.
- No watermark, so state grows forever.
- Using processing-time windows for business metrics.

**Follow-up questions**

- How would you compute sessions in batch SQL? — Use LAG to flag gaps above the threshold, then a running sum of the flags as the session ID (see [Q132](/interview/sql/#q132)).
- Which Spark version added `session_window`? — Spark 3.2.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q31" data-level="Beginner">
<summary><span class="qb-num">Q31</span><h3 class="qb-title">Data lake vs data warehouse vs lakehouse</h3><span class="qb-meta">Beginner · Very Common · Data Lake / Lakehouse</span></summary>

**Short answer**

A data lake stores raw files of any type cheaply in object storage, with schema applied on read. A data warehouse stores structured, modelled data in a managed engine with strong SQL performance and governance. A lakehouse adds warehouse features (ACID transactions, schema enforcement, fine-grained governance, fast SQL) to open-format files in the lake.

**Detailed explanation**

**Data lake.** Object storage (S3, ADLS, GCS) holding Parquet, JSON, CSV, images and logs. It is cheap and flexible and suits data science and raw retention. Without discipline, it lacks ACID updates and consistent schemas, has weak access control, and turns into a "data swamp" (see [Q34](/interview/data-engineering/#q34)). Engines like Spark, Trino and Athena query it.

**Data warehouse.** A managed analytical database (Snowflake, BigQuery, Redshift, Synapse) with schema-on-write, strong SQL performance, concurrency and built-in security. Historically storage was proprietary and costlier, and unstructured data or ML workloads were awkward.

**Lakehouse.** Open table formats (Delta Lake, Apache Iceberg, Apache Hudi; see [Q55](/interview/data-engineering/#q55)) add a transaction log over files in the lake. That provides ACID writes, MERGE and deletes, time travel and schema enforcement. A catalog (Unity Catalog, AWS Glue, Polaris/Horizon) adds governance. Multiple engines can read the same tables.

| Aspect | Data lake | Data warehouse | Lakehouse |
|---|---|---|---|
| Storage | Open files in object storage | Managed (often proprietary) | Open files + table format |
| Schema | On read | On write | On write (enforced), evolvable |
| ACID / MERGE | No (without a table format) | Yes | Yes |
| Data types | Any | Mostly structured, semi-structured | Any |
| BI performance | Variable | High | High (with optimisation) |
| Engine lock-in | Low | Higher | Low–medium |

The lines are converging. Snowflake and BigQuery read and write Iceberg tables, and Databricks offers serverless SQL warehouses on Delta. The interview point is the trade-off between openness and flexibility on one side and managed simplicity on the other, not brand loyalty.

**Real-world use case**

A media company keeps raw video metadata and logs in a lake. It runs its curated analytics as a lakehouse on Iceberg tables, queried by Spark for ML and by a warehouse engine for BI, without copying the data.

**Common mistakes and red flags**

- Saying "lakehouse = data lake + warehouse side by side".
- Not mentioning the transaction log or table format as the enabler.
- Claiming warehouses can't handle semi-structured data.

**Follow-up questions**

- What makes ACID possible on immutable object storage? — An atomic commit to a transaction log or metadata pointer (see [Q33](/interview/data-engineering/#q33)).
- When would you still choose a pure warehouse? — A SQL-heavy team, mostly structured data, and a preference for minimal platform operations.

**In depth:** [Data lake vs warehouse vs lakehouse: when would you choose each?](/interview/data-engineering/lake-warehouse-lakehouse/)

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q32" data-level="Beginner">
<summary><span class="qb-num">Q32</span><h3 class="qb-title">What is the medallion (bronze/silver/gold) architecture?</h3><span class="qb-meta">Beginner · Very Common · Data Lake / Lakehouse</span></summary>

**Short answer**

Medallion is a layering pattern. Bronze holds raw data as ingested. Silver holds cleaned, deduplicated, conformed data. Gold holds business-level models and aggregates for consumption. Each layer improves quality and structure, and you can always rebuild downstream layers from upstream ones.

**Detailed explanation**

- **Bronze (raw):** append-only copies of source data with ingestion metadata (`_ingested_at`, source file, batch ID). Minimal transformation, perhaps parsing into a table format. Its purpose is replay, audit and decoupling from source availability.
- **Silver (clean / conformed):** typed columns, deduplication, standardised formats (dates, currencies, codes), basic quality rules, joined reference data, and often current-state or SCD2 entity tables. This is the "single version of the enterprise data", reusable by many use cases.
- **Gold (curated / business):** dimensional models, aggregates, feature tables and KPIs built for specific consumers. Business logic and metric definitions live here.

**Why it helps**
- Clear contracts per layer, so teams know where to find raw versus trusted data.
- Reprocessing is easy: fix silver logic and rebuild from bronze.
- Access control maps to layers. Few people read bronze PII. Many read gold.

**Caveats interviewers like to hear**
- It is a naming convention, not a technology. Mapping to raw/staging/marts in dbt is the same idea.
- Don't add layers just for the pattern's sake. Some flows need only bronze → gold, and complex ones need intermediate layers.
- Every layer costs storage and compute, so retention and materialisation choices matter.

**Example**

```sql
-- Dialect: Databricks SQL
-- Silver from bronze: type, clean and dedupe
CREATE OR REPLACE TABLE silver.orders AS
SELECT CAST(order_id AS BIGINT)            AS order_id,
       CAST(customer_id AS BIGINT)         AS customer_id,
       TO_TIMESTAMP(order_ts)              AS order_ts,
       UPPER(TRIM(currency))               AS currency,
       CAST(amount AS DECIMAL(12,2))       AS amount
FROM bronze.orders_raw
WHERE order_id IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _ingested_at DESC) = 1;
```

**Real-world use case**

A retailer lands POS files into bronze every 15 minutes. Silver standardises store codes and removes duplicate transmissions. Gold publishes `fct_sales` and `agg_store_daily` for finance and merchandising dashboards. See [Q236](/interview/databricks/#q236) for the Databricks implementation.

**Common mistakes and red flags**

- Applying business logic in bronze.
- Letting dashboards read silver or bronze directly.
- Treating medallion as a Databricks-only concept.

**Follow-up questions**

- Where do you handle PII? — Mask or tokenise when moving to silver, and restrict bronze access tightly.
- Do you keep bronze forever? — Keep it as long as replay and compliance need, then apply lifecycle policies (see [Q94](/interview/data-engineering/#q94)).

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q33" data-level="Advanced">
<summary><span class="qb-num">Q33</span><h3 class="qb-title">How do open table formats make ACID possible on object storage?</h3><span class="qb-meta">Advanced · Common · Data Lake / Lakehouse</span></summary>

**Short answer**

Data files in object storage are immutable. A table format adds a metadata layer (a transaction log or snapshot tree) that defines which files make up each table version. Writers create new files, then commit by atomically adding a new log entry or swapping a metadata pointer. Readers only see committed snapshots, which gives atomicity and snapshot isolation.

**Detailed explanation**

Without a table format, a "table" is just a folder of Parquet files. A failed job leaves half-written files that readers pick up. Two writers can overwrite each other. Updates mean rewriting folders.

Table formats solve this with metadata:

- **Delta Lake:** a `_delta_log/` folder of ordered JSON commit files (`000…01.json`, `000…02.json`), each listing files *added* and *removed*, plus periodic Parquet checkpoints. A commit succeeds only if the writer can create the next sequential log file. The underlying storage or a coordination service must guarantee "put if absent".
- **Apache Iceberg:** a tree of metadata. The catalog points to the current `metadata.json`, which lists snapshots. Each snapshot points to a manifest list, then manifests, then data files. A commit is an atomic compare-and-swap of the catalog pointer from the old metadata file to the new one.
- **Apache Hudi:** a timeline of instants (commit, delta commit, compaction) in a `.hoodie/` folder.

**How ACID maps**
- **Atomicity:** the commit is a single metadata operation. Uncommitted files are invisible and later cleaned up.
- **Consistency:** schema and constraints are checked at write time.
- **Isolation:** **optimistic concurrency**. Writers read a snapshot, write files, then try to commit. If another writer committed conflicting changes first, the commit fails and is retried or aborted (see [Q204](/interview/databricks/#q204)).
- **Durability:** data and metadata live in durable object storage.

The same metadata enables **time travel** (read an older snapshot), **file-level statistics** for data skipping, and **row-level MERGE, UPDATE and DELETE** through file rewrites or deletion vectors (see [Q210](/interview/databricks/#q210)).

**Example**

```text
Delta table layout
orders/
  _delta_log/
    00000000000000000000.json   -> add part-0001.parquet, part-0002.parquet
    00000000000000000001.json   -> remove part-0002.parquet, add part-0003.parquet  (an UPDATE)
    00000000000000000010.checkpoint.parquet
  part-0001.parquet
  part-0002.parquet   (no longer in current version; removed by VACUUM later)
  part-0003.parquet
```

**Real-world use case**

Before adopting Delta, a team's nightly overwrite of a Parquet folder sometimes left dashboards reading a half-written table at 2 AM. With Delta, readers keep seeing the previous version until the new commit lands.

**Common mistakes and red flags**

- Saying ACID comes from Parquet itself.
- Not knowing the commit is a metadata operation.
- Thinking table formats use locks like an OLTP database.

**Follow-up questions**

- Why does Iceberg need a catalog? — To provide the atomic pointer swap and a single source of the current metadata location.
- What does VACUUM have to do with this? — It deletes files no longer referenced by any retained version (see [Q206](/interview/databricks/#q206)).

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q34" data-level="Intermediate">
<summary><span class="qb-num">Q34</span><h3 class="qb-title">What is a data swamp, and how do you prevent one?</h3><span class="qb-meta">Intermediate · Occasional · Data Lake / Lakehouse</span></summary>

**Short answer**

A data swamp is a data lake where data has been dumped without ownership, documentation, quality controls or structure, so nobody can find or trust anything. You prevent it with a catalog and metadata, clear layering and naming, ownership, quality checks, access control and lifecycle policies.

**Detailed explanation**

Lakes make it easy to store anything, and that is the problem. Symptoms of a swamp:
- Hundreds of folders like `/data/tmp_final_v2/` with no owner.
- The same dataset copied five times with slightly different logic.
- Unknown schemas and file formats, so every consumer reverse-engineers them.
- Stale data nobody deletes, with storage cost growing every month.
- PII sitting in places nobody knows about.

**Prevention**
1. **Catalog everything.** Register every dataset as a table in a catalog (Unity Catalog, Glue, Iceberg REST catalog) instead of reading raw paths. Add descriptions, owners and tags.
2. **Standard layering and naming.** Bronze/silver/gold (see [Q32](/interview/data-engineering/#q32)) with predictable paths and names.
3. **Ownership.** Every dataset has a team and a contact. Unowned data gets flagged and eventually archived.
4. **Quality gates** on promotion between layers (see [Q36](/interview/data-engineering/#q36)).
5. **Access control and classification.** Tag PII, restrict raw zones, audit access.
6. **Lifecycle and retention.** Automatically tier or delete temp and old data (see [Q94](/interview/data-engineering/#q94)).
7. **Lineage**, so consumers can see where data came from and producers can see who uses it (see [Q84](/interview/data-engineering/#q84)).
8. **Self-service with guardrails.** Templates and platform tooling make the right way the easy way.

**Real-world use case**

A company found 40% of its S3 lake had no reads in 12 months. Tagging owners, archiving unread prefixes to cold storage and forcing new datasets through catalog registration cut storage cost and made discovery possible.

**Common mistakes and red flags**

- Blaming the technology rather than the missing process.
- Proposing a catalog without ownership or enforcement.
- Ignoring cost as a swamp symptom.

**Follow-up questions**

- How do you find unused data? — Storage access logs or catalog and query history showing tables with no reads in N days.
- How do you enforce registration? — Only let pipelines write through governed tables, and block direct writes to raw paths with IAM.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q35" data-level="Beginner">
<summary><span class="qb-num">Q35</span><h3 class="qb-title">What are the dimensions of data quality?</h3><span class="qb-meta">Beginner · Very Common · Data Quality &amp; Observability</span></summary>

**Short answer**

The commonly cited dimensions are accuracy, completeness, consistency, timeliness (freshness), validity and uniqueness. Some frameworks add integrity and reasonableness. Each maps to concrete checks: null rates, duplicate keys, referential integrity, freshness lag, allowed values and reconciliation with the source.

**Detailed explanation**

| Dimension | Question it answers | Example check |
|---|---|---|
| Accuracy | Does the data reflect reality? | Revenue reconciles with the finance ledger within 0.1% |
| Completeness | Is anything missing? | `email` not null for 99% of customers; row count vs source |
| Consistency | Do related values agree across tables/systems? | Order totals equal the sum of their lines |
| Timeliness / freshness | Is it recent enough? | Max `event_ts` within 2 hours of now |
| Validity | Does it follow the rules/format? | `country_code` in ISO list; dates parseable |
| Uniqueness | Are there duplicates? | `order_id` unique |
| Integrity | Do references resolve? | Every `customer_id` in orders exists in customers |

Two points earn credit in interviews:

1. **Quality is relative to use.** A 2% null rate in `phone_number` may be fine for analytics but unacceptable for an SMS campaign. Define thresholds with consumers and record them as SLAs or contracts (see [Q38](/interview/data-engineering/#q38)).
2. **Detect at the right place.** Validate schema and format at ingestion, keys and integrity at silver, and business reconciliation at gold. Fail fast on critical checks and warn on others.

**Example**

```sql
-- Dialect: ANSI SQL
-- A few dimension checks expressed as one profiling query
SELECT
    COUNT(*)                                                  AS row_count,
    COUNT(*) - COUNT(DISTINCT order_id)                       AS duplicate_order_ids,      -- uniqueness
    SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END)      AS null_customer_ids,         -- completeness
    SUM(CASE WHEN amount < 0 THEN 1 ELSE 0 END)               AS negative_amounts,          -- validity
    MAX(order_ts)                                             AS latest_order_ts            -- timeliness
FROM silver_orders;
```

**Real-world use case**

A subscription company's churn model degraded silently because the `cancel_reason` field went 40% null after an app release. A completeness check with an alert at 5% would have caught it the same day.

**Common mistakes and red flags**

- Listing dimensions with no example checks.
- Treating all checks as blocking.
- Ignoring freshness as a quality dimension.

**Follow-up questions**

- Which dimension is hardest to test? — Accuracy, because it needs an external source of truth or reconciliation.
- How do you set thresholds? — From historical distributions plus consumer requirements, reviewed periodically.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q36" data-level="Intermediate">
<summary><span class="qb-num">Q36</span><h3 class="qb-title">How do you build data quality checks into a pipeline?</h3><span class="qb-meta">Intermediate · Very Common · Data Quality &amp; Observability</span></summary>

**Short answer**

Put checks at each layer boundary: schema and format at ingestion, keys, nulls and referential integrity in silver, and business rules and reconciliation in gold. Classify each check as blocking or warning. Run checks before publishing to consumers, route failing rows to quarantine when appropriate, and alert the owning team with context.

**Detailed explanation**

**Where to check**
- **Ingestion / bronze:** file arrived, row count in expected range, schema matches the contract, required fields parseable.
- **Silver:** primary key uniqueness, not-null on required columns, accepted values, referential integrity, deduplication verified.
- **Gold:** business rules (revenue ≥ 0, order total = sum of lines), reconciliation against source totals, and metric anomaly detection (today vs the last 4 same weekdays).

**How to act on failures**
- **Blocking (error):** stop the pipeline before publishing. Consumers keep seeing yesterday's correct data rather than today's wrong data.
- **Warning:** publish but alert, for issues like a slight increase in nulls on a non-critical field.
- **Row-level quarantine:** send invalid rows to a quarantine table with a reason code and publish the rest (see [Q39](/interview/data-engineering/#q39)). Use this when partial data is better than none.

**Implementation options**
- dbt tests (generic, singular, unit; see [Q313](/interview/dbt/#q313)–[Q316](/interview/dbt/#q316)).
- Great Expectations, Soda or similar frameworks.
- Platform-native features: Lakeflow pipeline expectations (see [Q222](/interview/databricks/#q222)), Snowflake data metric functions.
- Custom SQL assertions in the orchestrator.

**Publishing pattern:** build into a staging table, run checks, then atomically swap or publish (Write-Audit-Publish; see [Q91](/interview/data-engineering/#q91)).

**Example**

```sql
-- Dialect: Snowflake SQL
-- Blocking check: fail the task if today's load reconciles poorly with the source
SELECT CASE
         WHEN ABS(t.total - s.total) / NULLIF(s.total, 0) > 0.001
         THEN TO_NUMBER('reconciliation_failed')   -- deliberate runtime error to fail the step
         ELSE 1
       END AS check_passed
FROM (SELECT SUM(amount) AS total FROM gold.fct_payments WHERE payment_date = CURRENT_DATE - 1) t,
     (SELECT SUM(amount) AS total FROM src_ledger_daily  WHERE ledger_date  = CURRENT_DATE - 1) s;
```
Most teams express this as a dbt test or orchestrator check rather than a forced cast error. The point is that the step fails loudly.

**Real-world use case**

A payments team added a gold reconciliation test against the ledger. In its first month it blocked two bad publishes caused by duplicated CDC events, and finance never saw wrong numbers.

**Common mistakes and red flags**

- Running tests after the data is already exposed to dashboards.
- Making every check blocking, which causes alert fatigue and constant outages.
- Alerts without an owner or runbook.

**Follow-up questions**

- How do you avoid alert fatigue? — Tier checks by criticality, deduplicate alerts, and tune thresholds from history.
- What do you do when a blocking check fails at 3 AM? — Follow the runbook: assess impact, fix or quarantine, rerun, and communicate (see [Q401](/interview/scenario-based/#q401)).

**In depth:** [What are the most important data-quality checks in production?](/interview/data-engineering/data-quality-checks/)

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q37" data-level="Intermediate">
<summary><span class="qb-num">Q37</span><h3 class="qb-title">Data observability vs data quality testing</h3><span class="qb-meta">Intermediate · Common · Data Quality &amp; Observability</span></summary>

**Short answer**

Data quality tests are explicit assertions you write in advance: this column is unique, this value is not null. Data observability is continuous monitoring of data and pipeline health signals (freshness, volume, schema changes, distributions, lineage) that catches problems you didn't think to test. You need both.

**Detailed explanation**

**Tests ("known unknowns")**
- Written per table by engineers, versioned with the code, and run as part of the pipeline.
- Deterministic pass or fail, usually blocking.
- Limited to what someone anticipated.

**Observability ("unknown unknowns")**
- Often described in five pillars: **freshness, volume, schema, distribution, lineage**.
- Uses metadata and statistical baselines, for example "this table normally gets 1.2M rows on Mondays; today 300k".
- Detects anomalies across all tables without hand-written rules, and uses lineage to show impact and root cause.
- Includes pipeline signals: run durations, failures, retries and cost trends.

| Aspect | DQ testing | Data observability |
|---|---|---|
| Defined by | Engineers, explicit rules | Baselines, anomaly detection, metadata |
| Coverage | Targeted tables/columns | Broad, across the platform |
| Output | Pass/fail, blocks pipeline | Alerts, incidents, impact analysis |
| Catches | Known failure modes | Unexpected drift |
| Tools | dbt tests, Great Expectations, Soda | Monte Carlo, Bigeye, platform-native monitors, custom metrics |

A mature setup combines them. Tests guard critical contracts. Observability watches everything else. Incidents found by observability become new tests.

**Example**

```sql
-- Dialect: Snowflake SQL
-- Simple volume anomaly: compare today's row count to the trailing 4-week same-weekday average
WITH daily AS (
    SELECT DATE(_loaded_at) AS load_date, COUNT(*) AS row_count
    FROM bronze_orders
    WHERE _loaded_at >= DATEADD(day, -35, CURRENT_DATE)
    GROUP BY 1
)
SELECT d.load_date, d.row_count,
       AVG(h.row_count) AS baseline,
       d.row_count / NULLIF(AVG(h.row_count), 0) AS ratio
FROM daily d
JOIN daily h
  ON DAYOFWEEK(h.load_date) = DAYOFWEEK(d.load_date)
 AND h.load_date < d.load_date
 AND h.load_date >= DATEADD(day, -28, d.load_date)
WHERE d.load_date = CURRENT_DATE
GROUP BY d.load_date, d.row_count;
```

**Real-world use case**

All tests passed, but an upstream change halved the volume of a marketing events table. A volume monitor flagged it within an hour, lineage showed three affected dashboards, and the team notified stakeholders before the Monday business review.

**Common mistakes and red flags**

- Thinking observability tools replace tests.
- Monitoring pipeline success only, not the data itself.
- No link from alerts to owners and downstream impact.

**Follow-up questions**

- What are the five pillars? — Freshness, volume, schema, distribution and lineage.
- How do you reduce false positives? — Seasonality-aware baselines, minimum thresholds, and feedback loops to tune monitors.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q38" data-level="Advanced">
<summary><span class="qb-num">Q38</span><h3 class="qb-title">What are data contracts, and how do you enforce them?</h3><span class="qb-meta">Advanced · Common · Data Quality &amp; Observability</span></summary>

**Short answer**

A data contract is an explicit, versioned agreement between a data producer and its consumers. It covers schema, semantics, quality guarantees, SLAs and ownership. You enforce it in code: schema validation at the producer (CI checks, schema registry), contract checks at ingestion, and a change process for breaking versions.

**Detailed explanation**

Most pipeline breakages start upstream. An application team renames a column, changes a type or repurposes a status code without knowing the warehouse depends on it. Contracts make that dependency explicit.

**Typical contents**
- **Schema:** fields, types, nullability, primary key.
- **Semantics:** what each field means, units, allowed values.
- **Quality expectations:** uniqueness, freshness, completeness thresholds.
- **SLA:** delivery time and frequency.
- **Ownership and support:** producing team, contact, escalation path.
- **Versioning policy:** what counts as breaking (removing or renaming a field, changing a type) vs non-breaking (adding an optional field).

**Enforcement points**
1. **Producer CI:** contract tests run on the producer's pull requests, so a breaking schema change fails *their* build.
2. **Schema registry** for event streams, with compatibility modes (backward, forward, full; see [Q72](/interview/data-engineering/#q72)).
3. **Ingestion validation:** reject or quarantine records that violate the contract.
4. **Warehouse-side contracts:** dbt model contracts (see [Q326](/interview/dbt/#q326)) enforce column names and types on published models.
5. **Process:** breaking changes need a new version, a deprecation window and consumer sign-off.

Contracts are as much organisational as technical. They work when producers accept ownership of the data they emit, and that usually needs leadership support.

**Example**

```yaml
# Data contract (illustrative YAML structure)
dataset: orders_events
version: 2.1.0
owner: checkout-team@company.example
sla:
  freshness_minutes: 15
schema:
  - name: order_id
    type: string
    required: true
    unique: true
  - name: amount
    type: decimal(12,2)
    required: true
    description: "Order total in minor-currency-adjusted major units, tax included"
  - name: status
    type: string
    allowed_values: [created, paid, shipped, cancelled]
breaking_change_policy: "New major version; 30-day dual publish"
```

**Real-world use case**

After repeated incidents from renamed fields, a company required contracts for all Kafka topics feeding the warehouse. The registry rejected incompatible schemas at deploy time, and incidents from upstream changes dropped sharply.

**Common mistakes and red flags**

- Treating a contract as a wiki page with no enforcement.
- Applying contracts to every internal intermediate table.
- No versioning or deprecation path.

**Follow-up questions**

- What if the producer team refuses? — Start with the highest-impact datasets, show incident cost, and get leadership to sponsor ownership.
- How are dbt contracts different? — They enforce shape on models you publish, not on upstream producers.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q39" data-level="Intermediate">
<summary><span class="qb-num">Q39</span><h3 class="qb-title">How do you handle bad records: quarantine tables and dead-letter queues</h3><span class="qb-meta">Intermediate · Common · Data Quality &amp; Observability</span></summary>

**Short answer**

Instead of failing the whole pipeline or silently dropping bad rows, route invalid records to a quarantine table (batch) or dead-letter queue (streaming), with the original payload, an error reason and metadata. The valid rows continue. Then monitor the quarantine volume and reprocess fixed records.

**Detailed explanation**

Bad records include unparseable JSON, missing required fields, invalid types, unknown reference codes and business-rule violations. You have three choices:

1. **Fail the pipeline.** Right for systemic problems (the schema changed) or critical data where partial output is worse than none.
2. **Drop silently.** Almost never acceptable, because you lose data and visibility.
3. **Quarantine / dead-letter.** The default for isolated row-level issues.

**Good quarantine design**
- Keep the **raw payload** exactly as received, so you can reprocess it.
- Add `error_code`, `error_message`, `pipeline_name`, `source_file` or `offset`, and `quarantined_at`.
- Keep a **threshold**: if quarantined rows exceed, say, 1% of the batch, fail the pipeline, because something systemic is wrong.
- **Alert** on volume and **report** per error type.
- Provide a **replay path** that reinserts corrected rows idempotently.

In streaming (Kafka), a **dead-letter topic** receives messages that fail deserialisation or processing after N retries, so a "poison pill" message doesn't block the partition. Make sure each failure type has an owner. Vendor-format issues go back to the vendor, and code bugs go to the pipeline team.

**Example**

```python
# Runtime: PySpark 3.5
from pyspark.sql import functions as F

# raw_df columns: value STRING (JSON), _source_file STRING
schema = "order_id STRING, amount DECIMAL(12,2), currency STRING"
parsed = raw_df.withColumn("data", F.from_json("value", schema))

is_valid = (F.col("data.order_id").isNotNull()
            & F.col("data.amount").isNotNull()
            & F.col("data.currency").isin("USD", "EUR", "INR"))

good = parsed.filter(is_valid).select("data.*")
bad = (parsed.filter(~is_valid)
       .select(F.col("value").alias("raw_payload"),
               "_source_file",
               F.lit("VALIDATION_FAILED").alias("error_code"),
               F.current_timestamp().alias("quarantined_at")))

good.write.mode("append").saveAsTable("silver.orders")
bad.write.mode("append").saveAsTable("quarantine.orders")
```
`from_json` returns NULL fields for malformed JSON in the default permissive mode, so those rows also fail `is_valid` and land in quarantine.

**Real-world use case**

A vendor occasionally sends rows with an unknown currency code. They go to quarantine, the vendor-management team receives a daily report, and corrected rows are replayed weekly. Meanwhile 99.9% of the data publishes on time.

**Common mistakes and red flags**

- Quarantining without ever looking at the table.
- Storing only parsed fields, not the raw payload.
- No upper threshold, so a broken feed quietly quarantines everything.

**Follow-up questions**

- How do you avoid poison-pill messages blocking Kafka consumers? — Bounded retries, then publish to a dead-letter topic and commit the offset.
- How do you reprocess quarantined rows? — Fix the cause, then replay from quarantine through the same validation path with idempotent writes.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q40" data-level="Beginner">
<summary><span class="qb-num">Q40</span><h3 class="qb-title">What is Apache Airflow? Explain DAGs, tasks and operators</h3><span class="qb-meta">Beginner · Very Common · Orchestration (Airflow)</span></summary>

**Short answer**

Airflow is an open-source workflow orchestrator in which pipelines are defined as Python code. A DAG (directed acyclic graph) is a workflow with a schedule. Tasks are its nodes. Operators are templates for what a task does, such as running SQL, a Python function or a Spark job. Airflow schedules, retries and monitors the tasks; the heavy processing usually runs elsewhere.

**Detailed explanation**

- **DAG:** a collection of tasks and their dependencies, with no cycles. It has a `dag_id`, a schedule (cron, timedelta, or data-aware via Assets), a start date and default arguments such as retries.
- **Task:** one unit of work in a DAG. Each scheduled execution creates a **task instance** with a state (queued, running, success, failed, up_for_retry, skipped).
- **Operator:** a class that defines what a task does. `BashOperator`, `PythonOperator` and the `@task` decorator (TaskFlow API) are generic. Provider packages add `SnowflakeSqlApiOperator`, `DatabricksRunNowOperator`, `KubernetesPodOperator` and others.
- **Sensor:** a special operator that waits for a condition, such as a file landing (see [Q44](/interview/data-engineering/#q44)).
- **DAG run:** one execution of the DAG for a given logical date or data interval (see [Q45](/interview/data-engineering/#q45)).

**Good practice**
- Airflow is an **orchestrator, not a processing engine**. Push heavy work to Snowflake, Databricks, Spark or Kubernetes pods.
- Keep top-level DAG code light. The scheduler parses DAG files frequently, so avoid database calls at import time.
- Make tasks **idempotent** and **atomic** (see [Q46](/interview/data-engineering/#q46)).

**Example**

```python
# Runtime: Python 3.11, Apache Airflow 2.x/3.x (TaskFlow API)
from datetime import datetime, timedelta
from airflow.decorators import dag, task

@dag(
    dag_id="daily_orders",
    schedule="0 2 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
)
def daily_orders():
    @task
    def extract(ds: str | None = None) -> str:
        # ds is the logical date string injected by Airflow, e.g. '2026-03-01'
        return f"s3://landing/orders/{ds}/"

    @task
    def load(path: str) -> None:
        print(f"Loading files from {path}")

    load(extract())

daily_orders()
```
In Airflow 3 the same decorators are also available from the `airflow.sdk` namespace (the Task SDK).

**Real-world use case**

A nightly DAG triggers an ingestion job, waits for it, runs `dbt build` on the affected models, then refreshes a BI extract. Airflow handles retries and alerts, while Snowflake does all the computation.

**Common mistakes and red flags**

- Processing large data inside the Airflow worker with pandas.
- Putting expensive code at DAG file top level.
- Calling Airflow a "streaming tool".

**Follow-up questions**

- What is the TaskFlow API? — A decorator-based way to write tasks as Python functions, with return values passed automatically through XCom.
- How do you pass data between tasks? — Pass references (paths, table names) through XCom, not the data itself (see [Q43](/interview/data-engineering/#q43)).

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q41" data-level="Intermediate">
<summary><span class="qb-num">Q41</span><h3 class="qb-title">Airflow architecture: scheduler, executor, workers, metadata DB, API server</h3><span class="qb-meta">Intermediate · Common · Orchestration (Airflow)</span></summary>

**Short answer**

The scheduler parses DAGs, decides which task instances are ready and hands them to the executor. Workers run the tasks. The metadata database stores all state (DAG runs, task instances, connections, variables). The web UI and API let users monitor and trigger runs. In Airflow 3, the API server serves the UI and API, and a separate DAG processor parses DAG files.

**Detailed explanation**

| Component | Responsibility |
|---|---|
| Scheduler | Creates DAG runs on schedule, evaluates dependencies, queues ready task instances, handles retries |
| DAG processor | Parses DAG files into serialized DAGs stored in the metadata DB (a separate component in Airflow 3; part of the scheduler by default in 2.x) |
| Executor | The scheduler's plug-in that decides *how and where* tasks run (local process, Celery queue, Kubernetes pod; see [Q42](/interview/data-engineering/#q42)) |
| Workers | Processes or pods that actually execute task code |
| Metadata database | PostgreSQL/MySQL holding state for DAG runs, task instances, XComs, connections, variables, users |
| Web server / API server | UI for monitoring, logs, manual triggers; REST API. Airflow 3 consolidates this into an API server |
| Triggerer | Runs async triggers for deferrable operators (see [Q44](/interview/data-engineering/#q44)) |

**Airflow 3 changes worth knowing** (released 2025)
- Task execution goes through the **Task Execution API / Task SDK**, so workers no longer need direct metadata-DB access. This improves security and enables remote execution.
- **DAG versioning:** the UI shows which DAG version each run used.
- "Datasets" were renamed **Assets**, and asset-driven scheduling was expanded.
- A rewritten React UI, and removal of some deprecated features (for example the `execution_date` context key in favour of `logical_date`).

**Scaling and reliability**
- Run multiple schedulers (supported since 2.0) for high availability.
- The metadata DB is a common bottleneck: keep it healthy, clean old records, and avoid heavy XComs.
- Scale workers horizontally (Celery) or per task (Kubernetes).

**Example**

```text
           +-------------+        +----------------+
DAG files->| DAG processor|------>| Metadata DB    |<------+
           +-------------+        +----------------+       |
                                     ^      ^              |
                     +---------------+      |              |
                     |                      |              |
               +-----------+   queue   +---------+   +-----------+
               | Scheduler |---------->| Executor|-->|  Workers  |
               +-----------+           +---------+   +-----------+
                                                         |
                          +--------------+   Task SDK/API |
                          | API server/UI|<---------------+
                          +--------------+
```

**Real-world use case**

A team running about 2,000 DAGs saw tasks sitting in "scheduled" state for minutes. The metadata DB was overloaded by large XComs and years of task-instance history. Pruning history with `airflow db clean` and moving payloads to object storage fixed scheduling latency.

**Common mistakes and red flags**

- Thinking the web server runs tasks.
- Not knowing where state is stored.
- Ignoring the metadata DB as a scaling limit.

**Follow-up questions**

- What happens if the scheduler dies? — Running tasks continue on workers; with multiple schedulers another takes over, otherwise no new tasks are scheduled until it restarts.
- What are managed Airflow options? — Amazon MWAA, Google Cloud Composer, Astronomer, and Azure-hosted options.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q42" data-level="Advanced">
<summary><span class="qb-num">Q42</span><h3 class="qb-title">Airflow executors: Local vs Celery vs Kubernetes</h3><span class="qb-meta">Advanced · Common · Orchestration (Airflow)</span></summary>

**Short answer**

LocalExecutor runs tasks as subprocesses on the scheduler's machine, which is simple but doesn't scale out. CeleryExecutor sends tasks through a message broker to a pool of long-running workers, giving fast start-up and horizontal scale. KubernetesExecutor launches one pod per task, giving strong isolation and per-task resources at the cost of start-up latency.

**Detailed explanation**

| Executor | How tasks run | Pros | Cons | Fits |
|---|---|---|---|---|
| Sequential (dev only) | One at a time | Zero setup | No parallelism | Local testing |
| Local | Parallel subprocesses on one host | Simple, no broker | Single-machine limit | Small teams, light orchestration |
| Celery | Queue (Redis/RabbitMQ) → persistent workers | Low latency, autoscaling worker pools, named queues | Broker to operate; shared worker environment | Many short tasks, steady load |
| Kubernetes | New pod per task | Isolation, per-task image/resources, scales to zero | Pod start-up seconds; needs K8s skills | Mixed workloads, varied dependencies |
| CeleryKubernetes (2.x) / multiple executors (2.10+) | Route per task or queue | Best of both | More complexity | Large platforms |

Since Airflow 2.10 you can configure **multiple executors** at once and choose per task with the `executor` argument. This replaces the older hybrid CeleryKubernetesExecutor pattern. Executors were also moved into provider packages.

**Choosing**
- Most tasks only trigger remote work (Snowflake SQL, Databricks jobs) and are light: Celery or Local is efficient.
- Tasks run Python with conflicting dependencies or need big memory: Kubernetes, or `KubernetesPodOperator` on any executor.
- Strict tenant isolation for many teams: Kubernetes.

**Example**

```python
# Runtime: Python 3.11, Apache Airflow 2.10+ with multiple executors configured
# airflow.cfg: [core] executor = CeleryExecutor,KubernetesExecutor
from airflow.decorators import task

@task(executor="KubernetesExecutor")
def heavy_transform():
    # runs in its own pod with its own resources
    pass

@task  # uses the default (first listed) executor: Celery
def trigger_dbt_job():
    pass
```

**Real-world use case**

A platform team ran Celery for thousands of small "trigger and wait" tasks. It routed a few memory-hungry Python tasks to the Kubernetes executor, so one 16 GB task no longer starved shared workers.

**Common mistakes and red flags**

- Choosing Kubernetes executor for thousands of 5-second tasks, where pod start-up dominates.
- Confusing `KubernetesPodOperator` (an operator) with `KubernetesExecutor` (how all tasks run).
- Not mentioning the broker for Celery.

**Follow-up questions**

- What limits concurrency? — `parallelism`, per-DAG `max_active_tasks`, pools, and worker or pod capacity.
- How do you isolate a noisy team? — Use dedicated Celery queues or pools, or the Kubernetes executor with namespaces and resource quotas.

**In depth:** [How do you choose an Airflow executor: Local, Celery, Kubernetes or several?](/interview/airflow/choosing-an-executor/)

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q43" data-level="Intermediate">
<summary><span class="qb-num">Q43</span><h3 class="qb-title">What are XComs, and what are their limits?</h3><span class="qb-meta">Intermediate · Common · Orchestration (Airflow)</span></summary>

**Short answer**

XComs ("cross-communications") let Airflow tasks pass small values to each other. They are stored by default in the metadata database. They suit IDs, file paths, row counts or small JSON. They are not for datasets: large XComs bloat the database and slow the scheduler.

**Detailed explanation**

- A task **pushes** an XCom explicitly (`ti.xcom_push`) or implicitly by returning a value from a TaskFlow `@task` or a `PythonOperator` callable.
- A downstream task **pulls** it (`ti.xcom_pull(task_ids=...)`) or receives it as a function argument in TaskFlow.
- Values are serialised and stored with `dag_id`, `task_id`, `run_id` and key.

**Limits and good practice**
- **Size:** the default backend stores values in the metadata DB, and practical size limits depend on the database (on the order of KB to a few GB depending on the DB). Treat anything beyond small payloads as wrong.
- **Pass references, not data:** write the DataFrame to S3 or a table, then pass the path or table name.
- **Custom XCom backends:** store values in object storage and keep only a reference in the DB. Airflow provides an object-storage XCom backend in its common IO provider.
- **Serialisation:** values must be serialisable. TaskFlow handles common types.
- **Coupling:** heavy XCom use can hide data dependencies that should instead be modelled as Assets (data-aware scheduling).

**Example**

```python
# Runtime: Python 3.11, Apache Airflow 2.x/3.x (TaskFlow)
from airflow.decorators import task

@task
def stage_orders(ds: str | None = None) -> dict:
    path = f"s3://staging/orders/{ds}/part-0000.parquet"
    # ... write data to path ...
    return {"path": path, "row_count": 125_000}   # small metadata only

@task
def validate(meta: dict) -> None:
    if meta["row_count"] == 0:
        raise ValueError(f"No rows staged at {meta['path']}")

validate(stage_orders())
```

**Real-world use case**

A DAG returned a 200 MB pandas DataFrame from a task. The metadata DB grew rapidly and the scheduler slowed for every DAG. Writing the data to Parquet in S3 and returning only the path fixed it.

**Common mistakes and red flags**

- Returning whole DataFrames from tasks.
- Not knowing XComs live in the metadata DB by default.
- Using XCom to coordinate between different DAGs instead of Assets or triggers.

**Follow-up questions**

- How do you share data across DAGs? — Use Assets (data-aware scheduling) or `TriggerDagRunOperator` with a config payload.
- Are XComs cleaned up? — They're cleared on task retry/clear; old records should be pruned with `airflow db clean`.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q44" data-level="Advanced">
<summary><span class="qb-num">Q44</span><h3 class="qb-title">Sensors vs deferrable operators</h3><span class="qb-meta">Advanced · Common · Orchestration (Airflow)</span></summary>

**Short answer**

Sensors wait for a condition, such as a file existing or an external job finishing. In `poke` mode they hold a worker slot the whole time; in `reschedule` mode they release it between checks. Deferrable operators hand the wait to the async triggerer process and free the worker entirely, so thousands of waits cost almost nothing.

**Detailed explanation**

**Classic sensors**
- `mode="poke"` (default): the sensor sleeps and re-checks inside a running worker slot. A sensor waiting 6 hours occupies a slot for 6 hours.
- `mode="reschedule"`: the task ends after each check and is rescheduled. That frees the slot, but adds scheduler overhead and a minimum interval.
- Always set `timeout` and `poke_interval`, and choose `soft_fail` if a timeout should skip rather than fail.

**Deferrable operators (Airflow 2.2+)**
- The task starts, submits work or a check, then **defers**: it serialises a *trigger* and exits.
- The **triggerer** runs many triggers in one asyncio event loop. When a trigger fires (the file appears, the job completes), the task resumes on a worker.
- Most major providers offer deferrable versions, typically via `deferrable=True`: S3 key sensors, Databricks, Snowflake, BigQuery, dbt Cloud and others.
- You must run at least one triggerer process.

| Approach | Worker slot while waiting | Overhead | Use when |
|---|---|---|---|
| Sensor, poke | Held | Low latency | Short waits (minutes) |
| Sensor, reschedule | Released between checks | Scheduler churn | Long waits, no deferrable version |
| Deferrable | Released; triggerer handles | Very low | Long waits at scale |

**Example**

```python
# Runtime: Python 3.11, Apache Airflow 2.x + Amazon provider
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

wait_for_file = S3KeySensor(
    task_id="wait_for_vendor_file",
    bucket_name="vendor-drop",
    bucket_key="sales/{{ ds }}/_SUCCESS",
    deferrable=True,          # hand the wait to the triggerer
    poke_interval=300,        # check every 5 minutes
    timeout=6 * 60 * 60,      # give up after 6 hours
)
```

**Real-world use case**

A platform had 400 S3 sensors in poke mode waiting overnight for vendor files, and they consumed most worker capacity. Switching to deferrable sensors freed the workers, and one triggerer handled all the waits.

**Common mistakes and red flags**

- Leaving sensors in poke mode with no timeout.
- Not running a triggerer, so deferred tasks never resume.
- Using sensors where event-driven triggering (Assets, external events) fits better.

**Follow-up questions**

- What is a pool? — A named limit on concurrent tasks, often used to throttle sensors or protect an external system.
- How else can you avoid waiting? — Event-driven scheduling: trigger the DAG when the file lands, through Assets or an API call from a storage event.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q45" data-level="Intermediate">
<summary><span class="qb-num">Q45</span><h3 class="qb-title">Airflow catchup, backfill, start_date and logical date</h3><span class="qb-meta">Intermediate · Very Common · Orchestration (Airflow)</span></summary>

**Short answer**

Each scheduled DAG run covers a data interval and is labelled by its logical date, which is the start of that interval. A daily run for 1 March therefore actually executes just after 1 March ends. `catchup=True` makes Airflow create runs for every missed interval since `start_date`. A backfill explicitly runs a DAG for a chosen historical range.

**Detailed explanation**

**Data intervals and logical date**
- With `schedule="@daily"`, the run whose logical date is `2026-03-01` has the data interval `[2026-03-01 00:00, 2026-03-02 00:00)` and starts *after* the interval ends, at about `2026-03-02 00:00`.
- Templates such as `{{ ds }}`, `{{ data_interval_start }}` and `{{ data_interval_end }}` let tasks process exactly that slice. That is what makes runs reproducible and backfills correct.
- `execution_date` was the old name. It was renamed `logical_date` in 2.2 and removed as a context key in Airflow 3.

**start_date**
- A static date in the past, never `datetime.now()`, which shifts on every parse.
- The first run covers the interval starting at `start_date`.

**catchup**
- `catchup=True`: when a DAG is unpaused or deployed with an old `start_date`, Airflow schedules every missing interval, which can create hundreds of runs.
- `catchup=False`: only the most recent interval runs. In Airflow 3 the default for `catchup` changed to `False`.

**Backfill**
- Explicit reprocessing of a date range: `airflow dags backfill` in 2.x, and backfills managed by the scheduler (triggerable from UI or API) in Airflow 3.
- Control parallelism with `max_active_runs` so backfills don't overwhelm downstream systems.
- Only safe if tasks are idempotent and parameterised by the interval (see [Q46](/interview/data-engineering/#q46)).

**Example**

```python
# Runtime: Python 3.11, Apache Airflow 2.x/3.x
from datetime import datetime
from airflow.decorators import dag, task

@dag(schedule="@daily", start_date=datetime(2026, 1, 1), catchup=False, max_active_runs=3)
def orders_by_interval():
    @task
    def load_interval(data_interval_start=None, data_interval_end=None):
        # Process exactly this interval -> safe to rerun or backfill
        sql = f"""
            DELETE FROM fct_orders
             WHERE order_ts >= '{data_interval_start}' AND order_ts < '{data_interval_end}';
            INSERT INTO fct_orders
            SELECT * FROM stg_orders
             WHERE order_ts >= '{data_interval_start}' AND order_ts < '{data_interval_end}';
        """
        print(sql)  # in practice, execute via a hook with bound parameters
    load_interval()

orders_by_interval()
```

**Real-world use case**

An engineer deployed a DAG with `start_date` two years back and `catchup=True`. Around 700 runs started at once and saturated the warehouse. Setting `catchup=False` and doing a controlled backfill with `max_active_runs=4` fixed it.

**Common mistakes and red flags**

- Expecting the 1 March daily run to start at midnight on 1 March.
- Using `datetime.now()` as `start_date`.
- Processing "yesterday" via `CURRENT_DATE - 1` instead of the interval, which breaks backfills.

**Follow-up questions**

- How do you rerun a single day? — Clear that DAG run's tasks in the UI or CLI; idempotent tasks will recompute it.
- What is `depends_on_past`? — A task only runs if its previous run's instance succeeded, which enforces sequential processing.

**In depth:** [What is the difference between catchup and backfill in Airflow 3?](/interview/airflow/catchup-and-backfill/)

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q46" data-level="Intermediate">
<summary><span class="qb-num">Q46</span><h3 class="qb-title">How do you make Airflow tasks idempotent and safe to retry?</h3><span class="qb-meta">Intermediate · Common · Orchestration (Airflow)</span></summary>

**Short answer**

Make each task produce the same end state no matter how many times it runs for the same interval. Parameterise it by the data interval rather than "now", write with overwrite, partition replace or MERGE rather than blind append, and make external side effects deduplicatable. Then retries, clears and backfills become safe.

**Detailed explanation**

Airflow will rerun tasks: automatic retries, manual clears and backfills. If a task appends rows, each rerun duplicates data. See [Q61](/interview/data-engineering/#q61) for the general principle. Here are the Airflow-specific techniques.

1. **Use the interval, not the clock.** Read `data_interval_start` and `data_interval_end` (or `ds`). Never `datetime.now()` or `CURRENT_DATE` for business logic.
2. **Deterministic outputs.** Write to `.../dt={{ ds }}/` and overwrite that partition, or `DELETE` the interval then `INSERT` in one transaction, or `MERGE` on a natural key.
3. **Atomic writes.** Write to a temp location or staging table, then rename, swap or commit. A half-failed attempt then leaves nothing visible.
4. **Side effects:** emails, API posts and file drops to partners need idempotency keys (for example `dag_id + run_id`) or a "sent" log checked before sending.
5. **Retries config:** set `retries`, `retry_delay` and `retry_exponential_backoff`. Use `execution_timeout` so hung tasks fail instead of blocking.
6. **Small tasks.** One task, one responsibility, so a retry repeats only the failed step.
7. **No local state.** Workers are ephemeral, so pass references through XCom and storage, not local files.

**Example**

```python
# Runtime: Python 3.11, Apache Airflow 2.x/3.x + Snowflake provider
from datetime import timedelta
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

load_daily = SQLExecuteQueryOperator(
    task_id="load_daily_sales",
    conn_id="snowflake_default",
    sql="""
        BEGIN;
        DELETE FROM fct_daily_sales WHERE sales_date = '{{ ds }}';
        INSERT INTO fct_daily_sales
        SELECT sales_date, store_id, SUM(amount)
        FROM stg_sales WHERE sales_date = '{{ ds }}'
        GROUP BY sales_date, store_id;
        COMMIT;
    """,
    retries=3,
    retry_delay=timedelta(minutes=5),
    retry_exponential_backoff=True,
    execution_timeout=timedelta(hours=1),
)
```
`{{ ds }}` is rendered by Airflow from the run's logical date, not supplied by users, so this templating is not an injection risk here. For user-supplied values, use bound parameters.

**Real-world use case**

A task appended daily revenue. When the warehouse timed out after the insert but before the task reported success, Airflow retried and revenue doubled for that day. Changing to delete-and-insert in a transaction made retries harmless.

**Common mistakes and red flags**

- `INSERT` without a matching delete or MERGE.
- Using retries to mask non-idempotent tasks.
- Sending notifications on every retry.

**Follow-up questions**

- How do you test idempotency? — Run the task twice for the same interval and assert the target is identical (row counts, checksums).
- What about streaming? — Use checkpoints plus idempotent sinks or MERGE (see [Q291](/interview/pyspark/#q291)).

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q47" data-level="Advanced">
<summary><span class="qb-num">Q47</span><h3 class="qb-title">What changed in Airflow 3 (Assets, DAG versioning, Task SDK)?</h3><span class="qb-meta">Advanced · Occasional · Orchestration (Airflow)</span></summary>

**Short answer**

Airflow 3, released in 2025, introduced the Task Execution API and Task SDK, so tasks no longer talk directly to the metadata DB. It also added DAG versioning, renamed Datasets to Assets with richer event-driven scheduling, shipped a new React UI and scheduler-managed backfills, and removed long-deprecated features such as `execution_date` and SubDAGs.

**Detailed explanation**

Key changes to mention:

- **Task Execution API / Task SDK:** workers communicate with Airflow through an API instead of direct database connections. This improves security and isolation and enables **remote execution** (tasks running in another cloud or on-prem environment). The `airflow.sdk` namespace is the stable authoring interface.
- **DAG versioning:** Airflow tracks DAG versions, and the UI shows which version of the code each run used. Previously a code change silently applied to historical views.
- **Assets (formerly Datasets):** data-aware scheduling, where a DAG runs when an upstream asset is updated. It was extended with asset partitions and event-driven triggers (for example from message queues), and decorators like `@asset`.
- **Backfills as first-class:** created and run by the scheduler, visible in the UI, cancellable.
- **New UI:** rebuilt in React, with better asset and DAG-version views.
- **Removed or changed:**
  - `execution_date` context key removed (use `logical_date`).
  - SubDAGs removed (use TaskGroups).
  - The `schedule_interval` argument removed in favour of `schedule`.
  - `catchup` default is now `False`.
  - Some context variables were removed.
- **Architecture:** a separate DAG processor and an API server replacing the old web server.

**Example**

```python
# Runtime: Python 3.11, Apache Airflow 3.x
from airflow.sdk import Asset, dag, task

raw_orders = Asset("s3://lake/bronze/orders")

@dag(schedule=[raw_orders])      # run whenever the asset is updated
def build_silver_orders():
    @task
    def transform():
        print("rebuild silver orders")
    transform()

build_silver_orders()
```

**Real-world use case**

A company planning its upgrade from 2.x ran Airflow's upgrade-check ruff rules over its DAG repo. They flagged `execution_date`, `schedule_interval` and direct database access inside tasks, all of which had to be fixed before cutover.

**Common mistakes and red flags**

- Claiming Airflow 3 is a rewrite that breaks all DAGs. Most modern 2.x DAGs migrate with modest changes.
- Not knowing that direct metadata-DB access from tasks is no longer allowed.
- Confusing Assets with XComs.

**Follow-up questions**

- How would you migrate? — Upgrade to the latest 2.x first, fix deprecation warnings with the migration linting rules, test in staging, then cut over.
- Why did they restrict DB access from tasks? — For security and multi-tenancy, and so tasks can run remotely through the API.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q48" data-level="Intermediate">
<summary><span class="qb-num">Q48</span><h3 class="qb-title">Airflow vs Dagster vs Prefect vs cloud-native orchestrators</h3><span class="qb-meta">Intermediate · Occasional · Orchestration (Airflow)</span></summary>

**Short answer**

Airflow is the most widely adopted task-centric orchestrator, with a huge provider ecosystem. Dagster is asset-centric: you define the data assets and their dependencies, and it gives strong lineage, typing and testing. Prefect focuses on Pythonic, dynamic flows with a light setup. Cloud-native options (Lakeflow Jobs, Snowflake Tasks, AWS Step Functions, Azure Data Factory) integrate deeply with one platform.

**Detailed explanation**

| Tool | Core model | Strengths | Watch-outs |
|---|---|---|---|
| Apache Airflow | Tasks in DAGs (Assets added) | Ecosystem, maturity, hiring pool, managed offerings | Scheduler/DB tuning; historically weak local testing |
| Dagster | Software-defined assets | Asset lineage, partitions, type checks, local dev and testing | Smaller ecosystem; different mental model |
| Prefect | Python flows and tasks | Dynamic workflows, simple deployment, hybrid execution | Fewer prebuilt integrations than Airflow |
| Lakeflow Jobs (Databricks) | Jobs with tasks | Native to Databricks compute, repair runs, serverless | Mostly Databricks-centric |
| Snowflake Tasks / task graphs | SQL tasks in Snowflake | No extra infra, serverless option | Snowflake-only scope |
| AWS Step Functions / Azure Data Factory | State machine / pipelines | Native cloud integration, managed | Vendor lock-in; code-first experience varies |

**How I'd choose**
- **Platform scope:** if nearly everything runs in one platform (all Databricks or all Snowflake), its native scheduler is often enough and cheaper to operate.
- **Cross-system workflows** (SaaS APIs, multiple warehouses, ML, reverse ETL) favour a general orchestrator.
- **Team skills and existing investment:** Airflow knowledge is widespread.
- **Asset-centric needs** (lineage, partition-aware backfills): Dagster is compelling.
- **Operational appetite:** managed services (MWAA, Composer, Astronomer, Dagster+, Prefect Cloud) reduce operations.

**Real-world use case**

A team with all transformations in Databricks moved simple daily pipelines from Airflow to Lakeflow Jobs to cut infrastructure. It kept Airflow for cross-platform workflows that touch Salesforce, Snowflake and an internal API.

**Common mistakes and red flags**

- Picking a tool on hype without considering team skills and operations.
- Saying one tool is objectively "best".
- Running two general orchestrators with overlapping scope and no clear rule.

**Follow-up questions**

- What does "asset-centric" buy you? — The orchestrator knows which tables exist and how fresh they are, so it can materialise only stale assets and show lineage.
- Can dbt be the orchestrator? — dbt orders its own models; you still need a scheduler for runs, ingestion and non-dbt steps.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q49" data-level="Intermediate">
<summary><span class="qb-num">Q49</span><h3 class="qb-title">What is Change Data Capture? Log-based vs query-based vs trigger-based</h3><span class="qb-meta">Intermediate · Very Common · CDC</span></summary>

**Short answer**

Change Data Capture (CDC) identifies inserts, updates and deletes in a source database and delivers them downstream. Log-based CDC reads the database's transaction log (MySQL binlog, PostgreSQL WAL, Oracle redo, SQL Server CDC tables). Query-based CDC polls with a timestamp or ID filter. Trigger-based CDC uses database triggers that write changes to audit tables.

**Detailed explanation**

| Approach | How it works | Captures deletes | Source load | Latency | Notes |
|---|---|---|---|---|---|
| Log-based | Reads transaction log | Yes | Very low | Seconds | Needs log access/config; best fidelity |
| Query-based | `WHERE updated_at > :wm` | No (unless soft delete) | Query load on source | Minutes+ | Misses intermediate states; see [Q26](/interview/data-engineering/#q26) |
| Trigger-based | Triggers write to change tables | Yes | Adds write overhead to every transaction | Seconds | Intrusive; schema changes need trigger updates |

**Log-based CDC** is preferred for serious workloads because it:
- captures every change, including deletes and intermediate updates, in commit order;
- adds almost no query load to the source;
- provides before and after images and transaction metadata.

It requires database configuration (for example `wal_level=logical` and a replication slot in PostgreSQL, or row-based binlog in MySQL). It also requires monitoring, because an unconsumed replication slot makes the WAL grow until the disk fills.

**Typical tools**
- Debezium (open source, via Kafka Connect; see [Q50](/interview/data-engineering/#q50)).
- AWS DMS, Azure Data Factory/SQL Server CDC, Google Datastream, Oracle GoldenGate.
- Managed ELT connectors (Fivetran, Airbyte), Snowflake Openflow, Databricks Lakeflow Connect.

**CDC event shape:** usually an `op` field (c = create, u = update, d = delete, r = snapshot read), `before` and `after` images, a source position (LSN or binlog offset) and a commit timestamp. Downstream, you apply events in order with MERGE, or append them to a history table and derive current state (see [Q51](/interview/data-engineering/#q51), [Q409](/interview/scenario-based/#q409)).

**Initial snapshot:** CDC tools first take a consistent snapshot of the existing table, then stream changes from the snapshot's log position. That avoids gaps or double-counting.

**Real-world use case**

An e-commerce team replaced a nightly full dump of a 500 GB orders database with Debezium CDC. Source load dropped, deletes finally reached the warehouse, and freshness improved from 24 hours to under 5 minutes.

**Common mistakes and red flags**

- Calling `updated_at` polling "CDC" without stating its limits.
- Forgetting the initial snapshot.
- Not monitoring replication slot lag on PostgreSQL.

**Follow-up questions**

- How do you keep ordering? — Key the Kafka topic by primary key so changes per row stay in one partition, and apply them in log-position order.
- What happens on schema change in the source? — Log-based tools emit schema change events; downstream must evolve or a contract must block it (see [Q407](/interview/scenario-based/#q407)).

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q50" data-level="Advanced">
<summary><span class="qb-num">Q50</span><h3 class="qb-title">Design a log-based CDC pipeline with Debezium and Kafka</h3><span class="qb-meta">Advanced · Common · CDC</span></summary>

**Short answer**

Enable logical replication on the source, and run a Debezium source connector in Kafka Connect that snapshots tables and then streams changes to one Kafka topic per table, keyed by primary key. Register schemas in a schema registry. A sink (Spark, Flink or a connector) consumes the topics and MERGEs changes into lakehouse or warehouse tables, idempotently and in order.

**Detailed explanation**

**Components**
1. **Source DB preparation:** for PostgreSQL, `wal_level=logical`, a replication user, and a publication for the selected tables. Debezium creates or uses a replication slot.
2. **Kafka Connect cluster** running the Debezium PostgreSQL connector, configured with tables to include, snapshot mode and topic prefix.
3. **Topics:** one per table (`prefix.schema.table`), keyed by primary key so all changes to a row go to the same partition in order. Use log compaction on topics if consumers need latest-state replays (see [Q71](/interview/data-engineering/#q71)).
4. **Schema registry** with Avro or Protobuf, and compatibility rules (see [Q72](/interview/data-engineering/#q72)).
5. **Sink processing**, often Spark Structured Streaming with `foreachBatch`:
   - dedupe within the micro-batch, keeping the latest event per key by source LSN;
   - `MERGE` into the target: delete when `op = 'd'`, update or insert otherwise;
   - checkpoint offsets for exactly-once effects (see [Q291](/interview/pyspark/#q291)).
6. **Bronze history:** optionally append every raw change event to a bronze table for audit and replay.

**Operational concerns**
- **Replication slot monitoring:** if consumers stop, the WAL accumulates on the source. Alert on slot lag.
- **Tombstones:** Debezium emits a delete event followed by a tombstone (null value) for compaction. Sinks must handle both.
- **Schema changes:** a new nullable column flows through. Dropped or renamed columns need contract handling.
- **Re-snapshots:** plan for incremental snapshots (Debezium signals) when adding tables, rather than restarting everything.

**Example**

```python
# Runtime: PySpark 3.5 with Delta Lake (Databricks or OSS delta-spark)
from pyspark.sql import functions as F, Window
from delta.tables import DeltaTable

def upsert_batch(batch_df, batch_id):
    # batch_df columns: op, id, name, email, lsn (already parsed from Debezium JSON/Avro)
    latest = (batch_df
              .withColumn("rn", F.row_number().over(
                  Window.partitionBy("id").orderBy(F.col("lsn").desc())))
              .filter("rn = 1").drop("rn"))
    target = DeltaTable.forName(batch_df.sparkSession, "silver.customers")
    (target.alias("t")
        .merge(latest.alias("s"), "t.id = s.id")
        .whenMatchedDelete(condition="s.op = 'd'")
        .whenMatchedUpdate(condition="s.op <> 'd'",
                           set={"name": "s.name", "email": "s.email", "lsn": "s.lsn"})
        .whenNotMatchedInsert(condition="s.op <> 'd'",
                              values={"id": "s.id", "name": "s.name",
                                      "email": "s.email", "lsn": "s.lsn"})
        .execute())

(parsed_cdc_stream.writeStream
    .foreachBatch(upsert_batch)
    .option("checkpointLocation", "/checkpoints/silver_customers")
    .start())
```
For full idempotency on replays, also add `s.lsn > t.lsn` to the update condition, so older events never overwrite newer state.

**Real-world use case**

A logistics company streams 40 PostgreSQL tables through Debezium into Kafka. Spark jobs merge them into Delta silver tables every minute, and dispatch dashboards read near-real-time shipment state.

**Common mistakes and red flags**

- Not keying topics by primary key, which breaks ordering.
- Ignoring replication slot growth on the source.
- MERGE without in-batch deduplication, which causes "multiple source rows matched" errors.

**Follow-up questions**

- How do you handle a 2 TB initial snapshot? — Use Debezium incremental snapshots, or a bulk export plus streaming from a recorded log position.
- Why not write directly from Debezium to the warehouse? — You can with sink connectors, but Kafka decouples producers from multiple consumers and allows replay.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q51" data-level="Intermediate">
<summary><span class="qb-num">Q51</span><h3 class="qb-title">How do you handle deletes in CDC: soft vs hard deletes</h3><span class="qb-meta">Intermediate · Common · CDC</span></summary>

**Short answer**

A hard delete removes the row in the target when the source deletes it. A soft delete keeps the row and marks it, for example `is_deleted = TRUE` with a `deleted_at` timestamp. Soft deletes preserve history and auditability. Hard deletes are needed for privacy erasure and keep tables lean. Many teams soft-delete in silver and filter deleted rows in gold.

**Detailed explanation**

**Detecting deletes**
- **Log-based CDC** emits explicit delete events (`op = 'd'`) with the before image.
- **Query-based** extraction cannot see deletes. Options: the source uses soft deletes itself, periodic full key comparison (anti-join source keys against target keys), or a source-side delete log table.

**Applying deletes**
| Strategy | Target behaviour | Pros | Cons |
|---|---|---|---|
| Hard delete | `MERGE ... WHEN MATCHED AND op='d' THEN DELETE` | Mirrors source; lean | History lost; can't explain past reports |
| Soft delete | Update `is_deleted`, `deleted_at` | Audit trail; reversible | Every consumer must filter; table grows |
| SCD2 close-out | End the current version, optional tombstone version | Full history, point-in-time correct | More complex |

**Recommendations**
- Keep raw change events in bronze (append-only), so nothing is lost even if silver hard-deletes.
- Expose views or gold models that filter `is_deleted = FALSE`, so consumers don't need to remember.
- **Privacy deletions (GDPR) must be physical** across all layers, including history and time-travel retention (see [Q85](/interview/data-engineering/#q85), [Q412](/interview/scenario-based/#q412)). Soft delete is not enough there.
- Handle **delete-then-reinsert** of the same key: the later insert must win by log position.

**Example**

```sql
-- Dialect: Snowflake SQL
-- Soft-delete pattern from a batch of CDC events (latest event per key already selected)
MERGE INTO silver_customers t
USING cdc_customers_latest s
   ON t.customer_id = s.customer_id
WHEN MATCHED AND s.op = 'd' THEN UPDATE SET
     t.is_deleted = TRUE,
     t.deleted_at = s.commit_ts
WHEN MATCHED AND s.op <> 'd' THEN UPDATE SET
     t.full_name  = s.full_name,
     t.email      = s.email,
     t.is_deleted = FALSE,
     t.deleted_at = NULL
WHEN NOT MATCHED AND s.op <> 'd' THEN INSERT
     (customer_id, full_name, email, is_deleted, deleted_at)
     VALUES (s.customer_id, s.full_name, s.email, FALSE, NULL);
```

**Real-world use case**

A SaaS company soft-deletes cancelled accounts in silver, so churn analysis can still count them. A separate GDPR workflow physically purges personal fields for erasure requests across bronze, silver and gold.

**Common mistakes and red flags**

- Assuming incremental timestamp loads capture deletes.
- Soft-deleting but letting dashboards count deleted rows.
- Treating a soft delete as satisfying GDPR erasure.

**Follow-up questions**

- How do you find deletes without CDC? — Compare the full key set from the source with the target periodically and mark the missing keys.
- What about deletes in fact tables? — Usually soft-delete or reverse with negative adjustments, depending on accounting rules.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q52" data-level="Beginner">
<summary><span class="qb-num">Q52</span><h3 class="qb-title">Row-oriented vs columnar file formats</h3><span class="qb-meta">Beginner · Very Common · File Formats</span></summary>

**Short answer**

Row-oriented formats (CSV, JSON, Avro) store each record's fields together. They are good for writing and reading whole records. Columnar formats (Parquet, ORC) store each column's values together. They are good for analytics that read a few columns across many rows, and they compress far better.

**Detailed explanation**

Imagine `orders(order_id, customer_id, amount, status, notes)` with 1 billion rows, and a query `SELECT SUM(amount) WHERE status = 'paid'`.

- **Row format:** every row's bytes (including large `notes`) must be read to extract `amount` and `status`.
- **Columnar format:** only the `amount` and `status` column chunks are read. Min/max statistics let the reader skip chunks entirely. Similar values sit together, so encodings such as dictionary, run-length and bit-packing shrink them dramatically.

| Aspect | Row-oriented (CSV, JSON, Avro) | Columnar (Parquet, ORC) |
|---|---|---|
| Write pattern | Fast appends, streaming | Buffer rows then write column chunks |
| Read pattern | Whole records | Subsets of columns |
| Compression | Moderate | High |
| Predicate skipping | None (CSV/JSON) | Min/max stats, bloom filters |
| Schema | CSV none; JSON implicit; Avro embedded | Embedded in footer |
| Typical use | Ingestion, messaging, landing | Analytics storage, lakehouse tables |

**Format notes**
- **CSV:** human-readable, no types, ambiguous quoting and escaping, no nested data. Use only at boundaries.
- **JSON:** nested and self-describing, but verbose and slow to parse at scale.
- **Avro:** compact binary row format with an embedded schema and strong schema-evolution rules. Common for Kafka and CDC.
- **Parquet / ORC:** the analytics standards (see [Q53](/interview/data-engineering/#q53), [Q54](/interview/data-engineering/#q54)).

A typical lakehouse flow: land as JSON or Avro, convert to Parquet-based table formats (Delta/Iceberg) in bronze or silver.

**Example**

```python
# Runtime: Python 3.11 with pyarrow
import pyarrow as pa
import pyarrow.parquet as pq

table = pa.table({
    "order_id": [1, 2, 3],
    "status": ["paid", "paid", "refunded"],
    "amount": [120.50, 75.00, 30.25],
})
pq.write_table(table, "orders.parquet", compression="zstd")

# Read only the columns the query needs
subset = pq.read_table("orders.parquet", columns=["status", "amount"])
print(subset.num_rows, subset.column_names)
```

**Real-world use case**

Converting 4 TB of daily JSON logs to Parquet cut storage to about 600 GB, and typical Athena queries scanned a fraction of the bytes, which directly cut per-TB-scanned costs.

**Common mistakes and red flags**

- Storing analytical tables as CSV in the lake.
- Saying Avro is columnar.
- Ignoring that columnar formats are poor for single-row lookups.

**Follow-up questions**

- Why is Avro common in Kafka? — Compact binary, record-oriented and with well-defined schema evolution via a registry.
- Can you update a row in Parquet? — Not in place; you rewrite files, or use a table format with deletion vectors.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q53" data-level="Advanced">
<summary><span class="qb-num">Q53</span><h3 class="qb-title">Parquet internals: row groups, column chunks, pages and statistics</h3><span class="qb-meta">Advanced · Common · File Formats</span></summary>

**Short answer**

A Parquet file is split horizontally into row groups. Each row group holds one column chunk per column, and each chunk is divided into pages, which are the unit of encoding and compression. The footer stores the schema and per-row-group, per-column statistics such as min, max and null count. Readers use those statistics to skip row groups and read only the needed columns.

**Detailed explanation**

**Layout**
- **File:** magic bytes, then row groups, then footer (file metadata), then footer length and magic bytes.
- **Row group:** a horizontal slice of rows, often about 128 MB (writers commonly default to 128 MB). Larger groups give better compression and scans. Smaller ones give finer skipping and less writer memory.
- **Column chunk:** all values of one column within a row group, stored contiguously.
- **Pages:** data pages, plus an optional dictionary page per chunk. Encoding (dictionary, RLE/bit-packing, delta) and compression (Snappy, ZSTD, Gzip) apply per page.
- **Footer metadata:** schema, row-group locations, column-chunk offsets and **statistics**. Optional **page indexes** (column and offset indexes) allow page-level skipping, and **bloom filters** help equality lookups on high-cardinality columns.

**How readers use it**
1. Read the footer first.
2. **Projection pushdown:** fetch only the requested columns' chunks.
3. **Predicate pushdown:** compare filter values with row-group min/max and skip row groups that cannot match.
4. Decode only the required pages.

**Practical implications**
- Statistics only help if the data is **sorted or clustered** on the filter column. Random order means every row group's min/max spans everything. That is why clustering, Z-ORDER and liquid clustering matter (see [Q208](/interview/databricks/#q208), [Q209](/interview/databricks/#q209)).
- **Nested data** uses repetition and definition levels (from the Dremel paper) to encode arrays and structs in columns.
- **Small files** have tiny row groups and repeated footer overhead (see [Q57](/interview/data-engineering/#q57)).

**Example**

```python
# Runtime: Python 3.11 with pyarrow
import pyarrow.parquet as pq

meta = pq.ParquetFile("orders.parquet").metadata
print("row groups:", meta.num_row_groups)
rg = meta.row_group(0)
for i in range(rg.num_columns):
    col = rg.column(i)
    stats = col.statistics
    print(col.path_in_schema, col.compression,
          stats.min if stats and stats.has_min_max else None,
          stats.max if stats and stats.has_min_max else None)
```

**Real-world use case**

Queries filtering `event_date = '2026-03-01'` on an unsorted Parquet dataset still read most files. Sorting by `event_date` within each file during compaction made the row-group statistics selective, and scan bytes fell by over 90%.

**Common mistakes and red flags**

- Thinking Parquet has indexes like a B-tree.
- Expecting min/max skipping to work on unsorted, high-cardinality columns.
- Confusing row groups with partitions (folders).

**Follow-up questions**

- What is dictionary encoding? — It replaces repeated values with small integer codes plus a dictionary, which is great for low-cardinality strings.
- When do bloom filters help? — For equality filters on high-cardinality columns such as IDs, where min/max ranges overlap.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q54" data-level="Beginner">
<summary><span class="qb-num">Q54</span><h3 class="qb-title">Parquet vs ORC vs Avro</h3><span class="qb-meta">Beginner · Very Common · File Formats</span></summary>

**Short answer**

Parquet and ORC are columnar formats for analytics. Parquet is the de facto standard across Spark, warehouses and table formats. ORC came out of the Hive ecosystem. Avro is a row-based binary format with an embedded schema and strong schema evolution, so it is preferred for streaming, messaging and CDC.

**Detailed explanation**

| Feature | Parquet | ORC | Avro |
|---|---|---|---|
| Layout | Columnar | Columnar | Row |
| Best for | Analytics, lakehouse tables | Analytics (Hive/Trino heritage) | Kafka messages, CDC, write-heavy landing |
| Unit structure | Row groups → column chunks → pages | Stripes → streams, with indexes | Blocks of records with sync markers |
| Statistics / skipping | Footer stats, page index, bloom filters | Stripe/row-index stats, bloom filters | None for predicates |
| Schema evolution | Add columns; rename depends on engine/table format | Similar to Parquet | Rich rules (defaults, aliases) via reader/writer schemas |
| Ecosystem support | Universal (Spark, Snowflake, BigQuery, Databricks, DuckDB, pandas) | Strong in Hive/Trino/Presto | Kafka/Confluent, Hadoop, many connectors |
| Used by table formats | Delta (Parquet only), Iceberg (default), Hudi base files | Iceberg and Hudi optional | Iceberg manifests; Hudi log files |

**How to choose**
- **Analytical storage:** Parquet by default. Its ecosystem breadth and table-format support make it the safe choice.
- **ORC:** fine if your stack is Hive or Trino-centric and already standardised on it. Some ACID Hive tables require ORC.
- **Avro:** event streams and CDC payloads, where each message is written and read as a whole and schemas evolve frequently. Convert to Parquet on landing for analytics.

**Real-world use case**

A fintech publishes transaction events to Kafka as Avro with a schema registry. A streaming job writes them into Iceberg tables (Parquet data files) for analytics and Avro manifest files for metadata, so each format is used where it fits.

**Common mistakes and red flags**

- Claiming ORC is always faster or always slower; it depends on engine and data.
- Using Avro as the long-term analytics format.
- Not knowing Delta Lake data files are Parquet.

**Follow-up questions**

- How does Avro schema evolution work? — The reader schema is resolved against the writer schema; new fields need defaults for backward compatibility.
- What about Arrow? — Arrow is an in-memory columnar format for fast processing and interchange, not a storage format (though Feather/Arrow IPC files exist).

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q55" data-level="Advanced">
<summary><span class="qb-num">Q55</span><h3 class="qb-title">Delta Lake vs Apache Iceberg vs Apache Hudi</h3><span class="qb-meta">Advanced · Very Common · File Formats (Table Formats)</span></summary>

**Short answer**

All three add ACID transactions, time travel and schema evolution to files in object storage. Delta Lake (from Databricks) uses an ordered JSON transaction log and is native to Spark and Databricks. Iceberg (from Netflix) uses a snapshot and manifest tree with hidden partitioning and has the broadest multi-engine and catalog support. Hudi (from Uber) focuses on fast upserts and incremental processing with copy-on-write and merge-on-read tables.

**Detailed explanation**

| Feature | Delta Lake | Apache Iceberg | Apache Hudi |
|---|---|---|---|
| Metadata model | `_delta_log` JSON commits + Parquet checkpoints | Catalog pointer → metadata.json → manifest lists → manifests | `.hoodie` timeline of instants |
| Commit mechanism | Put-if-absent on next log file | Atomic catalog pointer swap | Timeline commit |
| Partitioning | Explicit columns; Liquid Clustering recommended | Hidden partitioning with transforms (day, bucket, truncate); partition evolution | Explicit; record-level index |
| Row-level updates | Copy-on-write + deletion vectors | Copy-on-write; merge-on-read via delete files (v2) and deletion vectors (v3) | Copy-on-write or merge-on-read |
| Incremental reads | Change Data Feed | Incremental scans between snapshots | Incremental queries (core strength) |
| Engine support | Spark, Databricks, Trino, Flink, Snowflake/BigQuery read paths, UniForm | Spark, Flink, Trino, Snowflake, BigQuery, Athena, Databricks, DuckDB | Spark, Flink, Trino, Presto, Hive |
| Catalogs | Unity Catalog, Hive/Glue | REST catalog spec (Polaris, Unity, Glue, Nessie, Snowflake Horizon) | Hive/Glue |

**Convergence**
- **Delta UniForm** writes Iceberg-compatible metadata alongside Delta, so Iceberg readers can query Delta tables.
- Databricks acquired Tabular (founded by Iceberg's creators) in 2024 and supports managed Iceberg in Unity Catalog.
- **Apache XTable** (incubating) translates metadata between formats.
- **Iceberg format v3** adds deletion vectors, row lineage and a VARIANT type. Engine support for v3 is still rolling out in 2026.

**How to choose**
- Databricks-centric: Delta (or managed Iceberg if you need multi-engine).
- Multi-engine, vendor-neutral lakehouse (Snowflake + Spark + Trino): Iceberg.
- Heavy streaming upserts with incremental pull semantics on Spark/Flink: Hudi is still a strong option.

**Real-world use case**

A company runs Spark ingestion, Snowflake for BI and Trino for ad-hoc queries. It standardised on Iceberg with a REST catalog, so all three engines read and write the same tables without copies.

**Common mistakes and red flags**

- Saying the formats are incompatible "forever". UniForm and XTable exist.
- Equating Delta Lake with Databricks only. Delta Lake is open source under the Linux Foundation.
- Not mentioning hidden partitioning as an Iceberg differentiator.

**Follow-up questions**

- What is hidden partitioning? — You partition by a transform such as `day(event_ts)`; queries filter on `event_ts` and Iceberg prunes automatically, with no partition column in user queries.
- Copy-on-write vs merge-on-read? — CoW rewrites files on update (fast reads, slower writes); MoR writes delete/delta files merged at read time (fast writes, slower reads until compaction).

**In depth:** [Delta Lake vs Apache Iceberg vs Apache Hudi: how do they differ?](/interview/delta-lake/delta-vs-iceberg-vs-hudi/)

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q56" data-level="Intermediate">
<summary><span class="qb-num">Q56</span><h3 class="qb-title">How do you choose a partition key, and what is over-partitioning?</h3><span class="qb-meta">Intermediate · Very Common · Partitioning</span></summary>

**Short answer**

Choose a low-to-moderate cardinality column that most queries filter on and that spreads data evenly. Event or ingestion date is the classic choice. Over-partitioning means too many partitions with too little data each, for example by user ID or by hour on a small table. It creates huge numbers of tiny files, slow metadata operations and poor scans.

**Detailed explanation**

Folder-style partitioning (`/table/event_date=2026-03-01/`) lets engines skip whole directories when a query filters on the partition column.

**Good partition key criteria**
1. **Commonly filtered:** most queries include it (`WHERE event_date BETWEEN ...`).
2. **Reasonable cardinality:** hundreds to low thousands of partitions in total, not millions.
3. **Healthy partition size:** a common guideline is at least about 1 GB per partition, with files of roughly 128 MB–1 GB. Databricks guidance is to avoid partitioning tables under about 1 TB at all.
4. **Even distribution:** avoid keys where one value holds most of the data.
5. **Stable:** the value doesn't change for a record (an update would move it between partitions).
6. **Aligned with lifecycle:** date partitions make retention deletes and backfills simple.

**Over-partitioning symptoms**
- Millions of small files (see [Q57](/interview/data-engineering/#q57)).
- Slow job planning and metastore or listing calls.
- Writers creating one file per partition per task, which explodes file counts.

**Alternatives to more partitioning**
- Partition coarsely by date, then **cluster or sort** within it on the second filter column (customer, region).
- Use modern layout features: Delta **Liquid Clustering** (see [Q209](/interview/databricks/#q209)), Iceberg **hidden partitioning** with transforms like `bucket(16, customer_id)`, and Snowflake **micro-partitions with clustering keys** (see [Q155](/interview/snowflake/#q155)). Snowflake has no user-defined folder partitions.

**Example**

```sql
-- Dialect: Spark SQL (Delta Lake)
-- Coarse date partition; cluster within files by customer via OPTIMIZE ZORDER
CREATE TABLE silver.events (
    event_id    STRING,
    customer_id BIGINT,
    event_type  STRING,
    event_ts    TIMESTAMP,
    event_date  DATE
)
USING DELTA
PARTITIONED BY (event_date);

OPTIMIZE silver.events
WHERE event_date >= current_date() - INTERVAL 7 DAYS
ZORDER BY (customer_id);
```
For new Databricks tables, Liquid Clustering (`CLUSTER BY`) is usually preferred over partitioning plus Z-ORDER (see [Q209](/interview/databricks/#q209)).

**Real-world use case**

A team partitioned a 50 GB table by `customer_id` (2 million values). It produced 2 million folders and queries took minutes just to plan. Repartitioning by month and clustering by customer cut query times to seconds.

**Common mistakes and red flags**

- Partitioning by a high-cardinality ID.
- Partitioning small tables at all.
- Choosing a column queries rarely filter on.

**Follow-up questions**

- What happens if queries don't filter on the partition column? — No pruning; full scan, and you've paid the small-file cost for nothing.
- Partition by event date or ingestion date? — Event date for query pruning; ingestion date if late data would otherwise rewrite old partitions, often both with clustering.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q57" data-level="Intermediate">
<summary><span class="qb-num">Q57</span><h3 class="qb-title">What is the small-files problem, and how do you fix it?</h3><span class="qb-meta">Intermediate · Very Common · Partitioning &amp; File Formats</span></summary>

**Short answer**

The small-files problem is when a table consists of a huge number of tiny files, often a few KB to a few MB each. Each file adds open, list and metadata overhead and has tiny row groups, so reads and planning slow down dramatically. You fix it by writing fewer, larger files (target roughly 128 MB–1 GB) and running regular compaction.

**Detailed explanation**

**Why small files hurt**
- **Per-file overhead:** opening each file, reading its footer, and an object-storage request per file. Ten thousand 1 MB files are far slower to read than ten 1 GB files holding the same data.
- **Metadata pressure:** listing operations, Hive metastore partitions and table-format manifests or logs all grow with file count.
- **Poor compression and skipping:** small row groups compress worse and carry weaker statistics.
- **Task scheduling:** engines may launch one task per file, so the cluster spends its time scheduling.

**Common causes**
- Streaming or micro-batch writes every few seconds.
- Over-partitioning (see [Q56](/interview/data-engineering/#q56)): every write task emits a file into every partition it touches.
- High-parallelism writes (`spark.sql.shuffle.partitions = 2000` writing a 1 GB result).
- Frequent small MERGE or UPDATE operations that rewrite files.

**Fixes**
1. **Write-time sizing:** `coalesce` or `repartition` by partition columns before writing (see [Q258](/interview/pyspark/#q258)). Use optimized writes and auto compaction on Databricks (see [Q207](/interview/databricks/#q207)). Let AQE coalesce shuffle partitions (see [Q265](/interview/pyspark/#q265)).
2. **Compaction jobs:** Delta `OPTIMIZE`, Iceberg `rewrite_data_files`, Hudi clustering and compaction, or scheduled rewrite jobs for plain Parquet.
3. **Batch longer in streaming:** a longer trigger interval, or `availableNow` runs, where latency allows.
4. **Fix the partition design.**
5. **Clean up** old file versions after compaction (Delta `VACUUM`, Iceberg `expire_snapshots` and `remove_orphan_files`).

**Example**

```python
# Runtime: PySpark 3.5
from pyspark.sql import functions as F

daily = spark.read.table("bronze.events").filter(F.col("event_date") == "2026-03-01")

# Write ~8 files instead of hundreds: size the output to the data (~8 GB -> ~1 GB/file)
(daily.repartition(8)
      .write.mode("overwrite")
      .option("replaceWhere", "event_date = '2026-03-01'")
      .format("delta")
      .saveAsTable("silver.events"))
```

**Real-world use case**

A streaming job writing every 10 seconds produced 8,640 files per day per partition. Queries that should take seconds took minutes. Nightly `OPTIMIZE` on the previous day's partitions, plus a 1-minute trigger, reduced the file count by over 99%.

**Common mistakes and red flags**

- Fixing only reads (caching) without addressing how files are written.
- Running compaction but never vacuuming old files, so storage grows.
- Coalescing to 1 file for huge outputs, which creates a single-task bottleneck.

**Follow-up questions**

- How do you measure it? — Look at file counts and average file size per partition (`DESCRIBE DETAIL` on Delta, Iceberg `files` metadata table).
- Does Snowflake have this problem? — Not for native tables, which Snowflake manages internally; it does affect loading many tiny staged files (see [Q165](/interview/snowflake/#q165)).

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q58" data-level="Intermediate">
<summary><span class="qb-num">Q58</span><h3 class="qb-title">Compression codecs: Snappy vs ZSTD vs Gzip</h3><span class="qb-meta">Intermediate · Occasional · File Formats</span></summary>

**Short answer**

Snappy is very fast with moderate compression and was long the default for Parquet in Spark. ZSTD gives better compression ratios at similar or slightly higher CPU cost, with tunable levels, and is increasingly the default choice. Gzip compresses well but is slower. A gzipped CSV or JSON file is not splittable, which limits parallel reads.

**Detailed explanation**

| Codec | Ratio | Compress speed | Decompress speed | Notes |
|---|---|---|---|---|
| Snappy | Moderate | Very fast | Very fast | Historic Spark/Parquet default; low CPU |
| ZSTD | High (tunable levels) | Fast (low levels) to slow (high) | Very fast | Good balance; growing default for Parquet/Delta |
| Gzip (deflate) | High | Slow | Moderate | Widely compatible; whole gzipped text files not splittable |
| LZ4 | Moderate | Extremely fast | Extremely fast | Common in Kafka, ORC, internal shuffle |
| Brotli | Very high | Slow | Moderate | Rare in data pipelines |

**Splittability**
- Inside **Parquet and ORC**, compression applies per page or stream, so the files stay splittable whatever the codec.
- A **whole-file compressed** CSV or JSON (`.csv.gz`) must be read by a single task from the start. A single 20 GB `.gz` file gives you no parallelism. Bzip2 is splittable but slow. The fix is many moderately sized compressed files, or conversion to Parquet.

**Choosing**
- Analytics storage: ZSTD (a low level such as 1–3) or Snappy. Benchmark on your data. ZSTD often shrinks files 10–30% versus Snappy with little read penalty.
- Landing text files from vendors: gzip is fine if files are reasonably sized.
- Kafka: LZ4 or ZSTD at producer level for throughput.
- Remember that compute cost (CPU) and storage or scan cost trade off. On pay-per-scan engines, better ratios save money directly.

**Example**

```python
# Runtime: PySpark 3.5
spark.conf.set("spark.sql.parquet.compression.codec", "zstd")

(spark.read.table("bronze.clicks")
      .write.mode("overwrite")
      .parquet("s3://lake/silver/clicks_zstd/"))
```

**Real-world use case**

Switching a 200 TB Parquet lake from Snappy to ZSTD reduced storage by roughly a fifth after rewrite. Query times were essentially unchanged because the workloads were I/O-bound rather than CPU-bound.

**Common mistakes and red flags**

- Receiving one huge `.gz` file and wondering why Spark uses one core.
- Assuming the highest compression level is always best.
- Thinking compression makes Parquet unsplittable.

**Follow-up questions**

- What does Snowflake use? — Snowflake compresses its internal storage automatically; for staged files you declare the codec in the file format (or `AUTO`).
- How do you pick a ZSTD level? — Benchmark: low levels for write-heavy pipelines, higher for cold archival data.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q59" data-level="Advanced">
<summary><span class="qb-num">Q59</span><h3 class="qb-title">How does schema evolution work across Avro, Parquet and table formats?</h3><span class="qb-meta">Advanced · Common · File Formats</span></summary>

**Short answer**

Avro resolves a writer schema against a reader schema, so fields can be added with defaults and removed safely. Plain Parquet stores a schema per file, so engines must merge differing file schemas at read time. Table formats (Delta, Iceberg, Hudi) keep the authoritative schema in metadata. Iceberg tracks columns by ID, so rename, reorder and drop are safe metadata-only operations.

**Detailed explanation**

**Types of change**
- *Additive:* a new nullable column. Almost always safe.
- *Type widening:* `int` → `long`, `float` → `double`. Safe in most formats, with explicit rules.
- *Rename / reorder / drop:* dangerous if columns are matched by name or position.
- *Type narrowing or semantic change:* breaking. It needs a new column or version.

**Avro**
- Data is written with the writer's schema. Readers supply their own reader schema, and Avro resolves the two: new reader fields need a **default**, and removed fields are ignored.
- A schema registry enforces compatibility modes (backward, forward, full; see [Q72](/interview/data-engineering/#q72)).

**Plain Parquet**
- Each file has its own schema. If files differ, Spark needs `mergeSchema=true`, which is expensive, and columns match **by name**. Renames look like a dropped plus a new column.

**Delta Lake**
- *Schema enforcement* rejects writes with unexpected columns or incompatible types by default.
- *Schema evolution* via `mergeSchema` / `autoMerge` adds new columns (see [Q212](/interview/databricks/#q212)).
- *Column mapping mode* (`delta.columnMapping.mode = 'name'`) enables rename and drop as metadata operations.
- *Type widening* is supported in newer versions (table feature).

**Iceberg**
- Every column has a unique **field ID**. Data files store IDs, not just names, so add, drop, rename, reorder and type promotion are metadata-only and correct.
- Partition specs can also evolve without rewriting data (partition evolution).

**Example**

```sql
-- Dialect: Spark SQL (Apache Iceberg)
ALTER TABLE lake.silver.customers ADD COLUMN loyalty_tier STRING;
ALTER TABLE lake.silver.customers RENAME COLUMN phone TO phone_number;
ALTER TABLE lake.silver.customers ALTER COLUMN visit_count TYPE BIGINT;  -- int -> long promotion
```

**Real-world use case**

A team renamed a column in a plain Parquet dataset and older files suddenly showed NULLs for it in Spark, because the match was by name. Moving to Iceberg let them rename columns safely as a metadata change.

**Common mistakes and red flags**

- Assuming rename is safe in plain Parquet or default Delta.
- Using `mergeSchema` everywhere, which lets typos create new columns silently.
- Not distinguishing schema enforcement from evolution.

**Follow-up questions**

- How do you evolve schemas in a warehouse? — `ALTER TABLE ADD COLUMN`, or features like Snowflake's `ENABLE_SCHEMA_EVOLUTION` for loads (see [Q176](/interview/snowflake/#q176)).
- How do you make breaking changes safely? — Add the new column, dual-write, migrate consumers, then deprecate the old one.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q60" data-level="Intermediate">
<summary><span class="qb-num">Q60</span><h3 class="qb-title">Explain the CAP theorem (and PACELC) for data engineers</h3><span class="qb-meta">Intermediate · Common · Distributed Systems</span></summary>

**Short answer**

The CAP theorem says that during a network partition, a distributed system must choose between consistency (every read sees the latest write) and availability (every request gets a response). It cannot guarantee both. PACELC extends this: even without a partition, the system trades latency against consistency.

**Detailed explanation**

- **C (consistency):** linearizability. All clients see the same, most recent value.
- **A (availability):** every non-failing node answers every request.
- **P (partition tolerance):** the system keeps working despite lost messages between nodes.

In real distributed systems, partitions *will* happen, so P is not optional. The real choice during a partition is **CP** (refuse or delay some requests to stay consistent) or **AP** (answer with possibly stale data and reconcile later).

**PACELC:** **if P**artition, choose **A** or **C**; **E**lse (normal operation), choose **L**atency or **C**onsistency. Synchronous cross-region replication gives consistency but adds latency. Asynchronous replication is fast but can serve stale reads.

| System (default config) | Partition behaviour | Normal behaviour | Notes |
|---|---|---|---|
| Single-leader RDBMS with sync replica | CP | Favours consistency | Fails over; may reject writes |
| Cassandra / DynamoDB (eventual reads) | AP | Favours latency | Tunable consistency levels |
| Kafka (acks=all, min.insync.replicas=2) | Leans CP for writes | Latency vs durability tunable | Writes fail if ISR too small |
| Object storage (S3) | Strong read-after-write consistency since Dec 2020 | — | Simplifies table formats |

**Why data engineers care**
- Replicas and CDC sources can lag, so reading a replica may give stale data.
- Cross-region warehouse replication is asynchronous, so the DR copy lags.
- Exactly-once and idempotency designs (see [Q61](/interview/data-engineering/#q61), [Q62](/interview/data-engineering/#q62)) exist because networks fail between systems.

**Real-world use case**

A team read from a lagging MySQL replica for its hourly extract and missed recent orders. Knowing replicas are asynchronous (latency over consistency), they added a lag check and a lookback window to the extract.

**Common mistakes and red flags**

- Saying "pick any two of three".
- Confusing CAP consistency with ACID consistency.
- Claiming a system is "CA" in a distributed setting.

**Follow-up questions**

- Is S3 strongly consistent? — Yes; S3 has provided strong read-after-write consistency for all operations since December 2020.
- What is eventual consistency? — If writes stop, all replicas eventually converge to the same value.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q61" data-level="Beginner">
<summary><span class="qb-num">Q61</span><h3 class="qb-title">What is idempotency, and why is it critical in pipelines?</h3><span class="qb-meta">Beginner · Very Common · Distributed Systems</span></summary>

**Short answer**

An operation is idempotent if running it once or many times leaves the system in the same final state. Pipelines get retried, rerun and backfilled constantly, so idempotent steps prevent duplicates and drift. The usual tools are overwriting a deterministic partition, MERGE on a key, or deduplicating on a unique ID.

**Detailed explanation**

Failures happen between "work done" and "success recorded": a timeout after the insert committed, a network blip, a worker killed. The orchestrator retries. If the step is not idempotent, data is duplicated or double-counted.

**Techniques**
1. **Deterministic scope:** each run processes a fixed input slice (a date, a file set, an offset range), not "whatever is new right now".
2. **Replace, don't append:** overwrite the target partition (`INSERT OVERWRITE`, Delta `replaceWhere`, delete-then-insert in one transaction).
3. **Upsert on a natural key:** `MERGE` so reprocessing the same record updates rather than duplicates (see [Q137](/interview/sql/#q137)).
4. **Dedupe on unique event IDs** for append-only data.
5. **Atomic publish:** write to staging, then swap or commit (see [Q91](/interview/data-engineering/#q91)).
6. **Idempotency keys for side effects:** an API call with a request ID, so the receiver ignores repeats.
7. **Deterministic surrogate keys** (hashes) instead of sequences, so reruns generate the same keys (see [Q10](/interview/data-engineering/#q10)).

| Write pattern | Idempotent? | Why |
|---|---|---|
| `INSERT INTO ... SELECT` new data | No | Each retry appends again |
| `INSERT OVERWRITE` partition | Yes | Same partition replaced |
| `MERGE` on key | Yes | Same key updated |
| Append + dedupe by event_id downstream | Yes (end to end) | Duplicates removed |
| Sending an email per run | No | Needs a sent-log check |

**Example**

```sql
-- Dialect: Spark SQL (Delta Lake)
-- Rerunning this for the same date always yields the same partition contents
INSERT OVERWRITE gold.daily_revenue
PARTITION (revenue_date = '2026-03-01')
SELECT store_id, SUM(amount) AS revenue
FROM silver.orders
WHERE order_date = '2026-03-01'
GROUP BY store_id;
```

**Real-world use case**

A retry after a warehouse timeout doubled a day's revenue in a dashboard. Rewriting the load as a partition overwrite meant later retries and backfills could never double-count.

**Common mistakes and red flags**

- Confusing idempotent with "it ran successfully".
- Relying on "we never retry" as a strategy.
- Using `now()` inside transformations, so reruns give different results.

**Follow-up questions**

- Is MERGE always idempotent? — Only if the source is deduplicated and the update logic doesn't accumulate (`SET total = total + s.amount` is not idempotent).
- How do you make streaming idempotent? — Checkpointed offsets plus idempotent or transactional sinks (see [Q291](/interview/pyspark/#q291)).

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q62" data-level="Advanced">
<summary><span class="qb-num">Q62</span><h3 class="qb-title">At-most-once vs at-least-once vs exactly-once delivery</h3><span class="qb-meta">Advanced · Very Common · Distributed Systems</span></summary>

**Short answer**

At-most-once may lose messages but never duplicates them. At-least-once never loses messages but may duplicate them. Exactly-once means each message affects the result once. In practice, "exactly-once" is achieved end to end by combining at-least-once delivery with idempotent or transactional processing and sinks, not by magic in the network.

**Detailed explanation**

The semantics depend on *when* progress (an offset or acknowledgement) is recorded relative to processing.

- **At-most-once:** commit the offset *before* processing. A crash after the commit loses the message. Acceptable only for low-value telemetry.
- **At-least-once:** process, *then* commit the offset. A crash after processing but before the commit causes reprocessing, so duplicates appear. This is the most common default.
- **Exactly-once (effectively-once):** make reprocessing harmless or atomic:
  - **Idempotent sinks:** upsert by key or dedupe by event ID, so duplicates collapse (see [Q61](/interview/data-engineering/#q61)).
  - **Transactional writes:** commit the output and the consumed offsets atomically. Kafka transactions do this for Kafka-to-Kafka pipelines (read-process-write). Spark Structured Streaming does it with checkpoints plus idempotent or transactional sinks such as Delta (see [Q291](/interview/pyspark/#q291)).
  - **Idempotent producers** in Kafka prevent duplicates caused by producer retries (see [Q70](/interview/data-engineering/#q70)).

| Semantics | Loss possible | Duplicates possible | Implementation |
|---|---|---|---|
| At-most-once | Yes | No | Ack/commit before processing |
| At-least-once | No | Yes | Process then commit |
| Exactly-once (effective) | No | No (in results) | At-least-once + idempotent/transactional sink |

**Scope matters.** Kafka's exactly-once guarantees cover Kafka topics and its transactional consumers. Once you call an external API or write to a non-transactional database, end-to-end exactly-once depends on that sink being idempotent.

**Real-world use case**

A payments stream used at-least-once consumers writing to a ledger table. Writes became `MERGE` on `payment_id`, so redeliveries after consumer restarts no longer double-posted payments. That gave exactly-once results without distributed transactions.

**Common mistakes and red flags**

- Claiming a tool "guarantees exactly-once" with no mention of sinks.
- Not explaining where the offset commit happens.
- Choosing at-most-once for financial data.

**Follow-up questions**

- What Kafka settings enable exactly-once in streams? — Idempotent producers, transactions (`transactional.id`), and consumers with `isolation.level=read_committed`.
- How do you dedupe at scale? — Keep a bounded state of seen event IDs within a watermark window, or MERGE on the event ID in the sink.

**In depth:** [What is at-least-once delivery and what problems can it create?](/interview/kafka/at-least-once-delivery/)

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q63" data-level="Beginner">
<summary><span class="qb-num">Q63</span><h3 class="qb-title">Horizontal vs vertical scaling</h3><span class="qb-meta">Beginner · Common · Distributed Systems</span></summary>

**Short answer**

Vertical scaling (scale up) means a bigger machine: more CPU, memory or faster disks. Horizontal scaling (scale out) means more machines sharing the work. Vertical is simpler but has a ceiling and a single point of failure. Horizontal scales much further but needs data partitioning and coordination.

**Detailed explanation**

| Aspect | Vertical (scale up) | Horizontal (scale out) |
|---|---|---|
| How | Larger instance/warehouse size | More nodes/clusters |
| Complexity | Low; no code change | Higher; partitioning, shuffles, coordination |
| Limit | Largest available machine | Practically very high |
| Failure impact | Single node failure is total | Node failures tolerated |
| Cost curve | Can rise steeply at the top | Roughly linear |
| Good for | Single large queries, memory-hungry jobs | Many concurrent users, very large data |

**In data platforms**
- **Snowflake:** a bigger warehouse size (XS → XL) is scaling *up* for one complex query. A **multi-cluster warehouse** is scaling *out* for concurrency (see [Q153](/interview/snowflake/#q153)).
- **Spark / Databricks:** more workers is scaling out. Bigger instance types with more memory per executor is scaling up, which helps when tasks spill or hit OOM (see [Q273](/interview/pyspark/#q273)).
- **Kafka:** more partitions and brokers is scaling out throughput.
- **Databases:** read replicas and sharding scale out. A bigger instance scales up.

Not everything scales out well. A skewed key or a step that needs global ordering still lands on one node, so scaling out doesn't help until skew is fixed (see [Q263](/interview/pyspark/#q263)).

**Real-world use case**

A dashboard queue built up at 9 AM when 200 analysts logged in. Scaling *up* the Snowflake warehouse didn't help. Enabling multi-cluster scale-*out* (1–4 clusters) removed queuing at the same per-query speed.

**Common mistakes and red flags**

- Treating "add more nodes" as the answer to every slowdown.
- Not distinguishing concurrency problems from single-query performance problems.
- Ignoring skew when proposing scale-out.

**Follow-up questions**

- What is elastic scaling? — Automatically adding or removing capacity based on load (autoscaling clusters, multi-cluster warehouses).
- Which is cheaper? — Depends; doubling a Snowflake warehouse doubles credits per hour but can halve runtime, so cost may stay similar.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q64" data-level="Advanced">
<summary><span class="qb-num">Q64</span><h3 class="qb-title">Partitioning vs sharding vs replication</h3><span class="qb-meta">Advanced · Common · Distributed Systems</span></summary>

**Short answer**

Partitioning splits a dataset into parts by a key, within one system or across nodes. Sharding is horizontal partitioning across separate database instances, where each shard holds a subset of rows. Replication stores copies of the same data on multiple nodes for availability, durability and read scale. Systems usually combine them: each shard or partition is replicated.

**Detailed explanation**

**Partitioning**
- *Horizontal:* rows split by key (date ranges, hash of ID).
- *Vertical:* columns split into separate tables or stores (hot vs cold columns).
- In analytics engines, partitioning mainly enables **pruning** and **parallelism** (see [Q56](/interview/data-engineering/#q56)).

**Sharding**
- Each shard is an independent database (for example customers A–M on shard 1, N–Z on shard 2, or hash-based).
- It scales writes and storage beyond one machine.
- Challenges: cross-shard joins and transactions, rebalancing when shards grow (consistent hashing helps; see [Q65](/interview/data-engineering/#q65)), and hot shards from skewed keys.
- For data engineers, a sharded source means CDC from many instances and a need to union and deduplicate them downstream.

**Replication**
- *Leader-follower:* writes go to the leader and are copied to followers (sync or async).
- *Multi-leader / leaderless:* writes accepted in several places, with conflict resolution (Cassandra, DynamoDB global tables).
- Benefits: failover, durability, and read scaling. Cost: replication lag and consistency trade-offs (see [Q60](/interview/data-engineering/#q60)).

| Concept | Purpose | Data on each node | Example |
|---|---|---|---|
| Partitioning | Pruning, parallelism | A subset | Kafka partitions, Delta date partitions |
| Sharding | Scale writes/storage | A subset (separate DBs) | Vitess, Citus, sharded MySQL |
| Replication | Availability, durability, read scale | A full copy (of its shard) | Postgres replicas, Kafka replication factor 3 |

**Real-world use case**

A Kafka topic with 24 partitions (for parallelism) and replication factor 3 (for durability) means every partition has one leader and two follower replicas on different brokers.

**Common mistakes and red flags**

- Using "sharding" and "replication" interchangeably.
- Forgetting that replicas lag, and reading them for exact extracts.
- Choosing a shard key that concentrates traffic (by country, when 80% of users are in one).

**Follow-up questions**

- How do you extract from a sharded source? — Run CDC or extracts per shard, tag rows with the shard ID, and union them into one table with globally unique keys.
- What is a hot partition? — A partition receiving a disproportionate share of traffic, which limits throughput (see [Q438](/interview/scenario-based/#q438)).

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q65" data-level="Advanced">
<summary><span class="qb-num">Q65</span><h3 class="qb-title">What is consistent hashing?</h3><span class="qb-meta">Advanced · Occasional · Distributed Systems</span></summary>

**Short answer**

Consistent hashing maps both keys and nodes onto the same hash ring. Each key belongs to the next node clockwise. When a node is added or removed, only the keys in its neighbouring range move, about 1/N of the data, instead of nearly all keys as with `hash(key) % N`.

**Detailed explanation**

**The problem with modulo hashing.** With `node = hash(key) % N`, changing N from 4 to 5 changes the result for about 80% of keys. In a cache or a sharded store, that means a huge data reshuffle or a cache-miss storm.

**How consistent hashing works**
1. Hash each node (often many times, as **virtual nodes**) onto a ring of hash values, for example 0 to 2³²−1.
2. Hash each key onto the same ring.
3. A key is owned by the first node at or after its position going clockwise.
4. **Adding a node** takes over only the keys between it and its predecessor. **Removing a node** hands its keys to its successor.

**Virtual nodes** (each physical node placed at many ring positions) give a more even spread and let bigger machines own more positions.

**Where it's used**
- Distributed databases and key-value stores: Cassandra, DynamoDB's original design, Riak.
- Distributed caches and load balancers.
- Some stream processors and partitioners. Kafka's default partitioner, by contrast, uses a murmur2 hash modulo the partition count, which is why **adding partitions to a topic changes key-to-partition mapping** and breaks per-key ordering for existing keys.

**Example**

```python
# Runtime: Python 3.11 — minimal consistent-hash ring with virtual nodes
import bisect
import hashlib

class HashRing:
    def __init__(self, nodes: list[str], vnodes: int = 100) -> None:
        self._ring: list[tuple[int, str]] = []
        for node in nodes:
            for i in range(vnodes):
                self._ring.append((self._hash(f"{node}#{i}"), node))
        self._ring.sort()
        self._keys = [h for h, _ in self._ring]

    @staticmethod
    def _hash(value: str) -> int:
        return int(hashlib.md5(value.encode()).hexdigest(), 16)

    def node_for(self, key: str) -> str:
        idx = bisect.bisect(self._keys, self._hash(key)) % len(self._ring)
        return self._ring[idx][1]

ring = HashRing(["node-a", "node-b", "node-c"])
print(ring.node_for("customer-42"))
```

**Real-world use case**

A team scaling a Redis-based dedup cache from 6 to 8 nodes used consistent hashing in the client, so only about a quarter of keys moved. The dedup hit rate stayed high during the change.

**Common mistakes and red flags**

- Saying consistent hashing means "no data moves".
- Not mentioning virtual nodes for balance.
- Assuming Kafka uses consistent hashing for partitions.

**Follow-up questions**

- What happens to Kafka key ordering if you add partitions? — Keys can map to new partitions, so ordering across the change is not preserved for those keys.
- What is rendezvous (HRW) hashing? — An alternative where each key picks the node with the highest hash(key, node) score; it also minimises movement.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q66" data-level="Advanced">
<summary><span class="qb-num">Q66</span><h3 class="qb-title">What is backpressure in streaming systems?</h3><span class="qb-meta">Advanced · Occasional · Distributed Systems</span></summary>

**Short answer**

Backpressure is what happens when a downstream stage can't keep up with the rate data arrives: the slowness propagates upstream. Systems handle it by slowing producers, buffering in a durable log, rate-limiting reads, or scaling the slow stage. Without handling, you get growing lag, memory exhaustion or dropped data.

**Detailed explanation**

**Where it shows up**
- A Spark Structured Streaming job takes longer to process each micro-batch than the trigger interval, so batches queue up and latency grows.
- A Flink operator's network buffers fill, so upstream operators slow down. This is Flink's built-in credit-based flow control.
- A Kafka consumer group falls behind, and consumer lag grows (see [Q422](/interview/scenario-based/#q422)).
- A sink (database, API) throttles writes.

**How systems cope**
| Mechanism | Example |
|---|---|
| Durable buffer | Kafka retains data, so slow consumers catch up later (within retention) |
| Pull-based consumption | Consumers fetch at their own pace (Kafka, Kinesis) |
| Rate limits per batch | Spark `maxOffsetsPerTrigger`, `maxFilesPerTrigger`, `maxBytesPerTrigger` |
| Flow control | Flink credit-based backpressure between operators |
| Autoscaling | Add consumers/partitions/executors |
| Load shedding | Sample or drop low-priority data deliberately |

**Diagnosing**
- Rising consumer lag or processing time per batch versus the trigger interval.
- In Flink, the backpressure monitor in the UI shows which operator is the bottleneck.
- Look for skewed partitions, slow sinks, expensive state operations or GC pauses.

**Example**

```python
# Runtime: PySpark 3.5 — cap how much each micro-batch reads so processing time stays bounded
stream = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker1:9092")
    .option("subscribe", "clickstream")
    .option("maxOffsetsPerTrigger", 2_000_000)   # upper bound per micro-batch
    .option("startingOffsets", "latest")
    .load())
```

**Real-world use case**

After a marketing push, event volume tripled. The streaming job's batches took 90 seconds against a 30-second trigger, and lag grew for hours. Capping offsets per trigger stabilised memory. Adding partitions and executors restored throughput, and the job then drained the backlog.

**Common mistakes and red flags**

- Simply increasing buffer sizes until memory runs out.
- Not knowing where lag is measured.
- Ignoring the sink as the bottleneck.

**Follow-up questions**

- What happens if lag exceeds Kafka retention? — Unread data is deleted and lost; alert on lag relative to retention.
- How do you size for spikes? — Provision for peak-to-average ratio, use autoscaling, and rely on the log as a buffer for short bursts.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q67" data-level="Beginner">
<summary><span class="qb-num">Q67</span><h3 class="qb-title">Kafka core concepts: broker, topic, partition, offset, replication</h3><span class="qb-meta">Beginner · Very Common · Kafka</span></summary>

**Short answer**

Kafka is a distributed, durable commit log. Producers write records to topics. Each topic is split into partitions, which are ordered, append-only logs spread across brokers. Every record in a partition has a sequential offset. Consumers read by offset, and partitions are replicated across brokers for fault tolerance.

**Detailed explanation**

- **Broker:** a Kafka server that stores partitions and serves reads and writes. A cluster has several brokers. Modern Kafka (3.3+ production-ready, and the only mode in 4.0) uses **KRaft** for metadata instead of ZooKeeper.
- **Topic:** a named stream of records, such as `orders`. Configured with partition count, replication factor and retention.
- **Partition:** the unit of parallelism and ordering. Records are ordered **within** a partition, not across the topic. A record's partition is chosen by its key's hash (same key, same partition) or by the producer's partitioner when there is no key.
- **Offset:** a monotonically increasing ID per record within a partition. Consumers track their position by committing offsets, stored in the internal `__consumer_offsets` topic.
- **Replication:** each partition has one **leader** and N−1 **followers** on other brokers. The **in-sync replicas (ISR)** are the followers that are fully caught up. With `acks=all` and `min.insync.replicas=2`, a write is acknowledged only once it is safely on at least two replicas.
- **Retention:** records stay for a configured time or size (for example 7 days) whether or not they've been consumed. **Compacted** topics keep the latest value per key instead (see [Q71](/interview/data-engineering/#q71)).
- **Producers and consumers:** producers batch and compress records. Consumers in a **consumer group** share partitions (see [Q68](/interview/data-engineering/#q68)).

**Example**

```python
# Runtime: Python 3.11 with confluent-kafka
from confluent_kafka import Producer

producer = Producer({
    "bootstrap.servers": "broker1:9092",
    "acks": "all",                 # wait for all in-sync replicas
    "enable.idempotence": True,    # no duplicates from producer retries
})

def on_delivery(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")

producer.produce("orders", key="order-1001", value=b'{"amount": 49.99}', callback=on_delivery)
producer.flush()
```

**Real-world use case**

An orders topic with 12 partitions and replication factor 3 lets up to 12 consumers process in parallel, and survives a broker failure without data loss. Keying by `order_id` keeps each order's events in order.

**Common mistakes and red flags**

- Saying Kafka guarantees ordering across a whole topic.
- Thinking messages are deleted once consumed.
- Not knowing what ISR or `acks=all` means.

**Follow-up questions**

- How many partitions should a topic have? — Enough for peak consumer parallelism and throughput, with headroom; increasing later changes key mapping.
- What replaced ZooKeeper? — KRaft (Kafka Raft) metadata quorum; Kafka 4.0 removed ZooKeeper support.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q68" data-level="Intermediate">
<summary><span class="qb-num">Q68</span><h3 class="qb-title">Kafka consumer groups and rebalancing</h3><span class="qb-meta">Intermediate · Very Common · Kafka</span></summary>

**Short answer**

A consumer group is a set of consumers that share the work of reading a topic. Each partition is assigned to exactly one consumer in the group, so parallelism is capped at the number of partitions. When consumers join, leave or fail, the group rebalances and reassigns partitions. Committed offsets let the new owner resume where the old one stopped.

**Detailed explanation**

**Assignment rules**
- 12 partitions and 4 consumers: 3 partitions each.
- 12 partitions and 15 consumers: 3 consumers sit idle.
- Different groups each get the full stream independently. That is how one topic serves many applications.

**Offsets**
- Consumers commit offsets per partition, automatically (`enable.auto.commit`) or manually after processing.
- Committing **after** processing gives at-least-once. Committing **before** gives at-most-once (see [Q62](/interview/data-engineering/#q62)).
- `auto.offset.reset` (`earliest` / `latest`) applies only when the group has no committed offset.

**Rebalancing**
- Triggered by a consumer joining or leaving, a missed heartbeat (`session.timeout.ms`), exceeding `max.poll.interval.ms` between polls, or topic partition changes.
- **Eager** rebalancing stops all consumers and reassigns everything, a "stop the world" pause.
- **Cooperative / incremental** rebalancing (the `CooperativeStickyAssignor`) moves only the partitions that need to move, so other consumers keep working.
- **Static membership** (`group.instance.id`) avoids rebalances on quick restarts, such as rolling deploys.
- Kafka 4.0 made the new consumer group protocol (KIP-848, with broker-side assignment) generally available, which reduces rebalance disruption further.

**Common rebalance trouble:** slow processing per poll exceeds `max.poll.interval.ms`, so the consumer is kicked out, partitions move, work is redone, and the cycle repeats.

**Example**

```python
# Runtime: Python 3.11 with confluent-kafka — commit only after successful processing
from confluent_kafka import Consumer

consumer = Consumer({
    "bootstrap.servers": "broker1:9092",
    "group.id": "orders-loader",
    "enable.auto.commit": False,
    "auto.offset.reset": "earliest",
    "partition.assignment.strategy": "cooperative-sticky",
})
consumer.subscribe(["orders"])

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None or msg.error():
            continue
        process(msg.value())                      # user-defined, should be idempotent
        consumer.commit(message=msg, asynchronous=False)
finally:
    consumer.close()
```

**Real-world use case**

A consumer called a slow enrichment API per message and kept exceeding `max.poll.interval.ms`. That triggered rebalance storms and duplicate processing. Batching API calls, lowering `max.poll.records` and switching to cooperative rebalancing stabilised the group.

**Common mistakes and red flags**

- Adding consumers beyond the partition count and expecting more throughput.
- Using auto-commit with long processing, which risks data loss on crashes.
- Not knowing what triggers a rebalance.

**Follow-up questions**

- How do you measure consumer lag? — Log-end offset minus committed offset per partition (tools: `kafka-consumer-groups`, Burrow, cloud metrics).
- How do you reprocess from a point in time? — Reset the group's offsets to a timestamp (`--reset-offsets --to-datetime`) while consumers are stopped.

**In depth:** [Explain Kafka partitions and consumer groups.](/interview/kafka/partitions-and-consumer-groups/)

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q69" data-level="Intermediate">
<summary><span class="qb-num">Q69</span><h3 class="qb-title">What ordering guarantees does Kafka provide?</h3><span class="qb-meta">Intermediate · Common · Kafka</span></summary>

**Short answer**

Kafka guarantees order only within a single partition: consumers read records in the order they were written to it. There is no global order across partitions. To keep related events in order, give them the same key so they land in the same partition, and make sure producer retries can't reorder them.

**Detailed explanation**

**What is guaranteed**
- Within a partition, offsets are sequential and consumers see records in offset order.
- Records with the same key go to the same partition, provided the partition count doesn't change.

**What can break ordering**
1. **Multiple partitions.** Events for one entity spread across partitions if they're unkeyed or keyed inconsistently.
2. **Producer retries with multiple in-flight requests.** Without idempotence, a failed batch retried after a later batch succeeded can land out of order. *Fix:* `enable.idempotence=true` (the default in modern clients). It preserves order with up to 5 in-flight requests per connection.
3. **Increasing partition count.** Key-to-partition mapping changes, so new events for a key may go to a different partition than old ones.
4. **Consumer-side parallelism.** A consumer that processes records from one partition with a thread pool can reorder them. Process per key sequentially, or use key-aware parallel consumers.
5. **Multiple producers for the same key** without coordination, which can interleave by time of arrival.

**Design guidance**
- Choose the key by the entity whose order matters (`order_id`, `account_id`).
- Even then, downstream should carry a sequence number or source log position and **apply the latest by sequence**, not by arrival (see [Q50](/interview/data-engineering/#q50)). That makes processing robust to replays and merges.
- If you truly need a total order, a single partition gives it, but caps throughput at one consumer.

**Example**

```python
# Runtime: Python 3.11 with confluent-kafka
from confluent_kafka import Producer

producer = Producer({
    "bootstrap.servers": "broker1:9092",
    "enable.idempotence": True,          # safe retries preserve per-partition order
    "acks": "all",
})
# Same key -> same partition -> ordered for this account
for event in ({"type": "debit", "amt": 50}, {"type": "credit", "amt": 20}):
    producer.produce("account-events", key="acct-778", value=str(event).encode())
producer.flush()
```

**Real-world use case**

Account balance updates were computed wrongly because debit and credit events for the same account were unkeyed and landed in different partitions. Keying by `account_id` and applying events by sequence number fixed the balances.

**Common mistakes and red flags**

- Claiming topic-level ordering.
- Forgetting that adding partitions changes key placement.
- Parallelising within a partition without preserving key order.

**Follow-up questions**

- How do you handle ordering across topics? — You can't rely on it; join on keys and event timestamps or sequence numbers with watermarks.
- What about hot keys? — One key's partition becomes a bottleneck; consider sub-keys if strict ordering isn't required for all events (see [Q438](/interview/scenario-based/#q438)).

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q70" data-level="Advanced">
<summary><span class="qb-num">Q70</span><h3 class="qb-title">Kafka idempotent producers and transactions</h3><span class="qb-meta">Advanced · Common · Kafka</span></summary>

**Short answer**

An idempotent producer attaches a producer ID and per-partition sequence numbers to batches, so the broker discards duplicates caused by retries. Transactions go further: a producer can write to multiple partitions and commit consumer offsets atomically. Consumers using `read_committed` see either all of a transaction's records or none, which enables exactly-once read-process-write pipelines inside Kafka.

**Detailed explanation**

**Idempotent producer** (`enable.idempotence=true`, default in recent clients)
- The broker assigns a **producer ID (PID)**, and each batch carries a **sequence number** per partition.
- If a retry resends a batch the broker already has, it is deduplicated. Ordering is preserved as well (see [Q69](/interview/data-engineering/#q69)).
- Scope: one producer session and one partition. A restarted producer without a transactional ID gets a new PID.

**Transactions** (`transactional.id` set)
1. `initTransactions()` registers with the **transaction coordinator** and fences older instances that use the same `transactional.id` ("zombie fencing").
2. `beginTransaction()`, then produce to any partitions.
3. `sendOffsetsToTransaction()` includes the consumer's input offsets in the same transaction.
4. `commitTransaction()` or `abortTransaction()`. The coordinator writes commit or abort markers to all involved partitions.

**Consumers** set `isolation.level=read_committed` to skip aborted and uncommitted records. The default `read_uncommitted` sees everything.

**Exactly-once in Kafka Streams:** set `processing.guarantee=exactly_once_v2`, and the framework handles transactions for you.

**Limits**
- Guarantees apply to Kafka-to-Kafka flows. Writing to an external database inside the loop is not covered. That sink needs its own idempotency (see [Q62](/interview/data-engineering/#q62)).
- Transactions add latency and throughput overhead, and long-open transactions delay `read_committed` consumers (up to the last stable offset).

**Example**

```python
# Runtime: Python 3.11 with confluent-kafka — transactional read-process-write
from confluent_kafka import Consumer, Producer

consumer = Consumer({"bootstrap.servers": "broker1:9092", "group.id": "enricher",
                     "enable.auto.commit": False, "isolation.level": "read_committed"})
producer = Producer({"bootstrap.servers": "broker1:9092",
                     "transactional.id": "enricher-tx-1"})
consumer.subscribe(["orders-raw"])
producer.init_transactions()

while True:
    msgs = consumer.consume(num_messages=500, timeout=1.0)
    if not msgs:
        continue
    producer.begin_transaction()
    for m in msgs:
        if m.error():
            continue
        producer.produce("orders-enriched", key=m.key(), value=enrich(m.value()))  # enrich: user-defined
    producer.send_offsets_to_transaction(
        consumer.position(consumer.assignment()),
        consumer.consumer_group_metadata())
    producer.commit_transaction()
```
A production version also catches errors, calls `abort_transaction()`, and rewinds the consumer to the last committed offsets.

**Real-world use case**

A fraud-scoring service reads transactions, scores them and writes results to another topic. Kafka transactions ensure a crash never produces duplicate or missing scores, because the output and the input offsets commit together.

**Common mistakes and red flags**

- Claiming idempotent producers give end-to-end exactly-once.
- Forgetting `read_committed` on consumers.
- Assuming transactions cover external database writes.

**Follow-up questions**

- What is zombie fencing? — A new producer instance with the same `transactional.id` bumps the epoch, so the old instance's writes are rejected.
- What is the last stable offset? — The offset up to which all transactions are resolved; `read_committed` consumers don't read past it.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q71" data-level="Advanced">
<summary><span class="qb-num">Q71</span><h3 class="qb-title">Kafka retention vs log compaction</h3><span class="qb-meta">Advanced · Common · Kafka</span></summary>

**Short answer**

Time- or size-based retention (`cleanup.policy=delete`) removes old log segments after a period such as 7 days or once a size limit is reached, regardless of key. Log compaction (`cleanup.policy=compact`) keeps at least the latest record for every key and removes older values. A compacted topic acts like a changelog or snapshot of current state, and a tombstone (null value) deletes a key.

**Detailed explanation**

**Delete retention**
- Settings: `retention.ms`, `retention.bytes`, and segment settings (`segment.ms`, `segment.bytes`). Only *closed* segments are eligible.
- Use for event streams such as clicks or logs, where history beyond the window isn't needed in Kafka.
- **Tiered storage** (GA in Kafka 3.9 open source, and in managed services) offloads old segments to object storage, which makes long retention affordable. That supports Kappa-style replays (see [Q28](/interview/data-engineering/#q28)).

**Compaction**
- A background cleaner rewrites old segments, keeping only the latest value per key. The active segment is never compacted.
- **Tombstones:** a record with a key and a null value marks deletion. It is removed after `delete.retention.ms`, so consumers have time to see it.
- `min.compaction.lag.ms` guarantees that recent records stay uncompacted for a while. `max.compaction.lag.ms` forces compaction within a bound.
- Ordering and offsets are preserved, though offsets become non-contiguous.
- Use for changelogs: CDC topics where consumers need current state, Kafka Streams state-store changelogs, and configuration or reference data.

**Combined:** `cleanup.policy=compact,delete` compacts and also removes data older than retention.

| Policy | Keeps | Good for |
|---|---|---|
| delete | Everything within time/size window | Event streams |
| compact | Latest value per key (forever) | Current-state changelogs, CDC |
| compact,delete | Latest per key within window | Bounded changelogs |

**Example**

```bash
# Create a compacted topic for customer current state (Kafka CLI)
kafka-topics.sh --bootstrap-server broker1:9092 --create \
  --topic customers.current \
  --partitions 12 --replication-factor 3 \
  --config cleanup.policy=compact \
  --config min.compaction.lag.ms=3600000 \
  --config delete.retention.ms=86400000
```

**Real-world use case**

A new microservice bootstraps its customer cache by reading a compacted `customers.current` topic from the beginning. It gets one record per live customer instead of years of change history, then keeps consuming updates.

**Common mistakes and red flags**

- Expecting compaction to happen immediately.
- Using compaction on unkeyed topics, where it does nothing useful.
- Thinking consumers can't miss tombstones. They can if offline longer than `delete.retention.ms`.

**Follow-up questions**

- Does compaction guarantee only one record per key? — No; it guarantees *at least* the latest, and duplicates can exist until the cleaner runs.
- How do you replay a year of events cheaply? — Tiered storage with long retention, or archive topics to the lake and replay from there.

**In depth:** [What is Kafka log compaction, and when would you use it?](/interview/kafka/log-compaction-use-cases/)

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q72" data-level="Intermediate">
<summary><span class="qb-num">Q72</span><h3 class="qb-title">What is a schema registry, and what are compatibility modes?</h3><span class="qb-meta">Intermediate · Common · Kafka</span></summary>

**Short answer**

A schema registry is a central service that stores versioned schemas (Avro, Protobuf, JSON Schema) for Kafka topics. Messages carry only a small schema ID. On each new version, the registry checks compatibility: backward (new readers can read old data), forward (old readers can read new data) or full (both).

**Detailed explanation**

**How it works**
1. A producer serialiser registers or looks up the schema under a **subject** (by default `<topic>-value` and `<topic>-key`).
2. The registry returns an ID, and the message is written as a magic byte, the schema ID and the binary payload.
3. Consumers fetch the schema by ID (cached) and deserialise.

**Compatibility modes** (examples use Avro rules)
| Mode | Guarantee | Allowed change examples | Upgrade order |
|---|---|---|---|
| BACKWARD (common default) | Consumers on new schema read data written with previous schema | Add field *with default*, delete field | Consumers first |
| FORWARD | Consumers on old schema read data written with new schema | Add field, delete field *with default* | Producers first |
| FULL | Both directions | Add/delete only fields with defaults | Any order |
| *_TRANSITIVE | Checked against **all** previous versions, not just the last | Same rules, stricter | — |
| NONE | No checks | Anything | Risky |

**Why it matters for data engineering**
- It blocks breaking changes at *produce* time instead of breaking pipelines at 3 AM (see [Q38](/interview/data-engineering/#q38)).
- It gives a self-documenting catalogue of event schemas.
- It enables automatic schema evolution in sinks (for example, a new optional field flowing into a lakehouse table).

**Implementations:** Confluent Schema Registry, AWS Glue Schema Registry, Azure Schema Registry (Event Hubs), Apicurio, Redpanda's built-in registry.

**Example**

```json
{
  "type": "record",
  "name": "OrderPlaced",
  "namespace": "com.shop.events",
  "fields": [
    {"name": "order_id", "type": "string"},
    {"name": "amount", "type": {"type": "bytes", "logicalType": "decimal", "precision": 12, "scale": 2}},
    {"name": "channel", "type": ["null", "string"], "default": null}
  ]
}
```
Adding `channel` as an optional field with a default is a backward-compatible change.

**Real-world use case**

A producer team tried to rename `amount` to `total_amount`. With BACKWARD compatibility, the registry rejected the new schema at deploy time. They added `total_amount` as a new optional field instead and deprecated `amount` over a release cycle.

**Common mistakes and red flags**

- Setting compatibility to NONE to "unblock" a deploy.
- Adding required fields without defaults.
- Not knowing which side must upgrade first for each mode.

**Follow-up questions**

- Avro vs Protobuf in a registry? — Both work; Protobuf is popular with gRPC-heavy services, Avro with data platforms; compatibility rules differ slightly.
- How do lakehouse sinks use it? — Connectors read the schema to create and evolve target table columns automatically.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q73" data-level="Intermediate">
<summary><span class="qb-num">Q73</span><h3 class="qb-title">Kafka vs Kinesis vs Pub/Sub vs Event Hubs</h3><span class="qb-meta">Intermediate · Occasional · Kafka</span></summary>

**Short answer**

All four are durable, scalable event-streaming services. Kafka is open source and the most flexible, self-managed or managed (Confluent, Amazon MSK, others). Kinesis Data Streams, Google Pub/Sub and Azure Event Hubs are cloud-native managed services with less operational work but provider-specific limits and APIs. Event Hubs also exposes a Kafka-compatible endpoint.

**Detailed explanation**

| Aspect | Apache Kafka | AWS Kinesis Data Streams | Google Cloud Pub/Sub | Azure Event Hubs |
|---|---|---|---|---|
| Model | Partitioned log | Shards (partitioned log) | Topic + subscriptions (message queue semantics) | Partitioned log |
| Ordering | Per partition | Per shard | Optional, per ordering key | Per partition |
| Replay | Offset/time based within retention | Within retention (up to 365 days) | Seek to timestamp/snapshot (retention configurable) | Within retention; Capture to storage |
| Scaling unit | Partitions/brokers | Shards or on-demand mode | Automatic | Throughput/processing units, partitions |
| Consumer model | Consumer groups, pull | Pull (KCL) or enhanced fan-out push | Pull or push subscriptions | Consumer groups (Kafka API or AMQP) |
| Ops burden | High if self-managed; lower with MSK/Confluent | Low | Very low | Low |
| Ecosystem | Kafka Connect, Streams, ksqlDB, broad | AWS services (Lambda, Firehose) | GCP services (Dataflow, BigQuery subscriptions) | Azure services; Kafka protocol support |

**How to choose**
- **Multi-cloud or portability, rich connector ecosystem, exactly-once Kafka Streams:** Kafka, usually managed.
- **All-in on one cloud with modest needs and no operations team:** the native service.
- **Global fan-out with many independent subscribers and no partition management:** Pub/Sub.
- **Azure shop with Kafka clients:** Event Hubs' Kafka endpoint, so existing clients connect with configuration changes.

Retention limits, per-shard or per-partition throughput quotas and pricing details change over time. Check current provider limits before quoting numbers.

**Real-world use case**

A retailer on AWS chose Kinesis with Firehose for clickstream landing (minimal operations). A separate CDC platform that needed Debezium and Kafka Connect used Amazon MSK.

**Common mistakes and red flags**

- Claiming Pub/Sub orders all messages by default.
- Ignoring operational cost when recommending self-managed Kafka.
- Assuming the APIs are interchangeable.

**Follow-up questions**

- What is Kinesis Data Firehose (now Amazon Data Firehose)? — A managed delivery service that batches streams into S3, Redshift, Snowflake and others with no consumer code.
- When is a queue (SQS) better than a log? — For task distribution where each message is processed once and replay isn't needed.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q74" data-level="Beginner">
<summary><span class="qb-num">Q74</span><h3 class="qb-title">Map the core AWS data services (S3, Glue, EMR, Redshift, Kinesis, Athena)</h3><span class="qb-meta">Beginner · Very Common · Cloud Data Services</span></summary>

**Short answer**

On AWS, S3 is the data lake storage. Glue provides the Data Catalog, crawlers and serverless Spark ETL. EMR runs managed Spark and Hadoop-ecosystem clusters. Redshift is the data warehouse. Athena runs serverless SQL over S3. Kinesis (and MSK) handle streaming. Lake Formation adds fine-grained lake permissions.

**Detailed explanation**

| Layer | Service | What it does |
|---|---|---|
| Storage | Amazon S3 | Object storage for raw/curated data; storage classes and lifecycle rules; S3 Tables (managed Iceberg) |
| Catalog / governance | AWS Glue Data Catalog, Lake Formation | Central metastore of tables; Lake Formation adds row/column-level permissions and tag-based access |
| Batch processing | AWS Glue jobs | Serverless Spark/Python ETL with auto-provisioning |
| Batch processing | Amazon EMR (on EC2, EKS, Serverless) | Managed Spark, Hive, Trino, Flink clusters with more control |
| Warehouse | Amazon Redshift (provisioned, Serverless) | MPP columnar warehouse; Spectrum queries S3 data |
| Ad-hoc SQL on lake | Amazon Athena | Serverless Trino-based SQL; pay per data scanned |
| Streaming | Kinesis Data Streams, Amazon Data Firehose, Amazon MSK, Managed Service for Apache Flink | Ingest, deliver and process streams |
| Database migration / CDC | AWS DMS | Replicate databases, full load + CDC |
| Orchestration | Amazon MWAA, Step Functions | Managed Airflow; serverless state machines |
| Security | IAM, KMS, Secrets Manager | Access, encryption keys, credential storage |

**A typical AWS reference pipeline:** DMS or MSK feeds raw data into S3 (bronze), Glue or EMR Spark transforms it into Iceberg or Parquet (silver/gold) registered in the Glue Catalog, Athena and Redshift serve SQL, and MWAA orchestrates.

**Real-world use case**

A logistics company lands IoT telemetry via Kinesis and Firehose into S3, runs hourly Glue jobs to curate Iceberg tables, and lets analysts query with Athena while finance uses Redshift. Lake Formation controls who can see driver PII.

**Common mistakes and red flags**

- Confusing Glue the catalog with Glue the ETL service.
- Not knowing Athena charges by data scanned, so formats and partitioning matter.
- Listing services with no explanation of how they connect.

**Follow-up questions**

- Glue vs EMR? — Glue for serverless, simpler jobs; EMR for heavy, tunable, long-running or non-Spark workloads.
- How do you reduce Athena cost? — Columnar formats, partitioning or clustering, compression, and selecting only needed columns.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q75" data-level="Beginner">
<summary><span class="qb-num">Q75</span><h3 class="qb-title">Map the core Azure data services (ADLS Gen2, ADF, Synapse/Fabric, Event Hubs)</h3><span class="qb-meta">Beginner · Common · Cloud Data Services</span></summary>

**Short answer**

On Azure, ADLS Gen2 is the lake storage. Azure Data Factory handles ingestion and orchestration pipelines. Azure Databricks and Synapse Spark provide Spark processing. Synapse and Microsoft Fabric provide warehousing and analytics, with Fabric's OneLake as a unified lake. Event Hubs handles streaming, and Microsoft Purview handles governance.

**Detailed explanation**

| Layer | Service | What it does |
|---|---|---|
| Storage | ADLS Gen2 | Blob storage with hierarchical namespace; the standard lake on Azure |
| Ingestion / orchestration | Azure Data Factory (ADF) | 90+ connectors, copy activities, mapping data flows, scheduling |
| Processing | Azure Databricks | Spark lakehouse (first-party partnership service) |
| Analytics platform | Azure Synapse Analytics | Dedicated/serverless SQL pools, Spark pools, pipelines |
| Unified SaaS analytics | Microsoft Fabric | OneLake storage, Lakehouse, Warehouse, Data Factory, Real-Time Intelligence, Power BI in one capacity model |
| Streaming | Azure Event Hubs, Stream Analytics | Event ingestion (Kafka-compatible); SQL-based stream processing |
| Governance | Microsoft Purview | Catalog, lineage, classification |
| Security | Entra ID, Key Vault | Identity; secrets and keys |

**Direction of travel:** Microsoft positions Fabric as the strategic analytics platform. Many Synapse capabilities have Fabric equivalents, and new investment focuses there. Existing Synapse and ADF estates remain common in interviews, so knowing both is valuable.

**A typical Azure reference pipeline:** ADF (or Fabric Data Factory) copies data from on-prem and SaaS sources into ADLS Gen2 bronze, Azure Databricks or Fabric Spark builds Delta silver and gold tables, a SQL warehouse layer serves BI, and Power BI sits on top. Identity is handled through Entra ID with managed identities, so pipelines need no stored passwords. Secrets that must exist live in Key Vault. Purview scans the estate for classification and lineage. Private endpoints keep traffic off the public internet, which regulated customers usually require.

**Real-world use case**

An insurer uses ADF to copy on-prem SQL Server tables into ADLS Gen2 nightly, Azure Databricks to build Delta silver and gold tables, and Power BI for reporting. Purview tracks lineage from source to report.

**Common mistakes and red flags**

- Confusing ADLS Gen1 (retired) with Gen2.
- Calling ADF a transformation engine for heavy logic. It orchestrates, and its data flows run on Spark under the hood.
- Not knowing Fabric exists.

**Follow-up questions**

- What is OneLake? — Fabric's single, tenant-wide logical lake built on ADLS, storing tables in Delta format.
- How do you run on-prem extracts with ADF? — With a self-hosted integration runtime installed inside the network.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q76" data-level="Beginner">
<summary><span class="qb-num">Q76</span><h3 class="qb-title">Map the core GCP data services (GCS, BigQuery, Dataflow, Dataproc, Pub/Sub, Composer)</h3><span class="qb-meta">Beginner · Common · Cloud Data Services</span></summary>

**Short answer**

On GCP, Cloud Storage (GCS) is the lake storage. BigQuery is the serverless data warehouse. Dataflow runs Apache Beam pipelines for batch and streaming. Dataproc runs managed Spark and Hadoop. Pub/Sub handles messaging. Cloud Composer is managed Airflow. Dataplex and BigLake provide governance and open-table access.

**Detailed explanation**

| Layer | Service | What it does |
|---|---|---|
| Storage | Cloud Storage (GCS) | Object storage with storage classes and lifecycle |
| Warehouse | BigQuery | Serverless columnar warehouse; on-demand (per TB scanned) or capacity (slots) pricing; partitioning and clustering; BigQuery ML |
| Open tables | BigLake | Governed access to Iceberg/Parquet in GCS from BigQuery and Spark |
| Stream/batch processing | Dataflow | Fully managed Apache Beam runner; autoscaling; unified batch and streaming |
| Spark | Dataproc (clusters, Serverless) | Managed Spark/Hadoop |
| Messaging | Pub/Sub | Global messaging; BigQuery and GCS subscriptions for direct delivery |
| CDC | Datastream | Serverless CDC from MySQL, PostgreSQL, Oracle, SQL Server into BigQuery/GCS |
| Orchestration | Cloud Composer | Managed Airflow |
| Governance | Dataplex (incl. Data Catalog features) | Discovery, lineage, data quality, policies |

**Notable BigQuery points for interviews**
- Separation of storage and compute. Slots are the compute units.
- Partitioned tables (by date or integer range) plus clustering (up to four columns) to cut bytes scanned.
- On-demand pricing charges per bytes processed, so `SELECT *` on wide tables is costly.

**Real-world use case**

A media company sends app events via Pub/Sub with a BigQuery subscription straight into a raw table. Dataflow handles complex streaming enrichment, and Composer runs nightly dbt models in BigQuery.

**Common mistakes and red flags**

- Thinking BigQuery has indexes for performance (clustering and partitioning do that job).
- Writing `SELECT *` in BigQuery cost discussions.
- Not knowing Dataflow means Apache Beam.

**Follow-up questions**

- Dataflow vs Dataproc? — Dataflow for serverless Beam pipelines, especially streaming; Dataproc for existing Spark code and more control.
- How do you limit BigQuery cost? — Partition filters (optionally required), clustering, selecting columns, and slot reservations or quotas.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q77" data-level="Beginner">
<summary><span class="qb-num">Q77</span><h3 class="qb-title">Object vs block vs file storage</h3><span class="qb-meta">Beginner · Common · Cloud Data Services</span></summary>

**Short answer**

Object storage (S3, ADLS, GCS) stores immutable objects addressed by key over an HTTP API. It is massively scalable and cheap, and it is the foundation of data lakes. Block storage (EBS, managed disks) presents raw volumes to one machine for low-latency databases. File storage (EFS, Azure Files, Filestore) offers a shared POSIX file system to many machines.

**Detailed explanation**

| Aspect | Object | Block | File |
|---|---|---|---|
| Access | HTTP API (PUT/GET/LIST) by key | Mounted as a disk volume | Mounted network file system (NFS/SMB) |
| Mutability | Whole-object replace (no in-place edits) | Byte-level read/write | File-level read/write |
| Scale | Virtually unlimited | Per volume limits | Large, shared |
| Latency | Higher per request (ms) | Lowest (sub-ms) | Low–moderate |
| Sharing | Any number of clients | Usually one instance | Many instances |
| Cost | Lowest per GB | Higher | Highest |
| Data use | Data lakes, backups, table formats | Database storage, local Spark shuffle/scratch | Shared configs, legacy apps, some ML training |

**Why lakes use object storage**
- Cheap, durable (designed for 11 nines of durability on S3), and elastic.
- Compute and storage scale independently.
- Every engine can read it.

**Constraints to design around**
- No atomic rename of directories, and listing can be slow. Table formats solve commit semantics with metadata (see [Q33](/interview/data-engineering/#q33)).
- Per-request costs and latency, which make small files expensive (see [Q57](/interview/data-engineering/#q57)).
- Strong read-after-write consistency now on S3 (since 2020), GCS and ADLS.

**Real-world use case**

A Spark cluster stores tables in S3 (object), uses local NVMe or EBS for shuffle spill (block), and mounts EFS (file) only for a legacy model-scoring library that expects a shared directory.

**Common mistakes and red flags**

- Treating S3 like a file system with cheap renames.
- Putting a transactional database on object storage.
- Not knowing why ADLS Gen2's hierarchical namespace matters (atomic directory operations).

**Follow-up questions**

- What is the hierarchical namespace in ADLS Gen2? — It adds real directories with atomic rename and ACLs on top of Blob storage.
- Why is listing slow? — Object stores are flat key spaces; listing a prefix with millions of keys requires many paginated API calls.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q78" data-level="Intermediate">
<summary><span class="qb-num">Q78</span><h3 class="qb-title">Serverless vs provisioned compute for data workloads</h3><span class="qb-meta">Intermediate · Common · Cloud Data Services</span></summary>

**Short answer**

Serverless compute is allocated per job or query and billed for what you use, with the provider managing capacity and start-up. Examples: BigQuery on-demand, Athena, Glue, Databricks serverless, Snowflake serverless features. Provisioned compute means you size and run clusters or warehouses yourself. Serverless suits spiky or unpredictable workloads. Provisioned can be cheaper for steady, heavy usage.

**Detailed explanation**

| Aspect | Serverless | Provisioned |
|---|---|---|
| Capacity management | Provider | You (sizes, autoscaling, pools) |
| Start-up | Seconds or instant | Minutes for clusters (unless pools/warm) |
| Billing | Per query/job/second of use | Per running time, regardless of utilisation |
| Control | Limited (instance types, configs) | Full (versions, libraries, networking) |
| Cost profile | Great for bursty/low-utilisation; can be pricey at sustained high use | Efficient when highly utilised; waste when idle |
| Ops | Minimal | Patching, sizing, monitoring |

**Considerations**
- **Utilisation:** a cluster busy 20 hours a day may be cheaper provisioned, especially with reserved or committed pricing. Ten jobs a day of 5 minutes each favour serverless.
- **Latency requirements:** serverless removes cluster start-up, which matters for interactive and short jobs.
- **Governance and networking:** some serverless options restrict private networking or custom libraries. Check before committing.
- **Cost visibility:** serverless can surprise you. Set budgets, quotas and alerts (see [Q93](/interview/data-engineering/#q93)).
- Many platforms are **hybrid**: Snowflake warehouses are provisioned, but suspend when idle and bill per second after the first 60 seconds. Snowpipe and serverless tasks are serverless.

**Real-world use case**

A team moved 300 small daily Databricks jobs from classic job clusters, each waiting about 5 minutes to start, to serverless jobs. Total runtime fell and cost was roughly flat. Their 18-hour-a-day streaming job stayed on provisioned compute because that was cheaper.

**Common mistakes and red flags**

- "Serverless is always cheaper."
- Ignoring start-up latency when choosing provisioned compute for short jobs.
- No cost guardrails on serverless.

**Follow-up questions**

- How do you compare costs? — Model expected usage hours and data volumes per option, then validate with a trial run and billing data.
- What is a warm pool? — Pre-started instances (for example Databricks instance pools) that cut cluster start-up time.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q79" data-level="Intermediate">
<summary><span class="qb-num">Q79</span><h3 class="qb-title">AWS Glue: crawlers, Data Catalog and Glue jobs</h3><span class="qb-meta">Intermediate · Common · Cloud Data Services</span></summary>

**Short answer**

The Glue Data Catalog is a Hive-compatible metastore of databases and tables used by Athena, EMR, Redshift Spectrum and Glue jobs. Crawlers scan data in S3 or databases, infer schemas and create or update catalog tables. Glue jobs run serverless Spark (or Python shell or Ray) ETL, with job bookmarks for incremental processing.

**Detailed explanation**

**Data Catalog**
- Stores table definitions: location, format, schema, partitions. It also supports Iceberg, Delta and Hudi table metadata.
- Shared across AWS analytics services, so one definition serves Athena and EMR.
- Lake Formation layers fine-grained permissions on top of it.

**Crawlers**
- Point at an S3 path or JDBC source, infer schema with classifiers, and detect new partitions.
- Useful for discovery and evolving raw zones.
- Pitfalls: unexpected schema inference (mixed types in JSON), crawlers creating many tables when folder layouts are inconsistent, and cost if they run too often. For well-known tables, define the schema explicitly (DDL or IaC) and add partitions programmatically or through partition projection.

**Glue jobs**
- Spark-based ETL with DPUs (data processing units) as the billing unit, plus auto scaling and Flex execution for cheaper non-urgent jobs.
- **DynamicFrames:** Glue's schema-flexible wrapper around DataFrames, with transforms like `ResolveChoice`. Many teams just use Spark DataFrames.
- **Job bookmarks** track processed data (files or JDBC keys) to support incremental runs.
- **Glue Data Quality** offers rule-based checks (DQDL).
- **Glue Studio** for visual authoring. **Interactive sessions** for notebooks.

**Example**

```python
# Runtime: AWS Glue 4.0/5.0 job (PySpark 3.x) — read catalog table, write Parquet partitioned by date
import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
glue_ctx = GlueContext(SparkContext.getOrCreate())
job = Job(glue_ctx)
job.init(args["JOB_NAME"], args)

orders = glue_ctx.create_dynamic_frame.from_catalog(
    database="bronze", table_name="orders_raw",
    transformation_ctx="orders_src")          # transformation_ctx enables bookmarks

df = orders.toDF().withColumn("order_date", F.to_date("order_ts"))
df.write.mode("append").partitionBy("order_date").parquet("s3://lake/silver/orders/")

job.commit()                                   # persists bookmark state
```

**Real-world use case**

A team replaced hourly crawlers on a 10,000-partition table, which cost money and sometimes changed column types, with partition projection in Athena and a fixed DDL. That cut cost and eliminated surprise schema changes.

**Common mistakes and red flags**

- Running crawlers on every load for stable, known schemas.
- Forgetting `job.commit()`, so bookmarks never advance.
- Not knowing what a DPU is.

**Follow-up questions**

- What is partition projection? — Athena computes partition locations from configured patterns instead of reading them from the catalog, which suits highly partitioned tables.
- Glue vs Databricks on AWS? — Glue is simpler and serverless for moderate ETL; Databricks offers a fuller lakehouse platform with richer tooling.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q80" data-level="Beginner">
<summary><span class="qb-num">Q80</span><h3 class="qb-title">What is data governance? Catalog, ownership, stewardship</h3><span class="qb-meta">Beginner · Common · Governance &amp; Security</span></summary>

**Short answer**

Data governance is the set of policies, roles and tools that make data findable, trustworthy, secure and compliant. It covers who owns each dataset, how it is defined and classified, who may access it, its quality standards, its lineage and how long it is kept. Data engineers implement much of it in the platform.

**Detailed explanation**

**Core building blocks**
- **Ownership:** every dataset has an accountable **owner** (business) and a **technical owner** (engineering team).
- **Stewardship:** **data stewards** maintain definitions, resolve quality issues and approve access.
- **Catalog:** searchable metadata such as descriptions, schemas, owners, tags, popularity and lineage. Tools: Unity Catalog, Snowflake Horizon, Microsoft Purview, Google Dataplex, Collibra, Alation, Atlan, DataHub, OpenMetadata.
- **Business glossary:** agreed definitions ("active customer", "net revenue") linked to tables and metrics.
- **Classification and tagging:** PII, PHI, confidential and public tags. These drive masking and access policies (see [Q82](/interview/data-engineering/#q82), [Q183](/interview/snowflake/#q183)).
- **Access control:** RBAC or ABAC policies, least privilege, access reviews (see [Q81](/interview/data-engineering/#q81)).
- **Quality standards:** SLAs and checks (see [Q35](/interview/data-engineering/#q35)–[Q38](/interview/data-engineering/#q38)).
- **Lineage:** source-to-report traceability (see [Q84](/interview/data-engineering/#q84)).
- **Retention and lifecycle:** legal holds, deletion schedules, privacy requests (see [Q85](/interview/data-engineering/#q85)).
- **Auditing:** who accessed what and when.

**Data engineer's role**
- Build governance *into* pipelines: register tables in the catalog, apply tags at creation, enforce masking by policy, emit lineage, and automate access requests through code (IaC, grants in version control).
- Make the governed path the easiest path, so people don't bypass it.

**Real-world use case**

A healthcare company tags PHI columns at ingestion. Masking policies keyed on the tag automatically hide those values from non-clinical roles in every downstream table, and Purview lineage proves to auditors which reports use PHI.

**Common mistakes and red flags**

- Treating governance as just "a catalog tool".
- Governance with no owners or enforcement.
- Seeing governance purely as a blocker rather than an enabler of trust and self-service.

**Follow-up questions**

- How do you keep catalog descriptions current? — Generate them from code (dbt docs, YAML) and make documentation part of code review.
- What is a data owner vs a data steward? — The owner is accountable for the data and decisions about it; the steward manages its definitions and quality day to day.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q81" data-level="Intermediate">
<summary><span class="qb-num">Q81</span><h3 class="qb-title">RBAC vs ABAC</h3><span class="qb-meta">Intermediate · Common · Governance &amp; Security</span></summary>

**Short answer**

RBAC (role-based access control) grants privileges to roles, and users get access by being assigned roles. ABAC (attribute-based access control) decides access dynamically from attributes of the user, the data and the context, such as department, data classification tag or region. RBAC is simpler to reason about. ABAC scales better when rules depend on many combinations.

**Detailed explanation**

**RBAC**
- Example: role `ANALYST_FINANCE` has `SELECT` on schema `finance_gold`, and Priya is granted that role.
- Hierarchies let roles inherit other roles (Snowflake role hierarchy; see [Q179](/interview/snowflake/#q179)).
- Pros: clear, auditable, supported everywhere.
- Cons: **role explosion**. Combining regions × departments × sensitivity levels can create hundreds of roles.

**ABAC**
- A policy evaluates attributes at query time. For example: "users can see rows where `row.region` is in `user.allowed_regions`", or "columns tagged `PII` are masked unless the user is in group `pii_readers`".
- Implemented with **tag-based policies** (Unity Catalog ABAC policies, Snowflake tag-based masking, AWS Lake Formation LF-tags), row access policies and masking functions that read session or user attributes.
- Pros: few policies cover many tables, and new tables are protected automatically once tagged.
- Cons: harder to debug ("why can't I see this row?"), and it depends on correct, complete tagging.

| Aspect | RBAC | ABAC |
|---|---|---|
| Decision basis | Role membership | Attributes (user, data, context) |
| Granularity | Object-level mostly | Row/column/conditional |
| Scale | Role explosion risk | Policy reuse via tags |
| Auditability | Simple | Needs good tooling |
| Typical use | Base object access | Fine-grained and dynamic rules |

Most platforms use **RBAC for object access plus ABAC-style policies for row and column security**.

**Example**

```sql
-- Dialect: Snowflake SQL
-- RBAC: object-level grant
GRANT SELECT ON ALL TABLES IN SCHEMA analytics.finance_gold TO ROLE analyst_finance;

-- ABAC-style: mask any column tagged PII unless the role is allowed
CREATE OR REPLACE MASKING POLICY mask_pii_string AS (val STRING) RETURNS STRING ->
  CASE WHEN IS_ROLE_IN_SESSION('PII_READER') THEN val ELSE '***MASKED***' END;

ALTER TAG governance.tags.pii SET MASKING POLICY mask_pii_string;
```

**Real-world use case**

A global company had 400 regional roles to restrict sales data. Replacing them with one row access policy driven by a user-to-region mapping table cut the number of roles sharply and made onboarding a single mapping-table insert.

**Common mistakes and red flags**

- Granting privileges directly to users.
- Proposing ABAC without a reliable tagging process.
- Not knowing that both models usually coexist.

**Follow-up questions**

- What is least privilege? — Grant only the minimum access required, for the minimum time, reviewed regularly.
- How do you audit access? — Platform access-history views (Snowflake ACCOUNT_USAGE.ACCESS_HISTORY, Unity Catalog audit system tables).

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q82" data-level="Intermediate">
<summary><span class="qb-num">Q82</span><h3 class="qb-title">How do you protect PII: masking, tokenisation, hashing, encryption</h3><span class="qb-meta">Intermediate · Very Common · Governance &amp; Security</span></summary>

**Short answer**

Minimise and classify PII (personally identifiable information) first. Then combine dynamic masking (hide by role), tokenisation (reversible via a vault), keyed hashing (irreversible, joinable pseudonyms) and encryption, under strict access control and auditing.

**Detailed explanation**

| Technique | Reversible? | Joinable? | Typical use |
|---|---|---|---|
| Dynamic masking | Underlying value intact; only display changes | Yes (for authorised roles) | Analysts see `***`, support sees full email |
| Static masking / redaction | No | No | Non-prod copies, test data |
| Tokenisation | Yes (via token vault/service) | Yes (deterministic tokens) | Payment data (PCI), re-identification by authorised service |
| Keyed hashing (HMAC) | No | Yes (same input → same hash) | Pseudonymous IDs for analytics joins |
| Column/field encryption | Yes (with key) | Only if deterministic | Highly sensitive fields; keys managed in KMS |
| Generalisation / aggregation | No | Partially | Age bands, ZIP3, k-anonymity for sharing |

**Principles**
1. **Data minimisation:** don't ingest PII you don't need.
2. **Classify early:** tag PII at ingestion, using automated classification plus review.
3. **Separate identity from behaviour:** keep a restricted identity table keyed by a pseudonymous ID, and keep analytics tables free of direct identifiers.
4. **Use keyed hashes, not plain hashes.** Unsalted MD5 or SHA of an email or phone number can be reversed by brute force or lookup tables, because the input space is small.
5. **Protect non-prod:** never copy raw production PII to dev. Use masked or synthetic data.
6. **Plan for deletion** (GDPR/CCPA). Pseudonymisation does not remove the obligation (see [Q85](/interview/data-engineering/#q85)).

**Example**

```sql
-- Dialect: Snowflake SQL
-- Pseudonymous, joinable ID with a secret key (keep the key in a secured secret/UDF, not in code)
SELECT
    SHA2(CONCAT(email_normalized, '|', $pii_hash_pepper), 256) AS customer_pid,
    signup_date,
    country
FROM (
    SELECT LOWER(TRIM(email)) AS email_normalized, signup_date, country
    FROM raw.customers
);
```
In production, put the pepper in a secured function or secret with restricted access rather than a session variable, so analysts can't read it.

**Real-world use case**

A marketplace replaced emails in its event tables with an HMAC-based customer pseudonym. Only the identity service can map the pseudonym back to an email. Analysts join across datasets freely, and a breach of the analytics tables would not expose emails.

**Common mistakes and red flags**

- Calling an unsalted hash of an email "anonymised".
- Masking in the BI tool only, leaving raw tables open.
- Copying production PII into development environments.

**Follow-up questions**

- Is pseudonymised data still personal data under GDPR? — Yes; only truly anonymised data falls outside it.
- Tokenisation vs encryption? — Tokens have no mathematical relationship to the value (lookup via vault); encryption is reversible with the key.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q83" data-level="Beginner">
<summary><span class="qb-num">Q83</span><h3 class="qb-title">Encryption at rest and in transit, and key management</h3><span class="qb-meta">Beginner · Common · Governance &amp; Security</span></summary>

**Short answer**

Encryption in transit (TLS) protects data moving over networks. Encryption at rest protects stored data on disks and in object storage. Keys are managed in a key management service (AWS KMS, Azure Key Vault, Google Cloud KMS), usually with envelope encryption and rotation. For extra control, some platforms support customer-managed keys.

**Detailed explanation**

**In transit**
- TLS (1.2+) on all connections: JDBC/ODBC drivers, APIs, Kafka (SSL listeners), storage endpoints.
- Enforce TLS-only bucket policies, and private networking (PrivateLink, private endpoints) to keep traffic off the public internet.

**At rest**
- Cloud object storage and managed warehouses encrypt by default (AES-256 is typical).
- Options: provider-managed keys (default), **customer-managed keys (CMK)** in the provider's KMS, or customer-supplied or external keys for strict regimes.
- **Envelope encryption:** each data object is encrypted with a data encryption key (DEK), and the DEK is encrypted with a key encryption key (KEK) stored in KMS. Rotating the KEK doesn't require re-encrypting all data.
- Platform examples: Snowflake encrypts all data with a hierarchical key model and offers **Tri-Secret Secure** (customer-managed key combined with Snowflake's key) on Business Critical edition. Databricks supports customer-managed keys for managed services and workspace storage.

**Key management practices**
- Separation of duties: key administrators are not data administrators.
- Automatic rotation (for example yearly for KMS keys), audit logs of key usage, and least-privilege key policies.
- Revoking a CMK can make data unreadable, which is a deliberate "kill switch" but also a risk if done by mistake.

**Column-level encryption** adds protection beyond disk encryption, because disk encryption doesn't stop a user with table access from reading values (see [Q82](/interview/data-engineering/#q82)).

**Real-world use case**

A bank requires CMKs for its lakehouse storage, with keys in its own KMS account, so it can revoke access to all data instantly in a security incident. TLS plus private endpoints secure all warehouse connections.

**Common mistakes and red flags**

- Thinking at-rest encryption protects against authorised users misusing data.
- Hardcoding encryption keys in code.
- Not knowing what envelope encryption is.

**Follow-up questions**

- What does key rotation do to existing data? — With envelope encryption, old DEKs are re-wrapped or remain decryptable through retained key versions; data isn't necessarily rewritten.
- Is S3 encrypted by default? — Yes; since January 2023, new objects get server-side encryption (SSE-S3) by default.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q84" data-level="Intermediate">
<summary><span class="qb-num">Q84</span><h3 class="qb-title">What is data lineage, and how is it captured?</h3><span class="qb-meta">Intermediate · Common · Governance &amp; Security</span></summary>

**Short answer**

Data lineage traces how data flows from sources through transformations to consumers, at table or column level. It is captured from query logs and SQL parsing, from orchestrator and engine integrations (OpenLineage), from transformation tools (dbt's DAG), or natively by platforms like Unity Catalog and Snowflake. You use it for impact analysis, root-cause debugging and compliance.

**Detailed explanation**

**Levels**
- **Table-level:** `raw.orders → silver.orders → gold.fct_revenue → Revenue dashboard`.
- **Column-level:** `gold.fct_revenue.net_amount` is derived from `silver.orders.amount - silver.orders.discount`.

**Capture methods**
| Method | How | Pros | Cons |
|---|---|---|---|
| Native platform | Engine records reads/writes per query (Unity Catalog lineage system tables, Snowflake ACCESS_HISTORY / lineage in Horizon) | Automatic, accurate within platform | Stops at platform boundary |
| SQL parsing of query history | Parse executed SQL to infer dependencies | Works across many tools | Dynamic SQL and UDFs are hard |
| OpenLineage events | Airflow, Spark, dbt, Flink emit standard run/job/dataset events to a backend (Marquez, catalogs) | Cross-system, open standard | Needs integration per tool |
| Transformation tool metadata | dbt manifest builds the model DAG | Precise for dbt models | Only within dbt |
| Manual documentation | Diagrams, catalog entries | Covers anything | Goes stale quickly |

**Uses**
- **Impact analysis:** "If I drop this column, which dashboards break?"
- **Root cause:** "This KPI is wrong. Which upstream table changed?"
- **Compliance:** show where PII flows, and prove report numbers trace to approved sources.
- **Cost and cleanup:** find unused tables with no downstream consumers.

**Real-world use case**

Before changing the definition of `customer_status`, a team checked column-level lineage and found 14 downstream models and 6 dashboards. They coordinated the change with owners instead of discovering breakages after deploy.

**Common mistakes and red flags**

- Relying on hand-drawn diagrams.
- Having lineage only inside one tool, so cross-system flows are invisible.
- Not mentioning column-level lineage for impact analysis.

**Follow-up questions**

- What is OpenLineage? — An open specification for emitting lineage events (job, run, input and output datasets) from data tools.
- How does lineage help incident response? — It shows downstream blast radius for notification, and upstream candidates for root cause.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q85" data-level="Advanced">
<summary><span class="qb-num">Q85</span><h3 class="qb-title">How does GDPR &quot;right to erasure&quot; affect lake and warehouse design?</h3><span class="qb-meta">Advanced · Common · Governance &amp; Security</span></summary>

**Short answer**

The right to erasure (GDPR Article 17) means you must be able to find and delete a person's personal data across every copy within the legal deadline. That includes raw, derived, history and time-travel versions, and backups by their documented process. Design for it with central identity mapping, minimal PII spread, partitioning or keys that make targeted deletes cheap, and retention limits on history.

**Detailed explanation**

**Why it's hard in data platforms**
- PII is copied into many layers and tables.
- Files are immutable, so deleting one person's rows means rewriting files.
- History features (time travel, snapshots, Kafka retention, backups) keep old versions.
- Derived aggregates may or may not contain personal data.

**Design patterns**
1. **Isolate identity.** Keep direct identifiers in one restricted identity table keyed by a pseudonymous ID. Analytical tables carry only the pseudonym.
2. **Crypto-shredding.** Encrypt each person's PII with a per-subject key, and erasure deletes the key. This works well with immutable logs and backups. Confirm with legal counsel that it meets your obligations.
3. **Know where PII lives:** classification tags plus lineage (see [Q80](/interview/data-engineering/#q80), [Q84](/interview/data-engineering/#q84)).
4. **Make deletes efficient:** table formats with row-level deletes (Delta, Iceberg). Cluster by subject ID where deletion volume is high. Batch requests daily.
5. **Purge history:** after deleting, run `VACUUM` (Delta) or expire snapshots (Iceberg), and set Time Travel retention so old versions age out within the legal window. Remember that Snowflake Fail-safe holds data for 7 days beyond Time Travel on permanent tables (see [Q159](/interview/snowflake/#q159)).
6. **Streams and logs:** use bounded retention on raw topics. Compacted topics need tombstones.
7. **Backups:** document a process, such as re-applying deletions on restore.
8. **Auditability:** log each request, what was deleted where, and when, without storing the deleted PII in the log itself.

The operational execution of a deletion request is covered in [Q412](/interview/scenario-based/#q412).

**Example**

```sql
-- Dialect: Databricks SQL (Delta Lake)
-- Delete a subject across a table, then remove old file versions after the retention window
DELETE FROM silver.customer_events
WHERE customer_pid IN (SELECT customer_pid FROM governance.erasure_requests WHERE status = 'approved');

-- Later, once the retention period that covers the delete has passed:
VACUUM silver.customer_events RETAIN 168 HOURS;
```

**Real-world use case**

An app company kept emails in 60 tables. After a regulator inquiry, it moved to a single identity vault, pseudonymous IDs elsewhere, and a daily erasure job across Delta tables followed by VACUUM. Request turnaround dropped from weeks to days.

**Common mistakes and red flags**

- Deleting only from the gold layer.
- Forgetting time travel, snapshots and Kafka retention.
- Treating soft deletes as erasure.

**Follow-up questions**

- What is the deadline? — Without undue delay, generally within one month (extendable in some cases); confirm with legal.
- Do aggregates need deletion? — Truly anonymous aggregates generally don't; small-group aggregates that could identify someone might.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q86" data-level="Beginner">
<summary><span class="qb-num">Q86</span><h3 class="qb-title">How do you manage secrets in pipelines?</h3><span class="qb-meta">Beginner · Common · Governance &amp; Security</span></summary>

**Short answer**

Never put credentials in code, notebooks, config files in Git, or logs. Store them in a secrets manager (AWS Secrets Manager, Azure Key Vault, GCP Secret Manager, HashiCorp Vault, Databricks secret scopes, Airflow secrets backends). Inject them at runtime. Prefer short-lived, identity-based authentication (IAM roles, workload identity, OAuth, key-pair) over static passwords.

**Detailed explanation**

**Hierarchy of good practice**
1. **No secret at all:** use workload identity. For example an EC2 or EKS IAM role reading S3, Azure managed identity, GCP service accounts, Snowflake storage integrations, or OIDC federation from CI (GitHub Actions to cloud roles).
2. **Short-lived tokens** issued by an identity provider (OAuth, STS).
3. **Managed secrets** with rotation for anything static (API keys, database passwords, Snowflake key-pair private keys).

**Operational rules**
- Restrict who can read each secret, and audit reads.
- **Rotate** regularly and after staff changes, and automate rotation where the provider supports it.
- **Redact** in logs. Many platforms (Databricks, Airflow) mask known secret values in output, but don't rely on that alone.
- Separate secrets per environment (dev, test, prod).
- Use **pre-commit hooks and secret scanners** (for example gitleaks, GitHub secret scanning) to block accidental commits.
- If a secret leaks: revoke it immediately, rotate it, check access logs, and scrub history.
- Keep secrets out of job parameters and notebook widgets, which are often visible in run histories and UIs.

**Example**

```python
# Runtime: Python 3.11 with boto3 — fetch a secret at runtime instead of hardcoding
import json
import boto3

def get_db_credentials(secret_name: str, region: str = "us-east-1") -> dict:
    client = boto3.client("secretsmanager", region_name=region)
    response = client.get_secret_value(SecretId=secret_name)
    return json.loads(response["SecretString"])

creds = get_db_credentials("prod/orders-db/readonly")
# use creds["username"], creds["password"] to connect; never print or log them
```

**Real-world use case**

A leaked Snowflake password in a public notebook prompted a move to key-pair authentication for service users. Private keys now live in Secrets Manager, CI uses OIDC, and network policies restrict service users to known IP ranges.

**Common mistakes and red flags**

- Storing passwords in Airflow Variables in plain text, or in `.env` files committed to Git.
- Sharing one service credential across all environments.
- Printing connection strings in logs for debugging.

**Follow-up questions**

- How does Airflow handle secrets? — Connections and variables can be served from a secrets backend (AWS, GCP, Azure, Vault) instead of the metadata DB.
- What is OIDC federation in CI? — The CI job gets a short-lived token from the cloud provider by proving its identity, with no stored keys.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q87" data-level="Beginner">
<summary><span class="qb-num">Q87</span><h3 class="qb-title">What does CI/CD mean for data pipelines?</h3><span class="qb-meta">Beginner · Common · CI/CD for Data</span></summary>

**Short answer**

CI (continuous integration) means every change goes through version control and automated checks on each pull request: linting, unit tests, building and testing changed models on sample or cloned data. CD (continuous delivery or deployment) automatically promotes approved changes through environments to production in a repeatable way. Pipeline code, infrastructure and configuration are all deployed as code.

**Detailed explanation**

**What goes in version control**
- Transformation code (SQL, dbt, PySpark, Python), orchestration definitions (DAGs, job YAML), infrastructure (Terraform; see [Q90](/interview/data-engineering/#q90)), and access grants and policies.

**Typical CI stages** (on every pull request)
1. **Static checks:** formatting and linting (SQLFluff, ruff), type checks, secret scanning.
2. **Unit tests:** pure-function and transformation tests on small fixtures (pytest, dbt unit tests).
3. **Build and integration tests:** run changed models plus their downstream models in an isolated CI schema, often with dbt slim CI (see [Q332](/interview/dbt/#q332)) or zero-copy clones (see [Q161](/interview/snowflake/#q161)).
4. **Data diff:** compare outputs between production and the branch (row counts, key metrics).
5. **Review and approval.**

**Typical CD stages**
- Merge to `main` deploys to dev or test automatically, then to production with an approval gate or automatically after tests.
- Use deployment tools such as Declarative Automation Bundles for Databricks (see [Q239](/interview/databricks/#q239)), Snowflake CLI or schemachange for Snowflake, dbt platform jobs, and Terraform plan/apply.
- Strategies for safe releases: blue-green or Write-Audit-Publish (see [Q91](/interview/data-engineering/#q91)), feature flags, and backward-compatible schema changes.

**What's different from app CI/CD**
- Data has **state**: you can't just redeploy a table, so migrations and backfills must be planned.
- Testing needs **representative data**, which is costly and sensitive. Use samples, clones or synthetic data.
- Deploys can trigger expensive rebuilds, so selective builds matter.

**Example**

```yaml
# GitHub Actions: minimal dbt CI on pull request
name: dbt-ci
on: [pull_request]
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
      - run: pip install dbt-snowflake sqlfluff
      - run: sqlfluff lint models/
      - run: dbt deps
      - run: dbt build --select state:modified+ --defer --state ./prod-artifacts --target ci
        env:
          SNOWFLAKE_PRIVATE_KEY: ${{ secrets.SNOWFLAKE_CI_PRIVATE_KEY }}
```

**Real-world use case**

A team that used to deploy SQL by copying it into the warehouse UI moved to Git plus dbt CI. Broken references and failing tests are now caught in the pull request, and production incidents from deploys dropped markedly.

**Common mistakes and red flags**

- "We test in production."
- CI that only checks syntax, not data.
- Manual production changes outside version control.

**Follow-up questions**

- How do you handle a breaking schema change in CD? — Make it backward compatible (add then remove), coordinate with consumers, and run the migration as a planned step.
- How do you keep CI cheap? — Build only modified models, defer unchanged ones to production, use small warehouses and auto-drop CI schemas.

**In depth:** [What does CI/CD look like for data pipelines?](/interview/data-engineering/cicd-for-data-pipelines/)

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q88" data-level="Intermediate">
<summary><span class="qb-num">Q88</span><h3 class="qb-title">The testing pyramid for data pipelines</h3><span class="qb-meta">Intermediate · Common · CI/CD for Data</span></summary>

**Short answer**

The base is many fast unit tests of transformation logic on small fixtures. Above that are integration tests that run the pipeline end to end against real platforms in a test environment. Next are data tests on actual output (uniqueness, nulls, referential integrity, reconciliation). At the top are a few end-to-end or consumer-facing checks, all backed by production monitoring.

**Detailed explanation**

| Layer | What it tests | Speed | Examples |
|---|---|---|---|
| Unit tests | Logic of functions/models on static inputs | Seconds | pytest for Python transforms; dbt unit tests; PySpark functions on small DataFrames |
| Contract / schema tests | Shapes and types of inputs/outputs | Seconds | dbt model contracts, schema registry checks |
| Integration tests | Components working together on real services | Minutes | Run DAG in a test env against a cloned DB; Kafka → sink with test topics |
| Data tests (on real data) | Properties of produced data | Minutes | not_null, unique, relationships, row-count and sum reconciliation |
| End-to-end / acceptance | Consumer-level outcomes | Slow | Dashboard KPI matches finance figure |
| Production monitoring | Ongoing health | Continuous | Freshness, volume, anomaly monitors (see [Q37](/interview/data-engineering/#q37)) |

**Notes**
- **Unit tests catch logic bugs** (date boundaries, null handling, currency rounding) before data ever runs. They are the most under-used layer in data teams.
- **Data tests catch data problems** that the code can't anticipate (duplicates from upstream).
- Keep test data **small, versioned and representative**, including edge cases: nulls, duplicates, late records, time-zone boundaries.
- Make transforms testable: pure functions that take DataFrames and return DataFrames, with no I/O inside.

**Example**

```python
# Runtime: Python 3.11, pytest, PySpark 3.5 — unit test a pure transformation function
import pytest
from pyspark.sql import SparkSession, functions as F

def add_net_amount(df):
    return df.withColumn("net_amount", F.col("gross_amount") - F.coalesce(F.col("discount"), F.lit(0)))

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[1]").appName("tests").getOrCreate()

def test_add_net_amount_handles_null_discount(spark):
    df = spark.createDataFrame([(100.0, 10.0), (50.0, None)], "gross_amount double, discount double")
    result = [r.net_amount for r in add_net_amount(df).collect()]
    assert result == [90.0, 50.0]
```

**Real-world use case**

A currency-conversion bug, where the wrong rate date was used at month boundaries, kept reaching production because only data tests existed. A dozen unit tests with boundary-date fixtures caught the next regression in CI.

**Common mistakes and red flags**

- Only having data tests in production.
- Unit tests that need a full warehouse to run.
- No edge cases in fixtures.

**Follow-up questions**

- How do you unit test SQL? — dbt unit tests with mocked inputs, or run SQL on a local engine (DuckDB) against fixtures.
- How do you test idempotency? — Run the job twice for the same input and assert identical output.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q89" data-level="Intermediate">
<summary><span class="qb-num">Q89</span><h3 class="qb-title">Dev/test/prod environments and promoting data code</h3><span class="qb-meta">Intermediate · Common · CI/CD for Data</span></summary>

**Short answer**

Keep separate environments (dev, test or staging, prod) with isolated databases or schemas, compute and credentials. Code is promoted, not data: the same versioned code is deployed to each environment with environment-specific configuration. Dev and test use sampled, masked or zero-copy-cloned data, never uncontrolled copies of production PII.

**Detailed explanation**

**Isolation options**
| Level | Example | Pros | Cons |
|---|---|---|---|
| Separate accounts/workspaces | Snowflake accounts per env; Databricks workspaces per env | Strong isolation, blast-radius control | More admin; cross-env data sharing needed |
| Separate databases/catalogs | `ANALYTICS_DEV`, `ANALYTICS_PROD`; UC catalogs `dev`, `prod` | Simple, common | Same account limits/permissions surface |
| Per-developer schemas | `dbt_priya`, `dbt_ci_pr_123` | Parallel development | Needs cleanup automation |

**Configuration, not code changes**
- Environment-specific values (database names, warehouse sizes, secret names, schedules) live in config: dbt targets, bundle targets, Terraform workspaces or variables, environment variables.
- The same artefact runs everywhere, so "it worked in test" means something.

**Data for non-prod**
- **Zero-copy clones** (Snowflake `CLONE`, Delta shallow clone) of production for realistic testing, with masking policies still applied (see [Q161](/interview/snowflake/#q161), [Q242](/interview/databricks/#q242)).
- **Sampling** (latest N days) to cut cost.
- **Synthetic or masked data** where PII rules forbid production copies.
- **Defer** to production for unchanged upstream models in dbt CI (see [Q332](/interview/dbt/#q332)).

**Promotion flow**
1. Feature branch, developing in a personal dev schema.
2. Pull request: CI builds in an ephemeral CI schema.
3. Merge: automatic deploy to test or staging, with integration and data tests.
4. Approval: deploy to prod, plus post-deploy smoke checks.

**Real-world use case**

A team used a single "shared dev" schema where developers overwrote each other's tables. Moving to per-developer schemas, plus CI schemas per pull request that are dropped on merge, ended the conflicts.

**Common mistakes and red flags**

- Hardcoding `PROD` database names in SQL.
- Copying full production data, PII included, to dev.
- Making hotfixes directly in prod, outside Git.

**Follow-up questions**

- How do you avoid stale clones? — Re-clone on a schedule or per CI run; clones are cheap until data diverges.
- Do you need a staging environment if you have good CI? — For complex, multi-system releases, yes; for simple dbt changes, CI plus prod defer is often enough.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q90" data-level="Intermediate">
<summary><span class="qb-num">Q90</span><h3 class="qb-title">Infrastructure as Code (Terraform) for data platforms</h3><span class="qb-meta">Intermediate · Common · CI/CD for Data</span></summary>

**Short answer**

Infrastructure as Code (IaC) defines cloud and data-platform resources in versioned, declarative files: buckets, IAM, networks, warehouses, catalogs, roles, grants, jobs. Terraform is the most common tool, with providers for AWS, Azure, GCP, Snowflake and Databricks. Changes go through plan, review and apply in CI, giving repeatable environments and an audit trail.

**Detailed explanation**

**Why it matters**
- **Reproducibility:** dev, test and prod are created from the same code with different variables.
- **Review:** `terraform plan` shows exactly what will change before it happens.
- **Drift detection:** manual console changes show up as differences in the next plan.
- **Disaster recovery:** rebuild infrastructure from code.
- **Access governance:** roles and grants defined in code, so access changes are reviewed pull requests.

**Key Terraform concepts**
- **Providers** (aws, azurerm, google, the Snowflake provider `snowflakedb/snowflake`, databricks).
- **Resources and data sources.** **Modules** for reusable patterns (for example "a team's schema + roles + warehouse").
- **State:** a mapping of code to real resources, stored remotely (S3 + DynamoDB lock, Terraform Cloud, Azure Storage) with locking. It may contain secrets, so restrict access.
- **Workspaces or separate state per environment.**

**What not to manage with Terraform**
- Fast-changing data objects such as individual tables built by dbt or pipelines. Let the transformation tool own those.
- Very high-churn grants on thousands of objects may be better handled with future grants or a dedicated access tool.

**Alternatives and companions:** OpenTofu (open-source fork), Pulumi (general-purpose languages), AWS CDK, Bicep, Declarative Automation Bundles for Databricks job and pipeline resources, and Snowflake's declarative tooling.

**Example**

```hcl
# Terraform (HCL) — Snowflake warehouse and role for a team
# Resource and argument names follow the snowflakedb/snowflake provider; check your provider version's docs.
resource "snowflake_warehouse" "marketing_wh" {
  name           = "MARKETING_WH"
  warehouse_size = "XSMALL"
  auto_suspend   = 60
  auto_resume    = true
}

resource "snowflake_account_role" "marketing_analyst" {
  name = "MARKETING_ANALYST"
}

resource "snowflake_grant_privileges_to_account_role" "wh_usage" {
  account_role_name = snowflake_account_role.marketing_analyst.name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.marketing_wh.name
  }
}
```

**Real-world use case**

A platform team manages 40 Snowflake warehouses, roles and grants in Terraform. A new team gets its full environment from a module call in one pull request, and quarterly access reviews read straight from the code.

**Common mistakes and red flags**

- Storing state locally or in Git.
- Mixing console changes with Terraform, causing drift.
- Managing every dbt table in Terraform.

**Follow-up questions**

- How do you import existing resources? — `terraform import` or import blocks, then write matching configuration.
- How do you protect production? — Separate state and credentials per environment, required plan review, and `prevent_destroy` on critical resources.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q91" data-level="Advanced">
<summary><span class="qb-num">Q91</span><h3 class="qb-title">Write-Audit-Publish and blue-green deployments for data</h3><span class="qb-meta">Advanced · Common · CI/CD for Data</span></summary>

**Short answer**

Write-Audit-Publish (WAP) builds new data in an isolated location, audits it with quality checks, and only then publishes it atomically to consumers. Blue-green keeps two versions of the dataset or environment and switches consumers from the current one ("blue") to the new one ("green") in one step, with instant rollback by switching back.

**Detailed explanation**

**Why:** if you write directly into production tables and test afterwards, consumers may read bad or partial data in between. WAP and blue-green separate *building* from *exposing*.

**Implementations**
| Platform | Write | Audit | Publish |
|---|---|---|---|
| Iceberg | Write on a branch (`WAP` branch / `wap.id`) | Run checks on the branch | `fast_forward` main to the branch or cherry-pick snapshot |
| Delta Lake | Write to a staging table or use `replaceWhere` into a staging location | Checks on staging | `INSERT OVERWRITE`/swap via view repoint, or clone over |
| Snowflake | Build `orders_new` | Checks | `ALTER TABLE orders SWAP WITH orders_new` (atomic) |
| Any warehouse | Build new schema `analytics_green` | Checks | Repoint views/semantic layer to green schema |
| dbt | Build in a CI/staging schema, or use clones | `dbt test` | Swap schemas, or `dbt clone`/promotion step |

**Blue-green for whole environments:** build all gold tables in `gold_green` while `gold_blue` serves traffic. After tests pass, switch the consumer-facing views or the semantic layer to green. Keep blue for fast rollback, then reuse it as the next build target.

**Trade-offs**
- Extra storage (two copies) and compute.
- All published objects must switch together to stay consistent, ideally with one atomic operation or a view layer.
- Downstream incremental consumers must handle the switch correctly.

**Example**

```sql
-- Dialect: Snowflake SQL — Write-Audit-Publish with an atomic swap
CREATE OR REPLACE TABLE gold.fct_orders_staging CLONE gold.fct_orders;  -- or build from scratch
-- WRITE: apply today's changes into fct_orders_staging
-- AUDIT: run checks; the orchestrator stops here if any fail
SELECT COUNT(*) AS dup_keys
FROM (SELECT order_id FROM gold.fct_orders_staging GROUP BY order_id HAVING COUNT(*) > 1);
-- PUBLISH: atomic swap of the two tables' contents and metadata
ALTER TABLE gold.fct_orders SWAP WITH gold.fct_orders_staging;
```

**Real-world use case**

A finance mart is rebuilt nightly in a green schema. A reconciliation check against the general ledger must pass before the reporting views are repointed. On one night a source issue failed the check, and executives saw the previous day's correct numbers instead of wrong ones.

**Common mistakes and red flags**

- Running tests after publishing.
- Swapping tables one at a time, which leaves inconsistent interim states.
- Forgetting grants and policies on newly created tables. Snowflake `SWAP` exchanges tables including their grants, so check your platform's behaviour.

**Follow-up questions**

- How does Iceberg branching support WAP? — Writes go to a named branch; after validation the main branch is fast-forwarded, atomically exposing the snapshot.
- How do you roll back? — Swap back, repoint views to blue, or use time travel to restore the prior version.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q92" data-level="Beginner">
<summary><span class="qb-num">Q92</span><h3 class="qb-title">Git branching strategies for data teams</h3><span class="qb-meta">Beginner · Occasional · CI/CD for Data</span></summary>

**Short answer**

Most data teams do well with trunk-based development: short-lived feature branches off `main`, pull requests with CI and review, and frequent merges, with `main` deployable at any time. GitFlow-style long-lived `develop` and `release` branches add overhead and merge pain, and are usually unnecessary for data projects.

**Detailed explanation**

| Strategy | Branches | Pros | Cons | Fits |
|---|---|---|---|---|
| Trunk-based (+ short feature branches) | `main` + feature branches lasting hours/days | Simple, fast feedback, fewer conflicts | Needs good CI, feature flags for unfinished work | Most dbt/data teams |
| GitHub flow | `main` + feature branches; deploy from main | Very simple | Same as trunk-based | Small–medium teams |
| Environment branches | `dev`, `test`, `prod` branches mirror environments | Maps to environments visually | Drift between branches, painful merges, cherry-picks | Legacy setups (often regretted) |
| GitFlow | `main`, `develop`, `release/*`, `hotfix/*` | Formal release management | Heavy, slow | Versioned products with release trains |

**Good practices for data repos**
- Small pull requests: one model or feature each.
- Branch protection on `main`: required CI checks and at least one reviewer.
- Conventional commit messages or pull-request templates that cover what changed, impact and how it was tested.
- Promote **by deploying the same commit** to each environment (see [Q89](/interview/data-engineering/#q89)) rather than merging between environment branches.
- Tag releases if consumers need version references (for example dbt packages).
- Keep secrets and data out of the repo, and use `.gitignore` for `target/`, `logs/` and local profiles.

**Example**

```bash
# Typical short-lived feature flow
git switch -c feature/add-refund-model
# ... edit models/marts/fct_refunds.sql and tests ...
git add models/marts/fct_refunds.sql models/marts/_marts.yml
git commit -m "feat(marts): add fct_refunds with grain tests"
git push -u origin feature/add-refund-model
# open PR -> CI runs -> review -> squash merge to main -> CD deploys
```

**Real-world use case**

A team with `dev`, `qa` and `prod` branches spent days each sprint resolving merge conflicts and cherry-picking hotfixes. Switching to trunk-based development, with environment promotion by pipeline, cut release effort to minutes.

**Common mistakes and red flags**

- Long-lived branches that diverge for weeks.
- Committing directly to `main` without review.
- Environment branches that drift from each other.

**Follow-up questions**

- How do you ship unfinished work? — Keep it disabled with feature flags or config (for example dbt `enabled: false`), or behind a non-public model access level.
- Squash vs merge commits? — Squash keeps `main` history readable; either is fine if consistent.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q93" data-level="Intermediate">
<summary><span class="qb-num">Q93</span><h3 class="qb-title">General principles of cloud data cost optimisation</h3><span class="qb-meta">Intermediate · Very Common · Cost Optimisation</span></summary>

**Short answer**

Make cost visible first: tagging, per-team dashboards, budgets and alerts. Then cut waste: idle compute, oversized clusters, unnecessary full refreshes, unused tables. Then make work cheaper: incremental processing, pruning-friendly layouts, right-sized and auto-suspending compute, and storage lifecycle policies. Treat cost as a non-functional requirement, like latency.

**Detailed explanation**

**1. Visibility**
- Tag or label compute (warehouses, clusters, jobs) by team, project and environment.
- Use platform usage data (Snowflake ACCOUNT_USAGE, Databricks `system.billing`, cloud cost explorers) for per-pipeline cost.
- Set budgets, resource monitors and anomaly alerts (see [Q187](/interview/snowflake/#q187), [Q241](/interview/databricks/#q241)).

**2. Compute**
- **Auto-suspend and terminate:** idle warehouses and clusters are the most common waste.
- **Right-size:** start small and scale up only if a bigger size improves runtime proportionally.
- **Use job compute, not interactive compute, for production** (see [Q229](/interview/databricks/#q229)).
- **Spot or preemptible instances** for fault-tolerant batch (see [Q96](/interview/data-engineering/#q96)).
- **Schedule smartly:** consolidate small jobs, and avoid running every 5 minutes when hourly meets the SLA.

**3. Work reduction**
- **Incremental instead of full refresh** (see [Q25](/interview/data-engineering/#q25)).
- **Prune effectively:** partitioning or clustering aligned with filters, and selecting needed columns only.
- **Fix the expensive queries:** the top 10 queries usually dominate the spend.
- **Avoid recomputation:** materialise expensive shared intermediates, and use result caching where it applies.
- **Stop unused pipelines and tables:** check lineage and query history for zero-consumer outputs.

**4. Storage**
- Lifecycle rules to cold tiers, and expiry of temp or staging data (see [Q94](/interview/data-engineering/#q94)).
- Shorter Time Travel or retention on large, reproducible tables, and transient tables for staging (see [Q160](/interview/snowflake/#q160)).
- Compaction and VACUUM to remove old file versions.

**5. Culture**
- Show teams their own spend. Include cost in design reviews and pull requests for large jobs.

**Real-world use case**

A monthly review showed three nightly full-refresh models made up 40% of warehouse credits. Converting them to incremental with a 3-day lookback cut those costs by about 80%, with no change in output.

**Common mistakes and red flags**

- Optimising tiny costs while ignoring the top consumers.
- Cutting cost by breaking SLAs with no agreement from stakeholders.
- No tagging, so nobody can see who spends what.

**Follow-up questions**

- Where do you start? — Rank spend by warehouse, cluster, job and query, and attack the top items first.
- How do you prevent regressions? — Budgets and alerts per team, plus cost checks in CI for heavy models.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q94" data-level="Intermediate">
<summary><span class="qb-num">Q94</span><h3 class="qb-title">Storage tiering and lifecycle policies</h3><span class="qb-meta">Intermediate · Common · Cost Optimisation</span></summary>

**Short answer**

Object stores offer storage classes from hot (frequent access) to archive (rare access, cheapest per GB, slow or costly retrieval). Lifecycle policies automatically move or delete objects by age or prefix. You keep active lake data hot, tier old raw data to infrequent-access or archive classes, and expire temporary and staging data.

**Detailed explanation**

**Typical classes**
| Provider | Hot | Infrequent | Archive |
|---|---|---|---|
| AWS S3 | Standard | Standard-IA, One Zone-IA | Glacier Instant/Flexible Retrieval, Deep Archive |
| Azure Blob/ADLS | Hot | Cool, Cold | Archive |
| GCS | Standard | Nearline, Coldline | Archive |
| Automatic | S3 Intelligent-Tiering | — | — |

**Trade-offs**
- Cheaper classes have **minimum storage durations**, **retrieval fees** and sometimes **retrieval delays** (hours for deep archive).
- Don't tier files that are still referenced by active table versions and queried. Moving Delta or Iceberg data files to archive classes can break queries.
- Many small objects in IA or archive classes can cost more because of per-object overheads and minimum object sizes.

**Policy patterns for a lake**
- `landing/` and `tmp/`: expire after 7–30 days.
- `bronze/` raw files: Standard for 30–90 days, then IA, then archive or expiry based on compliance needs.
- `silver/` and `gold/` table data: keep hot. Manage old versions with table-format maintenance (VACUUM, expire snapshots) rather than raw lifecycle rules.
- Versioned buckets: add rules for **noncurrent versions**, otherwise deleted data keeps costing money.
- Clean up **incomplete multipart uploads**.

**Example**

```json
{
  "Rules": [
    {
      "ID": "tier-bronze-raw",
      "Filter": { "Prefix": "bronze/raw/" },
      "Status": "Enabled",
      "Transitions": [
        { "Days": 90,  "StorageClass": "STANDARD_IA" },
        { "Days": 365, "StorageClass": "GLACIER" }
      ],
      "NoncurrentVersionExpiration": { "NoncurrentDays": 30 },
      "AbortIncompleteMultipartUpload": { "DaysAfterInitiation": 7 }
    },
    {
      "ID": "expire-tmp",
      "Filter": { "Prefix": "tmp/" },
      "Status": "Enabled",
      "Expiration": { "Days": 7 }
    }
  ]
}
```

**Real-world use case**

A company's S3 bill was growing about 15% a month. Two-thirds was noncurrent object versions from repeated overwrites in a versioned bucket. A noncurrent-version expiration rule and archiving year-old raw logs halved the bill.

**Common mistakes and red flags**

- Archiving files that active table metadata still references.
- Versioning enabled with no noncurrent-version rules.
- Ignoring retrieval costs when choosing archive classes.

**Follow-up questions**

- Does Snowflake let you tier storage? — Snowflake manages its own storage; cost levers are retention settings, transient tables and dropping unused data (and storage lifecycle policies for archival where available).
- When is Intelligent-Tiering a good fit? — Unknown or changing access patterns, with objects large enough that the monitoring fee is small relative to savings.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q95" data-level="Intermediate">
<summary><span class="qb-num">Q95</span><h3 class="qb-title">FinOps: tagging, chargeback and showback</h3><span class="qb-meta">Intermediate · Occasional · Cost Optimisation</span></summary>

**Short answer**

FinOps brings financial accountability to cloud spend. Tagging attributes every resource or workload to an owner, project and environment. Showback reports each team's costs to them. Chargeback actually bills those costs to the teams' budgets. Data platforms need workload-level attribution because many teams share warehouses and clusters.

**Detailed explanation**

**Tagging strategy**
- Mandatory tags: `team`, `cost_center`, `env`, `project` or `data_product`.
- Enforce them at creation through IaC modules, cloud tag policies, Databricks compute policies with custom tags, and Snowflake object tags on warehouses.
- For shared compute, attribute at a finer grain: **query tags** (Snowflake `QUERY_TAG`), job and pipeline tags on Databricks, dbt model-level query comments.

**Attribution on shared platforms**
- Snowflake: warehouse credit usage per warehouse. Per-query cost attribution is available in ACCOUNT_USAGE (for example `QUERY_ATTRIBUTION_HISTORY`), plus query tags for allocation (see [Q188](/interview/snowflake/#q188)).
- Databricks: `system.billing.usage` with `custom_tags`, joined with `list_prices` (see [Q241](/interview/databricks/#q241)).
- Cloud bills: cost allocation tags in AWS Cost Explorer, Azure Cost Management and GCP billing export to BigQuery.

**Showback vs chargeback**
| Model | What happens | When to use |
|---|---|---|
| Showback | Teams see their costs, no budget transfer | Early stages; builds awareness |
| Chargeback | Costs billed to team budgets | Mature orgs; strong accountability |

**Unit economics:** cost per pipeline run, per TB processed, per dashboard or per active customer. These turn spend into decisions ("this report costs $3k a month and has 2 viewers").

**Example**

```sql
-- Dialect: Snowflake SQL — tag queries per pipeline, then attribute credits by tag
ALTER SESSION SET QUERY_TAG = 'team=growth;pipeline=daily_attribution';

-- Later, analyse attributed compute by tag (ACCOUNT_USAGE has latency)
SELECT query_tag,
       SUM(credits_attributed_compute) AS credits
FROM snowflake.account_usage.query_attribution_history
WHERE start_time >= DATEADD(day, -30, CURRENT_TIMESTAMP())
GROUP BY query_tag
ORDER BY credits DESC;
```

**Real-world use case**

After introducing monthly showback by team, two teams found forgotten hourly pipelines feeding deprecated dashboards. Turning those off saved more than the cost of the FinOps tooling itself.

**Common mistakes and red flags**

- Tags as optional, so half the spend is "unallocated".
- Allocating shared warehouse cost only by warehouse, when many teams share it.
- Jumping straight to chargeback without trust in the numbers.

**Follow-up questions**

- How do you handle shared platform costs (catalog, orchestration)? — Allocate proportionally by usage, or keep them as a central platform cost.
- What are unit metrics? — Cost normalised by a business or workload unit, used to track efficiency over time.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q96" data-level="Intermediate">
<summary><span class="qb-num">Q96</span><h3 class="qb-title">Spot/preemptible and reserved capacity for data workloads</h3><span class="qb-meta">Intermediate · Occasional · Cost Optimisation</span></summary>

**Short answer**

Spot (AWS), Spot VMs (Azure) and Spot/preemptible VMs (GCP) are spare capacity at large discounts, but the provider can reclaim them at short notice. Reserved instances, savings plans or committed-use discounts trade a 1–3-year commitment for a lower price on steady usage. Use spot for fault-tolerant batch workers, and commitments for predictable baseline capacity.

**Detailed explanation**

**Spot / preemptible**
- Discounts are often large (up to roughly 90% versus on-demand on AWS, varying by instance and region).
- Interruption warnings are brief: about 2 minutes on AWS, and roughly 30 seconds for GCP and Azure.
- **Good fit:** Spark executors for batch ETL, because lost tasks are recomputed from lineage (shuffle files on lost nodes may need recomputation). Also stateless workers and CI runners.
- **Poor fit:** the Spark driver, single-node jobs with long runtimes, stateful streaming without checkpoints, and anything with strict SLAs and no fallback.
- **Patterns:** on-demand driver plus spot workers, with fallback to on-demand (Databricks `SPOT_WITH_FALLBACK`). Diversify instance types across availability zones. Use EMR instance fleets.

**Reserved / committed**
- AWS Savings Plans or Reserved Instances, Azure Reservations, GCP committed use discounts.
- Platform-level commitments: Snowflake capacity contracts (pre-purchased credits) and Databricks committed-use contracts.
- **Good fit:** always-on baseline such as streaming clusters, BI warehouses and core platform services.
- **Risk:** paying for unused commitments if workloads shrink or move.

| Option | Discount | Commitment | Interruption risk | Best for |
|---|---|---|---|---|
| On-demand | None | None | None | Spiky, critical, unknown |
| Spot | Highest | None | Yes | Fault-tolerant batch |
| Reserved / committed | Moderate–high | 1–3 years | None | Steady baseline |

**Example**

```json
{
  "cluster_name": "nightly-etl",
  "spark_version": "15.4.x-scala2.12",
  "node_type_id": "i3.xlarge",
  "autoscale": { "min_workers": 2, "max_workers": 20 },
  "aws_attributes": {
    "first_on_demand": 1,
    "availability": "SPOT_WITH_FALLBACK",
    "zone_id": "auto",
    "spot_bid_price_percent": 100
  }
}
```
`first_on_demand: 1` keeps the driver on on-demand capacity. Workers use spot with fallback.

**Real-world use case**

Moving nightly Spark ETL workers to spot with fallback cut compute cost by roughly 60%, and runtime increased only slightly on nights with interruptions. The 24/7 streaming cluster stayed on on-demand capacity covered by a savings plan.

**Common mistakes and red flags**

- Putting the driver on spot.
- Using spot for jobs with hard SLAs and no fallback.
- Committing to reservations before usage is stable.

**Follow-up questions**

- How does Spark handle losing a spot node? — Tasks rerun elsewhere; lost shuffle outputs are recomputed; graceful decommissioning can migrate shuffle data on notice.
- How do you choose commitment size? — Commit to the stable minimum observed over several months and leave the variable part on-demand or spot.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q97" data-level="Intermediate">
<summary><span class="qb-num">Q97</span><h3 class="qb-title">What is reverse ETL?</h3><span class="qb-meta">Intermediate · Occasional · Serving &amp; Activation</span></summary>

**Short answer**

Reverse ETL syncs modelled data from the warehouse or lakehouse back into operational tools such as CRM, marketing automation, support desks and ad platforms, so business teams can act on it. Examples: lead scores into Salesforce, churn-risk flags into a support tool, audiences into an ad platform.

**Detailed explanation**

Traditional pipelines end at dashboards. Reverse ETL closes the loop by pushing warehouse-computed attributes to the systems people work in.

**How it works**
1. Define a **model** in the warehouse, such as `customer_health` with `customer_id`, `health_score`, `plan` and `last_login`.
2. **Map** fields to the destination object (for example Salesforce Account custom fields).
3. **Sync on a schedule or trigger.** Tools diff the model against the last sync and send only changed rows, using upsert APIs keyed by an external ID.
4. **Handle API limits**, retries, partial failures and error reporting.

**Tools:** Hightouch, Census (now part of Fivetran), RudderStack, Segment's warehouse-native features, platform-native options, or custom code with API clients.

**Design considerations**
- **Identity matching:** a reliable key that exists in both systems (CRM ID, email).
- **Ownership of fields:** the warehouse should own computed fields, and the CRM should not overwrite them, which avoids sync wars.
- **Data quality is now operational:** a bad score reaches salespeople immediately, so gate syncs on tests (see [Q36](/interview/data-engineering/#q36)).
- **Rate limits and costs** of destination APIs.
- **PII and consent:** sync only permitted fields to permitted tools.

**Example**

```sql
-- Dialect: Snowflake SQL — model designed for sync: one row per destination record, stable key
CREATE OR REPLACE VIEW activation.crm_account_health AS
SELECT a.crm_account_id           AS external_id,   -- matches Salesforce Account ID
       h.health_score,
       h.churn_risk_band,
       h.last_active_date
FROM gold.dim_account a
JOIN gold.account_health h ON h.account_key = a.account_key
WHERE a.is_current = TRUE
  AND a.crm_account_id IS NOT NULL;
```

**Real-world use case**

A SaaS company syncs a daily product-usage score to Salesforce. Account managers see churn risk on the account page, and renewal outreach is prioritised by it, which used to require exporting dashboards to spreadsheets.

**Common mistakes and red flags**

- Syncing untested data into customer-facing tools.
- No stable matching key, which creates duplicate CRM records.
- Ignoring destination API limits.

**Follow-up questions**

- How is this different from CDC? — CDC moves operational changes into analytics; reverse ETL moves analytical results into operational tools.
- How do you avoid overwriting manual edits? — Sync only warehouse-owned fields, and document field ownership.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q98" data-level="Advanced">
<summary><span class="qb-num">Q98</span><h3 class="qb-title">What is data mesh, and when does it fail?</h3><span class="qb-meta">Advanced · Common · Architecture &amp; Organisation</span></summary>

**Short answer**

Data mesh is an organisational and architectural approach in which domain teams own and publish their data as products. A central platform team provides self-service infrastructure, and federated governance sets shared standards. It fails when domains lack data skills or incentives, when the platform isn't truly self-service, or when governance is too weak to keep products interoperable.

**Detailed explanation**

Data mesh (described by Zhamak Dehghani) rests on **four principles**:
1. **Domain ownership:** the teams closest to the data (orders, payments, logistics) own its analytical datasets end to end.
2. **Data as a product:** datasets are discoverable, documented, trustworthy (SLAs, quality), secure and versioned, with a named owner.
3. **Self-serve data platform:** a central team provides tooling for storage, pipelines, catalog, access and monitoring, so domains don't rebuild infrastructure.
4. **Federated computational governance:** shared global rules (identifiers, classification, interoperability standards, privacy) enforced automatically by the platform, with local autonomy otherwise.

**What it solves:** central data-team bottlenecks in large organisations with many domains, and lost context when a central team models data it doesn't understand.

**Common failure modes**
- **No skills or capacity in domains:** product engineers are told to own pipelines without training or headcount.
- **Platform not self-service:** domains need tickets for every step, so the bottleneck simply moves.
- **Weak governance:** inconsistent IDs and definitions mean products can't be joined, which recreates silos.
- **Over-application:** a 10-person data team adopting mesh adds overhead with no benefit.
- **"Mesh" as a rebranding** with no change in ownership, incentives or funding.

**Pragmatic adoption:** start with a few mature domains, define a minimal data-product standard (owner, contract, SLA, docs, tests), automate governance in the platform, and measure product usage and incidents.

**Real-world use case**

A large retailer's central team had a 6-month backlog. It piloted mesh with the payments and supply-chain domains, which already had data engineers, gave them a platform template (catalog registration, CI, access policies), and kept customer and product as centrally governed conformed products.

**Common mistakes and red flags**

- Describing mesh as a technology or tool.
- Ignoring the organisational cost.
- Saying mesh means "no central team".

**Follow-up questions**

- What makes a good data product? — A clear owner, contract/schema, SLA, documentation, quality tests, access policy and a stable interface.
- Data mesh vs data fabric? — Mesh is organisational (domain ownership); fabric is technology-driven integration through metadata and automation.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q99" data-level="Intermediate">
<summary><span class="qb-num">Q99</span><h3 class="qb-title">What is a semantic/metrics layer?</h3><span class="qb-meta">Intermediate · Occasional · Serving &amp; Activation</span></summary>

**Short answer**

A semantic layer defines business entities, dimensions and metrics (such as revenue, active users and churn rate) once, in a central, governed place. BI tools, notebooks, applications and AI assistants all query those definitions instead of reimplementing them. It translates metric requests into correct SQL against the underlying tables, so numbers match everywhere.

**Detailed explanation**

**The problem.** "Revenue" is defined in five dashboards, three notebooks and a spreadsheet, each with different refund, tax or currency logic. Leadership meetings turn into arguments about whose number is right.

**What a semantic layer holds**
- **Entities and joins:** how `orders`, `customers` and `products` relate (keys, join types).
- **Dimensions:** attributes and time grains to slice by.
- **Measures and metrics:** aggregations and formulas, including ratios computed correctly from components (see [Q7](/interview/data-engineering/#q7)), filters, and time-based metrics (MTD, rolling 28 days).
- **Access rules and documentation.**

**How it's used**
- A client asks for "`net_revenue` by `region` by `month` for 2026". The layer generates the SQL, handling joins, grain and fan-out safety, and returns results.
- Interfaces: SQL or JDBC endpoints, APIs, BI tool integrations, and LLM or AI agents that ground natural-language questions in governed definitions.

**Implementations**
- **dbt Semantic Layer / MetricFlow** (see [Q329](/interview/dbt/#q329)).
- **Snowflake semantic views**, used by Cortex Analyst and BI.
- **Databricks metric views** (Unity Catalog).
- **Cube**, **AtScale**, **LookML (Looker)**, Power BI semantic models.

**Trade-offs:** an extra layer to maintain, and tool-support gaps. Adoption requires stakeholders to agree definitions, which is the hard part.

**Real-world use case**

A company defined 40 core KPIs once in its semantic layer. Its BI tool, finance spreadsheets (through the API) and an internal AI assistant all reported identical numbers, which ended the monthly reconciliation exercise. [Q450](/interview/scenario-based/#q450) covers this as a scenario.

**Common mistakes and red flags**

- Thinking a semantic layer is "just views".
- Pre-aggregating ratios instead of defining them from components.
- Launching without agreed metric owners.

**Follow-up questions**

- Why not just use a gold table per KPI? — Tables fix the grain; a semantic layer computes metrics correctly at any requested grain and filter combination.
- How does it help AI assistants? — It gives the model governed, unambiguous metric definitions, which improves text-to-SQL accuracy.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q100" data-level="Advanced">
<summary><span class="qb-num">Q100</span><h3 class="qb-title">Data engineering for ML and GenAI: feature stores, embeddings, vector stores</h3><span class="qb-meta">Advanced · Occasional · ML &amp; AI Data</span></summary>

**Short answer**

For ML, data engineers build point-in-time-correct feature pipelines, and often a feature store that serves the same features offline for training and online at low latency for inference. For GenAI, they build document ingestion, chunking, embedding and vector-index pipelines for retrieval-augmented generation (RAG), with freshness, access control and evaluation data.

**Detailed explanation**

**Classical ML data needs**
- **Feature pipelines:** transform raw data into features ("orders in the last 30 days").
- **Point-in-time correctness:** training rows must use only feature values known *before* each label's timestamp. Otherwise future information leaks in and inflates offline accuracy (see [Q437](/interview/scenario-based/#q437)).
- **Feature store:** an offline store (lakehouse or warehouse tables) for training, and an online store (key-value, millisecond reads) for serving. It provides consistent definitions, lineage and reuse. Examples: Databricks Feature Store / Unity Catalog feature tables, Snowflake Feature Store, Feast, Tecton, SageMaker Feature Store, Vertex AI Feature Store.
- **Training-serving skew:** the same transformation code must feed training and inference.
- **Monitoring:** feature drift and data quality for model inputs.

**GenAI / RAG data needs**
1. **Ingest** documents (PDFs, wikis, tickets) with metadata and permissions.
2. **Parse and chunk:** split into passages sized for the embedding model, with overlap, and keep source references.
3. **Embed:** convert chunks to vectors with an embedding model, and track the model version.
4. **Index:** store vectors in a vector index (pgvector, Pinecone, OpenSearch, Snowflake Cortex Search, Databricks Vector Search) alongside metadata filters.
5. **Keep fresh incrementally:** re-embed only changed documents, and delete removed ones.
6. **Enforce access control** at retrieval time, so users only retrieve chunks they're allowed to see.
7. **Evaluate:** curated question-and-answer datasets to measure retrieval and answer quality.

**Example**

```sql
-- Dialect: Snowflake SQL — point-in-time feature join (features as of each label's timestamp)
SELECT l.customer_id,
       l.label_ts,
       l.churned,
       f.orders_30d,
       f.avg_order_value_90d
FROM ml.churn_labels l
ASOF JOIN ml.customer_features f
  MATCH_CONDITION (l.label_ts >= f.feature_ts)
  ON l.customer_id = f.customer_id;
```
Snowflake's `ASOF JOIN` picks the latest feature row at or before each label time. On other engines, use a range join plus `ROW_NUMBER`, or the feature store's point-in-time lookup.

**Real-world use case**

A support chatbot's answers were outdated because its vector index was rebuilt monthly. The team built an incremental pipeline: changed knowledge-base articles are re-chunked and re-embedded hourly, deleted articles are removed from the index, and retrieval filters by the user's product entitlements.

**Common mistakes and red flags**

- Joining features by "latest value" when building training data, which leaks future information.
- Rebuilding embeddings from scratch on every run.
- Ignoring document-level permissions in RAG retrieval.

**Follow-up questions**

- How do you choose chunk size? — Balance context completeness against retrieval precision, then tune with an evaluation set.
- What happens when you change the embedding model? — All vectors must be regenerated with the new model, so version indexes and run old and new side by side during migration.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>
