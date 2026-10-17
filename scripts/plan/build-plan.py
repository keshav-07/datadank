#!/usr/bin/env python3
"""Generate the 90-day study plan (docs/curriculum/plan-90-days.json).

Every day is 3 hours. Weekdays have 10 tasks, weekends 12 shorter ones (projects, mocks and reviews).
Day 1 is a Monday. Each task is one checkable item linked to an existing page.

Topics flow through four lanes, each in a hand-ordered, easy-to-hard sequence:
- SQL: one lesson topic and one practice problem a day (plus SQL as a focus track in weeks 1-2).
- DSA: two problems a weekday, three a weekend day; Easy before Medium inside each pattern group,
  Hard problems only in the last days.
- Focus: the day's main technology, in prerequisite order (Python and SQL, modeling, Spark, Delta,
  Airflow, Snowflake, Kafka, streaming, system design, then revision).
- Second: data-engineering concepts, dev tools, dbt and AWS.
Weekends add project steps, system design (from week 5), a mock or interview set, and a weekly review.

Run after `scripts/import-question-bank.py`, because interview tasks link to question-bank anchors.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CURRICULUM = ROOT / "docs/curriculum/curriculum.json"
BANKS = ROOT / "src/data/question-banks.json"
OUTPUT = ROOT / "docs/curriculum/plan-90-days.json"

DAYS = 90
MINUTES = 180
WEEKDAY_TASKS = 10
WEEKEND_TASKS = 12


def is_weekend(day: int) -> bool:
    return (day - 1) % 7 in (5, 6)  # day 1 is a Monday


PHASES = [
    {"title": "Phase 1: Foundations", "end": 28,
     "goal": "SQL, Python and easy DSA; data-engineering concepts and data modeling; start Spark; build a tested CSV-to-warehouse pipeline."},
    {"title": "Phase 2: Core pipelines", "end": 63,
     "goal": "Spark, Delta Lake, Airflow, dbt and Snowflake; medium DSA; batch and analytics projects; begin system design."},
    {"title": "Phase 3: Streaming, design and interviews", "end": 90,
     "goal": "Kafka, Spark streaming, AWS services and system design; graph and DP problems; a streaming project, mocks and final revision."},
]

WEEKLY = {
    1: "Write filtered and aggregated SQL, use Python for files and functions, and solve easy array and hashing problems.",
    2: "Join tables safely, structure Python with tests and logging, and explain file formats and batch vs streaming.",
    3: "Design a star schema at the right grain, handle slowly changing dimensions, and load data idempotently.",
    4: "Write window-function SQL, explain Spark's execution model, and finish the CSV-to-warehouse project.",
    5: "Transform data with PySpark DataFrames, choose join strategies, and start system design with batch ingestion.",
    6: "Diagnose shuffles and skew in Spark, explain Delta Lake transactions, and finish the large-scale batch project.",
    7: "Write Airflow DAGs with safe retries and backfills, build dbt models, and design an ELT pipeline.",
    8: "Schedule with sensors and assets in Airflow, test dbt models, and build the e-commerce analytics project.",
    9: "Load and model data in Snowflake, use streams and tasks, and design analytics platforms.",
    10: "Explain Kafka partitions, consumer groups and delivery guarantees; build the S3, PySpark and Snowflake pipeline.",
    11: "Explain Kafka replication and exactly-once, use AWS ingestion services, and design streaming systems.",
    12: "Build a watermarked Spark streaming job, design governed platforms, and run the streaming project end to end.",
    13: "Revise every track, solve hard DSA problems, and finish your project demo, résumé and mocks.",
}

PHASE_REVIEW = {
    28: "Phase 1 review: explain the CSV-to-warehouse project end to end, and re-solve five SQL and five DSA problems from weeks 1-4.",
    63: "Phase 2 review: walk through the batch and analytics projects, and do one timed SQL and DSA round.",
    90: "Final review: record your project demo, update your résumé, and list the topics to keep revising.",
}

# ---------------------------------------------------------------- SQL lessons (77, in learning order)
SQL_TOPICS = [
    "SELECT, WHERE & basic filtering", "Comparison & logical operators", "IN, BETWEEN, LIKE patterns",
    "ORDER BY multi-column sorting", "LIMIT / TOP / FETCH FIRST", "Aliases and column naming",
    "DISTINCT and de-duplication", "NULL handling with IS NULL / COALESCE", "CASE WHEN expressions",
    "Aggregate functions (SUM, AVG, MIN, MAX)", "COUNT vs COUNT(DISTINCT)", "GROUP BY fundamentals",
    "HAVING vs WHERE", "INNER JOIN basics", "LEFT JOIN basics", "String functions (CONCAT, SUBSTRING, TRIM)",
    "Date functions basics", "CAST and data type conversion", "Arithmetic & rounding", "UNION vs UNION ALL",
    "RIGHT JOIN & FULL OUTER JOIN", "Self Join patterns", "Multi-table joins", "Cross Join & cartesian products",
    "Conditional aggregation", "Semi-join with EXISTS", "Anti-join (NOT EXISTS / LEFT JOIN NULL)",
    "Subqueries in WHERE", "Subqueries in FROM (derived tables)", "Scalar subqueries", "Correlated subqueries",
    "Common Table Expressions (CTEs)", "Multiple chained CTEs", "EXCEPT / INTERSECT set operations",
    "Window function basics (OVER)", "PARTITION BY clause", "ROW_NUMBER()", "RANK() vs DENSE_RANK()",
    "LAG() and LEAD()", "FIRST_VALUE / LAST_VALUE", "NTILE() bucketing", "Running totals with SUM OVER",
    "Moving average with frame clause", "Percent of total", "Date truncation & bucketing",
    "Generate date series / calendar table", "PIVOT rows to columns", "UNPIVOT columns to rows", "GROUPING SETS",
    "ROLLUP and CUBE", "Top-N per group", "Deduplicate keeping latest record", "Month-over-month change",
    "Year-over-year growth", "Rolling 7/30 day metrics", "Slowly changing dimension queries", "Gaps and Islands problem",
    "Detecting consecutive streaks", "Sessionization of events", "Recursive CTEs", "Hierarchical / org-chart queries",
    "Retention analysis", "Cohort analysis", "Funnel / conversion analysis", "JSON parsing in SQL",
    "Array & nested data handling", "Median & percentile (PERCENTILE_CONT)", "Reading EXPLAIN / execution plans",
    "Avoiding full table scans", "Query rewriting for performance", "Index design (B-tree, composite)",
    "Partition pruning", "Columnar vs row storage", "Join algorithms (hash, merge, nested loop)",
    "Handling skew in aggregations", "Isolation levels (ACID)", "Materialized views",
]

# Order of the LeetCode SQL 50 study plan, with the two Hard problems moved to the end.
SQL_PRACTICE = [
    (1757, "Recyclable and Low Fat Products", "recyclable-and-low-fat-products"),
    (584, "Find Customer Referee", "find-customer-referee"),
    (595, "Big Countries", "big-countries"),
    (1148, "Article Views I", "article-views-i"),
    (1683, "Invalid Tweets", "invalid-tweets"),
    (175, "Combine Two Tables", "combine-two-tables"),
    (1378, "Replace Employee ID With The Unique Identifier", "replace-employee-id-with-the-unique-identifier"),
    (1068, "Product Sales Analysis I", "product-sales-analysis-i"),
    (1581, "Customer Who Visited but Did Not Make Any Transactions", "customer-who-visited-but-did-not-make-any-transactions"),
    (197, "Rising Temperature", "rising-temperature"),
    (1661, "Average Time of Process per Machine", "average-time-of-process-per-machine"),
    (577, "Employee Bonus", "employee-bonus"),
    (1280, "Students and Examinations", "students-and-examinations"),
    (570, "Managers with at Least 5 Direct Reports", "managers-with-at-least-5-direct-reports"),
    (1934, "Confirmation Rate", "confirmation-rate"),
    (620, "Not Boring Movies", "not-boring-movies"),
    (1251, "Average Selling Price", "average-selling-price"),
    (1075, "Project Employees I", "project-employees-i"),
    (1633, "Percentage of Users Attended a Contest", "percentage-of-users-attended-a-contest"),
    (1211, "Queries Quality and Percentage", "queries-quality-and-percentage"),
    (1193, "Monthly Transactions I", "monthly-transactions-i"),
    (1174, "Immediate Food Delivery II", "immediate-food-delivery-ii"),
    (550, "Game Play Analysis IV", "game-play-analysis-iv"),
    (2356, "Number of Unique Subjects Taught by Each Teacher", "number-of-unique-subjects-taught-by-each-teacher"),
    (1141, "User Activity for the Past 30 Days I", "user-activity-for-the-past-30-days-i"),
    (1070, "Product Sales Analysis III", "product-sales-analysis-iii"),
    (596, "Classes More Than 5 Students", "classes-more-than-5-students"),
    (586, "Customer Placing the Largest Number of Orders", "customer-placing-the-largest-number-of-orders"),
    (610, "Triangle Judgment", "triangle-judgment"),
    (511, "Game Play Analysis I", "game-play-analysis-i"),
    (1045, "Customers Who Bought All Products", "customers-who-bought-all-products"),
    (1731, "The Number of Employees Which Report to Each Employee", "the-number-of-employees-which-report-to-each-employee"),
    (1789, "Primary Department for Each Employee", "primary-department-for-each-employee"),
    (1741, "Find Total Time Spent by Each Employee", "find-total-time-spent-by-each-employee"),
    (180, "Consecutive Numbers", "consecutive-numbers"),
    (1164, "Product Price at a Given Date", "product-price-at-a-given-date"),
    (1204, "Last Person to Fit in the Bus", "last-person-to-fit-in-the-bus"),
    (1907, "Count Salary Categories", "count-salary-categories"),
    (1978, "Employees Whose Manager Left the Company", "employees-whose-manager-left-the-company"),
    (1341, "Movie Rating", "movie-rating"),
    (1321, "Restaurant Growth", "restaurant-growth"),
    (1077, "Project Employees III", "project-employees-iii"),
    (1393, "Capital Gain/Loss", "capital-gainloss"),
    (1158, "Market Analysis I", "market-analysis-i"),
    (1084, "Sales Analysis III", "sales-analysis-iii"),
    (176, "Second Highest Salary", "second-highest-salary"),
    (1667, "Fix Names in a Table", "fix-names-in-a-table"),
    (1527, "Patients With a Condition", "patients-with-a-condition"),
    (196, "Delete Duplicate Emails", "delete-duplicate-emails"),
    (182, "Duplicate Emails", "duplicate-emails"),
    (1517, "Find Users With Valid E-Mails", "find-users-with-valid-e-mails"),
    (1327, "List the Products Ordered in a Period", "list-the-products-ordered-in-a-period"),
    (184, "Department Highest Salary", "department-highest-salary"),
    (262, "Trips and Users", "trips-and-users"),
    (185, "Department Top Three Salaries", "department-top-three-salaries"),
]

SQL_QUESTIONS = [  # standalone SQL interview problems, easiest first (after the LeetCode set)
    "inner-vs-left-join", "sql-execution-order-where-vs-having", "customers-without-orders-anti-join",
    "window-functions-vs-group-by", "remove-duplicate-records", "second-highest-salary", "join-fan-out-double-counting",
    "top-n-per-group-department-salaries", "running-total-moving-average", "month-over-month-growth",
    "index-not-used-explain", "scd-type-2-merge-sql", "optimize-slow-query",
]

SQL_CASES = [  # SQL case studies, easiest first
    "Daily Active Users", "Monthly Revenue", "Average Order Value", "Top Selling Products",
    "New vs Returning Customers", "Customer Lifetime Value", "Churn Rate", "Conversion Funnel",
    "Inventory Turnover", "Session Duration", "Employee Hierarchy", "A/B Test Results",
    "Subscription Renewals", "Refund Analysis", "Lead Scoring", "Fraud Pattern Detection",
]

# ---------------------------------------------------------------- DSA (205 of 248)
DSA_GROUPS = [
    ["Arrays", "Hashing", "Strings", "Bit Manipulation", "Two Pointers", "Queue", "Sliding Window"],
    ["Stack", "Binary Search", "Linked List", "Trees", "BST"],
    ["Heap", "Greedy", "Backtracking", "Graph", "Dynamic Programming"],
]
DSA_SKIP = {  # Mediums that repeat a pattern already covered; left for extra practice
    "Maximum Subarray Sum with One Deletion", "Maximum Absolute Sum of Any Subarray", "Subarray Sums Divisible by K",
    "String to Integer (atoi)", "Single Number III", "Triplets with Smaller Sum", "4Sum",
    "Longest K unique characters substring", "Remove All Adjacent Duplicates in String II",
    "Remove Nodes From Linked List", "Aggressive Cows", "Maximum Candies Allocated to K Children",
    "Minimum Number of Days to Make m Bouquets", "Circular Array Loop", "Rotate List",
    "Extra Characters in a String", "Insert into a BST", "Min Cost to Connect Ropes", "Maximum Sum Combination",
    "Merge Triplets to Form Target", "Remove Duplicate Letters", "Letter Case Permutation",
    "Number of Closed Islands", "Path With Minimum Effort", "Count Subsets with Sum",
}
DSA_HARD = [  # the classic Hard problems, last
    "Trapping Rain Water", "Minimum Window Substring", "Sliding Window Maximum", "Largest Rectangle in Histogram",
    "Median of Two Sorted Arrays", "Merge k Sorted Lists", "Binary Tree Maximum Path Sum",
    "Serialize and Deserialize Binary Tree", "Find Median from Data Stream", "Word Ladder",
]

# ---------------------------------------------------------------- focus lane
PYTHON = [
    ("Python for data engineering", "articles:python/python-for-data-engineering"),
    ("Python data structures", "articles:python/data-structures-for-interviews"),
    ("Reusable functions and modules", "articles:python/functions-modules-reusable-code"),
    ("CSV and JSON file handling", "articles:python/file-io-csv-json"),
    ("Iterators and generators", "articles:python/iterators-generators"),
    ("Error handling and logging", "articles:python/error-handling-logging"),
    ("Testing with pytest", "articles:python/testing-with-pytest"),
    ("Pandas for data engineers", "articles:python/pandas-for-data-engineers"),
    ("Paginated API ingestion", "articles:python/working-with-apis"),
    ("Idempotent CSV loader", "articles:python/idempotent-csv-loader"),
]
MODELING = [
    "Kimball dimensional modeling", "Fact tables", "Dimension tables", "Star Schema", "Grain definition",
    "Snowflake Schema", "Normalization (1NF-3NF/BCNF)", "Denormalization trade-offs", "Natural vs surrogate keys",
    "SCD Type 1 (overwrite)", "SCD Type 2 (history)", "Conformed dimensions",
    "Additive/semi-additive/non-additive measures", "Transaction vs periodic snapshot vs accumulating snapshot",
    "Late arriving dimensions", "Medallion architecture (bronze/silver/gold)",
]
SPARK = [
    "Spark architecture (driver/executor)", "SparkSession & SparkContext", "DataFrame API basics",
    "Schema definition (StructType)", "select / withColumn / filter", "Handling nulls (na.fill/drop)",
    "groupBy & aggregations", "Joins in Spark SQL", "Window functions in Spark", "Reading/writing JSON & CSV",
    "Reading/writing Parquet", "Lazy evaluation & DAG", "RDD transformations vs actions",
    "Narrow vs wide transformations", "Job/stage/task hierarchy", "Partitions & parallelism",
    "Understanding shuffle cost", "Repartition vs coalesce", "Broadcast hash join", "Join strategy selection",
    "Persistence & caching levels", "UDFs and pandas UDFs", "Catalyst optimizer", "Predicate & projection pushdown",
    "AQE (Adaptive Query Execution)", "Detecting data skew", "Salting for skew", "Small files problem",
]
DELTA = [
    "Delta Lake architecture", "Transaction log (_delta_log)", "ACID transactions on Delta", "Schema enforcement",
    "MERGE / upsert", "Time travel & versioning", "Schema evolution", "OPTIMIZE & compaction",
]
AIRFLOW = [
    "DAG fundamentals", "Tasks & dependencies", "PythonOperator", "TaskFlow API", "Schedule intervals & cron",
    "Execution date vs logical date", "Catchup & start_date pitfalls", "Idempotent tasks", "Avoiding top-level code",
    "XCom push/pull", "Variables & Connections", "Hooks", "Sensor basics", "Deferrable operators & triggers",
    "Retries & alerting", "Backfills & reruns", "Data-aware scheduling (Datasets)", "Testing DAGs",
]
SNOWFLAKE = [
    "Three-layer architecture", "Separation of storage & compute", "Virtual warehouse sizing",
    "Scaling up vs scaling out", "Auto-suspend & auto-resume", "Micro-partitions deep dive",
    "Internal vs external stages", "COPY INTO command", "Snowpipe continuous ingestion", "Semi-structured (VARIANT)",
    "FLATTEN function", "Stream object basics", "Task fundamentals", "Task + Stream pattern", "Time Travel basics",
    "Zero-copy cloning", "Clustering keys", "RBAC model",
]
KAFKA = [
    "Topic fundamentals", "Partitions & parallelism", "Partition keys & ordering", "Producer API", "acks (0/1/all)",
    "Consumer API", "Poll loop", "Consumer group basics", "Offset management", "Auto vs manual commit",
    "Rebalancing protocols", "Leader & followers", "In-Sync Replicas (ISR)", "At-least-once",
    "Idempotent producer", "Exactly-once semantics (EOS)", "Consumer lag", "Schema Registry basics",
]
STREAMING = [
    "Structured Streaming model", "Input sources (Kafka/files)", "Output sinks & modes", "Triggers & micro-batch",
    "Event-time vs processing-time", "Watermarking", "Checkpointing in streaming", "Stateful aggregations",
]
DESIGN_WEEKDAY = [
    "Design a metrics / KPI platform", "Design a GDPR/PII compliant pipeline", "Design a data catalog & lineage system",
    "Design a schema registry & contract system", "Design a data SLA & freshness monitoring system",
    "Design a payment events pipeline (exactly-once)", "Design a fraud detection pipeline",
    "Design a multi-tenant data platform",
]
DESIGN_WEEKEND = [
    "Design a batch ingestion framework", "Design a Data Lake on cloud object storage",
    "Design a Lakehouse (bronze/silver/gold)", "Design an idempotent reprocessing system",
    "Design an ELT pipeline with dbt", "Design a slowly changing dimension framework",
    "Design a data quality framework", "Design a backfill & late-data handling system",
    "Design an analytics / BI platform", "Design a data warehouse for a SaaS product",
    "Design a CDC pipeline from OLTP to warehouse", "Design an event-driven architecture",
    "Design a clickstream analytics pipeline", "Design a streaming ETL with Kafka + Spark",
    "Design a real-time streaming platform", "Design a data observability system",
    "Design a unified batch + streaming (Lambda/Kappa)",
]
REVISION = [  # week 13: (task, ref, anchor)
    ("Revise SQL: window functions and Top-N queries", "articles:sql/window-functions", None),
    ("Revise SQL: answer the advanced section of the Top 50 SQL questions", "articles:sql/sql-interview-questions", "advanced-sql-interview-questions-experienced"),
    ("Revise Python: answer the intermediate Python questions aloud", "articles:python/python-interview-questions", "intermediate-python-interview-questions"),
    ("Revise data modeling: draw a star schema with an SCD Type 2 dimension", "articles:data-warehousing/slowly-changing-dimensions", None),
    ("Revise Spark: explain shuffles, skew and join strategies", "articles:pyspark/pyspark-interview-questions", "intermediate-pyspark-and-spark-interview-questions"),
    ("Revise Airflow: retries, backfills and idempotent tasks", "articles:airflow/airflow-interview-questions", "intermediate-airflow-interview-questions"),
    ("Revise Snowflake: warehouses, micro-partitions and cost", "articles:snowflake/snowflake-interview-questions", "intermediate-snowflake-interview-questions"),
    ("Revise Kafka: partitions, consumer groups and delivery guarantees", "articles:kafka/kafka-interview-questions", "intermediate-kafka-interview-questions"),
    ("Revise dbt: models, tests and incremental strategies", "articles:dbt/dbt-interview-questions", "intermediate-dbt-interview-questions"),
    ("Revise data-engineering concepts: CDC, idempotency and late data", "articles:career/data-engineering-interview-questions", "intermediate-data-engineering-interview-questions"),
    ("Revise system design: practise three scenario questions out loud", "articles:career/scenario-based-data-engineering-interview-questions", "advanced-scenario-based-questions-experienced"),
    ("Revise behavioral: prepare five STAR stories", "articles:career/behavioral-interview-questions", None),
    ("Revise AWS: S3, IAM, Glue and Athena in one diagram", "articles:aws/aws-for-data-engineers", None),
    ("Revise Delta Lake: MERGE, time travel and OPTIMIZE", "articles:databricks/databricks-interview-questions", "intermediate-databricks-interview-questions"),
    ("Re-solve the three DSA problems you found hardest", "articles:dsa/dsa-for-data-engineers", None),
    ("Re-solve the three SQL problems you found hardest", "articles:sql/sql-interview-questions", None),
]

EXERCISE = {  # the weekday hands-on task, by focus track
    "python": "Practice: write a short script that uses {t} on a small CSV",
    "sql": "Practice: write three queries using {t} on a sample table",
    "data-modeling": "Practice: model {t} for an orders dataset on paper",
    "pyspark": "Practice: try {t} in a local PySpark session on sample data",
    "delta-lake": "Practice: try {t} on a local Delta table",
    "airflow": "Practice: use {t} in a small local DAG and run it with dag.test()",
    "snowflake": "Practice: try {t} in a Snowflake trial account or write the SQL you would run",
    "kafka": "Practice: show {t} with the Kafka console tools or a short Python client",
    "spark-streaming": "Practice: try {t} in a small streaming job on a file source",
}

# ---------------------------------------------------------------- second lane
CONCEPTS = [
    "Data lifecycle and ownership", "OLTP and OLAP workloads", "Batch versus streaming", "ETL versus ELT",
    "Storage layers and raw landing", "CSV and JSON trade-offs", "Parquet and columnar storage",
    "Avro and schema evolution", "Partitioning and data layout", "Lake, warehouse and lakehouse",
    "Incremental loading and watermarks", "Idempotent pipeline writes", "Change data capture patterns",
    "Retries and failure recovery", "Data quality checks and contracts", "Testing pipelines and CI/CD",
    "Pipeline observability", "Late data and backfills", "Replay-safe event processing",
    "Secure data access and PII", "Cost and freshness trade-offs",
]
DEV_TOOLS = [
    ("Dev tools for data engineers", "articles:dev-tools/dev-tools-for-data-engineers"),
    ("Command line for data work", "articles:dev-tools/command-line-for-data-work"),
    ("Git for data engineers", "articles:dev-tools/git-for-data-engineers"),
    ("Python environments and packaging", "articles:dev-tools/python-environments-packaging"),
    ("Shell scripting for pipelines", "articles:dev-tools/shell-scripting-for-pipelines"),
    ("Docker for data engineers", "articles:dev-tools/docker-for-data-engineers"),
    ("Makefiles and pre-commit", "articles:dev-tools/makefiles-pre-commit-data-repos"),
]
DBT = [
    ("dbt for data engineers", "articles:dbt/dbt-for-data-engineers"),
    ("dbt models, ref and sources", "articles:dbt/models-ref-and-sources"),
    ("dbt materializations", "articles:dbt/materializations"),
    ("dbt tests and data quality", "articles:dbt/tests-and-data-quality"),
    ("dbt incremental models", "articles:dbt/incremental-models"),
    ("dbt snapshots (SCD Type 2)", "articles:dbt/snapshots-scd-type-2"),
    ("dbt Jinja, macros and packages", "articles:dbt/jinja-macros-and-packages"),
    ("dbt deployment, CI/CD and governance", "articles:dbt/deployment-ci-cd-and-governance"),
]
AWS = [
    "S3 buckets & objects", "Storage classes", "S3 partitioning for analytics", "IAM users/groups/roles",
    "Policies (identity/resource)", "Least privilege", "Assume role & STS", "Glue Data Catalog", "Glue Crawlers",
    "Glue ETL jobs (Spark)", "Athena basics", "Querying S3 with SQL", "Partition projection", "Lambda fundamentals",
    "Triggers & event sources", "State machines", "Redshift architecture", "Distribution styles", "Sort keys",
    "Spark on EMR", "Kinesis Data Streams", "Kinesis Firehose", "Metrics & alarms", "Secrets Manager",
]

# ---------------------------------------------------------------- weekends
PROJECT_STEPS = [  # two per weekend day, in order: (task, ref, anchor)
    ("Lab: create a project folder with a virtual environment and Git", "articles:dev-tools/python-environments-packaging", None),
    ("Lab: read and validate an orders CSV, writing rejects to a file", "articles:python/file-io-csv-json", None),
    ("Lab: add logging and clear error messages to the CSV reader", "articles:python/error-handling-logging", None),
    ("Lab: write pytest tests for the CSV reader", "articles:python/testing-with-pytest", None),
    ("Lab: load the CSV into local Postgres", "articles:python/idempotent-csv-loader", None),
    ("Lab: rerun the load and prove it creates no duplicates", "articles:python/idempotent-csv-loader", None),
    ("Lab: page through a public API and save the results", "articles:python/working-with-apis", None),
    ("Lab: commit the week's code with a short README", "articles:dev-tools/git-for-data-engineers", None),
] + [
    (f"CSV-to-warehouse project, {t}", "projects:csv-to-warehouse-pipeline", a) for t, a in [
        ("step 1: generate the data", "step-1-generate-the-data"),
        ("step 2: staging, rejects and audit tables", "step-2-staging-rejects-and-audit-tables"),
        ("step 3: an idempotent loader", "step-3-an-idempotent-loader"),
        ("step 4: the star schema", "step-4-the-star-schema"),
        ("step 5: data-quality checks", "step-5-data-quality-checks"),
        ("step 6: tests", "step-6-tests"),
        ("write the README with design decisions", "architecture"),
        ("practise explaining it in an interview", "explaining-it-in-an-interview"),
    ]
] + [
    (f"Large-scale batch project, {t}", "projects:large-scale-batch-processing", a) for t, a in [
        ("step 1: get the data", "step-1-get-the-data"),
        ("step 2: write the job as testable functions", "step-2-write-the-job-as-testable-functions"),
        ("step 3: choose the join strategy and prove it", "step-3-choose-the-join-strategy-and-prove-it"),
        ("step 4: find the skew", "step-4-find-the-skew"),
        ("step 5: fix the skew", "step-5-fix-the-skew"),
        ("step 6: write idempotently and prove it", "step-6-write-idempotently-and-prove-it"),
        ("step 7: unit tests", "step-7-unit-tests"),
        ("write the performance analysis", "writing-the-performance-analysis"),
    ]
] + [
    (f"E-commerce analytics project, {t}", "projects:ecommerce-analytics-platform", a) for t, a in [
        ("step 0: definitions before SQL", "step-0-definitions-before-sql"),
        ("step 1: generate and load raw data", "step-1-generate-and-load-raw-data"),
        ("step 2: staging models", "step-2-staging-one-model-per-source-table"),
        ("step 3: the star schema", "step-3-the-star-schema"),
        ("step 4: certified metrics", "step-4-certified-metrics-defined-once"),
        ("step 5: tests", "step-5-tests"),
        ("step 6: port it to dbt", "step-6-port-it-to-dbt"),
        ("practise explaining it in an interview", "explaining-it-in-an-interview"),
    ]
] + [
    (f"S3, PySpark and Snowflake project, {t}", "projects:s3-pyspark-snowflake-pipeline", a) for t, a in [
        ("step 1: generate raw files", "step-1-generate-raw-files"),
        ("step 2: the job, parameterised by date", "step-2-the-job-parameterised-by-date"),
        ("step 3: write curated Parquet idempotently", "step-3-write-curated-parquet-idempotently"),
        ("step 4: unit-test the transformation", "step-4-unit-test-the-transformation"),
        ("step 5: load the warehouse idempotently", "step-5-load-the-warehouse-idempotently"),
        ("step 6: reconcile and schedule", "step-6-reconcile-and-schedule"),
        ("security and cost notes for the README", "security-and-cost-notes-for-the-readme"),
        ("practise explaining it in an interview", "explaining-it-in-an-interview"),
    ]
] + [
    (f"Kafka, Spark and Delta streaming project, {t}", "projects:kafka-spark-delta-streaming", a) for t, a in [
        ("step 1: run Kafka locally", "step-1-run-kafka-locally"),
        ("step 2: the producer", "step-2-the-producer"),
        ("step 3: develop the logic on a local stream", "step-3-develop-the-logic-on-a-local-stream"),
        ("step 4: land raw events idempotently", "step-4-land-raw-events-idempotently"),
        ("step 5: watermarked, deduplicated aggregates", "step-5-watermarked-deduplicated-aggregates"),
        ("step 6: prove recovery and replay", "step-6-prove-recovery-and-replay"),
        ("step 7: switch the source to Kafka", "step-7-switch-the-source-to-kafka"),
        ("practise explaining it in an interview", "explaining-it-in-an-interview"),
    ]
] + [
    ("Record a five-minute demo of your best project", "articles:career/explain-a-data-engineering-project", None),
    ("Update your résumé and portfolio README", "roadmaps:resume-and-project-selection", None),
]

FOUNDATION_LABS = {  # weekend system-design slot in weeks 1-4 (no design yet): a hands-on concept lab
    6: ("Lab: trace one pipeline from source to dashboard and name each stage's owner", "Data lifecycle and ownership"),
    7: ("Lab: write one OLTP and one OLAP query against the same orders table", "OLTP and OLAP workloads"),
    13: ("Lab: save the same data as CSV, JSON and Parquet and compare size and read time", "Parquet and columnar storage"),
    14: ("Lab: write a dt=YYYY-MM-DD partitioned folder layout and read one partition", "Partitioning and data layout"),
    20: ("Lab: load only new rows using a watermark column, then run it twice", "Incremental loading and watermarks"),
    21: ("Lab: make a daily load idempotent with delete-and-insert in one transaction", "Idempotent pipeline writes"),
    27: ("Lab: add row-count, null and duplicate checks to your loader", "Data quality checks and contracts"),
    28: ("Lab: backfill one missed day safely and check the totals", "Late data and backfills"),
}

MOCKS = {  # weekend mock/interview-set slot by week: (Saturday, Sunday) bank hubs or mock types
    1: ("sql", "python"), 2: ("sql", "data-engineering"), 3: ("data-engineering", "sql"),
    4: ("mock:project", "pyspark"), 5: ("pyspark", "mock:sql-dsa"), 6: ("databricks", "mock:project"),
    7: ("airflow", "dbt"), 8: ("mock:sql-dsa", "dbt"), 9: ("snowflake", "mock:design"),
    10: ("kafka", "mock:behavioral"), 11: ("mock:sql-dsa", "scenario-based"), 12: ("mock:design", "behavioral"),
    13: ("mock:full",),
}
MOCK_TASKS = {
    "mock:project": ("Mock: explain your latest project in five minutes, then answer follow-ups", "articles:career/explain-a-data-engineering-project"),
    "mock:sql-dsa": ("Mock: a timed 45-minute round with one SQL and one DSA problem", "roadmaps:interview-preparation-roadmap"),
    "mock:design": ("Mock: a 45-minute data system design round out loud", "roadmaps:system-design-interview-roadmap"),
    "mock:behavioral": ("Mock: answer three behavioral questions with STAR stories", "articles:career/behavioral-interview-data-engineers"),
    "mock:full": ("Mock: a full interview loop (SQL, DSA, design and behavioral)", "roadmaps:interview-preparation-roadmap"),
}
# ---------------------------------------------------------------------------------------------------


def slug(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")


def main() -> None:
    curriculum = json.loads(CURRICULUM.read_text(encoding="utf-8"))
    banks = {b["hub"]: b for b in json.loads(BANKS.read_text(encoding="utf-8"))}

    def tracker_item(tracker: str, topic: str) -> dict:
        rows = {row["topic"]: row for row in curriculum[tracker]["items"]}
        if topic not in rows:
            raise SystemExit(f"{tracker}: unknown topic {topic!r}")
        row = rows[topic]
        ref = row.get("question") if tracker == "dsa" else (row.get("lesson") or row.get("question") or row.get("design"))
        return {"topic": topic, "ref": ref, "tracker": tracker}

    # --- lanes -------------------------------------------------------------------------------------
    sql_topics = [tracker_item("sql", t) for t in SQL_TOPICS]
    assert len(set(SQL_TOPICS)) == len(SQL_TOPICS) == 77

    dsa_rows = curriculum["dsa"]["items"]
    dsa: list[dict] = []
    for group in DSA_GROUPS:
        for level in ("Easy", "Medium"):
            for cat in group:
                dsa += [r for r in dsa_rows if r["category"] == cat and r["difficulty"] == level and r["topic"] not in DSA_SKIP]
    dsa += [next(r for r in dsa_rows if r["topic"] == t) for t in DSA_HARD]
    dsa = [tracker_item("dsa", r["topic"]) for r in dsa]

    def lesson_items(pairs, track):
        return [{"topic": t, "ref": r, "track": track} for t, r in pairs]

    def tracked(tracker, names, track=None):
        return [{**tracker_item(tracker, n), "track": track or tracker} for n in names]

    py = lesson_items(PYTHON, "python")
    # SQL is also a focus track in weeks 1-2. Both slots draw from one ordered SQL stream, so topics
    # arrive in sequence: placeholders in the focus lane take the next SQL topic when they are reached.
    SQL_NEXT = {"placeholder": "sql"}
    sql_lane = sql_topics
    focus = [x for pair in zip(py, [SQL_NEXT] * 10) for x in pair] + [SQL_NEXT] * 2
    focus += tracked("data-modeling", MODELING) + tracked("pyspark", SPARK)
    focus += tracked("pyspark", DELTA, "delta-lake") + tracked("airflow", AIRFLOW)
    focus += tracked("snowflake", SNOWFLAKE) + tracked("kafka", KAFKA)
    focus += tracked("pyspark", STREAMING, "spark-streaming") + tracked("system-design", DESIGN_WEEKDAY)
    second = tracked("de-concepts", CONCEPTS) + lesson_items(DEV_TOOLS, "dev-tools") + lesson_items(DBT, "dbt")
    second += tracked("aws", AWS)
    revision = [{"topic": t, "ref": r, "anchor": a, "track": "review"} for t, r, a in REVISION]

    weekdays = [d for d in range(1, DAYS + 1) if not is_weekend(d)]
    weekends = [d for d in range(1, DAYS + 1) if is_weekend(d)]
    focus_slots = 2 * len(weekdays) + len(weekends)
    content_focus = focus_slots - 11  # the last 11 focus slots (from day 85) are revision
    assert len(focus) == content_focus, f"focus lane has {len(focus)} topics for {content_focus} slots"
    assert len(second) == len(weekdays) - 5, f"second lane has {len(second)} for {len(weekdays) - 5} slots"
    focus += revision[:11]
    second += revision[11:]
    assert len(dsa) == 2 * len(weekdays) + 3 * len(weekends) == 205

    sql_practice_total = len(weekdays) + len(weekends)
    case_rows = {c["scenario"]: c["ref"] for c in curriculum["sql"]["caseStudies"]}
    def page_exists(ref: str) -> bool:
        collection, name = ref.split(":")
        return any((ROOT / "src/content" / collection / name).parent.glob(f"{Path(name).name}.md*"))

    # Technique drills, skipping the few whose case-study page has not been written yet.
    technique = [r for r in curriculum["sql"]["items"] if r.get("technique") and page_exists(r["question"])]
    seen_tech: set[str] = set()
    tech_pick = []
    for band in ("Intermediate", "Advanced", "Expert"):
        for r in technique:
            if r["difficulty"] == band and r["technique"] not in seen_tech:
                seen_tech.add(r["technique"])
                tech_pick.append(r)
    sql_practice = [{"topic": f"LeetCode {n}. {t}", "url": f"https://leetcode.com/problems/{s}/", "track": "sql"} for n, t, s in SQL_PRACTICE]
    for name in SQL_QUESTIONS:
        page = next((ROOT / "src/content/interview-questions/sql").glob(f"{name}.md*"))
        title = re.search(r'^title:\s*"(.*)"', page.read_text(encoding="utf-8"), re.M).group(1)
        sql_practice.append({"topic": f"SQL problem: {title}", "ref": f"interview-questions:sql/{name}", "track": "sql"})
    tech_needed = sql_practice_total - len(sql_practice)
    sql_practice += [{**tracker_item("sql", r["topic"]), "track": "sql"} for r in tech_pick[:tech_needed]]
    assert len(sql_practice) == sql_practice_total

    weekend_sql = [{"topic": "SQL recap: re-solve this week's practice problems without notes", "ref": "articles:sql/sql-fundamentals", "track": "sql"}] * 4
    weekend_sql += [{"topic": f"SQL case study: {c}", "ref": case_rows[c], "track": "sql"} for c in SQL_CASES]
    extra_tech = tech_pick[tech_needed:]
    weekend_sql += [{**tracker_item("sql", r["topic"]), "track": "sql"} for r in extra_tech[: len(weekends) - len(weekend_sql)]]
    assert len(weekend_sql) == len(weekends)

    bank_cursor = {hub: 0 for hub in banks}
    bank_order = {hub: sorted(b["questions"], key=lambda q: (["Beginner", "Intermediate", "Advanced"].index(q["level"]), q["n"]))
                  for hub, b in banks.items()}

    def bank_question(hub: str, verb="Interview question") -> dict:
        q = bank_order[hub][bank_cursor[hub]]
        bank_cursor[hub] += 1
        return {"topic": f"{verb}: {q['title']}", "ref": banks[hub]["ref"], "anchor": q["anchor"], "track": "interview"}

    BANK_FOR = {"python": "python", "sql": "sql", "data-modeling": "data-engineering", "pyspark": "pyspark",
                "delta-lake": "databricks", "airflow": "airflow", "snowflake": "snowflake", "kafka": "kafka",
                "spark-streaming": "pyspark", "system-design": "scenario-based", "review": "behavioral"}

    # --- days --------------------------------------------------------------------------------------
    cur = {"sql": 0, "prac": 0, "dsa": 0, "focus": 0, "second": 0, "wsql": 0, "proj": 0, "design": 0, "wd": 0}

    def take(lane_name, lane, n=1):
        out = lane[cur[lane_name]: cur[lane_name] + n]
        cur[lane_name] += n
        assert len(out) == n, f"{lane_name} ran out"
        return out

    def task(track, minutes, kind, item):
        it = {k: v for k, v in item.items() if k not in ("track",) and v is not None}
        return {"track": track, "minutes": minutes, "kind": kind, "items": [it]}

    history: list[dict] = []  # focus topics already studied, for spaced recall
    days = []
    for day in range(1, DAYS + 1):
        week = (day - 1) // 7 + 1
        phase = next(i for i, p in enumerate(PHASES, 1) if day <= p["end"])
        tasks = []

        def practice_task(minutes):
            p = take("prac", sql_practice)[0]
            if "url" in p:
                return {"track": "sql", "minutes": minutes, "kind": "practice", "items": [],
                        "practice": [{"title": p["topic"].removeprefix("LeetCode "), "url": p["url"]}]}
            return task("sql", minutes, "practice", p)

        def next_focus(n):
            out = []
            for x in take("focus", focus, n):
                out.append({**take("sql", sql_lane)[0], "track": "sql"} if x.get("placeholder") else x)
            return out

        if not is_weekend(day):
            sql_today = take("sql", sql_lane)[0]
            a, b = next_focus(2)
            s = take("second", second)[0]
            tasks.append(task("sql", 15, "learn", sql_today))
            tasks.append(practice_task(20))
            for p in take("dsa", dsa, 2):
                tasks.append(task("dsa", 20, "practice", p))
            tasks.append(task(a["track"], 25, "learn", a))
            tasks.append(task(b["track"], 25, "learn", b))
            tasks.append(task(s["track"], 20, "learn", s))
            hub = "sql" if cur["wd"] % 2 == 0 else BANK_FOR.get(a["track"], "data-engineering")
            if a["track"] == "review":
                hub = "behavioral" if bank_cursor["behavioral"] < len(bank_order["behavioral"]) else "scenario-based"
            cur["wd"] += 1
            tasks.append(task("interview", 10, "mock", bank_question(hub)))
            if a["track"] == "system-design":
                ex = f"Sketch it: draw sources, storage, processing and serving for “{a['topic'].removeprefix('Design ')}”"
            elif a["track"] == "review":
                ex = "Re-do one exercise from this week's weakest topic without notes"
            else:
                ex = EXERCISE.get(a["track"], "Practice: work through the lesson's example for {t} yourself").format(t=a["topic"])
            tasks.append(task("practice", 15, "project", {"topic": ex, "ref": a["ref"]}))
            past = history[-10] if len(history) >= 10 else (history[-1] if history else None)
            if past:
                tasks.append(task("review", 10, "revise", {"topic": f"Recall: explain {past['topic']} without notes", "ref": past["ref"]}))
            else:
                tasks.append(task("review", 10, "revise", {"topic": "Set your start date above and block 3 hours a day for the next 90 days", "ref": "roadmaps:data-engineer-roadmap"}))
            history += [a, b]
            theme, main = a["topic"], a
            if a["track"] == "review":
                theme = "Final revision: " + a["topic"].removeprefix("Revise ").split(":")[0]
        else:
            saturday = (day - 1) % 7 == 5
            tasks.append(practice_task(15))
            tasks.append(task("sql", 15, "practice", take("wsql", weekend_sql)[0]))
            for p in take("dsa", dsa, 3):
                tasks.append(task("dsa", 15, "practice", p))
            f = next_focus(1)[0]
            tasks.append(task(f["track"], 15, "learn", f))
            for t, r, anc in take("proj", PROJECT_STEPS, 2):
                tasks.append(task("project", 25, "project", {"topic": t, "ref": r, "anchor": anc}))
            if day in FOUNDATION_LABS:
                text, concept = FOUNDATION_LABS[day]
                tasks.append(task("de-concepts", 20, "project", {"topic": text, "ref": tracker_item("de-concepts", concept)["ref"]}))
            else:
                tasks.append(task("system-design", 20, "practice", tracker_item("system-design", take("design", DESIGN_WEEKEND)[0])))
            mock = MOCKS[week][0 if saturday or len(MOCKS[week]) == 1 else 1]
            if mock.startswith("mock:"):
                text, ref = MOCK_TASKS[mock]
                tasks.append(task("interview", 10, "mock", {"topic": text, "ref": ref}))
            else:
                q = bank_question(mock, "Answer aloud")
                tasks.append(task("interview", 10, "mock", q))
            if saturday:
                tasks.append(task("review", 5, "revise", {"topic": "Weekly review: re-solve two of this week's DSA problems from memory", "ref": dsa[max(cur['dsa'] - 8, 0)]["ref"]}))
                tasks.append(task("review", 5, "revise", {"topic": "Note three weak topics from this week", "ref": "roadmaps:data-engineer-roadmap"}))
            else:
                last = history[-1] if history else f
                tasks.append(task("review", 5, "revise", {"topic": f"Weekly review: explain this week's {last['track'].replace('-', ' ')} topics in your own words", "ref": last["ref"]}))
                tasks.append(task("review", 5, "revise", {"topic": "Plan next week: schedule time for your three weak topics", "ref": "roadmaps:data-engineer-roadmap"}))
            history.append(f)
            theme, main = f["topic"], f
            if f["track"] == "review":
                theme = "Final revision: " + f["topic"].removeprefix("Revise ").split(":")[0]

        want = WEEKEND_TASKS if is_weekend(day) else WEEKDAY_TASKS
        count = sum(len(t["items"]) + len(t.get("practice", [])) for t in tasks)
        assert count == want, f"day {day}: {count} tasks"
        assert sum(t["minutes"] for t in tasks) == MINUTES, f"day {day}: minutes"
        exercise = next((t["items"][0]["topic"] for t in tasks if t["track"] in ("practice", "project")), "")
        iq = next(t["items"][0]["topic"] for t in tasks if t["track"] == "interview")
        days.append({
            "day": day, "phase": phase, "week": week, "hours": MINUTES // 60, "theme": theme, "tasks": tasks,
            "exercise": exercise,
            "interviewQuestion": iq.split(": ", 1)[-1],
            "outcome": f"You can explain {main['topic']}, and you solved today's SQL and DSA problems without hints.",
            **({"weekly": WEEKLY[week]} if day == min(week * 7, DAYS) else {}),
            **({"monthly": PHASE_REVIEW[day]} if day in PHASE_REVIEW else {}),
        })

    assert cur["dsa"] == len(dsa) and cur["prac"] == len(sql_practice) and cur["proj"] == len(PROJECT_STEPS)
    assert cur["design"] == len(DESIGN_WEEKEND), cur["design"]
    document = {"version": 3, "phases": [{"title": p["title"], "goal": p["goal"]} for p in PHASES], "days": days}
    OUTPUT.write_text(json.dumps(document, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote {len(days)} days to {OUTPUT.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
