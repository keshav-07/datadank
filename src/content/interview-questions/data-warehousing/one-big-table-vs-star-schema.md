---
publishedDate: "2026-10-16"
updatedDate: "2026-10-16"
reviewedDate: "2026-10-16"
title: "One big table or a star schema: how do you decide?"
seoTitle: "One Big Table vs Star Schema: Interview"
description: "One big table vs star schema interview answer: when a wide pre-joined table wins on columnar engines, what it costs in freshness and grain, and the hybrid."
technology: ["data-warehousing", "data-engineering"]
topic: ["one-big-table", "star-schema", "denormalization", "columnar-storage"]
difficulty: "Medium"
questionType: ["architecture", "scenario"]
estimatedMinutes: 10
interviewRelevance: "High"
shortAnswer: "A one big table (OBT) pre-joins a fact with all its dimension attributes into one wide table. On columnar engines it is cheap to store, fast to scan and simple for BI tools and ad hoc users, because there are no joins to get wrong. It costs flexibility: every dimension change (a Type 1 correction, a new attribute) means rebuilding or backfilling the wide table, it supports one grain only, so other questions need another wide table, and the same attribute is repeated in many tables that can drift apart. A star schema keeps dimensions once, handles history and conformance, and supports many facts and grains. My default is a star schema as the modelled source of truth, with OBTs generated from it for specific high-traffic dashboards or tools that cannot join well."
followUps: ["Why are wide tables cheap on columnar warehouses?", "How would you keep an OBT fresh after a dimension correction?", "What goes wrong if you add order-level metrics to a line-level OBT?", "Where do semantic layers fit in this decision?"]
related: ["articles:data-warehousing/normalization-denormalization", "articles:data-warehousing/star-schema", "interview-questions:data-warehousing/kimball-vs-inmon-vs-data-vault", "interview-questions:data-warehousing/design-star-schema-grain"]
versionContext: "SQL verified on PostgreSQL 16 with scripts/verify-examples.py; outputs copied from psql. Columnar storage and compression behaviour described from warehouse documentation, not measured here."
sources:
  - { label: "Kimball Group: Star schemas and OLAP cubes", url: "https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/star-schema-olap-cube/" }
  - { label: "dbt documentation: Marts (how we structure our dbt projects)", url: "https://docs.getdbt.com/best-practices/how-we-structure/4-marts" }
  - { label: "Microsoft Fabric documentation: Dimensional modeling in Fabric Data Warehouse", url: "https://learn.microsoft.com/en-us/fabric/data-warehouse/dimensional-modeling-overview" }
---

## Detailed explanation

### Why OBT became popular

Star schemas were designed partly for row-oriented databases, where storing a customer's segment on every fact row wasted space and joins were the price of keeping it once. Columnar cloud warehouses changed the arithmetic:

- A query reads only the columns it uses, so a 200-column table costs nothing for a 5-column query.
- Repeated values (the same segment on millions of rows) compress very well.
- Joins on large tables are still more work than none, and much more error-prone for casual users.

So a wide, pre-joined table is often the fastest and simplest thing to put under a dashboard.

### What OBT costs

| Concern | Star schema | One big table |
|---|---|---|
| Dimension correction (Type 1) | update one dimension row | rewrite every fact row carrying the old value |
| New attribute | add a column to the dimension | add and backfill a column on the whole table |
| History (Type 2) | versioned rows, as-was and as-is both possible | baked in at build time; "as-is" needs a rebuild |
| Several grains | one fact per grain, shared dimensions | one table per grain, attributes copied into each |
| Consistency across tables | conformed dimensions | copies of attributes that can drift |
| Ease for analysts | needs correct joins | no joins |
| Scan performance | joins at query time | usually fastest |

The grain problem is the subtle one. An OBT at invoice-line grain cannot hold an account-level number such as "seats purchased" without repeating it on every line, which double counts it the moment someone sums it. So "one" big table quickly becomes several, each with its own copy of the account attributes.

### A sensible answer

Model facts and conformed dimensions (the source of truth, where corrections and history are handled once). Generate wide tables from them for specific consumers: a dashboard with heavy traffic, a data science feature table, a tool that cannot join. Rebuild those tables from the star on a schedule or incrementally, and document their grain. A semantic layer can give users the "no joins" experience on top of a star without materialising wide tables at all.

## Example

```sql
CREATE TABLE dim_account (account_key int PRIMARY KEY, account_name text, segment text);
CREATE TABLE dim_plan    (plan_key int PRIMARY KEY, plan_name text, billing_cycle text);
CREATE TABLE fact_invoice_line (invoice_id text, line_no int, account_key int REFERENCES dim_account,
                                plan_key int REFERENCES dim_plan, invoice_month date, amount numeric(10,2),
                                PRIMARY KEY (invoice_id, line_no));

INSERT INTO dim_account VALUES (1, 'Arcadia Foods', 'SMB'), (2, 'Northwind Health', 'Enterprise');
INSERT INTO dim_plan VALUES (1, 'Team', 'monthly'), (2, 'Scale', 'annual');
INSERT INTO fact_invoice_line VALUES
  ('I-1', 1, 1, 1, '2026-09-01', 300), ('I-1', 2, 1, 1, '2026-09-01', 50),
  ('I-2', 1, 2, 2, '2026-09-01', 4000);

-- The OBT: pre-joined, one row per invoice line
CREATE TABLE obt_invoice_line AS
SELECT f.invoice_id, f.line_no, f.invoice_month, f.amount,
       a.account_name, a.segment, p.plan_name, p.billing_cycle
FROM fact_invoice_line f JOIN dim_account a USING (account_key) JOIN dim_plan p USING (plan_key);

-- Sales ops corrects a misclassified account (Type 1: the old value was simply wrong)
UPDATE dim_account SET segment = 'Mid-market' WHERE account_key = 1;

SELECT 'star' AS source, a.segment, sum(f.amount) AS revenue
FROM fact_invoice_line f JOIN dim_account a USING (account_key) GROUP BY a.segment
UNION ALL
SELECT 'obt', segment, sum(amount) FROM obt_invoice_line GROUP BY segment
ORDER BY source DESC, segment;
```

```text
 source |  segment   | revenue
--------+------------+---------
 star   | Enterprise | 4000.00
 star   | Mid-market |  350.00
 obt    | Enterprise | 4000.00
 obt    | SMB        |  350.00
```

The star reflects the correction immediately; the OBT still reports "SMB" until it is rebuilt. Keeping it fresh means rewriting every affected row, which is cheap here and expensive on a few billion lines:

```sql
UPDATE obt_invoice_line o SET segment = a.segment
FROM fact_invoice_line f JOIN dim_account a USING (account_key)
WHERE o.invoice_id = f.invoice_id AND o.line_no = f.line_no AND o.segment IS DISTINCT FROM a.segment;

-- The grain trap: an account-level number copied onto every line
ALTER TABLE obt_invoice_line ADD COLUMN account_seats int;
UPDATE obt_invoice_line SET account_seats = CASE account_name WHEN 'Arcadia Foods' THEN 25 ELSE 400 END;
SELECT sum(account_seats) AS seats_summed_wrongly,
       (SELECT sum(s) FROM (SELECT DISTINCT account_name, account_seats AS s FROM obt_invoice_line) d) AS seats_actual
FROM obt_invoice_line;
```

```text
 seats_summed_wrongly | seats_actual
----------------------+--------------
                  450 |          425
```

Arcadia's 25 seats were counted once per invoice line. In a star, seats live on the account dimension (or an account-grain fact) and are never summed through the line-level fact.

## Trade-offs and pitfalls

- **OBT as the only model**: every new question becomes a new wide table, attributes get copied everywhere, and corrections must be applied N times. Generate OBTs from a modelled layer instead.
- **Mixed-grain columns** in a wide table look harmless and break sums; name them clearly (`account_seats_do_not_sum`) or keep them out.
- **Rebuild cost**: full rebuilds of wide tables on every dimension change get expensive; rebuild incrementally for affected keys, or accept a documented staleness window.
- **Over-normalising the star** (deep snowflakes) removes most of its usability advantage; keep dimensions flat.
- **Assuming joins are always slow**: modern engines join a large fact to small dimensions very efficiently, often with broadcast joins. Measure before materialising.

## Follow-up answers

- **Why wide tables are cheap on columnar engines**: storage and scans are per column, unused columns are not read, and low-cardinality repeated values compress to almost nothing.
- **Fresh after a correction**: rebuild from the star for the affected keys (a merge on the grain), or rebuild the whole table on a schedule and expose its build time to users. Treat the OBT as derived, never edited directly.
- **Order-level metrics in a line-level OBT**: they are repeated per line and inflate every `SUM`. Allocate them to lines with a rule, or keep them in an order-grain table.
- **Semantic layers**: they define joins and metrics once on top of a star, so users get join-free querying and consistent metrics without copying attributes into wide tables; wide tables then become a performance optimisation rather than the model.
