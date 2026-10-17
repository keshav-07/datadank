---
publishedDate: "2026-10-16"
updatedDate: "2026-10-16"
reviewedDate: "2026-10-16"
title: "How do you implement an SCD Type 2 dimension and query it with as-of joins?"
seoTitle: "SCD Type 2 Queries and As-Of Joins"
description: "SCD Type 2 interview answer: design validity ranges, prevent overlapping versions, and query history with as-of joins for as-was and as-is reports in SQL."
technology: ["data-warehousing", "data-engineering"]
topic: ["scd", "scd-type-2", "point-in-time-joins", "dimension-tables"]
difficulty: "Hard"
questionType: ["coding", "conceptual"]
estimatedMinutes: 15
interviewRelevance: "High"
shortAnswer: "A Type 2 row carries a surrogate key, the durable natural key, the tracked attributes and a half-open validity range: valid_from inclusive, valid_to exclusive, with a far-future date (or NULL) on the current row, plus an is_current flag for convenience. Loading closes the current row and inserts a new version when a tracked attribute changes. Querying is where most mistakes happen: to report 'as it was', join each fact to the version whose range contains the fact's timestamp (valid_from <= ts AND ts < valid_to), or better, resolve that surrogate key once at load time and store it on the fact. To report 'as it is now', join through the durable key to the current row. Guard the table with tests or a constraint for exactly one current row and no overlapping or gapped ranges per key."
followUps: ["Why half-open ranges instead of BETWEEN?", "Facts store only the natural key. How do you attach the right version?", "How do you show last year's sales by today's product category?", "How do you detect overlapping or missing ranges?"]
related: ["articles:data-warehousing/slowly-changing-dimensions", "interview-questions:sql/scd-type-2-merge-sql", "interview-questions:data-warehousing/surrogate-vs-natural-keys", "articles:sql/top-n-deduplication-scd-queries"]
versionContext: "SQL verified on PostgreSQL 16 (btree_gist extension for the exclusion constraint) with scripts/verify-examples.py; outputs copied from psql."
sources:
  - { label: "Kimball Group: Type 2 add new row", url: "https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/type-2/" }
  - { label: "PostgreSQL 16 documentation: Range types and exclusion constraints", url: "https://www.postgresql.org/docs/16/rangetypes.html" }
  - { label: "PostgreSQL 16 documentation: btree_gist", url: "https://www.postgresql.org/docs/16/btree-gist.html" }
---

## Detailed explanation

### The table

| Column | Purpose |
|---|---|
| `product_key` | surrogate key, one per version; what facts store |
| `product_id` | natural / durable key, the same for every version of one product |
| tracked attributes (`category`, `brand`) | changes here create a new version |
| `valid_from` | first moment the version is true (inclusive) |
| `valid_to` | first moment it is no longer true (exclusive); `9999-12-31` on the current row |
| `is_current` | redundant with `valid_to`, but makes "current" filters cheap and readable |

**Half-open ranges** (`[valid_from, valid_to)`) are the important design choice. The previous version's `valid_to` equals the next version's `valid_from`, so there are no gaps and no overlaps, and an event at exactly the change instant matches exactly one row. With inclusive `BETWEEN` and "end = next start minus one day", time-of-day events either match twice or fall into a gap.

A far-future end date instead of `NULL` keeps the join predicate simple (`ts < valid_to` works without `coalesce`); pick one convention and use it everywhere.

### Loading, in short

For each staged entity: if no current row exists, insert one; if a tracked attribute differs (`IS DISTINCT FROM`, so NULLs compare correctly), set the current row's `valid_to` to the change time and `is_current` to false, then insert the new version with `valid_from` = change time. Use the **source's change timestamp** where you have one, not the load time, and run both steps in one transaction. The [SCD Type 2 MERGE question](/interview/sql/scd-type-2-merge-sql/) covers the single-statement version.

### Querying: as-was, as-is and as-of

- **As-was** (historical truth): each sale is reported with the category the product had when it was sold. Best practice is to look up the version's surrogate key **when the fact is loaded** and store it, so reports are a plain equality join.
- **As-of join**: when facts only carry the natural key (raw event tables, late-bound marts), join on the natural key **and** the range: `d.product_id = f.product_id AND d.valid_from <= f.sold_at AND f.sold_at < d.valid_to`.
- **As-is** (restated by today's attributes): join the fact's version to the current version through the durable key, so last year's sales roll up under today's category.
- **Snapshot as of a date**: "what did the catalogue look like on 1 July?" is `WHERE valid_from <= '2026-07-01' AND '2026-07-01' < valid_to`.

## Example

```sql
CREATE EXTENSION IF NOT EXISTS btree_gist;

CREATE TABLE dim_product (
  product_key int PRIMARY KEY,
  product_id  text NOT NULL,
  category    text NOT NULL,
  valid_from  timestamp NOT NULL,
  valid_to    timestamp NOT NULL DEFAULT '9999-12-31',
  is_current  boolean NOT NULL,
  CHECK (valid_from < valid_to),
  -- no two versions of one product may overlap in time
  EXCLUDE USING gist (product_id WITH =, tsrange(valid_from, valid_to) WITH &&)
);
CREATE UNIQUE INDEX one_current_version ON dim_product (product_id) WHERE is_current;

INSERT INTO dim_product VALUES
  (1, 'P-7', 'Snacks',         '2026-01-01', '2026-06-15 10:00', false),
  (2, 'P-7', 'Healthy snacks', '2026-06-15 10:00', '9999-12-31', true),
  (3, 'P-9', 'Drinks',         '2026-01-01', '9999-12-31', true);

-- Facts that carry only the natural key and an event timestamp
CREATE TABLE sales (sale_id int, product_id text, sold_at timestamp, amount numeric(8,2));
INSERT INTO sales VALUES
  (1, 'P-7', '2026-05-20 09:00', 40), (2, 'P-7', '2026-06-15 10:00', 25),   -- exactly at the change
  (3, 'P-7', '2026-08-01 18:30', 30), (4, 'P-9', '2026-03-03 12:00', 10);

-- As-of join: each sale matches exactly one version
SELECT s.sale_id, s.sold_at, d.product_key, d.category AS category_as_was
FROM sales s
JOIN dim_product d ON d.product_id = s.product_id
                  AND d.valid_from <= s.sold_at AND s.sold_at < d.valid_to
ORDER BY s.sale_id;
```

```text
 sale_id |       sold_at       | product_key | category_as_was
---------+---------------------+-------------+-----------------
       1 | 2026-05-20 09:00:00 |           1 | Snacks
       2 | 2026-06-15 10:00:00 |           2 | Healthy snacks
       3 | 2026-08-01 18:30:00 |           2 | Healthy snacks
       4 | 2026-03-03 12:00:00 |           3 | Drinks
```

Sale 2 happened at the change instant and matched only the new version. With `BETWEEN valid_from AND valid_to` it would have matched both and been counted twice.

As-was versus as-is totals by category:

```sql
WITH sales_with_version AS (
  SELECT s.*, d.product_key, d.product_id AS durable_id, d.category AS category_as_was
  FROM sales s
  JOIN dim_product d ON d.product_id = s.product_id
                    AND d.valid_from <= s.sold_at AND s.sold_at < d.valid_to
)
SELECT 'as was' AS view, category_as_was AS category, sum(amount) AS revenue
FROM sales_with_version GROUP BY category_as_was
UNION ALL
SELECT 'as is', cur.category, sum(v.amount)
FROM sales_with_version v
JOIN dim_product cur ON cur.product_id = v.durable_id AND cur.is_current
GROUP BY cur.category
ORDER BY view DESC, category;
```

```text
  view  |    category    | revenue
--------+----------------+---------
 as was | Drinks         |   10.00
 as was | Healthy snacks |   55.00
 as was | Snacks         |   40.00
 as is  | Drinks         |   10.00
 as is  | Healthy snacks |   95.00
```

The constraint rejects a load bug that would create overlapping versions:

<!-- expect-error -->
```sql
INSERT INTO dim_product VALUES (4, 'P-9', 'Soft drinks', '2026-05-01', '9999-12-31', false);
```

And a quality check finds gaps between consecutive versions, which the exclusion constraint does not catch:

```sql
SELECT product_id, valid_to AS gap_starts, next_from AS gap_ends
FROM (SELECT product_id, valid_to,
             lead(valid_from) OVER (PARTITION BY product_id ORDER BY valid_from) AS next_from
      FROM dim_product) v
WHERE next_from IS NOT NULL AND next_from <> valid_to;
```

```text
 product_id | gap_starts | gap_ends
------------+------------+----------
(0 rows)
```

## Trade-offs and pitfalls

- **Resolving versions at query time** with range joins is flexible but slower and easy to get wrong. Store the surrogate key on the fact at load time and keep as-of joins for raw data and ad hoc analysis.
- **Using load time as `valid_from`**: a change made on Monday and loaded on Wednesday misattributes two days of facts. Use the source's change timestamp (CDC commit time, `updated_at`).
- **Late changes** (a change dated before the current row's `valid_from`) need the range split and the affected facts re-pointed; plain "close current, insert new" logic gets this wrong.
- **Tracking every column**: a Type 2 change on a column nobody reports by (a last-login timestamp) explodes the table. Choose tracked columns deliberately and treat the rest as Type 1.
- **Date vs timestamp ranges**: if changes can happen several times a day, day-grain ranges lose versions or create zero-length ones.
- **Warehouses without exclusion constraints** (Snowflake, BigQuery, Databricks) need the same rules as tests: one current row per key, no overlaps, no gaps.

## Follow-up answers

- **Half-open ranges**: consecutive versions share a boundary without overlapping, every instant maps to exactly one version, and the predicate works the same for dates and timestamps.
- **Facts with only the natural key**: do the as-of join during the fact load and store the resulting surrogate. Facts that arrive before their dimension version get an inferred member or are held and retried.
- **Last year's sales by today's category**: an as-is report; join the fact's version to the current version through the durable key (or keep a Type 1 "current category" column on every version, which is the Type 6 pattern).
- **Detecting overlaps and gaps**: `lead()` over each key ordered by `valid_from`, comparing each `valid_to` with the next `valid_from`; plus a uniqueness test on current rows. In PostgreSQL an exclusion constraint prevents overlaps outright.
