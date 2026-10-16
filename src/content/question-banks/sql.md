---
title: "SQL interview questions for Data Engineers"
seoTitle: "SQL Interview Questions & Answers for Data Engineers"
description: "50 SQL interview questions for data engineers with answers and runnable queries: joins, window functions, gaps and islands, deduplication and query tuning."
hub: "sql"
label: "SQL"
section: 2
publishedDate: "2026-10-16"
updatedDate: "2026-10-16"
levels: {"Beginner": 15, "Intermediate": 23, "Advanced": 12}
questions: [{"num": 101, "id": "SQL-01", "title": "What is the logical order of SQL query execution?", "topic": "Query Fundamentals", "level": "Beginner", "frequency": "Very Common"}, {"num": 102, "id": "SQL-02", "title": "WHERE vs HAVING", "topic": "Query Fundamentals", "level": "Beginner", "frequency": "Very Common"}, {"num": 103, "id": "SQL-03", "title": "Explain INNER, LEFT, RIGHT, FULL and CROSS joins", "topic": "Joins", "level": "Beginner", "frequency": "Very Common"}, {"num": 104, "id": "SQL-04", "title": "Self join: find employees and their managers", "topic": "Joins", "level": "Beginner", "frequency": "Very Common"}, {"num": 105, "id": "SQL-05", "title": "EXISTS vs IN vs NOT IN (and the NULL trap)", "topic": "Joins & Subqueries", "level": "Intermediate", "frequency": "Very Common"}, {"num": 106, "id": "SQL-06", "title": "UNION vs UNION ALL", "topic": "Set Operations", "level": "Beginner", "frequency": "Very Common"}, {"num": 107, "id": "SQL-07", "title": "Find the Nth highest salary (several approaches)", "topic": "Ranking", "level": "Beginner", "frequency": "Very Common"}, {"num": 108, "id": "SQL-08", "title": "ROW_NUMBER vs RANK vs DENSE_RANK", "topic": "Window Functions", "level": "Beginner", "frequency": "Very Common"}, {"num": 109, "id": "SQL-09", "title": "Window functions: PARTITION BY, ORDER BY and frames", "topic": "Window Functions", "level": "Beginner", "frequency": "Very Common"}, {"num": 110, "id": "SQL-10", "title": "ROWS vs RANGE window frames", "topic": "Window Functions", "level": "Intermediate", "frequency": "Common"}, {"num": 111, "id": "SQL-11", "title": "Running totals and moving averages", "topic": "Window Functions", "level": "Intermediate", "frequency": "Very Common"}, {"num": 112, "id": "SQL-12", "title": "LAG/LEAD: day-over-day change", "topic": "Window Functions", "level": "Intermediate", "frequency": "Very Common"}, {"num": 113, "id": "SQL-13", "title": "Why does LAST_VALUE return unexpected results? (default frame)", "topic": "Window Functions", "level": "Intermediate", "frequency": "Common"}, {"num": 114, "id": "SQL-14", "title": "Top N rows per group", "topic": "Window Functions", "level": "Intermediate", "frequency": "Very Common"}, {"num": 115, "id": "SQL-15", "title": "Delete duplicates but keep one row", "topic": "De-duplication", "level": "Intermediate", "frequency": "Very Common"}, {"num": 116, "id": "SQL-16", "title": "Find duplicate records", "topic": "De-duplication", "level": "Beginner", "frequency": "Very Common"}, {"num": 117, "id": "SQL-17", "title": "Gaps and islands: longest streak of consecutive login days", "topic": "Gaps & Islands", "level": "Advanced", "frequency": "Very Common"}, {"num": 118, "id": "SQL-18", "title": "Find missing IDs or dates in a sequence", "topic": "Gaps & Islands", "level": "Intermediate", "frequency": "Common"}, {"num": 119, "id": "SQL-19", "title": "CTE vs subquery vs temporary table", "topic": "CTEs", "level": "Beginner", "frequency": "Very Common"}, {"num": 120, "id": "SQL-20", "title": "Recursive CTE: walk an org hierarchy", "topic": "CTEs", "level": "Advanced", "frequency": "Common"}, {"num": 121, "id": "SQL-21", "title": "Generate a date spine with a recursive CTE or generator", "topic": "CTEs", "level": "Intermediate", "frequency": "Occasional"}, {"num": 122, "id": "SQL-22", "title": "Pivot rows to columns (CASE and PIVOT)", "topic": "Pivot / Unpivot", "level": "Intermediate", "frequency": "Common"}, {"num": 123, "id": "SQL-23", "title": "Unpivot columns to rows", "topic": "Pivot / Unpivot", "level": "Intermediate", "frequency": "Common"}, {"num": 124, "id": "SQL-24", "title": "Correlated subqueries and their performance", "topic": "Subqueries", "level": "Intermediate", "frequency": "Common"}, {"num": 125, "id": "SQL-25", "title": "COUNT(*) vs COUNT(col) vs COUNT(DISTINCT col)", "topic": "Aggregation", "level": "Beginner", "frequency": "Common"}, {"num": 126, "id": "SQL-26", "title": "NULL handling: three-valued logic, COALESCE, NULLIF", "topic": "NULL Handling", "level": "Beginner", "frequency": "Very Common"}, {"num": 127, "id": "SQL-27", "title": "Diagnose a join that multiplies rows (fan-out)", "topic": "Joins", "level": "Intermediate", "frequency": "Very Common"}, {"num": 128, "id": "SQL-28", "title": "New vs returning users and cumulative distinct counts", "topic": "Analytics Patterns", "level": "Advanced", "frequency": "Common"}, {"num": 129, "id": "SQL-29", "title": "Compute median and percentiles", "topic": "Aggregation", "level": "Intermediate", "frequency": "Common"}, {"num": 130, "id": "SQL-30", "title": "Month-over-month growth percentage", "topic": "Analytics Patterns", "level": "Intermediate", "frequency": "Very Common"}, {"num": 131, "id": "SQL-31", "title": "Build a cohort retention table", "topic": "Analytics Patterns", "level": "Advanced", "frequency": "Common"}, {"num": 132, "id": "SQL-32", "title": "Sessionise events with a 30-minute inactivity gap", "topic": "Analytics Patterns", "level": "Advanced", "frequency": "Common"}, {"num": 133, "id": "SQL-33", "title": "Merge overlapping date ranges", "topic": "Gaps & Islands", "level": "Advanced", "frequency": "Occasional"}, {"num": 134, "id": "SQL-34", "title": "Customers who bought product A but never product B", "topic": "Joins & Subqueries", "level": "Intermediate", "frequency": "Common"}, {"num": 135, "id": "SQL-35", "title": "Year-to-date and same-period-last-year comparisons", "topic": "Analytics Patterns", "level": "Intermediate", "frequency": "Common"}, {"num": 136, "id": "SQL-36", "title": "Conditional aggregation (SUM(CASE…) and FILTER)", "topic": "Aggregation", "level": "Beginner", "frequency": "Common"}, {"num": 137, "id": "SQL-37", "title": "MERGE for upserts", "topic": "DML", "level": "Intermediate", "frequency": "Very Common"}, {"num": 138, "id": "SQL-38", "title": "DELETE vs TRUNCATE vs DROP", "topic": "DDL & DML", "level": "Beginner", "frequency": "Very Common"}, {"num": 139, "id": "SQL-39", "title": "Views vs materialized views", "topic": "Views", "level": "Intermediate", "frequency": "Very Common"}, {"num": 140, "id": "SQL-40", "title": "Indexes (clustered/non-clustered) and why columnar warehouses differ", "topic": "Query Optimisation", "level": "Advanced", "frequency": "Very Common"}, {"num": 141, "id": "SQL-41", "title": "How do you read an execution plan?", "topic": "Query Optimisation", "level": "Intermediate", "frequency": "Very Common"}, {"num": 142, "id": "SQL-42", "title": "A practical query optimisation checklist", "topic": "Query Optimisation", "level": "Intermediate", "frequency": "Very Common"}, {"num": 143, "id": "SQL-43", "title": "What makes a predicate sargable?", "topic": "Query Optimisation", "level": "Advanced", "frequency": "Common"}, {"num": 144, "id": "SQL-44", "title": "Nested loop vs hash vs merge join algorithms", "topic": "Query Optimisation", "level": "Advanced", "frequency": "Common"}, {"num": 145, "id": "SQL-45", "title": "Window function vs GROUP BY + join: performance trade-offs", "topic": "Query Optimisation", "level": "Advanced", "frequency": "Occasional"}, {"num": 146, "id": "SQL-46", "title": "Transaction isolation levels and anomalies", "topic": "Transactions", "level": "Advanced", "frequency": "Common"}, {"num": 147, "id": "SQL-47", "title": "ACID properties", "topic": "Transactions", "level": "Beginner", "frequency": "Very Common"}, {"num": 148, "id": "SQL-48", "title": "Stored procedures vs user-defined functions", "topic": "Procedural SQL", "level": "Intermediate", "frequency": "Common"}, {"num": 149, "id": "SQL-49", "title": "Data-type traps: integer division, FLOAT vs DECIMAL, time zones", "topic": "Data Types", "level": "Intermediate", "frequency": "Common"}, {"num": 150, "id": "SQL-50", "title": "Approximate aggregation (APPROX_COUNT_DISTINCT, HyperLogLog)", "topic": "Aggregation", "level": "Advanced", "frequency": "Occasional"}]
---

<details class="qb-item" id="q101" data-level="Beginner">
<summary><span class="qb-num">Q101</span><h3 class="qb-title">What is the logical order of SQL query execution?</h3><span class="qb-meta">Beginner · Very Common · Query Fundamentals</span></summary>

**Short answer**

Logically, a SELECT is evaluated as FROM/JOIN → WHERE → GROUP BY → HAVING → window functions → SELECT → DISTINCT → ORDER BY → LIMIT/OFFSET. Some dialects also evaluate QUALIFY after the window functions. This order explains why a WHERE clause can't reference a SELECT alias or an aggregate, but ORDER BY can.

**Detailed explanation**

The order you write clauses in is not the order they are logically processed.

1. **FROM and JOINs:** build the working row set from the tables, including join conditions (`ON`).
2. **WHERE:** filter individual rows. Aggregates and window functions don't exist yet, so `WHERE SUM(x) > 10` is invalid.
3. **GROUP BY:** collapse rows into groups.
4. **HAVING:** filter groups, so aggregates are allowed here.
5. **Window functions:** computed over the remaining rows (after grouping, if any).
6. **QUALIFY** (Snowflake, Databricks, BigQuery, Teradata): filter on window-function results.
7. **SELECT:** compute output expressions and assign aliases.
8. **DISTINCT:** remove duplicate output rows.
9. **ORDER BY:** sort. Aliases from SELECT are visible here.
10. **LIMIT / OFFSET / FETCH:** take a slice.

**Practical consequences**
- You can't filter on a window function in WHERE. Use a subquery or CTE, or QUALIFY where supported.
- A column alias usually can't be used in WHERE or GROUP BY in ANSI SQL. Snowflake and some other engines relax this as an extension, but don't rely on it for portable code.
- An `ON` condition versus a `WHERE` condition matters for outer joins: filtering the right table in WHERE turns a LEFT JOIN into an inner join (see [Q103](/interview/sql/#q103)).

This is the **logical** order. The optimizer may physically reorder operations, for example pushing filters below joins, as long as the result is the same (see [Q141](/interview/sql/#q141)).

**Example**

```sql
-- Dialect: ANSI SQL
-- Invalid: WHERE runs before aggregation
-- SELECT customer_id, SUM(amount) AS total FROM orders WHERE SUM(amount) > 1000 GROUP BY customer_id;

-- Valid: filter the groups in HAVING; ORDER BY can use the alias
SELECT customer_id, SUM(amount) AS total_spend
FROM orders
WHERE order_status = 'completed'      -- row filter, before grouping
GROUP BY customer_id
HAVING SUM(amount) > 1000             -- group filter, after grouping
ORDER BY total_spend DESC;            -- alias visible here
```

**Real-world use case**

A report filtered `WHERE rn = 1` on a ROW_NUMBER defined in the same SELECT and failed. Knowing the order, the engineer moved the window into a CTE (or used QUALIFY in Snowflake), and the query worked.

**Common mistakes and red flags**

- Saying SQL executes "top to bottom as written".
- Using aggregates in WHERE.
- Not knowing where window functions sit relative to WHERE and GROUP BY.

**Follow-up questions**

- Can you GROUP BY an alias? — Not in ANSI SQL; several engines (Snowflake, BigQuery, Databricks) allow it as an extension.
- Where does QUALIFY fit? — After window functions, before DISTINCT/ORDER BY; it filters on window results.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q102" data-level="Beginner">
<summary><span class="qb-num">Q102</span><h3 class="qb-title">WHERE vs HAVING</h3><span class="qb-meta">Beginner · Very Common · Query Fundamentals</span></summary>

**Short answer**

WHERE filters individual rows before grouping and cannot use aggregate functions. HAVING filters groups after GROUP BY and can use aggregates. Put row-level conditions in WHERE, which is cheaper because fewer rows get aggregated, and aggregate conditions in HAVING.

**Detailed explanation**

| Aspect | WHERE | HAVING |
|---|---|---|
| Applies to | Rows | Groups |
| When | Before GROUP BY | After GROUP BY |
| Aggregates allowed | No | Yes |
| Typical use | `status = 'paid'`, date range | `COUNT(*) > 5`, `SUM(amount) > 1000` |
| Performance | Reduces data early | Runs on already-aggregated rows |

**Notes worth adding in an interview**
- Putting a non-aggregate condition in HAVING (`HAVING country = 'IN'`) is legal in most engines if the column is grouped, but it is clearer in WHERE. Optimizers usually push it down anyway. Still, write intent clearly.
- HAVING without GROUP BY treats the whole result as one group: `SELECT COUNT(*) FROM orders HAVING COUNT(*) > 0`.
- Filtering on window functions needs neither WHERE nor HAVING. Use a subquery, CTE or QUALIFY (see [Q101](/interview/sql/#q101)).
- If you need both row filters and group filters, use both. WHERE shrinks the input, and HAVING filters the output.
- In distributed engines, a selective WHERE also reduces the data shuffled for the GROUP BY. That is often the single biggest cost of an aggregation query, so moving a condition from HAVING to WHERE (when it is row-level) can matter at scale.

**Example**

```sql
-- Dialect: ANSI SQL
-- Customers in India with more than 3 paid orders in 2026
SELECT customer_id,
       COUNT(*)    AS paid_orders,
       SUM(amount) AS paid_amount
FROM orders
WHERE country = 'IN'
  AND order_status = 'paid'
  AND order_date >= DATE '2026-01-01'
GROUP BY customer_id
HAVING COUNT(*) > 3;
```

Sample input:
| customer_id | country | order_status | order_date |
|---|---|---|---|
| 1 | IN | paid | 2026-01-05 |
| 1 | IN | paid | 2026-02-10 |
| 1 | IN | paid | 2026-03-02 |
| 1 | IN | paid | 2026-03-20 |
| 2 | IN | paid | 2026-01-08 |

Output: only customer 1 (4 paid orders). Customer 2 has 1 and is removed by HAVING.

**Real-world use case**

A fraud analyst flags merchants with more than 50 refunds in a day. WHERE restricts rows to refund transactions for that day, and HAVING keeps merchants whose refund count exceeds 50.

**Common mistakes and red flags**

- `WHERE COUNT(*) > 3`.
- Putting every condition in HAVING "to be safe".
- Not knowing HAVING can be used without GROUP BY.

**Follow-up questions**

- Can HAVING reference a SELECT alias? — In some engines (MySQL, Snowflake) yes; in ANSI SQL repeat the expression.
- Which is faster? — WHERE generally, because it reduces rows before the expensive grouping step.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q103" data-level="Beginner">
<summary><span class="qb-num">Q103</span><h3 class="qb-title">Explain INNER, LEFT, RIGHT, FULL and CROSS joins</h3><span class="qb-meta">Beginner · Very Common · Joins</span></summary>

**Short answer**

An INNER JOIN returns only rows that match in both tables. A LEFT JOIN returns all left rows plus matches, with NULLs where there is no match. RIGHT is the mirror image. FULL OUTER returns all rows from both sides, matched where possible. CROSS JOIN returns every combination of rows (a Cartesian product).

**Detailed explanation**

| Join | Rows returned | Unmatched rows |
|---|---|---|
| INNER | Matching pairs | Dropped from both sides |
| LEFT [OUTER] | All left + matches | Right columns NULL |
| RIGHT [OUTER] | All right + matches | Left columns NULL |
| FULL [OUTER] | All rows from both | NULLs on the missing side |
| CROSS | Every left × every right | No condition |

**Important behaviours**
- **Duplicates multiply.** If a key appears 3 times on the left and 2 times on the right, an inner join produces 6 rows for it (see [Q127](/interview/sql/#q127)).
- **NULL keys never match** with `=`, so rows with NULL join keys drop out of inner joins.
- **ON vs WHERE in outer joins.** Conditions on the right table belong in `ON` if you want to keep unmatched left rows. Putting them in `WHERE` removes those rows (their right columns are NULL), which silently turns the query into an inner join.
- **Anti-join pattern:** `LEFT JOIN ... WHERE right.key IS NULL` finds left rows with no match (see [Q105](/interview/sql/#q105)).
- Most teams avoid RIGHT JOIN and swap the table order instead, for readability.

**Example**

```sql
-- Dialect: ANSI SQL
-- customers: (1,'Asha'), (2,'Ben'), (3,'Chen')
-- orders:    (101, cust 1, 'paid'), (102, cust 1, 'cancelled'), (103, cust 4, 'paid')

-- Keep all customers; attach only PAID orders (condition in ON)
SELECT c.customer_id, c.name, o.order_id
FROM customers c
LEFT JOIN orders o
  ON o.customer_id = c.customer_id
 AND o.status = 'paid';
-- Result: (1,Asha,101), (2,Ben,NULL), (3,Chen,NULL)

-- Same filter in WHERE turns it into an inner join
SELECT c.customer_id, c.name, o.order_id
FROM customers c
LEFT JOIN orders o ON o.customer_id = c.customer_id
WHERE o.status = 'paid';
-- Result: (1,Asha,101) only

-- FULL OUTER also keeps the orphan order for customer 4
SELECT COALESCE(c.customer_id, o.customer_id) AS customer_id, c.name, o.order_id
FROM customers c
FULL OUTER JOIN orders o ON o.customer_id = c.customer_id;
```

**Real-world use case**

A reconciliation between a source extract and a warehouse table uses FULL OUTER JOIN on the key. Rows with NULLs on either side show records missing from one system.

**Common mistakes and red flags**

- Filtering the right table in WHERE after a LEFT JOIN and expecting unmatched rows.
- Accidental cross joins from missing join conditions.
- Not anticipating row multiplication from duplicate keys.

**Follow-up questions**

- When is CROSS JOIN useful? — Generating combinations (date spine × stores), or attaching a single-row parameter table.
- What is a NATURAL JOIN? — A join on all same-named columns; avoid it in production because schema changes alter its behaviour.

**In depth:** [Explain INNER JOIN vs LEFT JOIN with a practical example.](/interview/sql/inner-vs-left-join/)

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q104" data-level="Beginner">
<summary><span class="qb-num">Q104</span><h3 class="qb-title">Self join: find employees and their managers</h3><span class="qb-meta">Beginner · Very Common · Joins</span></summary>

**Short answer**

A self join joins a table to itself with two aliases, so rows can be compared or related within the same table. The classic example is `employees e LEFT JOIN employees m ON e.manager_id = m.employee_id`, which shows each employee next to their manager's name.

**Detailed explanation**

Self joins are needed whenever a table contains a relationship among its own rows:
- employee → manager (hierarchies);
- comparing each row with another row of the same entity (this month vs last month, before window functions were common);
- finding pairs (customers in the same city, overlapping bookings).

**Key points**
- Use **clear aliases** (`e` for employee, `m` for manager).
- Use a **LEFT JOIN** if top-level employees (the CEO, with a NULL `manager_id`) should still appear.
- For **pairs**, add `a.id < b.id` to avoid self-pairs and mirrored duplicates.
- Self joins only resolve **one level** of hierarchy. For full depth (all reports under a VP), use a recursive CTE (see [Q120](/interview/sql/#q120)).
- Many "compare to previous row" self joins are better written with `LAG`/`LEAD` (see [Q112](/interview/sql/#q112)), which scan once instead of joining.
- In a dimensional model, the hierarchy is often flattened at load time into columns such as `manager_name` and `department_head` on `dim_employee`, or into a bridge table. Reports then avoid self joins entirely.

**Example**

```sql
-- Dialect: ANSI SQL
-- employees(employee_id, full_name, manager_id, salary)
SELECT e.employee_id,
       e.full_name           AS employee,
       m.full_name           AS manager
FROM employees e
LEFT JOIN employees m
  ON e.manager_id = m.employee_id
ORDER BY e.employee_id;

-- Employees who earn more than their manager
SELECT e.full_name AS employee, e.salary, m.full_name AS manager, m.salary AS manager_salary
FROM employees e
JOIN employees m ON e.manager_id = m.employee_id
WHERE e.salary > m.salary;
```

Sample input:
| employee_id | full_name | manager_id | salary |
|---|---|---|---|
| 1 | Ravi | NULL | 200000 |
| 2 | Meera | 1 | 150000 |
| 3 | Tom | 2 | 160000 |

First query: (1, Ravi, NULL), (2, Meera, Ravi), (3, Tom, Meera).
Second query: Tom, 160000, Meera, 150000.

**Real-world use case**

An HR analytics model shows each employee's direct manager and the manager's department, so headcount can be attributed to the manager's cost centre.

**Common mistakes and red flags**

- Using an inner join and losing the CEO.
- Forgetting `a.id < b.id` when generating pairs, which doubles them.
- Trying to get multi-level hierarchies with repeated self joins of unknown depth.

**Follow-up questions**

- How do you get the whole chain up to the CEO? — A recursive CTE walking `manager_id` upward (see [Q120](/interview/sql/#q120)).
- How do you find duplicate customers in the same city? — Self join on city with `a.id < b.id` and a similarity condition.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q105" data-level="Intermediate">
<summary><span class="qb-num">Q105</span><h3 class="qb-title">EXISTS vs IN vs NOT IN (and the NULL trap)</h3><span class="qb-meta">Intermediate · Very Common · Joins &amp; Subqueries</span></summary>

**Short answer**

IN and EXISTS both express a semi-join ("rows that have a match"), and modern optimizers usually run them the same way. The trap is `NOT IN`: if the subquery returns any NULL, `NOT IN` returns no rows at all. Use `NOT EXISTS` or a `LEFT JOIN ... IS NULL` anti-join instead.

**Detailed explanation**

**Semi-join (has a match)**
- `WHERE customer_id IN (SELECT customer_id FROM orders)`
- `WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.customer_id)`
- Both return each outer row at most once, unlike a JOIN, which can duplicate outer rows when there are several matches.

**Anti-join (has no match)**
- `NOT EXISTS (...)`: correct with NULLs.
- `LEFT JOIN ... WHERE o.customer_id IS NULL`: correct, and common.
- `NOT IN (subquery)`: **dangerous**.

**Why NOT IN fails with NULL.** `x NOT IN (1, 2, NULL)` expands to `x <> 1 AND x <> 2 AND x <> NULL`. `x <> NULL` is UNKNOWN, so the whole predicate is never TRUE and the query returns nothing (see [Q126](/interview/sql/#q126) on three-valued logic).

| Construct | NULL in subquery | Duplicates outer rows | Readability |
|---|---|---|---|
| IN | Fine for IN | No | High |
| EXISTS | Fine | No | High |
| JOIN | Fine | Yes, if many matches | Medium |
| NOT IN | **Returns no rows if any NULL** | No | Misleading |
| NOT EXISTS | Fine | No | High |
| LEFT JOIN … IS NULL | Fine | No (if you filter) | Medium |

Performance: on modern engines (Snowflake, Spark, PostgreSQL, SQL Server), IN and EXISTS usually compile to the same semi-join plan. Choose by correctness and clarity.

**Example**

```sql
-- Dialect: ANSI SQL
-- customers: 1, 2, 3     orders.customer_id: 1, NULL
-- Customers with no orders

-- Wrong: returns zero rows because orders contains a NULL customer_id
SELECT customer_id FROM customers
WHERE customer_id NOT IN (SELECT customer_id FROM orders);

-- Right: returns 2 and 3
SELECT c.customer_id
FROM customers c
WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.customer_id);
```

**Real-world use case**

A "customers who never ordered" report suddenly showed zero rows after a guest-checkout feature started writing orders with NULL `customer_id`. Switching from `NOT IN` to `NOT EXISTS` restored correct results.

**Common mistakes and red flags**

- Using `NOT IN` against nullable columns.
- Using JOIN for "has a match" and then adding DISTINCT to hide duplicates.
- Claiming EXISTS is always faster than IN.

**Follow-up questions**

- How do you make NOT IN safe? — Add `WHERE customer_id IS NOT NULL` in the subquery, but NOT EXISTS is clearer.
- What does Spark call these? — `left_semi` and `left_anti` join types.

**In depth:** [Find customers who never placed an order, and explain NOT IN vs NOT EXISTS.](/interview/sql/customers-without-orders-anti-join/)

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q106" data-level="Beginner">
<summary><span class="qb-num">Q106</span><h3 class="qb-title">UNION vs UNION ALL</h3><span class="qb-meta">Beginner · Very Common · Set Operations</span></summary>

**Short answer**

UNION combines result sets and removes duplicate rows, which requires a sort or hash step. UNION ALL keeps every row and is faster. Use UNION ALL by default when combining disjoint sources or when duplicates are meaningful, and UNION only when you actually need de-duplication.

**Detailed explanation**

**Rules for both**
- Same number of columns, in the same order, with compatible types.
- Column names come from the first SELECT.
- ORDER BY applies to the combined result, written once at the end.

**Differences**
| Aspect | UNION | UNION ALL |
|---|---|---|
| Duplicates | Removed (across *and* within inputs) | Kept |
| Cost | Extra distinct step (sort/hash) | Concatenation only |
| Typical use | Combining overlapping lists where uniqueness is needed | Appending partitions, sources, history |

**Subtleties**
- UNION removes duplicates *within* each input too, not just across them. That can silently drop legitimate rows, such as two identical transactions.
- Two NULLs are treated as duplicates for UNION's de-duplication, even though `NULL = NULL` is not TRUE in a WHERE clause.
- Related set operators: `INTERSECT` (rows in both) and `EXCEPT`/`MINUS` (rows in the first but not the second). They also de-duplicate by default. `EXCEPT ALL` and `INTERSECT ALL` exist in some engines.
- In data pipelines, UNION ALL plus explicit dedupe logic (ROW_NUMBER on a key) is usually clearer than UNION, because you control which duplicate wins.

**Example**

```sql
-- Dialect: ANSI SQL
-- Combine orders from two regional systems; add the source for lineage
SELECT order_id, amount, 'EU' AS source_region FROM eu_orders
UNION ALL
SELECT order_id, amount, 'US' AS source_region FROM us_orders;

-- Distinct list of emails across two marketing lists
SELECT LOWER(email) AS email FROM newsletter_signups
UNION
SELECT LOWER(email) FROM webinar_signups;
```
Sample: list A = {a@x.com, b@x.com}, list B = {b@x.com, c@x.com}. UNION gives 3 rows. UNION ALL gives 4.

**Real-world use case**

A finance view used UNION to combine payment tables, and two genuine identical payments on the same day (same amount and customer, no ID column selected) collapsed into one. Switching to UNION ALL and selecting `payment_id` fixed the revenue understatement.

**Common mistakes and red flags**

- Defaulting to UNION and paying for an unnecessary distinct.
- Not realising UNION de-duplicates within a single input.
- Mismatched column order that type-coerces silently.

**Follow-up questions**

- How do you find rows in table A but not B? — `EXCEPT`/`MINUS`, or NOT EXISTS for keyed comparison.
- Does Snowflake support UNION BY NAME? — Yes, Snowflake supports `UNION [ALL] BY NAME`, which matches columns by name rather than position.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q107" data-level="Beginner">
<summary><span class="qb-num">Q107</span><h3 class="qb-title">Find the Nth highest salary (several approaches)</h3><span class="qb-meta">Beginner · Very Common · Ranking</span></summary>

**Short answer**

The robust approach is `DENSE_RANK() OVER (ORDER BY salary DESC)` and filtering on rank = N, which handles ties and works per group with PARTITION BY. Alternatives are `ORDER BY ... LIMIT 1 OFFSET N-1` on distinct salaries, or a correlated subquery counting distinct higher salaries.

**Detailed explanation**

First, clarify what "Nth highest" means when there are ties. If salaries are 100, 100, 90, is the 2nd highest 100 or 90? Usually interviewers mean the 2nd highest **distinct** value, which is 90. That points to DENSE_RANK.

**Approaches**
1. **DENSE_RANK** (preferred): ties share a rank and no ranks are skipped. Filter `rnk = N`. Works per department with `PARTITION BY dept_id`.
2. **Distinct + ORDER BY + OFFSET:** `SELECT DISTINCT salary ... ORDER BY salary DESC LIMIT 1 OFFSET N-1`. Simple for one global answer, awkward per group.
3. **Correlated subquery:** the salary where exactly N−1 distinct salaries are higher. Portable, but O(n²) without optimisation.
4. **MAX below MAX** (only for N = 2): `SELECT MAX(salary) WHERE salary < (SELECT MAX(salary) ...)`.

**Edge cases**
- If fewer than N distinct salaries exist, return NULL or no row. Clarify which the interviewer wants.
- NULL salaries: exclude them explicitly, because ordering puts NULLs first or last depending on the engine.

**Example**

```sql
-- Dialect: ANSI SQL
-- 1) Per-department 2nd highest distinct salary
WITH ranked AS (
    SELECT dept_id, employee_id, salary,
           DENSE_RANK() OVER (PARTITION BY dept_id ORDER BY salary DESC) AS rnk
    FROM employees
    WHERE salary IS NOT NULL
)
SELECT dept_id, employee_id, salary
FROM ranked
WHERE rnk = 2;

-- 2) Global Nth (N = 3) using OFFSET (ANSI FETCH syntax)
SELECT DISTINCT salary
FROM employees
WHERE salary IS NOT NULL
ORDER BY salary DESC
OFFSET 2 ROWS FETCH NEXT 1 ROW ONLY;

-- 3) Correlated subquery, N = 3
SELECT DISTINCT e1.salary
FROM employees e1
WHERE 2 = (SELECT COUNT(DISTINCT e2.salary) FROM employees e2 WHERE e2.salary > e1.salary);
```
Sample: salaries in dept 10 are 120, 120, 100, 90. DENSE_RANK gives 1, 1, 2, 3, so the 2nd highest is 100.

**Real-world use case**

A compensation dashboard shows the top-3 salary bands per department. DENSE_RANK per department handles tied salaries without skipping band numbers.

**Common mistakes and red flags**

- Using ROW_NUMBER, which gives an arbitrary winner among ties.
- `LIMIT N` without DISTINCT, which returns the Nth row rather than the Nth value.
- Not asking how ties should be treated.

**Follow-up questions**

- What if they want employees tied at the Nth value? — DENSE_RANK returns all of them because they share the rank.
- RANK vs DENSE_RANK here? — RANK skips numbers after ties (1, 1, 3), so "rank = 2" might not exist (see [Q108](/interview/sql/#q108)).

**In depth:** [Find the second-highest salary without using a simple MAX approach.](/interview/sql/second-highest-salary/)

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q108" data-level="Beginner">
<summary><span class="qb-num">Q108</span><h3 class="qb-title">ROW_NUMBER vs RANK vs DENSE_RANK</h3><span class="qb-meta">Beginner · Very Common · Window Functions</span></summary>

**Short answer**

All three number rows within a window by an ORDER BY. ROW_NUMBER gives unique sequential numbers, breaking ties arbitrarily. RANK gives tied rows the same rank and skips numbers afterwards (1, 1, 3). DENSE_RANK gives ties the same rank without gaps (1, 1, 2).

**Detailed explanation**

| Score | ROW_NUMBER | RANK | DENSE_RANK |
|---|---|---|---|
| 95 | 1 | 1 | 1 |
| 95 | 2 | 1 | 1 |
| 90 | 3 | 3 | 2 |
| 85 | 4 | 4 | 3 |

**When to use which**
- **ROW_NUMBER:** de-duplication ("keep the latest row per key"; see [Q115](/interview/sql/#q115)), pagination, and picking exactly one row per group. Make the ORDER BY **deterministic** by adding tie-breaker columns, otherwise reruns can pick different rows.
- **RANK:** competition-style ranking where ties consume positions (two people tied for first means nobody is second).
- **DENSE_RANK:** "Nth distinct value" problems (see [Q107](/interview/sql/#q107)), and tiering into bands.

**Related functions**
- `NTILE(n)`: splits rows into n roughly equal buckets (quartiles).
- `PERCENT_RANK()`: computed as (rank − 1)/(rows − 1).
- `CUME_DIST()`: cumulative distribution.

All of them require `ORDER BY` in the OVER clause. `PARTITION BY` restarts numbering per group.

In Snowflake, Databricks and BigQuery you can filter on these directly with `QUALIFY` (for example `QUALIFY ROW_NUMBER() OVER (...) = 1`). Elsewhere, wrap the query in a CTE and filter in the outer query, because window results aren't available in WHERE (see [Q101](/interview/sql/#q101)).

**Example**

```sql
-- Dialect: ANSI SQL
SELECT student_id,
       class_id,
       score,
       ROW_NUMBER() OVER (PARTITION BY class_id ORDER BY score DESC, student_id) AS row_num,
       RANK()       OVER (PARTITION BY class_id ORDER BY score DESC)             AS rnk,
       DENSE_RANK() OVER (PARTITION BY class_id ORDER BY score DESC)             AS dense_rnk
FROM exam_results;
```
Adding `student_id` to ROW_NUMBER's ORDER BY makes tie-breaking deterministic.

**Real-world use case**

A CDC staging table holds several versions per `order_id`. `ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY updated_at DESC, lsn DESC) = 1` keeps exactly one latest version per order, consistently across reruns.

**Common mistakes and red flags**

- Using RANK for de-duplication, which keeps all tied rows.
- Non-deterministic ROW_NUMBER ordering in pipelines.
- Forgetting PARTITION BY, so numbering runs across the whole table.

**Follow-up questions**

- Is ROW_NUMBER deterministic? — Only if the ORDER BY uniquely orders the rows within each partition.
- How do you get the top 3 per group including ties? — DENSE_RANK (or RANK) ≤ 3 instead of ROW_NUMBER ≤ 3 (see [Q114](/interview/sql/#q114)).

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q109" data-level="Beginner">
<summary><span class="qb-num">Q109</span><h3 class="qb-title">Window functions: PARTITION BY, ORDER BY and frames</h3><span class="qb-meta">Beginner · Very Common · Window Functions</span></summary>

**Short answer**

A window function computes a value for each row using a set of related rows, without collapsing them as GROUP BY does. PARTITION BY defines the groups. ORDER BY defines the order within each group. The frame clause (ROWS or RANGE BETWEEN ...) defines which rows relative to the current one are included, which is what running totals and moving averages need.

**Detailed explanation**

**Anatomy:** `function(args) OVER (PARTITION BY ... ORDER BY ... frame)`

- **PARTITION BY** splits rows into independent windows, such as per customer. If omitted, the whole result is one window.
- **ORDER BY** orders rows inside each partition. It is required for ranking, LAG/LEAD and running calculations.
- **Frame:** a subset of the partition relative to the current row.
  - `ROWS BETWEEN 2 PRECEDING AND CURRENT ROW`: a physical row count.
  - `RANGE BETWEEN INTERVAL '7' DAY PRECEDING AND CURRENT ROW`: a value range (support varies by engine; see [Q110](/interview/sql/#q110)).
  - `UNBOUNDED PRECEDING` / `UNBOUNDED FOLLOWING` / `CURRENT ROW`.

**Default frame behaviour (an important trap).** With ORDER BY and no explicit frame, the ANSI default is `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`. That makes `SUM(...) OVER (ORDER BY d)` a running total, which is usually what you want. But it includes **all peer rows that share the same ORDER BY value**, and it makes `LAST_VALUE` return the current row rather than the last (see [Q113](/interview/sql/#q113)). Without ORDER BY, the frame is the entire partition.

**Families**
- Ranking: ROW_NUMBER, RANK, DENSE_RANK, NTILE.
- Offset: LAG, LEAD, FIRST_VALUE, LAST_VALUE, NTH_VALUE.
- Aggregate as window: SUM, AVG, COUNT, MIN, MAX over frames.

**Example**

```sql
-- Dialect: ANSI SQL
SELECT customer_id,
       order_date,
       amount,
       SUM(amount) OVER (PARTITION BY customer_id)                                   AS customer_total,   -- whole partition
       SUM(amount) OVER (PARTITION BY customer_id ORDER BY order_date
                         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)           AS running_total,
       AVG(amount) OVER (PARTITION BY customer_id ORDER BY order_date
                         ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)                   AS moving_avg_3,
       amount * 1.0 / SUM(amount) OVER (PARTITION BY customer_id)                    AS share_of_total
FROM orders;
```

**Real-world use case**

A customer-360 model computes each order's share of the customer's lifetime spend, a running spend total and a 3-order moving average in one pass, with no self joins.

**Common mistakes and red flags**

- Thinking window functions reduce row count.
- Relying on the default RANGE frame when duplicate order values exist.
- Filtering window results in WHERE (see [Q101](/interview/sql/#q101)).

**Follow-up questions**

- Can you nest a window function inside an aggregate? — Not directly; compute the window in a CTE, then aggregate.
- What is a named window? — `WINDOW w AS (PARTITION BY ... ORDER BY ...)`, reused as `OVER w`; supported in several engines.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q110" data-level="Intermediate">
<summary><span class="qb-num">Q110</span><h3 class="qb-title">ROWS vs RANGE window frames</h3><span class="qb-meta">Intermediate · Common · Window Functions</span></summary>

**Short answer**

ROWS frames count physical rows relative to the current row, for example the previous 2 rows. RANGE frames are defined by the ORDER BY value, for example all rows within 7 days before the current row's date, and they always include peer rows that share the current row's value. Use ROWS for "last N rows" and RANGE for value or time windows, where supported.

**Detailed explanation**

**Peers.** Rows with the same ORDER BY value are peers. RANGE treats them as one unit, and ROWS does not.

Example: daily sales with two rows on 2026-03-02.

| sale_date | amount | ROWS UNBOUNDED→CURRENT | RANGE UNBOUNDED→CURRENT |
|---|---|---|---|
| 2026-03-01 | 10 | 10 | 10 |
| 2026-03-02 | 20 | 30 | 60 |
| 2026-03-02 | 30 | 60 | 60 |
| 2026-03-03 | 5 | 65 | 65 |

With RANGE, both 2026-03-02 rows include each other and show 60. With ROWS, the result depends on the arbitrary order between the peers, which is non-deterministic unless you add a tie-breaker.

**Value-based RANGE with offsets**
- `RANGE BETWEEN INTERVAL '6' DAY PRECEDING AND CURRENT ROW` gives a true 7-day window even when days are missing. ROWS with 6 PRECEDING would span more than 7 days if dates have gaps.
- Engine support varies. PostgreSQL 11+, Oracle, Spark SQL and Databricks support numeric and interval RANGE offsets with a single ORDER BY column. Snowflake supports RANGE frames with `UNBOUNDED`/`CURRENT ROW` and, in newer releases, explicit offsets for numeric and date/time ORDER BY columns. Check your engine's documentation before relying on offset RANGE frames.
- If interval RANGE isn't available, aggregate to one row per day over a date spine (see [Q121](/interview/sql/#q121)) and then use ROWS.

**GROUPS frames** (PostgreSQL, some others) count peer groups rather than rows.

**Example**

```sql
-- Dialect: Spark SQL / Databricks SQL
-- True 7-day rolling revenue per store, robust to missing days
SELECT store_id,
       sale_date,
       daily_revenue,
       SUM(daily_revenue) OVER (
           PARTITION BY store_id
           ORDER BY sale_date
           RANGE BETWEEN INTERVAL 6 DAYS PRECEDING AND CURRENT ROW
       ) AS revenue_7d
FROM store_daily_revenue;
```

**Real-world use case**

A "7-day active users" metric built with `ROWS 6 PRECEDING` overstated activity for low-traffic stores with missing days. Switching to an interval RANGE frame (or a date spine plus ROWS) fixed it.

**Common mistakes and red flags**

- Using ROWS N PRECEDING for time windows on gappy data.
- Not knowing about peer rows in RANGE.
- Assuming every engine supports interval RANGE offsets.

**Follow-up questions**

- Why can ROWS-based running totals differ between runs? — Ties in ORDER BY make peer order arbitrary; add a unique tie-breaker.
- What is the default frame with ORDER BY? — RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW (ANSI).

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q111" data-level="Intermediate">
<summary><span class="qb-num">Q111</span><h3 class="qb-title">Running totals and moving averages</h3><span class="qb-meta">Intermediate · Very Common · Window Functions</span></summary>

**Short answer**

A running total is `SUM(x) OVER (PARTITION BY ... ORDER BY ... ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)`. A moving average uses a bounded frame such as `ROWS BETWEEN 6 PRECEDING AND CURRENT ROW`. Use explicit ROWS frames with a deterministic ORDER BY, and handle missing periods with a date spine.

**Detailed explanation**

**Running (cumulative) total**
- Explicit `ROWS ... UNBOUNDED PRECEDING AND CURRENT ROW` avoids the peer-row behaviour of the default RANGE frame (see [Q110](/interview/sql/#q110)).
- Reset per group with `PARTITION BY` (per customer, per month).
- For month-to-date, partition by month: `PARTITION BY customer_id, DATE_TRUNC('month', d)`.

**Moving average**
- `AVG(x) OVER (... ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)` gives a 7-row trailing average.
- The first rows have fewer than 7 values. Decide whether to show partial averages or NULL until a full window exists (use `COUNT(*) OVER` with the same frame to check).
- If days are missing, rows are not days. Densify with a date spine first (see [Q121](/interview/sql/#q121)), or use an interval RANGE frame (see [Q110](/interview/sql/#q110)).

**Pre-aggregate first.** If the raw table has many rows per day, aggregate to one row per day, then apply the window. Otherwise "7 rows" means 7 transactions, not 7 days.

**Determinism.** If two rows share the same ORDER BY value, a ROWS frame may include them in either order, and reruns can differ. Order by a unique column (or add a tie-breaker), especially when results feed reconciled financial reports.

**Example**

```sql
-- Dialect: ANSI SQL
WITH daily AS (
    SELECT store_id, sale_date, SUM(amount) AS revenue
    FROM sales
    GROUP BY store_id, sale_date
)
SELECT store_id,
       sale_date,
       revenue,
       SUM(revenue) OVER (PARTITION BY store_id ORDER BY sale_date
                          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_revenue,
       CASE WHEN COUNT(*) OVER (PARTITION BY store_id ORDER BY sale_date
                                ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) = 3
            THEN AVG(revenue) OVER (PARTITION BY store_id ORDER BY sale_date
                                    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
       END AS moving_avg_3d
FROM daily
ORDER BY store_id, sale_date;
```

Sample input (store 1): 03-01 = 100, 03-02 = 50, 03-03 = 150, 03-04 = 100.

| sale_date | revenue | running_revenue | moving_avg_3d |
|---|---|---|---|
| 03-01 | 100 | 100 | NULL |
| 03-02 | 50 | 150 | NULL |
| 03-03 | 150 | 300 | 100 |
| 03-04 | 100 | 400 | 100 |

**Real-world use case**

A finance dashboard shows cumulative revenue against target for each month and a 7-day moving average to smooth weekday seasonality. Both are computed in one query over a daily aggregate densified with a calendar table.

**Common mistakes and red flags**

- Applying the window on raw transactions when days were intended.
- Using the default frame and getting odd values on tied dates.
- Ignoring missing dates in moving averages.

**Follow-up questions**

- How would you do a running total without window functions? — A self join summing all earlier rows (O(n²)), or a correlated subquery; window functions are far more efficient.
- How do you compute YTD? — Partition by year and order by date with an unbounded preceding frame (see [Q135](/interview/sql/#q135)).

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q112" data-level="Intermediate">
<summary><span class="qb-num">Q112</span><h3 class="qb-title">LAG/LEAD: day-over-day change</h3><span class="qb-meta">Intermediate · Very Common · Window Functions</span></summary>

**Short answer**

`LAG(col, n, default)` returns the value from n rows before the current row in the window order. `LEAD` returns the value from n rows after. Day-over-day change is `value - LAG(value) OVER (PARTITION BY entity ORDER BY day)`, and the percentage change divides by the lagged value with NULLIF to avoid division by zero.

**Detailed explanation**

**Syntax:** `LAG(expr [, offset [, default]]) OVER (PARTITION BY ... ORDER BY ...)`
- `offset` defaults to 1.
- `default` replaces the NULL returned when no row exists (the first row in the partition).

**Typical uses**
- Period-over-period deltas and growth rates.
- Time between events: `event_ts - LAG(event_ts)` for session gaps (see [Q132](/interview/sql/#q132)).
- Detecting changes: `status <> LAG(status)` marks a status transition. This is the core of change detection and gaps-and-islands (see [Q117](/interview/sql/#q117)).
- Next-event lookups with LEAD: the time until the next purchase, or the next status.

**Pitfalls**
- **Missing periods.** LAG returns the previous *row*, not the previous *day*. If a day is missing, "day-over-day" compares against two days ago. Use a date spine, or check the gap explicitly.
- **Ties in ORDER BY** produce arbitrary previous rows. Add a tie-breaker.
- **Division by zero** in percentage change. Use `NULLIF(prev, 0)`.
- Some engines support `IGNORE NULLS` in LAG to skip NULL values (Snowflake, Oracle, BigQuery, Spark).

**Repeated expressions.** Computing `LAG(...)` three times in one SELECT is fine for readability. Optimizers evaluate identical window expressions once. For clarity, compute it once in a CTE and reference the column.

**Example**

```sql
-- Dialect: ANSI SQL
SELECT metric_date,
       daily_active_users AS dau,
       LAG(daily_active_users) OVER (ORDER BY metric_date)                        AS prev_dau,
       daily_active_users - LAG(daily_active_users) OVER (ORDER BY metric_date)   AS dau_change,
       ROUND(100.0 * (daily_active_users - LAG(daily_active_users) OVER (ORDER BY metric_date))
             / NULLIF(LAG(daily_active_users) OVER (ORDER BY metric_date), 0), 2) AS dau_change_pct,
       LEAD(daily_active_users) OVER (ORDER BY metric_date)                       AS next_dau
FROM daily_metrics;
```

Sample input and output:
| metric_date | dau | prev_dau | dau_change | dau_change_pct | next_dau |
|---|---|---|---|---|---|
| 2026-03-01 | 1000 | NULL | NULL | NULL | 1100 |
| 2026-03-02 | 1100 | 1000 | 100 | 10.00 | 990 |
| 2026-03-03 | 990 | 1100 | -110 | -10.00 | NULL |

**Real-world use case**

An alerting job flags stores whose daily revenue dropped more than 30% from the same weekday last week, using `LAG(revenue, 7)` over a densified daily series.

**Common mistakes and red flags**

- Assuming LAG means "previous calendar day".
- Integer division in percentage calculations (see [Q149](/interview/sql/#q149)).
- No PARTITION BY, so values from one entity leak into another.

**Follow-up questions**

- How do you compare with the same day last year? — Join to the spine on `date - INTERVAL 1 YEAR`, or use LAG over a dense daily series with offset 364/365 (careful with leap years).
- How do you carry forward the last non-null value? — `LAST_VALUE(col IGNORE NULLS) OVER (ORDER BY d ROWS UNBOUNDED PRECEDING)` where supported.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q113" data-level="Intermediate">
<summary><span class="qb-num">Q113</span><h3 class="qb-title">Why does LAST_VALUE return unexpected results? (default frame)</h3><span class="qb-meta">Intermediate · Common · Window Functions</span></summary>

**Short answer**

With an ORDER BY and no explicit frame, the frame ends at the current row (RANGE UNBOUNDED PRECEDING to CURRENT ROW). So `LAST_VALUE(x)` returns the current row's value, not the partition's last value. Specify `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`, or use `FIRST_VALUE` with descending order.

**Detailed explanation**

FIRST_VALUE looks at the start of the frame, and LAST_VALUE at the end. The default frame with ORDER BY starts at the partition's first row (good for FIRST_VALUE) but **ends at the current row and its peers**. LAST_VALUE therefore just echoes the current row, or the last of its peers.

**Fixes**
1. Explicit full frame: `LAST_VALUE(x) OVER (PARTITION BY k ORDER BY t ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)`.
2. Reverse the order: `FIRST_VALUE(x) OVER (PARTITION BY k ORDER BY t DESC)`. This is equivalent and often clearer.
3. Use aggregate-based alternatives where the engine supports them, such as `MAX_BY(x, t)` in Spark and Databricks or `MAX_BY` in Snowflake, which return the x of the row with the greatest t.

**Also watch**
- **NULLs:** FIRST_VALUE and LAST_VALUE return NULL if the boundary row's value is NULL. Use `IGNORE NULLS` where supported.
- **Ties** on the ORDER BY column make the result arbitrary. Add a tie-breaker.
- Some engines (Snowflake) implement the default frame for some functions differently from ANSI. Explicit frames remove any doubt.

**Example**

```sql
-- Dialect: ANSI SQL
-- account_status(account_id, status, changed_at)
SELECT account_id,
       status,
       changed_at,
       LAST_VALUE(status) OVER (PARTITION BY account_id ORDER BY changed_at)               AS last_status_wrong,
       LAST_VALUE(status) OVER (PARTITION BY account_id ORDER BY changed_at
                                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)  AS last_status_right,
       FIRST_VALUE(status) OVER (PARTITION BY account_id ORDER BY changed_at DESC)         AS last_status_alt
FROM account_status;
```

Sample for account 7: trial (Jan 1), active (Feb 1), cancelled (Mar 1).
- `last_status_wrong`: trial, active, cancelled (just the current row).
- `last_status_right` and `last_status_alt`: cancelled on every row.

**Real-world use case**

A churn model's "current status" feature showed historical statuses on older rows because of the default frame. Engineers had trusted LAST_VALUE without a frame. Switching to FIRST_VALUE with DESC order fixed the feature.

**Common mistakes and red flags**

- Not knowing the default frame.
- Fixing it with DISTINCT or GROUP BY hacks instead of the frame.
- Ignoring NULL handling.

**Follow-up questions**

- What is NTH_VALUE? — It returns the value at position n in the frame; it has the same frame caveat.
- How do you get the latest row's value per group without a window? — `MAX_BY` / `ARG_MAX` functions in engines that support them.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q114" data-level="Intermediate">
<summary><span class="qb-num">Q114</span><h3 class="qb-title">Top N rows per group</h3><span class="qb-meta">Intermediate · Very Common · Window Functions</span></summary>

**Short answer**

Rank rows within each group with ROW_NUMBER (exactly N per group) or DENSE_RANK/RANK (to include ties), then keep rank ≤ N in an outer query, or with QUALIFY in Snowflake, Databricks and BigQuery. Make the ORDER BY deterministic.

**Detailed explanation**

**Pattern**
1. Compute `rn = ROW_NUMBER() OVER (PARTITION BY group_col ORDER BY metric DESC, tie_breaker)`.
2. Filter `rn <= N`.

**Ties: choose deliberately**
| Function | Top 2 with scores 100, 90, 90, 80 | Rows returned |
|---|---|---|
| ROW_NUMBER | 100, 90 (one of the 90s, arbitrary unless tie-broken) | Exactly 2 |
| RANK | 100, 90, 90 (ranks 1, 2, 2) | 3 |
| DENSE_RANK | 100, 90, 90, 80 (ranks 1, 2, 2, 3 → top 2 dense ranks = 100 and 90s) | 3 |

For "top 2 distinct values", DENSE_RANK ≤ 2 gives 100 and both 90s.

**Alternatives**
- Correlated subquery: rows where fewer than N rows in the group have a higher value. Portable but slow.
- `LATERAL` / `CROSS APPLY` with `ORDER BY ... LIMIT N` per group (PostgreSQL, SQL Server). Efficient with indexes in OLTP engines.
- Spark/Databricks: `max_by` / `min_by` for N = 1.

**Performance:** window ranking requires partitioning and sorting the whole dataset. In distributed engines, filter early (date range) before ranking.

**Skewed groups:** if one category holds most of the rows, its partition is processed by a single task. Pre-filtering, or pre-aggregating to one row per item before ranking, keeps the sort small.

**Example**

```sql
-- Dialect: Snowflake SQL (QUALIFY also works in Databricks SQL and BigQuery)
-- Top 3 products by revenue in each category
SELECT category, product_id, revenue
FROM product_revenue
QUALIFY ROW_NUMBER() OVER (PARTITION BY category ORDER BY revenue DESC, product_id) <= 3;

-- Dialect: ANSI SQL equivalent
SELECT category, product_id, revenue
FROM (
    SELECT category, product_id, revenue,
           ROW_NUMBER() OVER (PARTITION BY category ORDER BY revenue DESC, product_id) AS rn
    FROM product_revenue
) ranked
WHERE rn <= 3;
```

**Real-world use case**

A merchandising report lists the top 5 SKUs per store per week. ROW_NUMBER with `sku_id` as the tie-breaker guarantees exactly 5 rows per store, and the same 5 on reruns.

**Common mistakes and red flags**

- Using `ORDER BY ... LIMIT N`, which gives the top N overall, not per group.
- Not discussing ties.
- Filtering window results in WHERE.

**Follow-up questions**

- How do you do this in PySpark? — Window spec plus `row_number()` then filter (see [Q296](/interview/pyspark/#q296)).
- How do you get the bottom N? — Order ascending, or rank on the negated metric.

**In depth:** [Return the top three salaries in each department, including ties.](/interview/sql/top-n-per-group-department-salaries/)

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q115" data-level="Intermediate">
<summary><span class="qb-num">Q115</span><h3 class="qb-title">Delete duplicates but keep one row</h3><span class="qb-meta">Intermediate · Very Common · De-duplication</span></summary>

**Short answer**

Rank the rows of each duplicate group with `ROW_NUMBER() OVER (PARTITION BY <business key> ORDER BY <which row wins>)` and keep rank 1. In warehouses it is usually safest to rebuild the table (or insert-overwrite) with only rank-1 rows. On engines with row identifiers, you can delete rows where rank > 1.

**Detailed explanation**

**Step 1: define "duplicate."** Is it an exact copy of every column, or the same business key (for example `order_id`) with differing columns? The second case needs a rule for **which row wins**: latest `updated_at`, highest source sequence, most complete record.

**Step 2: choose the technique for the engine**
| Engine | Approach |
|---|---|
| Snowflake / BigQuery / Databricks | `CREATE OR REPLACE TABLE t AS SELECT ... QUALIFY ROW_NUMBER() ... = 1` (or `INSERT OVERWRITE`); exact duplicates: `SELECT DISTINCT` |
| PostgreSQL | `DELETE ... USING` with `ctid`, or `DELETE WHERE id IN (SELECT id FROM ranked WHERE rn > 1)` |
| SQL Server | `WITH cte AS (... ROW_NUMBER() ...) DELETE FROM cte WHERE rn > 1` (deletable CTE) |
| Oracle | Delete by `ROWID` where rank > 1 |
| Delta Lake | `MERGE` or overwrite with deduped data; or Delta `INSERT OVERWRITE` |

Snowflake has no row identifier, so you can't target one of two identical rows with DELETE. Rebuilding is the standard approach. For huge tables, dedupe only the affected partitions or keys.

**Step 3: prevent recurrence.** Find *why* duplicates appeared (retries, non-idempotent loads, upstream duplicates) and fix the load with MERGE or dedupe in staging (see [Q61](/interview/data-engineering/#q61), [Q403](/interview/scenario-based/#q403)). Add a uniqueness test.

**Example**

```sql
-- Dialect: Snowflake SQL
-- Keep the latest version per order_id (ties broken by load time)
CREATE OR REPLACE TABLE silver.orders AS
SELECT *
FROM silver.orders
QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id
                           ORDER BY updated_at DESC, _loaded_at DESC) = 1;

-- Dialect: SQL Server (T-SQL)
WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY updated_at DESC) AS rn
    FROM dbo.orders
)
DELETE FROM ranked WHERE rn > 1;
```
In Snowflake, `CREATE OR REPLACE` drops the table's existing grants unless you add `COPY GRANTS`. Add it in production: `CREATE OR REPLACE TABLE silver.orders COPY GRANTS AS ...`.

Sample input:
| order_id | status | updated_at |
|---|---|---|
| 1 | created | 09:00 |
| 1 | paid | 09:05 |
| 2 | created | 10:00 |

Output: (1, paid, 09:05), (2, created, 10:00).

**Real-world use case**

A retried ingestion job loaded the same file twice. Engineers deduplicated the affected day's partition with ROW_NUMBER on `event_id`, then changed the loader to MERGE on `event_id` so it couldn't recur.

**Common mistakes and red flags**

- Using DISTINCT when rows differ only in non-key columns.
- Deleting without a deterministic winner rule.
- Forgetting grants or other table properties when rebuilding.

**Follow-up questions**

- How do you dedupe a 10 TB table cheaply? — Dedupe only affected partitions or keys, then fix the source of duplicates.
- How do you find which rows are duplicates first? — GROUP BY key HAVING COUNT(*) > 1 (see [Q116](/interview/sql/#q116)).

**In depth:** [How would you detect and remove duplicate records safely?](/interview/sql/remove-duplicate-records/)

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q116" data-level="Beginner">
<summary><span class="qb-num">Q116</span><h3 class="qb-title">Find duplicate records</h3><span class="qb-meta">Beginner · Very Common · De-duplication</span></summary>

**Short answer**

Group by the columns that should be unique and keep groups with `COUNT(*) > 1`. To see the full duplicate rows, join back on the key, or use `COUNT(*) OVER (PARTITION BY key) > 1` and filter.

**Detailed explanation**

**Decide the key first**
- Exact duplicates: every column identical. Group by all columns, or compare a row hash.
- Business-key duplicates: the same `order_id`, `email` or `(customer_id, date)`, even if other columns differ. This is the more common and more dangerous kind.

**Approaches**
1. `GROUP BY key HAVING COUNT(*) > 1`: lists duplicate keys and their counts.
2. `COUNT(*) OVER (PARTITION BY key)`: keeps all columns, so you can inspect what differs between the copies.
3. Profiling: `COUNT(*) - COUNT(DISTINCT key)` gives the number of excess rows (see [Q35](/interview/data-engineering/#q35)).

**Normalisation matters.** Duplicates hide behind case, whitespace or formatting differences ("A@x.com" vs "a@x.com "). Normalise in the key expression: `LOWER(TRIM(email))`.

**NULL keys:** GROUP BY treats NULLs as one group, so many NULL keys appear as one big "duplicate" group. Decide whether NULL keys are an error in their own right.

**Make it a test.** Once you know the key that must be unique, turn the query into an automated check (a dbt `unique` test or a pipeline assertion), so duplicates are caught on every run rather than discovered by a stakeholder.

**Example**

```sql
-- Dialect: ANSI SQL
-- 1) Duplicate business keys with counts
SELECT LOWER(TRIM(email)) AS email_norm, COUNT(*) AS copies
FROM customers
GROUP BY LOWER(TRIM(email))
HAVING COUNT(*) > 1
ORDER BY copies DESC;

-- 2) Full rows for inspection
SELECT *
FROM (
    SELECT c.*,
           COUNT(*) OVER (PARTITION BY LOWER(TRIM(email))) AS copies
    FROM customers c
) x
WHERE copies > 1
ORDER BY LOWER(TRIM(email));
```

Sample: emails "a@x.com", "A@x.com ", "b@x.com" give the duplicate group `a@x.com` with 2 copies.

**Real-world use case**

Before launching a CRM sync, a team found 4% of customers duplicated by normalised email. Most came from a sign-up flow that didn't lowercase input. They merged the records and added a uniqueness test on the normalised key.

**Common mistakes and red flags**

- Checking duplicates on the surrogate key, which is always unique.
- Ignoring case and whitespace normalisation.
- Not separating NULL-key rows from true duplicates.

**Follow-up questions**

- How do you find near-duplicates? — Fuzzy matching (edit distance, phonetic codes such as SOUNDEX, or token similarity) in an entity-resolution step (see [Q433](/interview/scenario-based/#q433)).
- How do you prevent duplicates? — Unique constraints in OLTP, MERGE/idempotent loads, and uniqueness tests in the warehouse.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q117" data-level="Advanced">
<summary><span class="qb-num">Q117</span><h3 class="qb-title">Gaps and islands: longest streak of consecutive login days</h3><span class="qb-meta">Advanced · Very Common · Gaps &amp; Islands</span></summary>

**Short answer**

Deduplicate to one row per user per day, then subtract `ROW_NUMBER()` (in days) from the date. Consecutive days produce the same "group key", because both values increase by 1 together. Group by user and that key to get each streak's start, end and length, then take the maximum per user.

**Detailed explanation**

"Islands" are runs of consecutive values. "Gaps" are the missing values between them.

**Why the trick works.** For consecutive dates d, d+1, d+2, the row numbers are r, r+1, r+2, so `date − r` is constant. When a day is skipped, the date jumps by 2 but the row number by only 1, so the difference changes and a new island begins.

**Steps**
1. **Deduplicate** to distinct (user, date). Multiple logins on one day would otherwise break the arithmetic.
2. `rn = ROW_NUMBER() OVER (PARTITION BY user ORDER BY login_date)`.
3. `grp = login_date - rn days` (`DATEADD(day, -rn, login_date)` in Snowflake, `DATE_SUB(login_date, rn)` in Spark).
4. Group by user and `grp` to get `MIN(date)`, `MAX(date)` and `COUNT(*)` as the streak length.
5. Take the maximum streak per user.

**Alternative ("change flag" method).** Use LAG to flag rows where the gap from the previous date is greater than 1, then take a running SUM of the flags as the island ID. This generalises to "islands where status stays the same" or "gap > 30 minutes" (sessionisation; see [Q132](/interview/sql/#q132)).

**Example**

```sql
-- Dialect: Snowflake SQL
WITH days AS (
    SELECT DISTINCT user_id, CAST(login_ts AS DATE) AS login_date
    FROM logins
),
grouped AS (
    SELECT user_id,
           login_date,
           DATEADD(day, -ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY login_date), login_date) AS grp
    FROM days
),
streaks AS (
    SELECT user_id,
           MIN(login_date) AS streak_start,
           MAX(login_date) AS streak_end,
           COUNT(*)        AS streak_days
    FROM grouped
    GROUP BY user_id, grp
)
SELECT user_id, MAX(streak_days) AS longest_streak
FROM streaks
GROUP BY user_id;
```

Sample input for user 1: 03-01, 03-02, 03-02 (second login), 03-03, 03-05, 03-06.

| login_date (deduped) | rn | grp |
|---|---|---|
| 03-01 | 1 | 02-28 |
| 03-02 | 2 | 02-28 |
| 03-03 | 3 | 02-28 |
| 03-05 | 4 | 03-01 |
| 03-06 | 5 | 03-01 |

Streaks: 03-01 to 03-03 (3 days) and 03-05 to 03-06 (2 days). Longest streak = 3.

**Real-world use case**

A fitness app rewards users with 7-day activity streaks. The streak model runs nightly with this pattern over a deduplicated daily-activity table, and also produces each user's current streak for the app.

**Common mistakes and red flags**

- Not deduplicating same-day events.
- Using timestamps instead of dates.
- Getting the subtraction direction wrong, or using RANK (ties) instead of ROW_NUMBER.

**Follow-up questions**

- How do you find the gaps instead? — Use LEAD(date) − date > 1 to list each gap's start and end.
- How do you get each user's current streak? — Take the island whose end date is today or yesterday.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q118" data-level="Intermediate">
<summary><span class="qb-num">Q118</span><h3 class="qb-title">Find missing IDs or dates in a sequence</h3><span class="qb-meta">Intermediate · Common · Gaps &amp; Islands</span></summary>

**Short answer**

Either generate the complete expected sequence (a date spine or number series) and anti-join it with the actual data, or use LEAD to find where the next value jumps by more than one and report each gap's range. The spine approach lists every missing value. The LEAD approach summarises the gaps.

**Detailed explanation**

**Approach 1: spine + anti-join**
- Generate all expected values between min and max: a calendar table, `GENERATOR` in Snowflake, `sequence()` + `explode` in Spark, or `generate_series` in PostgreSQL. A recursive CTE also works (see [Q121](/interview/sql/#q121)).
- `LEFT JOIN actual ON spine.value = actual.value WHERE actual.value IS NULL`.
- Best for dates and for "which days had no data per store" (cross join the stores with the spine first).

**Approach 2: LEAD gap ranges**
- `next_id = LEAD(id) OVER (ORDER BY id)`. Where `next_id - id > 1`, the gap runs from `id + 1` to `next_id - 1`.
- Efficient for huge ID ranges where listing every missing value would be enormous.

**Context.** Missing invoice numbers can signal lost or deleted records (an audit concern). Missing dates in a daily feed signal a failed load. Interpret IDs carefully: database sequences can legitimately skip values on rollbacks or caching, so "missing" isn't always an error.

**Scale.** For a large ID range, the LEAD method touches only existing rows. A spine of every possible ID could be billions of rows. For dates, the spine is small and usually the clearer choice.

**Example**

```sql
-- Dialect: Snowflake SQL
-- 1) Missing dates per store in March 2026
WITH spine AS (
    SELECT DATEADD(day, SEQ4(), '2026-03-01'::DATE) AS d
    FROM TABLE(GENERATOR(ROWCOUNT => 31))
),
expected AS (
    SELECT s.store_id, sp.d
    FROM stores s CROSS JOIN spine sp
)
SELECT e.store_id, e.d AS missing_date
FROM expected e
LEFT JOIN store_daily_sales a
  ON a.store_id = e.store_id AND a.sale_date = e.d
WHERE a.store_id IS NULL;

-- 2) Gap ranges in invoice numbers (ANSI SQL)
SELECT invoice_no + 1      AS gap_start,
       next_no - 1         AS gap_end
FROM (
    SELECT invoice_no, LEAD(invoice_no) OVER (ORDER BY invoice_no) AS next_no
    FROM invoices
) t
WHERE next_no - invoice_no > 1;
```
`SEQ4()` can skip values in rare cases. For a guaranteed gap-free series in Snowflake, use `ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1` instead.

Sample invoices: 1001, 1002, 1005, 1006, 1010 give gaps (1003–1004) and (1007–1009).

**Real-world use case**

A data quality monitor checks that every store has a row in `store_daily_sales` for each of the last 7 days. The spine anti-join lists exactly which store-days failed to load.

**Common mistakes and red flags**

- Using a self join to find gaps on large tables.
- Treating every sequence gap as data loss.
- Forgetting to cross join entities with the spine when checking per entity.

**Follow-up questions**

- Why keep a permanent calendar table? — It is reusable, can carry fiscal calendar attributes and holidays, and avoids regenerating series.
- How do you densify a series for charting? — Left join the data onto the spine and COALESCE missing measures to 0 where appropriate.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q119" data-level="Beginner">
<summary><span class="qb-num">Q119</span><h3 class="qb-title">CTE vs subquery vs temporary table</h3><span class="qb-meta">Beginner · Very Common · CTEs</span></summary>

**Short answer**

A CTE (`WITH name AS (...)`) is a named subquery scoped to one statement, which is good for readability and step-by-step logic. A subquery is inline and anonymous. A temporary table materialises results for the session, so it can be reused across statements, indexed in some engines, and gives the optimizer real statistics. Whether CTEs are inlined or materialised depends on the engine.

**Detailed explanation**

| Aspect | CTE | Subquery | Temporary table |
|---|---|---|---|
| Scope | One statement | One place in a statement | Session (or transaction) |
| Reuse | Referenced multiple times in the statement | Must repeat | Many statements |
| Readability | High (top-down steps) | Low when nested | Medium |
| Materialised? | Engine-dependent | Usually inlined | Yes |
| Statistics for optimizer | Inherits from base tables | Same | Actual stats on stored data |
| Recursion | Yes (`WITH RECURSIVE`) | No | No |

**Engine behaviour**
- **PostgreSQL 12+:** CTEs are inlined by default unless they are referenced more than once or marked `MATERIALIZED`. Before version 12 they were always materialised, which acted as an "optimization fence".
- **Snowflake, BigQuery, Spark:** the optimizer decides. A CTE referenced several times *may* be computed once or recomputed. It is not guaranteed.
- **SQL Server:** CTEs are inlined, so referencing one twice usually computes it twice.

**When to use a temp table**
- An expensive intermediate result reused by multiple subsequent statements.
- Breaking a giant query into steps when the optimizer picks a bad plan for the monolith.
- Debugging: inspect intermediate row counts.

**Example**

```sql
-- Dialect: ANSI SQL — CTE for readable, stepwise logic
WITH paid_orders AS (
    SELECT customer_id, order_id, amount, order_date
    FROM orders
    WHERE status = 'paid'
),
customer_totals AS (
    SELECT customer_id, SUM(amount) AS total_spend, COUNT(*) AS order_count
    FROM paid_orders
    GROUP BY customer_id
)
SELECT c.segment,
       AVG(t.total_spend) AS avg_spend,
       AVG(t.order_count) AS avg_orders
FROM customer_totals t
JOIN customers c ON c.customer_id = t.customer_id
GROUP BY c.segment;

-- Dialect: Snowflake SQL — temp table when reused across statements
CREATE TEMPORARY TABLE tmp_paid_orders AS
SELECT customer_id, order_id, amount FROM orders WHERE status = 'paid';
```

**Real-world use case**

A 400-line nested-subquery report was rewritten as eight named CTEs. Reviewers could follow each step, and a bug in the refund logic became obvious in the `refunds` CTE.

**Common mistakes and red flags**

- Believing CTEs are always materialised (or never).
- Deeply nested subqueries where CTEs would be clearer.
- Using temp tables inside dbt models, where ephemeral models or CTEs fit better.

**Follow-up questions**

- Does a CTE improve performance? — Not by itself; it is mainly for readability unless the engine materialises it and avoids recomputation.
- What is a recursive CTE? — A CTE that references itself to walk hierarchies or generate series (see [Q120](/interview/sql/#q120)).

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q120" data-level="Advanced">
<summary><span class="qb-num">Q120</span><h3 class="qb-title">Recursive CTE: walk an org hierarchy</h3><span class="qb-meta">Advanced · Common · CTEs</span></summary>

**Short answer**

A recursive CTE has an anchor member (the starting rows, such as the CEO or one manager) combined with UNION ALL with a recursive member that joins the CTE back to the table to find the next level. It repeats until no new rows are produced. You track depth and the path, and guard against cycles and runaway recursion.

**Detailed explanation**

**Structure**
```text
WITH RECURSIVE cte AS (
    anchor query                 -- level 0
    UNION ALL
    recursive query joining cte  -- level n+1 from level n
)
SELECT ... FROM cte;
```

**How it executes:** the anchor produces the first working set. The recursive part runs against the *latest* working set to produce the next one. This repeats until an iteration returns zero rows. All sets are combined into the result.

**Practical additions**
- **Depth column** (`level + 1`) for indentation, limits and "levels below a VP".
- **Path column** (concatenated names or IDs) for breadcrumbs and cycle detection.
- **Cycle protection:** bad data such as A → B → A loops forever. Stop when the path already contains the ID, cap depth (`WHERE level < 20`), or use engine features (PostgreSQL 14+ `CYCLE` clause; Snowflake has a recursion limit and also offers `CONNECT BY`).
- **Direction:** top-down (manager → reports) or bottom-up (employee → chain of managers).
- **Keyword:** `WITH RECURSIVE` in ANSI, PostgreSQL, MySQL 8 and Spark SQL. SQL Server and Snowflake accept plain `WITH` (Snowflake accepts both). Spark SQL added recursive CTE support only in recent 4.x releases, so check your runtime version before relying on it.

**Example**

```sql
-- Dialect: Snowflake SQL
-- All reports (direct and indirect) under employee 2, with depth and path
WITH RECURSIVE org AS (
    SELECT employee_id, full_name, manager_id,
           0 AS depth,
           CAST(full_name AS VARCHAR) AS path
    FROM employees
    WHERE employee_id = 2                        -- anchor
    UNION ALL
    SELECT e.employee_id, e.full_name, e.manager_id,
           o.depth + 1,
           o.path || ' > ' || e.full_name
    FROM employees e
    JOIN org o ON e.manager_id = o.employee_id   -- recursive step
    WHERE o.depth < 20                           -- safety cap
)
SELECT employee_id, full_name, depth, path
FROM org
ORDER BY path;
```

Sample: 1 Ravi (no manager); 2 Meera reports to 1; 3 Tom reports to 2; 4 Ana reports to 3; 5 Li reports to 2.
Output: Meera (0, "Meera"), Li (1, "Meera > Li"), Tom (1, "Meera > Tom"), Ana (2, "Meera > Tom > Ana").

**Real-world use case**

A cost-allocation model rolls budgets up a 9-level cost-centre hierarchy. A recursive CTE builds a flattened "ancestor–descendant" bridge table nightly, and reports then join to it without recursion.

**Common mistakes and red flags**

- Using UNION instead of UNION ALL, which adds a costly distinct and can hide cycles inconsistently.
- No depth cap or cycle check.
- Recomputing deep hierarchies in every dashboard query instead of materialising a closure table.

**Follow-up questions**

- What is a closure table? — A table of all ancestor–descendant pairs with depth, so hierarchy queries become simple joins.
- How would you do this in Spark without recursive CTE support? — Iterative DataFrame joins in a loop until no new rows, or GraphFrames.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q121" data-level="Intermediate">
<summary><span class="qb-num">Q121</span><h3 class="qb-title">Generate a date spine with a recursive CTE or generator</h3><span class="qb-meta">Intermediate · Occasional · CTEs</span></summary>

**Short answer**

A date spine is a continuous list of dates used to fill gaps in time series. You can build it with a recursive CTE (start date plus 1 day until the end), an engine generator (`GENERATOR` in Snowflake, `sequence` + `explode` in Spark, `generate_series` in PostgreSQL), or, best for production, a permanent calendar table.

**Detailed explanation**

**Why you need it**
- Aggregates only contain days that had data. Charts and moving averages need *every* day (see [Q111](/interview/sql/#q111)).
- Checking completeness (which days are missing; see [Q118](/interview/sql/#q118)).
- Cross-joining entities with dates to build dense panels (store × day) for snapshots and features.

**Options**
| Method | Engine | Notes |
|---|---|---|
| Recursive CTE | Most engines | Portable; may hit recursion limits for very long ranges |
| `GENERATOR(ROWCOUNT => n)` | Snowflake | Fast; pair with `ROW_NUMBER()` for gap-free sequences |
| `sequence(start, end, interval)` + `explode` | Spark/Databricks | Native array generation |
| `generate_series` | PostgreSQL | Simplest |
| `GENERATE_DATE_ARRAY` + `UNNEST` | BigQuery | Native |
| Calendar table | All | Reusable; holds fiscal periods, holidays, week numbers |

**Calendar table columns worth having:** date key, calendar and fiscal year/quarter/month/week, day of week, weekend flag, holiday flag per country, and first/last day of the period. dbt users often generate one with `dbt_utils.date_spine` and enrich it.

Recursive CTEs are fine for a few thousand rows. For long ranges, engine generators are much faster, and a calendar table built once is cheapest of all.

**Example**

```sql
-- Dialect: ANSI SQL (WITH RECURSIVE) — dates for March 2026
WITH RECURSIVE spine (d) AS (
    SELECT DATE '2026-03-01'
    UNION ALL
    SELECT d + INTERVAL '1' DAY FROM spine WHERE d < DATE '2026-03-31'
)
SELECT d FROM spine;

-- Dialect: Spark SQL / Databricks SQL
SELECT explode(sequence(DATE'2026-03-01', DATE'2026-03-31', INTERVAL 1 DAY)) AS d;

-- Dialect: Snowflake SQL (gap-free with ROW_NUMBER)
SELECT DATEADD(day, ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1, '2026-03-01'::DATE) AS d
FROM TABLE(GENERATOR(ROWCOUNT => 31));
```
Date-plus-interval arithmetic syntax differs across engines (`d + 1` works in PostgreSQL and Snowflake for DATE). Adapt as needed.

**Real-world use case**

A daily store-sales chart showed misleading straight lines across closed days. Left-joining the sales onto a store × date spine, with COALESCE to 0, showed the zero days correctly.

**Common mistakes and red flags**

- Using `SEQ4()` alone and assuming no gaps.
- Building spines ad hoc in every query instead of keeping a calendar table.
- Forgetting time zones when converting timestamps to dates before joining to the spine.

**Follow-up questions**

- How do you build an hourly spine? — Same pattern with an hour interval, or cross join dates with 0–23.
- How do you handle fiscal calendars (4-4-5)? — Store fiscal attributes in the calendar table rather than computing them in queries.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q122" data-level="Intermediate">
<summary><span class="qb-num">Q122</span><h3 class="qb-title">Pivot rows to columns (CASE and PIVOT)</h3><span class="qb-meta">Intermediate · Common · Pivot / Unpivot</span></summary>

**Short answer**

Pivoting turns distinct values of one column into separate columns. The portable method is conditional aggregation: `SUM(CASE WHEN month = 'Jan' THEN amount END) AS jan`, grouped by the row key. Many engines also offer a `PIVOT` operator. Both need the output column list known in advance unless you use dynamic SQL or engine-specific dynamic pivots.

**Detailed explanation**

**Conditional aggregation (portable)**
- Group by the row identifier.
- One aggregate per output column, with CASE (or `FILTER (WHERE ...)` in PostgreSQL and Spark).
- Works everywhere and is easy to read.

**PIVOT operator**
- Supported in Snowflake, Databricks/Spark SQL, SQL Server, Oracle and BigQuery, with slightly different syntax.
- Requires listing the pivot values. Snowflake supports `ANY` (and `ANY ORDER BY`) and subquery-based dynamic value lists. Databricks and Spark require explicit values in SQL.
- Implicit grouping: all non-pivoted columns become the grouping key, which can surprise you if the input has extra columns. Select only the needed columns first.

**Dynamic pivots** (unknown values): generate SQL from the distinct values (stored procedure, Jinja in dbt with `dbt_utils.get_column_values` + `pivot`), or pivot in the BI tool instead.

**Missing combinations** return NULL. Wrap with `COALESCE(..., 0)` if zero is the right meaning.

**Aggregate choice.** Use SUM for measures, COUNT for occurrences, and MAX or MIN when pivoting one value per key (for example an attribute per key–name pair), where any aggregate would do.

**Example**

```sql
-- Dialect: ANSI SQL — conditional aggregation
SELECT store_id,
       SUM(CASE WHEN quarter = 'Q1' THEN revenue ELSE 0 END) AS q1_revenue,
       SUM(CASE WHEN quarter = 'Q2' THEN revenue ELSE 0 END) AS q2_revenue,
       SUM(CASE WHEN quarter = 'Q3' THEN revenue ELSE 0 END) AS q3_revenue,
       SUM(CASE WHEN quarter = 'Q4' THEN revenue ELSE 0 END) AS q4_revenue
FROM store_quarterly_revenue
GROUP BY store_id;

-- Dialect: Snowflake SQL — PIVOT operator
SELECT *
FROM (SELECT store_id, quarter, revenue FROM store_quarterly_revenue)
PIVOT (SUM(revenue) FOR quarter IN ('Q1', 'Q2', 'Q3', 'Q4'))
AS p (store_id, q1_revenue, q2_revenue, q3_revenue, q4_revenue);
```

Sample input: (1, Q1, 100), (1, Q2, 150), (2, Q1, 80).
Output: store 1 has Q1 = 100, Q2 = 150, Q3 = 0, Q4 = 0. Store 2 has Q1 = 80 and 0 for the rest (with the CASE version; the PIVOT version returns NULL for missing quarters).

**Real-world use case**

A finance extract needs one row per cost centre with 12 monthly columns for a planning tool. Conditional aggregation over the monthly fact produces it, and a dbt macro generates the 12 CASE expressions.

**Common mistakes and red flags**

- Passing extra columns into PIVOT, creating unexpected grouping.
- Assuming PIVOT discovers new values automatically.
- Pivoting huge numbers of values, which creates very wide tables, when a long format would be better.

**Follow-up questions**

- When should you not pivot in SQL? — When the column set changes often; keep data long and pivot in BI.
- How do you pivot in PySpark? — `df.groupBy("store_id").pivot("quarter", ["Q1","Q2","Q3","Q4"]).sum("revenue")`.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q123" data-level="Intermediate">
<summary><span class="qb-num">Q123</span><h3 class="qb-title">Unpivot columns to rows</h3><span class="qb-meta">Intermediate · Common · Pivot / Unpivot</span></summary>

**Short answer**

Unpivoting turns multiple columns into rows of (attribute name, value), converting wide data to long. Use the `UNPIVOT` operator where available (Snowflake, Databricks, SQL Server, Oracle, BigQuery), or a portable `UNION ALL` per column, or a `CROSS JOIN` with a `VALUES` list and CASE.

**Detailed explanation**

**When you need it**
- Source spreadsheets or extracts with one column per month or metric.
- Normalising survey responses (`q1`, `q2`, `q3` columns into question/answer rows).
- Feeding tools that expect long format (time series, semantic layers).

**Options**
| Method | Pros | Cons |
|---|---|---|
| `UNPIVOT` operator | Concise | Syntax varies; NULL values dropped by default (use `INCLUDE NULLS` where supported) |
| `UNION ALL` per column | Portable, explicit | Verbose; scans table once per column in some engines |
| `CROSS JOIN (VALUES ...)` + CASE | Portable, single scan | More complex |
| `LATERAL VIEW stack()` (Spark) | Single scan | Spark-specific |

**Type rules:** all unpivoted columns must share a compatible data type. Cast them first if needed.

**NULL handling:** most UNPIVOT implementations exclude NULLs by default. If "no value" must be kept as a row, use `INCLUDE NULLS` (Snowflake, Databricks) or the UNION ALL method.

**Derived keys.** After unpivoting, convert the attribute name into a proper key. For example, map `'jan'` to a month number or date, so the long table joins cleanly to a calendar dimension.

**Example**

```sql
-- Dialect: Snowflake SQL
-- budget_wide(cost_center, jan, feb, mar)
SELECT cost_center, month_name, budget
FROM budget_wide
UNPIVOT (budget FOR month_name IN (jan, feb, mar));

-- Dialect: ANSI SQL — portable single-scan version
SELECT b.cost_center,
       m.month_name,
       CASE m.month_name WHEN 'jan' THEN b.jan
                         WHEN 'feb' THEN b.feb
                         WHEN 'mar' THEN b.mar END AS budget
FROM budget_wide b
CROSS JOIN (VALUES ('jan'), ('feb'), ('mar')) AS m(month_name);
```

Sample input: (CC10, 100, 120, NULL).
UNPIVOT output: (CC10, JAN, 100), (CC10, FEB, 120). The NULL March row is excluded by default, and Snowflake returns the column names in uppercase. The portable version returns three rows, including (CC10, mar, NULL).

**Real-world use case**

Finance uploads a budget sheet with 12 month columns. The ingestion model unpivots it into `(cost_center, month, budget)` so it can join the actuals fact by month.

**Common mistakes and red flags**

- Losing NULL rows unintentionally.
- Unpivoting columns with mixed types without casting.
- Forgetting that names come back uppercase from Snowflake UNPIVOT, then filtering on lowercase.

**Follow-up questions**

- PySpark equivalent? — `df.unpivot(ids, values, "month", "budget")` (Spark 3.4+), or `stack()` in `selectExpr`.
- Why prefer long format in a warehouse? — New periods or metrics are new rows, not schema changes.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q124" data-level="Intermediate">
<summary><span class="qb-num">Q124</span><h3 class="qb-title">Correlated subqueries and their performance</h3><span class="qb-meta">Intermediate · Common · Subqueries</span></summary>

**Short answer**

A correlated subquery references columns from the outer query, so logically it runs once per outer row. Modern optimizers often decorrelate it into a join or semi-join, but not always. Complex correlations (non-equality conditions, LIMIT inside, scalar subqueries in SELECT) can still be slow or unsupported in distributed engines. Rewriting with joins or window functions is usually clearer and faster.

**Detailed explanation**

**Correlation illustrated:** "orders above that customer's average order value"
```text
WHERE o.amount > (SELECT AVG(o2.amount) FROM orders o2 WHERE o2.customer_id = o.customer_id)
```

**Performance reality**
- **OLTP engines** with indexes can execute row-by-row lookups cheaply for small outer sets.
- **Columnar or distributed engines** (Snowflake, Spark, BigQuery) prefer set-based plans. Their optimizers decorrelate common patterns (`EXISTS`, `IN`, scalar aggregates on equality) into joins. Others may run very slowly, or fail with errors such as Snowflake's "Unsupported subquery type", and Spark has limits on correlated subqueries in some positions.

**Rewrites**
| Pattern | Rewrite |
|---|---|
| Scalar aggregate per group | Window function: `AVG(amount) OVER (PARTITION BY customer_id)` |
| `EXISTS` / `IN` | Semi-join (often automatic) |
| Latest row per key | `ROW_NUMBER` + filter (see [Q114](/interview/sql/#q114)) |
| Lookup of one value | `LEFT JOIN` to an aggregated CTE |

**Semantics to preserve when rewriting**
- A scalar subquery that finds no row returns NULL. A rewrite with an INNER JOIN would drop that outer row, so use a LEFT JOIN to keep it.
- A scalar subquery must return at most one row. A join rewrite silently duplicates rows if the inner side isn't unique, so aggregate it first.
- Correlated `NOT EXISTS` must become an anti-join, not `NOT IN`, to stay NULL-safe (see [Q105](/interview/sql/#q105)).

After rewriting, compare both versions' outputs on a sample, and check the new plan to confirm a set-based join replaced the per-row evaluation.

**Example**

```sql
-- Dialect: ANSI SQL
-- Correlated version
SELECT o.order_id, o.customer_id, o.amount
FROM orders o
WHERE o.amount > (SELECT AVG(o2.amount)
                  FROM orders o2
                  WHERE o2.customer_id = o.customer_id);

-- Window-function rewrite: one pass, no correlation
SELECT order_id, customer_id, amount
FROM (
    SELECT order_id, customer_id, amount,
           AVG(amount) OVER (PARTITION BY customer_id) AS avg_amount
    FROM orders
) t
WHERE amount > avg_amount;
```
Sample: customer 1 has orders of 50, 100 and 150 (average 100), so only the 150 order is returned.

**Real-world use case**

A Spark SQL job with a scalar correlated subquery in the SELECT list took 3 hours. Rewritten as a pre-aggregated CTE joined back on the key, it ran in 6 minutes.

**Common mistakes and red flags**

- Assuming correlated subqueries always run row by row, or never do.
- Scalar subqueries in SELECT that return multiple rows, which raises a runtime error.
- Not knowing window functions can replace most of them.

**Follow-up questions**

- What is a LATERAL join? — A join where the right side can reference columns from the left, like a correlated subquery that returns multiple rows/columns.
- How do you check if the optimizer decorrelated it? — Inspect the plan for a join or semi-join instead of a nested-loop subplan (see [Q141](/interview/sql/#q141)).

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q125" data-level="Beginner">
<summary><span class="qb-num">Q125</span><h3 class="qb-title">COUNT(*) vs COUNT(col) vs COUNT(DISTINCT col)</h3><span class="qb-meta">Beginner · Common · Aggregation</span></summary>

**Short answer**

`COUNT(*)` counts all rows, including rows with NULLs. `COUNT(col)` counts rows where `col` is not NULL. `COUNT(DISTINCT col)` counts unique non-NULL values. `COUNT(1)` behaves the same as `COUNT(*)`, with no performance difference in modern engines.

**Detailed explanation**

| Expression | Counts | NULLs |
|---|---|---|
| `COUNT(*)` | Rows | Included |
| `COUNT(1)` | Rows (constant is never NULL) | Included |
| `COUNT(col)` | Non-NULL values of col | Excluded |
| `COUNT(DISTINCT col)` | Unique non-NULL values | Excluded |
| `COUNT(DISTINCT a, b)` | Unique non-NULL combinations (engine support varies) | Rows with any NULL excluded in most engines |

**Uses**
- Completeness checks: `COUNT(*) - COUNT(email)` is the number of NULL emails (see [Q35](/interview/data-engineering/#q35)).
- Duplicate checks: `COUNT(*) - COUNT(DISTINCT id)` is the number of excess rows (see [Q116](/interview/sql/#q116)).
- LEFT JOIN counts: count a column from the right table (`COUNT(o.order_id)`) so customers with no orders show 0 rather than 1.

**Performance**
- `COUNT(DISTINCT)` on high-cardinality columns is expensive, because it must track every unique value. On large data, approximate functions (`APPROX_COUNT_DISTINCT`, HyperLogLog) are much cheaper (see [Q150](/interview/sql/#q150)).
- Distinct counts are **non-additive**: you can't sum daily distinct users into monthly ones (see [Q7](/interview/data-engineering/#q7)).
- `COUNT(*)` on a large table can often be answered from metadata alone in Snowflake and some lakehouse engines, without scanning data. `COUNT(col)` and `COUNT(DISTINCT col)` usually require reading the column.

**Example**

```sql
-- Dialect: ANSI SQL
-- customers c LEFT JOIN orders o: count orders per customer correctly
SELECT c.customer_id,
       COUNT(*)            AS rows_after_join,   -- 1 even with no orders
       COUNT(o.order_id)   AS order_count,       -- 0 when no orders
       COUNT(DISTINCT o.product_id) AS distinct_products
FROM customers c
LEFT JOIN orders o ON o.customer_id = c.customer_id
GROUP BY c.customer_id;
```
Sample: customer 1 has orders (p1), (p1), (p2), and customer 2 has none.
Customer 1: rows_after_join 3, order_count 3, distinct_products 2. Customer 2: rows_after_join 1, order_count 0, distinct_products 0.

**Real-world use case**

A dashboard showed every customer with at least 1 order because it used `COUNT(*)` after a LEFT JOIN. Switching to `COUNT(o.order_id)` showed 18% of customers had never ordered.

**Common mistakes and red flags**

- Thinking `COUNT(1)` is faster than `COUNT(*)`.
- `COUNT(*)` after a LEFT JOIN to count child rows.
- Summing distinct counts across periods.

**Follow-up questions**

- Does `COUNT(DISTINCT)` count NULL as a value? — No; NULLs are ignored.
- How do you count rows matching a condition? — `COUNT_IF(cond)` in Snowflake/Spark, or `SUM(CASE WHEN cond THEN 1 ELSE 0 END)`, or `COUNT(*) FILTER (WHERE cond)`.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q126" data-level="Beginner">
<summary><span class="qb-num">Q126</span><h3 class="qb-title">NULL handling: three-valued logic, COALESCE, NULLIF</h3><span class="qb-meta">Beginner · Very Common · NULL Handling</span></summary>

**Short answer**

NULL means unknown, so comparisons with NULL evaluate to UNKNOWN, not TRUE or FALSE. WHERE keeps only TRUE rows. Use `IS NULL` / `IS NOT NULL` (or `IS DISTINCT FROM`) to test it, `COALESCE` to substitute a default, and `NULLIF(a, b)` to turn a value into NULL, commonly to avoid division by zero.

**Detailed explanation**

**Three-valued logic**
- `NULL = NULL` is UNKNOWN. `NULL <> 5` is UNKNOWN.
- `TRUE AND UNKNOWN` = UNKNOWN. `FALSE AND UNKNOWN` = FALSE. `TRUE OR UNKNOWN` = TRUE.
- `NOT UNKNOWN` = UNKNOWN.
- WHERE and JOIN ON keep only TRUE, so rows with NULL in a compared column silently disappear.

**Consequences**
- `WHERE status <> 'cancelled'` **excludes** rows where `status` is NULL. Add `OR status IS NULL` if they should stay.
- `NOT IN` with NULLs returns nothing (see [Q105](/interview/sql/#q105)).
- Joins never match NULL keys with `=`. Use `IS NOT DISTINCT FROM` (or Snowflake's `EQUAL_NULL`) for null-safe equality, or the `<=>` operator in Spark and MySQL.
- Aggregates (SUM, AVG, MIN, MAX, COUNT(col)) ignore NULLs. `AVG` divides by the non-NULL count, which may not be what the business wants.
- `NULL + 1` is NULL, and string concatenation with NULL is NULL in most engines (Snowflake `||` returns NULL; `CONCAT_WS` skips NULLs in some engines).
- Sorting: NULLS FIRST or LAST defaults differ by engine. Be explicit.

**Functions**
- `COALESCE(a, b, c)`: the first non-NULL value.
- `NULLIF(a, b)`: NULL if a = b, otherwise a. The classic use is `x / NULLIF(y, 0)`.
- Engine-specific: `IFNULL`, `NVL`, `ZEROIFNULL` (Snowflake), `NVL2`.

**Example**

```sql
-- Dialect: ANSI SQL
SELECT order_id,
       COALESCE(discount, 0)                          AS discount_filled,
       amount / NULLIF(quantity, 0)                   AS unit_price,      -- NULL instead of error
       CASE WHEN status IS NULL THEN 'unknown' ELSE status END AS status_clean
FROM orders
WHERE status IS DISTINCT FROM 'cancelled';           -- keeps NULL-status rows
```
`IS DISTINCT FROM` is supported in PostgreSQL, Snowflake, Databricks and others. Without it, write `WHERE status <> 'cancelled' OR status IS NULL`.

**Real-world use case**

A "non-cancelled orders" metric was 7% low because `status <> 'cancelled'` dropped orders whose status hadn't been set yet. A null-safe comparison fixed it, and a not-null test on `status` caught the root cause.

**Common mistakes and red flags**

- `WHERE col = NULL`.
- COALESCE-ing measures to 0 when NULL truly means "unknown".
- Forgetting AVG ignores NULLs.

**Follow-up questions**

- How do you join on nullable keys? — Null-safe equality, or coalesce both sides to a sentinel (carefully, ensuring the sentinel can't collide).
- Is an empty string NULL? — Not in most engines; Oracle treats `''` as NULL.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q127" data-level="Intermediate">
<summary><span class="qb-num">Q127</span><h3 class="qb-title">Diagnose a join that multiplies rows (fan-out)</h3><span class="qb-meta">Intermediate · Very Common · Joins</span></summary>

**Short answer**

Fan-out happens when the join key isn't unique on one side, so each row matches several rows and measures are counted multiple times. Diagnose it by checking key uniqueness on each side and comparing row counts before and after the join. Fix it by aggregating to the right grain before joining, deduplicating the dimension, or fixing the join key.

**Detailed explanation**

**Symptoms**
- Revenue is suddenly 2–3 times higher after adding a join.
- `COUNT(*)` after the join exceeds the fact's row count.
- Someone "fixed" it with DISTINCT or by dividing by a count. Both are red flags.

**Diagnosis checklist**
1. Check uniqueness on each side for the join key: `SELECT key, COUNT(*) FROM dim GROUP BY key HAVING COUNT(*) > 1`.
2. Compare row counts: fact rows vs joined rows.
3. Look for SCD2 dimensions joined without a validity filter (`is_current` or a date range). Every historical version matches.
4. Look for a missing join column in a composite key (joined on `product_id` but the dimension is unique on `product_id, country`).
5. Look for joins between two facts (orders × payments at line grain).

**Fixes**
- Join on the **full unique key**, plus the SCD2 validity condition.
- **Pre-aggregate** the many side to the target grain before joining (see [Q8](/interview/data-engineering/#q8)'s drill-across pattern).
- **Deduplicate** dimensions if duplicates are data errors, and add uniqueness tests.
- Use **semi-joins** (EXISTS) when you only need to filter, not fetch columns.

**Example**

```sql
-- Dialect: ANSI SQL
-- Problem: payments has multiple rows per order (partial payments) -> order amount counted twice
SELECT SUM(o.order_amount) AS revenue_wrong
FROM orders o
JOIN payments p ON p.order_id = o.order_id;

-- Diagnosis
SELECT order_id, COUNT(*) AS payment_rows
FROM payments GROUP BY order_id HAVING COUNT(*) > 1;

-- Fix: aggregate payments to order grain first
WITH pay AS (
    SELECT order_id, SUM(paid_amount) AS paid_amount
    FROM payments GROUP BY order_id
)
SELECT SUM(o.order_amount) AS revenue,
       SUM(pay.paid_amount) AS collected
FROM orders o
LEFT JOIN pay ON pay.order_id = o.order_id;
```
Sample: order 1 for 100 paid in two instalments (60 + 40). The wrong query gives 200. The fixed query gives revenue 100 and collected 100.

**Real-world use case**

After a dimension's SCD2 conversion, a sales dashboard tripled overnight because joins used only the natural key. Adding the validity condition (`order_date` between `valid_from` and `valid_to`) restored correct totals.

**Common mistakes and red flags**

- Using DISTINCT to hide fan-out.
- Not knowing the grain of each table before joining.
- No uniqueness tests on dimension keys.

**Follow-up questions**

- How do semantic layers prevent fan-out? — They know each entity's grain and aggregate before joining (symmetric aggregates or pre-aggregation).
- What is a chasm trap? — Two one-to-many joins from a common table, which multiplies both sides; resolve by aggregating each separately.

**In depth:** [Revenue doubled after you joined a second table. What happened and how do you fix it?](/interview/sql/join-fan-out-double-counting/)

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q128" data-level="Advanced">
<summary><span class="qb-num">Q128</span><h3 class="qb-title">New vs returning users and cumulative distinct counts</h3><span class="qb-meta">Advanced · Common · Analytics Patterns</span></summary>

**Short answer**

Compute each user's first activity date with `MIN(activity_date) OVER (PARTITION BY user_id)` or a GROUP BY. Label activity on that date as "new" and later activity as "returning". The cumulative count of distinct users by date is a running sum of new users per day, which avoids an expensive distinct count over a growing window.

**Detailed explanation**

**Why not `COUNT(DISTINCT user_id) OVER (ORDER BY date ...)`?** Most engines don't support DISTINCT inside window aggregates, and where they do, it is expensive. Cumulative distinct users equals the number of users whose first-seen date is on or before each date, which is just a running sum of first-seen counts.

**Steps**
1. Deduplicate activity to (user, date).
2. `first_date = MIN(date) OVER (PARTITION BY user)`.
3. Per date: `new_users = COUNT(DISTINCT CASE WHEN date = first_date THEN user END)` and `returning_users = COUNT(DISTINCT CASE WHEN date > first_date THEN user END)`.
4. Cumulative users: `SUM(new_users) OVER (ORDER BY date ROWS UNBOUNDED PRECEDING)`.

**Definitions to clarify**
- First-ever activity, or first within a lookback (users inactive 90+ days re-counted as "reactivated")?
- What counts as activity: login, purchase, any event?
- Time zone of the date boundary.

**Scale.** `MIN(...) OVER (PARTITION BY user_id)` shuffles all activity by user. At very large volumes, maintain a small persisted `user_first_seen` table and join to it instead.

**Example**

```sql
-- Dialect: ANSI SQL
WITH activity AS (
    SELECT DISTINCT user_id, CAST(event_ts AS DATE) AS activity_date
    FROM events
),
labelled AS (
    SELECT user_id, activity_date,
           MIN(activity_date) OVER (PARTITION BY user_id) AS first_date
    FROM activity
),
daily AS (
    SELECT activity_date,
           COUNT(CASE WHEN activity_date = first_date THEN 1 END) AS new_users,
           COUNT(CASE WHEN activity_date > first_date THEN 1 END) AS returning_users
    FROM labelled
    GROUP BY activity_date
)
SELECT activity_date,
       new_users,
       returning_users,
       SUM(new_users) OVER (ORDER BY activity_date
                            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_users
FROM daily
ORDER BY activity_date;
```
Because `activity` is already one row per user per day, `COUNT(CASE ...)` equals a distinct user count here.

Sample input (user, date): (A, 03-01), (B, 03-01), (A, 03-02), (C, 03-02), (B, 03-03).

| activity_date | new_users | returning_users | cumulative_users |
|---|---|---|---|
| 03-01 | 2 | 0 | 2 |
| 03-02 | 1 | 1 | 3 |
| 03-03 | 0 | 1 | 3 |

**Real-world use case**

A growth dashboard tracks daily new vs returning buyers and cumulative customers. The pre-aggregated daily table makes the chart instant, compared with a distinct count over all history on every load.

**Common mistakes and red flags**

- Computing cumulative distinct counts with a self join on all previous dates.
- Not deduplicating same-day activity.
- Ambiguous "new" definitions across teams.

**Follow-up questions**

- How do you add reactivated users? — Use LAG of activity date per user and flag gaps longer than the inactivity threshold.
- How do you do this incrementally? — Maintain a `user_first_seen` table and MERGE new users daily.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q129" data-level="Intermediate">
<summary><span class="qb-num">Q129</span><h3 class="qb-title">Compute median and percentiles</h3><span class="qb-meta">Intermediate · Common · Aggregation</span></summary>

**Short answer**

Use `PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY x)` for the interpolated median, or `PERCENTILE_DISC` to return an actual value from the data. Many engines also have `MEDIAN(x)`. For very large data, approximate functions such as `APPROX_PERCENTILE` are much cheaper. Without these functions, use ROW_NUMBER and COUNT to pick the middle row or rows.

**Detailed explanation**

**Exact percentiles**
- `PERCENTILE_CONT(p)`: continuous. It interpolates between the two nearest values (median of 1, 2, 3, 4 is 2.5).
- `PERCENTILE_DISC(p)`: discrete. It returns the first value whose cumulative distribution is ≥ p (median of 1, 2, 3, 4 is 2).
- Syntax `WITHIN GROUP (ORDER BY ...)` in ANSI, Snowflake, PostgreSQL, SQL Server and Oracle. They can also be window functions in some engines (`OVER (PARTITION BY ...)`).
- `MEDIAN(x)` is available in Snowflake, Oracle and Databricks.

**Approximate percentiles**
- Snowflake `APPROX_PERCENTILE`, Spark and Databricks `percentile_approx` / `approx_percentile`, BigQuery `APPROX_QUANTILES`.
- They use sketches (t-digest and similar) with bounded memory, and are mergeable and fast on billions of rows (see [Q150](/interview/sql/#q150)).

**Manual method (portable)**
- Order rows with ROW_NUMBER and count total rows. For odd n take row (n+1)/2. For even n, average rows n/2 and n/2+1.

**Why median matters:** averages are skewed by outliers. Median order value or median latency is often more representative, and latency SLOs use p95 or p99.

**Example**

```sql
-- Dialect: Snowflake SQL
SELECT region,
       MEDIAN(order_value)                                           AS median_value,
       PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY order_value)      AS p50_cont,
       PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY order_value)      AS p50_disc,
       PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY order_value)     AS p95,
       APPROX_PERCENTILE(order_value, 0.95)                          AS p95_approx
FROM orders
GROUP BY region;
```
Sample region values: 10, 20, 30, 40. p50_cont = 25. p50_disc = 20. MEDIAN = 25.

**Real-world use case**

An API team reports p50, p95 and p99 request latency per endpoint per hour from billions of log rows using `APPROX_PERCENTILE`. Exact percentiles on that volume were too slow for hourly refresh.

**Common mistakes and red flags**

- Using AVG when outliers dominate.
- Not knowing CONT vs DISC.
- Averaging percentiles across groups, which is not mathematically valid.

**Follow-up questions**

- Can you combine p95s from two days? — Not by averaging; merge the underlying sketches or recompute from raw data.
- How accurate are approximate percentiles? — Typically within a small relative error; check engine docs for guarantees.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q130" data-level="Intermediate">
<summary><span class="qb-num">Q130</span><h3 class="qb-title">Month-over-month growth percentage</h3><span class="qb-meta">Intermediate · Very Common · Analytics Patterns</span></summary>

**Short answer**

Aggregate to one row per month (and per entity, if needed), then compute `(current - LAG(current)) / NULLIF(LAG(current), 0)` ordered by month. Make sure missing months are present (date spine), and handle zero or NULL previous values explicitly.

**Detailed explanation**

**Steps**
1. Truncate dates to month: `DATE_TRUNC('month', order_date)`.
2. Aggregate the metric per month.
3. Use LAG over the month order, partitioned by entity if comparing per region or product.
4. Compute growth with NULLIF and decimal arithmetic.

**Pitfalls**
- **Missing months:** LAG compares with the previous *row*. If March has no row, April compares with February. Join to a month spine and COALESCE to 0 first (see [Q121](/interview/sql/#q121)).
- **Partial current month:** comparing month-to-date with a full previous month exaggerates declines. Compare MTD with the same number of days last month, or exclude the current month.
- **Integer division:** `100 * (a - b) / b` with integers truncates in some engines (see [Q149](/interview/sql/#q149)).
- **Business calendars:** fiscal months, or 4-4-5 calendars, need a calendar table.
- **Mix effects:** growth in totals may hide declines in segments. Partition by segment too.
- **Ratios of ratios:** MoM growth of a rate (conversion rate) should compare rates computed from monthly totals, not averages of daily rates (see [Q7](/interview/data-engineering/#q7)).

**Example**

```sql
-- Dialect: Snowflake SQL
WITH monthly AS (
    SELECT DATE_TRUNC('month', order_date) AS month_start,
           SUM(amount)                     AS revenue
    FROM orders
    WHERE order_date < DATE_TRUNC('month', CURRENT_DATE)   -- exclude partial current month
    GROUP BY 1
)
SELECT month_start,
       revenue,
       LAG(revenue) OVER (ORDER BY month_start) AS prev_revenue,
       ROUND(100 * (revenue - LAG(revenue) OVER (ORDER BY month_start))
             / NULLIF(LAG(revenue) OVER (ORDER BY month_start), 0), 2) AS mom_growth_pct
FROM monthly
ORDER BY month_start;
```

Sample:
| month_start | revenue | prev_revenue | mom_growth_pct |
|---|---|---|---|
| 2026-01-01 | 1000 | NULL | NULL |
| 2026-02-01 | 1200 | 1000 | 20.00 |
| 2026-03-01 | 900 | 1200 | -25.00 |

**Real-world use case**

An executive KPI report shows MoM revenue growth per region. A month spine ensures a region with zero sales in one month shows −100% followed by NULL, instead of silently skipping the month.

**Common mistakes and red flags**

- Including the partial current month.
- Dividing by zero, or integer-dividing.
- Skipping missing months.

**Follow-up questions**

- How do you compute YoY instead? — `LAG(revenue, 12)` over a dense monthly series, or join on `month_start - INTERVAL 1 YEAR`.
- How do you compute CAGR? — `(end / start)^(1/years) - 1` using `POWER`.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q131" data-level="Advanced">
<summary><span class="qb-num">Q131</span><h3 class="qb-title">Build a cohort retention table</h3><span class="qb-meta">Advanced · Common · Analytics Patterns</span></summary>

**Short answer**

Assign each user to a cohort by first-activity month. For every later activity month, compute the months since the cohort start. Count distinct active users per (cohort, month offset) and divide by the cohort size. That gives a retention matrix: cohorts as rows, months 0, 1, 2 and so on as columns.

**Detailed explanation**

**Steps**
1. **Cohort assignment:** `cohort_month = DATE_TRUNC('month', MIN(activity_date))` per user. Use signup date if retention is defined from signup.
2. **Activity months:** distinct (user, activity_month).
3. **Offset:** `months_since = DATEDIFF(month, cohort_month, activity_month)`.
4. **Counts:** `COUNT(DISTINCT user_id)` per (cohort_month, months_since).
5. **Cohort size:** the count at offset 0.
6. **Retention rate:** active / cohort size.
7. Optionally **pivot** offsets into columns for display (see [Q122](/interview/sql/#q122)), but keep the long format in the warehouse.

**Definitions to agree on**
- Activity event (any login vs purchase).
- **Classic retention** (active in month N) vs **rolling or unbounded retention** (active in month N or later).
- Calendar months vs 30-day periods.
- Treatment of the current, incomplete month (exclude it, or mark it as partial).

**Reading the matrix.** Read across a row to see one cohort's decay. Read down a column to compare cohorts at the same age, which reveals whether product changes improved retention. Recent cohorts have fewer offsets observed, so the bottom-right of the triangle is empty by design.

**Example**

```sql
-- Dialect: Snowflake SQL
WITH user_months AS (
    SELECT DISTINCT user_id, DATE_TRUNC('month', activity_date) AS activity_month
    FROM user_activity
),
cohorts AS (
    SELECT user_id, MIN(activity_month) AS cohort_month
    FROM user_months
    GROUP BY user_id
),
cohort_activity AS (
    SELECT c.cohort_month,
           DATEDIFF(month, c.cohort_month, um.activity_month) AS months_since,
           COUNT(DISTINCT um.user_id) AS active_users
    FROM user_months um
    JOIN cohorts c ON c.user_id = um.user_id
    GROUP BY c.cohort_month, DATEDIFF(month, c.cohort_month, um.activity_month)
)
SELECT cohort_month,
       months_since,
       active_users,
       FIRST_VALUE(active_users) OVER (PARTITION BY cohort_month ORDER BY months_since) AS cohort_size,
       ROUND(active_users / FIRST_VALUE(active_users) OVER (PARTITION BY cohort_month ORDER BY months_since), 4)
           AS retention_rate
FROM cohort_activity
ORDER BY cohort_month, months_since;
```
FIRST_VALUE is safe here with the default frame because it reads the start of the frame, and offset 0 is always present for each cohort.

Sample: the January cohort has 100 users. 40 are active in February (offset 1) and 25 in March (offset 2). Retention: 1.00, 0.40, 0.25.

**Real-world use case**

A subscription app compares retention curves of cohorts acquired through paid ads and through referrals. Referral cohorts retain about 1.5 times better at month 3, which shifts marketing budget.

**Common mistakes and red flags**

- Using a cohort size that changes with the offset.
- Mixing signup cohorts with first-purchase cohorts.
- Including the partial current month as if it were complete.

**Follow-up questions**

- How do you compute revenue retention instead? — Sum revenue per cohort per offset, divided by offset-0 revenue (net revenue retention).
- How do you make it incremental? — Persist `user_first_seen`, then append new activity months per run.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q132" data-level="Advanced">
<summary><span class="qb-num">Q132</span><h3 class="qb-title">Sessionise events with a 30-minute inactivity gap</h3><span class="qb-meta">Advanced · Common · Analytics Patterns</span></summary>

**Short answer**

Per user, order events by time and compute the gap from the previous event with LAG. Flag a new session when there is no previous event or the gap exceeds 30 minutes. A running SUM of the flags gives a session number. Combine user ID and session number for a session ID, then aggregate start, end, duration and event count.

**Detailed explanation**

This is gaps-and-islands with a time threshold (see [Q117](/interview/sql/#q117)).

**Steps**
1. `prev_ts = LAG(event_ts) OVER (PARTITION BY user_id ORDER BY event_ts)`.
2. `is_new_session = CASE WHEN prev_ts IS NULL OR event_ts - prev_ts > 30 min THEN 1 ELSE 0 END`.
3. `session_seq = SUM(is_new_session) OVER (PARTITION BY user_id ORDER BY event_ts ROWS UNBOUNDED PRECEDING)`.
4. `session_id = user_id || '-' || session_seq`, or a hash.
5. Aggregate per session: `MIN(event_ts)`, `MAX(event_ts)`, `COUNT(*)`, and the landing page via `MIN_BY` or FIRST_VALUE.

**Considerations**
- **Ties** in `event_ts`: add `event_id` to the ORDER BY for determinism.
- **Midnight boundaries:** decide whether a session spans days. Usually yes, and you attribute it to its start date.
- **Maximum session length:** some definitions also split sessions at a cap (for example 24 hours) or on a campaign change.
- **Incremental processing:** sessions can span batch boundaries. Reprocess a lookback window, such as the last session per user or the last hour, or use streaming session windows (see [Q30](/interview/data-engineering/#q30)).
- **Identity:** stitch anonymous IDs to user IDs before sessionising, or session by device.

**Example**

```sql
-- Dialect: Snowflake SQL
WITH ordered AS (
    SELECT user_id, event_id, event_ts, page,
           LAG(event_ts) OVER (PARTITION BY user_id ORDER BY event_ts, event_id) AS prev_ts
    FROM web_events
),
flagged AS (
    SELECT *,
           CASE WHEN prev_ts IS NULL
                  OR DATEDIFF(second, prev_ts, event_ts) > 1800 THEN 1 ELSE 0 END AS is_new_session
    FROM ordered
),
numbered AS (
    SELECT *,
           SUM(is_new_session) OVER (PARTITION BY user_id ORDER BY event_ts, event_id
                                     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_seq
    FROM flagged
)
SELECT user_id,
       user_id || '-' || session_seq            AS session_id,
       MIN(event_ts)                            AS session_start,
       MAX(event_ts)                            AS session_end,
       DATEDIFF(second, MIN(event_ts), MAX(event_ts)) AS duration_sec,
       COUNT(*)                                 AS events,
       MIN_BY(page, event_ts)                   AS landing_page
FROM numbered
GROUP BY user_id, session_seq;
```

Sample for user U1: 10:00, 10:10, 10:50, 10:55.
The gaps are –, 10 min, 40 min and 5 min, so the flags are 1, 0, 1, 0 and the session sequence is 1, 1, 2, 2.
Session U1-1 runs 10:00–10:10 with 2 events. Session U1-2 runs 10:50–10:55 with 2 events.

**Real-world use case**

A product analytics team builds `fct_sessions` daily from clickstream data, reprocessing the last 2 hours of the previous day so sessions crossing midnight are stitched correctly.

**Common mistakes and red flags**

- Using GROUP BY on hour buckets as "sessions".
- Not handling ties or late events.
- Forgetting sessions that cross batch boundaries.

**Follow-up questions**

- How would you do this in Spark Structured Streaming? — `session_window(event_ts, "30 minutes")` with a watermark (see [Q30](/interview/data-engineering/#q30)).
- How do you compute bounce rate? — Share of sessions with exactly one pageview event.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q133" data-level="Advanced">
<summary><span class="qb-num">Q133</span><h3 class="qb-title">Merge overlapping date ranges</h3><span class="qb-meta">Advanced · Occasional · Gaps &amp; Islands</span></summary>

**Short answer**

Order ranges by start date per entity. Track the maximum end date seen so far, excluding the current row, with a window MAX. A new group starts when the current start is after that running maximum end. A running sum of these flags gives a group ID. Then take MIN(start) and MAX(end) per group.

**Detailed explanation**

**Problem:** a customer has subscriptions or contracts with overlapping periods. You want continuous coverage spans, for example to compute tenure or gaps in coverage.

**Why LAG alone isn't enough.** A long earlier range can overlap ranges several rows later:
`[Jan 1–Jun 30]`, `[Feb 1–Feb 28]`, `[Mar 1–Mar 31]`. The third range overlaps the first, not the second. You need the **running maximum end**, not just the previous row's end.

**Steps**
1. `prev_max_end = MAX(end_date) OVER (PARTITION BY id ORDER BY start_date, end_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)`.
2. `is_new_group = CASE WHEN prev_max_end IS NULL OR start_date > prev_max_end THEN 1 ELSE 0 END`.
   - For "adjacent ranges also merge" (end Jan 31, next start Feb 1), use `start_date > prev_max_end + 1 day`.
3. `grp = SUM(is_new_group) OVER (PARTITION BY id ORDER BY start_date, end_date ROWS UNBOUNDED PRECEDING)`.
4. Aggregate: `MIN(start_date)`, `MAX(end_date)` per (id, grp).

The ORDER BY includes `end_date` as a tie-breaker, so ranges with the same start date are processed deterministically.

**Example**

```sql
-- Dialect: Snowflake SQL
WITH ordered AS (
    SELECT customer_id, start_date, end_date,
           MAX(end_date) OVER (PARTITION BY customer_id ORDER BY start_date, end_date
                               ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prev_max_end
    FROM subscriptions
),
flagged AS (
    SELECT *,
           CASE WHEN prev_max_end IS NULL
                  OR start_date > DATEADD(day, 1, prev_max_end) THEN 1 ELSE 0 END AS is_new_group
    FROM ordered
),
grouped AS (
    SELECT *,
           SUM(is_new_group) OVER (PARTITION BY customer_id ORDER BY start_date, end_date
                                   ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS grp
    FROM flagged
)
SELECT customer_id, MIN(start_date) AS span_start, MAX(end_date) AS span_end
FROM grouped
GROUP BY customer_id, grp
ORDER BY customer_id, span_start;
```

Sample for customer 9: [01-01, 06-30], [02-01, 02-28], [03-01, 03-31], [07-01, 07-31], [09-01, 09-30].

| start | end | prev_max_end | is_new_group | grp |
|---|---|---|---|---|
| 01-01 | 06-30 | NULL | 1 | 1 |
| 02-01 | 02-28 | 06-30 | 0 | 1 |
| 03-01 | 03-31 | 06-30 | 0 | 1 |
| 07-01 | 07-31 | 06-30 | 0 (adjacent) | 1 |
| 09-01 | 09-30 | 07-31 | 1 | 2 |

Output: [01-01, 07-31] and [09-01, 09-30].

**Real-world use case**

An insurer computes continuous coverage periods per member from overlapping policy records. That determines eligibility for loyalty discounts and finds coverage gaps for compliance reports.

**Common mistakes and red flags**

- Comparing only with the previous row's end.
- Not deciding whether adjacency counts as overlap.
- NULL end dates for open-ended ranges. COALESCE them to a far-future date first.

**Follow-up questions**

- How do you find coverage gaps? — LEAD over merged spans: gap from `span_end + 1` to the next `span_start - 1`.
- How do you count concurrent overlaps at a point in time? — Explode into +1/−1 events at start/end and take a running sum ordered by time.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q134" data-level="Intermediate">
<summary><span class="qb-num">Q134</span><h3 class="qb-title">Customers who bought product A but never product B</h3><span class="qb-meta">Intermediate · Common · Joins &amp; Subqueries</span></summary>

**Short answer**

Group purchases by customer and use conditional aggregation: `HAVING SUM(CASE WHEN product = 'A' THEN 1 ELSE 0 END) > 0 AND SUM(CASE WHEN product = 'B' THEN 1 ELSE 0 END) = 0`. Alternatively, find buyers of A with a semi-join, then exclude buyers of B with NOT EXISTS.

**Detailed explanation**

**Option 1: conditional aggregation (one pass)**
- Group by customer and compute flags for each product condition.
- Easily extended: "bought A and C but not B" or "bought at least 2 of X, Y, Z".
- Efficient on columnar engines because it is a single scan and aggregation.

**Option 2: EXISTS / NOT EXISTS**
- Readable and NULL-safe (see [Q105](/interview/sql/#q105)).
- Two correlated lookups, which optimizers usually turn into semi and anti joins.

**Option 3: set operations**
- `SELECT customer_id FROM purchases WHERE product = 'A' EXCEPT SELECT customer_id FROM purchases WHERE product = 'B'`.
- Concise, and removes duplicates automatically.

**Clarify with the interviewer**
- Is "product" a single SKU or a category?
- Is there a time window (bought A in 2026, never bought B ever)?
- Should returns or cancelled orders count?

Stating these assumptions before writing SQL is what interviewers look for. The query itself is short, and the definitions decide whether the answer is right. Data quality also matters: normalise product codes (case, whitespace) before comparing.

**Example**

```sql
-- Dialect: ANSI SQL
-- Option 1: conditional aggregation
SELECT customer_id
FROM purchases
GROUP BY customer_id
HAVING SUM(CASE WHEN product_code = 'A' THEN 1 ELSE 0 END) > 0
   AND SUM(CASE WHEN product_code = 'B' THEN 1 ELSE 0 END) = 0;

-- Option 2: NOT EXISTS
SELECT DISTINCT p.customer_id
FROM purchases p
WHERE p.product_code = 'A'
  AND NOT EXISTS (SELECT 1 FROM purchases p2
                  WHERE p2.customer_id = p.customer_id AND p2.product_code = 'B');
```

Sample (customer, product): (1, A), (1, B), (2, A), (2, C), (3, B).
Output: customer 2 only.

**Real-world use case**

A marketing team targets customers who bought a phone but never a case. The conditional-aggregation query feeds a reverse-ETL audience sync each morning (see [Q97](/interview/data-engineering/#q97)).

**Common mistakes and red flags**

- `WHERE product = 'A' AND product <> 'B'`, which looks at single rows, not the customer's history.
- Using NOT IN on a nullable column.
- Joining A-buyers to non-B rows, which returns everyone who bought anything besides B.

**Follow-up questions**

- How about "bought A then B within 30 days"? — Self join or window with LEAD on ordered purchases, checking product sequence and date difference.
- Which option scales best? — Usually conditional aggregation: one scan, one shuffle.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q135" data-level="Intermediate">
<summary><span class="qb-num">Q135</span><h3 class="qb-title">Year-to-date and same-period-last-year comparisons</h3><span class="qb-meta">Intermediate · Common · Analytics Patterns</span></summary>

**Short answer**

YTD is a running sum partitioned by year and ordered by date. Same-period-last-year compares each date's YTD with the YTD of the same calendar day one year earlier, usually by joining on `date - 1 year` or using a calendar table with day-of-year alignment. Handle leap years and fiscal calendars deliberately.

**Detailed explanation**

**YTD**
- `SUM(daily_value) OVER (PARTITION BY YEAR(d) ORDER BY d ROWS UNBOUNDED PRECEDING)`.
- Use a dense daily series so every date has a row (see [Q121](/interview/sql/#q121)).
- For fiscal years, partition by the fiscal year from the calendar table.

**Same period last year (SPLY)**
- **Date-aligned:** join this year's date to `DATEADD(year, -1, d)`. 29 February maps to 28 February in many engines' date arithmetic, so check yours.
- **Weekday-aligned (retail):** compare with 364 days earlier (52 weeks), so Mondays compare with Mondays. A 4-4-5 retail calendar table handles this cleanly.
- **YTD vs prior YTD:** prior YTD at the same day of year, not the full prior year.

**Presentation:** growth % = (YTD − prior YTD) / prior YTD, with NULLIF for divide-by-zero.

**Partial current day.** If today's data is still loading, compare YTD through yesterday with prior YTD through the same date last year. Otherwise growth looks artificially low. When the period being compared has different numbers of trading days (holidays, store closures), consider comparing average daily revenue as well as totals.

**Example**

```sql
-- Dialect: Snowflake SQL
WITH daily AS (
    SELECT c.calendar_date AS d,
           COALESCE(SUM(s.amount), 0) AS revenue
    FROM dim_date c
    LEFT JOIN sales s ON s.sale_date = c.calendar_date
    WHERE c.calendar_date BETWEEN '2025-01-01' AND '2026-12-31'
    GROUP BY c.calendar_date
),
ytd AS (
    SELECT d, revenue,
           SUM(revenue) OVER (PARTITION BY YEAR(d) ORDER BY d
                              ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS ytd_revenue
    FROM daily
)
SELECT cur.d,
       cur.ytd_revenue,
       prev.ytd_revenue AS prior_ytd_revenue,
       ROUND(100 * (cur.ytd_revenue - prev.ytd_revenue) / NULLIF(prev.ytd_revenue, 0), 2) AS ytd_growth_pct
FROM ytd cur
LEFT JOIN ytd prev ON prev.d = DATEADD(year, -1, cur.d)
WHERE YEAR(cur.d) = 2026
ORDER BY cur.d;
```
Sample: if YTD revenue on 2026-03-15 is 1.2M and on 2025-03-15 it was 1.0M, YTD growth is 20.00%.

**Real-world use case**

A retail chain's weekly business review shows YTD sales against last year on a 4-4-5 fiscal calendar. The calendar table supplies `fiscal_year`, `fiscal_week` and the matching `prior_year_date`, so every report aligns the same way.

**Common mistakes and red flags**

- Comparing YTD with the full prior year.
- Not densifying dates, so YTD skips days.
- Ignoring leap years and weekday alignment.

**Follow-up questions**

- How would a semantic layer handle this? — Time-intelligence metrics (cumulative, offset windows) defined once and applied to any grain (see [Q329](/interview/dbt/#q329)).
- How do you do QTD? — Same pattern, partitioned by quarter.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q136" data-level="Beginner">
<summary><span class="qb-num">Q136</span><h3 class="qb-title">Conditional aggregation (SUM(CASE…) and FILTER)</h3><span class="qb-meta">Beginner · Common · Aggregation</span></summary>

**Short answer**

Conditional aggregation puts a condition inside an aggregate, such as `SUM(CASE WHEN status = 'paid' THEN amount ELSE 0 END)` or `COUNT(*) FILTER (WHERE status = 'paid')`, so one pass computes several filtered metrics side by side. It replaces multiple queries or self joins, and it is the basis of manual pivots.

**Detailed explanation**

**Forms**
- `SUM(CASE WHEN cond THEN x ELSE 0 END)`: portable.
- `COUNT(CASE WHEN cond THEN 1 END)`: counts matches. CASE without ELSE returns NULL, which COUNT ignores.
- `AVG(CASE WHEN cond THEN x END)`: average over matching rows only. Don't use `ELSE 0`, which would skew the average.
- `agg(x) FILTER (WHERE cond)`: ANSI SQL:2003, supported by PostgreSQL, Spark/Databricks SQL, SQLite and DuckDB. Not every engine supports it. In Snowflake the usual idioms are `COUNT_IF(cond)` and `SUM(IFF(cond, x, 0))`.
- Ratios: `SUM(CASE WHEN converted THEN 1 ELSE 0 END) * 1.0 / COUNT(*)`.

**Why it's efficient:** one scan of the table instead of one query per condition, and no joins between separately aggregated results.

**ELSE 0 vs ELSE NULL**
| Aggregate | Use ELSE 0? | Reason |
|---|---|---|
| SUM | Optional (affects NULL vs 0 when no matches) | 0 for "none" is often clearer |
| COUNT | No (use ELSE NULL / omit ELSE) | ELSE 0 would be counted |
| AVG | No | 0 would lower the average |
| MIN/MAX | No | 0 may become the min |

**Example**

```sql
-- Dialect: ANSI SQL
SELECT order_date,
       COUNT(*)                                              AS total_orders,
       COUNT(CASE WHEN status = 'paid' THEN 1 END)           AS paid_orders,
       SUM(CASE WHEN status = 'paid' THEN amount ELSE 0 END) AS paid_revenue,
       AVG(CASE WHEN status = 'paid' THEN amount END)        AS avg_paid_order,
       1.0 * COUNT(CASE WHEN status = 'refunded' THEN 1 END) / COUNT(*) AS refund_rate
FROM orders
GROUP BY order_date;
```
Sample day: orders (paid 100), (paid 50), (refunded 30), (created 20). Result: total 4, paid 2, paid_revenue 150, avg_paid 75, refund_rate 0.25.

**Real-world use case**

A funnel report computes visits, add-to-carts, checkouts and purchases per day from one events table with four COUNT(CASE ...) expressions, replacing four separate queries joined by date.

**Common mistakes and red flags**

- `COUNT(CASE WHEN ... THEN 1 ELSE 0 END)`, which counts every row.
- `AVG` with `ELSE 0`.
- Integer division in ratios.

**Follow-up questions**

- Snowflake shortcut? — `COUNT_IF(status = 'paid')` and `IFF(cond, a, b)`.
- How does this relate to pivoting? — Pivot is conditional aggregation where each condition becomes a column (see [Q122](/interview/sql/#q122)).

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q137" data-level="Intermediate">
<summary><span class="qb-num">Q137</span><h3 class="qb-title">MERGE for upserts</h3><span class="qb-meta">Intermediate · Very Common · DML</span></summary>

**Short answer**

MERGE compares a source with a target on a key and, in one atomic statement, updates matched rows, inserts unmatched ones and optionally deletes. It is the standard way to apply incremental changes and CDC. The source must have at most one row per key, otherwise the result is ambiguous or the statement errors.

**Detailed explanation**

**Syntax shape (ANSI-style)**
```text
MERGE INTO target t
USING source s ON t.key = s.key
WHEN MATCHED [AND cond] THEN UPDATE SET ...
WHEN MATCHED [AND cond] THEN DELETE
WHEN NOT MATCHED [AND cond] THEN INSERT (...) VALUES (...)
```
Some engines also support `WHEN NOT MATCHED BY SOURCE` to delete or update target rows missing from the source (SQL Server, Databricks).

**Correctness rules**
1. **Deduplicate the source** per key, keeping the latest change (see [Q115](/interview/sql/#q115)). Duplicate source matches cause errors: Snowflake raises a nondeterministic merge error by default (see [Q193](/interview/snowflake/#q193)), and Delta raises "multiple source rows matched".
2. **Conditional updates:** only update when something changed (`AND s.row_hash <> t.row_hash`) or when the source is newer (`AND s.updated_at > t.updated_at`). That avoids needless rewrites and keeps replays idempotent.
3. **Deletes from CDC:** `WHEN MATCHED AND s.op = 'D' THEN DELETE`, or a soft delete (see [Q51](/interview/data-engineering/#q51)).
4. **Idempotency:** MERGE on a key is idempotent if the update sets values rather than accumulating them (see [Q61](/interview/data-engineering/#q61)).

**Performance**
- The join between source and target is the cost. Prune the target with partition or cluster-key predicates in the ON clause (for example `AND t.order_date >= '2026-03-01'`) when changes are recent.
- On file-based tables, a MERGE rewrites every file that contains a matched row. Clustering by the merge key, plus deletion vectors, reduces rewrites (see [Q213](/interview/databricks/#q213), [Q443](/interview/scenario-based/#q443)).

**Example**

```sql
-- Dialect: ANSI SQL (works with minor changes in Snowflake, Databricks, SQL Server, Oracle)
MERGE INTO dim_product t
USING (
    SELECT product_id, product_name, list_price, updated_at, op
    FROM stg_product_changes
    -- assume already deduplicated to one latest row per product_id
) s
ON t.product_id = s.product_id
WHEN MATCHED AND s.op = 'D' THEN DELETE
WHEN MATCHED AND s.updated_at > t.updated_at THEN UPDATE SET
     product_name = s.product_name,
     list_price   = s.list_price,
     updated_at   = s.updated_at
WHEN NOT MATCHED AND s.op <> 'D' THEN INSERT (product_id, product_name, list_price, updated_at)
     VALUES (s.product_id, s.product_name, s.list_price, s.updated_at);
```
Sample: the target has P1 at price 10 (updated 09:00). The source has P1 at price 12 (10:00), P2 at price 5 (insert) and P3 with op D. Result: P1 updated to 12, P2 inserted, P3 deleted if it existed.

**Real-world use case**

A CDC pipeline merges each 5-minute batch of customer changes into the silver customer table. A change-detection condition avoids rewriting rows when the source sends unchanged duplicates.

**Common mistakes and red flags**

- MERGE with a non-unique source key.
- `UPDATE SET qty = t.qty + s.qty`, which is not idempotent.
- No pruning on huge targets, which scans everything.

**Follow-up questions**

- MERGE vs DELETE + INSERT? — MERGE is atomic and changes only affected rows; delete+insert by partition is simpler for full-partition rewrites.
- How do you MERGE in Spark? — Delta/Iceberg `MERGE INTO` SQL or the DeltaTable API (see [Q213](/interview/databricks/#q213)).

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q138" data-level="Beginner">
<summary><span class="qb-num">Q138</span><h3 class="qb-title">DELETE vs TRUNCATE vs DROP</h3><span class="qb-meta">Beginner · Very Common · DDL &amp; DML</span></summary>

**Short answer**

DELETE removes the rows that match a WHERE clause (or all rows) and is logged DML, so it can run inside a transaction. TRUNCATE removes all rows quickly while keeping the table structure. It is DDL-like in many engines and resets identity in some. DROP removes the table itself: structure, data, privileges and dependent metadata.

**Detailed explanation**

| Aspect | DELETE | TRUNCATE | DROP |
|---|---|---|---|
| Removes | Selected rows (WHERE) | All rows | Entire object |
| Keeps structure | Yes | Yes | No |
| WHERE clause | Yes | No | No |
| Speed on large tables | Slower (row/file-level work) | Fast (metadata/deallocation) | Fast |
| Triggers fire (OLTP) | Yes | Usually no | No |
| Identity reset | No | Often yes (SQL Server, PostgreSQL with RESTART IDENTITY) | N/A |
| Transactional rollback | Yes | Engine-dependent (yes in PostgreSQL and SQL Server; Oracle no) | Engine-dependent |
| Recovery in cloud warehouses | Time Travel | Time Travel | `UNDROP` / time travel within retention |

**Cloud and lakehouse specifics**
- **Snowflake:** DELETE, TRUNCATE and DROP are all recoverable within the Time Travel retention period (`UNDROP TABLE`, or `AT`/`BEFORE` queries; see [Q158](/interview/snowflake/#q158), [Q162](/interview/snowflake/#q162)). TRUNCATE also clears the table's load metadata, so previously loaded files can be reloaded by COPY. DELETE does not.
- **Delta Lake:** DELETE and TRUNCATE create new versions, and old files remain until VACUUM, so time travel can restore them. DROP of a managed table removes the data after a retention period in Unity Catalog, and `UNDROP TABLE` works for managed tables within 7 days.
- **Foreign keys** (in OLTP engines) may block TRUNCATE or DROP of referenced tables.

**Example**

```sql
-- Dialect: Snowflake SQL
DELETE FROM stg_orders WHERE load_date < DATEADD(day, -30, CURRENT_DATE);  -- selected rows
TRUNCATE TABLE stg_orders;                                                 -- all rows, keep table
DROP TABLE IF EXISTS tmp_orders_backfill;                                  -- remove object
UNDROP TABLE tmp_orders_backfill;                                          -- restore within retention
```

**Real-world use case**

A staging table was truncated and reloaded nightly, but after a TRUNCATE in Snowflake, COPY reloaded files it had already processed, because TRUNCATE cleared the load history. That duplicated data in a downstream append. The team switched to DELETE for that table (preserving load metadata) and made the downstream step a MERGE.

**Common mistakes and red flags**

- DELETE without WHERE on production by accident (use transactions and reviews).
- Assuming TRUNCATE can always be rolled back.
- Not knowing about UNDROP or time travel.

**Follow-up questions**

- What about `CREATE OR REPLACE`? — It drops and recreates; the previous version is recoverable via Time Travel/UNDROP in Snowflake, but grants are lost unless you use `COPY GRANTS`.
- How do you delete billions of rows efficiently? — Delete by partition, or rebuild the table with the rows to keep (CTAS) and swap.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q139" data-level="Intermediate">
<summary><span class="qb-num">Q139</span><h3 class="qb-title">Views vs materialized views</h3><span class="qb-meta">Intermediate · Very Common · Views</span></summary>

**Short answer**

A view is a stored query: no data is stored, and it runs against the base tables every time it is queried. A materialized view stores the precomputed result and is refreshed automatically or on demand, so reads are fast but storage and refresh cost money. Engines limit what SQL a materialized view can contain.

**Detailed explanation**

| Aspect | View | Materialized view |
|---|---|---|
| Stores data | No | Yes |
| Freshness | Always current | Depends on refresh (auto/incremental/manual) |
| Read cost | Full query each time | Fast lookup of stored results |
| Maintenance cost | None | Refresh compute + storage |
| SQL restrictions | Almost none | Often many (joins, windows, non-deterministic functions limited) |
| Typical use | Abstraction, security, simplified access | Expensive aggregations queried often |

**Platform specifics**
- **Snowflake:** materialized views are Enterprise edition and above, are maintained automatically by a serverless service, can reference only a single table (no joins), and allow limited aggregate functions (see [Q173](/interview/snowflake/#q173)). Dynamic Tables are the more flexible option for multi-table transformations (see [Q171](/interview/snowflake/#q171)).
- **Databricks:** materialized views are created in Databricks SQL or Lakeflow pipelines and refresh incrementally where possible (see [Q221](/interview/databricks/#q221)).
- **PostgreSQL:** `REFRESH MATERIALIZED VIEW` is manual (optionally `CONCURRENTLY`).
- **BigQuery:** automatic incremental refresh for supported queries, with smart tuning that rewrites queries to use them.
- **Query rewrite:** some engines (Snowflake, BigQuery, Oracle) can automatically use a materialized view when you query the base table.

**Other view types**
- **Secure views** (Snowflake) hide the definition and prevent some optimizer leaks, which matters for data sharing.
- **Dynamic Tables and streaming tables** are declarative pipelines rather than classic views.

**Example**

```sql
-- Dialect: Snowflake SQL
CREATE OR REPLACE VIEW v_paid_orders AS
SELECT order_id, customer_id, amount, order_date
FROM orders
WHERE status = 'paid';

CREATE OR REPLACE MATERIALIZED VIEW mv_daily_paid_revenue AS
SELECT order_date, SUM(amount) AS revenue, COUNT(*) AS orders
FROM orders
WHERE status = 'paid'
GROUP BY order_date;
```

**Real-world use case**

A dashboard computed daily revenue from a 5-billion-row table on every load. A materialized view of the daily aggregate made the dashboard sub-second, and its maintenance cost was far below the credits previously spent on repeated scans.

**Common mistakes and red flags**

- Stacking dozens of nested views, which creates slow, hard-to-debug queries.
- Creating materialized views on frequently updated tables where maintenance dominates.
- Expecting complex joins in Snowflake materialized views.

**Follow-up questions**

- When would you pick a table built by dbt instead? — When logic is complex, refresh can be scheduled, and you want full control and testing.
- What is a secure view? — A view whose definition and underlying details are hidden from consumers, used for sharing and row-level security.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q140" data-level="Advanced">
<summary><span class="qb-num">Q140</span><h3 class="qb-title">Indexes (clustered/non-clustered) and why columnar warehouses differ</h3><span class="qb-meta">Advanced · Very Common · Query Optimisation</span></summary>

**Short answer**

In row-store databases, a clustered index defines the physical row order (usually on the primary key), and non-clustered indexes are separate B-tree structures that point back to rows. They speed up point lookups and selective filters but slow writes. Columnar cloud warehouses mostly avoid traditional indexes and instead rely on partition and file metadata (min/max pruning), clustering or sorting, and optional search structures.

**Detailed explanation**

**Row-store (OLTP) indexes**
- **Clustered index:** the table *is* the index, with rows stored in key order. One per table (SQL Server, MySQL InnoDB's primary key).
- **Non-clustered / secondary index:** a separate B-tree of key → row locator. Many are allowed.
- **Covering index:** includes all columns a query needs, avoiding table lookups (`INCLUDE` columns).
- **Composite index order matters:** an index on `(customer_id, order_date)` helps filters on `customer_id`, or `customer_id` plus `order_date`, but not on `order_date` alone (the leftmost-prefix rule).
- **Costs:** every insert or update maintains every index, plus storage. Too many indexes slow down writes.

**Why columnar warehouses differ**
- Queries scan millions of rows across few columns. B-tree lookups per row would be slower than vectorised scans.
- Storage is immutable files or micro-partitions with **per-file statistics**. The engine skips files whose min/max ranges can't match (**pruning**).
- Performance therefore comes from **data layout**:
  - Snowflake clustering keys (see [Q155](/interview/snowflake/#q155)), plus Search Optimization Service for selective point lookups (see [Q156](/interview/snowflake/#q156)).
  - Delta Liquid Clustering and Z-ORDER (see [Q209](/interview/databricks/#q209)), and Iceberg sort orders.
  - BigQuery partitioning and clustering. Redshift sort keys and distribution keys.
- Some engines add lightweight index-like structures: bloom filters, search indexes, Snowflake hybrid tables with real indexes (see [Q199](/interview/snowflake/#q199)).

| Aspect | Row-store index | Columnar warehouse |
|---|---|---|
| Structure | B-tree | Min/max metadata per file/micro-partition |
| Best for | Point lookups, small ranges | Large scans, aggregations |
| Write impact | Each index maintained | Clustering maintenance (background) |
| Tuning lever | CREATE INDEX | Clustering/sort keys, partitioning |

**Example**

```sql
-- Dialect: PostgreSQL — composite + covering index for an OLTP lookup
CREATE INDEX idx_orders_customer_date
    ON orders (customer_id, order_date DESC)
    INCLUDE (amount, status);

-- Dialect: Snowflake SQL — layout-based tuning instead of indexes
ALTER TABLE orders CLUSTER BY (order_date, customer_id);
ALTER TABLE orders ADD SEARCH OPTIMIZATION ON EQUALITY(order_id);
```

**Real-world use case**

A team migrating from SQL Server to Snowflake tried to recreate 40 indexes and found none existed. Analysing the filters in their slowest queries and adding one clustering key on `event_date`, plus search optimization on `order_id`, met their SLAs.

**Common mistakes and red flags**

- Suggesting `CREATE INDEX` for Snowflake standard tables.
- Not knowing the leftmost-prefix rule.
- Indexing every column in OLTP.

**Follow-up questions**

- What is index selectivity? — The fraction of rows a predicate returns; low selectivity (many rows) makes indexes useless versus scans.
- Do Snowflake primary keys enforce uniqueness? — No; on standard tables PK/UNIQUE/FK are informational only (NOT NULL is enforced).

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q141" data-level="Intermediate">
<summary><span class="qb-num">Q141</span><h3 class="qb-title">How do you read an execution plan?</h3><span class="qb-meta">Intermediate · Very Common · Query Optimisation</span></summary>

**Short answer**

An execution plan is the tree of physical operators (scans, filters, joins, aggregations, sorts, exchanges or shuffles) the engine will run or ran. Read it from the leaves (table scans) up to the root. Find where most time or rows go, check that estimated and actual row counts roughly agree, and look for expensive patterns: full scans with no pruning, exploding joins, spills, skew, and large sorts or shuffles.

**Detailed explanation**

**Getting the plan**
- `EXPLAIN` gives the estimated plan (most engines). `EXPLAIN ANALYZE` gives actual runtime statistics (PostgreSQL, MySQL 8, Databricks SQL query profile).
- Snowflake: `EXPLAIN` for the logical/physical plan, and the **Query Profile** in Snowsight (or `GET_QUERY_OPERATOR_STATS`) for actual per-operator statistics.
- Spark: `df.explain("formatted")` and the SQL tab in the Spark UI (see [Q276](/interview/pyspark/#q276)).

**What to look for**
1. **Most expensive operator:** percentage of time, bytes or rows. Optimise there first.
2. **Scans and pruning:** partitions or files scanned vs total. Snowflake shows "Partitions scanned / Partitions total". Poor pruning means filters aren't aligned with layout.
3. **Row counts through joins:** output rows far greater than inputs means fan-out or a missing join condition (see [Q127](/interview/sql/#q127)).
4. **Estimated vs actual rows (OLTP):** big differences point to stale statistics and bad join order or algorithm choice.
5. **Join algorithm and distribution:** hash vs merge vs nested loop (see [Q144](/interview/sql/#q144)); broadcast vs shuffle in distributed engines.
6. **Spilling:** "bytes spilled to local/remote storage" means memory pressure (bigger warehouse or less data per operator).
7. **Skew:** one worker processing far more than the others.
8. **Filter placement:** filters applied late (after a join) instead of being pushed down.
9. **Repeated subplans:** the same CTE computed multiple times.

**Example**

```sql
-- Dialect: PostgreSQL — actual run statistics
EXPLAIN (ANALYZE, BUFFERS)
SELECT c.segment, SUM(o.amount)
FROM orders o
JOIN customers c ON c.customer_id = o.customer_id
WHERE o.order_date >= DATE '2026-01-01'
GROUP BY c.segment;

-- Dialect: Snowflake SQL — operator stats for a finished query
SELECT operator_id, operator_type, operator_statistics, execution_time_breakdown
FROM TABLE(GET_QUERY_OPERATOR_STATS(LAST_QUERY_ID()))
ORDER BY operator_id;
```

**Real-world use case**

A 12-minute Snowflake query's profile showed a join producing 4 billion rows from inputs of 50 million and 2 million. A missing second join column caused it. Fixing the join cut runtime to 20 seconds.

**Common mistakes and red flags**

- Optimising by guesswork without reading the plan.
- Reading only the estimated plan in OLTP and missing estimate errors.
- Ignoring spill and pruning statistics in cloud warehouses.

**Follow-up questions**

- What causes bad cardinality estimates? — Stale or missing statistics, correlated predicates, functions on columns, and skewed data.
- What is an exchange operator? — Data redistribution between workers (shuffle or broadcast) in distributed plans.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q142" data-level="Intermediate">
<summary><span class="qb-num">Q142</span><h3 class="qb-title">A practical query optimisation checklist</h3><span class="qb-meta">Intermediate · Very Common · Query Optimisation</span></summary>

**Short answer**

Measure first with the plan or profile. Then reduce the data read: select only the needed columns, filter early on pruning-friendly columns, and avoid functions on filtered columns. Fix joins: correct keys, no fan-out, broadcast small tables, pre-aggregate. Avoid unnecessary sorts and DISTINCTs, materialise expensive repeated logic, and only then consider more compute.

**Detailed explanation**

**1. Read less data**
- No `SELECT *` on wide columnar tables. Project only the needed columns.
- Filter on partition, cluster or sort columns so pruning works (see [Q154](/interview/snowflake/#q154), [Q277](/interview/pyspark/#q277)).
- Keep predicates **sargable**: `order_date >= '2026-01-01'`, not `YEAR(order_date) = 2026` (see [Q143](/interview/sql/#q143)).
- Push filters before joins and aggregations. Most optimizers do this, but complex views and UDFs can block it.

**2. Join smarter**
- Join on the full unique key, and check grain to avoid fan-out (see [Q127](/interview/sql/#q127)).
- Pre-aggregate large fact tables before joining them to other facts.
- Let small dimensions broadcast (see [Q262](/interview/pyspark/#q262)), and avoid joining on expressions or mismatched types.
- Replace correlated subqueries with joins or window functions (see [Q124](/interview/sql/#q124)).

**3. Avoid unnecessary work**
- `UNION ALL` instead of `UNION` when duplicates are impossible (see [Q106](/interview/sql/#q106)).
- Remove redundant DISTINCT and ORDER BY in subqueries and CTEs (inner ORDER BY is usually ignored).
- Use `EXISTS` instead of joining and then deduplicating.
- Avoid repeatedly scanning the same big table in many CTEs. Compute once.

**4. Materialise and cache**
- Build incremental intermediate tables for heavy shared logic (dbt models).
- Materialized views or dynamic tables for hot aggregates (see [Q139](/interview/sql/#q139), [Q171](/interview/snowflake/#q171)).
- Result caching: identical queries on unchanged data can be served free (Snowflake result cache; see [Q157](/interview/snowflake/#q157)).

**5. Data layout and statistics**
- Clustering or sorting aligned with common filters, and compaction of small files (see [Q57](/interview/data-engineering/#q57)).
- Up-to-date statistics in OLTP engines (`ANALYZE`).

**6. Compute last**
- Scale up when the profile shows spilling or the work genuinely needs more memory or CPU. Scale out for concurrency (see [Q63](/interview/data-engineering/#q63)).

**Example**

```sql
-- Dialect: Snowflake SQL
-- Before: non-sargable filter, SELECT *, late filter after join
-- SELECT * FROM orders o JOIN customers c ON o.customer_id = c.customer_id
-- WHERE YEAR(o.order_date) = 2026 AND c.country = 'IN';

-- After: needed columns, sargable range, filter both sides early
SELECT o.order_id, o.amount, o.order_date, c.segment
FROM orders o
JOIN (SELECT customer_id, segment FROM customers WHERE country = 'IN') c
  ON o.customer_id = c.customer_id
WHERE o.order_date >= '2026-01-01' AND o.order_date < '2027-01-01';
```

**Real-world use case**

A nightly job's top query scanned 100% of micro-partitions because it filtered with `TO_CHAR(event_ts, 'YYYY-MM') = '2026-03'`. Switching to a timestamp range enabled pruning and cut the scan to about 3% of partitions.

**Common mistakes and red flags**

- Jumping straight to "use a bigger warehouse or cluster".
- Optimising without measuring before and after.
- Adding DISTINCT to fix duplicates caused by bad joins.

**Follow-up questions**

- How do you find the queries worth optimising? — Rank by total cost (credits × frequency) in query history, not just by single-run duration.
- When is a bigger warehouse the right answer? — When the profile shows spilling or the work is CPU-bound and parallelisable.

**In depth:** [How would you optimize a slow analytical SQL query?](/interview/sql/optimize-slow-query/)

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q143" data-level="Advanced">
<summary><span class="qb-num">Q143</span><h3 class="qb-title">What makes a predicate sargable?</h3><span class="qb-meta">Advanced · Common · Query Optimisation</span></summary>

**Short answer**

A predicate is sargable ("Search ARGument ABLE") when the engine can use it directly against an index or storage metadata, because the column appears bare on one side of a simple comparison. Wrapping the column in a function, doing arithmetic on it, using implicit type conversion or a leading-wildcard LIKE usually makes the predicate non-sargable. That forces full scans or disables pruning.

**Detailed explanation**

**Sargable**
- `order_date >= '2026-03-01' AND order_date < '2026-04-01'`
- `customer_id = 42`
- `name LIKE 'Ras%'` (prefix match)
- `amount BETWEEN 100 AND 200`
- `status IN ('paid', 'shipped')`

**Non-sargable (and fixes)**
| Non-sargable | Why | Sargable rewrite |
|---|---|---|
| `YEAR(order_date) = 2026` | Function on column | `order_date >= '2026-01-01' AND order_date < '2027-01-01'` |
| `DATE(event_ts) = '2026-03-01'` | Function on column | `event_ts >= '2026-03-01' AND event_ts < '2026-03-02'` |
| `amount * 1.18 > 1000` | Arithmetic on column | `amount > 1000 / 1.18` |
| `name LIKE '%son'` | Leading wildcard | Reverse-string column, full-text/search index |
| `CAST(id AS VARCHAR) = '42'` / implicit conversion | Type mismatch | Compare with the column's native type |
| `COALESCE(region, 'NA') = 'EU'` | Function on column | `region = 'EU'` (NULLs can't equal 'EU' anyway) |
| `col1 + col2 > 10` | Expression over columns | Persist a computed column if frequently filtered |

**In columnar warehouses** the same idea applies to **pruning**. Min/max statistics are stored on raw column values, so a function on the column can prevent the engine from using them. Some engines can still prune through certain simple functions, such as Snowflake with some date truncations, but don't count on it. Write range predicates.

**Implicit conversions** are sneaky: comparing a VARCHAR column with a number may cast the *column* row by row, which kills index use in OLTP engines and can cause errors on bad values.

**Example**

```sql
-- Dialect: ANSI SQL
-- Non-sargable: function on the indexed/clustered column
SELECT COUNT(*) FROM orders WHERE EXTRACT(MONTH FROM order_date) = 3 AND EXTRACT(YEAR FROM order_date) = 2026;

-- Sargable: half-open range on the bare column
SELECT COUNT(*) FROM orders
WHERE order_date >= DATE '2026-03-01' AND order_date < DATE '2026-04-01';
```

**Real-world use case**

A SQL Server report filtered `WHERE CONVERT(date, created_at) = @d` on a 300M-row table, which scanned the whole index. Rewriting it as a half-open range on `created_at` turned it into an index seek, and the runtime dropped from minutes to milliseconds.

**Common mistakes and red flags**

- Using `BETWEEN` with timestamps and an end-of-day literal, which misses fractional seconds. Use half-open ranges.
- Assuming the optimizer always fixes function-wrapped predicates.
- Ignoring data-type mismatches in joins and filters.

**Follow-up questions**

- Why half-open ranges? — `>= start AND < next_start` is correct for any timestamp precision and is sargable.
- How do you support case-insensitive search sargably? — Store a normalised (lowercased) column or use collation-aware indexes.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q144" data-level="Advanced">
<summary><span class="qb-num">Q144</span><h3 class="qb-title">Nested loop vs hash vs merge join algorithms</h3><span class="qb-meta">Advanced · Common · Query Optimisation</span></summary>

**Short answer**

A nested loop join compares each outer row with the inner side, which is efficient only when the outer side is small and the inner side is indexed. A hash join builds a hash table on the smaller input and probes it with the larger one. It is the workhorse for large equi-joins. A sort-merge join sorts both inputs on the key and merges them, which is good when inputs are already sorted or too big to hash in memory.

**Detailed explanation**

| Algorithm | How | Best when | Weakness | Join types |
|---|---|---|---|---|
| Nested loop | For each outer row, scan/seek inner | Tiny outer side + index on inner; non-equi conditions | O(n×m) without index | Any condition (including `<`, `BETWEEN`) |
| Hash join | Build hash table on smaller side; probe with larger | Large unsorted inputs, equality conditions | Needs memory; spills if build side too big; equality only | Equi-joins |
| Sort-merge join | Sort both sides on key; merge in one pass | Inputs pre-sorted/indexed; huge inputs | Sorting cost; equality (and some range) joins | Equi-joins mainly |

**Distributed variations (Spark, warehouses)**
- **Broadcast hash join:** copy the small table to every worker and hash-join locally. No shuffle of the big table (see [Q262](/interview/pyspark/#q262)).
- **Shuffle hash join:** repartition both sides by key, then hash-join per partition.
- **Sort-merge join:** Spark's default for large-large equi-joins. Shuffle and sort both sides by key, then merge (see [Q261](/interview/pyspark/#q261)).
- **Broadcast nested loop join:** used for non-equi joins in Spark. It can be very slow on big data.

**How optimizers choose:** estimated input sizes, available indexes, sort order, memory, join condition type and hints. Bad estimates lead to bad choices, for example a nested loop over millions of rows.

**Range or non-equi joins** (`event_ts BETWEEN valid_from AND valid_to`) can't use a pure hash join. Strategies include bucketing by date and adding an equality condition on the bucket, interval-join optimisations, or ASOF joins where supported (Snowflake `ASOF JOIN`, DuckDB; see [Q100](/interview/data-engineering/#q100)).

**Example**

```sql
-- Dialect: Spark SQL — steering the join strategy with hints
SELECT /*+ BROADCAST(d) */ f.order_id, d.region
FROM fct_orders f
JOIN dim_store d ON f.store_id = d.store_id;

SELECT /*+ MERGE(a, b) */ a.id, b.val
FROM big_a a JOIN big_b b ON a.id = b.id;
```

**Real-world use case**

A join between events and an SCD2 dimension on a date range produced a broadcast nested loop join in Spark that ran for hours. Adding an equality join on `customer_id` alongside the date-range condition let Spark use a sort-merge join on the key and filter the range afterwards. It finished in minutes.

**Common mistakes and red flags**

- Saying "hash join is always best".
- Not recognising that non-equi joins limit algorithm choices.
- Forcing hints permanently instead of fixing statistics or layout.

**Follow-up questions**

- What is the build side? — The input loaded into the hash table; it should be the smaller one.
- Why does Spark prefer sort-merge for large joins? — It scales beyond memory through external sorting and handles big inputs robustly.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q145" data-level="Advanced">
<summary><span class="qb-num">Q145</span><h3 class="qb-title">Window function vs GROUP BY + join: performance trade-offs</h3><span class="qb-meta">Advanced · Occasional · Query Optimisation</span></summary>

**Short answer**

To attach a group-level aggregate to every row, you can use a window function (`SUM(x) OVER (PARTITION BY k)`) or aggregate in a subquery and join it back. Windows avoid the join and read the table once. But they must partition, and often sort, all rows while keeping them, which can be heavier than hash-aggregating to a small result and broadcasting it. Measure for your data.

**Detailed explanation**

**Window approach**
- One scan. The engine repartitions by `k` (a shuffle in distributed engines) and, if there is an ORDER BY, sorts within partitions.
- All detail rows flow through the window operator, so memory and spill risk scale with the input.
- Without ORDER BY, many engines can compute partition-level aggregates without a full sort.

**Aggregate + join approach**
- Aggregate first. With few distinct groups, the result is tiny.
- Join back, possibly as a broadcast of the small aggregate.
- That means two passes over the data (unless cached), but each pass is cheap. Hash aggregation is very efficient and partial-aggregates before shuffling.

**Rules of thumb**
| Situation | Usually better |
|---|---|
| Need ranking, running totals, LAG/LEAD | Window (no practical alternative) |
| Partition-level aggregate, few groups, huge table | Aggregate + broadcast join can win |
| Many groups (high cardinality), moderate size | Window often simpler and comparable |
| Multiple different aggregates over same partition | Window (one partitioning step) |
| Readability | Window |

Distributed engines may also **reuse** a single shuffle for several window functions with the same PARTITION BY, so group them consistently. Mixing many different partition specs causes repeated shuffles.

**Example**

```sql
-- Dialect: ANSI SQL
-- Window version
SELECT order_id, customer_id, amount,
       amount / SUM(amount) OVER (PARTITION BY customer_id) AS share_of_customer
FROM orders;

-- Aggregate + join version
WITH totals AS (
    SELECT customer_id, SUM(amount) AS customer_total
    FROM orders
    GROUP BY customer_id
)
SELECT o.order_id, o.customer_id, o.amount,
       o.amount / t.customer_total AS share_of_customer
FROM orders o
JOIN totals t ON t.customer_id = o.customer_id;
```
Both return identical results. Compare their plans and runtimes on real data volumes.

**Real-world use case**

On a 20-billion-row Spark table with 200 country groups, computing a country-level total with a window shuffled all 20 billion rows. Aggregating to 200 rows and broadcasting them back was several times faster.

**Common mistakes and red flags**

- Claiming one approach is always faster.
- Using different PARTITION BY clauses for each window in one query without need.
- Not checking spill in the window operator.

**Follow-up questions**

- How do you reduce window cost? — Filter early, partition by the needed key only, avoid ORDER BY if not required, and pre-aggregate where possible.
- Can windows run in parallel? — Yes, across partitions; one giant partition (skew) becomes a bottleneck.

**In depth:** [How do window functions differ from GROUP BY?](/interview/sql/window-functions-vs-group-by/)

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q146" data-level="Advanced">
<summary><span class="qb-num">Q146</span><h3 class="qb-title">Transaction isolation levels and anomalies</h3><span class="qb-meta">Advanced · Common · Transactions</span></summary>

**Short answer**

Isolation levels control what concurrent transactions can see of each other's changes. The ANSI levels, from weakest to strongest, are Read Uncommitted, Read Committed, Repeatable Read and Serializable. They allow or prevent dirty reads, non-repeatable reads and phantoms. Many modern engines use MVCC snapshots, so actual behaviour and anomalies such as write skew vary by product.

**Detailed explanation**

**Anomalies**
- **Dirty read:** reading another transaction's uncommitted changes.
- **Non-repeatable read:** reading the same row twice in a transaction and getting different committed values.
- **Phantom read:** re-running a range query returns new rows inserted and committed by another transaction.
- **Lost update:** two transactions read and then write the same row, and one overwrites the other.
- **Write skew:** two transactions read overlapping data and write different rows, together violating a constraint (possible under snapshot isolation).

| Level | Dirty read | Non-repeatable | Phantom |
|---|---|---|---|
| Read Uncommitted | Possible | Possible | Possible |
| Read Committed | Prevented | Possible | Possible |
| Repeatable Read | Prevented | Prevented | Possible (ANSI); prevented in some MVCC engines |
| Serializable | Prevented | Prevented | Prevented |

**Engine notes**
- **PostgreSQL:** default Read Committed. Repeatable Read is snapshot isolation (no phantoms, write skew possible). Serializable uses SSI.
- **SQL Server:** default Read Committed (locking; RCSI optional). Snapshot isolation available.
- **MySQL InnoDB:** default Repeatable Read, with gap locks to limit phantoms.
- **Snowflake:** statements see a consistent snapshot. The supported isolation level is Read Committed. DML on the same table can block on table- or partition-level locks (see [Q194](/interview/snowflake/#q194)).
- **Delta/Iceberg:** snapshot isolation for readers. Writers use optimistic concurrency, and Delta offers WriteSerializable (default) and Serializable isolation levels (see [Q204](/interview/databricks/#q204)).

**Why data engineers care:** concurrent loads into the same table, reading consistent extracts from OLTP sources (use a snapshot or a consistent export), and avoiding lost updates in control tables (watermarks, job state).

**Example**

```sql
-- Dialect: PostgreSQL — consistent multi-table extract from one snapshot
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;
SELECT * FROM orders      WHERE updated_at >= '2026-03-01';
SELECT * FROM order_lines WHERE updated_at >= '2026-03-01';
COMMIT;
```

**Real-world use case**

An extract read `orders` and then `order_lines` in separate Read Committed statements while the application was writing. Some lines referenced orders that weren't in the extract. Running both reads in one Repeatable Read snapshot gave a consistent pair.

**Common mistakes and red flags**

- Thinking Serializable is free (it costs throughput, and can abort transactions).
- Assuming every engine implements the levels the same way.
- Not knowing what a phantom read is.

**Follow-up questions**

- How do you prevent lost updates? — `SELECT ... FOR UPDATE`, optimistic concurrency with a version column, or atomic `UPDATE ... SET x = x + 1`.
- What is MVCC? — Multi-version concurrency control: readers see a snapshot while writers create new row versions (see [Q3](/interview/data-engineering/#q3)).

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q147" data-level="Beginner">
<summary><span class="qb-num">Q147</span><h3 class="qb-title">ACID properties</h3><span class="qb-meta">Beginner · Very Common · Transactions</span></summary>

**Short answer**

ACID describes reliable transactions. Atomicity means all-or-nothing. Consistency means a transaction moves the database from one valid state to another, respecting constraints. Isolation means concurrent transactions don't interfere beyond the chosen isolation level. Durability means committed data survives crashes.

**Detailed explanation**

- **Atomicity:** a transfer debits account A and credits account B. Either both happen or neither does. Implemented with logs (undo/redo) or, in table formats, a single atomic metadata commit (see [Q33](/interview/data-engineering/#q33)).
- **Consistency:** constraints (NOT NULL, foreign keys, checks) and invariants hold before and after every transaction. Note that cloud warehouses often don't *enforce* PK or FK constraints, so consistency of keys becomes the pipeline's job (see [Q140](/interview/sql/#q140)).
- **Isolation:** concurrent transactions behave as if run in some order, to the degree the isolation level guarantees (see [Q146](/interview/sql/#q146)).
- **Durability:** once committed, data persists. Implemented with write-ahead logs, replication and durable object storage.

**ACID in data platforms**
| Platform | ACID support |
|---|---|
| OLTP RDBMS | Full, multi-statement transactions |
| Snowflake | ACID transactions; multi-statement via `BEGIN ... COMMIT`; DDL auto-commits |
| Delta Lake / Iceberg / Hudi | ACID per table commit; multi-table transactions limited (emerging in catalogs) |
| Plain Parquet in a lake | No ACID |
| Kafka | Atomic multi-partition writes with transactions; not a database |

**BASE vs ACID:** many NoSQL systems favour BASE (Basically Available, Soft state, Eventually consistent) for availability and scale (see [Q60](/interview/data-engineering/#q60)).

**Example**

```sql
-- Dialect: Snowflake SQL — both steps commit together or not at all
BEGIN;
UPDATE accounts SET balance = balance - 500 WHERE account_id = 'A-1';
UPDATE accounts SET balance = balance + 500 WHERE account_id = 'B-7';
INSERT INTO transfer_log (from_acct, to_acct, amount, transfer_ts)
VALUES ('A-1', 'B-7', 500, CURRENT_TIMESTAMP());
COMMIT;
```
In Snowflake, a DDL statement inside the block (such as CREATE TABLE) implicitly commits the open transaction, so keep DDL outside multi-statement DML transactions.

**Real-world use case**

A two-step SCD2 load (expire, then insert) ran as separate auto-committed statements. A failure between them left customers with no current row. Wrapping both in one transaction made the load atomic (see [Q12](/interview/data-engineering/#q12)).

**Common mistakes and red flags**

- Confusing ACID consistency with CAP consistency.
- Assuming lakehouse tables give multi-table transactions.
- Not knowing that DDL commits implicitly in some engines.

**Follow-up questions**

- How do you get atomic updates across several Delta tables? — Generally you can't in one commit; design idempotent steps, or publish through a single view/pointer switch (see [Q91](/interview/data-engineering/#q91)).
- What is a write-ahead log? — A log of changes written before data pages, enabling crash recovery and durability.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q148" data-level="Intermediate">
<summary><span class="qb-num">Q148</span><h3 class="qb-title">Stored procedures vs user-defined functions</h3><span class="qb-meta">Intermediate · Common · Procedural SQL</span></summary>

**Short answer**

A user-defined function (UDF) returns a value (scalar) or a table (table function) and is called inside a query, so it should be side-effect-free. A stored procedure is called on its own (`CALL`), can contain control flow, multiple statements, DDL and DML and transactions, and is used for administrative and orchestration logic.

**Detailed explanation**

| Aspect | UDF | Stored procedure |
|---|---|---|
| Invocation | Inside SELECT/WHERE/JOIN | `CALL proc(args)` |
| Returns | Scalar or table | Optional value/result set |
| Side effects (DML/DDL) | Not allowed (generally) | Allowed |
| Control flow | Limited (depends on language) | Loops, conditionals, exception handling |
| Typical use | Reusable calculation, parsing, masking logic | Maintenance jobs, dynamic SQL, multi-step loads |
| Performance concern | Called per row; non-SQL UDFs can block optimizations | Runs statements sequentially |

**Platform notes**
- **Snowflake:** UDFs in SQL, JavaScript, Python, Java and Scala (scalar, plus table functions as UDTFs). Procedures in Snowflake Scripting (SQL), JavaScript, Python, Java and Scala. Procedures run with **owner's rights** or **caller's rights** (see [Q190](/interview/snowflake/#q190)).
- **Databricks:** SQL and Python UDFs registered in Unity Catalog. Python UDFs are slower than built-ins (see [Q270](/interview/pyspark/#q270)).
- **PostgreSQL:** functions (PL/pgSQL and others) and, since version 11, `CREATE PROCEDURE` with transaction control.

**Good practice**
- Prefer **built-in functions** and plain SQL. UDFs, especially non-SQL ones, can prevent pushdown and parallel optimisations.
- SQL UDFs are often inlined by the optimizer, while Python and JavaScript UDFs are black boxes.
- Keep business transformation logic in version-controlled models (dbt) rather than opaque procedures. Use procedures for operational tasks.
- Parameterise dynamic SQL safely: bind variables and validated identifiers, to avoid SQL injection.

**Example**

```sql
-- Dialect: Snowflake SQL
-- Scalar SQL UDF: reusable, side-effect free
CREATE OR REPLACE FUNCTION net_amount(gross NUMBER(12,2), discount NUMBER(12,2))
RETURNS NUMBER(12,2)
AS 'gross - COALESCE(discount, 0)';

-- Stored procedure (Snowflake Scripting): maintenance with control flow
CREATE OR REPLACE PROCEDURE purge_old_staging(days_to_keep INTEGER)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
  cutoff DATE DEFAULT DATEADD(day, -1 * :days_to_keep, CURRENT_DATE());
  deleted_rows INTEGER;
BEGIN
  DELETE FROM stg_orders WHERE load_date < :cutoff;
  deleted_rows := SQLROWCOUNT;
  RETURN 'Deleted ' || deleted_rows || ' rows older than ' || cutoff;
END;
$$;

CALL purge_old_staging(30);
```

**Real-world use case**

A team moved a 2,000-line stored procedure that built all marts into dbt models. Logic became testable and reviewable, and the remaining procedures only handle operational chores such as purging staging data and rotating clones.

**Common mistakes and red flags**

- Putting heavy per-row Python UDFs in large queries without considering performance.
- Building dynamic SQL by string concatenation of user input.
- Hiding business logic in procedures with no tests or version control.

**Follow-up questions**

- What is a UDTF? — A table function that returns rows, called in FROM (for example to explode or parse data).
- Owner's vs caller's rights? — Owner's rights run with the procedure owner's privileges (controlled elevation); caller's rights use the caller's privileges and session.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q149" data-level="Intermediate">
<summary><span class="qb-num">Q149</span><h3 class="qb-title">Data-type traps: integer division, FLOAT vs DECIMAL, time zones</h3><span class="qb-meta">Intermediate · Common · Data Types</span></summary>

**Short answer**

Some engines truncate integer division (`5/2 = 2` in PostgreSQL and SQL Server), so cast to decimal or multiply by 1.0 for ratios. Use DECIMAL/NUMERIC for money, because FLOAT is binary-approximate and accumulates rounding errors. Store timestamps in UTC (or timezone-aware types) and convert for display, being careful with DST and date boundaries.

**Detailed explanation**

**Integer division**
| Engine | `5 / 2` |
|---|---|
| PostgreSQL, SQL Server, SQLite | 2 (integer) |
| Snowflake, MySQL, Oracle, BigQuery (`/`), Spark (ANSI default) | 2.5 |

Portable fix: `5 * 1.0 / 2` or `CAST(5 AS DECIMAL(10,4)) / 2`.

**FLOAT vs DECIMAL**
- `FLOAT`/`DOUBLE` are binary floating point: `0.1 + 0.2` is not exactly `0.3`. Fine for scientific measurements and ML features.
- `DECIMAL(p, s)` / `NUMBER(p, s)` is exact decimal: required for money, quantities and anything that must reconcile.
- Watch precision and scale on division and multiplication. Engines derive the result scale, and you may need to round explicitly.
- Summing millions of floats in a different order (parallel engines) can give slightly different results between runs.

**Timestamps and time zones**
- Types differ: Snowflake `TIMESTAMP_NTZ` (no zone), `TIMESTAMP_LTZ` (session zone) and `TIMESTAMP_TZ` (stores offset). PostgreSQL `timestamp` vs `timestamptz`. Spark `TIMESTAMP` (session-zone-aware) vs `TIMESTAMP_NTZ`.
- **Store in UTC.** Convert at the edge with `CONVERT_TIMEZONE` / `AT TIME ZONE` / `from_utc_timestamp`.
- **Business dates:** "orders on 1 March" depends on the business time zone. Derive `order_date_local` explicitly per region.
- **DST:** local days can have 23 or 25 hours, and local times can repeat or not exist. Do hourly bucketing in UTC.
- **Session settings** (`TIMEZONE`, `spark.sql.session.timeZone`) silently change results for zone-aware types.

**Strings and others**
- Lexicographic comparison of numbers stored as strings (`'10' < '9'`).
- Trailing spaces and collation in comparisons.
- Overflow in `INT` sums. Use BIGINT/NUMBER for aggregates.

**Example**

```sql
-- Dialect: PostgreSQL — integer division trap and fix
SELECT 7 / 2            AS int_division,      -- 3
       7 * 1.0 / 2      AS decimal_division,  -- 3.5000000000000000
       CAST(0.1 AS DOUBLE PRECISION) + 0.2 = 0.3 AS float_equal,   -- false
       CAST(0.1 AS NUMERIC) + 0.2 = 0.3          AS numeric_equal; -- true

-- Dialect: Snowflake SQL — business date in the store's time zone from a UTC timestamp
SELECT order_id,
       order_ts_utc,
       CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', order_ts_utc)::DATE AS order_date_ist
FROM orders;
```

**Real-world use case**

A conversion-rate KPI showed 0% for every day after a migration to PostgreSQL, because `conversions / visits` became integer division. Separately, an India sales report attributed late-evening orders to the next day until order dates were derived in IST instead of UTC.

**Common mistakes and red flags**

- FLOAT for currency.
- Mixing NTZ and TZ timestamps across tables.
- Deriving dates from UTC timestamps for local business reporting.

**Follow-up questions**

- How do you store money across currencies? — DECIMAL amount plus currency code, and convert with dated FX rates in a separate step.
- How do you test time-zone logic? — Unit tests with fixtures around midnight and DST transitions (see [Q88](/interview/data-engineering/#q88)).

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q150" data-level="Advanced">
<summary><span class="qb-num">Q150</span><h3 class="qb-title">Approximate aggregation (APPROX_COUNT_DISTINCT, HyperLogLog)</h3><span class="qb-meta">Advanced · Occasional · Aggregation</span></summary>

**Short answer**

Exact distinct counts on huge, high-cardinality data are memory- and shuffle-heavy. HyperLogLog (HLL) estimates cardinality in a small fixed-size sketch with a typical error of around 1–2%. The sketches are mergeable, so you can store daily HLL states and combine them into weekly or monthly unique counts without rescanning raw data.

**Detailed explanation**

**Why approximate?**
- `COUNT(DISTINCT user_id)` over billions of rows must track every distinct value, which means a large shuffle and memory use.
- Many use cases (dashboards, monitoring, trend analysis) tolerate around 1% error.

**HyperLogLog intuition.** Hash each value and look at the number of leading zeros in the hash. Rare long runs of zeros indicate many distinct values. Split the hashes into many registers and average across them to reduce variance. Memory is a few KB regardless of cardinality.

**Mergeable sketches**
- Store a sketch per day (or per segment). The union of sketches estimates the distinct count across days, which solves the "distinct counts are non-additive" problem (see [Q7](/interview/data-engineering/#q7), [Q125](/interview/sql/#q125)).
- Snowflake: `HLL_ACCUMULATE` (creates a state), `HLL_COMBINE` (merges states), `HLL_ESTIMATE` (reads an estimate), and `APPROX_COUNT_DISTINCT` / `HLL` for one-shot use.
- Spark/Databricks: `approx_count_distinct`, and HLL sketch functions such as `hll_sketch_agg`, `hll_union_agg` and `hll_sketch_estimate` (Spark 3.5+).
- BigQuery: `APPROX_COUNT_DISTINCT` and `HLL_COUNT.INIT` / `MERGE` / `EXTRACT`.

**Other sketches**
- **Approximate percentiles:** t-digest and KLL (see [Q129](/interview/sql/#q129)).
- **Top-K / frequent items:** Space-Saving, Snowflake `APPROX_TOP_K`.
- **Membership tests:** bloom filters.
- **Similarity:** MinHash (`MINHASH` in Snowflake).

**When not to use:** billing, finance, compliance, or anything that must reconcile exactly.

**Example**

```sql
-- Dialect: Snowflake SQL
-- 1) Store daily HLL states
CREATE OR REPLACE TABLE daily_user_hll AS
SELECT CAST(event_ts AS DATE) AS event_date,
       HLL_ACCUMULATE(user_id) AS users_hll
FROM events
GROUP BY 1;

-- 2) Monthly unique users from daily states, no raw rescan
SELECT DATE_TRUNC('month', event_date) AS month_start,
       HLL_ESTIMATE(HLL_COMBINE(users_hll)) AS approx_monthly_users
FROM daily_user_hll
GROUP BY 1
ORDER BY 1;

-- One-shot alternative on raw data
SELECT APPROX_COUNT_DISTINCT(user_id) AS approx_users FROM events;
```

**Real-world use case**

A media company needed unique viewers per show for any date range in its dashboard. Storing daily HLL sketches per show let the dashboard compute arbitrary-range uniques in milliseconds, instead of distinct-counting tens of billions of view events per request.

**Common mistakes and red flags**

- Using approximate counts in financial reporting.
- Summing daily approximate counts instead of merging sketches.
- Not stating the error tolerance to stakeholders.

**Follow-up questions**

- What is the typical HLL error? — Around 1.6% relative standard error for common configurations; it depends on the precision parameter.
- Are sketches comparable across engines? — Generally no; formats differ, so keep sketches within one engine (or use a portable library such as Apache DataSketches).

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>
