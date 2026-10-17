---
publishedDate: "2026-10-16"
updatedDate: "2026-10-16"
reviewedDate: "2026-10-16"
title: "What are conformed and role-playing dimensions, and how do you use them?"
seoTitle: "Conformed and Role-Playing Dimensions"
description: "Conformed and role-playing dimensions interview answer: share dimensions across facts, drill across without double counting, reuse one dimension in roles."
technology: ["data-warehousing", "data-engineering"]
topic: ["conformed-dimensions", "role-playing-dimensions", "bus-matrix", "drill-across"]
difficulty: "Medium"
questionType: ["conceptual", "coding"]
estimatedMinutes: 12
interviewRelevance: "High"
shortAnswer: "A conformed dimension is one dimension (same keys, same attribute names and values) shared by several fact tables, or a shrunken subset of it such as month for a monthly fact. It is what lets you compare business processes: bookings and complaints can be put side by side by route and month only because both use the same airport and date dimensions. You drill across by aggregating each fact separately to the shared attributes and then joining the results, never by joining two fact tables row to row. A role-playing dimension is one physical dimension referenced several times by the same fact in different roles, such as departure and arrival airport or order date and ship date; each role gets its own foreign key and, for readability, its own view or alias with role-specific column names."
followUps: ["What is the enterprise bus matrix?", "Why not join the two fact tables directly?", "How do you conform a dimension that two teams already built differently?", "When would a role need its own physical table?"]
related: ["articles:data-warehousing/dimension-tables", "articles:data-warehousing/star-schema", "interview-questions:data-warehousing/design-star-schema-grain"]
versionContext: "SQL verified on PostgreSQL 16 with scripts/verify-examples.py; outputs copied from psql."
sources:
  - { label: "Kimball Group: Conformed dimensions", url: "https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/conformed-dimension/" }
  - { label: "Kimball Group: Role-playing dimensions", url: "https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/role-playing-dimension/" }
  - { label: "Kimball Group: Enterprise data warehouse bus architecture", url: "https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/enterprise-data-warehouse-bus-architecture/" }
---

## Detailed explanation

### Conformed dimensions

Two dimensions are **conformed** when they are the same table, or when one is an exact subset of the other (a **shrunken** dimension, such as `dim_month` built from `dim_date`, or a `dim_brand` rolled up from `dim_product`), with identical keys, column names and attribute values. "Route PNQ-DEL" and "month 2026-09" must mean the same thing in every fact table.

Conformed dimensions are the integration point of a Kimball warehouse. Teams can build fact tables for different processes independently, and as long as they use the shared dimensions, the results line up. The plan for which processes use which dimensions is the **enterprise bus matrix**:

| Business process (fact) | Date | Customer | Airport | Fare class | Channel |
|---|---|---|---|---|---|
| Bookings | x | x | x (departure, arrival) | x | x |
| Flights flown | x | | x (departure, arrival) | | |
| Customer complaints | x | x | x (departure) | | x |
| Loyalty points | x | x | | | |

### Drilling across

To compare two processes, aggregate each fact **separately** to the shared grain, then join the aggregates on the conformed attributes (or `FULL JOIN` them so rows that exist on only one side survive). Joining fact rows directly multiplies them: every booking on a route matches every complaint on the same route, which is a fan trap.

### Role-playing dimensions

A fact often references the same dimension more than once: a flight booking has a departure airport and an arrival airport, a booking date and a travel date. There is still **one** airport dimension and **one** date dimension; the fact carries one foreign key per role (`departure_airport_key`, `arrival_airport_key`). Expose each role as a view or alias with role-specific column names (`departure_city`, `arrival_city`), so BI tools and analysts do not confuse them and self-joins become readable.

## Example

```sql
CREATE TABLE dim_airport (airport_key int PRIMARY KEY, iata text NOT NULL, city text NOT NULL);
CREATE TABLE dim_date    (date_key int PRIMARY KEY, full_date date NOT NULL, month text NOT NULL);
INSERT INTO dim_airport VALUES (1, 'PNQ', 'Pune'), (2, 'DEL', 'Delhi'), (3, 'BLR', 'Bengaluru');
INSERT INTO dim_date VALUES (20260905, '2026-09-05', '2026-09'), (20260920, '2026-09-20', '2026-09'),
                            (20261002, '2026-10-02', '2026-10');

-- Bookings: one row per booked seat; airport plays two roles, date plays two roles
CREATE TABLE fact_booking (
  booking_id int PRIMARY KEY,
  booking_date_key int REFERENCES dim_date, travel_date_key int REFERENCES dim_date,
  departure_airport_key int REFERENCES dim_airport, arrival_airport_key int REFERENCES dim_airport,
  fare numeric(8,2));
INSERT INTO fact_booking VALUES
  (1, 20260905, 20260920, 1, 2, 4500), (2, 20260905, 20260920, 1, 2, 5200),
  (3, 20260920, 20261002, 1, 2, 6100), (4, 20260920, 20261002, 3, 1, 3900);

-- Complaints: a different process with its own grain, sharing the same date and airport dimensions
CREATE TABLE fact_complaint (complaint_id int PRIMARY KEY, date_key int REFERENCES dim_date,
                             departure_airport_key int REFERENCES dim_airport);
INSERT INTO fact_complaint VALUES (10, 20260920, 1), (11, 20260920, 1), (12, 20261002, 3);

-- Role views give each role its own column names
CREATE VIEW dim_departure_airport AS SELECT airport_key AS departure_airport_key, iata AS departure_iata, city AS departure_city FROM dim_airport;
CREATE VIEW dim_arrival_airport   AS SELECT airport_key AS arrival_airport_key,   iata AS arrival_iata,   city AS arrival_city   FROM dim_airport;

SELECT b.booking_id, dep.departure_city, arr.arrival_city, bd.full_date AS booked_on, td.full_date AS travels_on
FROM fact_booking b
JOIN dim_departure_airport dep USING (departure_airport_key)
JOIN dim_arrival_airport   arr USING (arrival_airport_key)
JOIN dim_date bd ON bd.date_key = b.booking_date_key
JOIN dim_date td ON td.date_key = b.travel_date_key
ORDER BY b.booking_id;
```

```text
 booking_id | departure_city | arrival_city | booked_on  | travels_on
------------+----------------+--------------+------------+------------
          1 | Pune           | Delhi        | 2026-09-05 | 2026-09-20
          2 | Pune           | Delhi        | 2026-09-05 | 2026-09-20
          3 | Pune           | Delhi        | 2026-09-20 | 2026-10-02
          4 | Bengaluru      | Pune         | 2026-09-20 | 2026-10-02
```

Now drill across: travelled seats and complaints by departure city and month. The wrong way joins the facts directly:

```sql
SELECT a.city, d.month, count(DISTINCT b.booking_id) AS seats_if_distinct,
       count(b.booking_id) AS seats_fanned_out, count(c.complaint_id) AS complaints_fanned_out
FROM fact_booking b
JOIN dim_airport a ON a.airport_key = b.departure_airport_key
JOIN dim_date d    ON d.date_key = b.travel_date_key
JOIN fact_complaint c ON c.departure_airport_key = b.departure_airport_key AND c.date_key = b.travel_date_key
GROUP BY a.city, d.month ORDER BY a.city, d.month;
```

```text
   city    |  month  | seats_if_distinct | seats_fanned_out | complaints_fanned_out
-----------+---------+-------------------+------------------+-----------------------
 Bengaluru | 2026-10 |                 1 |                1 |                     1
 Pune      | 2026-09 |                 2 |                4 |                     4
```

Pune in September shows four seats and four complaints, where the truth is two of each, and Pune in October disappeared because it had no complaint. The right way aggregates each fact to the conformed attributes first:

```sql
WITH seats AS (
  SELECT a.city, d.month, count(*) AS seats
  FROM fact_booking b
  JOIN dim_airport a ON a.airport_key = b.departure_airport_key
  JOIN dim_date d    ON d.date_key = b.travel_date_key
  GROUP BY a.city, d.month),
complaints AS (
  SELECT a.city, d.month, count(*) AS complaints
  FROM fact_complaint c
  JOIN dim_airport a ON a.airport_key = c.departure_airport_key
  JOIN dim_date d    ON d.date_key = c.date_key
  GROUP BY a.city, d.month)
SELECT city, month, coalesce(seats, 0) AS seats, coalesce(complaints, 0) AS complaints,
       round(100.0 * coalesce(complaints, 0) / nullif(seats, 0), 1) AS complaints_per_100_seats
FROM seats FULL JOIN complaints USING (city, month)
ORDER BY city, month;
```

```text
   city    |  month  | seats | complaints | complaints_per_100_seats
-----------+---------+-------+------------+--------------------------
 Bengaluru | 2026-10 |     1 |          1 |                    100.0
 Pune      | 2026-09 |     2 |          2 |                    100.0
 Pune      | 2026-10 |     1 |          0 |                      0.0
```

This only works because both facts use the same `dim_airport` and `dim_date`. If complaints used a separately built airport table with "Poona" for Pune, the `FULL JOIN` would produce two half-empty rows instead of one comparable row.

## Trade-offs and pitfalls

- **Conforming is organisational work**: agreeing on one customer definition, one product hierarchy, one fiscal calendar. The SQL is easy; the agreement is not. Start with date, then the two or three dimensions most processes share.
- **Near-conformed dimensions** (same names, slightly different values or keys) are worse than obviously different ones, because drill-across results look plausible and are wrong.
- **Shrunken dimensions must be derived**, not rebuilt: generate `dim_month` from `dim_date` so the attributes cannot drift.
- **One view per role** is better than asking analysts to alias correctly every time; many BI tools also need separate logical tables per role.
- **Do not copy a dimension per role physically** unless the roles really need different attributes or different history; copies drift.

## Follow-up answers

- **Bus matrix**: a grid of business processes (rows) against dimensions (columns) that plans which conformed dimensions each fact table uses. It is the roadmap and the integration contract of a Kimball warehouse.
- **Joining facts directly**: two facts with a many-to-many relationship through shared dimensions multiply each other's rows (a fan or chasm trap), and an inner join drops rows that exist in only one process. Aggregate, then join.
- **Conforming existing dimensions**: build the master version, map each team's keys to it (a key map), migrate one fact table at a time, and add tests that compare attribute values across facts until the old version can be retired.
- **Separate physical table for a role**: when the role has its own attributes or history, for example a "ship-to address" with geocoding the billing address does not need. Otherwise keep one table and views.
