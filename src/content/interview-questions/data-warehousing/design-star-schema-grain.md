---
publishedDate: "2026-10-16"
updatedDate: "2026-10-16"
reviewedDate: "2026-10-16"
title: "Design a star schema for a business process. What is the grain?"
seoTitle: "Design a Star Schema and Declare the Grain"
description: "Star schema design interview answer: pick the business process, declare the grain, choose dimensions and facts, and stop order-level measures double counting."
technology: ["data-warehousing", "data-engineering"]
topic: ["star-schema", "grain", "dimensional-modeling", "fact-tables"]
difficulty: "Medium"
questionType: ["architecture", "scenario"]
estimatedMinutes: 12
interviewRelevance: "High"
shortAnswer: "Follow the four steps in order. Name one business process (food orders being placed), declare the grain in a sentence (one row per item on an order), then pick the dimensions that are true at that grain (date, time, customer, restaurant, menu item, promotion, with the order number as a degenerate dimension), and finally the numeric facts measured at that grain (quantity, gross and discount amounts). Choose the lowest grain the source gives you, because you can always roll up but never drill below it. Measures that belong to a coarser grain, such as a delivery fee per order, either get allocated down to the lines with a rule or go in their own order-grain fact table; copying them onto every line double counts them."
followUps: ["Why not choose one row per order as the grain?", "Where does the delivery fee go?", "What changes if the business also wants to analyse delivery times?", "How do you test that the fact table really has the grain you declared?"]
related: ["articles:data-warehousing/star-schema", "articles:data-warehousing/fact-tables", "articles:data-warehousing/dimension-tables", "interview-questions:data-warehousing/fact-table-types"]
versionContext: "SQL verified on PostgreSQL 16 with scripts/verify-examples.py; outputs copied from psql."
sources:
  - { label: "Kimball Group: Four-step dimensional design process", url: "https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/four-4-step-design-process/" }
  - { label: "Kimball Group: Grain", url: "https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/grain/" }
  - { label: "Kimball Group: Star schemas and OLAP cubes", url: "https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/star-schema-olap-cube/" }
---

## Detailed explanation

Interviewers ask this to see whether you design from the business process outwards or start drawing tables. Use the scenario they give you; this answer uses a food-delivery company that wants revenue by restaurant, cuisine, city, hour of day and promotion.

### Step 1: pick one business process

A business process is an activity the company performs that a source system records: placing orders, dispatching couriers, processing refunds. Each process gets its own fact table. "Orders" is the process here; courier deliveries are a second process with their own grain and their own fact table later.

### Step 2: declare the grain

The grain is a sentence saying exactly what one fact row represents: **one row per menu item on a customer order**. Say it out loud before naming any column, because every later decision is checked against it. A dimension belongs only if it has a single value for that row; a measure belongs only if it is measured at that row.

Choose the **lowest (atomic) grain** the source offers. One row per order would answer "revenue by restaurant", but not "which dishes sell with which promotions". Rolling up is a `GROUP BY`; drilling below the grain is impossible.

### Step 3: choose the dimensions

| Dimension | Why it is true at the grain |
|---|---|
| `dim_date`, plus an hour-of-day attribute or a small `dim_time_of_day` | the order was placed at one moment |
| `dim_customer` | one customer per order |
| `dim_restaurant` (city, cuisine, rating band) | one restaurant per order |
| `dim_menu_item` (dish, category, veg flag) | one item per line |
| `dim_promotion` (with a "no promotion" row) | at most one promotion per line |
| `order_number` in the fact table | a **degenerate dimension**: an identifier with no other attributes, kept for grouping lines into orders |

### Step 4: choose the facts

Numbers measured at the line: `quantity`, `gross_amount`, `discount_amount`, and a derived `net_amount`. All are fully additive across every dimension.

The **delivery fee** is charged per order, not per item, so it does not fit this grain. Either allocate it to lines with a documented rule (proportional to gross amount, rounding the remainder onto one line) or keep it in a separate fact at order grain. Copying it onto every line is the classic mistake.

## Example

```sql
CREATE TABLE dim_restaurant (restaurant_key int PRIMARY KEY, restaurant_id text NOT NULL,
                             name text, city text, cuisine text);
CREATE TABLE dim_menu_item  (menu_item_key int PRIMARY KEY, item_id text NOT NULL,
                             dish text, category text);
CREATE TABLE dim_promotion  (promotion_key int PRIMARY KEY, promo_code text, promo_type text);
CREATE TABLE dim_date       (date_key int PRIMARY KEY, full_date date NOT NULL, day_name text);

-- Grain: one row per menu item on a customer order
CREATE TABLE fact_order_line (
  order_number     text    NOT NULL,              -- degenerate dimension
  line_number      int     NOT NULL,
  date_key         int     NOT NULL REFERENCES dim_date,
  hour_of_day      int     NOT NULL,
  restaurant_key   int     NOT NULL REFERENCES dim_restaurant,
  menu_item_key    int     NOT NULL REFERENCES dim_menu_item,
  promotion_key    int     NOT NULL REFERENCES dim_promotion,
  quantity         int     NOT NULL,
  gross_amount     numeric(10,2) NOT NULL,
  discount_amount  numeric(10,2) NOT NULL,
  PRIMARY KEY (order_number, line_number)          -- enforces the declared grain
);

INSERT INTO dim_restaurant VALUES (1, 'R-10', 'Tandoor Tales', 'Pune', 'North Indian'),
                                  (2, 'R-22', 'Sushi Sora',    'Pune', 'Japanese');
INSERT INTO dim_menu_item  VALUES (1, 'M-1', 'Paneer tikka', 'Starter'), (2, 'M-2', 'Dal makhani', 'Main'),
                                  (3, 'M-3', 'Salmon roll',  'Main');
INSERT INTO dim_promotion  VALUES (0, NULL, 'No promotion'), (1, 'LUNCH20', 'Percent off');
INSERT INTO dim_date       VALUES (20261012, '2026-10-12', 'Monday');

INSERT INTO fact_order_line VALUES
  ('O-1', 1, 20261012, 13, 1, 1, 1, 2, 500.00, 100.00),
  ('O-1', 2, 20261012, 13, 1, 2, 1, 1, 300.00,  60.00),
  ('O-2', 1, 20261012, 20, 2, 3, 0, 3, 900.00,   0.00);

SELECT r.cuisine, f.hour_of_day, p.promo_type,
       sum(f.quantity) AS items, sum(f.gross_amount - f.discount_amount) AS net_revenue
FROM fact_order_line f
JOIN dim_restaurant r USING (restaurant_key)
JOIN dim_promotion  p USING (promotion_key)
GROUP BY r.cuisine, f.hour_of_day, p.promo_type
ORDER BY r.cuisine;
```

```text
   cuisine    | hour_of_day |  promo_type  | items | net_revenue
--------------+-------------+--------------+-------+-------------
 Japanese     |          20 | No promotion |     3 |      900.00
 North Indian |          13 | Percent off  |     3 |      640.00
```

Now the delivery fee. Order O-1 paid a 40.00 fee. Stored on both of its lines, it is summed twice:

```sql
ALTER TABLE fact_order_line ADD COLUMN delivery_fee numeric(10,2);
UPDATE fact_order_line SET delivery_fee = CASE order_number WHEN 'O-1' THEN 40.00 ELSE 30.00 END;

SELECT sum(delivery_fee) AS fees_wrong FROM fact_order_line;   -- 110.00, the real total is 70.00

-- Fix: allocate the fee to lines in proportion to gross amount, so the lines add up to the order.
-- Window functions are not allowed directly in UPDATE ... SET, so compute the shares in a CTE.
WITH fees(order_number, fee) AS (VALUES ('O-1', 40.00), ('O-2', 30.00)),
alloc AS (
  SELECT l.order_number, l.line_number,
         round(fees.fee * l.gross_amount / sum(l.gross_amount) OVER (PARTITION BY l.order_number), 2) AS share
  FROM fact_order_line l JOIN fees USING (order_number)
)
UPDATE fact_order_line f SET delivery_fee = a.share
FROM alloc a WHERE a.order_number = f.order_number AND a.line_number = f.line_number;

SELECT order_number, line_number, delivery_fee FROM fact_order_line ORDER BY 1, 2;
SELECT sum(delivery_fee) AS fees_right FROM fact_order_line;
```

```text
 order_number | line_number | delivery_fee
--------------+-------------+--------------
 O-1          |           1 |        25.00
 O-1          |           2 |        15.00
 O-2          |           1 |        30.00

 fees_right
------------
      70.00
```

The allocation here divides evenly. With awkward amounts, proportional rounding can leave a cent over or under, so production code puts the remainder on the largest line and a test checks that line totals equal the order total.

## Trade-offs and pitfalls

- **Mixed grain**: adding order-level or day-level numbers to a line-level table breaks every `SUM`. Keep one grain per fact table.
- **Starting from the report**: a fact table shaped like today's dashboard (one row per restaurant per day) cannot answer tomorrow's question. Build atomic facts, then aggregate.
- **Nullable foreign keys**: use a "no promotion" or "unknown" dimension row instead of `NULL`, so inner joins do not drop facts.
- **Snowflaking by reflex**: putting city and cuisine in separate normalised tables saves little and adds joins; keep them as restaurant attributes unless they have rich attributes of their own.
- **Processes merged into one fact**: orders and deliveries happen at different times with different grains. Two fact tables sharing conformed dimensions is the right shape.

## Follow-up answers

- **One row per order**: simpler and smaller, but it loses dish-level and promotion-per-item analysis, which cannot be recovered later. Build the order grain as an aggregate if people need it.
- **Delivery fee**: allocate with a documented rule, or keep an order-grain fact (`fact_order`) with the fee, tip and delivery time. Many teams keep both tables.
- **Delivery times**: a separate fact for the delivery process, probably an accumulating snapshot with one row per order and a timestamp per milestone (accepted, picked up, delivered).
- **Testing the grain**: a primary key or uniqueness test on the grain columns, row-count reconciliation against the source line count, and a check that every foreign key resolves to a dimension row.
