---
publishedDate: "2026-10-16"
updatedDate: "2026-10-16"
reviewedDate: "2026-10-16"
title: "Transaction, periodic snapshot or accumulating snapshot: which fact table would you build?"
seoTitle: "Fact Table Types: Transaction vs Snapshot"
description: "Fact table types interview answer: when to build transaction, periodic snapshot and accumulating snapshot facts, with SQL that builds all three from one event log."
technology: ["data-warehousing", "data-engineering"]
topic: ["fact-tables", "snapshots", "dimensional-modeling", "semi-additive"]
difficulty: "Medium"
questionType: ["conceptual", "scenario"]
estimatedMinutes: 12
interviewRelevance: "High"
shortAnswer: "Pick the type from the question being asked. A transaction fact has one row per event at the moment it happened; it is insert-only, sparse and fully additive, and answers 'how many, how much'. A periodic snapshot has one row per entity per period (account per day, product per store per week) whether or not anything happened; it answers 'what was the level at the end of each period' and its balances are semi-additive. An accumulating snapshot has one row per instance of a process with a known set of milestones (order, claim, application); the row is updated as each milestone is reached and answers 'how long between steps and where are things stuck'. Most mature processes end up with two or three of them built from the same events."
followUps: ["Why can you not sum a balance in a periodic snapshot over days?", "How do you load an accumulating snapshot incrementally?", "What if the process can go backwards, for example a claim is reopened?", "Where does a factless fact table fit?"]
related: ["articles:data-warehousing/fact-tables", "articles:data-warehousing/star-schema", "interview-questions:data-warehousing/design-star-schema-grain"]
versionContext: "SQL verified on PostgreSQL 16 with scripts/verify-examples.py; outputs copied from psql."
sources:
  - { label: "Kimball Group: Transaction fact tables", url: "https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/transaction-fact-table/" }
  - { label: "Kimball Group: Periodic snapshot fact tables", url: "https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/periodic-snapshot-fact-table/" }
  - { label: "Kimball Group: Accumulating snapshot fact tables", url: "https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/accumulating-snapshot-fact-table/" }
  - { label: "Kimball Group: Additive, semi-additive and non-additive facts", url: "https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/additive-semi-additive-non-additive-fact/" }
---

## Detailed explanation

| | Transaction | Periodic snapshot | Accumulating snapshot |
|---|---|---|---|
| Grain | one event | one entity per period | one process instance |
| Rows appear | when something happens | every period, even if nothing happened | when the instance starts |
| Rows change | never (insert-only) | never once the period is closed | updated at every milestone |
| Date keys | one (event date) | one (snapshot date) | several (one per milestone) |
| Typical measures | amounts, counts (additive) | balances, levels (semi-additive) | durations between milestones, counts of instances |
| Answers | "how much was paid this month?" | "what was outstanding on 30 September?" | "how long from filing to payment, and where are claims stuck?" |

Use an insurance claims process as the example. The source emits events: a claim is **filed**, **assessed**, **approved** and **paid**, with reserve amounts changing along the way.

- The **transaction fact** keeps every event row. It is the most flexible: any other table can be derived from it. But "how much reserve was outstanding at month end" needs the whole history replayed up to that date.
- The **periodic snapshot** stores the answer to that question for every claim (or adjuster) every day. It is dense: a claim open for 60 days has 60 rows. Reserves are **semi-additive**: you can add them across claims on one day, never across days. Over time, use the closing value or an average.
- The **accumulating snapshot** has one row per claim with `filed_date_key`, `assessed_date_key`, `approved_date_key`, `paid_date_key` and lag measures. Unreached milestones point at an "unknown / not yet" date row. Loading means updating existing rows, so it is the only fact type that is not insert-only.

## Example

```sql
CREATE TABLE claim_events (claim_id text, event_type text, event_date date, reserve_change numeric(10,2));
INSERT INTO claim_events VALUES
  ('C-1', 'filed',    '2026-09-01',  1000), ('C-1', 'assessed', '2026-09-03',  500),
  ('C-1', 'approved', '2026-09-04',     0), ('C-1', 'paid',     '2026-09-08', -1500),
  ('C-2', 'filed',    '2026-09-02',  2000), ('C-2', 'assessed', '2026-09-07', -400);

-- 1. Transaction fact: the events themselves, one row each
CREATE TABLE fact_claim_event AS
SELECT claim_id, event_type, event_date, reserve_change FROM claim_events;

-- 2. Periodic snapshot: one row per open claim per day, with the reserve at end of day
CREATE TABLE fact_claim_daily AS
WITH days AS (SELECT d::date AS snapshot_date FROM generate_series('2026-09-01'::date, '2026-09-09', '1 day') d),
spans AS (SELECT claim_id, min(event_date) AS opened,
                 min(event_date) FILTER (WHERE event_type = 'paid') AS closed
          FROM claim_events GROUP BY claim_id)
SELECT d.snapshot_date, s.claim_id,
       (SELECT sum(reserve_change) FROM claim_events e
        WHERE e.claim_id = s.claim_id AND e.event_date <= d.snapshot_date) AS reserve_balance
FROM days d JOIN spans s ON d.snapshot_date >= s.opened AND (s.closed IS NULL OR d.snapshot_date < s.closed);

-- 3. Accumulating snapshot: one row per claim, one date per milestone, plus lags
CREATE TABLE fact_claim_pipeline AS
SELECT claim_id,
       min(event_date) FILTER (WHERE event_type = 'filed')    AS filed_date,
       min(event_date) FILTER (WHERE event_type = 'assessed') AS assessed_date,
       min(event_date) FILTER (WHERE event_type = 'approved') AS approved_date,
       min(event_date) FILTER (WHERE event_type = 'paid')     AS paid_date
FROM claim_events GROUP BY claim_id;

SELECT snapshot_date, count(*) AS open_claims, sum(reserve_balance) AS total_reserve
FROM fact_claim_daily WHERE snapshot_date IN ('2026-09-05', '2026-09-08')
GROUP BY snapshot_date ORDER BY snapshot_date;

SELECT claim_id, assessed_date - filed_date AS days_to_assess, paid_date - filed_date AS days_to_pay
FROM fact_claim_pipeline ORDER BY claim_id;
```

```text
 snapshot_date | open_claims | total_reserve
---------------+-------------+---------------
 2026-09-05    |           2 |       3500.00
 2026-09-08    |           1 |       1600.00

 claim_id | days_to_assess | days_to_pay
----------+----------------+-------------
 C-1      |              2 |           7
 C-2      |              5 |
```

The snapshot answers "what was outstanding on 5 September" with a simple filter. Summing `reserve_balance` across all days instead would add the same money once per day, which is the semi-additive trap. The pipeline table shows C-2 is still waiting for approval, and its `days_to_pay` is empty because that milestone has not happened.

The accumulating snapshot is loaded incrementally with a merge on `claim_id`: new claims are inserted, and each new event fills in its milestone column:

```sql
INSERT INTO claim_events VALUES ('C-2', 'approved', '2026-09-10', 0);

MERGE INTO fact_claim_pipeline p
USING (SELECT claim_id, min(event_date) AS approved_date
       FROM claim_events WHERE event_type = 'approved' AND event_date = '2026-09-10'
       GROUP BY claim_id) n
ON p.claim_id = n.claim_id
WHEN MATCHED THEN UPDATE SET approved_date = n.approved_date
WHEN NOT MATCHED THEN INSERT (claim_id, approved_date) VALUES (n.claim_id, n.approved_date);

SELECT claim_id, filed_date, approved_date, paid_date FROM fact_claim_pipeline WHERE claim_id = 'C-2';
```

```text
 claim_id | filed_date | approved_date | paid_date
----------+------------+---------------+-----------
 C-2      | 2026-09-02 | 2026-09-10    |
```

## Trade-offs and pitfalls

- **Periodic snapshots are big**: rows = entities x periods. Snapshot only what is open or active, pick the coarsest period the business needs (daily, not hourly), and partition by snapshot date.
- **Semi-additive measures** need the right aggregation over time: closing balance, average balance, or `max(snapshot_date)` per period. Put a note on the measure in the semantic layer.
- **Accumulating snapshots assume a fixed, mostly forward process.** Loops (reopened claims, returned orders) need extra columns (reopen count, latest reopen date) or a separate transaction fact to keep the detail.
- **Updates are expensive on columnar warehouses** and lake tables; keep the accumulating snapshot small, or rebuild it from the transaction fact for recently changed instances only.
- **Late events** must be applied in event order: an approval that arrives after payment should still fill `approved_date`, not overwrite a later milestone.

## Follow-up answers

- **Summing balances over days**: a balance is a level, not a flow. Adding 30 daily balances gives 30 times the money. Sum across claims for one day; across time take the closing or average value.
- **Incremental accumulating snapshot**: find instances with new events since the last run, recompute their milestone columns from the transaction fact (or merge the new milestones), and update those rows only.
- **Process goes backwards**: either reset milestone columns and count the loops, or keep "first" and "latest" dates per milestone; the transaction fact keeps the full truth either way.
- **Factless facts**: a transaction fact with no measures, only keys (a student attended a class, a promotion was active for a product). They record that something happened, or could have, and are counted rather than summed.
