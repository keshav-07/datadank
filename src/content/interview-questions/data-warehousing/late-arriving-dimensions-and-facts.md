---
publishedDate: "2026-10-16"
updatedDate: "2026-10-16"
reviewedDate: "2026-10-16"
title: "How do you model late-arriving dimensions and late-arriving facts in a star schema?"
seoTitle: "Late-Arriving Dimensions and Facts"
description: "Late-arriving dimensions interview answer: inferred members for facts that beat their dimension, as-of key lookup for late facts, backdated Type 2 changes."
technology: ["data-warehousing", "data-engineering"]
topic: ["late-arriving-dimensions", "late-arriving-facts", "inferred-members", "scd-type-2"]
difficulty: "Hard"
questionType: ["scenario", "coding"]
estimatedMinutes: 14
interviewRelevance: "High"
shortAnswer: "Three cases. A fact that arrives before its dimension row gets an inferred member: a placeholder row with the natural key, a new surrogate, default attributes and an inferred flag; when the real row arrives it overwrites the placeholder in place (no new Type 2 version), so the fact needs no update. A fact that arrives late must look up the dimension version valid at its event time, not the current one, and its event-date aggregates must be rebuilt. A backdated dimension change splits an existing Type 2 range, and facts in the split period are re-pointed to the new version."
followUps: ["Why not just reject facts with an unknown key and retry later?", "Why is the inferred row overwritten instead of versioned?", "How do you find facts that need re-pointing after a backdated change?", "How does this differ in a lakehouse with hash keys?"]
related: ["articles:data-warehousing/dimension-tables", "articles:data-warehousing/fact-tables", "interview-questions:data-engineering/late-arriving-data", "interview-questions:data-warehousing/scd-type-2-as-of-joins"]
versionContext: "SQL verified on PostgreSQL 16 with scripts/verify-examples.py; outputs copied from psql."
sources:
  - { label: "Kimball Group: Late arriving dimensions", url: "https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/late-arriving-dimension/" }
  - { label: "Kimball Group: Late arriving facts", url: "https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/late-arriving-fact/" }
  - { label: "PostgreSQL 16 documentation: INSERT (ON CONFLICT)", url: "https://www.postgresql.org/docs/16/sql-insert.html" }
---

## Detailed explanation

This question is about the dimensional model. The pipeline side (selecting by ingestion time, rebuilding event-date partitions, lateness policies) is covered in [How do you handle late-arriving data?](/interview/data-engineering/late-arriving-data/); a good answer mentions both.

Scenario: a ride-hailing company loads `fact_trip` every hour from the trip service, and `dim_driver` once a day from an onboarding system. New drivers often complete trips before the onboarding extract includes them.

### Case 1: the fact arrives before its dimension (inferred members)

Options for a trip whose `driver_id` is not in `dim_driver` yet:

| Option | Problem |
|---|---|
| Drop the trip | revenue is understated, silently |
| Park it in a suspense table and retry | correct eventually, but today's numbers are wrong and the retry logic grows |
| Point it at a single "unknown driver" row (`-1`) | totals are right, but the link to the real driver is lost forever |
| **Insert an inferred member** | totals right now, and the link is kept |

An **inferred member** is a dimension row created by the fact load: real natural key, a new surrogate key, placeholder attributes ("Pending onboarding") and `is_inferred = true`. The fact gets that surrogate. When the real driver row arrives, the dimension load sees the inferred flag and **overwrites the row in place** (Type 1 style for this one update) and clears the flag. No new Type 2 version is created, because the placeholder values were never true; they were just unknown. The fact rows need no change because the surrogate key stayed the same.

### Case 2: the fact arrives late

A trip from 3 days ago turns up today, and the driver's vehicle category changed yesterday. Looking up the **current** row would attribute the old trip to the new category. The fact load must use an as-of lookup: the version whose `[valid_from, valid_to)` contains the trip's event time. Then rebuild any aggregates and snapshots for the trip's event date, not the load date.

### Case 3: the dimension change arrives late (backdated)

The onboarding system reports today that the driver switched to "Premium" effective 5 days ago. The current Type 2 row is split: close it at the effective date, insert the new version from that date, and **re-point facts** between the effective date and now to the new surrogate. This is the most expensive case, so agree with the business whether backdated changes are applied retroactively or only from the date they were received.

## Example

```sql
CREATE TABLE dim_driver (
  driver_key   int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  driver_id    text NOT NULL,
  driver_name  text NOT NULL,
  vehicle_tier text NOT NULL,
  valid_from   timestamp NOT NULL,
  valid_to     timestamp NOT NULL DEFAULT '9999-12-31',
  is_current   boolean NOT NULL DEFAULT true,
  is_inferred  boolean NOT NULL DEFAULT false
);
CREATE UNIQUE INDEX one_current_driver ON dim_driver (driver_id) WHERE is_current;

INSERT INTO dim_driver (driver_id, driver_name, vehicle_tier, valid_from) VALUES
  ('D-1', 'Kavya N', 'Standard', '2026-01-01');

CREATE TABLE fact_trip (trip_id text PRIMARY KEY, driver_key int NOT NULL REFERENCES dim_driver,
                        started_at timestamp NOT NULL, fare numeric(8,2) NOT NULL);
CREATE TABLE stg_trip (trip_id text, driver_id text, started_at timestamp, fare numeric(8,2));
INSERT INTO stg_trip VALUES ('T-1', 'D-1', '2026-10-10 08:00', 220), ('T-2', 'D-2', '2026-10-10 09:15', 180);

-- Fact load, step 1: create inferred members for drivers we have not seen
INSERT INTO dim_driver (driver_id, driver_name, vehicle_tier, valid_from, is_inferred)
SELECT DISTINCT s.driver_id, 'Pending onboarding', 'Unknown', '1900-01-01'::timestamp, true
FROM stg_trip s
WHERE NOT EXISTS (SELECT 1 FROM dim_driver d WHERE d.driver_id = s.driver_id);

-- Step 2: as-of lookup of the surrogate valid at the trip's start time
INSERT INTO fact_trip
SELECT s.trip_id, d.driver_key, s.started_at, s.fare
FROM stg_trip s
JOIN dim_driver d ON d.driver_id = s.driver_id
                 AND d.valid_from <= s.started_at AND s.started_at < d.valid_to;

SELECT f.trip_id, f.driver_key, d.driver_name, d.vehicle_tier, d.is_inferred
FROM fact_trip f JOIN dim_driver d USING (driver_key) ORDER BY f.trip_id;
```

```text
 trip_id | driver_key |    driver_name     | vehicle_tier | is_inferred
---------+------------+--------------------+--------------+-------------
 T-1     |          1 | Kavya N            | Standard     | f
 T-2     |          2 | Pending onboarding | Unknown      | t
```

The onboarding extract arrives with D-2. Because the row is inferred, it is completed in place:

```sql
CREATE TABLE stg_driver (driver_id text, driver_name text, vehicle_tier text, effective_from timestamp);
INSERT INTO stg_driver VALUES ('D-2', 'Imran S', 'Premium', '2026-10-09');

UPDATE dim_driver d
SET driver_name = s.driver_name, vehicle_tier = s.vehicle_tier, is_inferred = false
FROM stg_driver s
WHERE d.driver_id = s.driver_id AND d.is_inferred;

SELECT f.trip_id, f.driver_key, d.driver_name, d.vehicle_tier, d.is_inferred
FROM fact_trip f JOIN dim_driver d USING (driver_key) WHERE f.trip_id = 'T-2';
```

```text
 trip_id | driver_key | driver_name | vehicle_tier | is_inferred
---------+------------+-------------+--------------+-------------
 T-2     |          2 | Imran S     | Premium      | f
```

Now a backdated change: Kavya moved to Premium effective 9 October, but the system only reported it on 12 October, after trip T-1 (10 October) was loaded against the Standard version. Split the range and re-point the facts in the split period, in one transaction:

```sql
BEGIN;
UPDATE dim_driver SET valid_to = '2026-10-09', is_current = false
WHERE driver_id = 'D-1' AND is_current;

INSERT INTO dim_driver (driver_id, driver_name, vehicle_tier, valid_from)
VALUES ('D-1', 'Kavya N', 'Premium', '2026-10-09');

UPDATE fact_trip f
SET driver_key = new_v.driver_key
FROM dim_driver old_v, dim_driver new_v
WHERE f.driver_key = old_v.driver_key
  AND old_v.driver_id = 'D-1' AND new_v.driver_id = 'D-1' AND new_v.is_current
  AND f.started_at >= new_v.valid_from;
COMMIT;

SELECT f.trip_id, d.driver_key, d.vehicle_tier, d.valid_from, d.valid_to
FROM fact_trip f JOIN dim_driver d USING (driver_key) WHERE f.trip_id = 'T-1';
```

```text
 trip_id | driver_key | vehicle_tier |     valid_from      |      valid_to
---------+------------+--------------+---------------------+---------------------
 T-1     |          3 | Premium      | 2026-10-09 00:00:00 | 9999-12-31 00:00:00
```

## Trade-offs and pitfalls

- **Inferred members need a flag.** Without `is_inferred`, the dimension load cannot tell "placeholder to complete" from "real row that changed", and creates a bogus Type 2 version.
- **Placeholder attributes should be obviously placeholders** ("Pending onboarding", "Unknown"), so reports show the gap instead of a plausible wrong value. Monitor how many inferred rows are older than a day.
- **`valid_from` of an inferred row** should start early enough (or at the first fact's time) that the as-of lookup finds it.
- **Late facts on current rows**: using `is_current` in the fact lookup is the most common bug; it is only correct for facts that are on time.
- **Re-pointing facts is a rewrite** of historical partitions. In warehouses and lake tables, bound it (only the affected keys and dates) and rebuild downstream aggregates for those dates.
- **Hash keys change the picture**: if the fact computes the dimension key as a hash of the natural key, it never needs a lookup and an early fact is automatically linked; but a Type 2 version key still has to be resolved by time.

## Follow-up answers

- **Reject and retry**: acceptable for small, short delays, but the numbers are wrong until the retry succeeds and stuck rows need their own monitoring. Inferred members keep totals right immediately.
- **Overwrite, not version**: the placeholder values were never a real state of the driver, so recording them as history would make "as-was" reports show a tier the driver never had.
- **Finding facts to re-point**: facts whose key is the old version and whose event time is on or after the new version's `valid_from`, exactly the `UPDATE` above; for large tables, restrict by partition (event date) first.
- **Lakehouse**: the same patterns with `MERGE` on Delta or Iceberg tables; hash keys remove the lookup for Type 1 dimensions, and backdated Type 2 changes still mean rewriting the affected fact partitions.
