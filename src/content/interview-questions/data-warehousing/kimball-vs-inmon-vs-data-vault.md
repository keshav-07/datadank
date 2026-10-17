---
publishedDate: "2026-10-16"
updatedDate: "2026-10-16"
reviewedDate: "2026-10-16"
title: "Kimball vs Inmon vs Data Vault: which would you choose for a new warehouse?"
seoTitle: "Kimball vs Inmon vs Data Vault: Interview"
description: "Kimball vs Inmon vs Data Vault interview answer: how each layers the warehouse, what each costs, and how to choose, with a small Data Vault to star example in SQL."
technology: ["data-warehousing", "data-engineering"]
topic: ["kimball", "inmon", "data-vault", "modeling-methodologies"]
difficulty: "Medium"
questionType: ["architecture", "conceptual"]
estimatedMinutes: 12
interviewRelevance: "High"
shortAnswer: "They answer different questions, so I would say what each optimises and then choose by constraints. Kimball builds conformed star schemas per business process, bottom-up; it is fastest to value and easiest to query, but integration depends on discipline around conformed dimensions and restructuring is painful. Inmon builds a normalised, subject-oriented enterprise warehouse first and derives dimensional marts from it; it gives one integrated, consistent core at the cost of a long upfront modelling effort. Data Vault 2.0 models the integration layer as hubs (business keys), links (relationships) and satellites (attributes over time), loaded insert-only and in parallel; it absorbs new sources and changing rules well and keeps full history for audit, but it is not queryable by analysts and always needs marts on top. In practice many teams combine them: a raw or vault layer for integration and history, and Kimball stars (often in dbt) for consumption."
followUps: ["Why can analysts not query a Data Vault directly?", "Where does the medallion architecture fit?", "What would you pick for a 5-person startup with one source system?", "What does 'insert-only' buy you in Data Vault?"]
related: ["articles:data-warehousing/modeling-methodologies", "articles:data-warehousing/star-schema", "interview-questions:data-warehousing/one-big-table-vs-star-schema", "interview-questions:data-engineering/lake-warehouse-lakehouse"]
versionContext: "SQL verified on PostgreSQL 16 with scripts/verify-examples.py; outputs copied from psql. Data Vault hash keys use md5 for brevity; Data Vault 2.0 guidance commonly uses MD5 or SHA-1/SHA-256."
sources:
  - { label: "Kimball Group: Dimensional modeling techniques", url: "https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/" }
  - { label: "Kimball Group: Enterprise data warehouse bus architecture", url: "https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/enterprise-data-warehouse-bus-architecture/" }
  - { label: "Microsoft Tech Community: Data Vault 2.0 modelling on Microsoft Fabric", url: "https://techcommunity.microsoft.com/blog/analyticsonazure/data-vault-2-0-modelling-in-synapse-data-warehouse-on-microsoft-fabric/4003349" }
  - { label: "Databricks documentation: What is the medallion lakehouse architecture?", url: "https://docs.databricks.com/aws/en/lakehouse/medallion" }
---

## Detailed explanation

### What each one is

| | Kimball | Inmon | Data Vault 2.0 |
|---|---|---|---|
| Core layer | conformed star schemas (facts and dimensions) | normalised (about 3NF), subject-oriented enterprise data warehouse | hubs, links, satellites |
| Direction | bottom-up, one business process at a time, integrated by the bus matrix | top-down: enterprise model first, marts derived from it | integration layer first, marts (usually stars) derived from it |
| Who queries the core | analysts and BI tools directly | mostly ETL; users query the marts | only engineers; users query the marts |
| History | SCD types chosen per attribute | in the EDW, often with effective dates | every attribute change, insert-only, in satellites |
| Change tolerance | new sources and restructures can force fact and dimension rebuilds | changes to the enterprise model ripple through | new sources add hubs, links or satellites without changing existing tables |
| Main cost | discipline: conformed dimensions across teams | long upfront modelling before value | many tables and joins; a second modelling layer for consumption |

### How I would choose

- **Few sources, analytics-first team, need value in weeks**: Kimball stars, built in dbt from staged source data. Most cloud-warehouse teams land here.
- **Many sources with conflicting keys, frequent source change, regulatory audit ("show what we knew and when")**: Data Vault (or a similar insert-only raw history layer) for integration, Kimball marts for consumption.
- **Strong central data governance and a stable enterprise model**: Inmon-style integrated core, dimensional marts on top. Pure Inmon is less common in new builds, but its idea of one integrated layer survives in most architectures.

Then say the honest part: these are not exclusive. A typical modern stack has a raw landing layer (bronze), an integrated and cleaned layer (silver, sometimes a vault), and dimensional or wide consumption models (gold). The **medallion** architecture describes data refinement stages, not a modelling technique; you still choose how to model each layer.

### Data Vault in one paragraph

A **hub** holds one business key per entity (customer number) with a hash key, load timestamp and record source. A **link** holds a relationship between hubs (customer placed order). A **satellite** holds descriptive attributes for a hub or link over time, one row per change, keyed by the parent hash key and load timestamp; different sources get different satellites. Everything is insert-only, so loads are idempotent and can run in parallel, and nothing is ever updated or lost. The price is that a simple question ("current customers by city") needs several joins and a "latest row per key" step, so marts or views always sit on top.

## Example

The same customer arrives from two systems. The vault keeps both views with full history; a dimension view on top gives analysts one row per customer.

```sql
CREATE TABLE hub_customer (customer_hk char(32) PRIMARY KEY, customer_no text NOT NULL UNIQUE,
                           load_ts timestamp NOT NULL, record_source text NOT NULL);
CREATE TABLE sat_customer_crm (customer_hk char(32) REFERENCES hub_customer, load_ts timestamp,
                               full_name text, city text, PRIMARY KEY (customer_hk, load_ts));
CREATE TABLE sat_customer_billing (customer_hk char(32) REFERENCES hub_customer, load_ts timestamp,
                                   payment_terms text, PRIMARY KEY (customer_hk, load_ts));

-- Loads are insert-only and idempotent: rerunning them inserts nothing new
INSERT INTO hub_customer VALUES (md5('CUS-7'), 'CUS-7', '2026-09-01 02:00', 'crm')
ON CONFLICT DO NOTHING;
INSERT INTO sat_customer_crm VALUES
  (md5('CUS-7'), '2026-09-01 02:00', 'Lata Pillai', 'Kochi'),
  (md5('CUS-7'), '2026-10-01 02:00', 'Lata Pillai', 'Chennai')      -- she moved: a new row, nothing updated
ON CONFLICT DO NOTHING;
-- A new source later: just a new satellite, no change to existing tables
INSERT INTO sat_customer_billing VALUES (md5('CUS-7'), '2026-10-05 02:00', 'NET30')
ON CONFLICT DO NOTHING;

-- Consumption: a Type 1 customer dimension (latest values) built as a view
CREATE VIEW dim_customer AS
SELECT h.customer_no, crm.full_name, crm.city, bill.payment_terms
FROM hub_customer h
LEFT JOIN LATERAL (SELECT full_name, city FROM sat_customer_crm s
                   WHERE s.customer_hk = h.customer_hk ORDER BY load_ts DESC LIMIT 1) crm ON true
LEFT JOIN LATERAL (SELECT payment_terms FROM sat_customer_billing s
                   WHERE s.customer_hk = h.customer_hk ORDER BY load_ts DESC LIMIT 1) bill ON true;

SELECT * FROM dim_customer;

-- History is still there for audit: what did we believe about CUS-7 on 15 September?
SELECT city FROM sat_customer_crm
WHERE customer_hk = md5('CUS-7') AND load_ts <= '2026-09-15'
ORDER BY load_ts DESC LIMIT 1;
```

```text
 customer_no |  full_name  |  city   | payment_terms
-------------+-------------+---------+---------------
 CUS-7       | Lata Pillai | Chennai | NET30

 city
-------
 Kochi
```

A Kimball design would have put `full_name`, `city` and `payment_terms` in one `dim_customer` table loaded by a merge; adding the billing source means altering that table and its load. Neither is wrong: the vault buys change tolerance and auditability with more tables and an extra layer.

## Trade-offs and pitfalls

- **Treating the choice as a religion.** Interviewers want to hear constraints (team size, number of sources, audit needs, time to value) driving the decision.
- **Exposing a vault to analysts.** It is an integration model; without marts, every query reinvents the latest-row logic and gets it slightly differently.
- **Kimball without conformed dimensions** becomes a set of disconnected marts that disagree, which is the failure Inmon and vault proponents point to.
- **Big upfront enterprise models** (Inmon) can take so long that the business builds its own spreadsheets meanwhile; deliver marts incrementally even with an integrated core.
- **Vault sprawl**: one satellite per source per column group multiplies tables fast. Use automation (dbt packages, generators) and naming standards, or the layer becomes unmaintainable.

## Follow-up answers

- **Analysts and a vault**: the attributes of one entity are spread over several satellites with their own timelines, relationships live in separate link tables, and current values need a latest-row-per-key step. Marts flatten all that once, consistently.
- **Medallion**: a description of refinement stages (raw, cleaned and conformed, business-ready). Bronze is close to a raw vault or staging layer, silver is the integration layer, gold is where Kimball stars or wide tables live.
- **Five-person startup, one source**: Kimball-style stars in dbt directly on staged data, plus snapshots of source tables for history. A vault would be overhead with no integration problem to solve.
- **Insert-only**: loads are idempotent and parallel, nothing is lost, and any past state can be reconstructed for audits or to rebuild marts with new rules.
