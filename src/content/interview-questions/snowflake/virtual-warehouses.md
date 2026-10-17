---
publishedDate: "2026-10-04"
updatedDate: "2026-10-17"
reviewedDate: "2026-10-17"
title: "What are Snowflake virtual warehouses?"
seoTitle: "Snowflake Virtual Warehouses: Interview Answer"
description: "Interview answer: Snowflake virtual warehouses are independent compute over shared storage, billed per use, scaled up for heavy queries, out for concurrency."
inventoryId: "INT-20"
technology: ["snowflake"]
topic: ["compute", "cost"]
difficulty: "Easy"
questionType: ["conceptual"]
estimatedMinutes: 6
interviewRelevance: "High"
shortAnswer: "A virtual warehouse is an independent compute cluster that runs queries against Snowflake's shared storage. You choose its size, and it is billed only while running, with auto-suspend and auto-resume to stop paying when idle. Because warehouses are independent, different workloads can use separate warehouses without slowing each other down; you scale up a warehouse for heavy queries and scale out with multi-cluster warehouses for many concurrent users."
followUps: ["When would you scale up versus scale out?", "How would you reduce Snowflake compute cost?", "What happens to the warehouse cache when it suspends?", "Which statements run without a warehouse?", "What changes with Gen2 or adaptive warehouses?"]
related: ["articles:snowflake/architecture-virtual-warehouses", "articles:snowflake/virtual-warehouses-scaling", "interview-questions:snowflake/warehouse-sizing-vs-multi-cluster", "interview-questions:snowflake/caching-layers"]
versionContext: "Describes Snowflake behaviour as documented in October 2026. The Snowflake SQL is not executed here (noexec); the billing calculation runs on plain Python 3 and uses the documented Gen1 rates and the 60-second minimum."
sources:
  - { label: "Snowflake documentation: Overview of warehouses", url: "https://docs.snowflake.com/en/user-guide/warehouses-overview" }
  - { label: "Snowflake documentation: Gen2 standard warehouses", url: "https://docs.snowflake.com/en/user-guide/warehouses-gen2" }
  - { label: "Snowflake documentation: Adaptive Compute", url: "https://docs.snowflake.com/en/user-guide/warehouses-adaptive" }
---

## Detailed explanation

A virtual warehouse is Snowflake's unit of **compute**. Storage (micro-partitions) and cloud services (metadata, optimisation, security) are separate layers, so a warehouse holds no data of its own: it fetches the micro-partitions a query needs, caches them on local SSD while it runs, and returns results.

- **Independent**: each warehouse has its own nodes; a heavy transformation on one cannot slow dashboards on another, even on the same tables. (Locks are the exception: two `MERGE`s on one table wait for each other whatever warehouses run them.)
- **Shared data**: every warehouse reads the same storage, so adding one for a new team copies nothing.
- **Sized in T-shirt steps**: X-Small upwards, each step doubling compute and the credits per hour (Gen1 standard: 1, 2, 4, 8 ...). New standard warehouses default to **Gen2** where available, which also doubles per step but at a higher hourly rate, and turns on the query acceleration service.
- **Billed while running**: per second, with a **60-second minimum each time it resumes**. `AUTO_SUSPEND` (600 seconds by default when created with SQL) stops it when idle, `AUTO_RESUME` starts it on the next query.
- **Not every statement needs one**: DDL, `SHOW`, metadata-only answers such as an unfiltered `COUNT(*)`, and result-cache hits run in cloud services.
- **Two ways to scale**: **up** (a bigger size) for heavy single queries that spill or run long; **out** (a multi-cluster warehouse, Enterprise Edition) for many concurrent queries that queue.
- **Other kinds**: Snowpark-optimized warehouses for memory-heavy Python, and **adaptive warehouses** (Adaptive Compute, Enterprise and above in selected regions), where Snowflake chooses size and cluster count itself and bills per query.

## Example

One warehouse per workload, each sized and suspended for its pattern:

<!-- noexec -->
```sql
-- Snowflake SQL (not executed here)
CREATE WAREHOUSE load_wh      WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 60  AUTO_RESUME = TRUE INITIALLY_SUSPENDED = TRUE;
CREATE WAREHOUSE transform_wh WAREHOUSE_SIZE = 'MEDIUM' AUTO_SUSPEND = 60  AUTO_RESUME = TRUE INITIALLY_SUSPENDED = TRUE;
CREATE WAREHOUSE bi_wh        WAREHOUSE_SIZE = 'SMALL'  AUTO_SUSPEND = 300 AUTO_RESUME = TRUE INITIALLY_SUSPENDED = TRUE
  MIN_CLUSTER_COUNT = 1 MAX_CLUSTER_COUNT = 3 SCALING_POLICY = 'STANDARD';   -- Enterprise Edition

GRANT USAGE ON WAREHOUSE bi_wh TO ROLE reporting;      -- control who can spend on which warehouse
SHOW WAREHOUSES;                                       -- size, state, auto_suspend, generation
```

What the auto-suspend setting costs for a bursty workload: a Small Gen1 warehouse (2 credits per hour) receives three short bursts of work in an hour.

```python
RATE_PER_HOUR = 2                      # Small, Gen1 standard warehouse
bursts = [(0, 40), (300, 20), (1500, 30)]   # (start second, seconds of work)

def billed_seconds(bursts, auto_suspend):
    """Each resume bills at least 60 s; the warehouse stays up auto_suspend seconds after the last query."""
    total, running_until = 0, None
    for start, work in bursts:
        end = start + work
        if running_until is not None and start <= running_until:   # still running: extend
            total += end + auto_suspend - running_until
        else:                                                      # resumed: new 60 s minimum
            total += max(60, work + auto_suspend)
        running_until = end + auto_suspend
    return total

for suspend in (60, 300, 600):
    secs = billed_seconds(bursts, suspend)
    print(f"AUTO_SUSPEND={suspend:3}: billed {secs:4} s = {RATE_PER_HOUR * secs / 3600:.3f} credits")
```

```text
AUTO_SUSPEND= 60: billed  270 s = 0.150 credits
AUTO_SUSPEND=300: billed  950 s = 0.528 credits
AUTO_SUSPEND=600: billed 1550 s = 0.861 credits
```

The same 90 seconds of work costs almost six times more with the 600-second default than with 60 seconds, because the warehouse sits idle after each burst. The longer setting still makes sense for a BI warehouse whose users query every minute or two, where a warm cache and no resume delay matter more.

## Trade-offs and pitfalls

- **Shorter auto-suspend** saves idle credits but empties the local cache on every suspend and adds resume latency; below 60 seconds it saves nothing because each resume bills a minute anyway.
- **More warehouses** isolate workloads and make cost attributable, but too many tiny ones each pay their own idle time; isolate by workload pattern, not per person.
- **A bigger size** only pays off if runtime roughly halves; otherwise fix pruning first (see [sizing vs multi-cluster](/interview/snowflake/warehouse-sizing-vs-multi-cluster/)).
- **Resource monitors** can cap warehouse credits but not serverless features; budgets cover those.

## Common mistakes

1. Using one warehouse for everything, so loads, dbt runs and dashboards queue behind each other.
2. Scaling up to fix queueing (scale out instead) or scaling out to fix one slow query (it still runs on one cluster).
3. Leaving the 600-second default on batch warehouses.
4. Forgetting that suspending clears the local cache, so the first queries after resume may be slower.
5. Comparing generations by credits per hour instead of credits per job.
