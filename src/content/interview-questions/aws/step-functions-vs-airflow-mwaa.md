---
publishedDate: "2026-10-16"
updatedDate: "2026-10-16"
reviewedDate: "2026-10-16"
title: "Step Functions or Airflow on MWAA: how do you choose an orchestrator on AWS?"
seoTitle: "Step Functions vs Airflow (MWAA): Interview"
description: "Step Functions vs Airflow on Amazon MWAA: event-driven state machines vs scheduled DAGs, backfills, limits, billing models and when to combine the two."
technology: ["aws", "airflow"]
topic: ["step-functions", "mwaa", "orchestration", "comparison"]
difficulty: "Medium"
questionType: ["architecture", "scenario"]
estimatedMinutes: 10
interviewRelevance: "High"
shortAnswer: "Step Functions is a serverless state machine service: JSON (Amazon States Language) workflows triggered by events or schedules, with native .sync integrations for Glue, EMR, Athena, ECS and Lambda, built-in retries and catches, Distributed Map for fanning out over thousands of S3 objects, and billing per state transition (Standard) or per request and duration (Express). Airflow on MWAA is a Python DAG scheduler built around data intervals: schedules, catchup and backfills, cross-DAG dependencies, sensors, a large provider ecosystem and a UI that data teams know. Choose Step Functions for event-driven, AWS-only pipelines and per-file processing; choose Airflow for many interdependent scheduled pipelines, backfills, multi-cloud or SaaS tasks. Many platforms use both."
followUps: ["How do you backfill a month of data with Step Functions?", "What limits matter when a Step Functions workflow processes 100,000 files?", "How does MWAA bill compared with MWAA Serverless and Step Functions?", "How would Airflow trigger and wait for a Step Functions workflow?"]
related: ["articles:aws/step-functions", "interview-questions:data-engineering/choose-an-orchestrator", "articles:airflow/dags-scheduling-retries", "interview-questions:airflow/catchup-and-backfill"]
versionContext: "Step Functions quotas (Standard up to one year, Express up to five minutes, 25,000 history events, 256 KiB payloads, 10,000 parallel Distributed Map children) and billing models, and MWAA versions (Airflow up to v3.3.1) and MWAA Serverless checked against the AWS documentation in October 2026. The state machine JSON was checked to parse and the transition-count model runs on Python 3.11. The Airflow DAG needs the Amazon provider and AWS, so it was written from the provider documentation and not executed."
sources:
  - { label: "AWS Step Functions Developer Guide: Service quotas", url: "https://docs.aws.amazon.com/step-functions/latest/dg/service-quotas.html" }
  - { label: "AWS Step Functions pricing", url: "https://aws.amazon.com/step-functions/pricing/" }
  - { label: "Amazon MWAA: Apache Airflow versions", url: "https://docs.aws.amazon.com/mwaa/latest/userguide/airflow-versions.html" }
  - { label: "Amazon MWAA Serverless User Guide", url: "https://docs.aws.amazon.com/mwaa/latest/mwaa-serverless-userguide/what-is-mwaa-serverless.html" }
  - { label: "Amazon MWAA: Environment class", url: "https://docs.aws.amazon.com/mwaa/latest/userguide/environment-class.html" }
---

## Detailed explanation

| | AWS Step Functions | Apache Airflow on Amazon MWAA |
|---|---|---|
| Model | State machine: states, transitions, choices, parallel branches, maps | DAG of tasks per data interval (`logical_date`, `data_interval_start/end`) |
| Definition | Amazon States Language (JSON or YAML), Workflow Studio, CDK | Python DAG files (MWAA Serverless also accepts YAML definitions) |
| Triggers | EventBridge rules and schedules, API calls, S3 events via EventBridge | Cron or timetable schedules, assets (data-aware scheduling), API, sensors |
| Waiting for jobs | `.sync` integrations wait for Glue, EMR, EMR Serverless, Athena, ECS, Batch, nested workflows without polling code | Operators and deferrable sensors from the Amazon provider |
| Fan-out | `Map`; **Distributed Map** reads an S3 listing or manifest and runs up to 10,000 parallel child executions | Dynamic task mapping, pools to limit concurrency |
| Backfill | No built-in notion of intervals; start executions with the dates you need | First class: catchup, `airflow backfill`, clear and rerun |
| Limits | Standard: up to 1 year, 25,000 history events; Express: up to 5 minutes; 256 KiB state payloads | Scheduler and worker capacity by environment class; metadata database |
| Billing model | Standard: per state transition (retries count); Express: per request and duration | MWAA: environment-hours by class plus extra workers, schedulers and storage, running all the time; MWAA Serverless: compute time while tasks run |
| Ecosystem | AWS services, HTTP tasks, any AWS SDK call | Hundreds of provider packages (Snowflake, dbt, Databricks, SaaS APIs, other clouds) |

### How to choose

- **Step Functions** when the pipeline is triggered by events (a file lands, a CDC batch completes), runs on AWS services, needs per-item fan-out, and the team wants nothing to keep running between runs.
- **Airflow** when there are dozens or hundreds of scheduled pipelines with cross-pipeline dependencies, backfills are routine, tasks call non-AWS systems, and the team wants Python and the Airflow UI.
- **Both**: Airflow schedules the daily graph and starts a Step Functions workflow (or Glue job) for an AWS-heavy step; Step Functions handles event-driven ingestion while Airflow handles the daily marts.

## Example

A Step Functions workflow that runs a Glue job and waits for it, retries throttling, then refreshes partitions with Athena and alerts on failure:

```json
{
  "Comment": "Daily orders curation",
  "StartAt": "CurateOrders",
  "States": {
    "CurateOrders": {
      "Type": "Task",
      "Resource": "arn:aws:states:::glue:startJobRun.sync",
      "Parameters": {"JobName": "orders-curate", "Arguments": {"--run_date.$": "$.run_date"}},
      "Retry": [{"ErrorEquals": ["Glue.ConcurrentRunsExceededException", "States.TaskFailed"],
                 "IntervalSeconds": 60, "MaxAttempts": 2, "BackoffRate": 2.0}],
      "Catch": [{"ErrorEquals": ["States.ALL"], "ResultPath": "$.error", "Next": "NotifyFailure"}],
      "ResultPath": null,
      "Next": "RefreshPartitions"
    },
    "RefreshPartitions": {
      "Type": "Task",
      "Resource": "arn:aws:states:::athena:startQueryExecution.sync",
      "Parameters": {"QueryString": "MSCK REPAIR TABLE curated.orders", "WorkGroup": "pipelines"},
      "Catch": [{"ErrorEquals": ["States.ALL"], "ResultPath": "$.error", "Next": "NotifyFailure"}],
      "End": true
    },
    "NotifyFailure": {
      "Type": "Task",
      "Resource": "arn:aws:states:::sns:publish",
      "Parameters": {"TopicArn": "arn:aws:sns:eu-west-1:111122223333:data-alerts",
                     "Message.$": "States.JsonToString($.error)"},
      "Next": "Failed"
    },
    "Failed": {"Type": "Fail", "Error": "PipelineFailed"}
  }
}
```

The same step in Airflow 3 with the Amazon provider, where backfilling a month is one command:

<!-- noexec -->
```python
from datetime import datetime

from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.sdk import dag


@dag(schedule="@daily", start_date=datetime(2026, 10, 1), catchup=False, max_active_runs=3)
def orders_curation():
    curate = GlueJobOperator(
        task_id="curate_orders",
        job_name="orders-curate",
        script_args={"--run_date": "{{ ds }}"},
        wait_for_completion=True,
        deferrable=True,              # frees the worker slot while Glue runs
        retries=2,
    )
    refresh = AthenaOperator(
        task_id="refresh_partitions",
        query="MSCK REPAIR TABLE curated.orders",
        database="curated",
        workgroup="pipelines",
    )
    curate >> refresh


orders_curation()
# Backfill September: airflow backfill create --dag-id orders_curation \
#   --from-date 2026-09-01 --to-date 2026-09-30
```

Billing on Step Functions Standard follows state transitions, so fan-out design matters. A transition-count model for processing files (illustrative counts; Express child workflows are billed by requests and duration instead):

```python
def standard_transitions(files, states_per_file, retry_rate=0.02, retries_per_failure=1, outer_states=4):
    per_file = states_per_file + retry_rate * retries_per_failure
    return round(outer_states + files * per_file)

for files in (100, 10_000, 100_000):
    print(f"{files:>7} files: about {standard_transitions(files, states_per_file=3):,} state transitions per run")
```

```text
    100 files: about 306 state transitions per run
  10000 files: about 30,204 state transitions per run
 100000 files: about 302,004 state transitions per run
```

At 100,000 files a single Standard execution with an inline `Map` would also exceed the 25,000-event history limit; Distributed Map runs each item (or batch of items) as a child execution, and batching many files per child reduces both transitions and history.

## Trade-offs and pitfalls

- **Rebuilding Airflow in Step Functions**: dates, backfills and cross-workflow dependencies become custom code; if those dominate, use Airflow.
- **Heavy work inside Airflow workers**: MWAA workers are for orchestration; push processing to Glue, EMR, Athena or containers.
- **Large payloads between states**: 256 KiB limit; pass S3 keys, not data.
- **Always-on MWAA cost for a handful of DAGs**: a small number of simple workflows may be cheaper on Step Functions, EventBridge Scheduler or MWAA Serverless.
- **Non-idempotent tasks with retries**: both systems retry; every task must be safe to rerun for the same date or file.
- **Version upgrades**: MWAA runs specific Airflow versions (2.x and 3.x lines); Airflow 3 changes imports and the task SDK, so plan migrations.

## Follow-up answers

- **Backfill in Step Functions**: start one execution per date (from a script, or a Map over a list of dates) passing `run_date`, with a concurrency limit, and make each step idempotent for that date.
- **100,000 files**: use Distributed Map over the S3 listing or a manifest, batch items per child, set `MaxConcurrency` and a tolerated failure threshold, and write results to S3 rather than returning them in state.
- **Billing**: MWAA charges for the environment (by class) and extra workers whether or not tasks run; MWAA Serverless charges for compute time while tasks run; Step Functions Standard charges per state transition and Express per request and duration.
- **Airflow calling Step Functions**: `StepFunctionStartExecutionOperator` starts the execution and a sensor or deferrable mode waits for success, so Airflow keeps the schedule and lineage while Step Functions handles the AWS-native fan-out.
