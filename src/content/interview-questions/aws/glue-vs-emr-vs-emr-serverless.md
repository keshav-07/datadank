---
publishedDate: "2026-10-16"
updatedDate: "2026-10-16"
reviewedDate: "2026-10-16"
title: "AWS Glue vs Amazon EMR vs EMR Serverless: which would you use for Spark?"
seoTitle: "Glue vs EMR vs EMR Serverless: Interview Answer"
description: "Glue vs EMR vs EMR Serverless for Spark on AWS: how each one bills, starts, scales and is tuned, with boto3 job submission and a capacity model for choosing."
technology: ["aws", "spark"]
topic: ["glue", "emr", "emr-serverless", "spark", "compute-choice"]
difficulty: "Medium"
questionType: ["architecture", "scenario"]
estimatedMinutes: 10
interviewRelevance: "High"
shortAnswer: "All three run Apache Spark; they differ in how much you manage and how you pay. Glue is serverless Spark with the Data Catalog, bookmarks and connectors built in: you pick a worker type and count, pay per DPU-hour while the job runs, and accept a fixed set of Spark versions and settings. EMR Serverless runs EMR's Spark runtime without clusters: you submit jobs to an application, it adds and removes workers per job and bills vCPU, memory and storage for what workers use, with optional pre-initialised capacity for fast starts. EMR on EC2 (or EKS) gives full control of instances, Spot, configuration and long-running clusters, and is usually cheapest for large, steady workloads, at the cost of operating the cluster."
followUps: ["How do Glue job bookmarks work and when do they fail?", "What does pre-initialised capacity cost you on EMR Serverless?", "How would you use Spot instances safely on EMR?", "When would you choose Athena or Redshift instead of Spark at all?"]
related: ["articles:aws/glue-catalog-crawlers-etl", "articles:aws/emr", "articles:spark/apache-spark-architecture", "system-designs:scalable-batch-pipeline"]
versionContext: "Worker types, versions and billing models checked against the AWS Glue, Amazon EMR and EMR Serverless documentation in October 2026 (Glue 5.1 is the default version and Glue 6.0 ships Spark 4.1). boto3 calls were written from the documentation and not executed (no AWS account); the capacity model runs on Python 3.11 with illustrative inputs and counts resource-hours, not prices."
sources:
  - { label: "AWS Glue: Configuring job properties for Spark jobs (worker types)", url: "https://docs.aws.amazon.com/glue/latest/dg/add-job.html" }
  - { label: "AWS Glue release notes (versions)", url: "https://docs.aws.amazon.com/glue/latest/dg/release-notes.html" }
  - { label: "Amazon EMR Serverless: Pre-initialized capacity", url: "https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/pre-init-capacity.html" }
  - { label: "Amazon EMR Management Guide: Instance fleets", url: "https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-instance-fleet.html" }
---

## Detailed explanation

The question is not "which one is faster": the Spark engine is similar in all three. It is about **control, start-up time, utilisation and the team's operations budget**.

| | AWS Glue (Spark jobs) | EMR Serverless | EMR on EC2 / EMR on EKS |
|---|---|---|---|
| Unit you size | Worker type (G.1X to G.16X, memory-optimised R types) and count | Application with worker limits; per-job driver and executor sizes | EC2 instance types and counts (instance groups or fleets), or pods on your EKS cluster |
| Billing model | DPU-hours while the job runs (1 DPU = 4 vCPU, 16 GB) | vCPU, memory and storage used by workers, per second, plus any pre-initialised capacity | EC2 (On-Demand, Spot, Savings Plans) plus an EMR charge per instance, per second, for as long as the cluster runs, busy or idle |
| Start-up | Seconds to about a minute | Cold start unless pre-initialised capacity is kept warm | Minutes to create a cluster; zero for a running one |
| Versions | Fixed Glue versions (5.1 default, 6.0 with Spark 4.1) | EMR release labels | EMR release labels, any bootstrap actions and configuration |
| AWS extras | Data Catalog, job bookmarks, DynamicFrames, connectors, Glue Data Quality, Flex execution for non-urgent jobs | Same EMR runtime as EMR on EC2, Lake Formation integration | Full cluster: Spark, Hive, Trino, HBase, Flink; SSH, custom AMIs, Spot fleets |
| You operate | Job definitions | Applications and job runs | Clusters, scaling policies, AMIs, node failures |

**How to choose in practice**

- **Glue** for a catalogue-centric team that writes moderate Spark jobs and wants the least infrastructure. Bookmarks, crawlers and Glue Data Quality come with it. The trade-off: less control over Spark settings and libraries, and DPU pricing that becomes expensive for jobs that run many hours a day.
- **EMR Serverless** when you want EMR's runtime and per-job elasticity without clusters, especially for spiky or many independent jobs. Its auto-scaling within a job (dynamic allocation) is the main gain over a fixed Glue worker count.
- **EMR on EC2** for big, steady or long-running workloads where you can keep a cluster busy, use Spot task nodes and Graviton instances, or need applications beyond Spark. **EMR on EKS** when the company already runs Kubernetes and wants Spark to share that capacity.

## Example

Submitting the same PySpark script to Glue and to EMR Serverless with boto3:

<!-- noexec -->
```python
import boto3

glue = boto3.client("glue")
run = glue.start_job_run(
    JobName="orders-curate",            # job defined with GlueVersion "5.1", WorkerType "G.1X"
    Arguments={"--run_date": "2026-10-15"},
    NumberOfWorkers=10,                 # override the job's default count for this run
)
print(run["JobRunId"])

emr = boto3.client("emr-serverless")
run = emr.start_job_run(
    applicationId="00abcdefgh123456",
    executionRoleArn="arn:aws:iam::111122223333:role/emr-serverless-orders",
    jobDriver={
        "sparkSubmit": {
            "entryPoint": "s3://acme-code/jobs/orders_curate.py",
            "entryPointArguments": ["--run_date", "2026-10-15"],
            "sparkSubmitParameters": (
                "--conf spark.executor.cores=4 --conf spark.executor.memory=14g "
                "--conf spark.dynamicAllocation.maxExecutors=40"
            ),
        }
    },
)
print(run["jobRunId"])
```

Utilisation drives the decision more than list prices. This model counts the **compute hours you pay for** for a nightly job plus hourly small jobs, with a fixed Glue fleet, per-job serverless scaling, and an always-on cluster:

```python
nightly = {"runs_per_day": 1, "hours": 1.5, "peak_vcpu": 160, "avg_utilisation": 0.55}
hourly = {"runs_per_day": 24, "hours": 0.15, "peak_vcpu": 16, "avg_utilisation": 0.7}

def fixed_fleet_vcpu_hours(job):
    # a fixed worker count must be sized for the peak for the whole run
    return job["runs_per_day"] * job["hours"] * job["peak_vcpu"]

def elastic_vcpu_hours(job):
    # dynamic allocation releases executors, so you pay roughly for average use
    return job["runs_per_day"] * job["hours"] * job["peak_vcpu"] * job["avg_utilisation"]

def always_on_cluster_vcpu_hours(peak_vcpu):
    return 24 * peak_vcpu

jobs = [nightly, hourly]
print("fixed fleet per job :", round(sum(fixed_fleet_vcpu_hours(j) for j in jobs)), "vCPU-h/day")
print("elastic per job     :", round(sum(elastic_vcpu_hours(j) for j in jobs)), "vCPU-h/day")
print("always-on cluster   :", always_on_cluster_vcpu_hours(160), "vCPU-h/day")
```

```text
fixed fleet per job : 298 vCPU-h/day
elastic per job     : 172 vCPU-h/day
always-on cluster   : 3840 vCPU-h/day
```

A cluster sized for the nightly peak and left running pays for more than ten times the compute the jobs need; a transient EMR cluster, Glue or EMR Serverless avoids that. A cluster only wins when it is kept busy (many jobs, streaming, interactive users) and its per-hour rate is lower thanks to Spot, Savings Plans or Graviton. Multiply resource-hours by the current regional rates from the pricing pages before deciding; rates differ per service.

## Trade-offs and pitfalls

- **Treating Glue as "free Spark"**: a G.2X job with 50 workers running all day is a large bill. Right-size workers, enable auto scaling, and use Flex for jobs that can wait.
- **Ignoring start-up latency**: an EMR Serverless application with no pre-initialised capacity adds cold-start time to every job after it has stopped; pre-initialised workers are billed while the application is started, even when idle.
- **Long-lived EMR clusters nobody owns**: set auto-termination (idle timeout) and use transient clusters for scheduled batch.
- **Spot on core nodes**: losing core nodes loses HDFS blocks and shuffle data. Put Spot on task nodes (or use instance fleets with several instance types) and keep core nodes On-Demand.
- **Version drift**: Glue versions and EMR releases ship different Spark, Iceberg and Delta versions. Pin them and test upgrades; Glue 6.0 moves to Spark 4.1, Python 3.13 and removes EMRFS in favour of S3A.

## Follow-up answers

- **Bookmarks**: Glue stores state per job (for S3, which files or modification times were processed; for JDBC, a key column) so the next run reads only new data. They fail when files are overwritten in place, when the job is not committed (`job.commit()` missing), or when a backfill needs old data; reset or pause the bookmark for reprocessing.
- **Pre-initialised capacity**: you pay for those workers whenever the application is started, so size them for the steady part of the workload and let auto-stop shut the application down when idle.
- **Spot on EMR**: task nodes or instance fleets with several instance types and allocation strategies, graceful decommissioning, and checkpointed or idempotent jobs that tolerate retries.
- **Athena or Redshift instead**: when the transformation is SQL over catalogued tables, Athena CTAS/INSERT or Redshift often costs less to build and run than a Spark job.
