---
publishedDate: "2026-10-16"
updatedDate: "2026-10-16"
reviewedDate: "2026-10-16"
title: "How would you cut the AWS bill of a data platform without breaking SLAs?"
seoTitle: "AWS Data Platform Cost Optimisation: Interview"
description: "AWS data platform cost optimisation: find the spend with tags and Cost Explorer, then fix storage classes, bytes scanned, idle compute, NAT traffic and logs."
technology: ["aws"]
topic: ["cost-optimization", "finops", "s3", "architecture"]
difficulty: "Medium"
questionType: ["scenario", "optimization"]
estimatedMinutes: 12
interviewRelevance: "High"
shortAnswer: "First measure: tag every resource by team, pipeline and environment, activate cost allocation tags, and use Cost Explorer or Data Exports to find the biggest lines; do not guess. Then work through the usual drivers. Storage: lifecycle rules to cheaper S3 classes, expire raw copies and old versions, abort incomplete multipart uploads. Query engines: fewer bytes scanned through Parquet, partitions and compaction, plus workgroup limits. Compute: right-size Glue and EMR, auto scaling, Spot and Graviton, auto-termination, Flex for non-urgent jobs, Savings Plans for the steady base. Network: S3 gateway endpoints instead of NAT gateways, keep processing in one Region. Logs: retention policies. Add budgets and anomaly detection so it stays fixed."
followUps: ["When does S3 Intelligent-Tiering cost more than it saves?", "Why can a NAT gateway be one of the largest lines on a data platform bill?", "How do you choose between Redshift Serverless and a provisioned cluster on cost?", "How would you make teams accountable for their spend?"]
related: ["articles:aws/aws-data-services-landscape", "articles:aws/s3-for-data-engineers", "interview-questions:aws/athena-cost-performance-tuning", "system-designs:cost-optimized-warehouse-strategy"]
versionContext: "Pricing models (not prices), S3 minimum storage durations and sizes, Intelligent-Tiering monitoring rules, gateway endpoint charges and Redshift Serverless billing checked against the AWS documentation in October 2026. The lifecycle model runs on Python 3.11 and counts billed GB-days, not money; multiply by current regional rates from the pricing pages. CLI commands were written from the documentation and not executed."
sources:
  - { label: "Amazon S3 User Guide: Understanding and managing storage classes", url: "https://docs.aws.amazon.com/AmazonS3/latest/userguide/storage-class-intro.html" }
  - { label: "Amazon S3 User Guide: How S3 Intelligent-Tiering works", url: "https://docs.aws.amazon.com/AmazonS3/latest/userguide/intelligent-tiering-overview.html" }
  - { label: "AWS PrivateLink guide: Gateway endpoints", url: "https://docs.aws.amazon.com/vpc/latest/privatelink/gateway-endpoints.html" }
  - { label: "Amazon Redshift: Billing for Amazon Redshift Serverless", url: "https://docs.aws.amazon.com/redshift/latest/mgmt/serverless-billing.html" }
  - { label: "AWS Billing: Using cost allocation tags", url: "https://docs.aws.amazon.com/awsaccountbilling/latest/aboutv2/cost-alloc-tags.html" }
---

## Detailed explanation

A good answer is a **process**, not a list of tricks: make spend visible, fix the largest drivers first, check that SLAs still hold, then add guardrails so it does not drift back.

### 1. Make spend visible

- **Tags** (`team`, `pipeline`, `env`) on buckets, Glue jobs, EMR clusters, Redshift workgroups, Athena workgroups; activate them as **cost allocation tags**. Enforce with tag policies or IaC checks.
- **Cost Explorer** grouped by service and tag, and **Data Exports** (the Cost and Usage Report) into S3 for SQL analysis in Athena.
- **Service-level views**: S3 Storage Lens for bucket growth, noncurrent versions and incomplete uploads; Athena workgroup metrics for bytes scanned; Glue and EMR job histories for DPU-hours and idle time.

### 2. Fix the usual drivers

| Area | Pricing model | Typical waste | Fix |
|---|---|---|---|
| S3 storage | Per GB-month by storage class, plus requests and retrievals | Raw and intermediate copies kept forever, old object versions, incomplete multipart uploads | Lifecycle transitions and expirations, `NoncurrentVersionExpiration`, `AbortIncompleteMultipartUpload`, Intelligent-Tiering for unknown access patterns |
| S3 requests | Per request | Millions of tiny files read by every query | Compaction to larger files |
| Athena | Per byte scanned (or per DPU-hour with reservations) | JSON or CSV scans, missing partitions, `SELECT *` | Parquet or Iceberg, partition projection, workgroup scan limits, result reuse |
| Glue | Per DPU-hour, per second | Oversized fixed worker counts, dev endpoints and sessions left open | Auto scaling, smaller workers, Flex for non-urgent jobs, session idle timeouts |
| EMR | EC2 plus an EMR charge, per second | Clusters left running, On-Demand only | Transient clusters, auto-termination, Spot task nodes, Graviton, managed scaling |
| Redshift | Provisioned: node-hours; Serverless: RPU-hours (per second, 60-second minimum) plus storage | High base capacity, clusters running overnight with no queries | Right-size base RPUs and set max RPU-hour usage limits; pause or resize provisioned clusters; Reserved nodes for steady clusters |
| Network | NAT gateway hourly plus per GB processed; inter-AZ and inter-Region transfer per GB | Spark jobs in private subnets reading S3 through a NAT gateway | S3 and DynamoDB **gateway endpoints** (no additional charge), co-locate compute and data in one Region |
| CloudWatch Logs | Per GB ingested and stored | Debug logging and "never expire" retention | Log levels, retention policies, metric filters instead of storing everything |
| Commitments | Savings Plans, Reserved Instances or nodes | Paying On-Demand rates for a steady base | Commit only for the always-on base after right-sizing |

### 3. Guardrails

AWS Budgets with alerts per team, Cost Anomaly Detection, Athena per-query scan limits, Redshift Serverless usage limits, EMR auto-termination, and IaC defaults (lifecycle rules and log retention in every module) keep the savings.

## Example

Lifecycle rules for a raw zone: move to Standard-IA after 30 days, to Glacier Flexible Retrieval after 90, expire after two years, clean up versions and failed uploads:

```json
{
  "Rules": [
    {
      "ID": "raw-zone-tiering",
      "Status": "Enabled",
      "Filter": {"Prefix": "raw/"},
      "Transitions": [
        {"Days": 30, "StorageClass": "STANDARD_IA"},
        {"Days": 90, "StorageClass": "GLACIER"}
      ],
      "Expiration": {"Days": 730},
      "NoncurrentVersionExpiration": {"NoncurrentDays": 7},
      "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 3}
    }
  ]
}
```

<!-- noexec -->
```bash
aws s3api put-bucket-lifecycle-configuration --bucket acme-lake-raw \
  --lifecycle-configuration file://raw-lifecycle.json
```

Infrequent Access classes are not always cheaper. They bill at least 128 KB per object and at least 30 days of storage. This model counts **billed GB-days** for one day's landing of small JSON files that are deleted after 10 days, in Standard versus Standard-IA:

```python
KB, GB = 1024, 1024 ** 3

def billed_gb_days(objects, avg_kb, days_kept, min_object_kb=0, min_days=0):
    per_object_kb = max(avg_kb, min_object_kb)
    return objects * per_object_kb * KB / GB * max(days_kept, min_days)

objects, avg_kb, days = 2_000_000, 20, 10          # 2 million 20 KB files, kept 10 days
standard = billed_gb_days(objects, avg_kb, days)
standard_ia = billed_gb_days(objects, avg_kb, days, min_object_kb=128, min_days=30)
print(f"Standard    : {standard:8.1f} GB-days at the Standard rate")
print(f"Standard-IA : {standard_ia:8.1f} GB-days at the IA rate ({standard_ia / standard:.0f}x more GB-days)")

# The same volume compacted into 128 MB Parquet files and kept 400 days
compacted = billed_gb_days(objects * avg_kb // (128 * 1024), 128 * 1024, 400, 128, 30)
print(f"compacted, 400 days in IA: {compacted:8.1f} GB-days, objects: {objects * avg_kb // (128 * 1024)}")
```

```text
Standard    :    381.5 GB-days at the Standard rate
Standard-IA :   7324.2 GB-days at the IA rate (19x more GB-days)
compacted, 400 days in IA:  15250.0 GB-days, objects: 305
```

For short-lived small files, Standard-IA bills 19 times the GB-days, which wipes out its lower rate; the same data compacted into large files and kept for a year is exactly what IA and Glacier classes are for. Apply the same thinking to Intelligent-Tiering: objects under 128 KB are never moved to cheaper tiers, and every monitored object carries a monitoring fee.

## Trade-offs and pitfalls

- **Cutting without measuring**: optimising a minor service while a NAT gateway or forgotten cluster dominates.
- **Breaking SLAs**: shrinking Glue workers or Redshift base capacity can push jobs past their deadline; compare run times before and after.
- **Spot for everything**: drivers, core nodes and tight-SLA jobs need On-Demand capacity; Spot interruptions cost reruns.
- **Commitments too early**: a Savings Plan bought before right-sizing locks in waste.
- **Glacier for data that is still queried**: retrieval fees and restore delays; keep queryable tables in Standard or Intelligent-Tiering.
- **Deleting raw data you need for replay**: agree retention with data owners and compliance first.

## Follow-up answers

- **Intelligent-Tiering costing more**: many objects under 128 KB (no tiering, still Frequent Access), or millions of objects whose per-object monitoring fee exceeds the storage saving; compact first, or use lifecycle rules when the access pattern is known.
- **NAT gateway**: it charges per GB processed, so Spark or Glue in private subnets reading terabytes from S3 through it pays per GB on top of everything else. A gateway endpoint for S3 removes that charge and keeps traffic on the AWS network.
- **Redshift Serverless vs provisioned**: Serverless bills RPU-seconds only while queries run, which suits intermittent or spiky use; a provisioned cluster with Reserved nodes is usually cheaper when it is busy most hours of the day. Compare RPU-hours used per day with the cluster's always-on hours.
- **Accountability**: tags and cost categories per team, monthly reports in the team's own dashboard, budgets with alerts to the owning team, and cost as a review item for new pipelines.
