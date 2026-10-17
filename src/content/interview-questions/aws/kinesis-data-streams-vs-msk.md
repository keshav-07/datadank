---
publishedDate: "2026-10-16"
updatedDate: "2026-10-16"
reviewedDate: "2026-10-16"
title: "Kinesis Data Streams or Amazon MSK: which would you pick for streaming on AWS?"
seoTitle: "Kinesis Data Streams vs Amazon MSK: Interview"
description: "Kinesis Data Streams vs Amazon MSK: capacity modes, shard and partition limits, ordering, retention, ecosystem and operations, with a shard-sizing model."
technology: ["aws", "kafka"]
topic: ["kinesis", "msk", "streaming", "comparison"]
difficulty: "Medium"
questionType: ["architecture", "scenario"]
estimatedMinutes: 10
interviewRelevance: "High"
shortAnswer: "Both are partitioned, replayable logs with per-shard or per-partition ordering. Kinesis Data Streams is the AWS-native choice: no brokers, on-demand or provisioned capacity, IAM everywhere, and direct integration with Lambda, Firehose and Managed Service for Apache Flink; you work within per-shard limits (1 MB/s or 1,000 records/s in, 2 MB/s out) and up to 365 days of retention. Amazon MSK is managed Apache Kafka: pick it when you need the Kafka API and ecosystem (Kafka Connect, Kafka Streams, Debezium CDC, transactions, compaction), portability, existing Kafka skills, or very high throughput at steady load. MSK Serverless and Express brokers reduce the operations, but you still design topics, partitions and client settings."
followUps: ["How do you size a Kinesis stream in provisioned mode?", "What causes hot shards and how do you fix them?", "When is MSK Serverless a better fit than MSK Provisioned?", "How does a Lambda consumer handle a poison record on Kinesis?"]
related: ["articles:aws/kinesis", "interview-questions:kafka/kafka-vs-kinesis-vs-queues", "articles:kafka/topics-partitions-consumer-groups", "system-designs:real-time-analytics-pipeline"]
versionContext: "Kinesis Data Streams capacity modes and quotas (On-demand Standard, On-demand Advantage, Provisioned; 10 MiB record payloads) and MSK cluster types and supported Kafka versions (up to 4.2.x) checked against the AWS documentation in October 2026. The CLI commands were written from the documentation and not executed; the shard-sizing model runs on Python 3.11 with illustrative traffic."
sources:
  - { label: "Kinesis Data Streams: Quotas and limits", url: "https://docs.aws.amazon.com/streams/latest/dev/service-sizes-and-limits.html" }
  - { label: "Kinesis Data Streams: Choose the right capacity mode", url: "https://docs.aws.amazon.com/streams/latest/dev/how-do-i-size-a-stream.html" }
  - { label: "Amazon MSK: Express brokers", url: "https://docs.aws.amazon.com/msk/latest/developerguide/msk-broker-types-express.html" }
  - { label: "Amazon MSK: MSK Serverless", url: "https://docs.aws.amazon.com/msk/latest/developerguide/serverless.html" }
  - { label: "Amazon MSK: Supported Apache Kafka versions", url: "https://docs.aws.amazon.com/msk/latest/developerguide/supported-kafka-versions.html" }
---

## Detailed explanation

The data model is the same: records with a key go to a shard (Kinesis) or partition (Kafka) by hashing the key, order holds within that shard or partition, and consumers track their position so they can replay. The differences are capacity management, ecosystem and operations.

| | Kinesis Data Streams | Amazon MSK |
|---|---|---|
| Capacity | **On-demand Standard** (scales automatically, up to double the previous 30-day peak write throughput), **On-demand Advantage** (account-level mode with warm capacity and lower per-GB rates), or **Provisioned** shards | **Provisioned** clusters with Standard brokers (you size instances and storage) or **Express** brokers (pay-as-you-go storage, faster scaling), or **MSK Serverless** |
| Unit limits | Per shard: 1 MB/s or 1,000 records/s write, 2 MB/s read shared by standard consumers; enhanced fan-out gives each registered consumer its own 2 MB/s | Per broker and per partition, set by instance size, storage throughput and client quotas |
| Record size | Up to 10 MiB payload | `max.message.bytes`, 1 MB by default; larger is possible but usually a design smell |
| Retention | 24 hours default, up to 365 days | Time or size based, unlimited with tiered storage; log compaction per topic |
| Consumers | KCL apps, Lambda event source mappings, Firehose, Managed Service for Apache Flink; up to 20 enhanced fan-out consumers (50 with On-demand Advantage) | Any Kafka client, consumer groups, Kafka Streams, Flink, Spark, Lambda, Firehose (MSK as source) |
| Ecosystem | AWS integrations | Kafka Connect (MSK Connect), Debezium, Schema Registry, transactions and exactly-once processing in Kafka |
| Auth | IAM | IAM, SASL/SCRAM, mTLS (Serverless requires IAM) |
| Billing model | On-demand: per GB written and read plus per-stream charges in Standard; Provisioned: per shard-hour and PUT units | Provisioned: broker-hours plus storage (Express: storage used); Serverless: cluster-hours, partition-hours and data in and out |

### How to decide

- **Kinesis** for AWS-native teams, modest to large but variable throughput, Lambda or Firehose consumers, and no need for the Kafka API. Less to operate and nothing to patch.
- **MSK** when producers or consumers already speak Kafka, you need Connect connectors or Debezium CDC, compacted topics, Kafka transactions, a multi-cloud or on-prem story, or many consumer groups on the same topics at high steady throughput.
- **MSK Serverless** for Kafka API compatibility without sizing brokers, at modest scale (it has per-cluster throughput and partition quotas). **Express brokers** for large provisioned clusters where storage management and recovery time are the pain.

## Example

Sizing a provisioned Kinesis stream: shards needed are the maximum of the byte and record limits, then headroom for spikes. Illustrative traffic:

```python
import math

def shards_needed(records_per_sec, avg_record_kb, consumers_without_efo=1, headroom=1.3):
    write_mb = records_per_sec * avg_record_kb / 1024
    by_bytes = write_mb / 1.0                              # 1 MB/s write per shard
    by_records = records_per_sec / 1000                    # 1,000 records/s write per shard
    by_reads = write_mb * consumers_without_efo / 2.0      # 2 MB/s read per shard, shared
    return math.ceil(max(by_bytes, by_records, by_reads) * headroom), round(write_mb, 1)

for name, args in {
    "clickstream (small records)": (12_000, 0.8, 1),
    "CDC events (larger records)": (1_500, 6.0, 1),
    "three shared consumers": (1_500, 6.0, 3),
}.items():
    shards, mb = shards_needed(*args)
    print(f"{name:28} {mb:5} MB/s in -> {shards} shards")
```

```text
clickstream (small records)    9.4 MB/s in -> 16 shards
CDC events (larger records)    8.8 MB/s in -> 12 shards
three shared consumers         8.8 MB/s in -> 18 shards
```

Small records hit the records-per-second limit before the byte limit, and several standard consumers share the 2 MB/s read limit; enhanced fan-out gives each registered consumer its own 2 MB/s per shard instead. On-demand mode avoids this arithmetic for unpredictable traffic.

Creating each option with the AWS CLI:

<!-- noexec -->
```bash
# Kinesis Data Streams, on-demand
aws kinesis create-stream --stream-name orders-events \
  --stream-mode-details StreamMode=ON_DEMAND

# Kinesis: keep data 7 days for replay
aws kinesis increase-stream-retention-period --stream-name orders-events \
  --retention-period-hours 168

# MSK Serverless cluster with IAM auth (the only auth Serverless supports)
aws kafka create-cluster-v2 --cluster-name orders-kafka --serverless '{
  "VpcConfigs": [{"SubnetIds": ["subnet-0a1b2c3d", "subnet-4e5f6a7b"],
                  "SecurityGroupIds": ["sg-0123456789abcdef0"]}],
  "ClientAuthentication": {"Sasl": {"Iam": {"Enabled": true}}}
}'
```

## Trade-offs and pitfalls

- **Hot shards or partitions**: a skewed key (one big customer, a null key) overloads one shard while others idle. Use a higher-cardinality key, or add a suffix to hot keys and merge downstream if order per entity allows it.
- **Throttling on Kinesis** (`ProvisionedThroughputExceededException`): retries with backoff, batching with `PutRecords`, aggregation with the KPL, more shards, or on-demand mode. On-demand Standard can still throttle if traffic more than doubles the previous peak within 15 minutes.
- **Underestimating Kafka operations**: even on MSK you choose partition counts, replication factor, `min.insync.replicas`, client settings and upgrades.
- **Choosing MSK for a single Lambda consumer** pays for brokers and VPC networking you do not need; choosing Kinesis for a Debezium or Connect-heavy design means building those connectors yourself.
- **Retention as the archive**: neither log should be the system of record for years; land data in S3 (Firehose or a consumer) and replay from there for old periods.

## Follow-up answers

- **Provisioned sizing**: the maximum of write MB/s divided by 1, records/s divided by 1,000, and read MB/s per shared consumer group divided by 2, plus headroom; re-shard with `UpdateShardCount` and watch `WriteProvisionedThroughputExceeded`.
- **Hot shards**: check per-shard metrics (enhanced shard-level monitoring), change the partition key or salt hot keys, or split the hot shard.
- **MSK Serverless vs Provisioned**: Serverless for small to medium or unpredictable Kafka workloads within its quotas, with IAM-only auth; Provisioned (Standard or Express brokers) for high sustained throughput, custom broker configuration, or other auth methods.
- **Poison record on Kinesis with Lambda**: the event source mapping retries the batch and blocks the shard; configure `BisectBatchOnFunctionError`, a maximum retry count and record age, an on-failure destination (SQS, SNS or S3), and `ReportBatchItemFailures` so only the failed record is retried.
