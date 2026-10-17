---
publishedDate: "2026-10-16"
updatedDate: "2026-10-16"
reviewedDate: "2026-10-16"
title: "When is AWS Lambda a good fit for ETL, and what limits push you to Glue or containers?"
seoTitle: "AWS Lambda for ETL and Its Limits: Interview"
description: "AWS Lambda for ETL: which jobs fit, the limits that matter (15 minutes, 10 GB memory, payloads, concurrency) and how to handle partial batch failures."
technology: ["aws"]
topic: ["lambda", "serverless", "etl", "limits"]
difficulty: "Medium"
questionType: ["scenario", "conceptual"]
estimatedMinutes: 10
interviewRelevance: "High"
shortAnswer: "Lambda fits small, event-driven units of work: validate or convert one file when it lands, enrich a batch of stream or queue records, call an API, or start and check other jobs. It scales out per event with no servers and bills per request and per GB-second. Its limits decide the rest: at most 15 minutes per invocation, 128 MB to 10,240 MB of memory (CPU grows with memory), up to 10 GB of /tmp, 6 MB synchronous and 1 MB asynchronous payloads, a 250 MB unzipped package or 10 GB container image, and a regional concurrency quota shared by all functions. Long, memory-heavy or shuffle-heavy transformations (big joins, aggregations over many files) belong in Glue, EMR, Athena or a container task."
followUps: ["How do you make a Lambda that processes S3 files idempotent?", "What happens when one record in an SQS batch fails?", "How do you stop a Lambda from overwhelming a database?", "How do you choose the memory size?"]
related: ["articles:aws/lambda-for-data-pipelines", "interview-questions:aws/s3-event-driven-ingestion", "interview-questions:aws/glue-vs-emr-vs-emr-serverless", "articles:etl-elt/idempotency-in-data-pipelines"]
versionContext: "Quotas checked against the AWS Lambda Developer Guide (Lambda quotas) in October 2026, including the 1 MB asynchronous payload limit and up to 10,240 MB of ephemeral storage. The handler runs locally on Python 3.11 with a simulated SQS event and an in-memory store; it is not deployed. The boto3 configuration call was written from the documentation and not executed."
sources:
  - { label: "AWS Lambda Developer Guide: Lambda quotas", url: "https://docs.aws.amazon.com/lambda/latest/dg/gettingstarted-limits.html" }
  - { label: "AWS Lambda: Handling errors for an SQS event source (partial batch responses)", url: "https://docs.aws.amazon.com/lambda/latest/dg/services-sqs-errorhandling.html" }
  - { label: "AWS Lambda: Configuring reserved concurrency", url: "https://docs.aws.amazon.com/lambda/latest/dg/configuration-concurrency.html" }
---

## Detailed explanation

### Where Lambda fits in a data platform

| Good fit | Poor fit |
|---|---|
| Convert or validate one landed file (CSV to Parquet, schema check) when it is tens or hundreds of MB | Joining or aggregating many GB across many files |
| Enrich records from SQS, Kinesis, DynamoDB Streams or MSK in small batches | Jobs that run longer than 15 minutes or need to hold large state |
| Glue between services: start a Glue job, register partitions, post to Slack, call an API | Workloads that stay busy 24/7 at high, steady concurrency (a container or cluster is cheaper) |
| Custom resources, scheduled housekeeping | Heavy native dependencies beyond the package or image limits |

### Limits that shape designs

| Limit | Value | Design consequence |
|---|---|---|
| Timeout | 900 s (15 minutes) | Split work into chunks (one file, one partition) or move it to Glue, EMR or ECS/Fargate |
| Memory | 128 MB to 10,240 MB; about one vCPU at 1,769 MB | More memory also means more CPU; tune by measuring duration at several sizes |
| Ephemeral storage `/tmp` | 512 MB to 10,240 MB | Stream large objects instead of downloading them whole when possible |
| Payload | 6 MB request and response (synchronous), 1 MB (asynchronous) | Pass S3 keys, not data, between steps |
| Package | 50 MB zipped upload, 250 MB unzipped with layers; 10 GB container image | Use container images for pandas, PyArrow and similar heavy libraries |
| Concurrency | 1,000 per Region by default (raisable), shared by all functions | Reserved concurrency protects critical functions and caps others |

### Reliability with event sources

- **S3 and other asynchronous invokes** retry failed events (twice by default) and then send them to an on-failure destination or dead-letter queue, so handlers must be **idempotent**.
- **SQS event source mapping**: a failed batch becomes visible again and is retried; with `ReportBatchItemFailures`, the function returns only the failed message IDs so successful ones are deleted. Configure a redrive policy so poison messages land in a DLQ.
- **Kinesis and DynamoDB Streams**: a failed batch blocks its shard until it succeeds or expires; use bisect-on-error, maximum retry attempts and record age, and an on-failure destination.

## Example

An SQS-triggered transform that reports partial batch failures and skips messages it has already processed. The in-memory dictionary stands in for a DynamoDB table with a conditional write.

```python
import json

processed = {}          # stands in for a DynamoDB table keyed by message id
output = []             # stands in for objects written to S3

def transform(order):
    if order["amount"] < 0:
        raise ValueError(f"negative amount in order {order['order_id']}")
    return {**order, "amount_cents": round(order["amount"] * 100)}

def handler(event, context=None):
    failures = []
    for record in event["Records"]:
        msg_id = record["messageId"]
        if msg_id in processed:                 # duplicate delivery: already done
            continue
        try:
            row = transform(json.loads(record["body"]))
            output.append(row)
            processed[msg_id] = True            # conditional put in the real system
        except Exception as exc:
            print(f"failed {msg_id}: {exc}")
            failures.append({"itemIdentifier": msg_id})
    return {"batchItemFailures": failures}

event = {"Records": [
    {"messageId": "m1", "body": json.dumps({"order_id": 1, "amount": 19.99})},
    {"messageId": "m2", "body": json.dumps({"order_id": 2, "amount": -5})},
    {"messageId": "m3", "body": json.dumps({"order_id": 3, "amount": 7.5})},
]}
print(handler(event))
# SQS redelivers only m2 (plus, at least once, possibly m1 again); m1 is skipped
print(handler({"Records": [event["Records"][0], event["Records"][1]]}))
print("rows written:", len(output))
```

```text
failed m2: negative amount in order 2
{'batchItemFailures': [{'itemIdentifier': 'm2'}]}
failed m2: negative amount in order 2
{'batchItemFailures': [{'itemIdentifier': 'm2'}]}
rows written: 2
```

After the queue's `maxReceiveCount`, m2 moves to the dead-letter queue for inspection. Enabling partial batch responses on the event source mapping:

<!-- noexec -->
```python
import boto3

lam = boto3.client("lambda")
lam.create_event_source_mapping(
    FunctionName="orders-transform",
    EventSourceArn="arn:aws:sqs:eu-west-1:111122223333:orders-incoming",
    BatchSize=100,
    MaximumBatchingWindowInSeconds=20,
    FunctionResponseTypes=["ReportBatchItemFailures"],
    ScalingConfig={"MaximumConcurrency": 20},   # protects the downstream database
)
```

## Trade-offs and pitfalls

- **Treating Lambda as a small Spark**: loading a 3 GB CSV into pandas fails on memory or time. Use PyArrow streaming, split the file, or use Glue or Athena.
- **Recursive triggers**: a function triggered by `raw/` that writes back into `raw/` invokes itself repeatedly. Write to a different prefix or bucket; Lambda's recursive loop detection stops some loops but do not rely on it.
- **Unbounded concurrency against a database**: 1,000 concurrent functions open 1,000 connections. Cap with SQS maximum concurrency or reserved concurrency, and use RDS Proxy.
- **Ignoring cold starts in latency-sensitive paths**: large packages and VPC setup lengthen them; provisioned concurrency or SnapStart (for supported runtimes) helps when it matters.
- **No DLQ or destination**: failed asynchronous events are dropped after retries.

## Follow-up answers

- **Idempotent S3 processing**: derive the output key from the input key (rerun overwrites the same object), record processed keys plus version ID or `sequencer` with a conditional write, and make downstream loads merge on a business key.
- **One failed record in an SQS batch**: without partial batch responses the whole batch is retried, reprocessing successful messages; with `ReportBatchItemFailures` only the listed IDs return to the queue.
- **Protecting a database**: event source mapping maximum concurrency for SQS, reserved concurrency on the function, batching, and RDS Proxy for connection pooling.
- **Memory size**: run the function at several memory sizes on real input and compare duration and GB-seconds; CPU-bound functions often finish fast enough at higher memory to cost the same or less. AWS Lambda Power Tuning automates this.
