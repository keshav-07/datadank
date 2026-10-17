---
publishedDate: "2026-10-16"
updatedDate: "2026-10-16"
reviewedDate: "2026-10-16"
title: "What is KRaft, and why did Kafka 4.0 remove ZooKeeper?"
seoTitle: "Kafka KRaft vs ZooKeeper: Interview Answer"
description: "Kafka KRaft vs ZooKeeper: how the Raft metadata quorum works, how many controllers to run, what changed in Kafka 4.x, and how ZooKeeper clusters migrate."
technology: ["kafka"]
topic: ["kraft", "replication", "operations"]
difficulty: "Medium"
questionType: ["conceptual", "architecture"]
estimatedMinutes: 8
interviewRelevance: "High"
shortAnswer: "KRaft (Kafka Raft) is Kafka's built-in metadata layer. Cluster metadata (brokers, topics, partition leaders, ISRs, configs, ACLs) is stored as an event log, the __cluster_metadata topic, replicated with the Raft consensus protocol across a small quorum of controller nodes, usually 3 or 5. One controller is active and the others are hot standbys that already hold the log; brokers fetch the log and apply it. Before KRaft, metadata lived in a separate ZooKeeper ensemble and a broker-controller reloaded it all on failover. Kafka 4.0 removed ZooKeeper mode completely: one system to run and secure, faster controller failover, and room for many more partitions. A ZooKeeper cluster must be migrated to KRaft on a 3.x bridge release (3.9 is the last) before it can upgrade to 4.x."
followUps: ["Why should you run 3 or 5 controllers and not 4?", "What is the difference between combined and dedicated controller mode?", "What does kafka-storage.sh format do, and why is it required?", "How does a ZooKeeper-based cluster get to Kafka 4.x?", "What is a dynamic KRaft quorum?"]
related: ["articles:kafka/replication-isr", "articles:kafka/topics-partitions-consumer-groups", "interview-questions:kafka/acks-min-insync-replicas-durability", "system-designs:kafka-ingestion-system"]
versionContext: "Apache Kafka 4.3.1. The CLI output was produced on a single-node 4.3.1 KRaft server in combined mode (Java 21); the configuration and multi-controller commands follow the 4.3 KRaft operations documentation. The quorum calculation runs on plain Python 3."
sources:
  - { label: "Apache Kafka documentation: KRaft operations", url: "https://kafka.apache.org/43/operations/kraft/" }
  - { label: "Apache Kafka documentation: Upgrading to 4.3", url: "https://kafka.apache.org/43/getting-started/upgrade/" }
  - { label: "Apache Kafka 4.0.0 release announcement", url: "https://kafka.apache.org/blog/2025/03/18/apache-kafka-4.0.0-release-announcement/" }
---

## Detailed explanation

### What the metadata layer does

Every Kafka cluster needs one authoritative answer to questions such as "which broker leads `orders-3`?", "which replicas are in sync?" and "what is this topic's retention?". Some component must store that metadata, elect partition leaders when brokers fail, and tell every broker about changes.

| | ZooKeeper mode (up to 3.x) | KRaft (the only mode from 4.0) |
|---|---|---|
| Where metadata lives | An external ZooKeeper ensemble | The internal `__cluster_metadata` log, replicated by Raft |
| Who decides | One broker elected as controller | The active controller of the KRaft quorum |
| Controller failover | The new controller reads all metadata back from ZooKeeper before it can act | A standby controller already has the log and takes over quickly |
| How brokers learn changes | The controller pushes updates to each broker | Brokers fetch the metadata log, like followers fetching a partition |
| Systems to deploy, secure and monitor | Kafka and ZooKeeper | Kafka only |
| First start | Storage directories auto-formatted | `kafka-storage.sh format` with a cluster ID |

### How KRaft works

- Servers take roles with `process.roles`: `controller`, `broker`, or `broker,controller` (**combined** mode). Combined mode is fine for development and small setups; the documentation does not recommend it for critical deployments, because controllers cannot be rolled or scaled separately from brokers.
- The controllers run Raft on the metadata log. A **majority** must be alive to elect a leader and commit metadata changes, so 3 controllers tolerate 1 failure and 5 tolerate 2.
- The metadata log uses the same ideas as partition replication: a leader, an epoch, a high watermark and followers that fetch. Brokers are observers of that log.
- Every node finds the quorum with `controller.quorum.bootstrap.servers`. That setting belongs to **dynamic quorums** (KIP-853, `kraft.version=1`), where controllers can be added and removed without rebuilding the cluster. Older static quorums list every voter in `controller.quorum.voters`; since 4.1 a static quorum can be upgraded to a dynamic one.
- Storage must be formatted once per node before the first start. Formatting writes the cluster ID and the initial voter set, which prevents a node with an empty disk from silently joining (or electing a leader for) the wrong metadata.

## Example

A single-node combined server, the setup in the [topics lesson](/kafka/topics-partitions-consumer-groups/), on Kafka 4.3.1:

<!-- noexec -->
```bash
# config/server.properties (the parts that matter for KRaft)
process.roles=broker,controller
node.id=1
listeners=PLAINTEXT://:9092,CONTROLLER://:9093
controller.listener.names=CONTROLLER
controller.quorum.bootstrap.servers=localhost:9093

KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
bin/kafka-storage.sh format --standalone -t "$KAFKA_CLUSTER_ID" -c config/server.properties
bin/kafka-server-start.sh config/server.properties

bin/kafka-metadata-quorum.sh --bootstrap-server localhost:9092 describe --status
```

```text
ClusterId:              w9MePSkfRXy6mXPqlV3dqw
LeaderId:               1
LeaderEpoch:            2
HighWatermark:          15131
MaxFollowerLag:         0
MaxFollowerLagTimeMs:   0
CurrentVoters:          [{"id": 1, "directoryId": "w973K3YDSKmd50haZ4MH0w", "endpoints": ["CONTROLLER://localhost:9093"]}]
CurrentObservers:       []
```

The output reads like partition replication: the metadata log has a leader (`LeaderId`), a leader epoch and a high watermark. `--standalone` bootstraps a quorum with one voter; for a production cluster you either start that way and add controllers dynamically, or format each controller with the same `--initial-controllers` list. `kafka-features.sh describe` shows `kraft.version` (1 means a dynamic quorum) next to the other feature flags such as `group.version` and `share.version`.

How many controller failures a quorum survives, and why even numbers are wasted:

```python
def quorum(controllers: int) -> dict:
    majority = controllers // 2 + 1
    return {"controllers": controllers, "majority": majority, "can lose": controllers - majority}

for n in (1, 2, 3, 4, 5):
    print(quorum(n))

assert quorum(4)["can lose"] == quorum(3)["can lose"]   # a fourth controller adds cost, not safety
```

```text
{'controllers': 1, 'majority': 1, 'can lose': 0}
{'controllers': 2, 'majority': 2, 'can lose': 0}
{'controllers': 3, 'majority': 2, 'can lose': 1}
{'controllers': 4, 'majority': 3, 'can lose': 1}
{'controllers': 5, 'majority': 3, 'can lose': 2}
```

## What happens when controllers fail

- **One of three controllers down**: the other two still form a majority; if the active one failed, a standby is elected and carries on. Brokers keep serving clients throughout.
- **Two of three down**: no majority, so no metadata changes: no new topics, no leader elections, no ISR updates. Brokers generally keep serving clients with the metadata they already have, but a broker failure can no longer be repaired by electing new leaders. Restore a controller quickly.
- **Spread controllers across failure zones** (one per availability zone for three controllers), the same reasoning as rack-aware replicas in the [replication lesson](/kafka/replication-isr/).

## Migrating from ZooKeeper

A ZooKeeper-mode cluster cannot jump straight to 4.x. The supported path is to upgrade to a 3.x bridge release (3.9 is the last), run the ZooKeeper-to-KRaft migration there (provision KRaft controllers in migration mode, roll brokers to KRaft, then finalise), and only then upgrade to 4.x. The 4.x upgrade notes also require the cluster's metadata version to be at least 3.3, the first release where KRaft was production-ready.

## Trade-offs and pitfalls

- **Even controller counts** cost a node without adding tolerance: four controllers survive one failure, like three.
- **Combined mode in production** couples controller and broker load; a busy broker can slow metadata handling, and you cannot roll them separately.
- **Old runbooks** that pass `--zookeeper` to tools no longer work; every tool takes `--bootstrap-server` (or `--bootstrap-controller` to talk to controllers directly).
- **Formatting is not optional.** A node that was never formatted refuses to start, and reformatting a controller's metadata directory is a recovery operation, not a fix for start-up errors.
- **KRaft is not the data path.** It stores metadata only. Record durability still comes from replication factor, `min.insync.replicas` and `acks` (see [acks and min.insync.replicas](/interview/kafka/acks-min-insync-replicas-durability/)).

## Common mistakes

1. Saying Kafka 4.x "can still use ZooKeeper". It cannot; ZooKeeper mode was removed in 4.0.
2. Confusing the controller quorum with partition replication: losing a controller majority stops metadata changes, losing an ISR majority stops writes to that partition.
3. Running two controllers "for high availability"; two need both alive, so they tolerate no failure.
