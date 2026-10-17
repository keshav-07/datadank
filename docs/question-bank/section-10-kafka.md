## Section 10: Apache Kafka

### Q469. Walk through the life of one record from producer to consumer
**ID:** KFK-01
**Topic:** Kafka > Architecture
**Level:** Beginner
**Frequency:** Very Common
**Tags:** `kafka`, `architecture`, `producer`, `consumer`, `high-watermark`

**Short Answer (2–3 lines):**
The producer serialises the record, picks a partition, and adds it to an in-memory batch. A background sender ships the batch to that partition's leader broker. The leader appends it to its log, followers copy it, and the leader acknowledges once enough replicas have it. A consumer in a group then fetches from the leader up to the high watermark and later commits its offset.

**Detailed Explanation:**
Interviewers use this question to see whether you can connect the pieces covered in Q67 into one flow.

1. **Metadata.** On start-up the client contacts any `bootstrap.servers` broker. It learns which broker leads each partition and caches that map, refreshing it when leadership moves.
2. **Serialise and partition.** Key and value become bytes (Avro, JSON, Protobuf). The partitioner hashes the key, or uses a sticky choice when there's no key (see Q472).
3. **Accumulate.** The record joins a per-partition batch in the producer's buffer. The batch is sent when it reaches `batch.size` or `linger.ms` expires (see Q475).
4. **Produce request.** The sender thread groups batches by leader broker and sends one request per broker. With idempotence on (the default), each batch carries a producer ID and sequence number.
5. **Append and replicate.** The leader writes the batch to the active segment of the partition log (page cache, not a forced fsync). Followers fetch it just as consumers do.
6. **Acknowledge.** With `acks=all`, the leader replies once every in-sync replica has the batch and the ISR is at least `min.insync.replicas`. The high watermark (HW) advances to the last offset that all ISR members hold.
7. **Consume.** A group consumer's `poll()` sends fetch requests. The broker returns data only up to the HW, so consumers never read records that could vanish after a leader failover. `read_committed` consumers stop at the last stable offset instead (see Q70).
8. **Commit.** The consumer commits "next offset to read" to `__consumer_offsets`. The record stays on disk until retention removes it, whether or not anyone read it.

**Example / Code:**
```python
# Runtime: Python 3.11, confluent-kafka 2.16, Apache Kafka 4.3 broker on localhost:19092
import json
from confluent_kafka import Producer, Consumer

p = Producer({"bootstrap.servers": "localhost:19092", "acks": "all", "linger.ms": 5})

def on_delivery(err, msg):
    if err:
        print("FAILED", err)
    else:
        print(f"acked key={msg.key().decode()} partition={msg.partition()} offset={msg.offset()}")

for i, acct in enumerate(["acc-1", "acc-2", "acc-1"]):
    p.produce("payments", key=acct, value=json.dumps({"seq": i, "amount": 10 * i}),
              on_delivery=on_delivery)
p.flush(10)   # produce() is async; flush waits for every acknowledgement

c = Consumer({"bootstrap.servers": "localhost:19092", "group.id": "lifecycle-demo",
              "auto.offset.reset": "earliest"})
c.subscribe(["payments"])
seen = 0
while seen < 3:
    m = c.poll(5.0)
    if m is None or m.error():
        continue
    seen += 1
    print(f"read  key={m.key().decode()} partition={m.partition()} offset={m.offset()} value={m.value().decode()}")
c.close()   # commits final offsets (auto-commit) and leaves the group
```
Output against a 3-partition `payments` topic:
```text
acked key=acc-1 partition=2 offset=0
acked key=acc-1 partition=2 offset=1
acked key=acc-2 partition=0 offset=0
read  key=acc-1 partition=2 offset=0 value={"seq": 0, "amount": 0}
read  key=acc-1 partition=2 offset=1 value={"seq": 2, "amount": 20}
read  key=acc-2 partition=0 offset=0 value={"seq": 1, "amount": 10}
```
Both `acc-1` records share a partition and keep their order. Across partitions the consumer saw them in a different order from the one they were sent in.

**Real-World Use Case:**
A payments team saw "acknowledged" events missing from a downstream report. Walking through the flow showed the producer used `acks=1`. A leader crashed after acknowledging but before followers fetched, so those records were never replicated. Switching to `acks=all` with `min.insync.replicas=2` closed the gap.

**Common Mistakes / Interviewer Red Flags:**
- Saying the producer writes to "the topic" or to any broker; it writes to the partition leader.
- Thinking each `send()` is one network request, which ignores batching.
- Believing consumers can read a record as soon as the leader writes it, before the high watermark moves.
- Thinking records are deleted once consumed.

**Follow-up Questions Interviewers Ask:**
- Does Kafka fsync every write? — No; it relies on replication plus the OS page cache, and flush settings are left at their defaults in most deployments.
- What does a consumer commit? — The offset of the *next* record to read, per partition, stored in `__consumer_offsets`.
- Where does the consumer get partition leaders from? — From cluster metadata returned by any broker, refreshed on errors such as `NOT_LEADER_OR_FOLLOWER`.

---

### Q470. Running KRaft: controllers, quorum sizing and metadata versions
**ID:** KFK-02
**Topic:** Kafka > Architecture (KRaft)
**Level:** Intermediate
**Frequency:** Common
**Tags:** `kafka`, `kraft`, `controller`, `metadata`, `upgrades`

**Short Answer (2–3 lines):**
In KRaft, a small set of controller nodes runs a Raft quorum that stores all cluster metadata in the `__cluster_metadata` log. One of them is the active controller. Use three controllers (or five for very large clusters), on dedicated nodes in production. Upgrades are two-step: roll the binaries, then raise `metadata.version` with `kafka-features.sh`.

**Detailed Explanation:**
Why ZooKeeper went away is a separate question (it has its own page on this site). This one is about running KRaft day to day.

**Roles.** `process.roles` can be `broker`, `controller`, or `broker,controller` (combined). Combined mode suits development and small clusters. Production clusters use dedicated controllers, so a broker under heavy load can't starve the quorum.

**Quorum sizing.** Raft needs a majority. Three voters survive one failure and five survive two. An even number adds no extra tolerance. Spread voters across racks or availability zones.

**Metadata log.** Every change (topic created, ISR shrunk, ACL added) is a record in `__cluster_metadata`. Brokers replicate this log and apply it locally, so a failover means a new leader is elected among voters who already have the metadata. Nothing has to be reloaded, which is why failover is fast. Snapshots keep the log bounded.

**Static vs dynamic quorum.** Older setups listed voters in `controller.quorum.voters`. Newer ones use `controller.quorum.bootstrap.servers` and format with `--standalone` or `--initial-controllers`. Voters can then be added or removed at runtime (KIP-853).

**Feature versions.** `metadata.version` and feature flags such as `group.version`, `transaction.version`, `share.version` and `streams.version` turn on new protocols. Clusters freshly formatted with 4.3 enable them all, as the output below shows. An upgraded cluster must finalise them explicitly. Product behaviour changes; checked October 2026 against the Apache Kafka 4.3 docs.

**Example / Code:**
```bash
# Runtime: Apache Kafka 4.3.1, single combined-mode node (lab only)
bin/kafka-storage.sh format -t "$(bin/kafka-storage.sh random-uuid)" \
  -c server.properties --standalone
bin/kafka-server-start.sh server.properties &

bin/kafka-metadata-quorum.sh --bootstrap-server localhost:19092 describe --status
bin/kafka-features.sh --bootstrap-server localhost:19092 describe
# After a rolling upgrade of every node:
# bin/kafka-features.sh --bootstrap-server localhost:19092 upgrade --release-version 4.3
```
Abridged output:
```text
ClusterId:              k823IPTOTlqVLpX-4sSHCg
LeaderId:               1
LeaderEpoch:            1
HighWatermark:          22
MaxFollowerLag:         0
CurrentVoters:          [{"id": 1, "directoryId": "z8TbHZkrQJKUlHkNa007Ag", "endpoints": ["CONTROLLER://localhost:19093"]}]
Feature: metadata.version   SupportedMinVersion: 3.3-IV3  SupportedMaxVersion: 4.3-IV0  FinalizedVersionLevel: 4.3-IV0
Feature: share.version      SupportedMinVersion: 0        SupportedMaxVersion: 1        FinalizedVersionLevel: 1
Feature: streams.version    SupportedMinVersion: 0        SupportedMaxVersion: 1        FinalizedVersionLevel: 1
```

**Real-World Use Case:**
A team running combined nodes saw controller elections whenever a broker garbage-collected under heavy produce load. They moved to three small dedicated controller nodes in different zones, and the elections stopped.

**Common Mistakes / Interviewer Red Flags:**
- Proposing two or four controllers.
- Forgetting the `kafka-features.sh upgrade` step, then wondering why new protocols are unavailable after an upgrade.
- Assuming a 3.x ZooKeeper cluster can jump straight to 4.x; it must migrate to KRaft first, ideally on 3.9, the bridge release.
- Expecting `metadata.version` downgrades to always work; releases with metadata changes can't be downgraded.

**Follow-up Questions Interviewers Ask:**
- What does `MaxFollowerLag` tell you? — How far the slowest voter or observer trails the metadata leader; growth means a controller or broker is struggling.
- Can a broker serve traffic if the quorum loses its majority? — Existing partitions keep serving, but no metadata changes (leader elections, topic creation) can happen.

---

### Q471. Create and configure a topic from the CLI
**ID:** KFK-03
**Topic:** Kafka > Topics & Partitions
**Level:** Beginner
**Frequency:** Very Common
**Tags:** `kafka`, `topics`, `cli`, `configuration`

**Short Answer (2–3 lines):**
Use `kafka-topics.sh --create` with a name, partition count and replication factor, plus any `--config` overrides. Change settings later with `kafka-configs.sh --alter`. Topic-level settings such as `retention.ms` override broker defaults such as `log.retention.hours`. In production, disable auto-creation and manage topics as code.

**Detailed Explanation:**
Creating a topic means making three kinds of decisions:

| Decision | Flag or config | Can change later? |
|---|---|---|
| Partitions | `--partitions` | Increase only (see Q474) |
| Replication factor | `--replication-factor` | Yes, but only with a reassignment plan |
| Retention, cleanup, size limits | `--config retention.ms=…`, `cleanup.policy`, `max.message.bytes` | Yes, dynamically |
| Durability | `min.insync.replicas` | Yes, dynamically |

**Config precedence.** Topic override, then the broker's dynamic default (cluster-wide), then `server.properties`, then the built-in default. `kafka-configs.sh --describe` shows the "synonyms" chain, so you can see where each effective value comes from.

**Names differ between layers.** The topic setting `max.message.bytes` corresponds to the broker setting `message.max.bytes`, and `retention.ms` corresponds to `log.retention.ms`. Candidates mix these up often.

**Auto-creation.** `auto.create.topics.enable` is still `true` by default, with `num.partitions=1` and `default.replication.factor=1`. A typo in a producer's topic name then silently creates a one-partition, unreplicated topic. Turn it off and create topics through Terraform, a GitOps job or an operator such as Strimzi.

**Naming.** Pick a convention such as `<domain>.<entity>.<event>.v<major>` (see Q517). Avoid mixing `.` and `_`, because metric names treat them as the same character.

**Example / Code:**
```bash
# Runtime: Apache Kafka 4.3.1 CLI against a local broker
B=localhost:19092
bin/kafka-topics.sh --bootstrap-server $B --create --topic orders \
  --partitions 6 --replication-factor 1 \
  --config retention.ms=259200000 --config cleanup.policy=delete
bin/kafka-topics.sh --bootstrap-server $B --describe --topic orders

bin/kafka-configs.sh --bootstrap-server $B --alter --entity-type topics --entity-name orders \
  --add-config retention.ms=604800000,max.message.bytes=2097152
bin/kafka-configs.sh --bootstrap-server $B --describe --entity-type topics --entity-name orders
```
Output (abridged):
```text
Created topic orders.
Topic: orders	TopicId: U9XPxNLBRkKoSm5ODJFMyQ	PartitionCount: 6	ReplicationFactor: 1	Configs: min.insync.replicas=1,cleanup.policy=delete,retention.ms=259200000
	Topic: orders	Partition: 0	Leader: 1	Replicas: 1	Isr: 1	Elr: 	LastKnownElr: 
Completed updating config for topic orders.
  max.message.bytes=2097152 sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:max.message.bytes=2097152, DEFAULT_CONFIG:message.max.bytes=1048588}
  retention.ms=604800000 sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:retention.ms=604800000}
```
(Replication factor 1 is only for this single-node lab. Use 3 in production.)

**Real-World Use Case:**
A data platform team kept topic definitions in a YAML file per domain. CI ran a plan that showed partition and config changes, and refused any decrease in partitions or replication factor before applying.

**Common Mistakes / Interviewer Red Flags:**
- Leaving auto-creation on in production.
- Setting broker-level properties on a topic, or the reverse.
- Creating production topics with replication factor 1.
- Using `--alter --config` on `kafka-topics.sh`; topic configs are changed with `kafka-configs.sh`.

**Follow-up Questions Interviewers Ask:**
- How do you delete one override? — `kafka-configs.sh --alter --delete-config retention.ms`; since 4.3 this is a no-op if the key isn't set.
- How do you raise the replication factor? — Generate a JSON plan listing the new replica sets and run `kafka-reassign-partitions.sh --execute`, with throttles.
- Where is `TopicId` used? — It is a unique ID that tells a deleted topic apart from a recreated one with the same name.

---

### Q472. How the producer picks a partition, and why Python and Java can disagree
**ID:** KFK-04
**Topic:** Kafka > Topics & Partitions
**Level:** Intermediate
**Frequency:** Common
**Tags:** `kafka`, `partitioner`, `murmur2`, `librdkafka`, `keys`

**Short Answer (2–3 lines):**
With a key, the Java client uses `murmur2(key) % partitions`. Without a key, it fills a "sticky" batch for one partition before moving on, preferring faster brokers. librdkafka-based clients (Python `confluent-kafka`, Go, .NET) default to a CRC32-based `consistent_random` partitioner. So the same key can land in different partitions unless you set `partitioner=murmur2_random`.

**Detailed Explanation:**
**Java default (Kafka 3.3+)**
- Keyed record: `toPositive(murmur2(keyBytes)) % numPartitions`. Same key, same partition, as long as the partition count doesn't change.
- Null key: the built-in partitioner sticks to one partition until a batch fills, then switches. `partitioner.adaptive.partitioning.enable=true` (the default) sends fewer records to brokers that are responding slowly. The old `DefaultPartitioner` and `UniformStickyPartitioner` classes are deprecated.
- `partitioner.ignore.keys=true` ignores keys and spreads records evenly. Use it only when per-key ordering doesn't matter.

**librdkafka default**
- `partitioner=consistent_random` hashes keys with CRC32, and spreads null keys randomly.
- `murmur2_random` matches the Java client for keyed records.

**Why it matters.** Kafka Streams, ksqlDB, Connect and Java services all assume murmur2. Suppose a Python service writes `customer_id`-keyed records and a Java service writes the same keys to the same topic. Their records for one customer end up in different partitions, which breaks per-key ordering and joins. Co-partitioning for stream-table joins (see Q508) quietly fails too.

**Custom partitioners** are possible: implement `Partitioner` in Java, or specify the partition explicitly in `produce()`. They are hard to keep consistent across languages, so prefer choosing a good key (see Q473).

**Example / Code:**
```python
# Runtime: Python 3.11, confluent-kafka 2.16 (librdkafka 2.16), Apache Kafka 4.3, 6-partition topic "orders"
from confluent_kafka import Producer

def where(partitioner, keys):
    p = Producer({"bootstrap.servers": "localhost:19092", "partitioner": partitioner})
    out = {}
    def cb(err, msg):
        out[msg.key().decode()] = msg.partition()
    for k in keys:
        p.produce("orders", key=k, value=b"{}", on_delivery=cb)
    p.flush()
    return out

keys = ["cust-17", "cust-42", "cust-99"]
print("librdkafka default (consistent_random, CRC32):", where("consistent_random", keys))
print("Java-compatible   (murmur2_random)         :", where("murmur2_random", keys))
```
```text
librdkafka default (consistent_random, CRC32): {'cust-42': 4, 'cust-99': 5, 'cust-17': 2}
Java-compatible   (murmur2_random)         : {'cust-17': 1, 'cust-42': 2, 'cust-99': 2}
```
The same three keys written with the Java `kafka-console-producer.sh` (`--reader-property parse.key=true`) landed in partitions 1, 2 and 2, which matches `murmur2_random`.

**Real-World Use Case:**
A team rewrote a Java ingestion service in Python. A downstream Kafka Streams join started missing most matches. The new producer used librdkafka's CRC32 default, and setting `partitioner=murmur2_random` fixed it.

**Common Mistakes / Interviewer Red Flags:**
- Saying null-key records are always sent round-robin, one record per partition.
- Assuming every client library uses the same hash.
- Hashing on a value that changes for the same entity, such as an email address that can be edited.

**Follow-up Questions Interviewers Ask:**
- Why did Kafka move to sticky partitioning for null keys? — Larger batches, fewer requests and lower latency than spreading every record.
- Can you send to a specific partition? — Yes, pass `partition=` explicitly; you then own the balancing logic.
- What happens to key placement when partitions are added? — Most keys move (see Q474).

---

### Q473. What makes a good message key?
**ID:** KFK-05
**Topic:** Kafka > Topics & Partitions
**Level:** Beginner
**Frequency:** Very Common
**Tags:** `kafka`, `keys`, `ordering`, `compaction`, `data-skew`

**Short Answer (2–3 lines):**
The key should be the entity whose events must stay in order, or whose latest state you want kept, such as `order_id` or `account_id`. It should be stable, and have many values spread evenly. Use a null key only when ordering doesn't matter and you want even spread.

**Detailed Explanation:**
A key does three jobs at once:
1. **Routing and ordering.** Same key, same partition, so events for that key stay in order (see Q69).
2. **Compaction identity.** On compacted topics the key decides what counts as "the same row" (see Q71).
3. **Join co-partitioning.** Kafka Streams and Flink joins expect both sides keyed and partitioned the same way.

**A good key is:**
- **Aligned with the ordering need.** For order lifecycle events, `order_id`. For a ledger balance, `account_id` (all of an account's postings in sequence).
- **High-cardinality and roughly uniform.** `country` puts most traffic on a handful of partitions. Skewed keys create hot partitions (see Q438).
- **Stable.** Use an immutable ID, not a mutable attribute.
- **Small.** Keys are stored with every record and hashed on every send.

**Trade-offs to discuss**
| Key choice | Ordering scope | Skew risk |
|---|---|---|
| `null` | None | Lowest |
| `user_id` | Per user | Bots and heavy users |
| `tenant_id` | Per tenant | High when one tenant is very large |
| `tenant_id:user_id` | Per user within a tenant | Low |

**Composite keys** keep ordering for the smallest unit that needs it. If one tenant is 40% of traffic, `tenant_id` alone is a bad key, while `tenant_id:order_id` spreads load and still keeps each order in sequence.

**Keys vs headers.** Put routing and identity in the key. Put metadata such as trace IDs, schema hints and source system in headers, so the key stays meaningful for compaction.

**Example / Code:**
```python
# Runtime: Python 3.11, confluent-kafka 2.16
from confluent_kafka import Producer

p = Producer({"bootstrap.servers": "localhost:19092", "partitioner": "murmur2_random"})
event = {"tenant": "t-9", "order_id": "o-1001", "status": "PAID"}

# Order-level ordering, spread across partitions even for a huge tenant
key = f'{event["tenant"]}:{event["order_id"]}'
p.produce("orders", key=key, value=str(event),
          headers=[("trace_id", b"9f2c"), ("source", b"checkout")])
p.flush()
```

**Real-World Use Case:**
A marketplace keyed its `listing-events` topic by `seller_id`. One wholesale seller created a partition that lagged hours behind the rest. Re-keying to `listing_id` kept the guarantee that mattered (each listing's updates in order) and evened out the load.

**Common Mistakes / Interviewer Red Flags:**
- Using a timestamp or random UUID as the key on a compacted topic, which disables compaction in practice.
- Choosing low-cardinality keys such as status or country.
- Re-keying a live topic without planning a cut-over for consumers that depend on per-key order.

**Follow-up Questions Interviewers Ask:**
- How do you keep order when you must re-key? — Write to a new topic with the new key, drain the old topic, then switch consumers over.
- What if one key is hot but must stay ordered? — Scale that consumer vertically, or split the entity's events into sub-streams that don't need ordering between them.
- Does Kafka enforce key uniqueness? — No; a key is just bytes, and uniqueness only matters for compaction semantics.

---
