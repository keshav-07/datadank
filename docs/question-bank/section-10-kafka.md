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

**Why it matters.** Kafka Streams, ksqlDB, Connect and Java services all assume murmur2. Suppose a Python service writes `customer_id`-keyed records and a Java service writes the same keys to the same topic. Their records for one customer end up in different partitions, which breaks per-key ordering and joins. Co-partitioning for stream-table joins (see Q507) quietly fails too.

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

### Q474. Can you change a topic's partition count, and what breaks when you do?
**ID:** KFK-06
**Topic:** Kafka > Topics & Partitions
**Level:** Beginner
**Frequency:** Very Common
**Tags:** `kafka`, `partitions`, `scaling`, `keys`, `ordering`

**Short Answer (2–3 lines):**
You can only increase the partition count; Kafka refuses a decrease. Adding partitions changes `hash(key) % N`, so most keys start landing in a different partition. Per-key ordering breaks across the change, and compacted topics or keyed state can end up with a key's old and new values in two places. Existing data is never moved.

**Detailed Explanation:**
**What happens on increase**
- New, empty partitions are created and assigned to brokers. Existing records stay where they are.
- Producers notice on their next metadata refresh (`metadata.max.age.ms`, 5 minutes by default) and start routing keys by the new modulo.
- Consumer groups rebalance to pick up the new partitions. With `auto.offset.reset=latest`, records written to a new partition *before* the group gets its first assignment can be skipped. The consumer config docs warn about exactly this.

**What breaks**
| Concern | Effect |
|---|---|
| Per-key order | Old events for key K are in partition 0 and new ones in partition 3, and consumers may process them in either order |
| Compacted topics | Two "latest" values for K in different partitions; readers can see a stale one |
| Kafka Streams state | Stores are partitioned by input partition, so keys are now in the wrong task. Treat it as an application reset |
| Join co-partitioning | Both sides of a join must have equal partition counts |

**Why no decrease?** Records in a removed partition would have nowhere to go without rewriting offsets, and offsets are the contract consumers rely on.

**Safer patterns**
1. Over-provision at creation (see the partition-count page and Q510).
2. If you must grow a keyed topic: create `orders.v2` with more partitions, dual-write or replicate into it, and let consumers drain v1 before switching.
3. For unkeyed topics (logs, clicks), adding partitions in place is usually fine.

**Example / Code:**
```bash
# Runtime: Apache Kafka 4.3.1 CLI
bin/kafka-topics.sh --bootstrap-server localhost:19092 --alter --topic payments --partitions 2
# Error while executing topic command : The topic payments currently has 3 partition(s); 2 would not be an increase.
bin/kafka-topics.sh --bootstrap-server localhost:19092 --alter --topic payments --partitions 6
```
```python
# Runtime: Python 3.11, confluent-kafka 2.16, Apache Kafka 4.3 (topic "payments" grown from 3 to 6 partitions)
from confluent_kafka import Producer

def murmur2(data: bytes) -> int:
    """Kafka's Java murmur2 (same as Utils.murmur2), returns a signed 32-bit int."""
    m, r, length = 0x5BD1E995, 24, len(data)
    h = (0x9747B28C ^ length) & 0xFFFFFFFF
    for i in range(0, length - length % 4, 4):
        k = int.from_bytes(data[i:i + 4], "little")
        k = (k * m) & 0xFFFFFFFF; k ^= k >> r; k = (k * m) & 0xFFFFFFFF
        h = ((h * m) & 0xFFFFFFFF) ^ k
    tail = data[length - length % 4:]
    if len(tail) >= 3: h ^= tail[2] << 16
    if len(tail) >= 2: h ^= tail[1] << 8
    if len(tail) >= 1: h ^= tail[0]; h = (h * m) & 0xFFFFFFFF
    h ^= h >> 13; h = (h * m) & 0xFFFFFFFF; h ^= h >> 15
    return h - (1 << 32) if h >= 1 << 31 else h

def partition_for(key: str, n: int) -> int:
    return (murmur2(key.encode()) & 0x7FFFFFFF) % n   # toPositive(hash) % numPartitions

p = Producer({"bootstrap.servers": "localhost:19092", "partitioner": "murmur2_random"})
actual = {}
for k in ["acc-1", "acc-2", "acc-3", "acc-4"]:
    p.produce("payments", key=k, value=b"{}", on_delivery=lambda e, m: actual.update({m.key().decode(): m.partition()}))
p.flush()
for k in sorted(actual):
    print(f"{k}: with 3 partitions -> {partition_for(k, 3)}, with 6 -> {partition_for(k, 6)}, broker stored it in {actual[k]}")
```
```text
acc-1: with 3 partitions -> 0, with 6 -> 3, broker stored it in 3
acc-2: with 3 partitions -> 2, with 6 -> 2, broker stored it in 2
acc-3: with 3 partitions -> 2, with 6 -> 5, broker stored it in 5
acc-4: with 3 partitions -> 2, with 6 -> 5, broker stored it in 5
```
Three of the four keys moved partition, so their old and new events now live in different logs.

**Real-World Use Case:**
A team doubled partitions on a compacted `customer-profile` topic to fix lag. A downstream cache rebuilt from the topic began serving old addresses for some customers, because two "latest" records now existed per key. They rebuilt into a new topic and re-pointed consumers.

**Common Mistakes / Interviewer Red Flags:**
- Saying "just add partitions" for lag on a keyed or compacted topic without mentioning remapping.
- Believing Kafka moves existing data into the new partitions.
- Forgetting that Kafka Streams apps reading the topic need a reset.

**Follow-up Questions Interviewers Ask:**
- Is moving partitions between brokers the same thing? — No; `kafka-reassign-partitions.sh` moves replicas but keeps the count and the key mapping.
- How do consumers notice new partitions? — Through metadata refresh, followed by a rebalance.
- Is there any way to reduce partitions? — Only by creating a new topic and migrating to it.

---

### Q475. Producer batching, linger.ms and compression: tuning for throughput
**ID:** KFK-07
**Topic:** Kafka > Producers
**Level:** Intermediate
**Frequency:** Very Common
**Tags:** `kafka`, `producer`, `batching`, `linger-ms`, `compression`

**Short Answer (2–3 lines):**
The producer groups records per partition into batches. A batch is sent when it reaches `batch.size` or when `linger.ms` expires, whichever comes first. Bigger batches mean fewer requests and much better compression, and compression is applied per batch. Kafka 4.0 changed the Java default `linger.ms` from 0 to 5. Raise `linger.ms` and `batch.size` and use `lz4` or `zstd` for high-volume topics.

**Detailed Explanation:**
**The knobs (Java defaults, Kafka 4.3)**
| Setting | Default | Effect |
|---|---|---|
| `batch.size` | 16384 bytes | Upper bound per partition batch |
| `linger.ms` | 5 | Maximum wait to fill a batch |
| `buffer.memory` | 32 MB | Total memory for unsent records; when it's full, `send()` blocks up to `max.block.ms` |
| `compression.type` | `none` | `gzip`, `snappy`, `lz4`, `zstd` |
| `max.request.size` | 1 MB | Largest request, and so the largest single record |

librdkafka (Python, Go, .NET) uses the same names with different defaults: `linger.ms=5`, `batch.size=1000000`, `batch.num.messages=10000`.

**Why lingering can lower latency.** Under load, records queue behind in-flight requests anyway. Bigger batches mean fewer requests, less broker CPU and shorter queues, so tail latency often falls. That's the reasoning behind the 4.0 default change, and the lab run below shows the same effect.

**Compression.** It happens in the producer, per batch. Brokers store the compressed batch as-is when the topic's `compression.type=producer` (the default), and consumers decompress. So compression saves network, disk and replication traffic together. Small batches compress badly, which is why batching and compression are tuned together.
- `lz4`: very fast and a good default.
- `zstd`: best ratio for a little more CPU. Good for cross-region links and long retention.
- `snappy`: older and similar to lz4.
- `gzip`: high CPU, rarely the right choice today.

Setting a topic-level `compression.type` that differs from the producer's codec forces the broker to recompress every batch, which costs broker CPU.

**Example / Code:**
```bash
# Runtime: Apache Kafka 4.3.1, kafka-producer-perf-test.sh, single local broker, 6 partitions,
# payload.jsonl = 2,000 sample JSON click events (~150 bytes each)
for cfg in "linger.ms=0 compression.type=none" "linger.ms=5 compression.type=none" \
           "linger.ms=20 batch.size=131072 compression.type=lz4" \
           "linger.ms=20 batch.size=131072 compression.type=zstd"; do
  echo "== $cfg"
  bin/kafka-producer-perf-test.sh --bootstrap-server localhost:19092 --topic perf \
    --num-records 300000 --throughput -1 --payload-file payload.jsonl \
    --command-property ${cfg// / --command-property } 2>&1 | tail -1
done
```
```text
== linger.ms=0 compression.type=none
300000 records sent, 163309.744148 records/sec (22.78 MB/sec), 275.02 ms avg latency, 510.00 ms max latency, 301 ms 50th, 490 ms 95th, 503 ms 99th, 509 ms 99.9th.
== linger.ms=5 compression.type=none
300000 records sent, 200534.759358 records/sec (27.97 MB/sec), 127.28 ms avg latency, 417.00 ms max latency, 98 ms 50th, 303 ms 95th, 323 ms 99th, 331 ms 99.9th.
== linger.ms=20 batch.size=131072 compression.type=lz4
300000 records sent, 203389.830508 records/sec (28.37 MB/sec), 20.29 ms avg latency, 494.00 ms max latency, 18 ms 50th, 41 ms 95th, 73 ms 99th, 91 ms 99.9th.
== linger.ms=20 batch.size=131072 compression.type=zstd
300000 records sent, 190355.329949 records/sec (26.55 MB/sec), 44.56 ms avg latency, 548.00 ms max latency, 12 ms 50th, 183 ms 95th, 201 ms 99th, 211 ms 99.9th.
```
Writing 100,000 of the same records to single-partition topics, one per codec, then checking `kafka-log-dirs.sh --describe` gave: none 15.62 MB, snappy 3.78 MB, lz4 3.81 MB, zstd 2.12 MB. These are lab numbers on one laptop-class node, so measure your own payloads.

**Real-World Use Case:**
A clickstream producer sent more than 100k small JSON events per second with `linger.ms=0`. Brokers were CPU-bound on request handling. Moving to `linger.ms=20`, `batch.size=256KB` and `lz4` cut request rate and network egress, and p99 latency dropped as well.

**Common Mistakes / Interviewer Red Flags:**
- Assuming `linger.ms` always adds that much latency to every record.
- Compressing at the broker (a topic `compression.type` that differs from the producer's) and burning broker CPU.
- Raising `batch.size` without also raising `buffer.memory` when there are many partitions.

**Follow-up Questions Interviewers Ask:**
- How do you see actual batch sizes? — Producer metrics `batch-size-avg`, `records-per-request-avg` and `compression-rate-avg`.
- What happens when `buffer.memory` is exhausted? — `send()` blocks for up to `max.block.ms`, then throws a `TimeoutException`.
- Does compression change offsets? — No; each record keeps its own offset inside the compressed batch.

---

### Q476. Producer retries, timeouts and error handling
**ID:** KFK-08
**Topic:** Kafka > Producers
**Level:** Intermediate
**Frequency:** Common
**Tags:** `kafka`, `producer`, `retries`, `delivery-timeout`, `error-handling`

**Short Answer (2–3 lines):**
Modern producers retry transient errors, such as leader changes and `NOT_ENOUGH_REPLICAS`, almost indefinitely (`retries=2147483647`). The real limit is `delivery.timeout.ms` (2 minutes in Java), the total time from `send()` to success or failure. Non-retriable errors, such as a record that's too large or an authorisation failure, fail fast. Your code must handle both in the delivery callback, or the record is lost silently.

**Detailed Explanation:**
**Timeline of one record (Java defaults)**
- `max.block.ms` (60 s): how long `send()` itself may block waiting for metadata or buffer space.
- `linger.ms` (5): time spent waiting in a batch.
- `request.timeout.ms` (30 s): how long to wait for each broker response.
- `retry.backoff.ms` (100) growing to `retry.backoff.max.ms` (1 s): exponential backoff between retries.
- `delivery.timeout.ms` (120 s): an overall cap that must be at least `linger.ms + request.timeout.ms`.

In librdkafka the overall cap is `message.timeout.ms` (alias `delivery.timeout.ms`) and defaults to 300 s.

**Error classes**
| Retriable (the client handles it) | Non-retriable (your code must) |
|---|---|
| `NOT_LEADER_OR_FOLLOWER`, `NOT_ENOUGH_REPLICAS`, `REQUEST_TIMED_OUT`, network errors | `RECORD_TOO_LARGE` / `MSG_SIZE_TOO_LARGE`, `TOPIC_AUTHORIZATION_FAILED`, serialisation errors, `INVALID_RECORD` |

**What your code should do**
- Always pass a callback (Java `send(record, callback)`, Python `on_delivery=`). An ignored future means silent loss.
- On final failure: log, raise a metric, and either stop the application (for critical data) or write the record to a local spool or fallback store so it can be replayed.
- Don't wrap `send()` in your own retry loop on top of client retries. It duplicates records and breaks the ordering guarantees that idempotence gives you (see Q477).
- Keep `retries` at its default and tune `delivery.timeout.ms` instead, as the Kafka docs recommend.

**Example / Code:**
```python
# Runtime: Python 3.11, confluent-kafka 2.16, Apache Kafka 4.3
# Setup: topic "small-msgs" created with --config max.message.bytes=1000
import time
from confluent_kafka import Producer

def send(bootstrap, topic, value):
    p = Producer({"bootstrap.servers": bootstrap, "acks": "all",
                  "delivery.timeout.ms": 5000,   # total budget for send + all retries
                  "retry.backoff.ms": 200})
    start = time.time()
    def on_delivery(err, msg):
        took = time.time() - start
        if err is None:
            print(f"{topic}: delivered after {took:.1f}s")
        else:
            print(f"{topic}: failed after {took:.1f}s -> {err.name()} (retriable={err.retriable()})")
    p.produce(topic, value=value, on_delivery=on_delivery)
    p.flush(15)

send("localhost:19092", "small-msgs", b"x" * 500)    # fits
send("localhost:19092", "small-msgs", b"x" * 5000)   # too big for the topic: retrying can't help
send("localhost:1", "orders", b"x")                  # no broker: retried until delivery.timeout.ms
```
```text
small-msgs: delivered after 0.0s
small-msgs: failed after 0.0s -> MSG_SIZE_TOO_LARGE (retriable=False)
orders: failed after 5.0s -> _MSG_TIMED_OUT (retriable=False)
```
The third record was retried internally for the whole 5-second budget, and only then reported as a final failure.

**Real-World Use Case:**
An order service used fire-and-forget sends with no callback. During a broker upgrade some batches expired after `delivery.timeout.ms`, and nobody noticed for a week. The fix was a callback that increments a "failed sends" metric and writes failed records to a local spool replayed on start-up.

**Common Mistakes / Interviewer Red Flags:**
- Setting `retries=0` "to avoid duplicates", when idempotence already removes them.
- Ignoring the future or callback returned by `send()`.
- Setting `delivery.timeout.ms` below `linger.ms + request.timeout.ms`, which the Java client rejects.
- Treating `TimeoutException` as proof the record was *not* written; it may have been.

**Follow-up Questions Interviewers Ask:**
- Can a timed-out record still be in Kafka? — Yes; the broker may have written it before the response was lost, so consumers should be idempotent anyway.
- What does `max.block.ms` protect? — The calling thread, from blocking forever when metadata is missing or the buffer is full.
- How do you surface producer health? — Metrics such as `record-error-rate`, `record-retry-rate` and `request-latency-avg`.

---

### Q477. max.in.flight.requests.per.connection, idempotence and ordering
**ID:** KFK-09
**Topic:** Kafka > Producers
**Level:** Intermediate
**Frequency:** Common
**Tags:** `kafka`, `producer`, `idempotence`, `ordering`, `max-in-flight`

**Short Answer (2–3 lines):**
`max.in.flight.requests.per.connection` is how many unacknowledged requests a producer may have open to one broker. Without idempotence, a value above 1 combined with retries can reorder records: batch 2 succeeds while batch 1 is being retried. With idempotence on, the broker uses sequence numbers to keep order for any value up to 5. Java enables idempotence by default; librdkafka does not.

**Detailed Explanation:**
**The reordering scenario (idempotence off)**
1. Batch A (offsets 1–100) and batch B (101–200) are both in flight.
2. A fails with a transient error, while B succeeds and is appended.
3. A is retried and appended after B, so the partition holds B before A.

The old fix was `max.in.flight=1`, which caps throughput at one round trip per batch.

**With idempotence.** Each batch carries a producer ID, an epoch and a per-partition sequence number. The broker remembers the last 5 batches per producer and partition. It rejects an out-of-order sequence so the client retries in order, and it discards duplicates. Hence the limit: `max.in.flight ≤ 5`.

**Defaults differ by client**
| Client | `enable.idempotence` default | `acks` default | `max.in.flight` default |
|---|---|---|---|
| Java (3.0+) | `true` (unless conflicting configs) | `all` | 5 |
| librdkafka 2.16 (`confluent-kafka`) | `false` | `all` (-1) | 1000000 |

**The silent-disable trap (Java).** If you don't set `enable.idempotence` but do set `acks=1`, `retries=0` or `max.in.flight=6`, Java quietly turns idempotence off. If you set it explicitly to `true` alongside a conflicting value, the client throws a `ConfigException`. librdkafka refuses to start, as the output below shows.

Idempotence covers one producer session against each partition. For guarantees across restarts or across partitions, use transactions (see Q70 and Q494).

**Example / Code:**
```python
# Runtime: Python 3.11, confluent-kafka 2.16
from confluent_kafka import Producer

try:
    Producer({"bootstrap.servers": "localhost:19092",
              "enable.idempotence": True,
              "max.in.flight.requests.per.connection": 10})
except Exception as e:
    print(type(e).__name__, e)

# Safe, ordered, high-throughput settings for a Python producer:
p = Producer({"bootstrap.servers": "localhost:19092",
              "enable.idempotence": True,   # implies acks=all, retries, max.in.flight<=5
              "linger.ms": 20,
              "compression.type": "lz4"})
```
```text
KafkaException KafkaError{code=_INVALID_ARG,val=-186,str="Failed to create producer: `max.in.flight` must be set <= 5 when `enable.idempotence` is true"}
```

**Real-World Use Case:**
A Python CDC relay published row changes with librdkafka's defaults (idempotence off, a huge in-flight limit). During a broker restart, updates for some rows were applied out of order downstream, so the final value was stale. Enabling idempotence fixed it without any throughput loss.

**Common Mistakes / Interviewer Red Flags:**
- Recommending `max.in.flight=1` as the only way to keep order.
- Assuming Python and Java producers share defaults.
- Setting `acks=1` for speed and losing idempotence without noticing.

**Follow-up Questions Interviewers Ask:**
- What is `OutOfOrderSequenceException`? — The broker saw a sequence gap it can't explain, often after data loss on the broker; the producer must be recreated.
- Does idempotence cost much? — Very little: a few bytes per batch and some broker-side state per producer.
- Does idempotence survive a producer restart? — No; a new session gets a new producer ID unless you use a `transactional.id`.

---

### Q478. Synchronous vs asynchronous sends, flush() and delivery callbacks
**ID:** KFK-10
**Topic:** Kafka > Producers
**Level:** Beginner
**Frequency:** Common
**Tags:** `kafka`, `producer`, `async`, `callbacks`, `python`

**Short Answer (2–3 lines):**
`send()` / `produce()` is always asynchronous: it puts the record in a local buffer and returns at once. "Synchronous" means waiting for that record's acknowledgement (Java `send().get()`, Python `flush()` after each record). That's safe but slow. The usual pattern is async sends with a delivery callback, `poll(0)` in the loop, and `flush()` at shutdown.

**Detailed Explanation:**
| Pattern | Throughput | Error visibility | When |
|---|---|---|---|
| Fire-and-forget (no callback) | Highest | None, so errors are silent | Never in production |
| Sync (`get()` / `flush()` per record) | Low: one round trip per record | Immediate | Rare control messages, tests |
| Async with callback | High | Per record, slightly delayed | Default |
| Async, `flush()` per batch of input | High | At batch boundaries | Batch jobs, such as a file to Kafka |

**Python specifics (`confluent-kafka`)**
- `produce()` raises `BufferError` when the local queue is full (`queue.buffering.max.messages` / `queue.buffering.max.kbytes`). Call `poll()` and try again.
- Delivery callbacks run only inside `poll()` or `flush()`, so call `poll(0)` regularly.
- `flush(timeout)` returns how many messages are still queued. A non-zero return means data was never confirmed.
- Callbacks run on your thread, so keep them short.

**Java specifics**
- Callbacks run on the producer's I/O thread, so don't block in them.
- `close()` waits for in-flight records. Call it on shutdown (`try-with-resources` or a shutdown hook).

**Shutdown.** Kubernetes sends SIGTERM and waits `terminationGracePeriodSeconds`. Catch the signal, stop taking input, `flush()`, then exit. Otherwise records still in the buffer are lost.

**Example / Code:**
```python
# Runtime: Python 3.11, confluent-kafka 2.16, Apache Kafka 4.3 (local single broker)
import time
from confluent_kafka import Producer

conf = {"bootstrap.servers": "localhost:19092", "enable.idempotence": True}  # librdkafka default is False
N = 2000
failures = []

def on_delivery(err, msg):
    if err:
        failures.append((msg.key(), err))

# 1) "Synchronous": wait for each ack before sending the next record
p = Producer(conf)
t = time.time()
for i in range(N):
    p.produce("orders", key=str(i), value=b"{}", on_delivery=on_delivery)
    p.flush()                       # blocks until this record is acked
print(f"sync : {N / (time.time() - t):8.0f} msg/s")

# 2) Asynchronous: keep sending, serve callbacks with poll(0), flush once at the end
p = Producer(conf)
t = time.time()
for i in range(N):
    while True:
        try:
            p.produce("orders", key=str(i), value=b"{}", on_delivery=on_delivery)
            break
        except BufferError:         # local queue full: let callbacks drain it
            p.poll(0.1)
    p.poll(0)                       # run delivery callbacks that are ready
remaining = p.flush(30)
print(f"async: {N / (time.time() - t):8.0f} msg/s, unflushed={remaining}, failures={len(failures)}")
```
```text
sync :     1017 msg/s
async:     3875 msg/s, unflushed=0, failures=0
```
Even on a local broker with sub-millisecond round trips, waiting per record cost most of the throughput. Over a real network, with milliseconds per round trip, the gap gets much wider.

**Real-World Use Case:**
A Lambda-style function produced events and returned without flushing. Some invocations lost their last records, because the runtime froze the process before the background thread sent them. Adding `flush(5)` before returning fixed it.

**Common Mistakes / Interviewer Red Flags:**
- Thinking `produce()` returning means the record is stored.
- Never calling `poll()` in Python, so callbacks never fire and the queue fills.
- Calling `flush()` after every record in a high-volume path.

**Follow-up Questions Interviewers Ask:**
- How do you guarantee nothing is lost on shutdown? — Handle SIGTERM, stop input, `flush()` with a timeout, and alert if the return value is non-zero.
- Are callbacks called in order? — Per partition, yes, in send order.
- Can one producer be shared across threads? — Yes; Java and librdkafka producers are thread-safe, and one per process is usually best.

---

### Q479. The consumer poll loop: what poll() does and why max.poll.interval.ms matters
**ID:** KFK-11
**Topic:** Kafka > Consumers
**Level:** Beginner
**Frequency:** Very Common
**Tags:** `kafka`, `consumer`, `poll-loop`, `max-poll-interval`, `heartbeat`

**Short Answer (2–3 lines):**
A Kafka consumer is a loop. `poll()` fetches records the client has already prefetched from partition leaders, takes part in group membership, and fires rebalance callbacks. Heartbeats go out on a background thread, but if the application doesn't call `poll()` again within `max.poll.interval.ms` (5 minutes by default), the consumer is removed from the group. Its partitions go to other members, and its next commit fails.

**Detailed Explanation:**
**Two liveness checks**
| Check | Setting (Java default) | Detects |
|---|---|---|
| Heartbeat | `session.timeout.ms` (45 s), `heartbeat.interval.ms` (3 s) | Dead process, network partition |
| Poll interval | `max.poll.interval.ms` (5 min) | A live process stuck in processing |

**What `poll()` does**
- Returns up to `max.poll.records` (500) records in Java. librdkafka's `consume(num_messages=…)` and `poll()` read from a local prefetch queue.
- Sends fetch requests in the background (see Q484 for fetch tuning).
- Handles group joins and syncs, and calls `on_assign` / `on_revoke` listeners.
- With auto-commit, commits offsets returned by the *previous* poll.

**A sound loop**
1. `subscribe()` with rebalance callbacks.
2. `poll()`, process the batch, then mark or commit offsets (see Q480).
3. Handle `msg.error()`. Some errors are informational, such as partition EOF.
4. On shutdown, `close()`. It commits (when auto-commit is on) and leaves the group immediately, so others don't wait for a session timeout.

**If processing is slow**
- Lower `max.poll.records` so each batch finishes in time.
- Raise `max.poll.interval.ms` if a single record can legitimately take minutes.
- Hand work to a worker pool and `pause()` partitions while it catches up (see Q485).
- Never block on an external call with no timeout.

The Java `KafkaConsumer` is **not** thread-safe. Use one consumer per thread, and call `wakeup()` from another thread to break out of a long poll.

**Example / Code:**
```python
# Runtime: Python 3.11, confluent-kafka 2.16, Apache Kafka 4.3
# Shows what happens when processing takes longer than max.poll.interval.ms.
import time
from confluent_kafka import Consumer

c = Consumer({
    "bootstrap.servers": "localhost:19092",
    "group.id": "slow-worker",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
    "session.timeout.ms": 6000,      # heartbeats keep flowing in the background
    "max.poll.interval.ms": 6000,    # but poll() must be called at least this often
})
c.subscribe(["clicks"])
first = True
deadline = time.time() + 20
while time.time() < deadline:
    msg = c.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        print("poll() returned error:", msg.error().name(), "-", msg.error().str())
        continue
    if first:
        print(f"got offset {msg.offset()} from partition {msg.partition()}; simulating 9 s of work")
        time.sleep(9)                # longer than max.poll.interval.ms
        first = False
        try:
            c.commit(message=msg, asynchronous=False)
        except Exception as e:
            print("commit failed:", e)
c.close()
```
```text
got offset 0 from partition 1; simulating 9 s of work
commit failed: KafkaError{code=UNKNOWN_MEMBER_ID,val=25,str="Commit failed: Broker: Unknown member"}
poll() returned error: _MAX_POLL_EXCEEDED - Application maximum poll interval (6000ms) exceeded by 395ms
```
The record was processed but its offset couldn't be committed, so another member would process it again.

**Real-World Use Case:**
A consumer enriched each event with an HTTP call that sometimes hung for minutes. The group rebalanced again and again, and the same batch was reprocessed in a loop. Adding a 2-second client timeout and lowering `max.poll.records` to 50 stopped the churn.

**Common Mistakes / Interviewer Red Flags:**
- Raising `session.timeout.ms` to fix slow processing. Session timeout covers heartbeats, not processing time.
- Sharing one Java `KafkaConsumer` across threads.
- Forgetting `close()`, so every restart costs a full session timeout before partitions move.

**Follow-up Questions Interviewers Ask:**
- What does `CommitFailedException` mean? — The member was removed (usually by the poll interval) and its partitions now belong to someone else.
- Does `poll(timeout)` wait the whole timeout? — No; it returns as soon as records are available, or after the timeout with none.
- Where do rebalance callbacks run? — Inside `poll()` on the consumer's thread.

---

### Q480. Offset commit strategies: auto, sync, async and "store then commit"
**ID:** KFK-12
**Topic:** Kafka > Consumers
**Level:** Intermediate
**Frequency:** Very Common
**Tags:** `kafka`, `consumer`, `offsets`, `commit`, `at-least-once`

**Short Answer (2–3 lines):**
Auto-commit commits, every 5 seconds, whatever `poll()` last returned. That can mark records as done before you've processed them. For at-least-once, commit only after processing. Use sync commits at batch boundaries and in the revoke callback, and async commits in the hot path. In Python, a clean pattern is `enable.auto.offset.store=false` with `store_offsets()` after each record, letting the background auto-commit send only what you stored.

**Detailed Explanation:**
Q68 introduced the before-vs-after rule. This question is about the mechanics.

| Strategy | How | Latency cost | Risk |
|---|---|---|---|
| Auto-commit (Java) | `enable.auto.commit=true`, committed during `poll()` | None | With async hand-off, unprocessed records can be committed, so data is lost |
| Sync per batch | `commitSync()` after processing a batch | One round trip per batch | Duplicates after a crash, up to one batch |
| Async + final sync | `commitAsync()` in the loop, `commitSync()` on revoke and close | Low | A failed async commit isn't retried (an older one could overwrite a newer one) |
| Store then auto-commit (librdkafka) | `enable.auto.offset.store=false`, `store_offsets(msg)` after processing | None | Same duplicate window as async |
| External store | Offsets saved with the results in the sink's transaction | Depends on the sink | Most work, and gives exactly-once in effect (see Q493) |

**Details that come up**
- The committed value is the **next** offset to read: last processed + 1. `store_offsets(message=msg)` does the +1 for you.
- Commit in `on_revoke` (classic eager protocol) or for revoked partitions only (cooperative). Otherwise the new owner reprocesses everything since the last commit.
- Commits are per partition. A worker pool that finishes records out of order must commit only up to the lowest contiguous finished offset.
- Committed offsets expire `offsets.retention.minutes` (7 days) after a group becomes empty. After that, `auto.offset.reset` decides where it starts (see Q481).

**Example / Code:**
```python
# Runtime: Python 3.11, confluent-kafka 2.16, Apache Kafka 4.3
# At-least-once: process first, then mark the offset as done, then commit in batches.
import os
from confluent_kafka import Consumer, TopicPartition

c = Consumer({
    "bootstrap.servers": "localhost:19092",
    "group.id": "ledger-writer",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": True,         # background commit every auto.commit.interval.ms...
    "enable.auto.offset.store": False,  # ...but only of offsets we explicitly stored
    "auto.commit.interval.ms": 1000,
})
c.subscribe(["ledger-events"])          # 1 partition, 20 records txn-0..txn-19
done = 0
while done < 8:
    msg = c.poll(2.0)
    if msg is None or msg.error():
        continue
    # process(msg) -> write to the database here
    c.store_offsets(message=msg)        # mark as processed (stores msg.offset() + 1)
    done += 1
    if done == 5:
        c.commit(asynchronous=False)    # explicit sync commit of what has been stored
        print("sync-committed through", msg.offset())
committed = c.committed([TopicPartition("ledger-events", 0)], timeout=5)[0].offset
print(f"processed {done}, last processed offset {msg.offset()}, committed offset {committed}")
os._exit(0)                             # simulated crash: no close(), no final commit
```
```text
sync-committed through 4
processed 8, last processed offset 7, committed offset 5
```
A restarted consumer in the same group printed `resumes at offset 5 value txn-5`. Offsets 5–7 were processed twice, which is at-least-once by design, so the sink must tolerate duplicates.

**Real-World Use Case:**
A consumer pushed records onto an in-memory queue for worker threads, with Java auto-commit on. A deploy killed the pod while 2,000 records sat in the queue. Their offsets had already been committed, so they were never processed. Switching to "commit lowest completed offset per partition" removed the loss.

**Common Mistakes / Interviewer Red Flags:**
- Using auto-commit with asynchronous processing.
- Committing `msg.offset()` instead of `msg.offset() + 1` with low-level APIs.
- Calling `commitSync()` after every record in a high-throughput consumer.
- Never committing in the revoke callback.

**Follow-up Questions Interviewers Ask:**
- Why aren't failed async commits retried? — A retry could land after a newer commit and move the offset backwards; use a sequence check if you retry.
- Can you commit offsets with metadata? — Yes; each commit can carry a short metadata string, such as a batch ID.
- What happens to offsets of an idle group? — They're deleted after `offsets.retention.minutes` once the group is empty.

---

### Q481. auto.offset.reset: earliest, latest, none and by_duration
**ID:** KFK-13
**Topic:** Kafka > Consumers
**Level:** Beginner
**Frequency:** Very Common
**Tags:** `kafka`, `consumer`, `auto-offset-reset`, `offsets`

**Short Answer (2–3 lines):**
`auto.offset.reset` applies only when a group has **no valid committed offset** for a partition: a brand-new group, expired offsets, or an offset that retention has already deleted. `latest` (the default) starts at the end, `earliest` at the oldest retained record, and `none` throws an error. Since Kafka 4.0 the Java client also accepts `by_duration:<ISO-8601>`, which starts at records from that long ago.

**Detailed Explanation:**
| Value | Starts at | Typical use |
|---|---|---|
| `latest` | Log end | Live dashboards and alerts, where history is irrelevant |
| `earliest` | Log start | Pipelines that must see everything, such as loading a lake or rebuilding state |
| `by_duration:PT1H` | Records with timestamp ≥ now − 1 h | A new consumer that only needs recent context |
| `none` | Throws `NoOffsetForPartitionException` | Critical consumers where silently skipping or replaying is unacceptable |

**When it fires**
1. A new `group.id` that has never committed.
2. The group was empty longer than `offsets.retention.minutes` (7 days by default), so its offsets were deleted.
3. The committed offset is now below the log start, because retention deleted the data (`OFFSET_OUT_OF_RANGE`).
4. New partitions were added and the group has no offset for them. With `latest`, records written before the first assignment are skipped.

**It does not fire** when the group already has offsets. That's the most common confusion. Changing the setting from `latest` to `earliest` on an existing group does nothing, as the second run below shows. To move an existing group, reset its offsets (see Q482).

**Client differences.** librdkafka spells the values `smallest`/`earliest`/`beginning` and `largest`/`latest`/`end`, and uses `error` instead of `none`. Its default is also `latest` (`largest`). `by_duration` is a Java-client feature. Product behaviour changes; checked October 2026.

**Example / Code:**
```bash
# Runtime: Apache Kafka 4.3.1 CLI. Topic "clicks" holds 30 events, one every 6 minutes over the last 3 hours.
for r in latest earliest by_duration:PT1H; do
  n=$(bin/kafka-console-consumer.sh --bootstrap-server localhost:19092 --topic clicks \
        --group reset-demo-${r//[:]/-} --command-property auto.offset.reset=$r \
        --timeout-ms 5000 2>/dev/null | wc -l)
  echo "auto.offset.reset=$r -> $n records"
done
# Run the 'earliest' group again: it now has committed offsets, so the reset policy is ignored
bin/kafka-console-consumer.sh --bootstrap-server localhost:19092 --topic clicks \
  --group reset-demo-earliest --command-property auto.offset.reset=earliest --timeout-ms 5000 | wc -l
```
```text
auto.offset.reset=latest -> 0 records
auto.offset.reset=earliest -> 30 records
auto.offset.reset=by_duration:PT1H -> 10 records
0
```

**Real-World Use Case:**
A new fraud-scoring service went live with the default `latest` on a topic that had seven days of history. It never scored events from before launch, and the backfill had to be run as a separate job. With `earliest`, or a one-off reset to a timestamp, the launch would have covered the history.

**Common Mistakes / Interviewer Red Flags:**
- Expecting `earliest` to replay history for a group that has already committed.
- Using `latest` for pipelines that must not miss data, especially when partitions might be added.
- Not alerting when a consumer falls behind the retention boundary and silently jumps ahead.

**Follow-up Questions Interviewers Ask:**
- How would you detect an `OFFSET_OUT_OF_RANGE` reset? — Watch consumer logs or metrics for resets, and alert on lag approaching retention.
- Can a consumer without a group use this? — Yes; with `assign()` and no stored offset it applies the same policy.
- What's a safe choice for a critical sink? — `none`/`error`, so a human decides where to restart.

---

### Q482. Replaying data: seek, offsets-for-times and kafka-consumer-groups --reset-offsets
**ID:** KFK-14
**Topic:** Kafka > Consumers
**Level:** Intermediate
**Frequency:** Common
**Tags:** `kafka`, `consumer`, `replay`, `reset-offsets`, `seek`

**Short Answer (2–3 lines):**
To replay, move the group's committed offsets. Stop the consumers, run `kafka-consumer-groups.sh --reset-offsets` with `--dry-run` first, then `--execute`. Targets include `--to-datetime`, `--shift-by`, `--to-earliest` and `--to-offset`. Inside an application you can `seek()` a partition, or map a timestamp to an offset with `offsets_for_times()`.

**Detailed Explanation:**
**CLI reset options**
| Option | Moves to |
|---|---|
| `--to-earliest` / `--to-latest` | Log start or end |
| `--to-datetime 2026-10-17T12:00:00.000` | First offset with timestamp ≥ that time (UTC unless an offset is given) |
| `--by-duration PT30M` | Now minus the duration |
| `--shift-by -100` | Current offset ± N, per partition |
| `--to-offset 1234` | That offset |
| `--from-file plan.csv` | Per-partition values from a file |

Scope it with `--topic t` (all partitions), `--topic t:0,2` (some partitions) or `--all-topics`.

**Rules**
- The group must be inactive (no members), or the tool refuses. Scale consumers to zero first.
- `--dry-run` is the default if you pass neither flag. Always show the plan before `--execute`.
- Timestamp-based targets use record timestamps (see Q496 on `CreateTime` vs `LogAppendTime`). Producers that set odd timestamps make datetime resets unreliable.

**In-application seeking**
- `consumer.offsets_for_times([TopicPartition(t, p, ts_ms)])` returns offsets, then `seek()` or `assign()` to them.
- Seek inside `on_assign` to start every new assignment at a computed point.
- Use a *separate* group ID for a one-off replay, so the production group's position isn't touched.

**Is replay safe?** Only if downstream is idempotent: upserts keyed by business ID, or deduplication by event ID. Otherwise a replay double-counts.

**Example / Code:**
```bash
# Runtime: Apache Kafka 4.3.1 CLI; group "reset-demo-earliest" is stopped
G=reset-demo-earliest
bin/kafka-consumer-groups.sh --bootstrap-server localhost:19092 --group $G --topic clicks \
  --reset-offsets --to-datetime "$(date -u -d '-30 min' +%Y-%m-%dT%H:%M:%S.000)" --dry-run
bin/kafka-consumer-groups.sh --bootstrap-server localhost:19092 --group $G --topic clicks:0 \
  --reset-offsets --shift-by -2 --execute
```
```text
Warn: Partition 2 from topic clicks is empty. Falling back to latest known offset.

GROUP               TOPIC           PARTITION  NEW-OFFSET
reset-demo-earliest clicks          2          0
reset-demo-earliest clicks          1          20
reset-demo-earliest clicks          0          5

GROUP               TOPIC           PARTITION  NEW-OFFSET
reset-demo-earliest clicks          0          4
```
```python
# Runtime: Python 3.11, confluent-kafka 2.16 — replay the last 30 minutes in a throwaway group
import time
from confluent_kafka import Consumer, TopicPartition

c = Consumer({"bootstrap.servers": "localhost:19092", "group.id": "clicks-replay-oct17",
              "enable.auto.commit": False})
since = int((time.time() - 30 * 60) * 1000)
parts = [TopicPartition("clicks", p, since) for p in range(3)]
starts = c.offsets_for_times(parts, timeout=10)   # offset -1 means "no record that new"
c.assign([tp for tp in starts if tp.offset >= 0])
```

**Real-World Use Case:**
A bug in an enrichment consumer wrote wrong currency conversions for two hours. The team deployed the fix and scaled the consumer to zero. They reset the group `--to-datetime` the start of the incident with a dry run reviewed in the incident channel, and scaled back up. Upserts keyed on `transaction_id` overwrote the bad rows.

**Common Mistakes / Interviewer Red Flags:**
- Resetting offsets while consumers are running (the tool refuses).
- Replaying into a non-idempotent sink.
- Forgetting that `--to-datetime` is interpreted as UTC unless an offset is given.

**Follow-up Questions Interviewers Ask:**
- Can you reset offsets for one partition only? — Yes, `--topic name:partition`.
- How do you delete a group's offsets? — `kafka-consumer-groups.sh --delete-offsets` or `--delete` for the whole group.
- What if the data is already past retention? — It's gone from Kafka; replay from the lake or archive copy instead.

---

### Q483. How is consumer lag measured, and what does the output mean?
**ID:** KFK-15
**Topic:** Kafka > Consumers
**Level:** Beginner
**Frequency:** Very Common
**Tags:** `kafka`, `consumer-lag`, `monitoring`, `cli`

**Short Answer (2–3 lines):**
Lag per partition is log-end offset minus the group's committed offset: how many records are written but not yet committed as processed. Total lag sums over partitions. Offset lag can mislead, so also track **time lag**, the age of the oldest unconsumed record, which says how stale downstream data is.

**Detailed Explanation:**
How to *diagnose* growing lag is covered in Q422 and on this site's lag page. This question is about reading the numbers correctly.

**`kafka-consumer-groups.sh --describe` columns**
| Column | Meaning |
|---|---|
| `CURRENT-OFFSET` | Committed offset (next to read), not the consumer's in-memory position |
| `LOG-END-OFFSET` | Next offset to be written |
| `LAG` | The difference |
| `CONSUMER-ID`, `HOST` | The member that owns the partition; `-` means nobody does |

`--state` shows the group state (`Stable`, `PreparingRebalance`, `Empty`) and assignor. `--members --verbose` shows which partitions each member holds.

**Pitfalls**
- **Commit cadence**: lag is measured at commits, so a healthy consumer committing every 5 s shows a saw-tooth pattern.
- **Transactions**: control markers take offsets, so read-committed consumers can show a small lag that never reaches zero.
- **Compacted topics**: offsets have gaps, so offset lag overstates the number of records left.
- **Different traffic per partition**: 10,000 records of lag is seconds on a busy partition and hours on a quiet one. Time lag normalises this.

**Tools.** The CLI, consumer metrics (`records-lag-max`), Burrow (status by sliding window), Kafka exporters for Prometheus, and the lag metrics managed services provide.

**Example / Code:**
```bash
# Runtime: Apache Kafka 4.3.1 CLI
bin/kafka-consumer-groups.sh --bootstrap-server localhost:19092 --describe --group ledger-writer
bin/kafka-consumer-groups.sh --bootstrap-server localhost:19092 --describe --group ledger-writer --state
```
```text
Consumer group 'ledger-writer' has no active members.

GROUP           TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID     HOST            CLIENT-ID
ledger-writer   ledger-events   0          6               20              14              -               -               -

GROUP           COORDINATOR (ID)          ASSIGNMENT-STRATEGY  STATE                #MEMBERS
ledger-writer   localhost:19092  (1)      -                    Empty                0
```
```python
# Runtime: Python 3.11, confluent-kafka 2.16, Apache Kafka 4.3
# Offset lag and time lag for one group, without joining the group.
import time
from confluent_kafka import Consumer, TopicPartition

GROUP, TOPIC = "reset-demo-earliest", "clicks"
probe = Consumer({"bootstrap.servers": "localhost:19092", "group.id": GROUP,
                  "enable.auto.commit": False})        # never subscribe(), so no rebalance
parts = [TopicPartition(TOPIC, p) for p in probe.list_topics(TOPIC).topics[TOPIC].partitions]
now = time.time()
for tp in probe.committed(parts, timeout=5):
    low, high = probe.get_watermark_offsets(tp, timeout=5)
    committed = tp.offset if tp.offset >= 0 else low
    lag = high - committed
    age = "-"
    if lag > 0:                                       # timestamp of the oldest unconsumed record
        probe.assign([TopicPartition(TOPIC, tp.partition, committed)])
        m = probe.poll(5)
        age = f"{now - m.timestamp()[1] / 1000:.0f}s"
        probe.unassign()
    print(f"p{tp.partition}: committed={committed} end={high} lag={lag} oldest_unconsumed_age={age}")
probe.close()
```
```text
p0: committed=5 end=6 lag=1 oldest_unconsumed_age=156s
p1: committed=20 end=24 lag=4 oldest_unconsumed_age=1596s
p2: committed=0 end=0 lag=0 oldest_unconsumed_age=-
```
Partition 1 has only 4 records of lag, but they are 26 minutes old. That's the number a freshness SLA cares about.

**Real-World Use Case:**
A team alerted on "lag > 50,000" for every topic. The alert fired constantly on a busy clickstream topic that was only seconds behind, and never on a low-volume billing topic that was hours behind. Switching to a time-lag SLO per consumer group fixed both problems.

**Common Mistakes / Interviewer Red Flags:**
- Treating `CURRENT-OFFSET` as where the consumer actually is right now.
- Alerting on a single global offset-lag threshold.
- Expecting lag to reach exactly zero on transactional or compacted topics.

**Follow-up Questions Interviewers Ask:**
- Why does an `Empty` group still show lag? — Lag is computed from stored offsets, which remain after members leave until they expire.
- What is `records-lag-max`? — A client-side metric: the largest lag across the partitions this consumer currently owns.
- How would you alert on lag? — On time lag or lag growth rate per group, with thresholds tied to the freshness SLA.

---
