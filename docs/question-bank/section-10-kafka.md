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

### Q484. Consumer fetch tuning: fetch.min.bytes, fetch.max.wait.ms and partition fetch sizes
**ID:** KFK-16
**Topic:** Kafka > Consumers
**Level:** Intermediate
**Frequency:** Occasional
**Tags:** `kafka`, `consumer`, `fetch`, `latency`, `throughput`

**Short Answer (2–3 lines):**
A fetch request waits on the broker until it has `fetch.min.bytes` of data or `fetch.max.wait.ms` passes. It returns at most `max.partition.fetch.bytes` per partition and `fetch.max.bytes` in total. Raise `fetch.min.bytes` for bulk sinks to get fewer, larger fetches. Keep it at 1 for low-latency consumers, because on quiet topics a high value turns into waiting.

**Detailed Explanation:**
| Setting (Java default) | Effect | Raise when | Lower when |
|---|---|---|---|
| `fetch.min.bytes` (1) | Broker holds the response until this much data is ready | Bulk sinks; you want to cut broker request load | Latency-sensitive consumers |
| `fetch.max.wait.ms` (500) | Maximum the broker holds the request | Paired with a big `fetch.min.bytes` | You need a tight latency bound |
| `max.partition.fetch.bytes` (1 MiB) | Per-partition cap per response | Large records; few partitions | Many partitions and limited memory |
| `fetch.max.bytes` (50 MiB) | Per-response cap across partitions | Very high throughput | Memory pressure |
| `max.poll.records` (500) | Records handed to the application per `poll()` | Cheap per-record work | Slow per-record work (see Q479) |

**How they interact**
- The fetcher works ahead of the application. Java buffers completed fetches. librdkafka prefetches into a queue bounded by `queued.max.messages.kbytes` (64 MiB per client by default), which is a common cause of memory bloat when there are many partitions.
- One record bigger than `max.partition.fetch.bytes` is still returned, so the consumer can't get stuck. But memory must allow for it.
- Cross-AZ costs: fetching from a follower in the same zone (`client.rack`, see Q492) changes where bytes come from, not how many.

**Rule of thumb profiles**
- *Lake sink* (S3/Iceberg writer): `fetch.min.bytes=1 MB`, `fetch.max.wait.ms=500`, larger `max.poll.records`.
- *Alerting or fraud*: defaults or a lower `fetch.max.wait.ms`.

**Example / Code:**
```python
# Runtime: Python 3.11, confluent-kafka 2.16, Apache Kafka 4.3
# A low-traffic topic (one record every 100 ms): how fetch.min.bytes changes end-to-end latency.
import statistics, threading, time
from confluent_kafka import Consumer, Producer

def measure(fetch_conf, n=30):
    c = Consumer({"bootstrap.servers": "localhost:19092", "group.id": f"lat-{time.time()}",
                  "auto.offset.reset": "latest", **fetch_conf})
    c.subscribe(["trickle"])
    while not c.assignment():
        c.poll(0.2)
    time.sleep(1)
    def produce():
        p = Producer({"bootstrap.servers": "localhost:19092", "linger.ms": 0})
        for _ in range(n):
            p.produce("trickle", value=str(time.time()).encode()); p.poll(0); time.sleep(0.1)
        p.flush()
    threading.Thread(target=produce).start()
    lat = []
    while len(lat) < n:
        m = c.poll(2)
        if m and not m.error():
            lat.append((time.time() - float(m.value())) * 1000)
    c.close()
    return f"median {statistics.median(lat):6.1f} ms, max {max(lat):6.1f} ms"

print("fetch.min.bytes=1 (default)            :", measure({}))
print("fetch.min.bytes=1MB, fetch.wait.max=500 :", measure({"fetch.min.bytes": 1048576, "fetch.wait.max.ms": 500}))
```
```text
fetch.min.bytes=1 (default)            : median    1.7 ms, max    7.6 ms
fetch.min.bytes=1MB, fetch.wait.max=500 : median  267.4 ms, max  469.6 ms
```
On a quiet topic the 1 MB threshold is never reached, so every fetch waits for the timeout. On a busy topic the same settings would cost almost nothing and save many requests. (librdkafka names the wait setting `fetch.wait.max.ms`; Java calls it `fetch.max.wait.ms`.)

**Real-World Use Case:**
A team copied "throughput" consumer settings from a busy clickstream sink to a low-volume payments-alert consumer. Alerts arrived about half a second late on average. Restoring `fetch.min.bytes=1` for that consumer brought them back to milliseconds.

**Common Mistakes / Interviewer Red Flags:**
- Applying one consumer profile to every workload.
- Confusing `fetch.max.bytes` with `max.poll.records`: one limits network responses, the other what `poll()` hands you.
- Ignoring prefetch memory when one process consumes thousands of partitions.

**Follow-up Questions Interviewers Ask:**
- Which metrics show fetch behaviour? — `fetch-rate`, `fetch-size-avg`, `fetch-latency-avg` and `records-per-request-avg` on the consumer.
- Why might a consumer use lots of memory while idle? — Prefetch queues filled for many partitions; lower `queued.max.messages.kbytes` (librdkafka) or the fetch sizes.
- Does the broker read from disk for every fetch? — Usually not; recent data is served from the page cache with zero-copy transfer.

---

### Q485. Processing faster than one thread per partition: worker pools and ordering
**ID:** KFK-17
**Topic:** Kafka > Consumers
**Level:** Intermediate
**Frequency:** Common
**Tags:** `kafka`, `consumer`, `concurrency`, `ordering`, `offsets`

**Short Answer (2–3 lines):**
Partitions cap how many group members can work in parallel. Within one consumer you can still fan records out to a worker pool. To keep per-key order, route each key to a fixed worker. Commit only the lowest offset per partition below which everything is finished. Use `pause()`/`resume()` for backpressure instead of letting queues grow without limit.

**Detailed Explanation:**
**Options, from simple to complex**
| Approach | Ordering | Notes |
|---|---|---|
| More partitions + more consumers | Per key | Simplest, but changes key placement (see Q474) |
| One consumer per thread, `N` threads in a process | Per partition | Same as scaling out; each thread has its own consumer |
| Consumer + worker pool routed by key | Per key | Higher parallelism than partitions; needs offset tracking |
| Consumer + unordered pool | None | Highest throughput; only for independent records |
| Share groups (see Q488) | None | The broker does per-record dispatch and acknowledgement |
| Libraries such as Confluent Parallel Consumer (Java) | Per key, per partition or none | Handle offset tracking and retries for you |

**The offset problem.** Workers finish out of order. If offsets 10–12 are done but 9 isn't, committing 13 would lose 9 after a crash. Track finished offsets per partition and commit the contiguous high mark. This is what `advance()` does below.

**Backpressure.** When the work queues are full, call `pause()` on the assigned partitions but **keep calling `poll()`**, so the member stays in the group (see Q479). `resume()` when the backlog drains.

**Rebalances.** On revoke, either wait for in-flight work on those partitions and commit, or drop it and accept reprocessing. Records still queued for a revoked partition must not be committed later by the old owner.

**Example / Code:**
```python
# Runtime: Python 3.11, confluent-kafka 2.16, Apache Kafka 4.3
# One consumer, 8 worker threads. Per-key order is kept by routing each key to a fixed worker;
# offsets are committed only up to the lowest offset that is finished with no gaps before it.
import queue, threading, time, zlib
from collections import defaultdict
from confluent_kafka import Consumer, TopicPartition

N_WORKERS, MAX_QUEUED = 8, 200
queues = [queue.Queue() for _ in range(N_WORKERS)]
done = defaultdict(set)          # partition -> finished offsets not yet committed
lock = threading.Lock()
last_by_key = {}

def worker(q):
    while True:
        m = q.get()
        if m is None:
            return
        key, val = m.key().decode(), int(m.value())
        assert last_by_key.get(key, -1) < val, "per-key order violated"
        last_by_key[key] = val
        time.sleep(0.005)                        # pretend to call an API
        with lock:
            done[m.partition()].add(m.offset())

threads = [threading.Thread(target=worker, args=(q,)) for q in queues]
for t in threads: t.start()

c = Consumer({"bootstrap.servers": "localhost:19092", "group.id": "parallel-demo-2",
              "auto.offset.reset": "earliest", "enable.auto.commit": False})
c.subscribe(["events6"])
next_commit = {}                 # partition -> next offset we expect to commit from
paused, handled, idle = False, 0, 0

def advance():
    """Move each partition's commit point over contiguous finished offsets."""
    moved = []
    with lock:
        for p, nxt in next_commit.items():
            while nxt in done[p]:
                done[p].discard(nxt); nxt += 1
            if nxt != next_commit[p]:
                next_commit[p] = nxt
                moved.append(TopicPartition("events6", p, nxt))
    return moved

while idle < 6:
    m = c.poll(0.5)
    if m is not None and not m.error():
        idle = 0
        next_commit.setdefault(m.partition(), m.offset())
        queues[zlib.crc32(m.key()) % N_WORKERS].put(m)   # same key -> same worker
        handled += 1
    else:
        idle += 1
    backlog = sum(q.qsize() for q in queues)
    if backlog > MAX_QUEUED and not paused:
        c.pause(c.assignment()); paused = True           # keep polling, stop fetching
    elif backlog < MAX_QUEUED // 2 and paused:
        c.resume(c.assignment()); paused = False
    to_commit = advance()
    if to_commit:
        c.commit(offsets=to_commit, asynchronous=True)

for q in queues: q.put(None)
for t in threads: t.join()
advance()
c.commit(offsets=[TopicPartition("events6", p, o) for p, o in next_commit.items()], asynchronous=False)
print(f"handled {handled} records with {N_WORKERS} workers; committed:",
      {p: o for p, o in sorted(next_commit.items())})
c.close()
```
```text
handled 600 records with 8 workers; committed: {0: 84, 1: 108, 2: 120, 3: 96, 4: 108, 5: 84}
```
The committed offsets equal each partition's log-end offset (`kafka-get-offsets.sh` printed `events6:0:84 … events6:5:84`), and the per-key order assertion never fired.

**Real-World Use Case:**
An enrichment consumer called a geocoding API taking about 50 ms per record. Twelve partitions limited the group to 12 concurrent calls. A key-routed pool of 64 workers in each of 3 pods raised throughput many times over without re-partitioning, and still applied each customer's updates in order.

**Common Mistakes / Interviewer Red Flags:**
- Committing the highest finished offset instead of the contiguous one.
- Sharing one Java `KafkaConsumer` between threads.
- Unbounded in-memory queues, which turn a slow sink into an out-of-memory crash.
- Forgetting that a rebalance can hand a partition to someone else while its records sit in your queue.

**Follow-up Questions Interviewers Ask:**
- Why route by key hash and not round-robin? — Round-robin lets two workers handle the same key at once, breaking order.
- How do you handle a poison record in one worker? — Retry with limits, then send it to a DLQ and mark it finished so the commit point can advance (see Q500).
- When is adding partitions better? — When work is CPU-bound and can scale across pods, and the topic's keys can tolerate a remap.

---

### Q486. Partition assignment strategies: range, round-robin, sticky and cooperative-sticky
**ID:** KFK-18
**Topic:** Kafka > Consumer Groups
**Level:** Intermediate
**Frequency:** Common
**Tags:** `kafka`, `consumer-groups`, `assignor`, `cooperative-rebalancing`

**Short Answer (2–3 lines):**
In the classic protocol, the group leader's assignor decides who gets which partitions. **Range** assigns per topic in contiguous blocks, which co-locates the same partition numbers across topics but can be uneven. **Round-robin** spreads everything evenly. **Sticky** is balanced and minimises movement. **Cooperative-sticky** does the same with incremental rebalancing, so members keep their unaffected partitions while the group changes.

**Detailed Explanation:**
| Assignor | Balance | Movement on change | Rebalance style | Notes |
|---|---|---|---|---|
| `RangeAssignor` | Can be skewed with many topics | High | Eager | Same partition numbers across co-partitioned topics go to one member |
| `RoundRobinAssignor` | Even | High | Eager | Breaks cross-topic co-location |
| `StickyAssignor` | Even | Minimal | Eager | Still stops everything during the rebalance |
| `CooperativeStickyAssignor` | Even | Minimal | Incremental | Two-phase: revoke only what moves, then assign it |

**Eager vs incremental.** With eager assignors every member revokes everything, then receives a new assignment. With cooperative, only partitions that change owner are revoked, in a first round, and assigned in a follow-up round. Consumers keep processing the rest.

**Defaults.** Java's default `partition.assignment.strategy` is `[RangeAssignor, CooperativeStickyAssignor]`. That lets a rolling upgrade move a group to cooperative by removing Range in a second roll. librdkafka defaults to `range,roundrobin`, so Python groups are eager unless you opt in. All members must support a common strategy.

**Migrating to cooperative (classic protocol).** Roll once with both `[cooperative-sticky, range]` listed, then roll again with only `cooperative-sticky`. Switching all at once with mixed members fails to agree on a protocol.

**Where this is heading.** With the KIP-848 protocol the broker computes assignments (`uniform` or `range`) and every rebalance is incremental (see Q487). Client-side assignors then don't apply.

**Example / Code:**
```python
# Runtime: Python 3.11, confluent-kafka 2.16, Apache Kafka 4.3 (classic group protocol)
# Two consumers, two topics with 3 partitions each: compare assignment strategies.
import time
from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient, NewTopic

admin = AdminClient({"bootstrap.servers": "localhost:19092"})
for f in admin.create_topics([NewTopic(t, 3) for t in ("ta", "tb")]).values():
    try: f.result()
    except Exception: pass               # already exists

def run(strategy):
    group = f"assign-{strategy}-{int(time.time())}"
    cs = [Consumer({"bootstrap.servers": "localhost:19092", "group.id": group,
                    "partition.assignment.strategy": strategy}) for _ in range(2)]
    for c in cs:
        c.subscribe(["ta", "tb"])
    end = time.time() + 12
    while time.time() < end:
        for c in cs:
            c.poll(0.2)
    for i, c in enumerate(cs):
        parts = sorted(f"{tp.topic}-{tp.partition}" for tp in c.assignment())
        print(f"{strategy:18} consumer{i}: {len(parts)} -> {parts}")
    for c in cs:
        c.close()

for s in ("range", "roundrobin", "cooperative-sticky"):
    run(s)
```
```text
range              consumer0: 2 -> ['ta-2', 'tb-2']
range              consumer1: 4 -> ['ta-0', 'ta-1', 'tb-0', 'tb-1']
roundrobin         consumer0: 3 -> ['ta-1', 'tb-0', 'tb-2']
roundrobin         consumer1: 3 -> ['ta-0', 'ta-2', 'tb-1']
cooperative-sticky consumer0: 3 -> ['ta-1', 'tb-0', 'tb-2']
cooperative-sticky consumer1: 3 -> ['ta-0', 'ta-2', 'tb-1']
```
Range gave one member twice the work. The other two strategies balanced it 3/3.

**Real-World Use Case:**
A Python sink group of 20 pods read 15 topics with librdkafka's default range assignor. A few pods carried most partitions and lagged, and every deploy paused the whole group. Moving to `cooperative-sticky` balanced the load and made rolling deploys nearly invisible in lag graphs.

**Common Mistakes / Interviewer Red Flags:**
- Assuming Python and Java consumers share the same default assignor.
- Switching every member to cooperative in a single deploy.
- Using round-robin where a downstream process needs `orders-3` and `payments-3` on the same member.

**Follow-up Questions Interviewers Ask:**
- What does "sticky" preserve? — Existing assignments, so local caches and state stay warm.
- Does cooperative rebalancing make the rebalance shorter? — Not necessarily shorter, but far less disruptive, because unaffected partitions keep flowing.
- What extra callback matters with cooperative? — `on_revoke` receives only the partitions being moved, so commit just those.

---

### Q487. The KIP-848 consumer group protocol: what changes with group.protocol=consumer?
**ID:** KFK-19
**Topic:** Kafka > Consumer Groups
**Level:** Advanced
**Frequency:** Common
**Tags:** `kafka`, `kip-848`, `consumer-groups`, `rebalancing`, `kafka-4`

**Short Answer (2–3 lines):**
KIP-848 moves partition assignment from the client group leader to the broker's group coordinator. Members send periodic heartbeats with their subscriptions, and the coordinator reconciles each member towards a target assignment one member at a time. There's no group-wide join/sync barrier, so a slow member can't stall everyone, and every change is incremental. It's GA since Kafka 4.0, but clients still default to `group.protocol=classic`.

**Detailed Explanation:**
Q68 mentions KIP-848 in passing. The mechanics are worth knowing in detail.

**Classic protocol**
- JoinGroup and SyncGroup rounds. One member, the leader, runs the assignor and hands out the result.
- Every member must rejoin before anyone gets an assignment, so one slow member stalls the whole group.
- Session timeout and heartbeat interval are client settings.

**Consumer protocol (KIP-848)**
- A single `ConsumerGroupHeartbeat` API carries subscriptions, owned partitions and assignment updates.
- The **broker** runs a server-side assignor: `group.consumer.assignors=uniform,range` (default `uniform`). Clients may pick one with `group.remote.assignor`.
- Changes are reconciled per member: a partition is revoked from A, and only after A confirms is it assigned to C. No other member is touched.
- Liveness is configured on the broker or per group: `group.consumer.session.timeout.ms` (45 s) and `group.consumer.heartbeat.interval.ms` (5 s). Clients that set `session.timeout.ms` get an error (shown below).
- Regex subscriptions are evaluated on the broker.
- Static membership still works through `group.instance.id`.

**Migration.** An empty classic group converts automatically when the first new-protocol member joins. A live group can be rolled member by member: the group converts when the first new-protocol member joins, and the coordinator interoperates with the remaining classic members. This works only if the classic assignor embeds no custom metadata. Rack-aware assignment isn't fully supported yet. Clients: Java 4.0+, and librdkafka 2.12+ (GA). Check your client's release notes for gaps such as rack-aware assignment or specific callbacks.

Kafka Streams has its own broker-side protocol (KIP-1071, `group.protocol=streams`), production-ready for its core feature set since 4.2. Product behaviour changes; checked October 2026.

**Example / Code:**
```python
# Runtime: Python 3.11, confluent-kafka 2.16, Apache Kafka 4.3
# Two members own a 6-partition topic; a third joins. Which partitions move?
import sys, time
from confluent_kafka import Consumer

PROTOCOL = sys.argv[1]            # "classic-eager", "classic-cooperative" or "consumer"
closing = False
group = f"rb-{PROTOCOL}-{int(time.time())}"

def make(name):
    conf = {"bootstrap.servers": "localhost:19092", "group.id": group}
    if PROTOCOL == "consumer":
        conf["group.protocol"] = "consumer"            # KIP-848: broker computes assignments
    elif PROTOCOL == "classic-cooperative":
        conf["partition.assignment.strategy"] = "cooperative-sticky"
    else:
        conf["partition.assignment.strategy"] = "range"  # eager
    c = Consumer(conf)
    def log(verb):
        def cb(consumer, parts):
            if parts and not closing:     # skip empty incremental callbacks and shutdown noise
                print(f"  {name} {verb} {sorted(p.partition for p in parts)}")
        return cb
    c.subscribe(["events6"], on_assign=log("assigned"), on_revoke=log("revoked "))
    return c

def spin(cs, secs):
    end = time.time() + secs
    while time.time() < end:
        for c in cs:
            c.poll(0.1)

print(PROTOCOL)
members = [make("A"), make("B")]
spin(members, 15)
print(" -- C joins --")
members.append(make("C"))
spin(members, 15)
closing = True
for c in members:
    c.close()
```
Output (`python rebalance.py classic-eager`, then `classic-cooperative`, then `consumer`):
```text
classic-eager
  A assigned [3, 4, 5]
  B assigned [0, 1, 2]
 -- C joins --
  B revoked  [0, 1, 2]
  A revoked  [3, 4, 5]
  A assigned [4, 5]
  B assigned [0, 1]
  C assigned [2, 3]
classic-cooperative
  A assigned [1, 3, 5]
  B assigned [0, 2, 4]
 -- C joins --
  A revoked  [1]
  B revoked  [0]
  C assigned [0, 1]
consumer
  A assigned [0, 1, 2, 3, 4, 5]
  A revoked  [3, 4, 5]
  B assigned [3, 4, 5]
 -- C joins --
  B revoked  [5]
  C assigned [5]
  A revoked  [2]
  C assigned [2]
```
Eager stopped all six partitions to add one member. Both other protocols moved only two. Under KIP-848 each move was confirmed member by member. Inspecting a running new-protocol group:
```text
$ bin/kafka-consumer-groups.sh --bootstrap-server localhost:19092 --describe --group kip848-demo --state
GROUP           COORDINATOR (ID)          ASSIGNMENT-STRATEGY  STATE                #MEMBERS
kip848-demo     localhost:19092  (1)      uniform              Stable               1

# Setting a client-side session timeout with the new protocol:
KafkaError{code=_INVALID_ARG,val=-186,str="Failed to create consumer: `session.timeout.ms` is not supported for `group.protocol=consumer` or share consumer. It is defined broker side"}
```

**Real-World Use Case:**
A group of 150 consumers on Kubernetes rebalanced for minutes during each rolling deploy under the classic protocol, because every pod restart forced a group-wide join. After the brokers reached 4.x and the clients switched to `group.protocol=consumer`, pod restarts moved only that pod's partitions, and lag during deploys flattened.

**Common Mistakes / Interviewer Red Flags:**
- Assuming upgrading the brokers turns KIP-848 on; clients must opt in.
- Keeping `partition.assignment.strategy` or `session.timeout.ms` in client configs after switching.
- Calling KIP-848 just "cooperative sticky on the broker"; the barrier-free, per-member reconciliation is the real change.

**Follow-up Questions Interviewers Ask:**
- Where do you tune the session timeout now? — Broker defaults `group.consumer.session.timeout.ms`, or per group with `kafka-configs.sh --entity-type groups`.
- Can classic and new members share a group? — Yes, during an online migration; the coordinator converts the group when possible.
- Is the classic protocol going away? — The plan (KIP-1274) is that `KafkaConsumer` defaults to the new protocol in 5.0 and drops classic in 6.0, while brokers keep supporting classic clients.

---

### Q488. Share groups (Queues for Kafka): how do they differ from consumer groups?
**ID:** KFK-20
**Topic:** Kafka > Share Groups
**Level:** Advanced
**Frequency:** Occasional
**Tags:** `kafka`, `share-groups`, `kip-932`, `queues`, `acknowledgement`

**Short Answer (2–3 lines):**
A share group (KIP-932, production-ready in Kafka 4.2) lets many consumers read the **same partition** together. Each record is acquired by one member for a time-limited lock and acknowledged on its own as ACCEPT, RELEASE (redeliver) or REJECT. Kafka counts deliveries per record. You get queue semantics without ordering and without the members ≤ partitions limit.

**Detailed Explanation:**
| | Consumer group | Share group |
|---|---|---|
| Unit of assignment | Whole partition to one member | Records within a partition, to any member |
| Max useful members | Number of partitions | Not tied to partitions |
| Ordering | Per partition | None guaranteed |
| Progress tracking | One committed offset per partition | Per-record state: available, acquired, acknowledged, archived |
| Retries | Application's job | RELEASE, or lock expiry, redelivers; `delivery_count` is tracked |
| Poison records | DLQ pattern in app code (see Q500) | REJECT, or exceeding the delivery limit, archives the record |

**How it works**
- The **share-partition leader** (on the partition leader broker) tracks in-flight records between the share-partition start offset (SPSO) and end offset (SPEO). State is persisted in `__share_group_state`.
- `group.share.record.lock.duration.ms` (30 s) is how long a member holds a record before it becomes available again.
- `group.share.delivery.count.limit` (5) caps attempts. After that the record is archived and never delivered again (4.3 adds a per-group `share.delivery.count.limit`).
- Where a new group starts is set per group with `share.auto.offset.reset` (default `latest`) via `kafka-configs.sh --entity-type groups`, not in client config.
- Acknowledgement is implicit (accept the previous batch on the next poll) or explicit.

**When to use**
- Job or work queues where each item is independent: image processing, sending notifications, calling rate-limited APIs.
- Spiky workloads that need more workers than partitions.

**When not to**
- Per-key ordering, stateful stream processing, or Kafka transactions across read and write (not supported).

**Example / Code:**
```bash
# Runtime: Apache Kafka 4.3.1 CLI
bin/kafka-topics.sh --bootstrap-server localhost:19092 --create --topic jobs --partitions 1
seq 0 11 | sed 's/^/job-/' | bin/kafka-console-producer.sh --bootstrap-server localhost:19092 --topic jobs
bin/kafka-configs.sh --bootstrap-server localhost:19092 --alter --entity-type groups \
  --entity-name workers --add-config share.auto.offset.reset=earliest
```
```python
# Runtime: Python 3.11, confluent-kafka 2.16 (ShareConsumer), Apache Kafka 4.3 with share.version=1
# Two share-group members read ONE partition. Bad jobs are rejected; a flaky one is released and retried.
import time
from confluent_kafka import ShareConsumer, AcknowledgeType

def member(name):
    c = ShareConsumer({"bootstrap.servers": "localhost:19092", "group.id": "workers",
                       "share.acknowledgement.mode": "explicit"})
    c.subscribe(["jobs"])
    return name, c

members = [member("w1"), member("w2")]
seen, end = [], time.time() + 25
while time.time() < end and len(seen) < 13:
    for name, c in members:
        for m in c.poll(0.5):
            if m.error():
                continue
            job, n = m.value().decode(), m.delivery_count()
            if job == "job-3":
                ack = AcknowledgeType.REJECT          # poison: never redeliver
            elif job == "job-5" and n == 1:
                ack = AcknowledgeType.RELEASE         # transient failure: give it back
            else:
                ack = AcknowledgeType.ACCEPT
            c.acknowledge(m, ack)
            seen.append(job)
            print(f"{name} offset={m.offset():2} {job:6} delivery_count={n} -> {ack.name}")
        c.commit_async()
for _, c in members:
    c.close()
```
```text
w2 offset= 0 job-0  delivery_count=1 -> ACCEPT
w2 offset= 1 job-1  delivery_count=1 -> ACCEPT
w2 offset= 2 job-2  delivery_count=1 -> ACCEPT
w2 offset= 3 job-3  delivery_count=1 -> REJECT
w2 offset= 4 job-4  delivery_count=1 -> ACCEPT
w2 offset= 5 job-5  delivery_count=1 -> RELEASE
w2 offset= 6 job-6  delivery_count=1 -> ACCEPT
w2 offset= 7 job-7  delivery_count=1 -> ACCEPT
w2 offset= 8 job-8  delivery_count=1 -> ACCEPT
w2 offset= 9 job-9  delivery_count=1 -> ACCEPT
w2 offset=10 job-10 delivery_count=1 -> ACCEPT
w2 offset=11 job-11 delivery_count=1 -> ACCEPT
w1 offset= 5 job-5  delivery_count=2 -> ACCEPT
```
Both members read the single partition. The released job went to the other member with `delivery_count=2`, and the rejected job was never redelivered. In a consumer group, w1 would have sat idle.

**Real-World Use Case:**
A document-OCR service had bursts of 50,000 jobs on a 12-partition topic. Each job took seconds, so the consumer group could use only 12 workers. Moving to a share group let the autoscaler run 200 workers against the same topic. Jobs that kept failing hit the delivery limit and were archived, rather than blocking a partition.

**Common Mistakes / Interviewer Red Flags:**
- Promising ordering with share groups.
- Setting `auto.offset.reset` on the share consumer instead of `share.auto.offset.reset` on the group.
- Processing longer than the lock duration, so records get redelivered while still being worked on.
- Forgetting that clusters with fewer than 3 brokers need `share.coordinator.state.topic.replication.factor` lowered.

**Follow-up Questions Interviewers Ask:**
- How do you inspect a share group? — `kafka-share-groups.sh --describe --group workers` (start offset and lag) and `--members`.
- Is delivery exactly-once? — No; it's at-least-once with per-record retries, so processing must be idempotent.
- Which clients support it? — The Java `KafkaShareConsumer` is production-ready; librdkafka's share consumer (used by `confluent-kafka`'s `ShareConsumer`) is still marked Preview in its changelog, so check before relying on it.

---

### Q489. How replication works: leaders, followers, the ISR and the high watermark
**ID:** KFK-21
**Topic:** Kafka > Replication & Durability
**Level:** Intermediate
**Frequency:** Very Common
**Tags:** `kafka`, `replication`, `isr`, `high-watermark`, `min-insync-replicas`

**Short Answer (2–3 lines):**
Each partition has one leader that takes all writes. Followers copy from it by sending fetch requests, just as consumers do. Followers that keep up within `replica.lag.time.max.ms` (30 s) form the in-sync replica set (ISR). The high watermark is the highest offset every ISR member has, and consumers only see records below it. Since Kafka 4.0 the high watermark also won't advance while the ISR is smaller than `min.insync.replicas`, whatever `acks` the producer used.

**Detailed Explanation:**
How `acks`, replication factor and `min.insync.replicas` combine into a durability choice is covered on this site's durability page. This question is about the mechanism underneath.

**Follower fetching.** Followers send `Fetch` requests to the leader with their log-end offset. The leader learns how far each follower has got from these requests, and it returns its current high watermark (HW) in the response, so followers learn the HW too.

**ISR membership.** A follower drops out of the ISR if it hasn't caught up to the leader's log end within `replica.lag.time.max.ms`. A cleanly stopped broker is removed straight away. The leader asks the KRaft controller to shrink or expand the ISR, and the change is recorded in the metadata log.

**The high watermark**
- HW = the minimum log-end offset across ISR members.
- Consumers fetch only up to the HW, so they never see a record that could disappear in a leader failover.
- "Strict min ISR" (KIP-966, Kafka 4.0+): if ISR size < `min.insync.replicas`, the HW stops. Even `acks=1` writes are stored on the leader but stay invisible until enough replicas catch up. The run below shows this.

**`acks=all` + `min.insync.replicas=2` with replication factor 3**
- 3 in sync: writes succeed.
- 2 in sync: writes still succeed (you can lose one broker).
- 1 in sync: `acks=all` producers get `NOT_ENOUGH_REPLICAS`, retry, and finally time out. The partition is effectively read-only for them.

**Leader epochs.** Each leader change bumps an epoch. Followers use epoch boundaries to truncate divergent records after a failover, which replaced the older HW-based truncation and its edge cases.

**Example / Code:**
```bash
# Runtime: Apache Kafka 4.3.1 lab: 1 KRaft controller + 3 brokers (racks az-1..az-3) on one host; Python 3.11 + confluent-kafka 2.16
# send.py <acks> <value> produces one record to "ledger" with delivery.timeout.ms=8000 and prints the outcome
C=localhost:29091
kafka-topics.sh --bootstrap-server $C --create --topic ledger --replica-assignment 1:2:3 --config min.insync.replicas=2
python send.py all "deposit 100"
./stop.sh 3; python send.py all "deposit 50"
./stop.sh 2; python send.py all "withdraw 30"; python send.py 1 "withdraw 20"
kafka-get-offsets.sh --bootstrap-server $C --topic ledger      # shows the high watermark
./start.sh 2 3                                                  # bring the followers back
```
```text
Topic: ledger	Partition: 0	Leader: 1	Replicas: 1,2,3	Isr: 1,2,3	Elr: 	LastKnownElr: 
acks=all 'deposit 100': written at offset 0
## stop broker 3
Topic: ledger	Partition: 0	Leader: 1	Replicas: 1,2,3	Isr: 1,2	Elr: 	LastKnownElr: 
acks=all 'deposit 50': written at offset 1
  visible to consumers (high watermark): ledger:0:2
## stop broker 2
Topic: ledger	Partition: 0	Leader: 1	Replicas: 1,2,3	Isr: 1	Elr: 2	LastKnownElr: 
acks=all 'withdraw 30': FAILED _MSG_TIMED_OUT
acks=1   'withdraw 20': written at offset 2
  visible to consumers (high watermark): ledger:0:2
## restart brokers 2 and 3
Topic: ledger	Partition: 0	Leader: 1	Replicas: 1,2,3	Isr: 1,2,3	Elr: 	LastKnownElr: 
  visible to consumers (high watermark): ledger:0:3
```
With one replica in sync, the `acks=all` write was refused (retried until the 8-second budget ran out). The `acks=1` write was accepted, but stayed invisible (HW 2) until the followers rejoined and the HW moved to 3. Broker 2 also appeared in the ELR (see Q490).

**Real-World Use Case:**
During a zone outage, a team's dashboards showed producers "succeeding" with `acks=1` while consumers saw nothing new. That is strict min ISR at work: the data was safe on one broker but not yet committed. It became visible as soon as the zone returned. Producers using `acks=all` had failed loudly instead, which the runbook now treats as the expected signal.

**Common Mistakes / Interviewer Red Flags:**
- Saying followers receive pushed data from the leader. They pull it.
- Thinking `acks=1` data is readable immediately in every case.
- Confusing a replica falling out of the ISR with a replica being removed from the assignment.
- Setting `min.insync.replicas` equal to the replication factor, so any single broker restart blocks `acks=all` writes.

**Follow-up Questions Interviewers Ask:**
- What makes a follower fall out of the ISR? — Not catching up to the leader's log end within `replica.lag.time.max.ms`, usually from a slow disk, GC pauses or network trouble.
- What is the log end offset vs the high watermark? — LEO is the next offset the replica will write; HW is the committed boundary that consumers may read up to.
- Why does Kafka not need a majority of replicas, like Raft does? — The ISR is tracked by the controller, so any ISR member is safe to elect, and `min.insync.replicas` sets how many must confirm.

---
### Q490. Unclean leader election and Eligible Leader Replicas (ELR)
**ID:** KFK-22
**Topic:** Kafka > Replication & Durability
**Level:** Advanced
**Frequency:** Common
**Tags:** `kafka`, `unclean-leader-election`, `elr`, `kip-966`, `data-loss`

**Short Answer (2–3 lines):**
Unclean leader election lets a replica that is *not* in sync become leader when no in-sync replica is available. That restores availability, but it truncates any acknowledged writes the new leader never had, so it is off by default (`unclean.leader.election.enable=false`). Kafka 4.x adds Eligible Leader Replicas (KIP-966, on by default for new clusters since 4.1). The controller remembers which out-of-ISR replicas are still safe, so a clean election is possible in more failure cases.

**Detailed Explanation:**
**The dilemma.** All ISR members are down. You can wait for one of them (stay unavailable, lose nothing) or elect whatever replica comes back first (available now, possibly losing acknowledged data). Kafka defaults to waiting.

**How ELR helps.** With strict min ISR (see Q489), the high watermark can't advance while ISR < `min.insync.replicas`. So a replica that dropped out of the ISR *during* that period still has every committed record. The controller stores such replicas as the ELR. Election order is now:
1. A live ISR member.
2. An unfenced ELR member.
3. The last known leader, if it comes back.
4. Only if unclean election is enabled (or forced), any live replica.

**Turning on unclean election.** Set the topic or broker config, then either wait for the controller's periodic unclean-election check (about 5 minutes) or force it now with `kafka-leader-election.sh --election-type UNCLEAN`. The 4.3 config docs point this out.

**When people enable it**
- Metrics, logs and other data where availability matters more than a few lost records.
- As a manual, last-resort step during an outage, after a decision to accept loss. Set it back afterwards.

**Never** leave it on for ledgers, orders, CDC or compacted state topics.

**Example / Code:**
```bash
# Runtime: Apache Kafka 4.3.1 lab: 1 KRaft controller + 3 brokers on one host; Python 3.11 + confluent-kafka 2.16
source ../env.sh; C=localhost:29091,localhost:29092,localhost:29093
desc(){ kafka-topics.sh --bootstrap-server $C --describe --topic $1 2>/dev/null | grep 'Partition:' | sed 's/^\s*//'; }
offs(){ echo "  end offset: $(kafka-get-offsets.sh --bootstrap-server $C --topic $1 2>/dev/null)"; }
down(){ bash ../cluster/stop.sh $1 >/dev/null; }
up(){ bash ../cluster/start.sh $@ >/dev/null; sleep 6; }
T=$1; MODE=$2                       # MODE = elr | unclean
kafka-topics.sh --bootstrap-server $C --create --topic $T --replica-assignment 1:2:3 --config min.insync.replicas=2 >/dev/null
python send.py all "r0" $T
down 3; python send.py all "r1 (written while broker 3 was down)" $T
down 2; down 1; echo "## brokers 3, 2, 1 stopped in that order"; desc $T
up 3; echo "## broker 3 back (it missed r1)"; desc $T
if [ "$MODE" = elr ]; then
  up 2; echo "## broker 2 back (it is in the ELR)"; desc $T; offs $T
else
  kafka-configs.sh --bootstrap-server $C --alter --entity-type topics --entity-name $T --add-config unclean.leader.election.enable=true >/dev/null
  echo "## unclean.leader.election.enable=true, then trigger the election now"
  kafka-leader-election.sh --bootstrap-server $C --election-type UNCLEAN --topic $T --partition 0 2>&1 | grep -v WARN
  sleep 3; desc $T; offs $T
  up 1 2; desc $T; offs $T
fi
up 1 2 2>/dev/null
```
Run 1, `bash elr_exp.sh acct elr` (wait for the ELR member):
```text
acks=all 'r0': written at offset 0
acks=all 'r1 (written while broker 3 was down)': written at offset 1
## brokers 3, 2, 1 stopped in that order
## broker 3 back (it missed r1)
Topic: acct	Partition: 0	Leader: none	Replicas: 1,2,3	Isr: 	Elr: 1,2	LastKnownElr: 1
## broker 2 back (it is in the ELR)
Topic: acct	Partition: 0	Leader: 2	Replicas: 1,2,3	Isr: 2,3	Elr: 	LastKnownElr: 
  end offset: acct:0:2
```
Run 2, `bash elr_exp.sh acct5 unclean` (force an unclean election instead):
```text
## broker 3 back (it missed r1)
Topic: acct5	Partition: 0	Leader: none	Replicas: 1,2,3	Isr: 	Elr: 1,2	LastKnownElr: 1
## unclean.leader.election.enable=true, then trigger the election now
Successfully completed leader election (UNCLEAN) for partitions acct5-0
Topic: acct5	Partition: 0	Leader: 3	Replicas: 1,2,3	Isr: 3	Elr: 	LastKnownElr: 
Topic: acct5	Partition: 0	Leader: 3	Replicas: 1,2,3	Isr: 1,2,3	Elr: 	LastKnownElr: 
  end offset: acct5:0:1
```
Broker 3 alone wasn't eligible, so the partition stayed offline rather than lose data. Waiting for broker 2 (an ELR member) gave a clean leader with both records. Forcing an unclean election made broker 3 leader at once. When brokers 1 and 2 rejoined they truncated to its log: the acknowledged `r1` was gone for good (end offset 1 instead of 2).

**Real-World Use Case:**
A logging cluster lost two of three brokers in a rack failure. For the `app-logs` topics, the on-call engineer forced unclean elections to restore ingestion within minutes, accepting a small gap. For `payments` they waited about twenty minutes for an in-sync broker to come back, which kept every acknowledged write.

**Common Mistakes / Interviewer Red Flags:**
- Enabling unclean election cluster-wide "to improve availability".
- Expecting a dynamic config change to elect a leader instantly; it waits for the periodic check unless you trigger it.
- Not knowing that the truncated records are gone even after the old replicas return.
- Confusing the ELR with the ISR. ELR members are safe to elect but are not receiving writes as in-sync followers.

**Follow-up Questions Interviewers Ask:**
- How does `min.insync.replicas` relate to ELR? — ELR relies on strict min ISR: because the HW freezes below min ISR, replicas that left the ISR in that window still hold all committed data.
- Where do you see ELR state? — `kafka-topics.sh --describe` shows `Elr` and `LastKnownElr`; the DescribeTopicPartitions API returns it.
- What changes for broker-level `min.insync.replicas` once ELR is on? — It moves to the cluster level; the broker-level value is removed and can't be altered.

---
### Q491. What happens when a broker fails, and how does leadership recover?
**ID:** KFK-23
**Topic:** Kafka > Replication & Durability
**Level:** Beginner
**Frequency:** Very Common
**Tags:** `kafka`, `failover`, `leader-election`, `under-replicated-partitions`

**Short Answer (2–3 lines):**
The controller notices the broker has stopped heartbeating and fences it. For every partition it led, a new leader is picked from the remaining ISR, and clients find the new leaders through metadata refresh and retry. The partitions run under-replicated until the broker returns and catches up. Leadership then moves back to the "preferred" replica, automatically or with `kafka-leader-election.sh`.

**Detailed Explanation:**
**Timeline of a hard crash**
1. Brokers heartbeat to the active controller. After `broker.session.timeout.ms` (9 s by default) with no heartbeat, the controller fences the broker.
2. For each partition the broker led, the controller elects a new leader from the ISR (or the ELR, see Q490) and writes the change to the metadata log.
3. Brokers apply the change. Producers and consumers get `NOT_LEADER_OR_FOLLOWER`, refresh metadata and retry, which is invisible to the application with default retries.
4. The failed broker is also removed from the ISR of partitions it followed. Those partitions are now **under-replicated**.
5. When it restarts, the broker truncates any divergent tail (using leader epochs), catches up and rejoins the ISR.
6. **Preferred leader** = first replica in the assignment list. With `auto.leader.rebalance.enable=true` (the default) the controller moves leadership back to preferred replicas on a timer, every `leader.imbalance.check.interval.seconds` (300 s). The old `leader.imbalance.per.broker.percentage` threshold isn't in the 4.3 broker config list. You can force it at once with `kafka-leader-election.sh --election-type PREFERRED`.

**Controlled shutdown** (SIGTERM) is gentler: the broker asks the controller to move its leaderships away first, so clients see almost no errors. Always do rolling restarts this way, one broker at a time, waiting for zero under-replicated partitions between brokers.

**What a client notices**
- `acks=all` with `min.insync.replicas=2` and RF 3: one broker down, writes keep succeeding.
- A latency spike for a few seconds while leadership moves.
- Consumers keep reading from new leaders. Groups don't rebalance unless consumer processes themselves failed.

**Example / Code:**
```bash
# Runtime: Apache Kafka 4.3.1 lab: 1 KRaft controller + 3 brokers on one host
source ../env.sh; C=localhost:29091,localhost:29092,localhost:29093
leaders(){ kafka-topics.sh --bootstrap-server $C --describe --topic orders4 2>/dev/null | awk '/Partition:/ {printf "p%s:L%s(isr %s) ", $4, $6, $10} END {print ""}'; }
kafka-topics.sh --bootstrap-server $C --create --topic orders4 --partitions 6 --replication-factor 3 --config min.insync.replicas=2 >/dev/null
sleep 2; echo "start           : $(leaders)"
bash ../cluster/stop.sh 1 -9 >/dev/null                       # hard crash, no controlled shutdown
sleep 12; echo "broker 1 killed : $(leaders)"
kafka-topics.sh --bootstrap-server $C --describe --under-replicated-partitions 2>/dev/null | grep -c Partition: | sed "s/^/under-replicated partitions (all topics): /"
bash ../cluster/start.sh 1 >/dev/null; sleep 10   # restart broker 1
# preferred.json: {"partitions":[{"topic":"orders4","partition":2},{"topic":"orders4","partition":4}]}
echo "broker 1 back   : $(leaders)"
kafka-leader-election.sh --bootstrap-server $C --election-type PREFERRED --path-to-json-file preferred.json
echo "after preferred : $(leaders)"
```
```text
start           : p0:L2(isr 2,3,1) p1:L3(isr 3,1,2) p2:L1(isr 1,2,3) p3:L2(isr 2,1,3) p4:L1(isr 1,3,2) p5:L3(isr 3,2,1)
broker 1 killed : p0:L2(isr 2,3) p1:L3(isr 3,2) p2:L2(isr 2,3) p3:L2(isr 2,3) p4:L3(isr 3,2) p5:L3(isr 3,2)
under-replicated partitions (all topics): 13
broker 1 back   : p0:L2(isr 1,2,3) p1:L3(isr 1,2,3) p2:L2(isr 1,2,3) p3:L2(isr 1,2,3) p4:L3(isr 1,2,3) p5:L3(isr 1,2,3)
Successfully completed leader election (PREFERRED) for partitions orders4-4, orders4-2
after preferred : p0:L2(isr 1,2,3) p1:L3(isr 1,2,3) p2:L1(isr 1,2,3) p3:L2(isr 1,2,3) p4:L1(isr 1,2,3) p5:L3(isr 1,2,3)
```
Killing broker 1 moved its two leaderships (p2, p4) to broker 2 and broker 3 within seconds, and every partition dropped to two in-sync replicas. After the restart, broker 1 rejoined every ISR but led nothing. Within the run the automatic rebalance (5-minute timer) hadn't fired yet, so a preferred election moved p2 and p4 back.

**Real-World Use Case:**
During a rolling OS patch, an engineer restarted two brokers within a minute. Several partitions dropped to one in-sync replica and `acks=all` producers stalled. The runbook now waits for `UnderReplicatedPartitions == 0` (and preferred leaders restored) before touching the next broker.

**Common Mistakes / Interviewer Red Flags:**
- Thinking a consumer group rebalances when a broker fails.
- Restarting the next broker while partitions are still under-replicated.
- Forgetting preferred leader election, which leaves one broker leading most partitions after a restart.
- Using `kill -9` for routine restarts instead of a controlled shutdown.

**Follow-up Questions Interviewers Ask:**
- How do you find affected partitions? — `kafka-topics.sh --describe --under-replicated-partitions`, `--under-min-isr-partitions` and `--unavailable-partitions`.
- What if the failed broker's disk is lost? — Replace it with the same `node.id` and empty disks; it re-replicates everything from leaders (throttle it).
- How long does failover take? — Detection (session timeout) plus a fast metadata update; with KRaft, usually seconds.

---

### Q492. Rack awareness and fetching from the closest replica
**ID:** KFK-24
**Topic:** Kafka > Replication & Durability
**Level:** Advanced
**Frequency:** Occasional
**Tags:** `kafka`, `rack-awareness`, `follower-fetching`, `multi-az`, `cost`

**Short Answer (2–3 lines):**
Setting `broker.rack` to the availability zone makes Kafka spread each partition's replicas across zones, so losing one zone loses at most one copy. With `replica.selector.class=RackAwareReplicaSelector` on brokers and `client.rack` on consumers (KIP-392), consumers read from a replica in their own zone instead of the leader. That cuts cross-zone traffic, which is often a large part of a cloud Kafka bill.

**Detailed Explanation:**
**Replica placement.** With `broker.rack` set, topic creation and reassignment tools place replicas in different racks where possible. Three zones with RF 3 put one replica per zone. Combine with `min.insync.replicas=2`, and a whole zone can fail without losing acknowledged writes or `acks=all` availability.

**Where the cross-zone bytes come from (RF 3, three zones, producer in one zone)**
- Producer → leader: about two-thirds of produce traffic crosses zones on average (the leader is in another zone two times in three).
- Leader → two followers: always cross-zone, so 2× the produce volume.
- Leader → consumers: about two-thirds cross-zone per consumer group, unless consumers fetch from a same-zone follower.

Replication is unavoidable for durability. Consumer traffic is the part you can remove, and with many consumer groups it's often the biggest.

**Follower fetching set-up**
```properties
# broker
broker.rack=use1-az1
replica.selector.class=org.apache.kafka.common.replica.RackAwareReplicaSelector
# consumer
client.rack=use1-az1
```
The leader still serves the first fetch and redirects the consumer to a preferred read replica in its rack.

**Trade-offs**
- Slightly higher end-to-end latency: followers only expose data up to the high watermark they've learned, which trails the leader a little.
- If the follower falls out of the ISR, the consumer is sent back to the leader.
- Producers can't do this; writes always go to the leader. Some managed services offer zone-aware routing for producers in their own ways.

**Example / Code:**
```python
# Runtime: Python 3.11, confluent-kafka 2.16; Kafka 4.3.1 lab: brokers 1-3 with broker.rack=az-1..az-3 and
#   replica.selector.class=org.apache.kafka.common.replica.RackAwareReplicaSelector
# Topic created first with: kafka-topics.sh --create --topic rack-demo --replica-assignment 1:2:3
# (one partition, replicas 1,2,3, so broker 1 in az-1 is the leader)
import logging, re, time
from confluent_kafka import Consumer, Producer

BOOT = "localhost:29091,localhost:29092,localhost:29093"
p = Producer({"bootstrap.servers": BOOT})
for i in range(100):
    p.produce("rack-demo", value=f"event-{i}")
p.flush()

def fetched_from(client_rack):
    lines = []
    class Grab(logging.Handler):
        def emit(self, record):
            lines.append(record.getMessage())
    log = logging.getLogger(f"rk-{client_rack}"); log.addHandler(Grab()); log.setLevel(logging.DEBUG)
    c = Consumer({"bootstrap.servers": BOOT, "group.id": f"rack-{client_rack}-{time.time()}",
                  "auto.offset.reset": "earliest", "client.rack": client_rack, "debug": "fetch"},
                 logger=log)
    c.subscribe(["rack-demo"])
    got, end = 0, time.time() + 20
    while got < 100 and time.time() < end:
        m = c.poll(0.5)
        if m and not m.error():
            got += 1
    c.close()
    # librdkafka logs each fetch as "...localhost:2909N/N: Fetch topic rack-demo [0] at offset ..."
    brokers = sorted({int(b) for b in re.findall(r"localhost:2909\d/(\d+): Fetch topic rack-demo", "\n".join(lines))})
    return got, brokers

for rack in ("az-1", "az-3"):
    got, brokers = fetched_from(rack)
    print(f"client.rack={rack}: consumed {got} records, fetch requests went to broker(s) {brokers}")
```
```text
client.rack=az-1: consumed 100 records, fetch requests went to broker(s) [1]
client.rack=az-3: consumed 100 records, fetch requests went to broker(s) [1, 3]
```
A consumer in the leader's zone fetched only from broker 1. A consumer in `az-3` made its first fetch from the leader, was told to use the `az-3` replica, and fetched from broker 3 after that.

**Real-World Use Case:**
A platform with a dozen consumer groups on a 3-zone cluster found that cross-zone transfer cost more than the brokers themselves. Enabling follower fetching and setting `client.rack` from the Kubernetes node's zone label removed most consumer-side cross-zone traffic, with only a small latency increase.

**Common Mistakes / Interviewer Red Flags:**
- Setting `broker.rack` but not `replica.selector.class`, then expecting consumers to fetch locally.
- Hard-coding `client.rack` in images instead of reading the zone at start-up.
- Assuming rack awareness also balances leaders across zones; leadership still follows the preferred replica.

**Follow-up Questions Interviewers Ask:**
- Does rack awareness help when a partition has RF 2 across three zones? — Partly: each copy is in a different zone, but losing one zone leaves a single copy.
- Is there a producer-side equivalent? — Not in open-source Kafka; producers always write to the leader.
- How do you verify follower fetching? — Client debug logs or broker fetch metrics per replica show fetches arriving at followers.

---
### Q493. Exactly-once from Kafka into an external database
**ID:** KFK-25
**Topic:** Kafka > Delivery Semantics & Transactions
**Level:** Advanced
**Frequency:** Common
**Tags:** `kafka`, `exactly-once`, `sink`, `idempotency`, `offsets`

**Short Answer (2–3 lines):**
Kafka transactions stop at Kafka's edge, so a database sink can't join them. You get an exactly-once *effect* in one of two ways. Make writes idempotent (upsert on a business key or event ID), so replays do no harm. Or store the consumed offsets in the same database transaction as the results, and on start-up seek to the offsets kept in the database.

**Detailed Explanation:**
What EOS means inside Kafka (idempotent producer, transactions, `read_committed`) is covered in Q70 and on this site's exactly-once page. The real-world problem is the hop *out* of Kafka.

**Why "commit after write" isn't enough.** Write row, then commit offset: a crash between the two replays the row (a duplicate). Commit offset, then write: a crash loses the row. No order of two separate systems is atomic.

**Pattern 1: idempotent sink (most common)**
- `MERGE`/`UPSERT` keyed by a natural or event ID. Replaying the same event produces the same row.
- Version or sequence guards: `UPDATE … WHERE incoming_seq > current_seq`. This also protects against out-of-order replays.
- Deduplication table of processed `event_id`s with a TTL, for append-only facts.
- Offsets stay in Kafka with ordinary at-least-once commits.

**Pattern 2: transactional offsets in the sink**
- One database transaction holds the result rows and an `(topic, partition, next_offset)` row.
- On assignment (`on_assign`) read the stored offsets and `seek`. Kafka's committed offsets are only for monitoring.
- Works with any transactional store: Postgres, MySQL, or Delta and Iceberg commits, which is how Spark and Flink sinks do it.

**Pattern 3: two-phase commit**
- Flink's `TwoPhaseCommitSinkFunction` style: pre-commit on checkpoint, commit on checkpoint completion (see Q516). It needs a sink that supports prepared transactions or idempotent finalisation.

**Aggregations** (`amount = amount + x`) are not naturally idempotent. That makes Pattern 2 or a dedup key essential: the demo below counts events, which would double-count under plain at-least-once.

**Example / Code:**
```python
# Runtime: Python 3.11 (sqlite3 stdlib), confluent-kafka 2.16, Apache Kafka 4.3
# Exactly-once *effect* into an external database: results and offsets commit in ONE db transaction,
# and on (re)assignment the consumer seeks to the offsets stored in the database, not Kafka's.
import sqlite3, sys
from confluent_kafka import Consumer, TopicPartition

db = sqlite3.connect("ledger.db")
db.execute("CREATE TABLE IF NOT EXISTS balances (account TEXT PRIMARY KEY, amount INT)")
db.execute("CREATE TABLE IF NOT EXISTS kafka_offsets (topic TEXT, part INT, next_offset INT, PRIMARY KEY (topic, part))")
db.commit()

def on_assign(consumer, partitions):
    for tp in partitions:
        row = db.execute("SELECT next_offset FROM kafka_offsets WHERE topic=? AND part=?",
                         (tp.topic, tp.partition)).fetchone()
        tp.offset = row[0] if row else 0          # resume from the DB, the source of truth
    consumer.assign(partitions)

crash_after = int(sys.argv[1]) if len(sys.argv) > 1 else 10**9
c = Consumer({"bootstrap.servers": "localhost:19092", "group.id": "balance-sink",
              "enable.auto.commit": False, "isolation.level": "read_committed",
              "session.timeout.ms": 6000})
c.subscribe(["ledger-events"], on_assign=on_assign)
n, idle = 0, 0
while idle < 5:
    m = c.poll(1.0)
    if m is None or m.error():
        idle += 1 if c.assignment() else 0   # only count idle polls once we own partitions
        continue
    idle = 0
    with db:                                      # one atomic transaction
        db.execute("INSERT INTO balances VALUES ('acc-1', 1) "
                   "ON CONFLICT(account) DO UPDATE SET amount = amount + 1")
        db.execute("INSERT OR REPLACE INTO kafka_offsets VALUES (?, ?, ?)",
                   (m.topic(), m.partition(), m.offset() + 1))
    n += 1
    if n == crash_after:
        print(f"crashing after {n} records"); import os; os._exit(1)
print("processed", n, "->", db.execute("SELECT amount FROM balances").fetchone()[0],
      "events counted; stored offset", db.execute("SELECT next_offset FROM kafka_offsets").fetchone()[0])
c.close()
```
Run once with `7` (crash after seven records), then twice with no argument, against the 20-record `ledger-events` topic:
```text
crashing after 7 records
processed 13 -> 20 events counted; stored offset 20
processed 0 -> 20 events counted; stored offset 20
```
The restart resumed at offset 7 from the database, not from Kafka's committed offset (which this consumer never writes). The count is exactly 20 despite the crash and a third run.

**Real-World Use Case:**
A billing team summed usage events into Postgres with at-least-once commits. After every consumer restart, invoices were slightly inflated. They moved the `kafka_offsets` table into the same transaction as the usage aggregates and seeked on assignment. Restarts and rebalances then stopped changing totals.

**Common Mistakes / Interviewer Red Flags:**
- Claiming `enable.idempotence` or Kafka transactions make a database sink exactly-once.
- Storing offsets in the database but still letting the consumer start from Kafka's committed offsets.
- Using `INSERT` without a unique key and hoping for no duplicates.
- Forgetting that rebalances can move a partition to a member that hasn't read the DB offsets.

**Follow-up Questions Interviewers Ask:**
- How does the Kafka Connect JDBC sink handle this? — With `insert.mode=upsert` and primary keys from the record key, which is Pattern 1.
- What about S3 or a lake? — Use deterministic file names per offset range, or table-format commits that record offsets in their metadata.
- Is idempotency enough for side effects like sending emails? — Only with a dedup store keyed by event ID, checked before sending.

---

### Q494. Write a transactional consume-transform-produce loop
**ID:** KFK-26
**Topic:** Kafka > Delivery Semantics & Transactions
**Level:** Advanced
**Frequency:** Common
**Tags:** `kafka`, `transactions`, `exactly-once`, `read-committed`, `python`

**Short Answer (2–3 lines):**
Call `init_transactions()` once, with a stable `transactional.id`. Then, for each batch: `begin_transaction()`, produce the outputs, `send_offsets_to_transaction()` with the consumer's positions and group metadata, and `commit_transaction()`. On a failure that can be aborted, call `abort_transaction()` and rewind the consumer. Downstream consumers must use `read_committed`, or they will see aborted records.

**Detailed Explanation:**
Q70 explains what transactions are. This question is about writing the loop correctly.

**The pieces**
| Step | Why |
|---|---|
| `transactional.id` | Identity that survives restarts; `init_transactions()` bumps the epoch and fences zombies |
| `isolation.level=read_committed` on the input consumer | Don't process another app's aborted data |
| `enable.auto.commit=false` | Offsets are committed *inside* the transaction, not separately |
| `send_offsets_to_transaction(positions, group_metadata)` | Ties input progress to output records atomically |
| `commit_transaction()` | Writes commit markers to every output partition and `__consumer_offsets` |

**Error handling**
- *Abortable* errors (your own exception, a timeout in the transaction): `abort_transaction()`, then seek back to the first offset of the batch. Otherwise the consumer's in-memory position has already moved past it.
- *Fatal* errors (`ProducerFenced`, `InvalidProducerEpoch`): another instance owns the `transactional.id`. Close and exit; don't retry.
- `transaction.timeout.ms` (60 s by default) must cover a batch. The coordinator aborts transactions left open longer, and long transactions hold back the last stable offset for every `read_committed` reader.

**`transactional.id` strategy.** Since KIP-447 (Kafka 2.5+), fencing also uses the consumer group generation. One stable ID per application instance is enough; you no longer need one per input partition.

**Kafka 4.0+ (KIP-890, `transaction.version=2`)**: the broker verifies that each write belongs to an ongoing transaction, and the epoch is bumped on every transaction. This closes "hanging transaction" gaps. Clients get it automatically once the feature is finalised (this lab cluster shows `transaction.version` at level 2).

**Cost.** Each commit adds round trips and a marker record per partition, so batch several hundred records or a few hundred milliseconds per transaction.

**Example / Code:**
```python
# Runtime: Python 3.11, confluent-kafka 2.16, Apache Kafka 4.3 (transaction.version=2)
# Consume-transform-produce with Kafka transactions: output records and input offsets commit atomically.
from confluent_kafka import Consumer, Producer, TopicPartition

consumer = Consumer({"bootstrap.servers": "localhost:19092", "group.id": "enricher",
                     "auto.offset.reset": "earliest", "enable.auto.commit": False,
                     "isolation.level": "read_committed"})
producer = Producer({"bootstrap.servers": "localhost:19092",
                     "transactional.id": "enricher-tx-0",    # stable per instance/partition set
                     "transaction.timeout.ms": 60000})
producer.init_transactions()          # fences any older producer with the same transactional.id
consumer.subscribe(["tx-in"])

batch_no, idle, failed_once = 0, 0, False
while idle < 5:
    msgs = consumer.consume(num_messages=3, timeout=1.0)
    msgs = [m for m in msgs if not m.error()]
    if not msgs:
        idle += 1 if consumer.assignment() else 0; continue
    idle, batch_no = 0, batch_no + 1
    producer.begin_transaction()
    try:
        for m in msgs:
            producer.produce("tx-out", key=m.value(), value=m.value().upper())
        if batch_no == 2 and not failed_once:
            failed_once = True
            producer.flush()          # records reach the broker, then we fail before commit
            raise RuntimeError("simulated failure mid-batch")
        producer.send_offsets_to_transaction(consumer.position(consumer.assignment()),
                                             consumer.consumer_group_metadata())
        producer.commit_transaction()
        print(f"batch {batch_no}: committed {[m.value().decode() for m in msgs]}")
    except RuntimeError as e:
        producer.abort_transaction()
        print(f"batch {batch_no}: aborted ({e}); rewinding")
        first = msgs[0]               # rewind to the first record of the failed batch
        consumer.seek(TopicPartition(first.topic(), first.partition(), first.offset()))
consumer.close()
```
Input: `order-1` … `order-9` on `tx-in`. Output, then both isolation levels reading `tx-out`:
```text
batch 1: committed ['order-1', 'order-2', 'order-3']
batch 2: aborted (simulated failure mid-batch); rewinding
batch 3: committed ['order-4', 'order-5', 'order-6']
batch 4: committed ['order-7', 'order-8', 'order-9']
== read_uncommitted
ORDER-1 ORDER-2 ORDER-3 ORDER-4 ORDER-5 ORDER-6 ORDER-4 ORDER-5 ORDER-6 ORDER-7 ORDER-8 ORDER-9 
== read_committed
ORDER-1 ORDER-2 ORDER-3 ORDER-4 ORDER-5 ORDER-6 ORDER-7 ORDER-8 ORDER-9 
```
The aborted batch reached the log (flushed before the failure), so a `read_uncommitted` reader saw `ORDER-4..6` twice. A `read_committed` reader saw each exactly once. `kafka-get-offsets.sh` reported an end offset of 16: 12 data records plus 4 transaction markers, which is why offsets on transactional topics have gaps.

**Real-World Use Case:**
A currency-normalisation service read `payments.raw` and wrote `payments.normalised`. Under at-least-once, every redeploy produced a few duplicate payments, which downstream reconciliation flagged. Moving to this loop with `read_committed` consumers downstream removed the duplicates. The team batched 500 records per transaction to keep throughput.

**Common Mistakes / Interviewer Red Flags:**
- Committing offsets with `consumer.commit()` as well as inside the transaction.
- Not seeking back after an abort, which silently skips the failed batch.
- Leaving downstream consumers on Java's default `read_uncommitted` (librdkafka defaults to `read_committed`).
- Generating a random `transactional.id` per start, which disables zombie fencing.

**Follow-up Questions Interviewers Ask:**
- What does the transaction coordinator store? — Transaction state per `transactional.id` in `__transaction_state`, including the partitions involved.
- Why do `read_committed` consumers sometimes stall? — An open transaction holds the last stable offset back until it commits, aborts or times out.
- Does this work across clusters? — No; a transaction is scoped to one cluster.

---
### Q495. How Kafka stores a partition on disk: segments, indexes and deletion
**ID:** KFK-27
**Topic:** Kafka > Storage & Retention
**Level:** Intermediate
**Frequency:** Common
**Tags:** `kafka`, `segments`, `storage`, `retention`, `indexes`

**Short Answer (2–3 lines):**
A partition is a directory of **segments**. Each segment is a `.log` file of record batches, plus an `.index` (offset to file position) and a `.timeindex` (timestamp to offset), named after the segment's first offset. Writes go only to the newest, active segment. Retention and compaction work on whole closed segments, so data is deleted a segment at a time, never record by record.

**Detailed Explanation:**
**Files in `<log.dirs>/<topic>-<partition>/`**
| File | Purpose |
|---|---|
| `00000000000000006666.log` | Record batches starting at offset 6666 |
| `.index` | Sparse map offset → byte position, one entry per `index.interval.bytes` (4 KiB) |
| `.timeindex` | Sparse map timestamp → offset, used by `offsets_for_times` and time-based resets |
| `.snapshot` | Producer-state snapshot, used for idempotence and transactions after restart |
| `leader-epoch-checkpoint` | Epoch → start offset, used for truncation after failover |
| `partition.metadata` | Topic ID |

**Rolling.** The active segment closes ("rolls") when it reaches `segment.bytes` (1 GiB) or `segment.ms` (7 days) old, or when its index fills up. The output below shows the index pre-allocated at 10 MB for the active segment and trimmed when the segment rolls.

**Why segments matter in interviews**
- **Retention lags.** A segment is deleted only when it's closed *and* wholly past `retention.ms` (judged by its largest timestamp), or when deleting it would still leave the partition at or above `retention.bytes`. On a low-traffic topic with 1 GiB segments, a "7-day" topic can keep months of data, because the active segment never rolls by size. Lower `segment.ms` for those topics.
- **Retention is checked periodically**, every `log.retention.check.interval.ms` (5 minutes), and files are removed after `file.delete.delay.ms`.
- **Compaction never touches the active segment** (see Q71).
- **Reads are cheap**: a fetch binary-searches the sparse index, then streams bytes straight from the page cache to the socket (zero-copy).
- **Many partitions mean many open files and mapped indexes** per broker, which is one reason partition counts have practical limits (see Q510).

**Example / Code:**
```bash
# Runtime: Apache Kafka 4.3.1, single broker; topic created with --config segment.bytes=1048576
bin/kafka-producer-perf-test.sh --bootstrap-server localhost:19092 --topic segdemo \
  --num-records 20000 --throughput -1 --payload-file payload.jsonl --command-property linger.ms=10
ls -l data/segdemo-0/ | awk '{print $5, $9}'
bin/kafka-dump-log.sh --files data/segdemo-0/00000000000000000000.log | head -4
bin/kafka-dump-log.sh --files data/segdemo-0/00000000000000000000.index | head -4
```
```text
504 00000000000000000000.index
1041493 00000000000000000000.log
468 00000000000000000000.timeindex
504 00000000000000006666.index
1041737 00000000000000006666.log
56 00000000000000006666.snapshot
276 00000000000000006666.timeindex
10485760 00000000000000013334.index
1041434 00000000000000013334.log
56 00000000000000013334.snapshot
10485756 00000000000000013334.timeindex
8 leader-epoch-checkpoint
43 partition.metadata
Dumping data/segdemo-0/00000000000000000000.log
Log starting offset: 0
baseOffset: 0 lastOffset: 103 count: 104 baseSequence: 0 lastSequence: 103 producerId: 1002 producerEpoch: 0 partitionLeaderEpoch: 0 isTransactional: false isControl: false deleteHorizonMs: OptionalLong.empty position: 0 CreateTime: 1792262973044 size: 16267 ...
Dumping data/segdemo-0/00000000000000000000.index
offset: 207 position: 16267
offset: 311 position: 32536
offset: 415 position: 48828
```
Each line of the dump is a *batch* (104 records here, with the idempotent producer's ID and sequence numbers). Setting `retention.bytes=2200000` on the 3.1 MB partition deleted nothing, because removing the oldest 1.04 MB segment would have taken the partition below the limit. Kafka only deletes a segment if the partition is still at least `retention.bytes` after the deletion. With `retention.bytes=2000000`, the next retention check (about four minutes later) logged `Incremented log start offset to 6666 due to segment deletion`, and `kafka-get-offsets.sh --time earliest` moved from `segdemo:0:0` to `segdemo:0:6666`.

**Real-World Use Case:**
A compliance team required that `audit-events` keep no more than 30 days. The topic had `retention.ms` set correctly, but it received only a few MB a day, so its single 1 GiB segment never rolled and nothing was ever deleted. Setting `segment.ms=86400000` (one day) made deletion happen on schedule.

**Common Mistakes / Interviewer Red Flags:**
- Believing retention deletes individual records the moment they expire.
- Forgetting that the active segment is never deleted or compacted.
- Setting tiny `segment.bytes` cluster-wide, which creates huge numbers of files.

**Follow-up Questions Interviewers Ask:**
- How do you delete data before retention would? — `kafka-delete-records.sh` moves the log start offset forward; whole segments below it are then removed.
- What does `kafka-dump-log.sh --print-data-log` add? — The record keys and values, which is useful when debugging a poison message.
- What happens on unclean shutdown? — The broker recovers segments after the last checkpoint and rebuilds indexes, which can make start-up slow on large logs.

---

### Q496. Record timestamps: CreateTime vs LogAppendTime, and how they affect retention and replay
**ID:** KFK-28
**Topic:** Kafka > Storage & Retention
**Level:** Intermediate
**Frequency:** Occasional
**Tags:** `kafka`, `timestamps`, `retention`, `event-time`, `configuration`

**Short Answer (2–3 lines):**
Every record carries a timestamp. With `message.timestamp.type=CreateTime` (the default) it's whatever the producer set. With `LogAppendTime` the broker overwrites it with its own clock. Time-based retention, `offsets_for_times`, `--to-datetime` resets and stream windows all use this value. A wrong producer clock or replayed historical data can make records expire early, or get rejected by the broker.

**Detailed Explanation:**
| | `CreateTime` | `LogAppendTime` |
|---|---|---|
| Who sets it | Producer (default: send time; can be the event time) | Broker at append |
| Meaning | Event time, if the producer sets it carefully | Ingestion time |
| Validation | Rejected if more than `message.timestamp.after.max.ms` (1 h) in the future or more than `message.timestamp.before.max.ms` (unlimited by default) in the past | None; validation settings are ignored |
| Good for | Stream processing on event time | Retention and replay by arrival time, audit |

**How timestamps reach retention.** A segment is eligible for time-based deletion when its **largest** timestamp is older than `retention.ms`. So:
- Backfilling year-old events with `CreateTime` makes their segments eligible almost as soon as they roll. They may vanish within minutes.
- One record with a far-future timestamp would keep its segment alive. That's one reason Kafka 4.0 changed the default `message.timestamp.after.max.ms` from unlimited to 1 hour (KIP-1030). The before/after settings themselves arrived in 3.6 (KIP-937).

**Batch-level rejection.** Timestamp validation happens per batch. If one record in a batch is out of range, the whole batch fails with `INVALID_TIMESTAMP`, including valid records that happened to share it. The first run of the demo below batched both test records together, and both were rejected.

**Stream processing.** Kafka Streams and Flink extract event time from the record timestamp by default (see Q508 and Q29). If producers use `LogAppendTime`, windows reflect arrival time, not when things happened. Pick deliberately, and carry the business event time in the payload as well.

**Example / Code:**
```bash
# Runtime: Apache Kafka 4.3.1 CLI
bin/kafka-topics.sh --bootstrap-server localhost:19092 --create --topic ts-create --partitions 1
bin/kafka-topics.sh --bootstrap-server localhost:19092 --create --topic ts-append --partitions 1 \
  --config message.timestamp.type=LogAppendTime
```
```python
# Runtime: Python 3.11, confluent-kafka 2.16, Apache Kafka 4.3
import time
from datetime import datetime, timezone
from confluent_kafka import Producer, Consumer, TIMESTAMP_CREATE_TIME

now = int(time.time() * 1000)
cases = {"3 days old": now - 3 * 86400_000, "2 hours in future": now + 2 * 3600_000}
p = Producer({"bootstrap.servers": "localhost:19092"})
for topic in ("ts-create", "ts-append"):
    for label, ts in cases.items():
        def cb(err, msg, label=label, topic=topic):
            print(f"{topic:9} {label:17} -> " + (err.name() if err else f"stored at offset {msg.offset()}"))
        p.produce(topic, value=label, timestamp=ts, on_delivery=cb)
        p.flush()      # one record per batch: a bad timestamp fails the WHOLE batch it is in

c = Consumer({"bootstrap.servers": "localhost:19092", "group.id": f"ts-{now}", "auto.offset.reset": "earliest"})
c.subscribe(["ts-create", "ts-append"])
got = 0
while got < 3:
    m = c.poll(5)
    if m is None or m.error():
        continue
    got += 1
    kind, ts = m.timestamp()
    kind = "CreateTime" if kind == TIMESTAMP_CREATE_TIME else "LogAppendTime"
    print(f"read {m.topic():9} {m.value().decode():17} {kind:13} {datetime.fromtimestamp(ts / 1000, timezone.utc):%Y-%m-%d %H:%M}")
c.close()
```
```text
ts-create 3 days old        -> stored at offset 0
ts-create 2 hours in future -> INVALID_TIMESTAMP
ts-append 3 days old        -> stored at offset 0
ts-append 2 hours in future -> stored at offset 1
read ts-create 3 days old        CreateTime    2026-10-14 18:59
read ts-append 3 days old        LogAppendTime 2026-10-17 18:59
read ts-append 2 hours in future LogAppendTime 2026-10-17 18:59
```
(Run at 2026-10-17 18:59 UTC.) The `CreateTime` topic kept the producer's timestamp and rejected the future one. The `LogAppendTime` topic accepted both and replaced their timestamps with the append time.

**Real-World Use Case:**
A team replayed six months of orders from an archive into a 7-day topic, keeping the original event times as `CreateTime`. Downstream consumers started late and found most of the data already gone, because whole segments were past retention as soon as they rolled. Replaying into a topic with longer retention fixed it. `LogAppendTime` would have too, at the cost of losing event time in the record timestamp.

**Common Mistakes / Interviewer Red Flags:**
- Assuming retention is measured from when the broker received the record.
- Backfilling historical events into a short-retention `CreateTime` topic.
- Not knowing that a single bad timestamp fails its whole batch.

**Follow-up Questions Interviewers Ask:**
- Can a consumer tell which type a record has? — Yes; the timestamp type is returned with each record (`msg.timestamp()` in Python).
- What happens to timestamps when MirrorMaker 2 copies data? — It keeps the source timestamps, so the target topic's timestamp type and limits matter.
- How do you find the offset for 09:00 yesterday? — `offsets_for_times`, which uses the time index (see Q482).

---

### Q497. Tiered storage: how it works and what it changes operationally
**ID:** KFK-29
**Topic:** Kafka > Storage & Retention
**Level:** Advanced
**Frequency:** Common
**Tags:** `kafka`, `tiered-storage`, `kip-405`, `object-storage`, `retention`

**Short Answer (2–3 lines):**
Tiered storage (KIP-405, production-ready since Kafka 3.9) copies closed segments to remote storage such as S3, GCS or HDFS. It deletes them locally after a short `local.retention.ms`, while `retention.ms` still sets total retention. Brokers then hold mainly the hot tail on disk, so long retention is cheap and broker recovery or rebalancing moves far less data. Reads of old offsets are fetched from remote storage transparently.

**Detailed Explanation:**
**Architecture**
- **RemoteStorageManager (RSM)**: a plugin that copies segments and indexes to and from the remote store. Apache Kafka ships the interface and no production implementation, so you use a vendor or open-source plugin.
- **RemoteLogMetadataManager (RLMM)**: tracks which segments are remote. The default `TopicBasedRemoteLogMetadataManager` uses the internal `__remote_log_metadata` topic.
- The leader's remote log manager uploads closed segments (checked every `remote.log.manager.task.interval.ms`, 30 s). Followers don't upload.

**Two retention clocks**
| Setting | Meaning |
|---|---|
| `retention.ms` / `retention.bytes` | Total retention, local plus remote |
| `local.retention.ms` / `local.retention.bytes` | How long segments stay on broker disk; `-2` (default) means "same as total" |

**What gets better**
- Disk sizing depends on hot data only, so brokers become smaller and cheaper.
- Adding or replacing a broker copies only the local tail. In 4.3, `follower.fetch.last.tiered.offset.enable` lets new followers skip data already in remote storage.
- Long replays (Kappa-style reprocessing, see Q28) no longer need giant disks.

**Limits and trade-offs**
- **No compacted topics.**
- Historical reads have object-store latency, and backfill throughput depends on the RSM, its caches and the fetch thread pools.
- The metadata topic is critical. 4.3 adds `remote.log.metadata.topic.min.isr` (2 by default).
- Turning it off needs care: `remote.log.copy.disable` and `remote.log.delete.on.disable` control how a topic leaves tiered storage.
- Data in the bucket is in Kafka's segment format, not Parquet. It isn't a lake table. For analytics, still land data into Iceberg or Delta.

**Example / Code:**
```properties
# Runtime: Apache Kafka 4.3 broker + a RemoteStorageManager plugin (Kafka ships none; plugin class shown is a placeholder)
# --- broker (server.properties) ---
remote.log.storage.system.enable=true
remote.log.storage.manager.class.name=com.example.tiered.S3RemoteStorageManager
remote.log.storage.manager.class.path=/opt/kafka/plugins/tiered/*
remote.log.metadata.manager.listener.name=PLAINTEXT
```
```bash
# Runtime: Apache Kafka 4.3.1 CLI — per-topic switch: 1 day on local disk, 1 year in total
bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic clickstream \
  --partitions 48 --replication-factor 3 \
  --config remote.storage.enable=true \
  --config local.retention.ms=86400000 \
  --config retention.ms=31536000000
```
These commands were checked against the 4.3 topic and broker config reference. They weren't run in the lab, because there is no RSM plugin there to upload to.

**Real-World Use Case:**
A clickstream cluster kept 7 days on large local disks, and replacing a failed broker took many hours of re-replication. With tiered storage, 1 day local and 90 days total, disks shrank, broker replacement finished in a fraction of the time, and the analytics team could replay a month of events straight from Kafka.

**Common Mistakes / Interviewer Red Flags:**
- Expecting tiered storage to work on compacted changelog topics.
- Treating the remote bucket as a queryable data lake.
- Setting `local.retention.ms` so low that ordinary consumer lag pushes reads into remote storage.
- Assuming Apache Kafka includes an S3 plugin out of the box.

**Follow-up Questions Interviewers Ask:**
- What happens to consumers reading old offsets? — The broker fetches from remote storage transparently; the client doesn't change.
- Which metrics matter? — Remote copy and fetch lag and errors, and the size of data waiting to upload.
- How is it different from archiving to S3 with a Connect sink? — Tiered storage keeps data readable *as Kafka* by offset; a sink produces lake files for other engines.

---
### Q498. Kafka Connect architecture: workers, connectors, tasks and internal topics
**ID:** KFK-30
**Topic:** Kafka > Kafka Connect
**Level:** Beginner
**Frequency:** Very Common
**Tags:** `kafka-connect`, `source`, `sink`, `distributed-mode`, `rest-api`

**Short Answer (2–3 lines):**
Kafka Connect is a framework for moving data between Kafka and other systems through configuration, not code. **Source** connectors write external data into Kafka, and **sink** connectors write Kafka data out. A cluster of **workers** runs **connectors**, which split their work into **tasks**. In distributed mode, workers share state through three internal topics (configs, offsets, status), and you manage everything through a REST API.

**Detailed Explanation:**
**Pieces**
| Concept | What it is |
|---|---|
| Worker | A JVM process running the Connect runtime. Workers with the same `group.id` form a cluster |
| Connector | A configured instance of a plugin, such as a JDBC sink for one database |
| Task | A unit of parallel work. A sink's tasks act like consumers in a group; a source's tasks split tables, files or shards |
| Converter | Turns Connect's internal data into bytes and back (see Q499) |
| Transform (SMT) | A light, per-record change applied between connector and converter |
| Plugin path | Where connector JARs are loaded from, each in its own isolated classloader |

**Standalone vs distributed**
- *Standalone*: one process, with offsets in a local file. Good for development or a single edge agent.
- *Distributed*: any number of workers. Connector configs live in `config.storage.topic`, source offsets in `offset.storage.topic`, and state in `status.storage.topic`. Tasks rebalance across workers when one fails (incremental cooperative rebalancing, with `scheduled.rebalance.max.delay.ms` of 5 minutes before a lost worker's tasks are reassigned).

**Offsets**
- Sink connectors use normal consumer group offsets (`connect-<connector-name>`).
- Source connectors store their own "source offsets" (a file position, a WAL LSN, a row timestamp) in the offsets topic. Since Kafka 3.6 (KIP-875) you can read and reset them through the REST API (`GET/PATCH/DELETE /connectors/{name}/offsets`).

**When not to use Connect.** For complex business logic, use stream processing (Kafka Streams or Flink) and let Connect do only the I/O.

**Example / Code:**
```properties
bootstrap.servers=localhost:19092
group.id=connect-lab
listeners=http://localhost:18083
config.storage.topic=connect-lab-configs
offset.storage.topic=connect-lab-offsets
status.storage.topic=connect-lab-status
config.storage.replication.factor=1
offset.storage.replication.factor=1
status.storage.replication.factor=1
key.converter=org.apache.kafka.connect.storage.StringConverter
value.converter=org.apache.kafka.connect.json.JsonConverter
value.converter.schemas.enable=false
plugin.path=/opt/kafka/libs/connect-file-4.3.1.jar
exactly.once.source.support=enabled
```
```bash
# Runtime: Apache Kafka 4.3.1 — start one distributed worker, then use the REST API
bin/connect-distributed.sh worker.properties &
curl -s localhost:18083/
curl -s localhost:18083/connector-plugins | python3 -m json.tool | grep class
curl -s localhost:18083/connectors/access-source/status
curl -s localhost:18083/connectors/access-source/offsets
```
```text
{"version":"4.3.1","commit":"26b251a451ce941d","kafka_cluster_id":"k823IPTOTlqVLpX-4sSHCg"}
        "class": "org.apache.kafka.connect.file.FileStreamSinkConnector",
        "class": "org.apache.kafka.connect.file.FileStreamSourceConnector",
        "class": "org.apache.kafka.connect.mirror.MirrorCheckpointConnector",
        "class": "org.apache.kafka.connect.mirror.MirrorHeartbeatConnector",
        "class": "org.apache.kafka.connect.mirror.MirrorSourceConnector",
{"name":"access-source","connector":{"state":"RUNNING","worker_id":"localhost:18083","version":"4.3.1"},"tasks":[{"id":0,"state":"RUNNING","worker_id":"localhost:18083","version":"4.3.1"}],"type":"source"}
{"offsets":[{"partition":{"filename":"/var/log/web/access.log"},"offset":{"position":47}}]}
```
The file connectors ship with Kafka but have not been on the default classpath since 3.2, which is why `plugin.path` points at `connect-file-4.3.1.jar`. (Lab paths are shortened here.)

**Real-World Use Case:**
A team ran a Postgres CDC source and an S3 sink on a three-worker Connect cluster on Kubernetes, managed by an operator. When one worker's node was drained, its tasks moved to the other two workers. Source offsets in the offsets topic let the CDC task resume from the same log position.

**Common Mistakes / Interviewer Red Flags:**
- Running production pipelines in standalone mode.
- Creating Connect's internal topics with replication factor 1, or letting the offsets topic be deleted by retention (it must be compacted).
- Putting many unrelated teams' connectors on one shared worker cluster with no isolation.
- Setting `tasks.max` higher than the source can actually split into, and expecting more parallelism.

**Follow-up Questions Interviewers Ask:**
- How do you restart only failed tasks? — `POST /connectors/{name}/restart?includeTasks=true&onlyFailed=true`.
- Where do sink connectors keep their position? — In the consumer group `connect-<name>`, visible with `kafka-consumer-groups.sh`.
- How do you keep secrets out of connector configs? — Config providers, such as `${file:/secrets/db.properties:password}` or vault-backed providers.

---

### Q499. Converters vs SMTs vs serializers in Kafka Connect
**ID:** KFK-31
**Topic:** Kafka > Kafka Connect
**Level:** Intermediate
**Frequency:** Common
**Tags:** `kafka-connect`, `converters`, `smt`, `json`, `avro`

**Short Answer (2–3 lines):**
Connectors work with Connect's internal record model (schema plus struct). **Converters** turn that model into bytes on the way into Kafka and back on the way out: JSON, Avro, Protobuf, String, ByteArray. **Single Message Transforms (SMTs)** change one record at a time: rename, mask, route, insert a field. Both are set by configuration. Converters must match how the topic was actually written, or the sink fails to deserialise.

**Detailed Explanation:**
**Order on a source connector:** connector → SMTs → converter → producer → Kafka. On a sink it's the reverse: Kafka → consumer → converter → SMTs → connector.

**Common converters**
| Converter | Notes |
|---|---|
| `JsonConverter` | `schemas.enable=true` embeds a schema in every message (bulky); `false` gives plain JSON with no types |
| `AvroConverter` / `ProtobufConverter` / `JsonSchemaConverter` | Need a Schema Registry; compact and typed (see Q504) |
| `StringConverter` / `ByteArrayConverter` | Pass-through; good for keys or opaque payloads |

Set them per worker (`key.converter`, `value.converter`) and override per connector. Key and value converters can differ: a String key with an Avro value is common.

**Useful built-in SMTs** (Apache Kafka): `InsertField`, `ReplaceField` (include, exclude, rename), `MaskField`, `Cast`, `ExtractField`, `HoistField`, `ValueToKey`, `TimestampRouter`, `RegexRouter`, `Filter` with predicates, `HeaderFrom`, `InsertHeader`, `DropHeaders`, `TimestampConverter`, `Flatten`. Debezium adds `ExtractNewRecordState` and the outbox router (see Q503).

**When not to use SMTs.** SMTs are stateless and per record. Joins, aggregations, lookups and anything needing state belong in Kafka Streams or Flink. Long SMT chains are hard to test and to version.

**Predicates** let an SMT apply conditionally, for example only to tombstones or to topics matching a pattern.

**Example / Code:**
Source connector: wrap each log line, add a field, then route to a daily topic.
```json
{
  "connector.class": "org.apache.kafka.connect.file.FileStreamSourceConnector",
  "tasks.max": "1",
  "file": "/var/log/web/access.log",
  "topic": "web-access",
  "value.converter": "org.apache.kafka.connect.json.JsonConverter",
  "value.converter.schemas.enable": "false",
  "transforms": "wrap,addSource,route",
  "transforms.wrap.type": "org.apache.kafka.connect.transforms.HoistField$Value",
  "transforms.wrap.field": "line",
  "transforms.addSource.type": "org.apache.kafka.connect.transforms.InsertField$Value",
  "transforms.addSource.static.field": "source_host",
  "transforms.addSource.static.value": "web-01",
  "transforms.route.type": "org.apache.kafka.connect.transforms.TimestampRouter",
  "transforms.route.topic.format": "${topic}-${timestamp}",
  "transforms.route.timestamp.format": "yyyyMMdd"
}
```
```text
$ bin/kafka-console-consumer.sh --bootstrap-server localhost:19092 --topic web-access-20261017 --from-beginning
{"line":"GET /home 200","source_host":"web-01"}
{"line":"GET /cart 200","source_host":"web-01"}
{"line":"POST /checkout 500","source_host":"web-01"}
```
`TimestampRouter` renamed `web-access` to `web-access-20261017`. `HoistField` turned the raw string into a struct, so `InsertField` had a struct to add `source_host` to. The two would fail the other way round.

**Real-World Use Case:**
A sink team received Avro from upstream but configured the sink with `JsonConverter`. Every record failed with "Converting byte[] to Kafka Connect data failed". Switching the sink's `value.converter` to `AvroConverter` with the registry URL fixed it, and a `MaskField` SMT redacted PII before the data reached the warehouse.

**Common Mistakes / Interviewer Red Flags:**
- Mixing up converters (Connect) with serializers (plain producers and consumers).
- Leaving `schemas.enable=true` on, then wondering why every JSON message has a schema envelope.
- Building business logic as a chain of ten SMTs.
- Ordering SMTs wrongly (they run in the listed order).

**Follow-up Questions Interviewers Ask:**
- How do you route only some records through an SMT? — Use `predicates`, such as `TopicNameMatches` or `RecordIsTombstone`, with `transforms.<name>.predicate`.
- Where is the record key set for a JDBC source? — Use `ValueToKey` then `ExtractField$Key` to build the key from columns.
- Can you write your own SMT? — Yes, implement `Transformation<R>` and add the JAR to the plugin path.

---

### Q500. Error handling in Kafka Connect: errors.tolerance and dead letter queues
**ID:** KFK-32
**Topic:** Kafka > Kafka Connect
**Level:** Intermediate
**Frequency:** Common
**Tags:** `kafka-connect`, `dlq`, `error-handling`, `poison-messages`

**Short Answer (2–3 lines):**
By default (`errors.tolerance=none`) one bad record fails the task, and the pipeline stops until someone acts. With `errors.tolerance=all`, failures in conversion or transforms are skipped. For **sink** connectors you can send those records to a dead letter queue topic, with headers describing the error. Connect's DLQ covers converter and SMT failures; errors inside the connector's own `put()` (such as a database constraint) are handled by the connector or fail the task.

**Detailed Explanation:**
Handling poison messages in general is on this site's poison-messages page and in Q39. This question covers Connect's built-in features.

**Settings (per connector)**
| Setting | Default | Meaning |
|---|---|---|
| `errors.tolerance` | `none` | `all` skips bad records instead of failing |
| `errors.retry.timeout` | `0` | How long to retry retriable failures; `-1` retries forever |
| `errors.retry.delay.max.ms` | 60000 | Backoff cap between retries |
| `errors.log.enable`, `errors.log.include.messages` | `false` | Log failures, optionally with the record content |
| `errors.deadletterqueue.topic.name` | (none) | Sink only: where failed records go |
| `errors.deadletterqueue.context.headers.enable` | `false` | Adds `__connect.errors.*` headers: topic, partition, offset, stage, exception |
| `errors.deadletterqueue.topic.replication.factor` | 3 | Lower it for small labs |

**What the DLQ catches.** Failures in the key/value/header **converter** and in **transforms**. Since KIP-610, a sink connector may also report records with the `ErrantRecordReporter`, so a JDBC sink can route a row that hits a constraint violation. Whether it does depends on the connector.

**Sources have no DLQ.** A source connector produces records; if conversion fails, `errors.tolerance=all` skips and logs them.

**Operating a DLQ**
- Alert on DLQ growth. A DLQ nobody reads is silent data loss.
- Keep the original bytes (Connect writes the raw record) so a fixed pipeline can replay it.
- Replay with a small consumer or a second connector after fixing the cause.

**Example / Code:**
```json
{
  "connector.class": "org.apache.kafka.connect.file.FileStreamSinkConnector",
  "tasks.max": "1",
  "topics": "customers",
  "file": "/data/out/customers.out",
  "value.converter": "org.apache.kafka.connect.json.JsonConverter",
  "value.converter.schemas.enable": "false",
  "transforms": "mask,drop",
  "transforms.mask.type": "org.apache.kafka.connect.transforms.MaskField$Value",
  "transforms.mask.fields": "email",
  "transforms.drop.type": "org.apache.kafka.connect.transforms.ReplaceField$Value",
  "transforms.drop.exclude": "internal_note",
  "errors.tolerance": "all",
  "errors.retry.timeout": "0",
  "errors.log.enable": "true",
  "errors.deadletterqueue.topic.name": "dlq.customers-sink",
  "errors.deadletterqueue.topic.replication.factor": "1",
  "errors.deadletterqueue.context.headers.enable": "true"
}
```
```bash
# Runtime: Apache Kafka 4.3.1 — two good records and one that is not JSON
printf '%s\n' '{"id":1,"name":"Asha","email":"asha@example.com","internal_note":"vip"}' 'not-json{' \
  '{"id":2,"name":"Ben","email":"ben@example.com","internal_note":"-"}' \
  | bin/kafka-console-producer.sh --bootstrap-server localhost:19092 --topic customers
curl -s -X PUT -H 'Content-Type: application/json' --data @customers-sink.json \
  localhost:18083/connectors/customers-sink/config
cat /data/out/customers.out
bin/kafka-console-consumer.sh --bootstrap-server localhost:19092 --topic dlq.customers-sink \
  --from-beginning --formatter-property print.headers=true
```
```text
{name=Asha, email=, id=1}
{name=Ben, email=, id=2}
__connect.errors.topic:customers
__connect.errors.partition:0
__connect.errors.offset:1
__connect.errors.connector.name:customers-sink
__connect.errors.stage:VALUE_CONVERTER
__connect.errors.class.name:org.apache.kafka.connect.json.JsonConverter
__connect.errors.exception.class.name:org.apache.kafka.connect.errors.DataException
__connect.errors.exception.message:Converting byte[] to Kafka Connect data failed due to serialization error: 
	not-json{
```
(Header output reformatted one per line.) The task stayed `RUNNING`, the good records were written with `email` masked and `internal_note` dropped, and the poison record landed in the DLQ with its source offset.

**Real-World Use Case:**
An S3 sink stopped for a weekend because one producer briefly sent malformed JSON, and `errors.tolerance` was at its default. After the incident, sinks used `errors.tolerance=all` with a DLQ and a lag alert on it. The next malformed burst went to the DLQ, was fixed and replayed within the hour, and the main flow never stopped.

**Common Mistakes / Interviewer Red Flags:**
- Setting `errors.tolerance=all` without a DLQ or logging, which silently drops data.
- Expecting a DLQ on a source connector.
- Assuming every sink-side failure (like a database outage) goes to the DLQ; retriable errors are retried, and connector-level errors may fail the task.

**Follow-up Questions Interviewers Ask:**
- How do you build retry topics with delays for plain consumers? — Route failures to `retry-5m` / `retry-1h` topics consumed with a delay, then to a final DLQ (or use share groups' delivery count, see Q488).
- Which header tells you where the failure happened? — `__connect.errors.stage` (for example `VALUE_CONVERTER`, `TRANSFORMATION`, `TASK_PUT`).
- How do you replay a DLQ? — Fix the cause, then consume the DLQ and re-produce the original bytes to the source topic or straight to the sink.

---

### Q501. Exactly-once source connectors and what "exactly-once" means for sinks
**ID:** KFK-33
**Topic:** Kafka > Kafka Connect
**Level:** Advanced
**Frequency:** Occasional
**Tags:** `kafka-connect`, `exactly-once`, `kip-618`, `transactions`, `source-connectors`

**Short Answer (2–3 lines):**
Since Kafka 3.3 (KIP-618), distributed workers with `exactly.once.source.support=enabled` run each source task with a transactional producer. The records and the task's source offsets commit in the same Kafka transaction, so a crash can't cause duplicates or gaps in Kafka. Sinks don't use Kafka transactions. They are exactly-once only if the connector writes idempotently or stores offsets in the target atomically (see Q493).

**Detailed Explanation:**
**Without EOS (the default, `exactly.once.source.support=disabled`).** A source task produces records and, separately, flushes source offsets every `offset.flush.interval.ms` (60 s). A crash between the two replays everything since the last flush, so you get duplicates.

**With EOS**
- Turn it on cluster-wide in two rolls: `preparing`, then `enabled`. All workers must be 3.3 or newer and in distributed mode.
- Each task gets a transactional ID like `<group.id>-<connector>-<task>`, as the lab output below shows. The leader also uses a transactional producer to write configs, which fences zombie workers.
- `transaction.boundary` (per connector): `poll` (default, one transaction per poll batch), `interval` (time-based), or `connector` (the connector defines its own boundaries).
- `exactly.once.support=required` makes connector creation fail if the plugin can't support EOS. The default `requested` is best-effort.
- Downstream consumers need `isolation.level=read_committed` (the default in librdkafka, but not in Java).
- ACLs: the worker principal needs transactional ID and idempotent-write permissions.

**What the connector must do.** It must report precise source offsets and be able to resume from them. Debezium supports this for several databases; check each connector's documentation.

**Sinks**
- The Kafka side is just consumer offsets.
- Exactly-once into the target needs idempotent upserts (JDBC `insert.mode=upsert`), deterministic object names (S3 sinks name files by partition and start offset), or atomic offset storage in the target.

**Example / Code:**
```bash
# Runtime: Apache Kafka 4.3.1 — worker started with exactly.once.source.support=enabled (see Q498's worker.properties)
bin/kafka-transactions.sh --bootstrap-server localhost:19092 list
```
```text
TransactionalId            	Coordinator	ProducerId	TransactionState	
connect-cluster-connect-lab	1          	2000      	CompleteCommit  	
connect-lab-access-source-0	1          	2001      	CompleteCommit  	
enricher-tx-0              	1          	1001      	CompleteCommit  	
```
`connect-cluster-connect-lab` is the worker leader's config producer, and `connect-lab-access-source-0` is task 0 of the `access-source` connector, which commits a transaction per poll. (`enricher-tx-0` is the Python app from Q494.)
```json
{
  "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
  "exactly.once.support": "required",
  "transaction.boundary": "poll"
}
```

**Real-World Use Case:**
A CDC pipeline into a ledger topic produced a few duplicate change events after each Connect worker crash, and downstream balance calculations needed de-duplication. After enabling exactly-once source support (in two rolling steps) and setting `read_committed` on consumers, worker crashes no longer produced duplicates in the topic.

**Common Mistakes / Interviewer Red Flags:**
- Thinking enabling EOS on the worker makes sink connectors exactly-once.
- Enabling it in one step on a mixed-version cluster.
- Forgetting `read_committed` downstream, so consumers still see aborted records.
- Missing ACLs for transactional IDs, which shows up as tasks failing at start-up.

**Follow-up Questions Interviewers Ask:**
- Does EOS cost throughput? — Some: commit markers and coordinator round trips per transaction; `interval` boundaries can batch more.
- Where are source offsets stored with EOS? — Still in the offsets topic (or a per-connector `offsets.storage.topic`), written inside the task's transaction.
- What if the source system itself replays? — EOS covers Connect's own crash window, not a source that re-emits data; downstream idempotency still helps.

---
### Q502. Debezium snapshots, incremental snapshots and replication-slot risks
**ID:** KFK-34
**Topic:** Kafka > CDC with Debezium
**Level:** Intermediate
**Frequency:** Common
**Tags:** `debezium`, `cdc`, `snapshots`, `postgres`, `replication-slot`

**Short Answer (2–3 lines):**
On first start, Debezium usually takes a **snapshot**: it reads existing rows as `op=r` events, then switches to streaming changes from the transaction log. **Incremental snapshots**, triggered by a signal, re-read tables in chunks *while* streaming continues, so you can backfill or add a table without stopping. The operational risk is the source side: an inactive or lagging Postgres replication slot keeps WAL on the database until the disk fills.

**Detailed Explanation:**
The end-to-end CDC design is covered in Q50 and on the Debezium page. This question is about running it.

**`snapshot.mode` (Postgres connector, Debezium 3.x)**
| Mode | Behaviour |
|---|---|
| `initial` (default) | Snapshot if no offsets exist, then stream |
| `initial_only` | Snapshot, then stop |
| `no_data` | Skip existing rows; stream from now (formerly `never`) |
| `when_needed` | Snapshot again if stored offsets are no longer available in the log |
| `always` | Snapshot on every start |
| `configuration_based` / `custom` | Fine-grained or pluggable control |

**Incremental snapshots** (a watermark-based design)
- Configure a signalling table (`signal.data.collection`) or a Kafka signal topic.
- Insert an `execute-snapshot` signal. Debezium reads the table in primary-key chunks (`incremental.snapshot.chunk.size`, 1024) and interleaves them with live changes, so a row being updated isn't overwritten by a stale snapshot read.
- Events carry `op=r` with `source.snapshot="incremental"`.
- Use them to add a table to `table.include.list`, to repair a downstream sink, or to backfill after a long outage.

**Replication slot hazards (Postgres)**
- The slot keeps WAL until Debezium confirms it. A stopped connector means WAL grows without limit.
- Even an *active* slot can lag when the captured tables are quiet but the database is busy. Set `heartbeat.interval.ms` (default 0, off), and possibly a `heartbeat.action.query`, so offsets advance.
- Guard rails: monitor `pg_replication_slots` lag, set Postgres `max_slot_wal_keep_size`, and drop slots of retired connectors.
- On failover to a replica, the slot must exist there (Postgres 17+ failover slots, or recreate it and re-snapshot).

**Example / Code:**
```json
{
  "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
  "database.hostname": "localhost",
  "database.port": "5498",
  "database.user": "postgres",
  "database.password": "postgres",
  "database.dbname": "shop",
  "topic.prefix": "shop",
  "plugin.name": "pgoutput",
  "slot.name": "shop_cdc",
  "publication.autocreate.mode": "filtered",
  "table.include.list": "public.customers",
  "snapshot.mode": "initial",
  "signal.data.collection": "public.debezium_signal",
  "key.converter": "org.apache.kafka.connect.json.JsonConverter",
  "key.converter.schemas.enable": "false",
  "value.converter": "org.apache.kafka.connect.json.JsonConverter",
  "value.converter.schemas.enable": "false",
  "tasks.max": "1"
}
```
```bash
# Runtime: PostgreSQL 16 (wal_level=logical), Debezium 3.7.0.Final on Kafka Connect 4.3.1
psql -h localhost -p 5498 -U postgres -d shop -c "SELECT slot_name, plugin, active,
  pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS lag_behind
  FROM pg_replication_slots;"
# Trigger an incremental snapshot of one table while streaming continues
psql -h localhost -p 5498 -U postgres -d shop -c "INSERT INTO debezium_signal VALUES ('adhoc-1',
  'execute-snapshot', '{\"data-collections\": [\"public.customers\"], \"type\": \"incremental\"}');"
```
```text
 slot_name |  plugin  | active | lag_behind 
-----------+----------+--------+------------
 shop_cdc  | pgoutput | t      | 273 kB
(1 row)

INSERT 0 1
```
Events on `shop.public.customers`, summarised (initial snapshot, live changes, then the incremental snapshot):
```text
key={"id":1}  op=r snapshot=first lsn=26362464 before=null after={"id": 1, "name": "Asha", "tier": "gold"}
key={"id":2}  op=r snapshot=last  lsn=26362464 before=null after={"id": 2, "name": "Ben", "tier": "silver"}
key={"id":3}  op=c snapshot=false lsn=26362504 before=null after={"id": 3, "name": "Chen", "tier": "bronze"}
key={"id":1}  op=u snapshot=false lsn=26362640 before={"id": 1, "name": "Asha", "tier": "gold"} after={"id": 1, "name": "Asha", "tier": "platinum"}
key={"id":2}  op=d snapshot=false lsn=26362744 before={"id": 2, "name": "Ben", "tier": "silver"} after=null
key={"id":1}  op=r snapshot=incremental after={"id": 1, "name": "Asha", "tier": "platinum"}
key={"id":3}  op=r snapshot=incremental after={"id": 3, "name": "Chen", "tier": "bronze"}
```
The slot showed 273 kB of lag although active, because nothing in the captured table had changed recently. That is what heartbeats are for.

**Real-World Use Case:**
A team paused a Debezium connector over a long weekend during a migration. The Postgres primary's disk filled with retained WAL and the database stopped accepting writes. They now alert on slot lag, set `max_slot_wal_keep_size`, and enable heartbeats on every CDC connector.

**Common Mistakes / Interviewer Red Flags:**
- Deleting a connector but leaving its replication slot behind.
- Using `snapshot.mode=always` on big tables, which re-reads them on every restart.
- Re-snapshotting a huge table by resetting offsets, when an incremental snapshot would avoid downtime.
- Not knowing that tables without a primary key complicate incremental snapshots (you need a surrogate key via `message.key.columns`).

**Follow-up Questions Interviewers Ask:**
- How do you add a new table to a running connector? — Add it to `table.include.list`, then send an incremental snapshot signal for it.
- What is `REPLICA IDENTITY FULL` for? — It makes Postgres log the full old row, so `before` is populated for updates and deletes.
- How do downstream consumers tell snapshot rows from live inserts? — `op=r` versus `op=c`, and `source.snapshot`.

---

### Q503. Debezium change events: the envelope, tombstones and flattening with ExtractNewRecordState
**ID:** KFK-35
**Topic:** Kafka > CDC with Debezium
**Level:** Intermediate
**Frequency:** Common
**Tags:** `debezium`, `cdc`, `smt`, `tombstones`, `compaction`

**Short Answer (2–3 lines):**
Each Debezium event carries `before`, `after`, `op` (c, u, d, r), `source` metadata (LSN, transaction ID, timestamp, table) and `ts_ms`, keyed by the primary key. A delete is sent as a `d` event followed by a **tombstone** (null value), so compacted topics can drop the key. Most sinks want just the row, which the `ExtractNewRecordState` SMT produces, optionally adding `__op` and a `__deleted` flag.

**Detailed Explanation:**
**Envelope fields**
| Field | Meaning |
|---|---|
| `before` | Row before the change (nulls unless `REPLICA IDENTITY FULL` or the key-only default) |
| `after` | Row after the change (null for deletes) |
| `op` | `c` create, `u` update, `d` delete, `r` snapshot read, `t` truncate, `m` message |
| `source` | Connector, database, schema, table, `lsn`, `txId`, `snapshot`, source timestamp |
| `ts_ms` / `ts_us` | When Debezium processed the event |

**Keys and ordering.** The record key is the primary key, so all changes to one row go to one partition in order (see Q69). Downstream "latest wins" logic should compare `source.lsn` (or a similar position), not arrival order (see Q50).

**Deletes**
- `d` event: the consumer learns *what* was deleted.
- Tombstone: lets log compaction remove the key (see Q71). Controlled by `tombstones.on.delete` (default `true`).

**Flattening with `ExtractNewRecordState`**
- Emits only `after`, which is what JDBC, Elasticsearch or lake sinks expect.
- `add.fields=op,source.ts_ms` copies metadata in as `__op` and `__source_ts_ms`.
- `delete.tombstone.handling.mode`: `tombstone` (keep tombstones), `drop`, `rewrite` (a delete becomes the last known row with `__deleted=true`), `rewrite-with-tombstone`, or `delete-to-tombstone` (the delete becomes just a tombstone).

**Outbox pattern.** For domain events (not raw table changes), services write to an `outbox` table in the same transaction as their state change, and Debezium's `EventRouter` SMT routes each row to a topic. This avoids dual writes and keeps internal table schemas private.

**Example / Code:**
```json
{
  "topic.prefix": "shopflat",
  "slot.name": "shop_flat",
  "publication.name": "shop_flat_pub",
  "transforms": "unwrap",
  "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
  "transforms.unwrap.add.fields": "op,source.ts_ms",
  "transforms.unwrap.delete.tombstone.handling.mode": "rewrite"
}
```
(Added to the connector config shown in Q502.) After `UPDATE customers SET tier='gold' WHERE id=3; DELETE FROM customers WHERE id=1;`:
```text
{"id":3}	{"id":3,"name":"Chen","tier":"bronze","__deleted":"false","__op":"r","__source_ts_ms":1792264243073}
{"id":1}	{"id":1,"name":"Asha","tier":"platinum","__deleted":"false","__op":"r","__source_ts_ms":1792264243073}
{"id":3}	{"id":3,"name":"Chen","tier":"gold","__deleted":"false","__op":"u","__source_ts_ms":1792264247678}
{"id":1}	{"id":1,"name":"Asha","tier":"platinum","__deleted":"true","__op":"d","__source_ts_ms":1792264247678}
```
Compare the raw envelope in Q502, where the delete was a `d` event (`after=null`) followed by a `null` tombstone for key `{"id":2}`.

**Real-World Use Case:**
A team loaded CDC into a warehouse with a JDBC sink that couldn't parse the nested envelope. Adding `ExtractNewRecordState` with `rewrite` let a dbt model treat `__deleted='true'` as a soft delete, with `__source_ts_ms` for ordering, which fed the SCD logic in Q51.

**Common Mistakes / Interviewer Red Flags:**
- Dropping tombstones on a compacted CDC topic, so deleted keys live forever.
- Ordering updates by Kafka timestamp instead of the source LSN.
- Exposing raw table CDC as a public contract instead of an outbox topic.
- Forgetting that `before` is mostly empty without `REPLICA IDENTITY FULL`.

**Follow-up Questions Interviewers Ask:**
- What does `op=t` mean? — A truncate event (Postgres), emitted only if enabled; many sinks can't apply it.
- How do you get schema changes downstream? — Use Avro or Protobuf with a Schema Registry (see Q504); Debezium registers new schema versions as columns change.
- Can you capture only some columns? — Yes, `column.include.list` or `column.exclude.list`, and `column.mask.*` options for PII.

---

### Q504. Schema Registry under the hood: wire format, schema IDs and subject naming strategies
**ID:** KFK-36
**Topic:** Kafka > Schema Registry
**Level:** Intermediate
**Frequency:** Common
**Tags:** `schema-registry`, `avro`, `wire-format`, `subjects`, `serializers`

**Short Answer (2–3 lines):**
A registry-aware serializer registers (or looks up) the schema under a **subject** and gets a global **schema ID**. It then writes `magic byte 0` + `4-byte schema ID` + the encoded body; Protobuf adds message indexes. Consumers read the ID, fetch and cache the schema, and decode. The subject naming strategy decides what a "subject" is: per topic (default), per record type, or per topic plus record type. That in turn decides where compatibility rules apply.

**Detailed Explanation:**
Compatibility modes and safe evolution are covered in Q72 and on this site's schema-evolution page. This question is about the mechanics.

**Flow**
1. The producer's serializer derives the subject, for example `orders-value`.
2. With `auto.register.schemas=true` it registers the schema if it's new. The registry checks compatibility against earlier versions of that subject and returns the ID.
3. Each record carries only the 5-byte header plus the body, not the schema.
4. The consumer's deserializer fetches the schema by ID once, caches it, and decodes. With Avro it can also resolve to a *reader* schema (`specific.avro.reader` or a generated class).

**Subject naming strategies**
| Strategy | Subject | Use when |
|---|---|---|
| `TopicNameStrategy` (default) | `<topic>-key`, `<topic>-value` | One event type per topic |
| `RecordNameStrategy` | `<fully.qualified.RecordName>` | Same type in many topics, evolving together |
| `TopicRecordNameStrategy` | `<topic>-<RecordName>` | Several event types in one topic (to keep per-entity ordering) |

**Production settings**
- Set `auto.register.schemas=false` in production producers and register schemas from CI. Use `use.latest.version=true` or explicit IDs, so a stray build can't register a schema nobody reviewed.
- Use schema references (Avro, Protobuf, JSON Schema) for shared types such as `Money`, instead of copying them into every schema.
- Registries can enforce rules at registration time: data contracts with metadata and validation rules in Confluent's registry, and similar features elsewhere.

Some managed services (AWS Glue Schema Registry, Azure Schema Registry) use their own header formats. Clients must use the matching serializer.

**Example / Code:**
```python
# Runtime: Python 3.11, fastavro 1.12 (no registry needed: this builds and parses the wire format by hand)
# Confluent Schema Registry framing: [magic byte 0][4-byte big-endian schema ID][Avro binary body]
import io, struct
from fastavro import parse_schema, schemaless_writer, schemaless_reader

order_v1 = parse_schema({
    "type": "record", "name": "OrderPlaced", "namespace": "shop.orders",
    "fields": [{"name": "order_id", "type": "string"},
               {"name": "amount_cents", "type": "long"}]})

def encode(schema_id, schema, record):
    buf = io.BytesIO()
    buf.write(struct.pack(">bI", 0, schema_id))   # what the serializer prepends
    schemaless_writer(buf, schema, record)
    return buf.getvalue()

def decode(payload, registry):
    magic, schema_id = struct.unpack(">bI", payload[:5])
    assert magic == 0, "not Schema Registry framed"
    return schema_id, schemaless_reader(io.BytesIO(payload[5:]), registry[schema_id])

msg = encode(42, order_v1, {"order_id": "o-1001", "amount_cents": 2599})
print("bytes:", msg.hex(" "))
print("size :", len(msg), "bytes vs JSON", len('{"order_id": "o-1001", "amount_cents": 2599}'))
print("decoded:", decode(msg, {42: order_v1}))

# A v2 reader schema with a new optional field can still read v1 bytes (backward compatible)
order_v2 = parse_schema({**order_v1, "fields": order_v1["fields"] + [
    {"name": "coupon", "type": ["null", "string"], "default": None}]})
body = io.BytesIO(msg[5:])
print("read with v2:", schemaless_reader(body, order_v1, order_v2))
```
```text
bytes: 00 00 00 00 2a 0c 6f 2d 31 30 30 31 ce 28
size : 14 bytes vs JSON 44
decoded: (42, {'order_id': 'o-1001', 'amount_cents': 2599})
read with v2: {'order_id': 'o-1001', 'amount_cents': 2599, 'coupon': None}
```
Byte 0 is the magic byte and bytes 1–4 are schema ID 42 (`00 00 00 2a`). The rest is Avro: a zig-zag length prefix for the string, then the zig-zag varint for 2599. With the real client, `confluent_kafka.schema_registry.avro.AvroSerializer` writes exactly this framing after talking to the registry. This demo builds it by hand, so it runs without a registry server.

**Real-World Use Case:**
A team put `OrderPlaced`, `OrderShipped` and `OrderCancelled` into one `orders` topic to keep per-order ordering. With the default `TopicNameStrategy`, the second event type failed registration as "incompatible". Switching producers and consumers to `TopicRecordNameStrategy` gave each type its own subject and compatibility history.

**Common Mistakes / Interviewer Red Flags:**
- Thinking the schema travels with every message.
- Leaving `auto.register.schemas=true` in production.
- Mixing event types in one topic with `TopicNameStrategy`.
- Consumers using plain JSON or String deserializers on registry-framed data, which shows up as five garbage bytes at the start.

**Follow-up Questions Interviewers Ask:**
- What if the registry is down? — Clients with cached IDs keep working; new schemas or IDs can't be registered or fetched until it recovers.
- Are schema IDs per subject? — No; IDs are global in the registry, while versions are per subject.
- How do Kafka Connect and Debezium use it? — Through `AvroConverter` / `ProtobufConverter` with `schema.registry.url` (see Q499).

---

### Q505. Avro vs Protobuf vs JSON Schema for Kafka events
**ID:** KFK-37
**Topic:** Kafka > Schema Registry
**Level:** Beginner
**Frequency:** Common
**Tags:** `avro`, `protobuf`, `json-schema`, `serialization`, `schema-evolution`

**Short Answer (2–3 lines):**
All three can be enforced with a Schema Registry. **Avro** is compact, needs the writer's schema to decode, and has the most mature evolution rules in data tooling (Connect, Spark, Flink, lakes). **Protobuf** is compact, uses field numbers for evolution, and suits teams that already use gRPC. **JSON Schema** keeps human-readable JSON, at the cost of size and looser typing. Choose by ecosystem and evolution needs rather than raw speed.

**Detailed Explanation:**
| | Avro | Protobuf | JSON Schema |
|---|---|---|---|
| Encoding | Binary, no field tags (schema needed) | Binary with field numbers | Text JSON |
| Size | Smallest typically | Small | Largest |
| Evolution rule | Add or remove fields *with defaults*; resolution by field name | Never reuse field numbers; new fields optional by default (proto3) | Depends on `additionalProperties` and open or closed content models |
| Nullability | Explicit unions `["null", "string"]` | proto3 defaults and `optional`, which can hide "unset vs zero" | `type: ["string", "null"]` |
| Code generation | Optional (generic records work) | Usually required | Optional |
| Data platform support | Strongest (Connect, Debezium, Spark `from_avro`, Flink) | Good and growing | Good, but types are weaker (numbers, dates) |
| Human-readable on the wire | No | No | Yes |

**Guidance**
- *Data platform / CDC / lake*: Avro is the usual default, with logical types for decimals and timestamps.
- *Service-to-service events in a gRPC shop*: Protobuf, so teams share one IDL.
- *Low-volume integrations, or teams new to schemas*: JSON Schema. It's better than schemaless JSON, but enforce `additionalProperties: false` (or the reverse) on purpose, because it changes what counts as compatible.

**Pitfalls to mention**
- Avro `decimal` and timestamp logical types must be handled the same way by every reader.
- In proto3 a missing field and a zero value look identical unless the field is marked `optional`.
- Plain JSON without a schema makes "schema drift" the consumer's problem (see Q407).

**Example / Code:**
```text
# Schema definitions, not executable: the same event in each format
# Avro (orders-value, registered as AVRO)
{"type": "record", "name": "OrderPlaced", "namespace": "shop.orders",
 "fields": [{"name": "order_id", "type": "string"},
            {"name": "amount", "type": {"type": "bytes", "logicalType": "decimal", "precision": 12, "scale": 2}},
            {"name": "coupon", "type": ["null", "string"], "default": null}]}

# Protobuf (registered as PROTOBUF)
syntax = "proto3";
package shop.orders;
message OrderPlaced {
  string order_id = 1;
  int64 amount_cents = 2;
  optional string coupon = 3;   // never reuse 3 after deleting it; mark it `reserved 3;`
}

# JSON Schema (registered as JSON)
{"$schema": "https://json-schema.org/draft/2020-12/schema", "title": "OrderPlaced", "type": "object",
 "properties": {"order_id": {"type": "string"}, "amount": {"type": "number"},
                "coupon": {"type": ["string", "null"]}},
 "required": ["order_id", "amount"], "additionalProperties": false}
```
For size: the Q504 demo encoded a two-field order in 14 bytes as framed Avro, against 44 bytes as JSON.

**Real-World Use Case:**
A company with gRPC microservices wanted Protobuf events, while the data team wanted Avro for lakehouse ingestion. They chose Protobuf with a Schema Registry and used the `ProtobufConverter` in Connect sinks. Both sides kept their tooling, and there was one source of truth for each event.

**Common Mistakes / Interviewer Red Flags:**
- Choosing on benchmark speed alone.
- Reusing a deleted Protobuf field number.
- Adding an Avro field without a default and breaking backward compatibility.
- Representing money as floating-point in any format.

**Follow-up Questions Interviewers Ask:**
- Can one topic mix formats? — Technically yes with different subjects, but don't; consumers would need every deserializer.
- What does Avro schema resolution do? — It maps writer fields to reader fields by name, filling defaults and ignoring unknown fields.
- Is JSON Schema compatibility the same as Avro's? — No; it depends heavily on open versus closed content models.

---

### Q506. KStream vs KTable vs GlobalKTable in Kafka Streams
**ID:** KFK-38
**Topic:** Kafka > Kafka Streams
**Level:** Beginner
**Frequency:** Very Common
**Tags:** `kafka-streams`, `kstream`, `ktable`, `globalktable`, `stream-table-duality`

**Short Answer (2–3 lines):**
A **KStream** treats every record as an independent event (an insert). A **KTable** treats records as updates to the latest value per key (an upsert, with null meaning delete), backed by a local state store. A **GlobalKTable** is a KTable fully copied to every instance, so it can be joined on any field without co-partitioning. The same topic can be read either way; that's the stream-table duality.

**Detailed Explanation:**
| | KStream | KTable | GlobalKTable |
|---|---|---|---|
| Semantics | Event log | Changelog: latest per key | Changelog, fully replicated |
| A new record for key K | Another event | Replaces K's value | Replaces K's value |
| Null value | A record with null value | Deletes K (tombstone) | Deletes K |
| Partitioned? | Yes | Yes, each instance holds its own partitions | No, every instance holds all of it |
| Join requirements | Co-partitioned with the other side | Co-partitioned | Any key extractor; no co-partitioning |
| Typical source | Clicks, payments, sensor readings | Customer profile, account balance (a compacted topic) | Small reference data: currencies, product catalogue |

**Converting**
- `stream.groupByKey().aggregate(…)` or `.count()` gives a KTable. Aggregating a stream gives a table.
- `table.toStream()` gives the change stream of the table.

**Why aggregations over a KTable "retract".** When a table row changes, Streams subtracts the old value from the old group and adds the new one, so a count by tier stays correct as users change tiers. The demo shows this.

**Choosing GlobalKTable.** It's small, read-mostly data that every instance needs. The cost is that every instance stores and restores the whole topic, and GlobalKTable joins aren't time-synchronised: lookups use whatever is loaded.

**Example / Code:**
```java
// Runtime: Java 21, Apache Kafka Streams 4.3.1 + kafka-streams-test-utils (TopologyTestDriver, no broker)
// Run: java -cp "kafka_2.13-4.3.1/libs/*" StreamVsTable.java
import java.util.Properties;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

public class StreamVsTable {
    public static void main(String[] args) {
        StreamsBuilder b = new StreamsBuilder();
        // Same input topic, read two ways
        KStream<String, String> events = b.stream("tier-changes", Consumed.with(Serdes.String(), Serdes.String()));
        events.groupByKey().count()                                          // every record is an event
              .toStream().to("change-counts", Produced.with(Serdes.String(), Serdes.Long()));
        KTable<String, String> current = b.table("tier-changes-compacted",
              Consumed.with(Serdes.String(), Serdes.String()), Materialized.as("current-tier"));
        current.groupBy((user, tier) -> KeyValue.pair(tier, user),         // re-key by tier: updates retract
                       Grouped.with(Serdes.String(), Serdes.String()))
               .count().toStream().to("users-per-tier", Produced.with(Serdes.String(), Serdes.Long()));

        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-vs-table");
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        p.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);        // show every update
        p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        try (TopologyTestDriver d = new TopologyTestDriver(b.build(), p)) {
            var in1 = d.createInputTopic("tier-changes", new StringSerializer(), new StringSerializer());
            var in2 = d.createInputTopic("tier-changes-compacted", new StringSerializer(), new StringSerializer());
            String[][] changes = {{"asha", "silver"}, {"ben", "silver"}, {"asha", "gold"}, {"asha", "platinum"}};
            for (String[] c : changes) { in1.pipeInput(c[0], c[1]); in2.pipeInput(c[0], c[1]); }

            System.out.println("KStream count per user : " + d.createOutputTopic("change-counts",
                new StringDeserializer(), new LongDeserializer()).readKeyValuesToMap());
            System.out.println("KTable current tier    : asha=" + d.<String, String>getKeyValueStore("current-tier").get("asha")
                + ", ben=" + d.<String, String>getKeyValueStore("current-tier").get("ben"));
            System.out.println("KTable users per tier  : " + d.createOutputTopic("users-per-tier",
                new StringDeserializer(), new LongDeserializer()).readKeyValuesToList());
        }
    }
}
```
```text
KStream count per user : {asha=3, ben=1}
KTable current tier    : asha=platinum, ben=silver
KTable users per tier  : [KeyValue(silver, 1), KeyValue(silver, 2), KeyValue(silver, 1), KeyValue(gold, 1), KeyValue(gold, 0), KeyValue(platinum, 1)]
```
The stream counted three *events* for asha, while the table kept only her latest tier. When asha moved from silver to gold, the per-tier count emitted `silver → 1` (retracting her) and `gold → 1`, and then `gold → 0` when she moved on to platinum. Running a `.java` file directly needs only the Kafka `libs/` directory on the classpath (`java -cp "libs/*" StreamVsTable.java`).

**Real-World Use Case:**
A fraud service read `card-transactions` as a KStream and `card-status` (a compacted topic) as a KTable. It joined each transaction to the card's current status, and used a GlobalKTable of merchant categories looked up by `merchant_id` from the transaction value, with no repartition needed.

**Common Mistakes / Interviewer Red Flags:**
- Reading an event topic as a KTable and losing all but the last event per key.
- Using a GlobalKTable for a huge, fast-changing dataset.
- Forgetting that KTable source topics should be compacted.

**Follow-up Questions Interviewers Ask:**
- What is the record cache? — A write buffer in front of state stores (`statestore.cache.max.bytes`) that de-duplicates updates before they go downstream and to the changelog.
- How is a KTable restored after a crash? — From its changelog or source topic into the local store (see Q509).
- What does `toStream()` emit for a delete? — A record with a null value for that key.

---

### Q507. Joins in Kafka Streams: stream-table, stream-stream and co-partitioning
**ID:** KFK-39
**Topic:** Kafka > Kafka Streams
**Level:** Advanced
**Frequency:** Common
**Tags:** `kafka-streams`, `joins`, `co-partitioning`, `windows`, `foreign-key-join`

**Short Answer (2–3 lines):**
A **stream-table** join enriches each event with the table's value for its key *at that moment*; it isn't windowed, and later table updates don't re-trigger it. A **stream-stream** join must be windowed (`JoinWindows`), because both sides are unbounded, and it buffers each side in window stores. Both sides of a KStream/KTable join must be **co-partitioned**: same key, same partition count, same partitioner. Otherwise Streams repartitions, or the join fails.

**Detailed Explanation:**
| Join | Window | Triggered by | Result |
|---|---|---|---|
| KStream–KTable | No | Stream records only | Event plus the table value at that time (`leftJoin` keeps unmatched events with null) |
| KStream–GlobalKTable | No | Stream records | Lookup by any extracted key; no co-partitioning |
| KStream–KStream | `JoinWindows` (time difference + grace) | Either side | One result per matching pair in the window (can be many) |
| KTable–KTable | No | Either side | Continuously updated joined table |
| KTable–KTable foreign-key join | No | Either side | Joins on a field in the left value (since 2.4) |

**Co-partitioning**
- Same partition count on both topics, and the same partitioner. A Python producer on CRC32 breaks it silently (see Q472).
- If you change the key with `selectKey` or `map` before a join, Streams inserts a **repartition topic** automatically, at the cost of an extra network hop.
- Streams checks partition counts at start-up and throws `TopologyException` on a mismatch.

**Time semantics**
- Stream-stream joins match on record timestamps within the window. Late records are accepted within grace.
- Stream-table joins use the table's state as of processing. A table update that arrives slightly *after* an event isn't seen. You can make table updates wait by setting a versioned store with a grace period, so the join looks up the version valid at the event's timestamp (KIP-914, versioned stores).

**Example / Code:**
```java
// Runtime: Java 21, Apache Kafka Streams 4.3.1 + kafka-streams-test-utils (TopologyTestDriver, no broker)
import java.time.*;
import java.util.Properties;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

public class Joins {
    public static void main(String[] args) {
        var s = Serdes.String();
        StreamsBuilder b = new StreamsBuilder();
        KStream<String, String> orders = b.stream("orders", Consumed.with(s, s));        // key = customer_id
        KTable<String, String> customers = b.table("customers", Consumed.with(s, s));     // key = customer_id
        KStream<String, String> payments = b.stream("payments", Consumed.with(s, s));     // key = customer_id

        // 1) stream-table join: enrich each order with the customer's tier AS OF the order's arrival
        orders.leftJoin(customers, (order, tier) -> order + " tier=" + tier, Joined.with(s, s, s))
              .to("orders-enriched", Produced.with(s, s));

        // 2) stream-stream join: an order and a payment for the same key within 10 minutes
        orders.join(payments, (order, pay) -> order + " paid " + pay,
                    JoinWindows.ofTimeDifferenceAndGrace(Duration.ofMinutes(10), Duration.ofMinutes(1)),
                    StreamJoined.with(s, s, s))
              .to("orders-paid", Produced.with(s, s));

        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "joins-demo");
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        try (TopologyTestDriver d = new TopologyTestDriver(b.build(), p)) {
            var ser = new StringSerializer();
            var o = d.createInputTopic("orders", ser, ser);
            var c = d.createInputTopic("customers", ser, ser);
            var pay = d.createInputTopic("payments", ser, ser);
            Instant t0 = Instant.parse("2026-10-17T09:00:00Z");
            c.pipeInput("cust-1", "silver", t0);
            o.pipeInput("cust-1", "order-A", t0.plusSeconds(60));
            c.pipeInput("cust-1", "gold", t0.plusSeconds(120));          // tier changes after order-A
            o.pipeInput("cust-1", "order-B", t0.plusSeconds(180));
            o.pipeInput("cust-9", "order-C", t0.plusSeconds(200));       // unknown customer
            pay.pipeInput("cust-1", "pay-1", t0.plusSeconds(300));       // within 10 min of A and B
            pay.pipeInput("cust-9", "pay-9", t0.plusSeconds(200 + 1200)); // 20 min after order-C: no match

            var de = new StringDeserializer();
            System.out.println("stream-table : " + d.createOutputTopic("orders-enriched", de, de).readValuesToList());
            System.out.println("stream-stream: " + d.createOutputTopic("orders-paid", de, de).readValuesToList());
        }
    }
}
```
```text
stream-table : [order-A tier=silver, order-B tier=gold, order-C tier=null]
stream-stream: [order-A paid pay-1, order-B paid pay-1]
```
`order-A` kept the tier it had when it arrived (silver), even though the customer later became gold. The stream-stream join paired `pay-1` with both orders in its 10-minute window, a reminder that windowed joins can fan out. `pay-9` arrived 20 minutes after `order-C`, so it found no match.

**Real-World Use Case:**
An attribution pipeline joined `ad-clicks` with `purchases` within 30 minutes. The purchases topic had 12 partitions and the clicks topic 24, so the application failed to start. They created a 24-partition re-keyed copy of purchases (with `repartition()`) and kept the window and grace explicit, so late mobile events still matched.

**Common Mistakes / Interviewer Red Flags:**
- Expecting a stream-table join to update earlier results when the table changes.
- Joining topics with different partition counts or different partitioners.
- Huge join windows that make window stores and changelogs enormous.
- Forgetting that a stream-stream join can emit several results per record.

**Follow-up Questions Interviewers Ask:**
- When would you pick a foreign-key join? — Joining orders (key `order_id`) to customers (key `customer_id`) as tables, with no manual re-keying.
- What sets the join's state size? — Window size plus grace times the input rate, on both sides.
- How do you avoid repartitioning? — Produce with the join key already set, and keep partition counts and partitioners aligned.

---

### Q508. Windowing in Kafka Streams: tumbling, hopping, sliding, session windows and grace
**ID:** KFK-40
**Topic:** Kafka > Kafka Streams
**Level:** Intermediate
**Frequency:** Common
**Tags:** `kafka-streams`, `windows`, `grace-period`, `late-data`, `suppress`

**Short Answer (2–3 lines):**
Windowed aggregations group records by event time into **tumbling** (fixed, non-overlapping), **hopping** (fixed, overlapping), **sliding** (defined by the time difference between records) or **session** windows (activity separated by an inactivity gap). A **grace period** keeps a window open for late records after its end. Records later than that are dropped. By default Streams emits a running update per record; `suppress(untilWindowCloses)` emits one final result per window.

**Detailed Explanation:**
| Window | API | Example question |
|---|---|---|
| Tumbling | `TimeWindows.ofSizeAndGrace(5 min, 2 min)` | Orders per 5 minutes |
| Hopping | `TimeWindows…advanceBy(1 min)` | 5-minute moving count updated every minute |
| Sliding | `SlidingWindows.ofTimeDifferenceAndGrace(…)` | Any 10 minutes containing 3 failed logins |
| Session | `SessionWindows.ofInactivityGapAndGrace(30 min, …)` | User sessions (see Q132 for the SQL version) |

**Stream time.** Streams tracks the highest timestamp seen per partition. A window closes when stream time passes `window end + grace`, not when wall-clock time does. A quiet partition never advances stream time, so its last windows don't close until new data arrives. That's a classic surprise in tests and at low volume.

**Grace.** Kafka 3.0 deprecated the old implicit 24-hour grace. You now choose with `ofSizeAndGrace` or `ofSizeWithNoGrace`. Larger grace means more late data accepted, but more state and later final results.

**Emission**
- Default: an update downstream for every input (reduced by the record cache).
- `suppress(Suppressed.untilWindowCloses(…))`: one result per window, which suits alerts and sinks that can't take updates. The buffer must fit in memory, or set a bound with a policy.
- Kafka 3.3+ also has `emitStrategy(EmitStrategy.onWindowClose())` on windowed aggregations.

**Late records** past grace are dropped and counted in the `dropped-records-total` metric. Alert on it.

**Example / Code:**
```java
// Runtime: Java 21, Apache Kafka Streams 4.3.1 + kafka-streams-test-utils (TopologyTestDriver, no broker)
import java.time.*;
import java.util.Properties;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

public class Windows {
    public static void main(String[] args) {
        var s = Serdes.String();
        StreamsBuilder b = new StreamsBuilder();
        b.stream("clicks", Consumed.with(s, s))                                    // key = page
         .groupByKey()
         .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofMinutes(5), Duration.ofMinutes(2)))
         .count()
         .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))  // one final result per window
         .toStream((w, n) -> w.key() + "@" + w.window().startTime().toString().substring(11, 16))
         .to("clicks-per-5min", Produced.with(s, Serdes.Long()));

        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "windows-demo");
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        try (TopologyTestDriver d = new TopologyTestDriver(b.build(), p)) {
            var in = d.createInputTopic("clicks", new StringSerializer(), new StringSerializer());
            var out = d.createOutputTopic("clicks-per-5min", new StringDeserializer(), new LongDeserializer());
            Instant t = Instant.parse("2026-10-17T09:00:00Z");
            String[][] events = {          // {event-time offset in seconds, label}
                {"30", "on time"}, {"200", "on time"}, {"290", "on time"},
                {"330", "next window"},
                {"280", "late but within grace (stream time 09:05:30)"},
                {"430", "advances stream time to 09:07:10 -> 09:00 window closes"},
                {"100", "too late: 09:00 window already closed"},
                {"900", "advances stream time, closes 09:05 window"}};
            for (String[] e : events) {
                in.pipeInput("/home", e[1], t.plusSeconds(Long.parseLong(e[0])));
                System.out.printf("event at %s (%s) -> emitted %s%n",
                    t.plusSeconds(Long.parseLong(e[0])).toString().substring(11, 19), e[1], out.readKeyValuesToList());
            }
            System.out.println("dropped late records: " + d.metrics().entrySet().stream()
                .filter(m -> m.getKey().name().equals("dropped-records-total"))
                .mapToDouble(m -> (Double) m.getValue().metricValue()).sum());
        }
    }
}
```
```text
event at 09:00:30 (on time) -> emitted []
event at 09:03:20 (on time) -> emitted []
event at 09:04:50 (on time) -> emitted []
event at 09:05:30 (next window) -> emitted []
event at 09:04:40 (late but within grace (stream time 09:05:30)) -> emitted []
event at 09:07:10 (advances stream time to 09:07:10 -> 09:00 window closes) -> emitted [KeyValue(/home@09:00, 4)]
event at 09:01:40 (too late: 09:00 window already closed) -> emitted []
event at 09:15:00 (advances stream time, closes 09:05 window) -> emitted [KeyValue(/home@09:05, 2)]
dropped late records: 1.0
```
The 09:00 window counted four records, including the late 09:04:40 one that arrived within grace. It was emitted only once stream time passed 09:07 (end plus 2 minutes of grace). The 09:01:40 record came after that and was dropped.

**Real-World Use Case:**
A ride-hailing team computed per-zone demand in 1-minute tumbling windows with 30 seconds of grace. Phones with bad connectivity sent events up to several minutes late, which were dropped. They raised grace for that stream to 5 minutes for the "final" table, and kept a separate fast, un-suppressed stream for live dashboards.

**Common Mistakes / Interviewer Red Flags:**
- Expecting windows to close on wall-clock time.
- Using `suppress` with an unbounded buffer on high-cardinality keys.
- Not handling dropped late records, which fails silently.
- Mixing up hopping windows (fixed grid) and sliding windows (record-driven).

**Follow-up Questions Interviewers Ask:**
- How do you get event time from the payload? — Implement a `TimestampExtractor` and set it per source or via `default.timestamp.extractor`.
- What does the window store retain? — Windows for `window size + grace`, or longer with `Materialized.withRetention`.
- How do Spark and Flink compare? — Both use watermarks for the same idea (see Q290 and Q516).

---

### Q509. State stores, changelog topics and restoring state in Kafka Streams
**ID:** KFK-41
**Topic:** Kafka > Kafka Streams
**Level:** Intermediate
**Frequency:** Common
**Tags:** `kafka-streams`, `state-stores`, `changelog`, `standby-replicas`, `interactive-queries`

**Short Answer (2–3 lines):**
Stateful operators (aggregations, joins, windows) keep state in local **state stores**, RocksDB by default. Every update is also written to a compacted **changelog topic** named `<application.id>-<store>-changelog`. If an instance dies, or its local disk is wiped, the task's new owner rebuilds the store by replaying the changelog. **Standby replicas** keep warm copies elsewhere to cut that restore time. Stores can also be read directly with interactive queries.

**Detailed Explanation:**
**What gets created** (shown by `Topology.describe()` and the topic list below)
- One store per stateful operator, partitioned like its input: task N owns partition N of the store.
- A changelog topic per store, with `cleanup.policy=compact`, plus `compact,delete` for windowed stores so expired windows age out.
- **Repartition topics** (`<app>-<name>-repartition`) whenever you re-key before a stateful step.

**Restoration**
1. A task is assigned to an instance.
2. If the local RocksDB checkpoint is current, it resumes almost at once.
3. Otherwise it replays the changelog from the checkpointed offset, or from the start. For large stores that can take minutes to hours, and the task doesn't process during that time.

**Reducing downtime**
- `num.standby.replicas=1` or more: another instance keeps a shadow copy by consuming the changelog.
- Warm-up replicas (KIP-441): the assignor moves a task only once a new owner has caught up to within `acceptable.recovery.lag` (10,000 records). `max.warmup.replicas` (2) limits how many are in flight.
- Keep `state.dir` on a persistent volume (a StatefulSet on Kubernetes), so restarts don't replay from scratch.
- Static membership, or the new Streams rebalance protocol (KIP-1071, `group.protocol=streams`, production-ready since 4.2), reduces needless task movement.

**Exactly-once.** With `processing.guarantee=exactly_once_v2`, changelog writes, output records and input offsets commit in one transaction (see Q494). Commit interval drops to 100 ms by default.

**Interactive queries.** `KafkaStreams.store(…)` reads the local store. For keys held on other instances, use `queryMetadataForKey` to find the right host and call it over your own RPC layer.

**Example / Code:**
```java
// Runtime: Java 21, Apache Kafka Streams 4.3.1 against a real broker (localhost:19092)
import java.time.Duration;
import java.util.Properties;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.*;

public class StateStores {
    public static void main(String[] args) throws Exception {
        var s = Serdes.String();
        StreamsBuilder b = new StreamsBuilder();
        b.stream("page-views", Consumed.with(s, s))                      // key = user, value = country
         .selectKey((user, country) -> country)                          // re-key -> repartition topic
         .groupByKey(Grouped.with("by-country", s, s))
         .count(Materialized.<String, Long, KeyValueStore<org.apache.kafka.common.utils.Bytes, byte[]>>as("views-by-country")
                .withKeySerde(s).withValueSerde(Serdes.Long()));

        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "country-views");
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092");
        p.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/claude-0/-home-user/2fbb010b-4216-53a7-9bcb-97b9a1ca7ff7/scratchpad/kbank/streams-state");
        p.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        p.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1);
        p.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);               // single-broker lab only

        Topology t = b.build();
        System.out.println(t.describe());
        try (KafkaStreams app = new KafkaStreams(t, p)) {
            app.start();
            try (var prod = new KafkaProducer<String, String>(Properties.class.cast(new Properties() {{
                    put("bootstrap.servers", "localhost:19092");
                    put("key.serializer", StringSerializer.class.getName());
                    put("value.serializer", StringSerializer.class.getName()); }}))) {
                String[][] views = {{"u1", "IN"}, {"u2", "US"}, {"u3", "IN"}, {"u4", "DE"}, {"u5", "IN"}};
                for (String[] v : views) prod.send(new ProducerRecord<>("page-views", v[0], v[1]));
            }
            ReadOnlyKeyValueStore<String, Long> store = null;
            for (int i = 0; i < 60; i++) {                                   // wait for RUNNING + data
                Thread.sleep(500);
                if (app.state() != KafkaStreams.State.RUNNING) continue;
                store = app.store(StoreQueryParameters.fromNameAndType("views-by-country", QueryableStoreTypes.keyValueStore()));
                Long in = store.get("IN");
                if (in != null && in == 3 && store.get("US") != null && store.get("DE") != null) break;
            }
            System.out.println("interactive query: IN=" + store.get("IN") + " US=" + store.get("US") + " DE=" + store.get("DE"));
        }
    }
}
```
Run against the single lab broker, then again after deleting the local `state.dir` (the second run produced the same five page views again):
```text
Topologies:
   Sub-topology: 0
    Source: KSTREAM-SOURCE-0000000000 (topics: [page-views])
      --> KSTREAM-KEY-SELECT-0000000001
    Processor: KSTREAM-KEY-SELECT-0000000001 (stores: [])
      --> by-country-repartition-filter
      <-- KSTREAM-SOURCE-0000000000
    Processor: by-country-repartition-filter (stores: [])
      --> by-country-repartition-sink
      <-- KSTREAM-KEY-SELECT-0000000001
    Sink: by-country-repartition-sink (topic: by-country-repartition)
      <-- by-country-repartition-filter

  Sub-topology: 1
    Source: by-country-repartition-source (topics: [by-country-repartition])
      --> KSTREAM-AGGREGATE-0000000002
    Processor: KSTREAM-AGGREGATE-0000000002 (stores: [views-by-country])
      --> none
      <-- by-country-repartition-source

interactive query: IN=6 US=2 DE=2
$ bin/kafka-topics.sh --bootstrap-server localhost:19092 --list | grep country-views
country-views-by-country-repartition
country-views-views-by-country-changelog
$ bin/kafka-configs.sh --bootstrap-server localhost:19092 --describe --entity-type topics \
    --entity-name country-views-views-by-country-changelog
  cleanup.policy=compact sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:cleanup.policy=compact, DEFAULT_CONFIG:log.cleanup.policy=delete}
```
The local RocksDB files were gone, yet the counts continued from the first run's totals (IN 3 → 6). The store had been rebuilt from its changelog before the new records were counted.

**Real-World Use Case:**
A Streams app with a 200 GB session store ran on Kubernetes with `emptyDir` volumes. Every rolling deploy rebuilt the store from the changelog, and output stopped for close to an hour. Moving to a StatefulSet with persistent volumes and `num.standby.replicas=1` cut deploy impact to seconds.

**Common Mistakes / Interviewer Red Flags:**
- Thinking the changelog is optional "backup"; it *is* the store's durability.
- Changing `application.id` casually, which orphans every internal topic and state directory.
- Deleting internal topics by hand instead of using `kafka-streams-application-reset.sh`, which also refuses to run while members are active.
- Running stateful apps on ephemeral disks with no standbys.

**Follow-up Questions Interviewers Ask:**
- How do you size restore time? — Changelog size divided by restore throughput; watch the `restore-*` metrics and `StateRestoreListener`.
- Can you turn off the changelog? — Yes (`withLoggingDisabled()`), but the state is then lost on any failover.
- What does `exactly_once_v2` change for stores? — Store updates become visible downstream only when the transaction commits; on a crash the store is restored to the last committed state.

---

### Q510. Capacity planning: brokers, disks, network and partition-count limits
**ID:** KFK-42
**Topic:** Kafka > Operations & Sizing
**Level:** Advanced
**Frequency:** Common
**Tags:** `kafka`, `capacity-planning`, `throughput`, `partitions`, `sizing`

**Short Answer (2–3 lines):**
Size from the bytes, not from message counts. Peak produce rate × replication factor sets disk writes and inbound network. Replication plus every consumer group's read sets outbound network. Rate × retention × replication factor, with headroom, sets disk. The number of brokers is the largest of the network, disk and partition-count requirements, plus one spare for maintenance. Partition counts per broker are limited in practice by recovery time, open files and metadata, not by a hard rule.

**Detailed Explanation:**
How many partitions a single *topic* needs is on this site's partition-count page. This question is about sizing the *cluster*.

**The core arithmetic (RF = 3, G consumer groups)**
| Resource | Load at peak |
|---|---|
| Inbound network | produce × RF (client writes plus follower fetches arriving) |
| Outbound network | produce × (RF − 1) for replication + produce × G for consumers |
| Disk write | produce × RF |
| Disk capacity | average produce × retention × RF ÷ target fill (for example 60%) |
| Page cache | Enough RAM to hold the tail that consumers read (minutes to hours), so reads don't hit disk |

**Headroom matters more than averages.** You need spare capacity for a broker failure (its load moves to the others), for consumers catching up after an outage (reads at many times the normal rate), and for partition reassignment traffic (throttle it with `--throttle`).

**Partition limits.** Kafka has no fixed maximum, and KRaft raised practical cluster-wide limits a lot. Per broker, every replica costs file handles, memory-mapped indexes, replica fetcher work and leader-election time after a failure. A few thousand replicas per broker is a common planning number; very small brokers should hold fewer. Measure: controlled shutdown time and leader-election time grow with replicas per broker.

**Cost levers**
- Compression (see Q475) cuts every number in the table.
- Tiered storage (see Q497) breaks the link between retention and broker disk.
- Follower fetching (see Q492) moves consumer reads to same-zone replicas.

**Example / Code:**
```python
# Runtime: Python 3.11 (standard library only) — back-of-envelope Kafka capacity plan
import math

# ---- inputs (replace with your own measurements) ----
ingress_mb_s     = 120      # average produce rate across the cluster, MB/s (after compression)
peak_factor      = 2.0      # peak / average
rf               = 3        # replication factor
consumer_groups  = 4        # groups each reading the full stream (lag-free tail reads)
retention_days   = 3        # local retention
disk_headroom    = 0.6      # keep disks <= 60% full
broker_disk_tb   = 8        # usable disk per broker
broker_net_mb_s  = 1200     # ~10 Gbit/s NIC, usable MB/s per direction
net_headroom     = 0.5      # plan for <= 50% NIC use at peak (failover, catch-up, reassignment)
partitions_total = 600      # sum over topics, before replication
max_replicas_per_broker = 4000   # a conservative planning limit, not a hard Kafka limit

peak_in = ingress_mb_s * peak_factor
# Broker network: in = produce + replication from leaders; out = replication to followers + consumers
cluster_in  = peak_in * rf                          # every byte lands on rf brokers
cluster_out = peak_in * (rf - 1) + peak_in * consumer_groups
brokers_for_net = math.ceil(max(cluster_in, cluster_out) / (broker_net_mb_s * net_headroom))

stored_tb = ingress_mb_s * 86400 * retention_days * rf / 1e6
brokers_for_disk = math.ceil(stored_tb / (broker_disk_tb * disk_headroom))

replicas = partitions_total * rf
brokers_for_partitions = math.ceil(replicas / max_replicas_per_broker)

brokers = max(brokers_for_net, brokers_for_disk, brokers_for_partitions, rf)
print(f"peak produce          : {peak_in:,.0f} MB/s")
print(f"cluster network in/out: {cluster_in:,.0f} / {cluster_out:,.0f} MB/s at peak")
print(f"stored data           : {stored_tb:,.1f} TB ({retention_days} days x RF {rf})")
print(f"brokers needed  -> network {brokers_for_net}, disk {brokers_for_disk}, partitions {brokers_for_partitions}")
print(f"plan: {brokers} brokers (+1 for N+1 maintenance = {brokers + 1}); "
      f"{replicas / (brokers + 1):,.0f} replicas per broker")

# Same cluster with tiered storage keeping only 12 hours on broker disks (see Q497)
local_tb = ingress_mb_s * 86400 * 0.5 * rf / 1e6
print(f"with tiered storage (12 h local): {local_tb:,.1f} TB local -> "
      f"{math.ceil(local_tb / (broker_disk_tb * disk_headroom))} brokers for disk")
```
```text
peak produce          : 240 MB/s
cluster network in/out: 720 / 1,440 MB/s at peak
stored data           : 93.3 TB (3 days x RF 3)
brokers needed  -> network 3, disk 20, partitions 1
plan: 20 brokers (+1 for N+1 maintenance = 21); 86 replicas per broker
with tiered storage (12 h local): 15.6 TB local -> 4 brokers for disk
```
With these inputs, disk retention, not throughput, sets the broker count. Twelve hours of local data with tiered storage brings the disk requirement below the network one. Validate the per-broker figures with `kafka-producer-perf-test.sh` and `kafka-consumer-perf-test.sh` on your own hardware and payloads.

**Real-World Use Case:**
A team sized a cluster from "2 million messages per second", but its messages averaged 4 KB, not the 200 bytes they had assumed. Network saturated on day one. Re-planning in MB/s with compression ratios measured on real payloads, plus failover headroom, gave a cluster that ran at about 40% at peak.

**Common Mistakes / Interviewer Red Flags:**
- Ignoring replication and consumer fan-out in network math.
- Sizing for average load with no failover headroom.
- Putting tens of thousands of partitions on a few small brokers "just in case".
- Forgetting the page cache: consumers that read from disk compete with producers for I/O.

**Follow-up Questions Interviewers Ask:**
- Why does a lagging consumer hurt everyone? — It reads cold data from disk, evicting page cache and adding I/O that slows produce latency.
- How do you add capacity? — Add brokers, then reassign partitions with throttling (or use Cruise Control); new brokers don't take load on their own.
- What limits a single partition's throughput? — One leader's disk and network plus a single consumer's processing speed.

---

### Q511. What to monitor on a Kafka cluster, and the key broker metrics
**ID:** KFK-43
**Topic:** Kafka > Operations & Monitoring
**Level:** Beginner
**Frequency:** Very Common
**Tags:** `kafka`, `monitoring`, `jmx`, `under-replicated-partitions`, `alerting`

**Short Answer (2–3 lines):**
Alert first on availability and durability signals: **offline partitions**, **under-min-ISR partitions**, **under-replicated partitions**, and an active controller count other than one. Then watch saturation: request handler and network idle ratios, produce and fetch latency, disk usage, and ISR shrink rate. On the client side, consumer lag (time-based, see Q483) and producer errors are what users actually feel.

**Detailed Explanation:**
| Metric (JMX name) | Healthy | Meaning when bad |
|---|---|---|
| `kafka.controller:type=KafkaController,name=OfflinePartitionsCount` | 0 | Partitions with no leader: unavailable |
| `kafka.server:type=ReplicaManager,name=UnderMinIsrPartitionCount` | 0 | `acks=all` producers are failing on these |
| `kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions` | 0 | Reduced redundancy (broker down, slow disk, network) |
| `kafka.controller:type=KafkaController,name=ActiveControllerCount` | 1 across controllers | 0 means no metadata changes are possible |
| `IsrShrinksPerSec` / `IsrExpandsPerSec` | Near 0 | Flapping followers |
| `KafkaRequestHandlerPool … RequestHandlerAvgIdlePercent` | > 0.3 | Brokers are CPU-bound on requests |
| `SocketServer … NetworkProcessorAvgIdlePercent` | > 0.3 | Network threads saturated |
| `RequestMetrics … TotalTimeMs` (Produce, FetchConsumer) p99 | Stable | Latency regressions; break down by queue, local, remote time |
| `BrokerTopicMetrics … MessagesInPerSec`, `BytesInPerSec`, `BytesOutPerSec` | Expected | Traffic and capacity trends |
| Disk used per log dir, `LogFlush`, partition and leader count per broker | Balanced | Skew and capacity |

**How to collect.** Kafka exposes JMX. Most teams run the Prometheus JMX exporter as a Java agent, or use the managed service's metrics, and build alerts plus a per-broker dashboard. `kafka-jmx.sh` is handy for spot checks.

**Alerting tiers**
- *Page*: offline > 0, under-min-ISR > 0 for more than a minute, no active controller, disk > 85%.
- *Ticket*: URP > 0 for more than 10 minutes, idle ratios < 0.2, consumer time-lag SLO breaches, leader imbalance.

**Example / Code:**
```bash
# Runtime: Apache Kafka 4.3.1 lab cluster; broker 3 started with JMX_PORT=9993
source ../env.sh
jmx(){ kafka-jmx.sh --jmx-url service:jmx:rmi:///jndi/rmi://localhost:9993/jmxrmi \
         --object-name "$1" ${2:+--attributes $2} --one-time true --report-format tsv 2>/dev/null | grep -v '^time'; }
echo "== healthy";   for m in UnderReplicatedPartitions UnderMinIsrPartitionCount PartitionCount LeaderCount; do
  echo "$m: $(jmx kafka.server:type=ReplicaManager,name=$m Value | cut -f2)"; done
bash ../cluster/stop.sh 2 >/dev/null; sleep 5
echo "== broker 2 stopped"; for m in UnderReplicatedPartitions UnderMinIsrPartitionCount IsrShrinksPerSec; do
  echo "$m: $(jmx kafka.server:type=ReplicaManager,name=$m | cut -f2 | head -1)"; done
echo "MessagesInPerSec (1-min rate): $(jmx kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec OneMinuteRate | cut -f2)"
echo "Produce p99 total time (ms): $(jmx 'kafka.network:type=RequestMetrics,name=TotalTimeMs,request=Produce' 99thPercentile | cut -f2)"
echo "RequestHandlerAvgIdlePercent: $(jmx kafka.server:type=KafkaRequestHandlerPool,name=RequestHandlerAvgIdlePercent OneMinuteRate | cut -f2)"
kafka-topics.sh --bootstrap-server localhost:29091 --describe --under-min-isr-partitions 2>/dev/null | head -3
bash ../cluster/start.sh 2 >/dev/null
```
```text
== healthy
UnderReplicatedPartitions: 0
UnderMinIsrPartitionCount: 0
PartitionCount: 64
LeaderCount: 0
== broker 2 stopped
UnderReplicatedPartitions: 20
UnderMinIsrPartitionCount: 0
IsrShrinksPerSec: 0
MessagesInPerSec (1-min rate): 0.0
Produce p99 total time (ms): 0.0
RequestHandlerAvgIdlePercent: 0.9741596090110437
```
Stopping one of three brokers made 20 of broker 3's partitions under-replicated, but none fell below `min.insync.replicas`, so producers were unaffected. `LeaderCount: 0` is a finding too: broker 3 had just restarted and hadn't yet received leadership back (see Q491), so it carried replicas but no traffic. The lab was idle, which is why the rates read zero.

**Real-World Use Case:**
A team alerted only on broker CPU and disk. A slow disk on one broker caused followers to drop out of the ISR repeatedly. URP flapped for days unnoticed, until a second broker restart pushed partitions under min ISR and producers failed. Adding URP and ISR-shrink alerts would have caught it on day one.

**Common Mistakes / Interviewer Red Flags:**
- Paging on URP during planned rolling restarts (suppress alerts during maintenance).
- Monitoring brokers but not consumer lag.
- No per-broker breakdown, which hides the one bad broker behind cluster averages.

**Follow-up Questions Interviewers Ask:**
- Where does `OfflinePartitionsCount` come from in KRaft? — The active controller's metrics; scrape the controller nodes too.
- How do you find which partitions are affected? — `kafka-topics.sh --describe --under-replicated-partitions`, `--under-min-isr-partitions`, `--unavailable-partitions`.
- What does a high remote time in `TotalTimeMs` mean for Produce? — The leader is waiting for followers (`acks=all`), so look at replication and follower health.

---

### Q512. Securing Kafka: TLS, SASL mechanisms, ACLs and quotas
**ID:** KFK-44
**Topic:** Kafka > Security
**Level:** Beginner
**Frequency:** Common
**Tags:** `kafka`, `security`, `sasl`, `tls`, `acls`

**Short Answer (2–3 lines):**
Kafka security has three layers. **Encryption** with TLS on every listener. **Authentication** with mutual TLS or SASL: SCRAM, OAUTHBEARER/OIDC, Kerberos (GSSAPI) or PLAIN over TLS. **Authorization** with ACLs on topics, groups, transactional IDs and the cluster, enforced by `StandardAuthorizer` in KRaft. Add quotas so one client can't starve the others, and give each application its own principal with least privilege.

**Detailed Explanation:**
**Listeners.** Each listener has a security protocol: `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT` or `SASL_SSL`. Production uses `SSL` or `SASL_SSL` everywhere, including inter-broker and controller listeners.

**Authentication options**
| Mechanism | Credential | Notes |
|---|---|---|
| mTLS (`SSL` with `ssl.client.auth=required`) | Client certificate | Principal from the certificate DN (map it with `ssl.principal.mapping.rules`) |
| SASL/SCRAM-SHA-256/512 | Username + password | Credentials stored, salted, in cluster metadata; no external system |
| SASL/OAUTHBEARER | OIDC token | Integrates with an identity provider; short-lived tokens |
| SASL/GSSAPI | Kerberos ticket | Common in Hadoop-era enterprises |
| SASL/PLAIN | Username + password | Only over TLS; usually backed by a custom callback |

**ACLs**
- Resources: `Topic`, `Group`, `Cluster`, `TransactionalId`, `DelegationToken`. Pattern: `LITERAL` or `PREFIXED`.
- A consumer needs Read on the topic **and** Read on its group. An idempotent or transactional producer needs Write, plus Write and Describe on its `TransactionalId`.
- With an authorizer configured and no matching ACL, access is denied (`allow.everyone.if.no.acl.found=false` by default).
- Manage ACLs as code, with prefixed patterns per team (`team-a.`), rather than by hand.

**Quotas.** Per user or client ID: `producer_byte_rate`, `consumer_byte_rate`, `request_percentage`, and controller mutation rate. Clients over quota are throttled, not failed.

**Other**: encrypt data at rest (disk encryption or a cloud KMS), keep secrets out of config files with config providers, and audit authorizer logs.

**Example / Code:**
```bash
# Runtime: Apache Kafka 4.3.1 single broker: StandardAuthorizer, super.users=User:ANONYMOUS (the lab's PLAINTEXT admin),
#          extra listener SASL_PLAINTEXT://localhost:19094 with SCRAM-SHA-512
source ../env.sh
kafka-topics.sh --bootstrap-server $B --create --topic payments.secure --partitions 1 >/dev/null 2>&1
# 1) Create SCRAM credentials (stored in KRaft metadata; the password is salted and hashed)
kafka-configs.sh --bootstrap-server $B --alter --entity-type users --entity-name analytics \
  --add-config 'SCRAM-SHA-512=[iterations=8192,password=analytics-secret]'
# 2) Least privilege: analytics may read payments.secure with its own group prefix, nothing else
kafka-acls.sh --bootstrap-server $B --add --allow-principal User:analytics \
  --operation Read --operation Describe --topic payments.secure
kafka-acls.sh --bootstrap-server $B --add --allow-principal User:analytics \
  --operation Read --group analytics- --resource-pattern-type prefixed
kafka-acls.sh --bootstrap-server $B --list --principal User:analytics
```
```text
Completed updating config for user analytics.
Adding ACLs for resource `ResourcePattern(resourceType=TOPIC, name=payments.secure, patternType=LITERAL)`: 
 	(principal=User:analytics, host=*, operation=DESCRIBE, permissionType=ALLOW)
	(principal=User:analytics, host=*, operation=READ, permissionType=ALLOW)

Adding ACLs for resource `ResourcePattern(resourceType=GROUP, name=analytics-, patternType=PREFIXED)`: 
 	(principal=User:analytics, host=*, operation=READ, permissionType=ALLOW)
```
```python
# Runtime: Python 3.11, confluent-kafka 2.16, against the SASL_PLAINTEXT (SCRAM-SHA-512) listener
# Production would use SASL_SSL (TLS + SASL); plaintext SASL here only keeps the lab simple.
import time
from confluent_kafka import Consumer, Producer

def conf(password, **extra):
    return {"bootstrap.servers": "localhost:19094", "security.protocol": "SASL_PLAINTEXT",
            "sasl.mechanism": "SCRAM-SHA-512", "sasl.username": "analytics",
            "sasl.password": password, **extra}

def try_read(password, group):
    errors = []                                   # client-level errors (e.g. authentication) arrive here
    c = Consumer(conf(password, **{"group.id": group, "auto.offset.reset": "earliest",
                                   "error_cb": lambda e: errors.append(e.name())}))
    c.subscribe(["payments.secure"])
    try:
        for _ in range(20):
            m = c.poll(0.5)
            if errors:
                return errors[0]
            if m is None:
                continue
            return m.error().name() if m.error() else "read " + m.value().decode()
        return "no data"
    finally:
        c.close()

print("wrong password      :", try_read("guess", f"analytics-{time.time():.0f}"))
print("allowed group       :", try_read("analytics-secret", f"analytics-{time.time():.0f}"))
print("group not in ACL    :", try_read("analytics-secret", "finance-reports"))

p = Producer(conf("analytics-secret"))
p.produce("payments.secure", value=b"{}", on_delivery=lambda e, m: print("write attempt       :", e.name() if e else "ok"))
p.flush(10)
```
```text
wrong password      : _AUTHENTICATION
allowed group       : read {"id":"p-1","amount":42}
group not in ACL    : GROUP_AUTHORIZATION_FAILED
write attempt       : TOPIC_AUTHORIZATION_FAILED
```
The broker config added a `SASL_PLAINTEXT://localhost:19094` listener with `sasl.enabled.mechanisms=SCRAM-SHA-512` and `listener.name.sasl_plaintext.scram-sha-512.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required;`. Each denial matched exactly one missing permission.

**Real-World Use Case:**
A shared cluster gave every service the same super-user credentials. One misconfigured job deleted a production topic. Moving to one principal per service, prefixed ACLs per team, and no delete rights outside the platform team's automation made that class of incident impossible.

**Common Mistakes / Interviewer Red Flags:**
- SASL/PLAIN or SCRAM without TLS, which sends credentials in clear text.
- Granting a consumer topic Read but forgetting group Read.
- Using `allow.everyone.if.no.acl.found=true` "temporarily".
- Shared credentials across applications, which makes auditing and revocation impossible.

**Follow-up Questions Interviewers Ask:**
- How do you rotate SCRAM passwords? — Add a new credential (or user), roll clients to it, then remove the old one; mechanisms can coexist.
- What permissions does a transactional producer need? — Write on the topics, plus Write and Describe on its `TransactionalId`, and IdempotentWrite on the cluster for older brokers.
- How is the principal derived for mTLS? — From the certificate's subject DN, optionally rewritten by `ssl.principal.mapping.rules`.

---

### Q513. Multi-region Kafka: MirrorMaker 2, offset translation and cluster-linking concepts
**ID:** KFK-45
**Topic:** Kafka > Multi-Region & Replication
**Level:** Advanced
**Frequency:** Common
**Tags:** `kafka`, `mirrormaker-2`, `disaster-recovery`, `multi-region`, `offset-translation`

**Short Answer (2–3 lines):**
MirrorMaker 2 (MM2) is a set of Kafka Connect connectors that copy topics between clusters asynchronously. By default it renames them (`east.payments` on the target), syncs topic configs and ACLs, and emits **checkpoints** that translate consumer-group offsets, so consumers can fail over to the other region. Because replication is asynchronous, failover can lose the last few seconds of data and replay some. **Cluster linking**, in some commercial distributions, mirrors at the log level with identical offsets instead.

**Detailed Explanation:**
**MM2 components**
| Connector | Job |
|---|---|
| `MirrorSourceConnector` | Copies records and topic configs and ACLs; writes offset-sync records |
| `MirrorCheckpointConnector` | Translates group offsets into checkpoints on the target; with `sync.group.offsets.enabled=true` it writes them into the target's `__consumer_offsets` |
| `MirrorHeartbeatConnector` | Emits heartbeats to measure replication lag and detect topology |

**Topologies**
- *Active–passive*: one direction, for DR. Consumers fail over using translated offsets.
- *Active–active*: both directions. The default `DefaultReplicationPolicy` adds a cluster prefix, which prevents infinite loops, and consumers read `orders` plus `west.orders`. `IdentityReplicationPolicy` keeps names but is only safe for one-way flows.
- *Hub-and-spoke / aggregation*: many edge clusters feed a central analytics cluster.

**Offsets differ between clusters.** Offset 1,000 in the source isn't offset 1,000 in the target (retention, compaction and restarts all shift them). MM2 records offset-sync pairs and translates conservatively: a failed-over consumer may *re-read* some records but shouldn't skip any. Consumers must be idempotent.

**What a DR plan has to say**
- RPO is replication lag (seconds, typically); RTO is how fast clients re-point.
- Producers need a switch: DNS, a config flag, or client bootstrap lists per region.
- Schema Registry, ACLs, quotas and topic configs must exist in both regions.
- Fail-back: reverse the flow and catch up before switching back.

**Cluster linking (concept).** The destination broker fetches from the source like a follower, so offsets, record bytes and timestamps are preserved and no Connect cluster is needed. It's available in some commercial platforms rather than Apache Kafka itself. Apache Kafka's open-source path is MM2.

**Example / Code:**
```properties
# Runtime: Apache Kafka 4.3.1 MirrorMaker 2 (dedicated mode): replicate "east" (single broker) -> "west" (3-broker cluster)
clusters = east, west
east.bootstrap.servers = localhost:19092
west.bootstrap.servers = localhost:29091,localhost:29092,localhost:29093
east->west.enabled = true
east->west.topics = payments
east->west.groups = payments-reader
west->east.enabled = false
replication.factor = 3
sync.group.offsets.enabled = true
sync.group.offsets.interval.seconds = 5
emit.checkpoints.interval.seconds = 5
refresh.topics.interval.seconds = 10
refresh.groups.interval.seconds = 10
# single-broker source: internal topics on "east" can only have one replica
east.offset.storage.replication.factor = 1
east.config.storage.replication.factor = 1
east.status.storage.replication.factor = 1
checkpoints.topic.replication.factor = 3
heartbeats.topic.replication.factor = 3
offset-syncs.topic.replication.factor = 1
offset-syncs.topic.location = source
```
```bash
# Runtime: Apache Kafka 4.3.1 — "east" has topic payments and a group that has consumed part of it
bin/connect-mirror-maker.sh mm2.properties &
bin/kafka-topics.sh --bootstrap-server localhost:29091 --list
bin/kafka-get-offsets.sh --bootstrap-server localhost:29091 --topic east.payments
bin/kafka-consumer-groups.sh --bootstrap-server localhost:29091 --describe --group payments-reader
```
```text
east.checkpoints.internal
east.heartbeats
east.payments
heartbeats
mm2-configs.east.internal
mm2-offsets.east.internal
mm2-status.east.internal
east.payments:0:1
east.payments:1:0
east.payments:2:4
east.payments:3:2
east.payments:4:0
east.payments:5:2
GROUP           TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID     HOST            CLIENT-ID
payments-reader east.payments   0          1               1               0               -               -               -
payments-reader east.payments   2          0               4               4               -               -               -
payments-reader east.payments   3          2               2               0               -               -               -
payments-reader east.payments   5          2               2               0               -               -               -
```
The group's committed positions on `east` were p0=1, p2=0, p3=2 and p5=2, and they appear on `west` against `east.payments`. A consumer started on `west` with group `payments-reader` would resume where it left off. Lab note: both "regions" ran on one host, and the 1-broker source forced replication factor 1 for its MM2 internal topics.

**Real-World Use Case:**
A payments company ran active–passive across two regions with MM2. In a DR drill, consumers switched to the passive region and re-read about two seconds of events. That was harmless because the sinks upserted by `payment_id`. The drill also found that the Schema Registry hadn't been replicated, so they added registry replication to the runbook.

**Common Mistakes / Interviewer Red Flags:**
- Assuming offsets are identical in the target cluster.
- Using `IdentityReplicationPolicy` in active–active, which creates replication loops.
- Forgetting Schema Registry, ACLs and quotas in the DR region.
- Claiming zero data loss with asynchronous replication.

**Follow-up Questions Interviewers Ask:**
- How do you measure MM2 lag? — Replication latency metrics from MirrorSourceConnector, plus heartbeats topic timestamps.
- How does a consumer read both local and remote topics in active–active? — Subscribe by pattern (`.*\.?orders`) and deduplicate by event ID.
- What about a "stretched" cluster across regions? — Synchronous replication across regions gives RPO 0 but adds cross-region latency to every `acks=all` write; it suits nearby regions or zones, not distant ones.

---
