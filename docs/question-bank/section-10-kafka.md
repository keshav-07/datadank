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
A Lambda-style function produced events and returned without flushing. About one invocation in twenty lost its last records, because the runtime froze the process before the background thread sent them. Adding `flush(5)` before returning fixed it.

**Common Mistakes / Interviewer Red Flags:**
- Thinking `produce()` returning means the record is stored.
- Never calling `poll()` in Python, so callbacks never fire and the queue fills.
- Calling `flush()` after every record in a high-volume path.

**Follow-up Questions Interviewers Ask:**
- How do you guarantee nothing is lost on shutdown? — Handle SIGTERM, stop input, `flush()` with a timeout, and alert if the return value is non-zero.
- Are callbacks called in order? — Per partition, yes, in send order.
- Can one producer be shared across threads? — Yes; Java and librdkafka producers are thread-safe, and one per process is usually best.

---
