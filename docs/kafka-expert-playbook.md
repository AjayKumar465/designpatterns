# Kafka Expert Playbook -- Lead Role (Java, 10+ Years Experience)

A comprehensive end-to-end reference covering architecture, internals, production issues, error handling, and advanced patterns. Every section includes production-grade Java examples and real-world debugging strategies sourced from Kafka official docs, Confluent, Reddit, Medium, and production war stories.

---

## Table of Contents

1. [Kafka Fundamentals (Beyond Textbook Definitions)](#section-1-kafka-fundamentals-beyond-textbook-definitions)
2. [Storage Internals](#section-2-storage-internals)
3. [Replication and Fault Tolerance](#section-3-replication-and-fault-tolerance)
4. [Producer Deep Dive](#section-4-producer-deep-dive)
5. [Consumer Deep Dive](#section-5-consumer-deep-dive)
6. [Partition Strategy and Data Skew](#section-6-partition-strategy-and-data-skew)
7. [Delivery Guarantees](#section-7-delivery-guarantees)
8. [Error Handling and DLQ Patterns (Spring Boot)](#section-8-error-handling-and-dlq-patterns-spring-boot)
9. [Message Ordering](#section-9-message-ordering)
10. [Schema Registry and Schema Evolution](#section-10-schema-registry-and-schema-evolution)
11. [Kafka 4.0 -- Major Changes](#section-11-kafka-40----major-changes)
12. [Kafka Streams](#section-12-kafka-streams)
13. [Kafka Connect](#section-13-kafka-connect)
14. [Security](#section-14-security)
15. [Monitoring and Observability](#section-15-monitoring-and-observability)
16. [Production Issue Runbook](#section-16-production-issue-runbook)
17. [Design Patterns](#section-17-design-patterns)
18. [Performance Tuning Cheat Sheet](#section-18-performance-tuning-cheat-sheet)
19. [Spring Boot Production Configuration Summary](#section-19-spring-boot-production-configuration-summary)
20. [Quick Reference -- Interview Answer Templates](#section-20-quick-reference----interview-answer-templates)
21. [Lead Interview Questions -- Logical and Production Scenarios](#section-21-lead-interview-questions----logical-and-production-scenarios)

---

## Section 1: Kafka Fundamentals (Beyond Textbook Definitions)

### 1.1 Kafka vs Traditional Message Queues

Kafka is a **distributed commit log**, not a queue.

| Aspect | Kafka | RabbitMQ / ActiveMQ |
|--------|-------|---------------------|
| Message lifecycle | Retained based on retention policy | Deleted on consumption |
| Consumer model | Pull-based, read by offset | Push-based (or pull), ack-and-delete |
| Replay | Yes -- seek to any offset | No -- once consumed, gone |
| Ordering | Per-partition guarantee | Per-queue guarantee |
| Throughput | Millions of msgs/sec | Tens of thousands of msgs/sec |
| Semantics | Queue (same group) + Pub-Sub (different groups) simultaneously | Either queue OR pub-sub (exchanges) |

### 1.2 Core Architecture

```
Producer --> Broker Cluster (Partitioned Topics) --> Consumer Groups
                    |
              KRaft Controller Quorum (metadata management)
```

**Key components:**

- **Broker**: Stores data, serves clients. Each broker handles hundreds of thousands of reads/writes per second.
- **Topic**: Logical channel, split into partitions.
- **Partition**: Ordered, immutable sequence of records. The unit of parallelism.
- **Offset**: Unique sequential ID per record within a partition.
- **Consumer Group**: Set of consumers sharing workload across partitions.
- **KRaft Controller**: Manages cluster metadata (replaced ZooKeeper in Kafka 4.0).

### 1.3 How Does Kafka Achieve High Throughput?

Four reinforcing mechanisms:

1. **Sequential Disk I/O**: Append-only writes at ~500MB/s vs ~100KB/s for random writes.
2. **OS Page Cache**: Kafka delegates caching to the OS, avoiding JVM heap pressure.
3. **Zero-Copy Transfer**: Uses `sendfile()` system call (`FileChannel.transferTo()` in Java). Data goes from page cache directly to NIC buffer, bypassing user space entirely. Eliminates 2 data copies and 2 context switches compared to traditional transfer.
4. **Batching + Compression**: Producers batch records, compress at batch level (LZ4, Snappy, ZStd).

**When does zero-copy NOT work?**
When producer and consumer use different compression codecs, or broker-side re-compression is configured. Data must enter user space for decompression/recompression. Best practice: use consistent end-to-end compression codec.

### 1.4 Kafka Message Structure

Every Kafka record contains:

| Field | Description |
|-------|-------------|
| Key | Optional. Used for partitioning and compaction. |
| Value | The payload (your event/message). |
| Timestamp | CreateTime (producer-set) or LogAppendTime (broker-set). |
| Headers | Key-value metadata pairs (tracing IDs, content-type, etc.). |
| Offset | Assigned by the broker on append. Immutable. |
| Partition | Determined by key hash or custom partitioner. |

---

## Section 2: Storage Internals

### 2.1 Log Segments

Each partition is stored as a `UnifiedLog` consisting of multiple immutable segments:

| File | Purpose |
|------|---------|
| `.log` | Actual message data (FileRecords) |
| `.index` | Sparse offset index (entry every ~4KB) for fast lookups |
| `.timeindex` | Time-based index for time-based lookups |
| `.txnindex` | Transaction index for transactional consumers |
| `.snapshot` | Producer state snapshots for idempotent recovery |

**Segment rolling**: Controlled by `log.segment.bytes` (default 1GB) or `log.roll.ms`/`log.roll.hours`. Old segments are sealed and become immutable.

**How offset lookup works:**
1. Binary search on `.index` file to find the segment
2. Binary search within the segment's sparse index to find the nearest entry
3. Sequential scan from that entry to the target offset

### 2.2 Retention Policies

| Policy | Config | Behavior |
|--------|--------|----------|
| Delete | `retention.ms` (default 7 days), `retention.bytes` | Remove entire segments past threshold |
| Compact | `cleanup.policy=compact` | Keep only the latest value per key |
| Delete + Compact | `cleanup.policy=delete,compact` | Both policies applied |

### 2.3 Log Compaction Deep Dive

- Compaction retains the **last value for each key** within a partition.
- Tombstone records (key + null value) mark deletions; retained for `delete.retention.ms` before removal.
- Use cases: Rebuilding state from changelog, maintaining latest snapshots, KTable backing topics.
- **Compaction does NOT guarantee immediate removal** -- it runs in the background on sealed segments only. The active segment is never compacted.

### 2.4 Tiered Storage (Kafka 3.6+)

- Hot data on local broker disks, cold data on remote storage (S3, HDFS).
- Reduces broker disk costs for long-retention topics.
- Transparent to consumers -- offset-based reads work across tiers.

---

## Section 3: Replication and Fault Tolerance

### 3.1 ISR (In-Sync Replicas)

- ISR = set of replicas fully caught up with the leader's log (within `replica.lag.time.max.ms`, default 30s).
- Leader handles all reads and writes for a partition.
- A message is **committed** only when replicated to **all ISR members**.
- Follower evicted from ISR if it falls behind beyond the lag threshold.
- When a follower catches up, it is added back to the ISR.

### 3.2 Leader Election

- When a leader fails, the controller elects a new leader **exclusively from the current ISR**.
- Guarantees no data loss for committed messages.
- **Unclean leader election** (`unclean.leader.election.enable=false` by default): If enabled and no ISR member is available, an out-of-sync replica can become leader -- this **causes data loss**.

### 3.3 The Golden Durability Configuration

```properties
# Broker
default.replication.factor=3
min.insync.replicas=2
unclean.leader.election.enable=false

# Producer
acks=all
enable.idempotence=true
retries=2147483647
delivery.timeout.ms=120000
```

**What this guarantees:**
- `RF=3, min.insync.replicas=2, acks=all` = survive loss of 1 broker without losing committed data
- Can lose up to `RF - min.insync.replicas = 1` broker

**Data loss scenarios:**

| acks | min.insync.replicas | RF | Max broker failures without data loss |
|------|--------------------|----|---------------------------------------|
| 0 | any | any | 0 (fire-and-forget) |
| 1 | any | 3 | 0 (leader failure loses recent records) |
| all | 1 | 3 | 2 |
| all | 2 | 3 | 1 (recommended) |
| all | 3 | 3 | 0 (any broker down = writes fail) |

**Production pitfall**: If `replication.factor=1`, Kafka silently caps `min.insync.replicas` to 1. No warning. No error at topic creation or produce time. You can run for months thinking you have a safety guarantee you don't. Always monitor `UnderMinIsrPartitionCount`.

### 3.4 Rack Awareness

```properties
broker.rack=us-east-1a
```

When enabled, Kafka distributes replicas across different racks/availability zones to survive rack-level failures.

---

## Section 4: Producer Deep Dive

### 4.1 Producer Architecture

```
Application Thread --> RecordAccumulator (batches by partition)
                           |
                    Sender Thread (I/O thread)
                           |
                    Broker (leader partition)
```

**Key configs:**

| Config | Default | Purpose |
|--------|---------|---------|
| `buffer.memory` | 32MB | Total memory for unsent records |
| `batch.size` | 16KB | Max batch size per partition |
| `linger.ms` | 0 | Delay before sending incomplete batch |
| `max.block.ms` | 60000ms | Block time when buffer is full |
| `request.timeout.ms` | 30000ms | Per-request timeout |
| `delivery.timeout.ms` | 120000ms | Total time for send including retries |
| `max.in.flight.requests.per.connection` | 5 | Max unacknowledged requests |
| `compression.type` | none | none, gzip, snappy, lz4, zstd |

**Timeout chain rule:**
`delivery.timeout.ms >= linger.ms + request.timeout.ms`

If violated, you get `ConfigException` on producer startup.

### 4.2 Idempotent Producer

```java
Properties props = new Properties();
props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092,broker2:9092");
props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);  // default since Kafka 3.0
props.put(ProducerConfig.ACKS_CONFIG, "all");                // required for idempotence

KafkaProducer<String, String> producer = new KafkaProducer<>(props);

producer.send(new ProducerRecord<>("orders", "order-123", orderJson), (metadata, ex) -> {
    if (ex != null) {
        log.error("Send failed for order-123", ex);
    } else {
        log.info("Sent to {}-{} @ offset {}",
            metadata.topic(), metadata.partition(), metadata.offset());
    }
});
```

**How it works internally:**
- Broker assigns each producer a **Producer ID (PID)**.
- Producer maintains a **sequence number** per partition.
- Broker deduplicates: if sequence == previous + 1, accept; if duplicate, reject silently.
- Prevents duplicates due to retries, network errors.
- PID changes on producer restart -- idempotence is per-session only. For cross-session, use transactions.

### 4.3 Transactional Producer (Exactly-Once)

```java
Properties props = new Properties();
props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092,broker2:9092");
props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "order-processor-1");
props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

KafkaProducer<String, String> producer = new KafkaProducer<>(props);
producer.initTransactions();

try {
    producer.beginTransaction();

    producer.send(new ProducerRecord<>("orders-processed", key, value));
    producer.send(new ProducerRecord<>("audit-log", key, auditJson));

    // Commit consumer offsets in the same transaction (consume-transform-produce)
    Map<TopicPartition, OffsetAndMetadata> offsets = Map.of(
        new TopicPartition("orders-raw", 0), new OffsetAndMetadata(nextOffset)
    );
    producer.sendOffsetsToTransaction(offsets, consumer.groupMetadata());

    producer.commitTransaction();
} catch (ProducerFencedException | OutOfOrderSequenceException e) {
    // Fatal -- producer is fenced by a newer instance with the same transactional.id
    producer.close();
} catch (KafkaException e) {
    producer.abortTransaction();
}
```

**Why `transactional.id` must be stable**: It persists across producer restarts. The broker tracks which transaction belongs to which `transactional.id`. On restart, the broker fences out the old (zombie) producer with the same `transactional.id` to prevent split-brain writes.

**Two-Phase Commit internally:**
1. Producer registers with Transaction Coordinator.
2. On `commitTransaction()`, coordinator writes PREPARE to the transaction log.
3. Coordinator writes transaction markers to all involved partitions.
4. Coordinator writes COMMITTED to the transaction log.

### 4.4 Production Issue: BufferExhaustedException

**Root cause**: Producer generates records faster than they can be sent to brokers.

**Symptoms**: `BufferExhaustedException` or `TimeoutException` on send after `max.block.ms` expires.

**Three-pronged fix:**

```java
// 1. Increase buffer
props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 64 * 1024 * 1024L); // 64MB

// 2. Improve batching efficiency (more records per request)
props.put(ProducerConfig.BATCH_SIZE_CONFIG, 65536);  // 64KB
props.put(ProducerConfig.LINGER_MS_CONFIG, 10);
props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
```

```java
// 3. Implement application-level backpressure with semaphore
private final Semaphore inflightLimiter = new Semaphore(1000);

public void sendWithBackpressure(String topic, String key, String value)
        throws InterruptedException {
    inflightLimiter.acquire();  // blocks if too many in-flight
    producer.send(new ProducerRecord<>(topic, key, value), (metadata, ex) -> {
        inflightLimiter.release();
        if (ex != null) {
            log.error("Send failed for key={}", key, ex);
            metrics.incrementSendError();
        }
    });
}
```

### 4.5 Production Issue: TimeoutException

**Three types of timeouts:**

| Type | Config | Default | Triggers When |
|------|--------|---------|---------------|
| Metadata fetch | `max.block.ms` | 60000ms | Cannot discover topic/partition |
| Request | `request.timeout.ms` | 30000ms | Broker does not respond in time |
| Delivery | `delivery.timeout.ms` | 120000ms | Total send time including retries exceeded |

**Troubleshooting checklist:**
1. Network connectivity: `kafka-broker-api-versions.sh --bootstrap-server broker:9092`
2. Broker health: Check `MessagesInPerSec` JMX metric
3. Topic exists and has leaders: `kafka-topics.sh --describe --topic X`
4. Review timeout chain: `delivery.timeout.ms >= request.timeout.ms + linger.ms`
5. Check `advertised.listeners` matches what clients can resolve
6. Monitor `record-error-rate` and `record-retry-rate` producer metrics

### 4.6 Production Issue: Producer Retry Storm

**Symptoms**: `record-retry-rate` climbing, latency spikes, increased broker load.

**Critical insight**: Do NOT reduce retry count as the first response. Rising retries are a **delivery-timing symptom**, not a producer tuning problem.

**Diagnosis steps:**
1. Compare retry growth with request latency -- are brokers responding slower?
2. Check broker acknowledgement timing (`request-latency-avg` JMX metric).
3. Review `delivery.timeout.ms` budget -- is it too tight for actual cluster conditions?
4. Confirm whether idempotence or `acks=all` settings are amplifying the effect.
5. Inspect broker load, leader movement, and ISR health.
6. Only after all the above, consider adjusting retry backoff (`retry.backoff.ms`).

### 4.7 Async vs Sync Send

```java
// Sync (blocks until ack -- lowest throughput, safest)
RecordMetadata metadata = producer.send(record).get();

// Async with callback (high throughput, non-blocking)
producer.send(record, (metadata, exception) -> {
    if (exception != null) {
        handleFailure(record, exception);
    }
});

// Fire-and-forget (highest throughput, data loss acceptable)
producer.send(record);
```

**Production rule**: Always use async with callback. Never fire-and-forget for business-critical data. Never use sync in hot paths (kills throughput).

---

## Section 5: Consumer Deep Dive

### 5.1 How to Send the Same Message to Two Consumers (Fan-Out / Broadcast)

**This is one of the most asked logical questions.**

Answer: Use **different consumer group IDs**.

```java
// Service A -- gets ALL messages
Properties propsA = new Properties();
propsA.put(ConsumerConfig.GROUP_ID_CONFIG, "notification-service");
KafkaConsumer<String, String> consumerA = new KafkaConsumer<>(propsA);
consumerA.subscribe(List.of("order-events"));

// Service B -- also gets ALL messages independently
Properties propsB = new Properties();
propsB.put(ConsumerConfig.GROUP_ID_CONFIG, "analytics-service");
KafkaConsumer<String, String> consumerB = new KafkaConsumer<>(propsB);
consumerB.subscribe(List.of("order-events"));
```

**Key insight**: Kafka stores messages once. Each consumer group maintains a separate cursor (offset). Fan-out via consumer groups is essentially free in terms of storage -- no data duplication at the broker level. Within a group, partitions are distributed among members (load balancing / queue semantics). Across groups, every group gets every message (broadcast / pub-sub semantics).

### 5.2 Consumer Group Rebalancing -- The #1 Production Pain Point

**What triggers a rebalance?**
- Consumer joins the group (new deployment, scaling up)
- Consumer leaves the group (shutdown, scaling down)
- Consumer crashes (missed heartbeats past `session.timeout.ms`)
- `max.poll.interval.ms` exceeded (processing too slow between `poll()` calls)
- Topic partition count changes
- Consumer subscribes to a new topic (regex subscription match changes)

**The cascading rebalance nightmare (real production scenario):**
1. Consumer is slow processing a batch (e.g., downstream DB is slow).
2. Exceeds `max.poll.interval.ms` -> broker considers consumer dead -> triggers rebalance.
3. Rebalance revokes all partitions from all consumers (eager protocol) -> all consumers stop.
4. Rebalance completes, consumers resume, but lag has grown.
5. More consumers time out due to increased load -> more rebalances.
6. System enters death spiral. Lag grows exponentially.

**Solution stack (apply in order of priority):**

```java
Properties props = new Properties();

// 1. Use Cooperative Sticky Assignor (only revoke partitions that must move)
props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
    "org.apache.kafka.clients.consumer.CooperativeStickyAssignor");

// 2. Static Group Membership (no rebalance on restart within session timeout)
props.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG,
    "consumer-pod-" + InetAddress.getLocalHost().getHostName());

// 3. Tune timeouts
props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "45000");
props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "15000");
props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "600000");
props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");

// 4. Kafka 4.0+: Use new consumer protocol (KIP-848)
props.put("group.protocol", "consumer");
```

**Understanding the two timeout threads:**

| Config | Thread | Detects |
|--------|--------|---------|
| `session.timeout.ms` | Background heartbeat thread | Process crash, network loss |
| `max.poll.interval.ms` | Main application thread | Application stall, deadlock, slow processing |

`heartbeat.interval.ms` should be 1/3 of `session.timeout.ms`. Heartbeats are sent by a dedicated background thread, independent of `poll()` calls.

### 5.3 Offset Management

```java
// Manual commit (recommended for production)
props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

while (running) {
    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

    for (ConsumerRecord<String, String> record : records) {
        processRecord(record);
    }

    // Commit after successful processing
    consumer.commitSync();
}
```

**Commit strategies:**

| Strategy | Method | Behavior | Use When |
|----------|--------|----------|----------|
| Sync | `commitSync()` | Blocks until committed, retries on failure | Maximum safety |
| Async | `commitAsync(callback)` | Non-blocking, no automatic retry | High throughput |
| Per-partition | `commitSync(Map<TP, Offset>)` | Commit specific partitions | Fine-grained control |
| Transactional | `sendOffsetsToTransaction()` | Atomic with produce | Exactly-once |

**What happens when offsets are lost?**

| `auto.offset.reset` | Behavior | Risk |
|---------------------|----------|------|
| `earliest` | Reprocess from beginning | Duplicate processing |
| `latest` | Skip to newest | Data loss |
| `none` | Throw exception | Application must handle explicitly |

**Production pitfall**: `offsets.retention.minutes` (default 7 days). If a consumer group is idle longer than this, committed offsets are deleted. On restart, `auto.offset.reset` kicks in. Monitor consumer group idle time.

### 5.4 Backpressure Handling -- Pause/Resume Pattern

```java
private final Set<TopicPartition> pausedPartitions = ConcurrentHashMap.newKeySet();

while (running) {
    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

    for (TopicPartition partition : records.partitions()) {
        List<ConsumerRecord<String, String>> partitionRecords =
            records.records(partition);

        if (isOverloaded(partition)) {
            consumer.pause(Set.of(partition));
            pausedPartitions.add(partition);
            scheduler.schedule(() -> {
                consumer.resume(Set.of(partition));
                pausedPartitions.remove(partition);
            }, 30, TimeUnit.SECONDS);
        } else {
            for (ConsumerRecord<String, String> record : partitionRecords) {
                processRecord(record);
            }
            consumer.commitSync(Map.of(
                partition,
                new OffsetAndMetadata(
                    partitionRecords.get(partitionRecords.size() - 1).offset() + 1
                )
            ));
        }
    }
}
```

### 5.5 Graceful Shutdown

```java
Runtime.getRuntime().addShutdownHook(new Thread(() -> {
    running = false;
    consumer.wakeup();  // causes poll() to throw WakeupException
}));

try {
    while (running) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
        // process records...
        consumer.commitSync();
    }
} catch (WakeupException e) {
    // expected on shutdown
} finally {
    consumer.commitSync();  // commit any remaining offsets
    consumer.close();       // triggers LeaveGroup -> clean rebalance
}
```

Calling `consumer.close()` sends a LeaveGroup request, which triggers a clean rebalance instead of waiting for session timeout.

---

## Section 6: Partition Strategy and Data Skew

### 6.1 How Partitioning Works

| Key State | Partitioner Behavior |
|-----------|---------------------|
| Key present | `murmur2(key) % numPartitions` -> deterministic partition |
| Key absent (Kafka 2.4+) | Sticky partitioner -- fills one batch then switches (better batching) |
| Key absent (pre-2.4) | Round-robin (poor batching) |
| Custom | Your `Partitioner` implementation |

### 6.2 Partition Count Selection

**Formula**: `max(target_throughput_MB / per_consumer_throughput_MB, expected_consumer_count)`

**Rules:**
- Over-provision for keyed topics -- you CANNOT safely add partitions later.
- Make partition count a multiple of broker count for even distribution.
- Slack's rule: anything under 100 partitions per topic starts to produce uneven load distribution on large clusters.
- Maximum practical limit: ~4,000 partitions per broker in KRaft mode.

### 6.3 Hot Partition Detection and Fix

**Detection:**
```bash
kafka-consumer-groups.sh --bootstrap-server broker:9092 \
  --describe --group my-group
# Look for one partition with 5-10x the lag of others
```

**Alert threshold**: If max-to-average per-partition byte rate ratio exceeds 1.5, investigate. Above 2.0 is urgent.

**Fixes:**

```java
// BAD: Low-cardinality key causes traffic concentration
String key = order.getStatus();  // "PENDING", "COMPLETED" -> 2-3 partitions get all traffic

// GOOD: Entity ID as key (high cardinality, even distribution)
String key = order.getOrderId();

// GOOD: Composite key for large tenants (preserves per-entity ordering)
String key = order.getTenantId() + "|" + order.getOrderId();

// GOOD: Salted key for known hot keys (sacrifices per-key ordering)
int salt = ThreadLocalRandom.current().nextInt(10);
String key = order.getCustomerId() + "#" + salt;

// GOOD: Time-bucketed key (for bursty hotness)
String key = order.getUserId() + "|" + Instant.now().truncatedTo(ChronoUnit.HOURS);
```

**Critical rule**: NEVER add partitions to an existing keyed topic to fix skew. Adding partitions breaks `murmur2(key) % numPartitions` mapping permanently. Old data stays where it was; new data with the same key may land on a different partition. Per-key ordering guarantees are destroyed. Create a new topic with the correct partition count and migrate data.

### 6.4 Custom Partitioner

```java
public class TenantAwarePartitioner implements Partitioner {
    private static final Set<String> HOT_TENANTS =
        Set.of("enterprise-corp", "mega-retail");

    @Override
    public int partition(String topic, Object key, byte[] keyBytes,
                         Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();
        int reservedForHot = 5;

        String tenantId = extractTenantId((String) key);
        if (HOT_TENANTS.contains(tenantId)) {
            int salt = Math.abs(key.hashCode()) % reservedForHot;
            return (numPartitions - reservedForHot) + salt;
        }
        return Math.abs(Utils.murmur2(keyBytes)) % (numPartitions - reservedForHot);
    }

    @Override
    public void close() {}

    @Override
    public void configure(Map<String, ?> configs) {}

    private String extractTenantId(String compositeKey) {
        return compositeKey.split("\\|")[0];
    }
}
```

Register it:
```java
props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, TenantAwarePartitioner.class.getName());
```

---

## Section 7: Delivery Guarantees

### 7.1 Three Delivery Semantics

| Semantic | How Achieved | Trade-off |
|----------|-------------|-----------|
| At-most-once | `acks=0` or commit offsets before processing | Data loss possible |
| At-least-once | `acks=all` + commit offsets after processing | Duplicates possible |
| Exactly-once | Idempotent producer + transactions + `read_committed` | Higher latency, more complexity |

### 7.2 Exactly-Once: The Complete Picture

Three pillars working together:

1. **Idempotent Producer**: PID + sequence number prevents duplicates within a single partition. Per-session only.
2. **Transactions**: Atomic writes across multiple partitions + consumer offset commits. Cross-session via `transactional.id`.
3. **Consumer `isolation.level=read_committed`**: Consumers only see committed transactions, skip aborted ones.

```java
// Consumer side for exactly-once
props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
// With read_committed:
//   - Consumer will NOT see messages from aborted transactions
//   - Consumer will only see committed messages in offset order
//   - LastStableOffset (LSO) tracks the earliest open transaction
//   - Messages after LSO are buffered until transaction resolves
```

**Warning**: `read_committed` adds latency because the consumer must wait for transaction resolution. Long-running transactions will hold up the LSO and increase end-to-end latency for all consumers on those partitions.

### 7.3 Kafka Streams Exactly-Once

```java
Properties streamsProps = new Properties();
streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "order-enrichment");
streamsProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092");
streamsProps.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG,
    StreamsConfig.EXACTLY_ONCE_V2);  // use v2 for Kafka 2.6+
// v2 uses a single transactional producer per Streams instance
// (v1 used one per task -- much higher overhead)
```

### 7.4 Making Consumer Processing Idempotent

Even with exactly-once producer semantics, consumer-side idempotency is crucial because:
- Application crashes after processing but before offset commit
- Rebalance during processing causes re-delivery
- Manual offset reset for replay

**Strategies:**
```java
// 1. Database upsert (INSERT ON CONFLICT)
jdbcTemplate.update(
    "INSERT INTO orders (order_id, status, amount) VALUES (?, ?, ?) " +
    "ON CONFLICT (order_id) DO UPDATE SET status = ?, amount = ?",
    orderId, status, amount, status, amount
);

// 2. Processed-message tracking table
boolean alreadyProcessed = jdbcTemplate.queryForObject(
    "SELECT EXISTS(SELECT 1 FROM processed_events WHERE event_id = ?)",
    Boolean.class, eventId
);
if (!alreadyProcessed) {
    processEvent(event);
    jdbcTemplate.update(
        "INSERT INTO processed_events (event_id, processed_at) VALUES (?, NOW())",
        eventId
    );
}

// 3. Redis dedup with TTL
Boolean isNew = redisTemplate.opsForValue()
    .setIfAbsent("processed:" + eventId, "1", Duration.ofHours(24));
if (Boolean.TRUE.equals(isNew)) {
    processEvent(event);
}
```

---

## Section 8: Error Handling and DLQ Patterns (Spring Boot)

### 8.1 The Poison Pill Problem

A single corrupt/invalid message blocks the entire partition forever. Default Spring Kafka behavior: retry infinitely at the same offset. The consumer never moves forward.

**What qualifies as a poison pill:**
- Malformed JSON/Avro that cannot be deserialized
- Message from a producer with an incompatible schema version
- Corrupted bytes on the wire
- A valid message that triggers a non-transient application bug

### 8.2 Production-Grade Error Handler (Spring Kafka)

```java
@Configuration
public class KafkaErrorConfig {

    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<String, Object> kafkaTemplate) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
            kafkaTemplate,
            (record, ex) -> new TopicPartition(
                record.topic() + ".DLT",
                record.partition()
            )
        );

        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(3);
        backOff.setInitialInterval(1_000);
        backOff.setMultiplier(2.0);
        backOff.setMaxInterval(10_000);

        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, backOff);

        handler.addNotRetryableExceptions(
            DeserializationException.class,
            JsonParseException.class,
            ClassCastException.class,
            NullPointerException.class
        );

        return handler;
    }
}
```

### 8.3 Preventing Poison Pills at the Deserializer Level

```yaml
spring:
  kafka:
    consumer:
      value-deserializer: org.springframework.kafka.support.serializer.ErrorHandlingDeserializer
      properties:
        spring.deserializer.value.delegate.class: org.apache.kafka.common.serialization.StringDeserializer
```

This wraps deserialization errors so they reach the error handler instead of crashing the consumer loop. Without this, a single corrupt message causes an infinite crash loop.

### 8.4 Non-Blocking Retry with @RetryableTopic

```java
@RetryableTopic(
    attempts = "4",
    backoff = @Backoff(delay = 1000, multiplier = 2.0, maxDelay = 10000),
    dltStrategy = DltStrategy.FAIL_ON_ERROR,
    topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
    autoCreateTopics = "true"
)
@KafkaListener(topics = "orders", groupId = "order-processor")
public void processOrder(OrderEvent event) {
    orderService.process(event);
}

@DltHandler
public void handleDlt(
        OrderEvent event,
        @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
        @Header(KafkaHeaders.EXCEPTION_MESSAGE) String errorMsg) {
    log.error("DLT received. Topic: {}, Error: {}, Event: {}", topic, errorMsg, event);
    alertService.sendAlert("Order failed permanently: " + event.getOrderId());
}
```

This auto-creates retry chain: `orders` -> `orders-retry-0` -> `orders-retry-1` -> `orders-retry-2` -> `orders.DLT`

Main consumer is never blocked -- failed messages move to retry topics with delays.

### 8.5 DLT Best Practices

| Practice | Why |
|----------|-----|
| DLT retention: 7-30 days | Longer than source topic for investigation window |
| DLT partition count = source topic | Preserve partition affinity for debugging |
| Monitor DLT depth | Growing DLT = systematic problem, not transient |
| Rich metadata in DLT headers | Include original topic, partition, offset, exception, timestamp |
| Build DLT replay mechanism | Allow fix-and-reprocess workflows |
| Alert on DLT ingestion rate | Detect issues early, before DLT fills up |
| Separate DLT consumer for analysis | Automated triage and alerting |

### 8.6 Error Classification Strategy

| Exception Type | Retryable? | Action |
|---------------|------------|--------|
| `DeserializationException` | No | DLT immediately |
| `JsonParseException` | No | DLT immediately |
| `ConnectException` (downstream DB/API) | Yes | Retry with backoff |
| `TimeoutException` (downstream) | Yes | Retry with backoff |
| `ConstraintViolationException` | No | DLT immediately |
| `OutOfMemoryError` | No | Crash and restart |
| Custom `BusinessValidationException` | No | DLT immediately |

---

## Section 9: Message Ordering

### 9.1 Core Rule

Kafka guarantees ordering **within a single partition only**. There is NO cross-partition ordering guarantee.

### 9.2 Ordering Strategies

| Strategy | Ordering Guarantee | Throughput | When to Use |
|----------|-------------------|------------|-------------|
| Single partition topic | Global strict ordering | Very low (no parallelism) | Event sequencing for small volumes |
| Entity-key partitioning | Per-entity ordering | High | Most use cases (orders per customer) |
| Consumer-side reordering | Pseudo-global (with buffer) | Medium | When you need cross-entity ordering |

### 9.3 Ordering with Retries

**Problem**: With `max.in.flight.requests.per.connection > 1` and retries enabled, messages can be reordered. Batch 1 fails, batch 2 succeeds, batch 1 retries and succeeds -> out of order.

**Fix**: Enable idempotent producer (allows up to 5 in-flight safely):
```java
props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
// Idempotent producer uses sequence numbers per partition.
// Broker rejects out-of-order batches, forcing retry in correct order.
// Safe with max.in.flight.requests.per.connection <= 5 (Java client limit).
```

### 9.4 Multiple Producers and Ordering

Kafka does NOT guarantee ordering across different producer instances for the same partition. If Producer A and Producer B both write to partition 0, the order depends on network latency and which request arrives at the broker first.

**Solution**: Use a single producer instance per ordering domain, or embed a logical sequence number in the message payload and reorder on the consumer side using event timestamps.

---

## Section 10: Schema Registry and Schema Evolution

### 10.1 Why Schema Registry?

Without it: Producer changes JSON field name from `userId` to `user_id` -> all consumers silently get `null` for `userId`. At scale, this is a silent data corruption disaster.

Schema Registry provides:
- Central schema storage with versioning
- Compatibility checking before schema registration
- Schema ID embedded in each message (4 bytes) for efficient serialization
- Language-agnostic contract between producers and consumers

### 10.2 Compatibility Modes

| Mode | Rule | Upgrade Order | Example Change |
|------|------|---------------|----------------|
| BACKWARD (default) | New schema can read old data | Consumers first | Add optional field with default |
| FORWARD | Old schema can read new data | Producers first | Remove optional field |
| FULL | Both backward and forward | Any order | Add/remove fields with defaults |
| TRANSITIVE variants | Checks against ALL previous versions | Same as base | Strictest enforcement |
| NONE | No checking | Any order | Dev only -- never in production |

### 10.3 Avro Safe Changes

| Change | BACKWARD | FORWARD | FULL |
|--------|----------|---------|------|
| Add field WITH default | Yes | Yes | Yes |
| Add field WITHOUT default | No | Yes | No |
| Remove field WITH default | Yes | Yes | Yes |
| Remove field WITHOUT default | Yes | No | No |
| Rename field (with alias) | Yes | Yes | Yes |
| Change field type (promotion) | Depends | Depends | Depends |

### 10.4 Production Rules

**Never set `auto.register.schemas=true` in production.** Schemas should be registered via CI/CD pipeline:

```xml
<plugin>
    <groupId>io.confluent</groupId>
    <artifactId>kafka-schema-registry-maven-plugin</artifactId>
    <configuration>
        <schemaRegistryUrls>
            <param>http://schema-registry:8081</param>
        </schemaRegistryUrls>
        <subjects>
            <order-events-value>src/main/avro/OrderEvent.avsc</order-events-value>
        </subjects>
    </configuration>
</plugin>
```

Run `mvn schema-registry:test-compatibility` in CI to block breaking changes before deployment.

**Additional rules:**
- Use subject naming strategy: `TopicNameStrategy` (default), `RecordNameStrategy`, or `TopicRecordNameStrategy`
- Set `mode=READONLY` on production Schema Registry subjects to prevent accidental writes
- Always include a default value for new fields in Avro
- Consider Protobuf for complex schemas -- better evolution story than JSON Schema

---

## Section 11: Kafka 4.0 -- Major Changes

### 11.1 KRaft Mode (ZooKeeper Removed)

- ZooKeeper support **completely removed** in Kafka 4.0.
- KRaft = Kafka Raft metadata mode. Metadata managed by a quorum of controllers using the Raft consensus algorithm.
- Supports ~1.9 million partitions (vs ~200K with ZooKeeper).
- Simpler deployment: no separate ZooKeeper ensemble to manage.
- **Cannot upgrade directly from ZooKeeper mode to 4.0.**

**Migration path:**
1. Upgrade existing cluster to **Kafka 3.9** (the bridge release).
2. Deploy KRaft controller quorum with `zookeeper.metadata.migration.enable=true`.
3. Rolling restart brokers with migration flag -- metadata is dual-written to both ZK and KRaft.
4. Remove ZooKeeper configs, restart brokers in pure KRaft mode.
5. Finalize metadata version: `kafka-features.sh upgrade --release-version 4.0`
6. Rolling upgrade to Kafka 4.0.
7. Decommission ZooKeeper ensemble.

### 11.2 KIP-848: New Consumer Rebalance Protocol

- **Broker-driven**, fully incremental (no stop-the-world rebalances).
- No elected group leader, no JoinGroup/SyncGroup round-trips.
- Each consumer converges independently via continuous heartbeat mechanism.
- Broker uses **Uniform Assignor** by default (distributes partitions evenly, minimizes movement).
- Enable: `group.protocol=consumer`
- Can upgrade consumer groups without downtime via online protocol migration.

### 11.3 KIP-932: Share Groups (Queues for Kafka)

Traditional queue semantics on standard Kafka topics. This is a game-changer.

| Feature | Consumer Groups | Share Groups |
|---------|----------------|--------------|
| Partition-to-consumer mapping | 1:1 (one partition per consumer max) | Many:many |
| Scaling | Limited by partition count | Elastic, beyond partitions |
| Acknowledgment | Offset-based (batch) | Per-message (ACCEPT/RELEASE/REJECT) |
| Ordering | Per-partition guaranteed | Partial (within batch only) |
| Delivery | At-least-once / exactly-once | At-least-once |

```java
Properties props = new Properties();
props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "broker:9092");
props.put(ConsumerConfig.GROUP_ID_CONFIG, "task-workers");
props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

KafkaShareConsumer<String, String> consumer = new KafkaShareConsumer<>(props);
consumer.subscribe(List.of("task-queue"));

while (true) {
    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
    for (ConsumerRecord<String, String> record : records) {
        try {
            processTask(record);
            consumer.acknowledge(record, AcknowledgeType.ACCEPT);
        } catch (TransientException e) {
            consumer.acknowledge(record, AcknowledgeType.RELEASE); // make available for retry
        } catch (PermanentException e) {
            consumer.acknowledge(record, AcknowledgeType.REJECT);  // dead letter
        }
    }
}
```

- Early Access in Kafka 4.0, GA in Kafka 4.2.
- Records have a time-limited acquisition lock (default 30s via `share.record.lock.duration.ms`).
- No maximum queue depth -- but there is a limit to in-flight records (`group.share.partition.max.record.locks`).

### 11.4 Java Version Requirements

| Component | Minimum Java Version |
|-----------|---------------------|
| Clients + Streams | Java 11 |
| Brokers + Connect + Tools | Java 17 |

### 11.5 Other Kafka 4.0 Changes

- KIP-896: Old client protocol API versions removed. Clients must be version 2.1+.
- `KStream#transformValues()` removed -- use `KStream#processValues()` instead.
- Config files moved from `config/kraft/` to `config/` (ZK-specific configs removed).

---

## Section 12: Kafka Streams

### 12.1 Core Abstractions

| Abstraction | Description |
|-------------|-------------|
| `KStream` | Unbounded stream of records (insert semantics) |
| `KTable` | Changelog stream (upsert semantics per key) |
| `GlobalKTable` | Fully replicated KTable on every instance |
| `StateStore` | Local key-value store backed by RocksDB |
| `Topology` | DAG of stream processing nodes |

### 12.2 KTable vs GlobalKTable

| Feature | KTable | GlobalKTable |
|---------|--------|-------------|
| Data distribution | Partitioned (each instance holds its assigned partitions) | Fully replicated to every instance |
| Requires co-partitioning | Yes | No |
| Use case | Large datasets, stream-table joins | Small reference data (config, categories) |
| Memory impact | Proportional to owned partitions | Full dataset on every instance |
| Event-time sync | Yes | No |

**Rule of thumb**: 10K-row category table -> GlobalKTable. 100M-row user table -> KTable with co-partitioning.

### 12.3 When to Use Kafka Streams vs Flink

| Criteria | Kafka Streams | Apache Flink |
|----------|--------------|-------------|
| Deployment | Library (embedded in your app) | Separate cluster |
| Sources/Sinks | Kafka only | Kafka, JDBC, S3, Kinesis, etc. |
| State size | Up to ~100GB per instance | Terabytes+ (distributed RocksDB) |
| Exactly-once | End-to-end with Kafka | End-to-end with checkpoints |
| CEP | No | Yes (Flink CEP library) |
| SQL | No native SQL (ksqlDB is separate) | Flink SQL |
| Team profile | Java-first, microservice-oriented | Data engineering, SQL-oriented |
| Operational overhead | Zero (library) | High (cluster management) |
| Testing | `TopologyTestDriver` for unit tests | Integration test framework |

### 12.4 Kafka Streams Windowing

| Window Type | Description | Use Case |
|-------------|-------------|----------|
| Tumbling | Fixed-size, non-overlapping | Hourly aggregations |
| Hopping | Fixed-size, overlapping | Moving averages |
| Sliding | Fixed-size, triggered by events | "Last 5 minutes" |
| Session | Dynamic, gap-based | User activity sessions |

---

## Section 13: Kafka Connect

### 13.1 Architecture

- **Workers**: JVM processes that run connectors. Can be standalone or distributed.
- **Connectors**: Define what data to move (configuration).
- **Tasks**: Actual data movers (parallelism unit).
- **Converters**: Serialize/deserialize between Connect data format and wire format.
- **Transforms (SMTs)**: Single Message Transforms applied inline.

### 13.2 Production Error Handling

```json
{
  "name": "jdbc-sink-orders",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
    "tasks.max": "3",
    "topics": "orders",
    "errors.tolerance": "all",
    "errors.deadletterqueue.topic.name": "orders-connect-dlq",
    "errors.deadletterqueue.context.headers.enable": true,
    "errors.log.enable": true,
    "errors.log.include.messages": true,
    "errors.retry.timeout": "60000",
    "errors.retry.delay.max.ms": "5000"
  }
}
```

| Config | Purpose |
|--------|---------|
| `errors.tolerance=all` | Keep running on errors (don't stop the task) |
| `errors.deadletterqueue.topic.name` | DLQ topic for failed records |
| `errors.deadletterqueue.context.headers.enable` | Include stack trace and metadata in DLQ headers |
| `errors.retry.timeout` | Total time to retry transient errors |
| `errors.retry.delay.max.ms` | Max delay between retries |

### 13.3 Troubleshooting Steps

1. **Check connector status**: `GET /connectors/{name}/status`
2. **Check failed tasks**: `GET /connectors/{name}/tasks/{id}/status` -- includes stack trace.
3. **Read worker logs** for stack traces and error context.
4. **Dynamic log level change** via REST API (no restart needed):
   ```bash
   PUT /admin/loggers/io.debezium -H "Content-Type:application/json" \
     -d '{"level": "TRACE"}'
   ```
5. **Inspect DLQ headers** for failure context (original topic, partition, exception).
6. **Check external system logs** (database connection errors, API failures).
7. **Verify storage topics** (`config.storage.topic`, `offset.storage.topic`, `status.storage.topic`) are unique per Connect cluster.

### 13.4 Common Connector Issues

| Issue | Symptom | Fix |
|-------|---------|-----|
| Missing JDBC driver | `ClassNotFoundException` in stack trace | Add driver JAR to `plugin.path` |
| Schema mismatch | Continuous task restarts | Check converter config, use `ErrorHandlingDeserializer` |
| Connector lag | Growing offset lag on Connect consumer group | Increase `tasks.max`, tune `batch.size` |
| Task OOM | Task dies, restarts | Increase worker heap, reduce `max.poll.records` |

---

## Section 14: Security

### 14.1 Three Pillars

| Component | Protocol | Purpose |
|-----------|----------|---------|
| Encryption | SSL/TLS | Protect data in transit |
| Authentication | SASL (SCRAM-SHA-512, OAUTHBEARER, GSSAPI/Kerberos, PLAIN) | Verify client identity |
| Authorization | ACLs (StandardAuthorizer for KRaft) | Control who can do what |

### 14.2 SASL Mechanism Comparison

| Mechanism | Credential Storage | Production Ready | Notes |
|-----------|--------------------|-----------------|-------|
| PLAIN | Broker config file | No (unless with TLS) | Passwords in plain text on disk |
| SCRAM-SHA-512 | Kafka metadata | Yes | Recommended. Salted + iterated hash |
| GSSAPI (Kerberos) | KDC | Yes | Enterprise standard, complex setup |
| OAUTHBEARER | OAuth2 provider | Yes | Modern, token-based, supports rotation |

### 14.3 Java Client Security Config

```java
Properties props = new Properties();
props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
props.put("sasl.mechanism", "SCRAM-SHA-512");
props.put("sasl.jaas.config",
    "org.apache.kafka.common.security.scram.ScramLoginModule required " +
    "username=\"order-service\" password=\"${KAFKA_PASSWORD}\";");
props.put("ssl.truststore.location", "/etc/kafka/truststore.jks");
props.put("ssl.truststore.password", "${TRUSTSTORE_PASSWORD}");
```

### 14.4 ACL Management

```bash
# Grant producer permission
kafka-acls.sh --bootstrap-server broker:9092 --add \
  --allow-principal User:order-service \
  --operation Write --operation Describe \
  --topic orders

# Grant consumer permission
kafka-acls.sh --bootstrap-server broker:9092 --add \
  --allow-principal User:analytics-service \
  --operation Read --operation Describe \
  --topic orders --group analytics-group

# List all ACLs
kafka-acls.sh --bootstrap-server broker:9092 --list
```

### 14.5 ACL Best Practices

- Default: `allow.everyone.if.no.acl.found=false` (deny by default).
- Grant minimum required permissions per service (principle of least privilege).
- Use `super.users` sparingly (admin operations only).
- Prefer SCRAM-SHA-512 or OAUTHBEARER over SASL/PLAIN in production.
- Always combine SASL with TLS to encrypt credentials in transit.
- Rotate credentials regularly. SCRAM supports credential updates without broker restart.
- For KRaft: use `StandardAuthorizer` (replaces `AclAuthorizer` used with ZooKeeper).

---

## Section 15: Monitoring and Observability

### 15.1 Critical Metrics to Monitor

| Metric | What It Means | Alert Threshold |
|--------|--------------|-----------------|
| `UnderReplicatedPartitions` | Replicas not caught up with leader | > 0 |
| `UnderMinIsrPartitionCount` | Writes will fail for `acks=all` | > 0 |
| `ActiveControllerCount` | Must be exactly 1 in the cluster | != 1 |
| `UncleanLeaderElectionsPerSec` | DATA LOSS has occurred | > 0 |
| `IsrShrinksPerSec` | Replicas falling behind leader | Sustained > 0 |
| `IsrExpandsPerSec` | Replicas catching up | Should follow shrinks |
| `records-lag-max` | Consumer lag (worst partition) | Growing trend |
| `request-latency-avg` | Broker response time | > 100ms |
| `BytesInPerSec` / `BytesOutPerSec` | Broker throughput | Approaching NIC limit |
| `RequestQueueSize` | Pending requests on broker | > 0 sustained |
| `LogFlushRateAndTimeMs` | Disk flush performance | p99 > 100ms |
| `NetworkProcessorAvgIdlePercent` | Network thread utilization | < 30% |

### 15.2 Prometheus + Grafana Setup

**Tools:**
- **kafka-exporter** (by danielqsj): Exposes consumer group lag via Admin API on port 9308. No JMX required.
- **JMX Exporter**: Exposes broker JMX metrics to Prometheus.
- **Grafana Dashboard**: Import ID 7589 ("Kafka Exporter Overview").

**Essential PromQL queries:**
```promql
# Total lag per consumer group
sum(kafka_consumergroup_lag) by (consumergroup, topic)

# Lag growth rate (positive = falling behind)
deriv(sum(kafka_consumergroup_lag) by (consumergroup)[10m])

# Consumer throughput (messages processed per second)
rate(kafka_consumergroup_current_offset[2m])

# Hotspot detection (top 5 lagging partitions)
topk(5, kafka_consumergroup_lag)

# Time-to-drain estimate (minutes to clear backlog at current rate)
sum(kafka_consumergroup_lag) by (consumergroup)
  / (rate(kafka_consumergroup_current_offset[5m]) * 60)
```

### 15.3 Debugging High Consumer Lag -- Step by Step

1. `kafka-consumer-groups.sh --describe --group X` for per-partition lag.
2. Check `deriv(lag)` in Prometheus: Is it one hot partition or all partitions?
3. If one partition: likely key skew. Check partition key distribution.
4. If all partitions: consumer is under-provisioned or processing is too slow.
5. Check consumer CPU, memory, GC pauses.
6. Check downstream dependency latency (DB response times, API latency).
7. If producer traffic spike: scale consumers (up to partition count).
8. If rebalance: check consumer count drop, tune timeouts (Section 5.2).
9. If poison pill: check for `SerializationException` in consumer logs.
10. If all else fails: profile the consumer processing logic.

---

## Section 16: Production Issue Runbook

### Issue 1: Consumer Lag Growing Continuously

**Diagnosis**: `deriv(kafka_consumergroup_lag[10m]) > 0` sustained for > 15 minutes.

**Steps:**
1. Per-partition lag analysis -- is it one hot partition or all?
2. Check consumer processing time vs `max.poll.interval.ms`.
3. Profile downstream dependencies (DB queries, HTTP calls).
4. Scale consumers (but only up to partition count -- more consumers than partitions = idle threads).
5. Reduce `max.poll.records` to process smaller batches.
6. Consider batch processing + async worker threads.
7. If partition skew: redesign partition key (Section 6).
8. If still stuck: increase partition count on a NEW topic and migrate.

### Issue 2: NotEnoughReplicasException

**Cause**: ISR count < `min.insync.replicas`. Producers with `acks=all` cannot write.

**Steps:**
1. Check broker health: `kafka-metadata.sh --snapshot` or broker logs.
2. Identify which brokers are missing from ISR.
3. Check disk space (`df -h` on broker machines).
4. Check network connectivity between brokers.
5. Check GC pauses (`-XX:+PrintGCDetails` in broker JVM flags).
6. Restart unhealthy brokers.
7. Monitor `IsrExpandsPerSec` to confirm recovery.
8. Temporary workaround: reduce `min.insync.replicas` to 1 (trades durability for availability). Restore to 2 after recovery.

### Issue 3: Frequent Rebalances

**Symptoms**: Rebalance counter climbing, lag growing, `CommitFailedError` in logs.

**Steps:**
1. Switch to `CooperativeStickyAssignor`.
2. Enable static membership (`group.instance.id` = stable identifier per pod/instance).
3. Increase `max.poll.interval.ms` beyond worst-case processing time.
4. Reduce `max.poll.records` to ensure batches complete within poll interval.
5. Implement graceful shutdown (`consumer.wakeup()` + `close()` in shutdown hook).
6. On Kafka 4.0+: upgrade to KIP-848 consumer protocol (`group.protocol=consumer`).
7. Check for slow downstream dependencies causing processing stalls.

### Issue 4: Message Duplication

**Causes**: Retries without idempotence, rebalance during processing, at-least-once delivery.

**Steps:**
1. Enable idempotent producer (`enable.idempotence=true`).
2. Use transactions for consume-transform-produce patterns.
3. Make consumer processing idempotent (database upserts with natural keys, dedup via Redis/DB).
4. Commit offsets AFTER processing, not before.
5. Use `isolation.level=read_committed` on consumers consuming from transactional producers.

### Issue 5: Out-of-Memory on Broker

**Causes**: Retention too high, JVM heap misconfigured, too many partitions, too many open file handles.

**Steps:**
1. Check retention policy per topic: `kafka-configs.sh --describe --entity-type topics`.
2. Review JVM heap: recommended 6GB for broker, max 8GB. Kafka is designed for OS page cache, not JVM heap.
3. Count partitions per broker (recommended max ~4000 per broker in KRaft).
4. Check open file descriptors (`ulimit -n` should be >= 100,000).
5. Check log cleaner thread health (`LogCleanerTimeMs`, `NumLogSegmentsBeingCleaned`).

### Issue 6: Schema Deserialization Errors

**Cause**: Schema mismatch between producer and consumer (schema drift).

**Steps:**
1. Use Schema Registry with compatibility enforcement.
2. Wrap deserializer with `ErrorHandlingDeserializer`.
3. Failed messages automatically routed to DLT.
4. Validate compatibility in CI before deploy (`mvn schema-registry:test-compatibility`).
5. Never use `auto.register.schemas=true` in production.
6. Check Schema Registry for unexpected schema versions.

### Issue 7: Producer Retry Storm

**Symptoms**: `record-retry-rate` climbing, latency spikes, broker CPU increase.

**Steps:**
1. Compare retry growth with request latency (`request-latency-avg`).
2. Check broker acknowledgement timing -- are acks getting slower?
3. Review `delivery.timeout.ms` budget -- is it sufficient for current cluster conditions?
4. Confirm whether `acks=all` with ISR shrink is forcing retries.
5. Check broker load, leader movement, partition reassignment.
6. Only AFTER confirming root cause, consider adjusting `retry.backoff.ms`.

### Issue 8: Disk Full on Broker

**Steps:**
1. Identify large topics: `du -sh /kafka-logs/*/`.
2. Check retention settings per topic.
3. Reduce retention temporarily for non-critical topics.
4. Delete unused topics.
5. Check log compaction health -- stuck compaction causes segment accumulation.
6. Add disk or move to tiered storage.

### Issue 9: SSL Handshake Failures

**Steps:**
1. Verify certificate validity: `openssl s_client -connect broker:9093`.
2. Check truststore/keystore paths and passwords.
3. Ensure certificate SAN matches the broker hostname clients use.
4. Check certificate expiry dates.
5. Verify `advertised.listeners` uses the correct hostname (not IP if certs use DNS).

---

## Section 17: Design Patterns

### 17.1 Event Sourcing with Kafka

- Kafka as the event store (append-only log is a natural fit).
- Compacted topics for entity snapshots (latest state per entity).
- Rebuild state by replaying events from the beginning (`auto.offset.reset=earliest`).
- Use Schema Registry to evolve event schemas safely.
- Caveat: Kafka is not a database. For complex queries on events, project to a read store.

### 17.2 Transactional Outbox Pattern

```
Service --> [DB Transaction: write business data + outbox row]
                                    |
                              Debezium CDC Connector
                                    |
                              Kafka Topic
```

- Write event to outbox table in the same DB transaction as business data.
- Debezium CDC connector reads the outbox table's WAL and publishes to Kafka.
- Decouples database writes from Kafka delivery.
- **Guarantee**: If the business transaction commits, the event will eventually reach Kafka.
- Eliminates the dual-write problem (write to DB + send to Kafka is not atomic without this pattern).

### 17.3 CQRS with Kafka

- **Commands** flow through Kafka topics to write services.
- **Events** are published on successful command processing.
- Events are projected to **read-optimized stores** (Elasticsearch, Redis, materialized views).
- Separate consumer groups per read model -- each can be rebuilt independently.
- Kafka acts as the durable event backbone connecting command and query sides.

### 17.4 Saga Pattern with Kafka (Choreography)

```
Order Service --[order.created]--> Payment Service --[payment.completed]--> Shipping Service
                                          |
                                   [payment.failed]
                                          |
                                   Order Service (compensate: cancel order)
```

- Each service publishes domain events, other services react.
- Compensation events handle rollback (e.g., `payment.failed` -> cancel order).
- Use DLT for failed compensation actions.
- **Idempotent consumers are mandatory** -- events may be replayed on rebalance.
- Consider adding a correlation ID to track the saga across services.

### 17.5 Change Data Capture (CDC)

- Debezium captures row-level changes from database WAL (MySQL binlog, PostgreSQL WAL).
- Publishes changes as Kafka events with before/after state.
- Enables real-time data sync between microservices without coupling.
- Combined with Outbox pattern for reliable event publishing.

### 17.6 Event-Carried State Transfer

- Instead of services calling each other's APIs, services publish their state changes as events.
- Consuming services build local read replicas of the data they need.
- Eliminates synchronous API dependencies.
- Trade-off: eventual consistency, more storage.

---

## Section 18: Performance Tuning Cheat Sheet

### Producer Tuning

```properties
# HIGH THROUGHPUT profile
batch.size=65536            # 64KB batches
linger.ms=10                # Wait 10ms for batch to fill
compression.type=lz4        # Best compression/speed ratio
buffer.memory=67108864      # 64MB buffer
max.in.flight.requests.per.connection=5

# LOW LATENCY profile
batch.size=1
linger.ms=0
compression.type=none
```

### Consumer Tuning

```properties
# HIGH THROUGHPUT profile
fetch.min.bytes=1048576     # 1MB -- broker waits to accumulate before responding
fetch.max.wait.ms=500       # Max wait for fetch.min.bytes
max.poll.records=500        # Process 500 records per poll

# LOW LATENCY profile
fetch.min.bytes=1           # Respond immediately with any data
fetch.max.wait.ms=0         # No wait
max.poll.records=1          # Process one record at a time
```

### Broker Tuning

```properties
num.network.threads=8           # Network I/O threads (default 3)
num.io.threads=16               # Disk I/O threads (default 8)
socket.send.buffer.bytes=1048576
socket.receive.buffer.bytes=1048576
log.flush.interval.messages=10000
num.replica.fetchers=4          # Replication threads (default 1)
```

### Compression Comparison

| Codec | Compression Ratio | Speed | CPU Usage | When to Use |
|-------|-------------------|-------|-----------|-------------|
| none | 1x | Fastest | Lowest | Small messages, already compressed |
| snappy | ~2x | Fast | Low | General purpose |
| lz4 | ~2x | Very fast | Low | Best default choice |
| zstd | ~3x | Moderate | Medium | Maximum compression needed |
| gzip | ~3x | Slow | High | Legacy compatibility only |

---

## Section 19: Spring Boot Production Configuration Summary

```java
@Configuration
public class KafkaProductionConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        props.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 32768);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120000);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object>
            kafkaListenerContainerFactory(
                ConsumerFactory<String, Object> consumerFactory,
                KafkaTemplate<String, Object> kafkaTemplate) {

        var factory = new ConcurrentKafkaListenerContainerFactory<String, Object>();
        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(3);  // match partition count per instance
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.BATCH);

        factory.setCommonErrorHandler(new DefaultErrorHandler(
            new DeadLetterPublishingRecoverer(kafkaTemplate),
            new ExponentialBackOff(1000L, 2.0)
        ));

        return factory;
    }

    @Bean
    public ConsumerFactory<String, Object> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "45000");
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "15000");
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "300000");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS,
            JsonDeserializer.class.getName());
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "com.example.events");
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
            CooperativeStickyAssignor.class.getName());
        return new DefaultKafkaConsumerFactory<>(props);
    }
}
```

### Spring application.yml summary

```yaml
spring:
  kafka:
    bootstrap-servers: broker1:9092,broker2:9092,broker3:9092
    producer:
      acks: all
      properties:
        enable.idempotence: true
        compression.type: lz4
        linger.ms: 10
        delivery.timeout.ms: 120000
    consumer:
      auto-offset-reset: earliest
      enable-auto-commit: false
      properties:
        isolation.level: read_committed
        max.poll.records: 500
        session.timeout.ms: 45000
        heartbeat.interval.ms: 15000
        max.poll.interval.ms: 300000
        partition.assignment.strategy: org.apache.kafka.clients.consumer.CooperativeStickyAssignor
    listener:
      ack-mode: batch
      concurrency: 3
```

---

## Section 20: Quick Reference -- Interview Answer Templates

**Q: How is Kafka different from RabbitMQ?**
A: Kafka is a distributed commit log; messages persist based on retention policy and are read by offset. RabbitMQ is a message broker; messages are deleted on consumption. Kafka supports replay, parallel processing via partitions, and both queue and pub-sub semantics through consumer groups. See [Section 1.1](#11-kafka-vs-traditional-message-queues).

**Q: How do you ensure no data loss?**
A: `replication.factor=3`, `min.insync.replicas=2`, `acks=all`, `unclean.leader.election.enable=false`, idempotent producer enabled. Monitor `UnderMinIsrPartitionCount` and `UncleanLeaderElectionsPerSec`. See [Section 3.3](#33-the-golden-durability-configuration).

**Q: How do you handle duplicate messages?**
A: Enable idempotent producer for producer-side dedup. For consumer-side: use database upserts with natural keys, maintain a processed-message-ID set (in Redis/DB), or use Kafka transactions for consume-transform-produce. See [Section 7.4](#74-making-consumer-processing-idempotent).

**Q: How do you handle a slow consumer?**
A: Reduce `max.poll.records`, use pause/resume API for backpressure, offload heavy processing to async worker threads, scale consumer group (up to partition count), optimize downstream dependencies. See [Section 5.4](#54-backpressure-handling----pauseresume-pattern).

**Q: Your consumer group is constantly rebalancing. What do you do?**
A: Switch to `CooperativeStickyAssignor`, enable static membership (`group.instance.id`), increase `max.poll.interval.ms`, reduce `max.poll.records`, implement graceful shutdown. On Kafka 4.0+, use `group.protocol=consumer`. See [Section 5.2](#52-consumer-group-rebalancing----the-1-production-pain-point).

**Q: How do you achieve exactly-once semantics?**
A: Idempotent producer (PID + sequence) + transactional producer (`transactional.id` + `beginTransaction/commitTransaction`) + consumer `isolation.level=read_committed`. For Kafka Streams: `processing.guarantee=exactly_once_v2`. See [Section 7](#section-7-delivery-guarantees).

**Q: How do you send the same message to two different services?**
A: Use different `group.id` values. Each consumer group independently reads all messages from the topic. Kafka maintains separate offsets per group. This is Kafka's native pub-sub model. See [Section 5.1](#51-how-to-send-the-same-message-to-two-consumers-fan-out--broadcast).

**Q: How do you choose the number of partitions?**
A: Start with `max(target_throughput / consumer_throughput, number_of_consumer_instances)`. Over-provision for keyed topics (cannot add partitions later without breaking key distribution). Make partition count a multiple of broker count. See [Section 6.2](#62-partition-count-selection).

**Q: What happens when a broker goes down?**
A: Controller detects via missed heartbeats, elects new leaders from ISR for all partitions hosted on the failed broker. If `min.insync.replicas` is met, writes continue. If not, producers with `acks=all` receive `NotEnoughReplicasException`. Broker recovers by replaying its log from the leader. See [Section 3](#section-3-replication-and-fault-tolerance).

**Q: How do you migrate from ZooKeeper to KRaft?**
A: Cannot upgrade directly to Kafka 4.0. Use Kafka 3.9 as bridge: deploy KRaft controller quorum with `zookeeper.metadata.migration.enable=true`, rolling restart brokers, remove ZooKeeper configs, finalize metadata version, then upgrade to 4.0. See [Section 11.1](#111-kraft-mode-zookeeper-removed).

---

## Section 21: Lead Interview Questions -- Logical and Production Scenarios

This section contains 50+ questions specifically designed for lead-level interviews. Each answer references the relevant playbook section for deep-dive study.

---

### Category A: Architecture and Design Decisions

**Q1: You are designing a new event-driven microservices system. How do you decide topic granularity -- one topic per event type or one topic per domain?**

A: Use **one topic per event type** (e.g., `order.created`, `order.shipped`) for most cases. This gives consumers fine-grained subscription control and independent retention per event type. Use **one topic per domain** (e.g., `order-events`) only when consumers always need all events for that domain and you need cross-event ordering per entity (same key = same partition). The trade-off: fewer topics are simpler to manage but force consumers to filter events they don't need, wasting network and CPU. At lead level, you should also consider schema evolution -- single-topic-per-domain requires a union schema or header-based type discriminator.

**Q2: A team wants to use Kafka as a database. What do you tell them?**

A: Kafka is an append-only commit log, not a database. It excels at event streaming and decoupling, but lacks: random-access reads by key (without Kafka Streams state stores), indexes for complex queries, ACID transactions across entities, and efficient point lookups. Use Kafka as the **event backbone** and project data to purpose-built read stores (PostgreSQL, Elasticsearch, Redis). The Transactional Outbox pattern (Section 17.2) bridges the two worlds safely. Log compaction provides "latest state per key" semantics, but it's not a substitute for a queryable data store.

**Q3: How would you handle cross-datacenter replication?**

A: Use **MirrorMaker 2** (MM2), which is built on Kafka Connect. MM2 replicates topics, consumer group offsets, and ACLs across clusters. Key decisions: active-passive (one cluster is primary, other is DR) vs active-active (both accept writes, requires conflict resolution). For active-active, use topic prefixes (e.g., `dc1.orders`, `dc2.orders`) to avoid circular replication. Monitor replication lag with `MirrorSourceConnector` metrics. Set `replication.policy.class` to `IdentityReplicationPolicy` if you want the same topic names in both clusters.

**Q4: How do you handle multi-tenant Kafka -- separate clusters or shared?**

A: For most cases, use a **shared cluster with topic-per-tenant naming** (e.g., `tenant-A.orders`) and ACLs for isolation. This is operationally simpler and cost-effective. Use **separate clusters** only for: regulatory compliance (data residency), extreme isolation requirements, or tenants with wildly different SLAs. With shared clusters, use quotas (`producer_byte_rate`, `consumer_byte_rate`) to prevent noisy-neighbor problems. Monitor per-tenant throughput and set alerts on quota violations.

---

### Category B: Logical/Scenario Questions

**Q5: Producer sends messages M1, M2, M3 in order. Consumer receives M2 before M1. How is this possible?**

A: Three possibilities:
1. **Messages went to different partitions** (different keys or null keys). Kafka only guarantees ordering within a partition. See Section 9.1.
2. **Multiple producers** wrote to the same partition -- Kafka does not order across producers. See Section 9.4.
3. **Retries without idempotence** and `max.in.flight.requests > 1`: M1 fails, M2 succeeds, M1 retries and succeeds after M2. Fix: enable idempotent producer. See Section 9.3.

**Q6: You need to process exactly 1 million messages and stop. How do you implement this with Kafka?**

A: Assign specific partitions manually using `consumer.assign()` instead of `subscribe()`. Track a counter across all assigned partitions. When the counter reaches 1 million, call `consumer.close()`. Use `commitSync()` after each batch to track progress for restart. Alternatively, use Kafka Streams with a custom `Transformer` that maintains a count in a state store and produces a "done" signal when the threshold is reached.

**Q7: Two microservices must process the same message but in different orders. How?**

A: Use **different consumer groups** (Section 5.1). Each group independently reads all messages from the topic and processes at its own pace. If Service A needs FIFO and Service B needs priority-based processing, Service B can consume into an internal priority queue (e.g., `PriorityBlockingQueue`) and reorder before processing. Kafka guarantees delivery to both groups; processing order is the consumer's responsibility.

**Q8: How do you implement delayed/scheduled message delivery in Kafka?**

A: Kafka has no native delay queue. Workarounds:
1. **Retry topics with delay** (`@RetryableTopic` in Spring Kafka): messages are published to retry topics with increasing delays. See Section 8.4.
2. **Timestamp-based consumer pause**: Consume messages, check if `scheduledTime > now()`, if yes, pause the partition and resume after delay.
3. **External scheduler**: Consume immediately, store in a database with a `fire_at` timestamp, and poll the database for due messages.
4. **Kafka 4.0+ Share Groups**: Use `RELEASE` acknowledgment to make a message available for retry after the lock duration expires. See Section 11.3.

**Q9: How do you replay all messages for a specific consumer group from a specific date?**

A: Use `kafka-consumer-groups.sh --reset-offsets`:
```bash
kafka-consumer-groups.sh --bootstrap-server broker:9092 \
  --group my-group --topic my-topic \
  --reset-offsets --to-datetime 2026-01-01T00:00:00.000 \
  --execute
```
Or programmatically:
```java
Map<TopicPartition, Long> timestampToSearch = new HashMap<>();
for (TopicPartition tp : consumer.assignment()) {
    timestampToSearch.put(tp, targetTimestamp);
}
Map<TopicPartition, OffsetAndTimestamp> offsets =
    consumer.offsetsForTimes(timestampToSearch);
for (Map.Entry<TopicPartition, OffsetAndTimestamp> entry : offsets.entrySet()) {
    consumer.seek(entry.getKey(), entry.getValue().offset());
}
```

**Q10: How would you implement a request-reply pattern with Kafka?**

A: Use a dedicated reply topic per service instance. Producer sends a request with a `correlationId` header and a `replyTo` header indicating the reply topic. Consumer processes the request and publishes the response to the reply topic with the same `correlationId`. The original producer has a consumer on the reply topic that matches by `correlationId`. Spring Kafka provides `ReplyingKafkaTemplate` for this:
```java
@Bean
public ReplyingKafkaTemplate<String, String, String> replyingTemplate(
        ProducerFactory<String, String> pf,
        ConcurrentMessageListenerContainer<String, String> repliesContainer) {
    return new ReplyingKafkaTemplate<>(pf, repliesContainer);
}

// Usage
RequestReplyFuture<String, String, String> future =
    replyingTemplate.sendAndReceive(new ProducerRecord<>("requests", key, value));
ConsumerRecord<String, String> reply = future.get(10, TimeUnit.SECONDS);
```

---

### Category C: Production Troubleshooting

**Q11: Consumers are processing messages, but lag keeps increasing during peak hours and recovers at night. What do you do?**

A: This is a classic **capacity mismatch** during peak load. Steps:
1. Measure peak producer rate vs consumer processing rate.
2. If consumer rate < producer rate during peak: scale consumers (add instances up to partition count).
3. If already at partition count: increase partition count on a new topic.
4. Consider auto-scaling consumers based on lag metrics (KEDA on Kubernetes, or custom HPA with Prometheus adapter).
5. Pre-warm consumers 15 minutes before expected peak.
6. Optimize processing logic: profile for slow DB queries, HTTP calls, serialization.
See Section 16 Issue 1.

**Q12: After a deployment, one consumer instance is processing much slower than others. What do you investigate?**

A: Likely causes:
1. **Hot partition**: Check per-partition lag. If one partition has disproportionate lag, it's key skew. See Section 6.3.
2. **JVM issue on that instance**: Check GC logs, heap usage, CPU throttling (Kubernetes resource limits).
3. **Network latency**: The instance may be in a different AZ with higher latency to the broker or downstream DB.
4. **Thread starvation**: If `concurrency > partition count`, some threads are idle while the working ones are overloaded.
5. **Sticky assignment**: After rebalance, one consumer may have received more partitions. Check `kafka-consumer-groups.sh --describe`.

**Q13: You get a PagerDuty alert: `UnderMinIsrPartitionCount > 0`. What is your incident response?**

A: This means at least one partition cannot accept `acks=all` writes. Immediate response:
1. Identify affected partitions: `kafka-topics.sh --describe --under-min-isr-partitions`.
2. Check which broker(s) are out of ISR.
3. Check broker process health (is it running? Check systemd/supervisor).
4. Check broker disk: `df -h` -- full disk prevents followers from replicating.
5. Check broker GC: long GC pauses cause replicas to fall out of ISR.
6. Check network: packet loss between brokers causes replica lag.
7. If broker is down: restart it. Monitor `IsrExpandsPerSec` for recovery.
8. If disk full: emergency retention reduction on large topics, then add disk.
9. Communicate status: "Writes to X topics may fail for producers using acks=all."
See Sections 3.1, 16 Issue 2.

**Q14: A producer is getting `TimeoutException` but the broker is healthy. What is wrong?**

A: Check these in order:
1. **`advertised.listeners` misconfiguration**: Producer can connect to bootstrap server but cannot reach the advertised address of the partition leader. Common in Docker/Kubernetes.
2. **Metadata staleness**: Topic was recently created. Producer cached old metadata. Increase `metadata.max.age.ms`.
3. **`delivery.timeout.ms` too tight**: If broker latency increased (new replicas syncing, disk contention), the total delivery time exceeds the budget. Increase it.
4. **Firewall/security group**: Port 9092 is open but 9093 (SSL) or inter-broker port is blocked.
5. **DNS resolution**: Producer resolves a different IP than what the broker is listening on.
See Section 4.5.

**Q15: You deployed a new version of a consumer and now it's stuck on a specific offset, not progressing. Logs show `DeserializationException`. What happened?**

A: A **poison pill** -- a message the new consumer cannot deserialize. Common causes:
1. Producer changed the message format (e.g., added a required field in Avro without a default).
2. Schema ID in the message points to a schema the consumer's deserializer doesn't understand.
3. Raw bytes corruption.

Fix:
1. Wrap deserializer with `ErrorHandlingDeserializer` to prevent infinite loop. See Section 8.3.
2. Configure `DefaultErrorHandler` with `DeadLetterPublishingRecoverer` to route the bad message to DLT. See Section 8.2.
3. Root cause: enforce schema compatibility in CI. See Section 10.4.
4. Immediate fix: skip the message by manually advancing the consumer offset:
```bash
kafka-consumer-groups.sh --bootstrap-server broker:9092 \
  --group my-group --topic my-topic --reset-offsets \
  --to-offset <bad_offset + 1> --partition 3 --execute
```

---

### Category D: Design and Scale Questions

**Q16: How would you design a notification system using Kafka that must deliver to email, SMS, and push notification channels?**

A: Use fan-out via consumer groups:
- Single `notification-events` topic produced by business services.
- Three independent consumer groups: `email-sender`, `sms-sender`, `push-sender`.
- Each gets every notification and processes it through its channel.
- Each channel service has its own DLT for failed deliveries.
- Rate limiting per channel (email providers have rate limits).
- Dedup by notification ID to handle replays.
See Section 5.1.

**Q17: How do you handle a Kafka topic with 10 billion messages and a consumer that needs to process all of them for a one-time data migration?**

A: Steps:
1. Create a dedicated consumer group for the migration.
2. Set `auto.offset.reset=earliest`.
3. Use batch processing: `max.poll.records=1000` or higher.
4. Set `fetch.min.bytes` to 1MB+ for larger fetches.
5. Use multiple consumer instances (up to partition count).
6. Disable retries on transient errors -- log failures and continue (process DLT after main run).
7. Set generous `max.poll.interval.ms` to avoid rebalances during heavy processing.
8. Monitor progress via lag metrics. Estimated completion time = total lag / processing rate.
9. After migration completes, delete the consumer group.

**Q18: Your company processes 500K events/second. How do you size the Kafka cluster?**

A: Back-of-the-napkin calculation:
1. Average message size: e.g., 1KB -> 500MB/sec raw throughput.
2. With RF=3, broker ingestion = 500MB/sec * 3 = 1.5GB/sec.
3. Each broker can sustain ~100-200MB/sec writes. Need ~8-15 brokers.
4. Partitions: if each consumer processes 10K msgs/sec, need 50 partitions minimum per topic.
5. Retention: 7 days * 500MB/sec = ~300TB storage. Plan disk accordingly.
6. Network: each broker needs at least 10Gbps NIC.
7. Always benchmark with your actual message sizes and processing patterns.

**Q19: How do you implement rate limiting on Kafka consumers to protect a downstream API?**

A: Three approaches:
1. **Consumer-side throttle**: Use a `RateLimiter` (Guava) before each API call.
2. **Pause/Resume**: Monitor downstream API latency. When it exceeds threshold, pause consumer partitions. Resume when latency recovers. See Section 5.4.
3. **Kafka-side quotas**: Set consumer byte-rate quotas on the broker. This limits how fast the consumer can fetch.
```java
RateLimiter rateLimiter = RateLimiter.create(100); // 100 requests/sec
for (ConsumerRecord<String, String> record : records) {
    rateLimiter.acquire(); // blocks until rate allows
    callDownstreamApi(record);
}
```

**Q20: How do you ensure ordering when consuming from Kafka and writing to a database?**

A: Ordering is guaranteed within a partition. To maintain it end-to-end:
1. Use the entity ID as partition key -> all events for that entity go to the same partition.
2. Consumer processes records from each partition sequentially (default behavior).
3. Do NOT use `concurrency > partition count` -- it doesn't help and can break ordering.
4. If using async processing threads, maintain per-key ordering by using a per-key single-threaded executor:
```java
Map<String, ExecutorService> keyExecutors = new ConcurrentHashMap<>();
for (ConsumerRecord<String, String> record : records) {
    ExecutorService executor = keyExecutors.computeIfAbsent(
        record.key(), k -> Executors.newSingleThreadExecutor()
    );
    executor.submit(() -> processRecord(record));
}
```

---

### Category E: Kafka Internals Deep Dive

**Q21: Explain the Kafka consumer poll loop. What happens internally when you call `poll()`?**

A: `poll()` does much more than fetching records:
1. **Heartbeat management**: Ensures heartbeat thread is running (separate thread since KIP-62).
2. **Coordinator discovery**: If not connected, finds the group coordinator broker.
3. **Group join/sync**: On first poll or after rebalance, joins group and receives partition assignment.
4. **Offset fetch**: Fetches committed offsets for assigned partitions.
5. **Fetch requests**: Sends fetch requests to partition leaders for assigned partitions.
6. **Deserialization**: Deserializes key, value, and headers.
7. **Interceptor callbacks**: Runs configured consumer interceptors.
8. **Returns records**: Batched up to `max.poll.records`.
If you block too long between `poll()` calls (beyond `max.poll.interval.ms`), the coordinator considers you dead. See Section 5.2.

**Q22: How does Kafka achieve zero-copy? Why can't a JVM-based system normally do this?**

A: Traditional data transfer requires 4 copies and 4 context switches:
1. Disk -> kernel buffer (DMA)
2. Kernel buffer -> user buffer (context switch)
3. User buffer -> socket buffer (context switch)
4. Socket buffer -> NIC (DMA)

Kafka uses `FileChannel.transferTo()` which maps to Linux `sendfile()`:
1. Disk -> kernel page cache (DMA)
2. Page cache -> NIC buffer (DMA, with DMA-capable NIC)

Only 2 copies, 2 context switches (just the system call itself). Data never enters JVM heap. This only works because Kafka doesn't need to inspect or transform the data on the broker -- it's a commit log that stores and forwards bytes. See Section 1.3.

**Q23: What is the role of the `__consumer_offsets` topic?**

A: It stores committed consumer group offsets. Key = `(group_id, topic, partition)`, value = `(offset, metadata, timestamp)`. It's a compacted topic with 50 partitions by default. Each consumer group's offsets are stored on a single partition (determined by hash of group ID). The broker serving as the **Group Coordinator** for a given group is the leader of that group's partition in `__consumer_offsets`. When a consumer commits, it writes to this topic. On consumer restart, offsets are read from here.

**Q24: What is the difference between `session.timeout.ms` and `max.poll.interval.ms`?**

A: They detect fundamentally different failure modes using different threads:
- `session.timeout.ms`: Monitored by the **background heartbeat thread**. Detects process-level failures (crash, network partition). If no heartbeat is received within this window, the coordinator considers the consumer dead.
- `max.poll.interval.ms`: Monitored by the **main application thread**. Detects application-level stalls (deadlock, infinite loop, slow processing). If `poll()` is not called within this window, the coordinator triggers a rebalance.
You can have a running process (heartbeats flowing) that's stuck processing (not calling `poll()`) -- only `max.poll.interval.ms` catches this. See Section 5.2.

**Q25: How does log compaction work internally? Can it lose data?**

A: The log cleaner thread periodically scans closed (sealed) segments. For each key, it keeps only the record with the highest offset and discards older ones. The active segment is never compacted. Tombstones (key + null value) mark keys for deletion and are retained for `delete.retention.ms` (default 24 hours) before being removed. Compaction does NOT lose "the latest state" -- it only removes superseded values. However, if you need full event history (audit trail), do NOT use compaction. See Section 2.3.

---

### Category F: Spring Kafka Specifics

**Q26: What is the difference between `DefaultErrorHandler` and `CommonErrorHandler` in Spring Kafka?**

A: `CommonErrorHandler` is the interface. `DefaultErrorHandler` is the primary implementation (replaced the deprecated `SeekToCurrentErrorHandler`). `DefaultErrorHandler` provides: configurable retry with backoff, non-retryable exception classification, integration with `DeadLetterPublishingRecoverer`, and support for both record and batch listeners. See Section 8.2.

**Q27: How does `@RetryableTopic` differ from `DefaultErrorHandler` retry?**

A: `DefaultErrorHandler` retries **in-memory** -- the consumer thread is blocked during backoff, and the partition is not processed. `@RetryableTopic` uses **non-blocking retry** -- failed messages are published to separate retry topics with delay headers. The main consumer continues processing other messages immediately. Use `DefaultErrorHandler` for transient errors with short backoff (seconds). Use `@RetryableTopic` when retry delays are long (minutes/hours) and you can't afford to block the consumer. See Section 8.4.

**Q28: How do you test Kafka consumers and producers in Spring Boot without a running Kafka cluster?**

A: Use `@EmbeddedKafka` for integration tests:
```java
@SpringBootTest
@EmbeddedKafka(partitions = 3, topics = {"test-topic"})
class OrderConsumerIntegrationTest {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private OrderConsumer orderConsumer;

    @Test
    void shouldProcessOrderEvent() throws Exception {
        kafkaTemplate.send("test-topic", "order-1", orderJson).get();
        // Assert processing result with Awaitility
        Awaitility.await().atMost(10, TimeUnit.SECONDS)
            .untilAsserted(() -> verify(orderService).process(any()));
    }
}
```
For Kafka Streams: use `TopologyTestDriver` for unit testing without any broker.

**Q29: How do you set different concurrency for different `@KafkaListener` methods?**

A: Override per listener:
```java
@KafkaListener(topics = "orders", groupId = "order-group", concurrency = "6")
public void processOrders(OrderEvent event) { ... }

@KafkaListener(topics = "analytics", groupId = "analytics-group", concurrency = "2")
public void processAnalytics(AnalyticsEvent event) { ... }
```
Or create multiple `ConcurrentKafkaListenerContainerFactory` beans with different concurrency values and reference them via `containerFactory`. See Section 19.

---

### Category G: Cross-Cutting Concerns

**Q30: How do you implement distributed tracing across Kafka producers and consumers?**

A: Inject trace context into Kafka headers:
```java
// Producer side
Span span = tracer.nextSpan().name("kafka-send").start();
ProducerRecord<String, String> record = new ProducerRecord<>("topic", key, value);
record.headers().add("traceparent", span.context().toString().getBytes());
producer.send(record);

// Consumer side
String traceparent = new String(record.headers().lastHeader("traceparent").value());
Span span = tracer.nextSpan(extractContext(traceparent)).name("kafka-consume").start();
processRecord(record);
span.end();
```
Spring Cloud Sleuth / Micrometer Tracing auto-instruments `KafkaTemplate` and `@KafkaListener` -- just add the dependency and tracing propagates automatically.

**Q31: How do you implement Kafka consumer health checks in a Spring Boot microservice?**

A: Check the listener container state:
```java
@Component
public class KafkaHealthIndicator implements HealthIndicator {

    @Autowired
    private KafkaListenerEndpointRegistry registry;

    @Override
    public Health health() {
        boolean allRunning = registry.getListenerContainers().stream()
            .allMatch(MessageListenerContainer::isRunning);

        if (allRunning) {
            return Health.up()
                .withDetail("containers", registry.getListenerContainerIds())
                .build();
        }
        return Health.down()
            .withDetail("stoppedContainers", getStoppedContainers())
            .build();
    }
}
```

**Q32: How do you handle Kafka cluster upgrades with zero downtime for producers and consumers?**

A: Rolling upgrade strategy:
1. Ensure clients are compatible with both old and new broker versions (check KIP-896 for minimum version requirements).
2. Upgrade brokers one at a time: stop broker, upgrade binary, restart.
3. Wait for ISR to fully sync before upgrading the next broker.
4. After all brokers are upgraded, finalize the metadata version: `kafka-features.sh upgrade --release-version X.Y`.
5. Upgrade clients (producers/consumers) at your own pace -- they're forward-compatible.
For ZK-to-KRaft migration, see Section 11.1.

**Q33: How do you handle backfilling a new consumer that joins an existing topic with months of data?**

A: Options:
1. **Set `auto.offset.reset=earliest`**: Process everything from the beginning. Only viable if the consumer can handle the volume and processing is idempotent.
2. **Reset to a specific timestamp**: `kafka-consumer-groups.sh --reset-offsets --to-datetime`.
3. **Create a snapshot**: Use Kafka Connect or a one-time job to create a compacted "snapshot" topic with current state, then consume from that + the live topic from a specific offset.
4. **Use a separate backfill consumer group**: Process historical data at a controlled rate without affecting the live consumer group's lag metrics.

---

### Category H: Advanced Production Scenarios

**Q34: Your Kafka cluster has 100 partitions for an orders topic, but one customer generates 60% of all orders. What do you do?**

A: This is the **hot key / celebrity key** problem.
1. **Detect**: `kafka-consumer-groups.sh --describe` shows one partition with 10x lag. Or monitor per-partition byte rate in Prometheus.
2. **Composite key**: Change from `customerId` to `customerId|orderId`. Spreads load but loses per-customer ordering.
3. **Salted key**: `customerId#salt` where salt = `orderId.hashCode() % 10`. Spreads across 10 partitions. Consumer re-aggregates downstream if needed.
4. **Custom partitioner**: Route the hot customer to a dedicated partition range. See Section 6.4.
5. **Dedicated topic**: For extreme cases, give the hot customer their own topic.
See Section 6.3.

**Q35: A Kafka producer is sending messages with `acks=all` and `min.insync.replicas=2`. One broker out of three goes down. What happens?**

A: With RF=3 and 1 broker down:
- ISR shrinks from 3 to 2 for partitions that had the failed broker as a replica.
- Since `min.insync.replicas=2` and ISR still has 2 members, writes continue successfully.
- `UnderReplicatedPartitions` metric increases (alert should fire).
- If ANOTHER broker goes down, ISR drops to 1 < `min.insync.replicas=2`, and producers receive `NotEnoughReplicasException`.
See Section 3.3.

**Q36: You need to consume from Kafka and write to both a database and another Kafka topic atomically. How?**

A: Use Kafka transactions (consume-transform-produce pattern):
```java
producer.initTransactions();
while (true) {
    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
    producer.beginTransaction();
    for (ConsumerRecord<String, String> record : records) {
        // Write to output topic
        producer.send(new ProducerRecord<>("output-topic", record.key(), transform(record.value())));
    }
    // Commit input offsets in the same transaction
    producer.sendOffsetsToTransaction(currentOffsets(records), consumer.groupMetadata());
    producer.commitTransaction();
}
```
For the database write: use the **Transactional Outbox pattern** (Section 17.2). Write to the database in a local DB transaction that also inserts into an outbox table. A CDC connector (Debezium) publishes the outbox events to Kafka. This is the only way to atomically write to both a DB and Kafka.

**Q37: How do you handle a scenario where Kafka messages arrive out of order due to producer retries, and your consumer depends on strict ordering?**

A: Prevention is better than cure:
1. Enable idempotent producer to prevent retry-caused reordering. See Section 9.3.
2. Use a single partition for strict global ordering (sacrifices throughput).
3. For entity-level ordering: use entity ID as key (all events for that entity go to the same partition).

If out-of-order messages are already in the topic:
4. Consumer-side reordering: add a monotonic sequence number or timestamp in the message payload. Consumer buffers messages in a priority queue and emits in order, with a configurable wait window for late arrivals.
5. Accept eventual consistency and design the consumer to handle out-of-order processing idempotently.

**Q38: Your team uses Kafka for inter-service communication. Service B takes 30 seconds to process some messages. How do you prevent rebalancing?**

A: Multiple options (apply in combination):
1. Set `max.poll.interval.ms=120000` (2 minutes) to accommodate worst-case processing.
2. Set `max.poll.records=10` to process smaller batches within the timeout.
3. Offload heavy processing to a separate thread pool. The consumer poll loop only enqueues work and commits offsets after the thread pool confirms completion.
4. Use the **pause/resume pattern**: pause the assigned partitions while processing, resume when done. The consumer continues to send heartbeats during pause. See Section 5.4.

**Q39: How do you handle schema migration where you need to change a field type (e.g., `int` to `long`)?**

A: In Avro, `int` to `long` is a "promotion" and is backward-compatible (old consumers using `int` can read `long` data -- Avro handles this). But going from `long` to `int` is NOT compatible (potential data truncation). Steps:
1. Check compatibility mode in Schema Registry (default BACKWARD).
2. Run `mvn schema-registry:test-compatibility` to validate.
3. If the change is compatible: register new schema version, deploy consumers first (for BACKWARD mode), then producers.
4. If the change is NOT compatible: create a new topic with the new schema, migrate producers, then consumers. The old topic continues with old schema until retention expires.
See Section 10.

**Q40: How do you implement event deduplication when multiple producers might send the same event?**

A: Layers of dedup:
1. **Producer-side**: Use idempotent producer for retry-based dedup within a session. See Section 4.2.
2. **Kafka-side**: Log compaction keeps only the latest value per key -- natural dedup for stateful entities. See Section 2.3.
3. **Consumer-side**: Maintain a dedup store (Redis set with TTL, or database unique constraint on event ID). See Section 7.4.
4. **Application-level**: Include a globally unique event ID (UUID) in every message. Consumers check if the ID was already processed before executing business logic.

---

### Category I: Kafka vs Alternatives

**Q41: When would you NOT use Kafka?**

A: Kafka is not the right choice for:
1. **Request-reply patterns with low latency**: Use gRPC or HTTP. Kafka adds latency due to batching and polling.
2. **Small-scale simple pub-sub**: Redis Pub/Sub or a simple message queue is operationally lighter.
3. **Complex routing and filtering**: RabbitMQ's exchange/binding model is more expressive for message routing.
4. **Primary database**: Kafka is a log, not a database. See Q2.
5. **Tasks requiring strict global ordering at high throughput**: Single-partition throughput is limited to what one consumer can handle.
6. **Very small payloads with sub-millisecond latency**: Kafka's batching and disk writes add latency. Consider in-memory solutions.

**Q42: Kafka vs Pulsar -- when to pick which?**

A: Use **Kafka** when: you have a Java ecosystem, need mature tooling, want the largest community, and your workload is event streaming with consumer groups. Use **Pulsar** when: you need built-in multi-tenancy, geo-replication, tiered storage as a first-class feature, or queue + streaming unified from day one. Kafka 4.0's Share Groups and tiered storage close some of these gaps.

---

### Category J: Operational Excellence

**Q43: How do you perform a Kafka topic migration (rename or repartition)?**

A: Kafka does not support topic renaming. Steps:
1. Create new topic with desired name/partition count.
2. Run dual-write: producers send to BOTH old and new topics temporarily.
3. Start consumers on the new topic (new consumer group, `auto.offset.reset=earliest`).
4. Wait until new topic consumers are caught up.
5. Stop producers writing to old topic.
6. Wait until old topic consumers drain.
7. Switch all producers and consumers to new topic only.
8. Delete old topic after retention period.

**Q44: How do you handle Kafka in a Kubernetes environment?**

A: Key considerations:
1. **StatefulSets**: Use for brokers (stable network identity, persistent storage).
2. **Persistent Volumes**: Use local SSDs or EBS io2 for broker data. NEVER use network-attached storage without testing.
3. **Pod anti-affinity**: Spread brokers across nodes/AZs.
4. **Static group membership**: Set `group.instance.id` to pod name for consumers. Prevents rebalances on rolling restarts.
5. **Liveness/readiness probes**: Check broker health via JMX or admin API.
6. **Resource limits**: Set CPU limits carefully -- Kafka is I/O bound, not CPU bound. Over-limiting CPU causes unnecessary throttling.
7. **Operators**: Consider Strimzi or Confluent Operator for lifecycle management.

**Q45: How do you calculate the right `retention.ms` for a topic?**

A: Consider:
1. **Replay window**: How far back might consumers need to replay? Set retention >= this.
2. **Consumer downtime tolerance**: If a consumer is down for maintenance, how long before data is lost?
3. **Compliance**: Regulatory requirements may mandate minimum or maximum retention.
4. **Storage cost**: 7 days of 100MB/sec = ~60TB per topic (with RF=3 = ~180TB on disk).
5. **Offset retention**: `offsets.retention.minutes` must be <= `retention.ms`, otherwise consumer groups lose offsets before data expires.

**Q46: What is your Kafka production readiness checklist?**

A: Before any Kafka deployment goes to production:

| Category | Check |
|----------|-------|
| Durability | RF >= 3, `min.insync.replicas` = RF - 1, `acks=all`, `unclean.leader.election.enable=false` |
| Producer | Idempotence enabled, async with callbacks, `delivery.timeout.ms` set to match SLA |
| Consumer | Auto-commit disabled, `ErrorHandlingDeserializer`, DLT configured, `CooperativeStickyAssignor` |
| Error handling | `DefaultErrorHandler` with backoff, non-retryable exception classification, DLT monitoring |
| Monitoring | Lag alerts, ISR alerts, broker disk alerts, DLT depth alerts |
| Security | SASL + TLS enabled, ACLs configured, deny by default |
| Schema | Schema Registry with compatibility enforcement, CI validation |
| Testing | Integration tests with `@EmbeddedKafka`, load tested with production-like data |
| Operations | Rolling restart tested, backup/restore procedure documented, runbooks written |

---

### Category K: Kafka 4.0 Specific Questions

**Q47: What is the practical impact of KIP-848 for a team running 100-consumer groups?**

A: Major improvements:
1. **No stop-the-world rebalances**: Only the affected partitions are revoked. Other consumers continue processing.
2. **Faster recovery**: A crashed consumer's partitions are reassigned within seconds, not minutes.
3. **No group leader bottleneck**: In the old protocol, one consumer was elected as group leader and computed assignments for the entire group. With 100 consumers, this was a bottleneck. KIP-848 moves this to the broker.
4. **Simpler consumer code**: Consumers become "thin clients" -- subscribe, heartbeat, process. No leader election logic.
5. **Rolling deployments are smooth**: Each pod restart is an incremental handover, not a full group rebalance.
See Section 11.2.

**Q48: How do Share Groups (KIP-932) change Kafka's positioning vs RabbitMQ?**

A: Share Groups bring queue semantics natively to Kafka:
1. **No partition-consumer limit**: Scale consumers elastically beyond partition count. This eliminates one of Kafka's biggest limitations for queue workloads.
2. **Per-message ack**: ACCEPT, RELEASE, REJECT per record. Similar to RabbitMQ's ack/nack/reject.
3. **Use case fit**: Task queues, job processing, work distribution -- use cases that previously required RabbitMQ alongside Kafka.
4. **What it does NOT replace**: RabbitMQ's sophisticated routing (exchanges, bindings, headers-based routing) and sub-millisecond latency for small messages.
See Section 11.3.

**Q49: Your cluster is on Kafka 3.6 with ZooKeeper. Management wants to upgrade to 4.0 next quarter. What is your migration plan?**

A: Present a phased approach:
1. **Phase 1 (Week 1-2)**: Upgrade to Kafka 3.9 (the bridge release). Rolling upgrade, broker by broker.
2. **Phase 2 (Week 3)**: Deploy KRaft controller quorum (3 controllers). Enable `zookeeper.metadata.migration.enable=true`. Metadata is dual-written.
3. **Phase 3 (Week 4)**: Rolling restart brokers into KRaft-only mode (remove ZK configs). Verify metadata consistency.
4. **Phase 4 (Week 5)**: Finalize metadata version. Decommission ZooKeeper ensemble.
5. **Phase 5 (Week 6)**: Rolling upgrade to Kafka 4.0.
6. **Phase 6 (Week 7)**: Update clients to Kafka 4.0 client libraries. Enable KIP-848 consumer protocol.
At each phase: run integration tests, verify lag, verify ISR health, run for 2-3 days before proceeding.
See Section 11.1.

**Q50: With Java 17 required for Kafka 4.0 brokers, what JVM tuning do you recommend?**

A: Key JVM settings for Kafka brokers on Java 17:
```bash
-Xms6g -Xmx6g                          # 6GB heap (never more than 8GB)
-XX:+UseG1GC                            # G1 is recommended for Kafka
-XX:MaxGCPauseMillis=20                  # Target 20ms GC pauses
-XX:InitiatingHeapOccupancyPercent=35    # Start GC early
-XX:G1HeapRegionSize=16m                 # Match to Kafka's allocation patterns
-XX:+ExplicitGCInvokesConcurrent         # Prevent STW on System.gc()
-XX:+ParallelRefProcEnabled              # Parallel reference processing
-XX:MetaspaceSize=96m
-XX:MinMetaspaceFreeRatio=50
-XX:MaxMetaspaceFreeRatio=80
```
Do NOT allocate more than 8GB heap. Kafka relies on OS page cache for performance, not JVM heap. Leaving 60-70% of RAM for page cache is critical.

---

### Category L: Bonus -- Curveball Questions

**Q51: Can a consumer read from a follower replica instead of the leader?**

A: Yes, since KIP-392 (Kafka 2.4+). Configure `client.rack` on the consumer to match `broker.rack`. The consumer can then read from the closest replica (follower in the same rack/AZ) instead of always going to the leader. This reduces cross-AZ network traffic and latency. However, follower reads may serve slightly stale data (up to `replica.lag.time.max.ms` behind).

**Q52: What happens if you set `acks=all` but `min.insync.replicas=1`?**

A: `acks=all` means "wait for all ISR members to acknowledge." If `min.insync.replicas=1`, the ISR can shrink to just the leader. So `acks=all` effectively degrades to `acks=1` -- the leader alone acknowledges. You get the latency cost of `acks=all` without the durability benefit. Always pair `acks=all` with `min.insync.replicas >= 2`. See Section 3.3.

**Q53: A message was produced successfully (got RecordMetadata back) but the consumer never sees it. Why?**

A: Possible causes:
1. **Transactional producer**: Message was produced but transaction was never committed (or was aborted). Consumer with `read_committed` will never see it.
2. **Consumer started after `retention.ms` expired**: Message was already deleted.
3. **Consumer group offset past the message**: Group was reset to `latest` and the message was already "in the past."
4. **Topic recreation**: Topic was deleted and recreated. Consumer group offset points to a different incarnation.
5. **Consumer is on a different topic**: Typo in topic name (surprisingly common).

**Q54: How does Kafka handle back-to-back broker failures during a rolling restart?**

A: If Broker 1 is stopped for upgrade and Broker 2 also goes down before Broker 1 returns:
- Partitions with leaders on Broker 2 trigger leader election from ISR.
- If ISR for some partitions only had Brokers 1 and 2, those partitions become unavailable (no ISR member alive).
- With `unclean.leader.election.enable=false`, these partitions stay unavailable until one of them returns.
- **Mitigation**: Wait for full ISR recovery after each broker restart before proceeding to the next. Monitor `UnderReplicatedPartitions == 0` as the gate condition.

**Q55: You're asked to build a real-time leaderboard using Kafka. How?**

A: Use Kafka Streams:
1. **Input topic**: `game-events` (key = player_id, value = score_delta).
2. **KTable aggregation**: Group by player_id, aggregate scores using `reduce()` or `aggregate()`.
3. **Materialized state store**: Backed by RocksDB locally, changelog topic for fault tolerance.
4. **Interactive Queries**: Expose the state store via REST API using Kafka Streams Interactive Queries (`ReadOnlyKeyValueStore`).
5. For top-N: Use a custom processor that maintains a sorted set and emits to an output topic on change.
6. Serve via WebSocket for real-time updates to clients.

---

## Appendix: Configuration Quick Reference

### Producer Configs (Production Defaults)

```properties
acks=all
enable.idempotence=true
retries=2147483647
delivery.timeout.ms=120000
request.timeout.ms=30000
max.block.ms=60000
batch.size=32768
linger.ms=10
compression.type=lz4
buffer.memory=67108864
max.in.flight.requests.per.connection=5
```

### Consumer Configs (Production Defaults)

```properties
enable.auto.commit=false
auto.offset.reset=earliest
isolation.level=read_committed
max.poll.records=500
max.poll.interval.ms=300000
session.timeout.ms=45000
heartbeat.interval.ms=15000
fetch.min.bytes=1
fetch.max.wait.ms=500
partition.assignment.strategy=org.apache.kafka.clients.consumer.CooperativeStickyAssignor
```

### Broker Configs (Production Defaults)

```properties
default.replication.factor=3
min.insync.replicas=2
unclean.leader.election.enable=false
num.partitions=6
log.retention.hours=168
log.segment.bytes=1073741824
num.network.threads=8
num.io.threads=16
num.replica.fetchers=4
offsets.retention.minutes=10080
auto.create.topics.enable=false
delete.topic.enable=true
```

### JMX Metrics to Monitor

```
kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions
kafka.server:type=ReplicaManager,name=UnderMinIsrPartitionCount
kafka.controller:type=KafkaController,name=ActiveControllerCount
kafka.controller:type=ControllerStats,name=UncleanLeaderElectionsPerSec
kafka.server:type=ReplicaManager,name=IsrShrinksPerSec
kafka.server:type=ReplicaManager,name=IsrExpandsPerSec
kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec
kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec
kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec
kafka.network:type=RequestMetrics,name=RequestsPerSec,request=Produce
kafka.network:type=RequestMetrics,name=TotalTimeMs,request=Produce
kafka.log:type=LogFlushStats,name=LogFlushRateAndTimeMs
```

---

## How to Talk About Kafka in an Interview (Human English)

> This section is how you'd actually explain Kafka in a conversation — casual, clear, with real examples. No bullet-point overload. Just talk like a senior engineer would.

---

### "What is Kafka?"

**Start here when the interviewer asks this:**

> "Kafka is a distributed, append-only log — basically a super durable, ordered message bus that multiple services can write to and read from independently. Think of it like a shared logbook where producers write entries and consumers read from wherever they left off. The key thing that separates Kafka from something like RabbitMQ is that messages don't disappear after they're consumed. They stay on disk for as long as you configure — days, weeks, forever. So if your payment service goes down at 3am and comes back up at 4am, it just picks up right where it stopped. No message lost."

**Good analogy to use:**

> "I usually explain it like a newspaper. Publishers print newspapers and put them in the rack. Each reader picks up today's paper on their own schedule. If you miss Monday's paper, you can still go back and read it. Kafka topics are the newspaper. Partitions are different city editions. Consumer groups are different families — each family reads their own copy independently."

---

### "What is a topic, partition, and offset?"

> "A topic is like a category name — 'orders', 'payments', 'user-events'. A partition is how that topic gets split across the cluster for parallelism. If I have 12 partitions, I can have up to 12 consumers in a group reading in parallel. The offset is just a sequential number — the position of a message inside a partition. Consumer groups track their offset so they know where to resume. That's how Kafka gives you both scalability and durability."

---

### "Why partitions? Can I have too many?"

> "Partitions are the unit of parallelism. More partitions = more consumers can work in parallel. But there's a real cost — every partition is a file on disk, a set of open file descriptors, and extra metadata the controller has to manage. In production, you don't want 10,000 partitions per broker. Rule of thumb I use: target maybe 10-25 partitions per broker for a balanced cluster. And the message key determines which partition the message lands in — so I always make sure my partition key distributes evenly, otherwise you get hot partitions where one consumer does all the work."

---

### "What's the difference between at-least-once, at-most-once, and exactly-once?"

> "This is about what happens when things fail. At-most-once means I fire the message and don't retry — so if it fails, it's gone. At-least-once means I retry until I get an ack — so the message might arrive more than once if there's a network hiccup. Exactly-once is the dream — the message lands exactly one time no matter what. In Kafka, you get exactly-once with idempotent producers (Kafka dedupes retries) and transactional APIs. But exactly-once is only within Kafka itself — your consumer side still needs to be idempotent. I always tell teams: design for at-least-once and make your consumers idempotent. That's the pragmatic production answer."

---

### "What's consumer lag and why does it matter?"

> "Consumer lag is how many messages the consumer is behind compared to the latest message on the broker. If my topic has offset 1000 but my consumer is at offset 800, the lag is 200. In normal operation, lag should be near zero. If it's growing, it means the consumer can't keep up — either the producer is generating faster than we consume, or the consumer is slow for some reason. I alert on lag per consumer group per partition. If lag spikes past a threshold and doesn't recover in, say, 10 minutes, that's a page. I use Prometheus JMX exporter or Kafka's own metrics to track this."

---

### "How do you handle poison pill messages?"

> "A poison pill is a message your consumer can't process — maybe it's malformed JSON, maybe it triggers a bug, maybe it references data that doesn't exist. The naive consumer will just keep retrying it and block all subsequent messages in that partition forever. The production fix is a Dead Letter Queue — after N retries, move the bad message to a separate DLQ topic and continue. In Spring Kafka I use `DefaultErrorHandler` with exponential backoff and `@RetryableTopic` for non-blocking retries. The DLQ gets monitored and replayed manually after the root cause is fixed."

---

### "When would you NOT use Kafka?"

> "Honest answer: Kafka is powerful but it's also infrastructure you have to operate. If my use case is simple point-to-point messaging between two services, and the team is small, I'd reach for Redis Streams or even SQS first. Kafka makes sense when I need high throughput, multiple consumers of the same event, long-term retention, or replay capability. For a startup with one producer and one consumer doing 10 messages per second, Kafka is overkill. For a platform doing a billion events a day across 20 services, Kafka is the right call."

---

### Quick Cheat Sheet for Verbal Answers

| Question | One-line answer |
|---|---|
| What's a topic? | Named category of messages, like a table in a DB |
| What's a partition? | Unit of parallelism inside a topic; one consumer per partition |
| What's an offset? | Sequential position of a message inside a partition |
| How does replication work? | One leader handles reads/writes; followers replicate; ISR list tracks who is in sync |
| What's a consumer group? | Set of consumers that together read a topic, each partition read by exactly one |
| What is the ISR? | In-Sync Replicas — followers that are not lagging behind the leader |
| What is `acks=all`? | Producer waits for leader + all ISR replicas to confirm — strongest durability |
| What is log compaction? | Keeps only the latest value per key — turns Kafka into a changelog/KV store |
| What is Schema Registry? | Central service to store and evolve Avro/Protobuf schemas; consumers reject incompatible payloads |

