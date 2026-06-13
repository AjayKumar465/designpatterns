# Transactional Outbox — Expert Playbook (Lead/Architect, Java, 10+ Years)

A comprehensive end-to-end reference covering the dual-write problem, event envelope design, SQL schema, Spring `@Transactional` writes, polling relay with `FOR UPDATE SKIP LOCKED`, exponential backoff, CDC/Debezium comparison, consumer idempotency, Spring Boot relay schedulers, metrics, alerts, production runbooks, and lead-level interview preparation. Every section includes production-grade Java examples and real-world debugging strategies.

---

## Table of Contents

1. [Why the Outbox Exists — The Dual-Write Problem](#section-1-why-the-outbox-exists--the-dual-write-problem)
2. [Real Production Scenarios and Business Impact](#section-2-real-production-scenarios-and-business-impact)
3. [Event Envelope Design and Schema Versioning](#section-3-event-envelope-design-and-schema-versioning)
4. [Outbox Table Schema, Indexes, and Lifecycle](#section-4-outbox-table-schema-indexes-and-lifecycle)
5. [Writing Domain Data + Outbox in One Transaction (Spring Boot)](#section-5-writing-domain-data--outbox-in-one-transaction-spring-boot)
6. [Polling Relay — FOR UPDATE SKIP LOCKED Deep Dive](#section-6-polling-relay--for-update-skip-locked-deep-dive)
7. [Backoff, Retry, and Terminal Failure Policies](#section-7-backoff-retry-and-terminal-failure-policies)
8. [CDC / Debezium Relay — Architecture and Comparison](#section-8-cdc--debezium-relay--architecture-and-comparison)
9. [Consumer Idempotency and Side-Effect Safety](#section-9-consumer-idempotency-and-side-effect-safety)
10. [Spring Boot Relay Scheduler and Production Wiring](#section-10-spring-boot-relay-scheduler-and-production-wiring)
11. [Kafka Integration — Headers, Keys, and Publish Semantics](#section-11-kafka-integration--headers-keys-and-publish-semantics)
12. [Ordering, Partition Keys, and Event Sequencing](#section-12-ordering-partition-keys-and-event-sequencing)
13. [Monitoring, Metrics, and Alerting](#section-13-monitoring-metrics-and-alerting)
14. [Production Issue Runbook](#section-14-production-issue-runbook)
15. [Common Production Pitfalls and Anti-Patterns](#section-15-common-production-pitfalls-and-anti-patterns)
16. [Testing Strategy — Unit, Integration, Chaos](#section-16-testing-strategy--unit-integration-chaos)
17. [Production Readiness Checklist](#section-17-production-readiness-checklist)
18. [Quick Reference — Interview Answer Templates](#section-18-quick-reference--interview-answer-templates)
19. [Lead Interview Questions — Logical and Production Scenarios](#section-19-lead-interview-questions--logical-and-production-scenarios)
20. [How to Talk About the Transactional Outbox Pattern in an Interview](#how-to-talk-about-the-transactional-outbox-pattern-in-an-interview)

---

## Section 1: Why the Outbox Exists — The Dual-Write Problem

### 1.1 The Core Problem

Any service that both **persists local state** and **publishes an event** faces a dual-write problem. These are two separate systems with no native atomic boundary:

```
                    ┌─────────────────┐
  Business Logic ──►│  Local Database │──► Commit succeeds
                    └─────────────────┘
                              │
                              ▼
                    ┌─────────────────┐
                    │  Message Broker │──► Publish succeeds or fails independently
                    └─────────────────┘
```

**Failure mode A — DB commits, broker fails:**

1. Order saved as `PLACED` in PostgreSQL.
2. Kafka producer throws `TimeoutException`.
3. Downstream `payment-service` and `inventory-service` never receive `OrderPlaced`.
4. Customer sees a confirmed order; warehouse never reserves stock.

**Failure mode B — Broker succeeds, DB rolls back:**

1. Kafka publish succeeds.
2. Database transaction fails on constraint violation or deadlock.
3. Downstream systems process an event for data that does not exist.
4. Inventory reserved for a ghost order.

**Failure mode C — Partial visibility during crash:**

1. Application writes to DB, begins Kafka publish, JVM crashes mid-flight.
2. On restart, you cannot know whether the message was delivered.
3. Retrying the publish risks duplicates; skipping risks data loss.

### 1.2 What the Outbox Guarantees (and What It Does Not)

| Guarantee | Outbox Provides? |
|-----------|------------------|
| Business write and event record are atomic | **Yes** — same DB transaction |
| Event will eventually reach the broker | **Yes** — relay retries until success or terminal failure |
| Exactly-once delivery to broker | **No** — at-least-once to broker is the realistic target |
| Exactly-once side effects at consumer | **No** — requires consumer idempotency |
| Ordering across unrelated aggregates | **No** — only per `partition_key` if designed correctly |

The outbox is a **producer-side consistency** pattern. It does not replace consumer deduplication.

### 1.3 Outbox vs Alternatives

| Approach | Atomicity | Complexity | When to Use |
|----------|-----------|------------|-------------|
| **Transactional Outbox** | Strong (same DB TX) | Medium | Default choice for microservices with local DB |
| **2PC / XA transactions** | Strong across DB + broker | High, fragile | Rarely — poor broker support, operational pain |
| **Saga + compensation** | Eventual | High | Long-running workflows, not a replacement for outbox |
| **Change Data Capture only** | Eventual (WAL lag) | Medium-High | High throughput, platform team owns Connect |
| **Fire-and-forget after commit** | None | Low | Never in production for critical events |

### 1.4 Mental Model

Think of the outbox table as a **durable send queue owned by the service**:

```
┌──────────────────────────────────────────────────────────────┐
│  @Transactional                                              │
│  ┌─────────────┐    ┌──────────────────┐                       │
│  │ orders row  │ +  │ outbox_event row │  ──► single commit  │
│  └─────────────┘    └──────────────────┘                       │
└──────────────────────────────────────────────────────────────┘
                              │
                              ▼ (async, separate process)
                    ┌─────────────────┐
                    │  Relay Worker   │──► Kafka / RabbitMQ / SQS
                    └─────────────────┘
```

The reference demo in this repo (`examples/outbox/OutboxDemo.java`) models this flow in memory: `OrderService.createOrderAndOutbox()` simulates the atomic write, `OutboxRelay.pollAndPublishOnce()` simulates the relay, and `IdempotentConsumer` demonstrates duplicate handling.

---

## Section 2: Real Production Scenarios and Business Impact

### Scenario 1: E-commerce Order Placement

**Flow:**

1. `order-service` writes `orders` row (`PLACED`) and outbox event `OrderPlaced.v1` in one transaction.
2. Relay publishes `OrderPlaced` to Kafka topic `orders.events`.
3. `payment-service` and `inventory-service` consume and act.
4. If Kafka is down, order write still succeeds; event is delayed but not lost.

**Business impact:**

- Customer order is durable immediately — no "order lost" support tickets.
- Dependent systems converge eventually — inventory and payment catch up within SLO.
- Operations monitors `outbox_oldest_pending_age_seconds` instead of running data repair scripts.

**Lead-level talking point:** The outbox trades **synchronous coupling** (request thread waits for Kafka) for **controlled async lag** with observability. You define an SLO like "99% of events published within 30 seconds" and alert when violated.

### Scenario 2: Payment Capture in a Saga

**Flow:**

1. `payment-service` captures payment and writes outbox `PaymentCaptured.v1`.
2. Saga orchestrator consumes and triggers shipment.
3. Relay crashes after publishing but before marking `SENT` — same event is re-published.
4. Orchestrator dedupes by `event_id` and proceeds once.

**Business impact:**

- No missed saga transitions — payment capture always produces a durable event.
- Duplicate events do not produce duplicate shipments — idempotent consumer is mandatory.

**Lead-level talking point:** In sagas, the outbox is the **reliable handoff** between local transaction boundaries and distributed workflow steps. Without it, you need compensating transactions for "payment captured but event never sent."

### Scenario 3: User Profile Update + Search Index Sync

**Flow:**

1. `profile-service` updates canonical user profile in PostgreSQL.
2. Outbox event `UserProfileUpdated.v1` written in same transaction.
3. Search-index updater consumes and updates Elastic/OpenSearch index.
4. Index lag is tolerated; correctness is maintained.

**Business impact:**

- Source of truth remains the relational DB.
- Read models catch up asynchronously and safely.
- Reindex jobs can replay from Kafka without touching the write path.

### Scenario 4: Audit Trail and Compliance

**Flow:**

1. Financial transaction recorded in `ledger_entries`.
2. Outbox event `LedgerEntryPosted.v1` with immutable payload snapshot.
3. Compliance pipeline consumes for immutable audit store.

**Business impact:**

- Regulatory requirement: "every state change produces an auditable event."
- Outbox ensures the audit event cannot exist without the ledger entry.

### Scenario 5: Multi-Tenant SaaS — Tenant Isolation

**Flow:**

1. Each tenant's events carry `tenant_id` in envelope headers.
2. Relay publishes with `tenant_id` header for downstream routing/filtering.
3. Consumer applies tenant-scoped idempotency keys.

**Business impact:**

- Cross-tenant duplicate handling is impossible if `event_id` is globally unique.
- Tenant-level backlog monitoring via `tenant_id` label on metrics.

---

## Section 3: Event Envelope Design and Schema Versioning

### 3.1 Minimum Envelope Fields

Every outbox row and published message must carry enough metadata for routing, deduplication, tracing, and schema evolution:

| Field | Type | Purpose |
|-------|------|---------|
| `event_id` | UUID | Immutable, globally unique — consumer dedup key |
| `event_type` | String | Semantic name with version suffix (`OrderPlaced.v1`) |
| `aggregate_type` | String | Entity type (`Order`, `Payment`, `User`) |
| `aggregate_id` | String | Entity identifier — used for debugging and replay |
| `partition_key` | String | Broker ordering key — usually `aggregate_id` |
| `payload` | JSON/Avro/Protobuf | Business data — immutable after insert |
| `headers` | JSON map | `trace_id`, `tenant_id`, `schema_version`, `correlation_id` |
| `occurred_at` | Timestamp | When the business event happened (domain time) |
| `created_at` | Timestamp | When the outbox row was inserted (system time) |

**Hard rules:**

1. **Never mutate payload after insert.** Corrections require a new event (`OrderCancelled.v1`), not an update.
2. **`event_id` is generated at write time**, not at relay time.
3. **`event_type` includes version** — consumers bind handlers to `OrderPlaced.v1`, not `OrderPlaced`.
4. **`partition_key` is explicit** — do not derive it implicitly at relay time from payload parsing.

### 3.2 Java Event Envelope Model

```java
public record EventEnvelope(
    UUID eventId,
    String eventType,
    String aggregateType,
    String aggregateId,
    String partitionKey,
    String payloadJson,
    Map<String, String> headers,
    Instant occurredAt
) {
    public static EventEnvelope pending(
            String aggregateType,
            String aggregateId,
            String eventType,
            Object payload,
            Map<String, String> headers,
            ObjectMapper mapper) {
        try {
            return new EventEnvelope(
                UUID.randomUUID(),
                eventType,
                aggregateType,
                aggregateId,
                aggregateId, // default: order per aggregate
                mapper.writeValueAsString(payload),
                headers,
                Instant.now()
            );
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Cannot serialize payload for " + eventType, e);
        }
    }
}
```

### 3.3 Payload Design Guidelines

**Do:**

- Include only fields downstream consumers need — not the entire entity graph.
- Use stable field names — `customerId`, not `cust_id` in one version and `customer_id` in another.
- Include `schema_version` in headers for consumer routing.
- Keep payloads small (< 4KB ideal, < 64KB hard limit for most teams).

**Do not:**

- Put PII you do not need to broadcast (use tokenized references).
- Embed large blobs (PDFs, images) — use reference IDs to object storage.
- Include computed fields that consumers could derive — reduces coupling.

### 3.4 Schema Versioning Strategy

| Strategy | Description | When |
|----------|-------------|------|
| **Explicit version in event_type** | `OrderPlaced.v1`, `OrderPlaced.v2` | Default — simple, explicit |
| **Schema Registry (Avro/Protobuf)** | BACKWARD/FULL transitive compatibility | High-volume, many consumers |
| **Upscaling consumers first** | Deploy consumer that handles v1 + v2, then deploy producer v2 | Zero-downtime migrations |

**Breaking change checklist:**

1. Add new event type version (`OrderPlaced.v2`).
2. Deploy consumers that handle both v1 and v2.
3. Switch producer to v2.
4. Monitor v1 consumption until zero.
5. Deprecate v1 handler after retention window.

### 3.5 Headers for Cross-Cutting Concerns

```java
Map<String, String> headers = Map.of(
    "traceId",     MDC.get("traceId"),
    "tenantId",    TenantContext.getCurrentTenantId(),
    "schemaVersion", "1",
    "sourceService", "order-service",
    "correlationId", command.correlationId()
);
```

Relay must copy all headers to Kafka record headers unchanged. Losing `trace_id` breaks distributed tracing across the async boundary.

---

## Section 4: Outbox Table Schema, Indexes, and Lifecycle

### 4.1 Production DDL (PostgreSQL)

```sql
create table outbox_event (
  event_id           uuid primary key,
  aggregate_type     varchar(100) not null,
  aggregate_id       varchar(100) not null,
  event_type         varchar(200) not null,
  payload            jsonb not null,
  headers            jsonb not null default '{}',
  status             varchar(20) not null default 'PENDING',
  partition_key      varchar(100) not null,
  created_at         timestamptz not null default now(),
  occurred_at        timestamptz not null default now(),
  published_at       timestamptz,
  retry_count        int not null default 0,
  next_retry_at      timestamptz,
  last_error         text,
  constraint chk_outbox_status check (status in ('PENDING', 'SENT', 'FAILED'))
);

-- Relay fetch: PENDING rows due for retry, ordered FIFO
create index idx_outbox_relay_fetch
  on outbox_event (status, next_retry_at, created_at)
  where status = 'PENDING';

-- Debugging / replay by aggregate
create index idx_outbox_aggregate
  on outbox_event (aggregate_type, aggregate_id, created_at);

-- Ops dashboard: terminal failures
create index idx_outbox_failed
  on outbox_event (status, created_at)
  where status = 'FAILED';
```

### 4.2 Index Design Rationale

| Index | Serves |
|-------|--------|
| `idx_outbox_relay_fetch` (partial) | Relay `SELECT ... FOR UPDATE SKIP LOCKED` — only scans hot `PENDING` rows |
| `idx_outbox_aggregate` | Support queries: "show all events for order X" |
| `idx_outbox_failed` | On-call dashboard for terminal failures needing manual replay |

**Partial index on `PENDING`** keeps the relay query fast even when the table has millions of archived `SENT` rows.

### 4.3 Status State Machine

```
                    ┌──────────┐
         insert ──► │ PENDING  │◄──── retry (transient error)
                    └────┬─────┘
                         │ publish OK
                         ▼
                    ┌──────────┐
                    │   SENT   │  (terminal — archive candidate)
                    └──────────┘

                    ┌──────────┐
         max retries│  FAILED  │  (terminal — page on-call, manual replay)
                    └──────────┘
```

### 4.4 Archival and Retention

**Problem:** Unbounded `SENT` rows bloat the table, slow vacuum, increase backup size.

**Solutions:**

| Strategy | Description |
|----------|-------------|
| **Partition by month** | `outbox_event_2026_06` — detach/drop old partitions |
| **Archive job** | Nightly: `INSERT INTO outbox_event_archive SELECT ... WHERE status='SENT' AND published_at < now() - interval '7 days'` then `DELETE` |
| **TTL on SENT only** | Keep `PENDING`/`FAILED` indefinitely until resolved |

**Recommended retention:**

- `SENT`: 7–30 days (enough for debugging and replay verification).
- `FAILED`: indefinite until manually resolved.
- `PENDING`: should never be old — if it is, you have an incident.

### 4.5 Multi-Database Considerations

| Database | `FOR UPDATE SKIP LOCKED` | Notes |
|----------|--------------------------|-------|
| PostgreSQL 9.5+ | Supported | Production default |
| MySQL 8.0+ | `FOR UPDATE SKIP LOCKED` | Supported |
| SQL Server | `READPAST` hint | Different syntax, same concept |
| Oracle | `FOR UPDATE SKIP LOCKED` | Supported 11g+ |
| H2 (tests) | Limited | Use Testcontainers with PostgreSQL for integration tests |

---

## Section 5: Writing Domain Data + Outbox in One Transaction (Spring Boot)

### 5.1 The Golden Rule

> **Never call the Kafka producer inside the business `@Transactional` method.**

The entire point of the outbox is decoupling the broker from the request transaction. A Kafka call inside the transaction reintroduces the dual-write problem with extra steps.

### 5.2 JPA Entity Model

```java
@Entity
@Table(name = "outbox_event")
public class OutboxEventEntity {

    @Id
    private UUID eventId;

    @Column(nullable = false)
    private String aggregateType;

    @Column(nullable = false)
    private String aggregateId;

    @Column(nullable = false)
    private String eventType;

    @Column(columnDefinition = "jsonb", nullable = false)
    @JdbcTypeCode(SqlTypes.JSON)
    private String payload;

    @Column(columnDefinition = "jsonb", nullable = false)
    @JdbcTypeCode(SqlTypes.JSON)
    private Map<String, String> headers;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private OutboxStatus status = OutboxStatus.PENDING;

    @Column(nullable = false)
    private String partitionKey;

    @Column(nullable = false)
    private Instant createdAt = Instant.now();

    @Column(nullable = false)
    private Instant occurredAt = Instant.now();

    private Instant publishedAt;
    private int retryCount = 0;
    private Instant nextRetryAt;
    private String lastError;

    public static OutboxEventEntity pending(EventEnvelope envelope) {
        OutboxEventEntity entity = new OutboxEventEntity();
        entity.eventId = envelope.eventId();
        entity.aggregateType = envelope.aggregateType();
        entity.aggregateId = envelope.aggregateId();
        entity.eventType = envelope.eventType();
        entity.payload = envelope.payloadJson();
        entity.headers = envelope.headers();
        entity.partitionKey = envelope.partitionKey();
        entity.occurredAt = envelope.occurredAt();
        return entity;
    }
}
```

### 5.3 Service Layer — Atomic Write

```java
@Service
public class OrderService {

    private final OrderRepository orderRepository;
    private final OutboxEventRepository outboxRepository;
    private final ObjectMapper objectMapper;

    @Transactional
    public OrderId placeOrder(PlaceOrderCommand cmd) {
        Order order = orderRepository.save(Order.place(cmd));

        EventEnvelope envelope = EventEnvelope.pending(
            "Order",
            order.getId().toString(),
            "OrderPlaced.v1",
            Map.of(
                "orderId", order.getId().toString(),
                "customerId", order.getCustomerId().toString(),
                "totalAmount", order.getTotalAmount().toPlainString()
            ),
            Map.of(
                "traceId", TraceContext.currentTraceId(),
                "tenantId", TenantContext.getCurrentTenantId(),
                "schemaVersion", "1"
            ),
            objectMapper
        );

        outboxRepository.save(OutboxEventEntity.pending(envelope));
        return order.getId();
    }
}
```

This mirrors `OutboxDemo.java`:

```java
// examples/outbox/OutboxDemo.java — OrderService.createOrderAndOutbox()
void createOrderAndOutbox(String orderId) {
    Order order = new Order(orderId, "CREATED");
    store.orders.add(order);  // domain write

    OutboxEvent event = new OutboxEvent(orderId, "OrderCreated", payload);
    store.outbox.add(event);  // outbox write — same simulated transaction

    System.out.println("TX COMMIT: order + outbox event id=" + event.eventId);
}
```

### 5.4 Transaction Propagation Pitfalls

| Mistake | Consequence | Fix |
|---------|-------------|-----|
| `@Transactional(propagation = REQUIRES_NEW)` for outbox | Outbox can commit when main TX rolls back | Use default `REQUIRED` — same transaction |
| Outbox save in `@Async` method | Different thread, different transaction | Save outbox synchronously in caller's TX |
| `@TransactionalEventListener(AFTER_COMMIT)` for outbox | Not atomic — event can be lost if crash between commit and listener | Write outbox in same TX, not after commit |
| Self-invocation bypassing proxy | `@Transactional` not applied | Inject self or move to separate bean |

### 5.5 Multiple Events in One Transaction

```java
@Transactional
public void cancelOrderAndRefund(OrderId orderId) {
    Order order = orderRepository.findById(orderId).orElseThrow();
    order.cancel();
    orderRepository.save(order);

    outboxRepository.save(OutboxEventEntity.pending(
        envelope("OrderCancelled.v1", order)));
    outboxRepository.save(OutboxEventEntity.pending(
        envelope("RefundRequested.v1", order)));
}
```

Both events commit or neither does. Relay publishes them independently — ordering is only guaranteed if they share the same `partition_key`.

### 5.6 Outbox in the Same Schema vs Separate Schema

| Approach | Pros | Cons |
|----------|------|------|
| Same schema as domain tables | Simplest TX, no 2PC | Relay poll adds load to OLTP DB |
| Separate schema, same DB | Logical separation | Still same DB connection pool |
| Separate database | Isolates relay load | **Cannot** use single local TX — avoid |

**Lead recommendation:** Same schema, same database, partial indexes. Move to CDC when relay poll load becomes measurable on OLTP.

---

## Section 6: Polling Relay — FOR UPDATE SKIP LOCKED Deep Dive

### 6.1 Why Polling with Row Locking

The relay must satisfy four properties simultaneously:

1. **Concurrency-safe** — multiple relay instances can run without double-publishing the same row.
2. **Crash-safe** — if relay dies mid-batch, rows become available again after lock release.
3. **FIFO-ish** — oldest `PENDING` events publish first.
4. **Non-blocking** — one relay must not wait for another's locks.

`SELECT ... FOR UPDATE SKIP LOCKED` achieves all four on PostgreSQL.

### 6.2 Fetch Query

```sql
select event_id, aggregate_type, aggregate_id, event_type,
       payload, headers, partition_key, retry_count
from outbox_event
where status = 'PENDING'
  and (next_retry_at is null or next_retry_at <= now())
order by created_at
for update skip locked
limit 200;
```

**What `SKIP LOCKED` does:**

- Instance A locks rows 1–200.
- Instance B's query skips locked rows, picks up 201–400.
- No blocking, no duplicate processing.

**What happens on crash:**

- Instance A holds locks until TX commit or connection drop.
- Connection drop → PostgreSQL releases locks → rows remain `PENDING`.
- Another instance picks them up. If A had published but not committed `SENT`, event is re-published (at-least-once).

### 6.3 Relay Batch Flow

```
┌─────────────────────────────────────────────────────────────┐
│ BEGIN TRANSACTION                                           │
│   1. SELECT ... FOR UPDATE SKIP LOCKED (batch)              │
│   2. FOR EACH row:                                          │
│        a. Publish to Kafka                                  │
│        b. On success: SET status='SENT', published_at=now() │
│        c. On failure: SET retry_count++, next_retry_at, error │
│   3. COMMIT                                                 │
└─────────────────────────────────────────────────────────────┘
```

**Critical design choice — per-row vs batch commit:**

| Strategy | Pros | Cons |
|----------|------|------|
| **Single TX for entire batch** | Fewer commits | One poison row blocks whole batch commit |
| **Sub-batch per row** | Isolated failures | More commits, slightly lower throughput |
| **Publish outside TX, mark inside** | Shorter lock duration | Must handle publish-without-mark race |

**Production recommendation:** Publish inside the open transaction's lock scope but commit status per-row using savepoints, or keep batches small (50–100) with all-or-nothing and short TX duration.

### 6.4 Java Relay Implementation (JdbcTemplate)

```java
@Repository
public class OutboxRelayRepository {

    private final JdbcTemplate jdbc;

    public List<OutboxEventEntity> fetchPendingBatch(int batchSize) {
        String sql = """
            select event_id, aggregate_type, aggregate_id, event_type,
                   payload::text, headers::text, partition_key, retry_count
            from outbox_event
            where status = 'PENDING'
              and (next_retry_at is null or next_retry_at <= now())
            order by created_at
            for update skip locked
            limit ?
            """;
        return jdbc.query(sql, outboxRowMapper, batchSize);
    }

    public void markSent(UUID eventId) {
        jdbc.update("""
            update outbox_event
            set status = 'SENT', published_at = now(), last_error = null
            where event_id = ?
            """, eventId);
    }

    public void markRetry(UUID eventId, int retryCount, Instant nextRetryAt, String error) {
        jdbc.update("""
            update outbox_event
            set retry_count = ?, next_retry_at = ?, last_error = ?
            where event_id = ?
            """, retryCount, Timestamp.from(nextRetryAt), truncate(error, 2000), eventId);
    }

    public void markFailed(UUID eventId, String error) {
        jdbc.update("""
            update outbox_event
            set status = 'FAILED', last_error = ?
            where event_id = ?
            """, truncate(error, 2000), eventId);
    }
}
```

### 6.5 Mapping to OutboxDemo.java

The demo's relay is intentionally simplified — no locking, no backoff:

```java
// examples/outbox/OutboxDemo.java — OutboxRelay.pollAndPublishOnce()
void pollAndPublishOnce() {
    for (OutboxEvent e : store.outbox) {
        if (e.status == Status.SENT) continue;
        try {
            e.attempts++;
            broker.publish(e);
            e.status = Status.SENT;
        } catch (RuntimeException ex) {
            // stays PENDING — will retry on next poll
        }
    }
}
```

Production adds: `FOR UPDATE SKIP LOCKED`, exponential backoff via `next_retry_at`, `FAILED` terminal state, and multi-instance safety.

### 6.6 Throughput Tuning

| Parameter | Starting Value | Tuning Direction |
|-----------|----------------|------------------|
| Batch size | 100–200 | Increase if publish latency is low |
| Poll interval | 1–5 seconds | Decrease for lower latency, increases DB load |
| Relay instances | 2–4 | Scale horizontally; `SKIP LOCKED` distributes work |
| Kafka `linger.ms` | 5–10ms | Batch broker requests in relay |

**Back-of-envelope:** 200 rows/batch × 4 instances × 1 poll/sec = 800 events/sec. Sufficient for most OLTP services. Beyond ~2K/sec, evaluate CDC.

---

## Section 7: Backoff, Retry, and Terminal Failure Policies

### 7.1 Error Classification

| Error Type | Examples | Action |
|------------|----------|--------|
| **Transient** | Broker timeout, `NotLeaderForPartition`, DNS blip | Retry with backoff, stay `PENDING` |
| **Permanent** | `SerializationException`, unknown topic, ACL denied | Mark `FAILED` immediately, page on-call |
| **Quota / throttle** | `ThrottlingQuotaExceededException` | Retry with longer backoff |
| **Payload too large** | `RecordTooLargeException` | `FAILED` — fix payload, manual replay |

### 7.2 Exponential Backoff with Jitter

```java
public final class OutboxRetryPolicy {

    private static final Duration BASE_DELAY = Duration.ofSeconds(1);
    private static final Duration MAX_DELAY = Duration.ofMinutes(15);
    private static final int MAX_ATTEMPTS = 8;
    private static final Duration JITTER = Duration.ofMillis(500);
    private static final ThreadLocalRandom RANDOM = ThreadLocalRandom.current();

    public static Instant nextRetryAt(int retryCount) {
        long exponentialMs = BASE_DELAY.toMillis() * (1L << Math.min(retryCount, 10));
        long cappedMs = Math.min(exponentialMs, MAX_DELAY.toMillis());
        long jitterMs = RANDOM.nextLong(JITTER.toMillis() + 1);
        return Instant.now().plusMillis(cappedMs + jitterMs);
    }

    public static boolean isExhausted(int retryCount) {
        return retryCount >= MAX_ATTEMPTS;
    }
}
```

**Delay schedule (approximate):**

| Attempt | Base Delay | With Jitter |
|---------|------------|-------------|
| 1 | 1s | 1–1.5s |
| 2 | 2s | 2–2.5s |
| 3 | 4s | 4–4.5s |
| 4 | 8s | 8–8.5s |
| 5 | 16s | 16–16.5s |
| 6 | 32s | 32–32.5s |
| 7 | 64s | 64–64.5s |
| 8 | 128s → capped 15m | 15m |

### 7.3 Relay Error Handling

```java
public void processEvent(OutboxEventEntity event) {
    try {
        kafkaOutboxPublisher.publish(event);
        relayRepository.markSent(event.getEventId());
        meterRegistry.counter("outbox.publish.success").increment();
    } catch (TransientPublishException ex) {
        int newCount = event.getRetryCount() + 1;
        if (OutboxRetryPolicy.isExhausted(newCount)) {
            relayRepository.markFailed(event.getEventId(), ex.getMessage());
            meterRegistry.counter("outbox.publish.failed_terminal").increment();
        } else {
            relayRepository.markRetry(
                event.getEventId(), newCount,
                OutboxRetryPolicy.nextRetryAt(newCount),
                ex.getMessage()
            );
            meterRegistry.counter("outbox.publish.retry").increment();
        }
    } catch (PermanentPublishException ex) {
        relayRepository.markFailed(event.getEventId(), ex.getMessage());
        meterRegistry.counter("outbox.publish.failed_terminal").increment();
    }
}
```

### 7.4 FAILED Event Manual Replay Runbook

1. Query: `SELECT * FROM outbox_event WHERE status = 'FAILED' ORDER BY created_at DESC;`
2. Inspect `last_error` — fix root cause (topic ACL, schema, payload size).
3. Reset for replay:
   ```sql
   update outbox_event
   set status = 'PENDING', retry_count = 0, next_retry_at = null, last_error = null
   where event_id = '...';
   ```
4. Monitor relay picks it up within one poll cycle.
5. Verify consumer processed correctly.
6. Document in incident ticket.

**Lead note:** Build an admin API or internal tool for step 3 — never ask on-call to run raw SQL under pressure.

---

## Section 8: CDC / Debezium Relay — Architecture and Comparison

### 8.1 How CDC Outbox Works

```
┌──────────────┐     WAL/binlog     ┌──────────────┐     ┌─────────┐
│  Application │──►│  PostgreSQL  │──►│   Debezium   │──►│  Kafka  │
│  (write TX)  │   │ outbox_event │   │  Connector   │   │         │
└──────────────┘     └──────────────┘     └──────────────┘     └─────────┘
```

1. Application inserts domain row + outbox row in one TX.
2. PostgreSQL writes to WAL.
3. Debezium reads WAL (logical replication slot).
4. Debezium emits change event to Kafka.
5. **Separate** Debezium Outbox Event Router SMT transforms outbox row into proper Kafka message.

### 8.2 Debezium Outbox Event Router (SMT)

```json
{
  "transforms": "outbox",
  "transforms.outbox.type": "io.debezium.transforms.outbox.EventRouter",
  "transforms.outbox.table.field.event.id": "event_id",
  "transforms.outbox.table.field.event.key": "partition_key",
  "transforms.outbox.table.field.event.type": "event_type",
  "transforms.outbox.table.field.event.payload": "payload",
  "transforms.outbox.route.by.field": "aggregate_type",
  "transforms.outbox.route.topic.replacement": "${routedByValue}.events"
}
```

This routes `aggregate_type = Order` to topic `Order.events` with Kafka key = `partition_key`.

### 8.3 Polling vs CDC — Decision Matrix

| Dimension | Polling Relay | CDC (Debezium) |
|-----------|---------------|----------------|
| **Latency** | Poll interval (1–5s typical) | Sub-second (WAL tail) |
| **Throughput ceiling** | ~1–3K events/sec per service | 10K+ with tuned Connect |
| **Operational ownership** | App team — a `@Scheduled` bean | Platform team — Kafka Connect cluster |
| **DB load** | Periodic `SELECT` queries | Replication slot — WAL retention pressure |
| **Multi-instance safety** | `SKIP LOCKED` | Single connector per table (usually) |
| **Status tracking** | Native (`PENDING`/`SENT`/`FAILED`) | Requires custom status column or offset tracking |
| **Retry control** | Full app-level backoff | Depends on Connect error tolerance |
| **Debugging** | Query outbox table directly | Connector logs + Kafka Connect REST API |
| **Exactly-once to Kafka** | At-least-once | At-least-once (idempotent Connect 2.0 helps) |

### 8.4 When to Start with Polling

- Team owns the service end-to-end.
- Throughput < 1K events/sec.
- No Kafka Connect platform yet.
- You want `PENDING`/`SENT`/`FAILED` visibility in SQL.

**Quote for interviews:** "I start with polling because it's one `@Scheduled` bean, one table, and my on-call can debug it with SQL. When publish volume or latency SLO pushes me past ~2K events/sec or sub-second delivery, I move the relay to Debezium and keep the same outbox table schema."

### 8.5 When to Use CDC

- Platform team runs Kafka Connect reliably (HA workers, monitoring, upgrades).
- Sub-second event delivery is an SLO.
- Many services share the same outbox infrastructure.
- DB team can manage replication slots and WAL retention.

### 8.6 CDC-Specific Pitfalls

| Pitfall | Impact | Mitigation |
|---------|--------|------------|
| **Replication slot lag** | WAL accumulates, disk fills | Monitor `pg_replication_slots`, alert on lag |
| **No FAILED state** | Poison events loop in Connect | Dead letter queue in Connect + custom error handler |
| **Schema changes** | Connector breaks on DDL | Use Debezium schema change events, coordinate migrations |
| **Multi-tenant table bloat** | Connector replays all tenants | Route by `aggregate_type` or `tenant_id` header |
| **Deleting SENT rows** | No effect on CDC — it reads WAL at insert time | CDC does not re-read deleted rows; archival is safe |

### 8.7 Hybrid Approach

Some teams run **polling as fallback** when CDC is down:

1. Primary: Debezium streams inserts.
2. Fallback: Polling relay processes rows still `PENDING` after N minutes.
3. Idempotent consumer handles duplicates from both paths.

Complexity is high — only for critical financial paths.

---

## Section 9: Consumer Idempotency and Side-Effect Safety

### 9.1 Why Consumer Idempotency Is Non-Negotiable

The outbox relay delivers **at-least-once** to the broker:

1. Relay publishes to Kafka → success.
2. Relay crashes before `markSent()`.
3. Restart → same event published again.

The demo proves this explicitly:

```java
// examples/outbox/OutboxDemo.java
consumer.onEvent(e);
consumer.onEvent(e);  // duplicate — must be dropped
// Output: DUPLICATE DROPPED eventId=...
```

**Outbox solves producer consistency. Consumer idempotency solves effect consistency.**

### 9.2 Idempotency Store Design

```sql
create table processed_event (
  event_id   uuid primary key,
  consumer   varchar(100) not null,
  processed_at timestamptz not null default now()
);

create index idx_processed_event_ttl
  on processed_event (processed_at);
```

**Consumer flow:**

```
1. Receive message with event_id header
2. BEGIN TX
3. INSERT INTO processed_event (event_id, consumer) — ON CONFLICT DO NOTHING
4. If insert count = 0 → duplicate → COMMIT → ack offset
5. Apply side effects (update inventory, call API, etc.)
6. COMMIT TX
7. Acknowledge Kafka offset (after commit)
```

### 9.3 Spring Kafka Idempotent Consumer

```java
@KafkaListener(topics = "orders.events", groupId = "inventory-service")
@Transactional
public void onOrderPlaced(ConsumerRecord<String, String> record) {
    String eventId = header(record, "event_id");
    if (!processedEventRepository.tryInsert(eventId, "inventory-service")) {
        meterRegistry.counter("consumer.duplicate.dropped").increment();
        return;
    }

    OrderPlacedPayload payload = objectMapper.readValue(record.value(), OrderPlacedPayload.class);
    inventoryService.reserveStock(payload.orderId(), payload.items());
}
```

```java
@Repository
public class ProcessedEventRepository {
    public boolean tryInsert(String eventId, String consumer) {
        int inserted = jdbc.update("""
            insert into processed_event (event_id, consumer)
            values (?::uuid, ?)
            on conflict (event_id) do nothing
            """, eventId, consumer);
        return inserted == 1;
    }
}
```

### 9.4 Idempotency Strategies by Effect Type

| Side Effect | Idempotency Approach |
|-------------|---------------------|
| DB insert | Natural key unique constraint (`order_id` in target table) |
| DB update | Upsert with version check (`WHERE version = expected`) |
| External API call | Idempotency-Key header with `event_id` |
| Email/notification | Dedup table — never send twice for same `event_id` |
| Financial debit | Ledger with unique `event_id` constraint — double-entry |

### 9.5 Offset Commit Ordering

**Wrong:**

```java
// Ack BEFORE processing — message loss on crash
ack.acknowledge();
processEvent(record);
```

**Correct:**

```java
// Process in TX, ack AFTER commit
processEventInTransaction(record);
ack.acknowledge();
```

With Spring Kafka: `AckMode.RECORD` or `MANUAL_IMMEDIATE` after transactional listener completes.

### 9.6 Natural Key vs event_id Dedup

| Method | Pros | Cons |
|--------|------|------|
| `event_id` dedup table | Works for all event types | Extra table, grows forever |
| Natural key (`order_id + event_type`) | No extra table if business key is unique | Fails if same action produces two legitimate events |
| Idempotent broker + consumer | Defense in depth | Still need consumer dedup for relay duplicates |

**Lead recommendation:** Always use `event_id`. Natural keys are a bonus, not a replacement.

---

## Section 10: Spring Boot Relay Scheduler and Production Wiring

### 10.1 Relay Scheduler

```java
@Component
@ConditionalOnProperty(name = "outbox.relay.enabled", havingValue = "true", matchIfMissing = true)
public class OutboxRelayScheduler {

    private final OutboxRelayService relayService;
    private final OutboxRelayProperties properties;

    @Scheduled(fixedDelayString = "${outbox.relay.poll-interval-ms:2000}")
    public void pollAndPublish() {
        if (!properties.isEnabled()) return;
        relayService.processBatch(properties.getBatchSize());
    }
}
```

### 10.2 Relay Service with Transaction Boundary

```java
@Service
public class OutboxRelayService {

    private final OutboxRelayRepository relayRepository;
    private final OutboxKafkaPublisher publisher;
    private final PlatformTransactionManager txManager;

    public void processBatch(int batchSize) {
        TransactionTemplate tx = new TransactionTemplate(txManager);
        tx.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        tx.executeWithoutResult(status -> {
            List<OutboxEventEntity> batch = relayRepository.fetchPendingBatch(batchSize);
            for (OutboxEventEntity event : batch) {
                processEvent(event);
            }
        });
    }
}
```

`REQUIRES_NEW` ensures each poll cycle is an independent transaction — locks are released on commit.

### 10.3 Configuration Properties

```java
@ConfigurationProperties(prefix = "outbox.relay")
public record OutboxRelayProperties(
    boolean enabled,
    int batchSize,
    long pollIntervalMs,
    int maxAttempts,
    Duration maxDelay
) {
    public OutboxRelayProperties {
        if (batchSize <= 0) batchSize = 100;
        if (pollIntervalMs <= 0) pollIntervalMs = 2000;
        if (maxAttempts <= 0) maxAttempts = 8;
        if (maxDelay == null) maxDelay = Duration.ofMinutes(15);
    }
}
```

```yaml
# application.yml
outbox:
  relay:
    enabled: true
    batch-size: 100
    poll-interval-ms: 2000
    max-attempts: 8
    max-delay: 15m

spring:
  kafka:
    producer:
      acks: all
      enable-idempotence: true
      retries: 2147483647
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.apache.kafka.common.serialization.StringSerializer
```

### 10.4 Leader Election (Optional)

When you cannot rely on `SKIP LOCKED` (e.g., legacy DB), use ShedLock:

```java
@Scheduled(fixedDelayString = "${outbox.relay.poll-interval-ms:2000}")
@SchedulerLock(name = "outboxRelay", lockAtMostFor = "PT5M", lockAtLeastFor = "PT1S")
public void pollAndPublish() {
    relayService.processBatch(properties.getBatchSize());
}
```

With `SKIP LOCKED`, ShedLock is optional — horizontal scaling is safe without it.

### 10.5 Graceful Shutdown

```java
@PreDestroy
public void shutdown() {
    // Spring @Scheduled stops scheduling new polls
    // In-flight batch completes via TX commit or rollback
    log.info("Outbox relay shutting down — in-flight batch will complete or roll back");
}
```

Ensure `server.shutdown=graceful` and sufficient `spring.lifecycle.timeout-per-shutdown-phase` for in-flight batches.

### 10.6 Separate Deployment vs Embedded

| Model | When |
|-------|------|
| **Embedded** — relay in same JVM as API | Default for polling; simplest ops |
| **Sidecar** — relay in separate pod, same DB | Scale relay independently from API |
| **CDC** — no app relay | Debezium in Connect cluster |

---

## Section 11: Kafka Integration — Headers, Keys, and Publish Semantics

### 11.1 Publishing from Relay

```java
@Component
public class OutboxKafkaPublisher {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final OutboxTopicResolver topicResolver;

    public void publish(OutboxEventEntity event) {
        String topic = topicResolver.resolve(event.getEventType(), event.getAggregateType());

        ProducerRecord<String, String> record = new ProducerRecord<>(
            topic,
            event.getPartitionKey(),
            event.getPayload()
        );

        record.headers().add("event_id", uuidToBytes(event.getEventId()));
        record.headers().add("event_type", event.getEventType().getBytes(StandardCharsets.UTF_8));
        record.headers().add("aggregate_type", event.getAggregateType().getBytes(StandardCharsets.UTF_8));
        record.headers().add("aggregate_id", event.getAggregateId().getBytes(StandardCharsets.UTF_8));

        event.getHeaders().forEach((k, v) ->
            record.headers().add(k, v.getBytes(StandardCharsets.UTF_8)));

        kafkaTemplate.send(record).get(30, TimeUnit.SECONDS);
    }
}
```

### 11.2 Producer Configuration for Outbox Relay

```properties
acks=all
enable.idempotence=true
retries=2147483647
delivery.timeout.ms=120000
max.in.flight.requests.per.connection=5
compression.type=lz4
```

**Why idempotent producer still does not eliminate consumer dedup:**

- Idempotent producer deduplicates **producer retries within a single producer session**.
- Outbox relay re-publish after crash is a **new producer session** sending the same `event_id`.
- Consumer must still dedup by `event_id`.

### 11.3 Topic Naming

| Pattern | Example | Use |
|---------|---------|-----|
| Per domain | `orders.events` | Consumers need all order events |
| Per event type | `order.placed` | Fine-grained subscription |
| Debezium routed | `Order.events` | CDC outbox router default |

Include `event_type` in headers regardless — consumers use it for dispatch.

### 11.4 Dead Letter for Terminal Failures

When an event is `FAILED`, optionally publish to `outbox.dlq` for tooling:

```java
public void publishToDlq(OutboxEventEntity event) {
    kafkaTemplate.send("outbox.dlq", event.getEventId().toString(), event.getPayload());
}
```

Consumers should **not** process DLQ automatically — it's for human investigation and replay tooling.

---

## Section 12: Ordering, Partition Keys, and Event Sequencing

### 12.1 Ordering Guarantee Scope

Kafka orders messages **per partition**. The outbox `partition_key` becomes the Kafka record key.

**Correct:**

```java
// All events for order-123 go to same partition
partitionKey = order.getId().toString();
```

**Incorrect:**

```java
// Random key — events for same order can reorder
partitionKey = UUID.randomUUID().toString();
```

### 12.2 Multiple Events per Aggregate in One Transaction

If `OrderPlaced` and `OrderLineItemsAdded` are written in the same TX:

1. Both have `partition_key = orderId`.
2. Relay publishes in `created_at` order within the batch.
3. Kafka preserves order within the partition.

**Risk:** If relay publishes `OrderLineItemsAdded` before `OrderPlaced` due to separate poll cycles (different insert times), order is preserved by `created_at` ordering in relay fetch.

### 12.3 Cross-Aggregate Ordering

Outbox does **not** guarantee that `PaymentCaptured` (payment aggregate) arrives before `OrderShipped` (order aggregate) unless you design choreography to tolerate out-of-order or use a saga waiting for both.

**Lead answer:** "Ordering is per aggregate, per partition. Cross-aggregate ordering is a saga design concern, not an outbox concern."

### 12.4 Head-of-Line Blocking

If one event for partition key `order-123` is stuck in `PENDING` (broker down), subsequent events for `order-123` in the same partition may be delivered first (they were published in earlier successful polls). Consumers must tolerate out-of-order across different event types for the same aggregate, or you serialize per aggregate in relay (lower throughput).

---

## Section 13: Monitoring, Metrics, and Alerting

### 13.1 Core Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `outbox_pending_count` | Gauge | `SELECT count(*) WHERE status='PENDING'` |
| `outbox_oldest_pending_age_seconds` | Gauge | `now() - min(created_at) WHERE status='PENDING'` |
| `outbox_publish_success_total` | Counter | Successful relay publishes |
| `outbox_publish_failure_total` | Counter | Transient failures (will retry) |
| `outbox_publish_retry_total` | Counter | Retry attempts |
| `outbox_failed_terminal_total` | Counter | Events marked `FAILED` |
| `outbox_relay_batch_size` | Histogram | Rows per poll cycle |
| `outbox_relay_duration_seconds` | Histogram | End-to-end batch processing time |
| `outbox_publish_latency_seconds` | Histogram | Kafka `send().get()` duration |
| `consumer_duplicate_dropped_total` | Counter | Idempotent skips at consumer |

### 13.2 Micrometer Implementation

```java
@Scheduled(fixedRate = 30_000)
public void recordOutboxGauges() {
    long pending = jdbc.queryForObject(
        "select count(*) from outbox_event where status = 'PENDING'", Long.class);
    meterRegistry.gauge("outbox.pending.count", pending);

    Optional<Instant> oldest = jdbc.query(
        "select min(created_at) from outbox_event where status = 'PENDING'",
        rs -> rs.next() ? Optional.of(rs.getTimestamp(1).toInstant()) : Optional.empty()
    );
    oldest.ifPresent(inst -> meterRegistry.gauge(
        "outbox.oldest.pending.age.seconds",
        Duration.between(inst, Instant.now()).toSeconds()
    ));
}
```

### 13.3 Prometheus / PromQL Examples

```promql
# Pending backlog
outbox_pending_count

# Backlog growth rate
deriv(outbox_pending_count[5m])

# Publish failure ratio
rate(outbox_publish_failure_total[5m])
  / (rate(outbox_publish_success_total[5m]) + rate(outbox_publish_failure_total[5m]))

# p99 publish latency
histogram_quantile(0.99, rate(outbox_publish_latency_seconds_bucket[5m]))

# Time to drain backlog (estimate)
outbox_pending_count
  / rate(outbox_publish_success_total[5m])
```

### 13.4 Alert Rules

| Alert | Condition | Severity | Action |
|-------|-----------|----------|--------|
| **OutboxLagHigh** | `outbox_oldest_pending_age_seconds > 120` for 10m | Warning | Check broker health, relay pods |
| **OutboxBacklogGrowing** | `deriv(outbox_pending_count[10m]) > 0` sustained | Warning | Scale relay, check publish latency |
| **OutboxPublishFailureRate** | Failure rate > 5% over 5m | Critical | Broker incident, ACL, topic missing |
| **OutboxTerminalFailure** | `increase(outbox_failed_terminal_total[5m]) > 0` | Critical | Manual replay required |
| **OutboxRelayStalled** | `rate(outbox_publish_success_total[10m]) == 0` AND pending > 0 | Critical | Relay not running or DB unreachable |
| **ConsumerDuplicateSpike** | `rate(consumer_duplicate_dropped_total[5m])` anomaly | Warning | Relay double-publish — check crash loops |

### 13.5 Dashboard Panels

1. **Pending count** — time series, should be near zero in steady state.
2. **Oldest pending age** — single stat with red/yellow/green thresholds.
3. **Publish throughput** — success vs failure stacked.
4. **Retry histogram** — are events cycling through retries?
5. **FAILED events table** — linked to runbook.
6. **End-to-end latency** — `created_at` to `published_at` for SENT rows (SLI).

### 13.6 SLO Example

| SLI | Target | Measurement |
|-----|--------|-------------|
| Publish latency | 99% < 30s | `published_at - created_at` for SENT rows |
| Backlog | pending_count < 50 steady state | Gauge |
| Durability | 0 events lost | `FAILED` count + audit |

---

## Section 14: Production Issue Runbook

### Issue 1: Outbox Backlog Growing — `outbox_pending_count` Increasing

**Diagnosis:** `deriv(outbox_pending_count[10m]) > 0` sustained.

**Steps:**

1. Check Kafka broker health — `UnderReplicatedPartitions`, produce latency.
2. Check relay pod logs — exceptions, thread pool exhaustion.
3. Check `outbox_publish_latency_seconds` p99 — broker slow or network?
4. Check DB — is relay fetch query slow? `EXPLAIN ANALYZE` on fetch query.
5. Scale relay instances horizontally (`SKIP LOCKED` safe).
6. Increase batch size temporarily if publish latency is low.
7. If producer rate exceeds relay capacity sustained → evaluate CDC migration.

### Issue 2: Oldest Pending Age > SLO

**Diagnosis:** `outbox_oldest_pending_age_seconds > 120` for 10+ minutes.

**Steps:**

1. Identify the stuck row(s):
   ```sql
   select event_id, event_type, created_at, retry_count, next_retry_at, last_error
   from outbox_event
   where status = 'PENDING'
   order by created_at
   limit 20;
   ```
2. If `next_retry_at` is in the future — event is in backoff; check `last_error`.
3. If `next_retry_at` is null and row is old — relay may be stalled (Issue 5).
4. If single row blocking — check for poison payload (`RecordTooLargeException`).
5. Mark poison as `FAILED` and fix payload; reset healthy rows.

### Issue 3: Terminal FAILED Events

**Diagnosis:** `increase(outbox_failed_terminal_total[5m]) > 0`.

**Steps:**

1. Pull failed events from DB or alert payload.
2. Classify `last_error`:
   - `UnknownTopicOrPartitionException` → create topic, reset to `PENDING`.
   - `RecordTooLargeException` → fix payload, split event, manual replay.
   - `SerializationException` → permanent; fix schema.
   - `AuthorizationException` → fix ACL, reset to `PENDING`.
3. After fix: `UPDATE status='PENDING', retry_count=0, next_retry_at=null`.
4. Verify consumer idempotency handles replay safely.
5. Post-incident: add alert for this error class.

### Issue 4: Duplicate Processing Spike at Consumer

**Diagnosis:** `consumer_duplicate_dropped_total` rate increases.

**Steps:**

1. Check relay pods for crash loop — `kubectl get pods`, restart counts.
2. Check if relay marks `SENT` after publish — race in TX logic?
3. Verify consumer dedup is working (duplicates should be **dropped**, not double-processed).
4. If double-processing occurs (not just duplicates detected) — consumer idempotency is broken. **Stop consumer**, fix, replay from known offset.

### Issue 5: Relay Not Running

**Diagnosis:** `rate(outbox_publish_success_total[10m]) == 0` AND `outbox_pending_count > 0`.

**Steps:**

1. Check `outbox.relay.enabled` property — accidental disable in config?
2. Check `@EnableScheduling` is present.
3. Check DB connectivity from relay pod.
4. Check thread pool — `@Scheduled` blocked by long-running batch?
5. Restart relay pods; monitor first successful `outbox_publish_success_total`.

### Issue 6: Database Lock Contention on Outbox Table

**Diagnosis:** Relay fetch query slow; `pg_locks` shows contention.

**Steps:**

1. Verify `SKIP LOCKED` is in query — without it, relays block each other.
2. Reduce batch size to shorten lock hold time.
3. Reduce relay instance count if too many concurrent pollers.
4. Check for long-running open transactions holding locks on outbox rows.
5. Ensure partial index `WHERE status = 'PENDING'` exists.

### Issue 7: WAL / Replication Slot Bloat (CDC)

**Diagnosis:** PostgreSQL disk growing; `pg_replication_slots` lag increasing.

**Steps:**

1. Check Debezium connector status — is it running?
2. Check `confirmed_flush_lsn` vs current LSN.
3. If connector stopped — restart immediately; WAL accumulates fast.
4. Set `wal_keep_size` appropriately; alert on slot lag.
5. Never drop replication slot without understanding WAL replay implications.

### Issue 8: Post-Deployment Event Flood

**Diagnosis:** Pending count spikes after deployment.

**Steps:**

1. Check if migration reset statuses incorrectly.
2. Check if new code writes more events per request.
3. Check if relay batch size or poll interval changed.
4. Temporary: scale relay instances.
5. Long-term: rate-limit outbox inserts or batch domain writes.

---

## Section 15: Common Production Pitfalls and Anti-Patterns

### 15.1 Publishing from Request Transaction

**Anti-pattern:**

```java
@Transactional
public void placeOrder(PlaceOrderCommand cmd) {
    orderRepository.save(order);
    kafkaTemplate.send("orders.events", ...);  // WRONG
}
```

**Why it fails:** Kafka is not a transaction manager participant (without rare XA). Rollback leaves message sent; commit + Kafka failure loses message.

**Fix:** Outbox only.

### 15.2 Treating Outbox as Optional Fallback

**Anti-pattern:** "Try Kafka first, write to outbox if Kafka fails."

**Why it fails:** Kafka success + DB rollback = ghost event. Asymmetric failure handling.

**Fix:** Outbox is the **only** publish path.

### 15.3 Missing Consumer Dedup

**Anti-pattern:** "We use idempotent Kafka producer, so consumers don't need dedup."

**Why it fails:** Outbox relay re-publish after crash is outside producer idempotence scope.

**Fix:** `event_id` dedup at every consumer. See `OutboxDemo.java` `IdempotentConsumer`.

### 15.4 Unbounded Outbox Growth

**Anti-pattern:** Never archiving `SENT` rows.

**Why it fails:** Table bloat, slow relay scans (even with partial index), backup pain.

**Fix:** Archive job or monthly partitions.

### 15.5 Bad Partition Key

**Anti-pattern:** `partition_key = UUID.randomUUID()` or null.

**Why it fails:** Related events reorder; consumers see `OrderShipped` before `OrderPlaced`.

**Fix:** `partition_key = aggregate_id` by default.

### 15.6 Mutating Outbox Payload

**Anti-pattern:** Updating `payload` column after insert to "fix" data.

**Why it fails:** CDC may have already emitted old payload; audit trail corrupted.

**Fix:** New compensating event.

### 15.7 No Alerting on Pending Age

**Anti-pattern:** Only alert on `FAILED` count.

**Why it fails:** Slow broker degradation shows as growing pending age long before terminal failure.

**Fix:** Alert on `outbox_oldest_pending_age_seconds`.

### 15.8 `@TransactionalEventListener(AFTER_COMMIT)` as Outbox

**Anti-pattern:** Using after-commit listener to publish to Kafka.

**Why it fails:** Crash after DB commit but before listener runs = lost event. Not atomic with business write in the sense outbox provides.

**Fix:** Write outbox row in same TX.

### 15.9 Giant Outbox Payloads

**Anti-pattern:** Storing full entity graph (50KB+) in outbox.

**Why it fails:** `RecordTooLargeException`, slow relay, DB bloat.

**Fix:** Reference IDs; consumers fetch details if needed.

### 15.10 Running Relay Without Integration Tests

**Anti-pattern:** Only unit testing order service; never testing relay + consumer path.

**Why it fails:** Schema drift, header loss, and locking bugs appear in production.

**Fix:** Testcontainers test: write → relay → Kafka → consumer dedup.

---

## Section 16: Testing Strategy — Unit, Integration, Chaos

### 16.1 Unit Tests — Write Path

```java
@Test
void placeOrder_persistsOrderAndOutboxInSameTransaction() {
    PlaceOrderCommand cmd = PlaceOrderCommand.builder()...build();
    orderService.placeOrder(cmd);

    assertThat(orderRepository.findAll()).hasSize(1);
    assertThat(outboxRepository.findByStatus(PENDING)).hasSize(1);
    assertThat(outboxRepository.findAll().get(0).getEventType()).isEqualTo("OrderPlaced.v1");
}
```

### 16.2 Integration Test — Full Pipeline (Testcontainers)

```java
@SpringBootTest
@Testcontainers
class OutboxIntegrationTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16");

    @Container
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.0"));

    @Test
    void orderPlacement_eventReachesKafkaAndConsumerDedupes() {
        orderService.placeOrder(command);

        await().atMost(30, SECONDS).untilAsserted(() ->
            assertThat(kafkaConsumer.poll(Duration.ofSeconds(1))).isNotEmpty()
        );

        // Simulate relay crash duplicate
        // Second poll should not double-process
    }
}
```

### 16.3 Relay Tests — Locking and Concurrency

```java
@Test
void twoRelayInstances_doNotProcessSameRow() throws Exception {
    insertPendingEvents(10);

    ExecutorService executor = Executors.newFixedThreadPool(2);
    executor.submit(() -> relayService.processBatch(10));
    executor.submit(() -> relayService.processBatch(10));
    executor.shutdown();
    executor.awaitTermination(30, SECONDS);

    assertThat(countSent()).isEqualTo(10);  // no double publish
}
```

### 16.4 Chaos Scenarios

| Scenario | Expected Behavior |
|----------|-------------------|
| Kill relay mid-batch | Rows remain `PENDING`, re-published on restart |
| Kafka unavailable for 5 min | Pending grows, no FAILED until max retries |
| DB failover | Relay pauses, resumes without duplicate if consumer dedupes |
| Duplicate delivery | Consumer drops via `event_id` |

### 16.5 Contract Tests

Use Pact or schema compatibility checks to verify:

1. `event_type` values match consumer handlers.
2. Payload fields are backward compatible.
3. Headers include `trace_id` and `tenant_id`.

---

## Section 17: Production Readiness Checklist

| # | Category | Check |
|---|----------|-------|
| 1 | **Write path** | Domain + outbox in same `@Transactional`, no Kafka in TX |
| 2 | **Envelope** | `event_id`, `event_type` versioned, `partition_key`, headers |
| 3 | **Schema** | Partial index on `PENDING`, archival strategy for `SENT` |
| 4 | **Relay** | `FOR UPDATE SKIP LOCKED`, configurable batch size and poll interval |
| 5 | **Retry** | Exponential backoff + jitter, terminal `FAILED` state |
| 6 | **Consumer** | `event_id` idempotency, offset commit after processing |
| 7 | **Kafka** | `acks=all`, idempotent producer, `event_id` in headers |
| 8 | **Ordering** | `partition_key = aggregate_id` verified |
| 9 | **Metrics** | pending count, oldest age, publish success/failure, FAILED count |
| 10 | **Alerts** | Lag, failure rate, terminal failures, relay stalled |
| 11 | **Runbook** | FAILED replay procedure documented and tested |
| 12 | **Testing** | Testcontainers integration test for write → relay → consume |
| 13 | **CDC decision** | Documented threshold for polling → Debezium migration |
| 14 | **Security** | Outbox payloads contain no excess PII |
| 15 | **Graceful shutdown** | Relay completes or rolls back in-flight batch |

---

## Section 18: Quick Reference — Interview Answer Templates

**Q: What problem does the Transactional Outbox solve?**
A: The dual-write problem — you need to save business data and publish an event atomically. The outbox writes both in one database transaction; a relay publishes asynchronously. See [Section 1](#section-1-why-the-outbox-exists--the-dual-write-problem).

**Q: Does the outbox guarantee exactly-once delivery?**
A: No. It guarantees at-least-once to the broker. Consumers must deduplicate by `event_id`. See [Section 9](#section-9-consumer-idempotency-and-side-effect-safety).

**Q: Why FOR UPDATE SKIP LOCKED?**
A: Multiple relay instances can poll concurrently without blocking each other or processing the same row. See [Section 6](#section-6-polling-relay--for-update-skip-locked-deep-dive).

**Q: Polling vs CDC — when do you switch?**
A: Start with polling for simplicity. Move to Debezium when you need sub-second latency or >2K events/sec and have a platform team running Connect. See [Section 8](#section-8-cdc--debezium-relay--architecture-and-comparison).

**Q: What happens if relay crashes after publish but before markSent?**
A: Event is re-published on restart. Consumer dedupes by `event_id`. See `examples/outbox/OutboxDemo.java`.

**Q: Can you call Kafka inside the @Transactional method?**
A: Never. It reintroduces the dual-write problem. See [Section 5.1](#51-the-golden-rule).

**Q: How do you handle FAILED events?**
A: Fix root cause, reset status to `PENDING`, let relay replay. Consumer idempotency makes replay safe. See [Section 7.4](#74-failed-event-manual-replay-runbook).

**Q: What metrics do you alert on?**
A: `outbox_oldest_pending_age_seconds`, pending count growth, publish failure rate, any terminal FAILED event. See [Section 13](#section-13-monitoring-metrics-and-alerting).

---

## Section 19: Lead Interview Questions — Logical and Production Scenarios

This section contains 30 questions designed for lead/architect interviews. Each answer references playbook sections for deep-dive study.

---

### Category A: Architecture and Design Decisions

**Q1: When would you choose Transactional Outbox over Kafka transactions (consume-transform-produce)?**

A: Outbox is for **domain services with a local database** that need to emit events after a write. Kafka transactions are for **stream processing** where read and write are both Kafka topics. If your source of truth is PostgreSQL, outbox is the correct pattern — you cannot atomically commit to PostgreSQL and Kafka without outbox or XA. Kafka transactions do not participate in your DB transaction. See [Section 1.3](#13-outbox-vs-alternatives).

**Q2: Should the outbox table live in the same database as the domain tables?**

A: **Yes**, in the same database and schema (or at minimum same DB instance) so a single local transaction covers both writes. A separate database requires distributed transactions — avoid. The relay poll load on OLTP is manageable with partial indexes until you hit high throughput, then CDC offloads the poll. See [Section 5.6](#56-outbox-in-the-same-schema-vs-separate-schema).

**Q3: How do you handle multiple events from one business transaction?**

A: Insert multiple outbox rows in the same `@Transactional` method. Each gets its own `event_id`. Use the same `partition_key` if consumers need ordering per aggregate. Relay publishes each independently. See [Section 5.5](#55-multiple-events-in-one-transaction).

**Q4: How do you version events without breaking consumers?**

A: Version in `event_type` (`OrderPlaced.v1` → `OrderPlaced.v2`). Deploy consumers that handle both versions. Switch producer. Deprecate v1 after retention window. For Avro/Protobuf, enforce Schema Registry compatibility. See [Section 3.4](#34-schema-versioning-strategy).

**Q5: Is the outbox pattern compatible with Event Sourcing?**

A: Partially. Event sourcing stores events as the source of truth; outbox stores events as a **side effect of a state mutation**. You can combine them: domain events are the authoritative log, and outbox is the delivery mechanism to external systems. Do not use outbox as your event store — it is a transport reliability pattern, not a domain model. See [Section 1.2](#12-what-the-outbox-guarantees-and-what-it-does-not).

**Q6: How do you design outbox for multi-tenant SaaS?**

A: Include `tenant_id` in envelope headers. Use globally unique `event_id` (UUID). Metrics labeled by `tenant_id` for noisy-neighbor detection. Consumer dedup table can include `consumer` column but `event_id` alone is sufficient for uniqueness. Consider per-tenant FAILED alerting thresholds. See [Scenario 5](#scenario-5-multi-tenant-saas--tenant-isolation).

---

### Category B: Relay Implementation

**Q7: Why not use `SELECT FOR UPDATE` without `SKIP LOCKED`?**

A: Without `SKIP LOCKED`, relay instance B blocks waiting for instance A's locks. Throughput drops to serial. With multiple instances, you want parallel batch processing, not lock queues. `SKIP LOCKED` lets B skip A's locked rows and process the next available batch. See [Section 6.1](#61-why-polling-with-row-locking).

**Q8: Should publish happen inside or outside the database transaction?**

A: **Inside the lock scope** (row is locked) but be aware of long TX risk. Best practice: keep batches small (50–100), publish with timeout, update status, commit quickly. Publishing outside TX without lock risks two relays picking up the same row — prevented by `SKIP LOCKED` only while TX is open. See [Section 6.3](#63-relay-batch-flow).

**Q9: How do you scale the polling relay horizontally?**

A: Add more relay instances (pods). `FOR UPDATE SKIP LOCKED` distributes rows automatically. No leader election required. Tune batch size and poll interval so combined throughput exceeds producer insert rate. See [Section 6.6](#66-throughput-tuning).

**Q10: What batch size do you recommend and why?**

A: Start with 100–200. Larger batches improve throughput but hold locks longer and increase blast radius on failure. Measure `outbox_relay_duration_seconds` and DB lock wait time. If p99 relay duration < 1s, try increasing batch size. See [Section 6.6](#66-throughput-tuning).

**Q11: How do you prevent the relay from overwhelming Kafka during recovery?**

A: Use backoff on individual row failures, cap relay instances during recovery, and optionally rate-limit publishes (token bucket in relay). Monitor broker `RequestsPerSec`. Consider temporary `batch.size` reduction on Kafka producer. See [Section 7](#section-7-backoff-retry-and-terminal-failure-policies).

---

### Category C: CDC and Debezium

**Q12: What is the Debezium Outbox Event Router SMT?**

A: A Single Message Transform that reads raw outbox table CDC events and routes them to Kafka topics based on `aggregate_type`, setting the message key from `partition_key` and body from `payload`. It saves you from writing custom relay code. See [Section 8.2](#82-debezium-outbox-event-router-smt).

**Q13: How do you track SENT/FAILED status with CDC?**

A: CDC emits on **insert** — it does not wait for relay status. Options: (1) treat CDC as the relay and do not use `PENDING`/`SENT` status (event is "sent" when CDC processes insert); (2) hybrid with polling for status tracking; (3) update status in a separate post-delivery consumer. Most CDC deployments treat insert-to-WAL as the handoff and skip `SENT` marking. See [Section 8.3](#83-polling-vs-cdc--decision-matrix).

**Q14: What happens to PostgreSQL WAL when Debezium falls behind?**

A: WAL segments are retained until the replication slot catches up. If Debezium is down long enough, WAL disk fills, PostgreSQL may halt writes. This is a **P0 platform alert**. See [Section 8.6](#86-cdc-specific-pitfalls) and [Issue 7](#issue-7-wal--replication-slot-bloat-cdc).

**Q15: Can you run polling and CDC simultaneously?**

A: Yes, as a migration or hybrid strategy, but consumers must dedup by `event_id` because both paths may deliver the same event. Complexity is high — use only for critical paths during CDC rollout. See [Section 8.7](#87-hybrid-approach).

---

### Category D: Consumer Idempotency

**Q16: Why isn't idempotent Kafka producer enough?**

A: Idempotent producer prevents duplicate **broker appends within a single producer ID session** when the producer retries a send. Outbox relay crash creates a **new publish attempt** for the same `event_id` — semantically a new delivery. Consumer must dedup. See [Section 11.2](#112-producer-configuration-for-outbox-relay).

**Q17: How do you implement idempotency for external API calls?**

A: Pass `event_id` as `Idempotency-Key` header. Store `event_id` in a local `processed_event` table before calling the API. If duplicate, skip the call. See [Section 9.4](#94-idempotency-strategies-by-effect-type).

**Q18: What if the consumer processes the event but crashes before offset commit?**

A: Kafka redelivers the message. `processed_event` insert returns conflict → duplicate dropped → no double side effect. This is why dedup check and side effect must be in the same consumer transaction. See [Section 9.5](#95-offset-commit-ordering).

**Q19: Should you use Redis or DB for the dedup store?**

A: **Database** (same as consumer's write DB) for ACID guarantees — dedup insert and side effect in one TX. Redis is faster but creates a separate consistency boundary. Use Redis only if side effects are idempotent by natural key and you accept Redis failure modes. See [Section 9.2](#92-idempotency-store-design).

**Q20: How does the repo demo illustrate consumer idempotency?**

A: `OutboxDemo.java` calls `consumer.onEvent(e)` twice for the same event. The second call prints `DUPLICATE DROPPED` because `processedEventIds.add()` returns false. See `examples/outbox/OutboxDemo.java` lines 107–117 and 136–140.

---

### Category E: Production Troubleshooting

**Q21: Pending count is low but consumers report missing events. What do you check?**

A: (1) Events stuck in `FAILED` state — query terminal failures. (2) Relay publishing to wrong topic — ACL or config mismatch. (3) Consumer lag on different topic. (4) Events in `PENDING` with future `next_retry_at` — still in backoff. (5) CDC connector stopped but polling relay disabled. See [Section 14](#section-14-production-issue-runbook).

**Q22: After a PostgreSQL failover, relay publishes duplicates. Is this expected?**

A: Yes. Failover may replay uncommitted or re-read rows. Consumer dedup handles it. Verify `processed_event` table is on the same failed-over DB. See [Section 9](#section-9-consumer-idempotency-and-side-effect-safety).

**Q23: One poison event blocks the entire batch. How do you fix the design?**

A: Use savepoints per row or process each event in its own short transaction within the poll cycle. Mark poison as `FAILED` immediately (permanent error) so it does not retry forever. See [Section 6.3](#63-relay-batch-flow) and [Section 7.1](#71-error-classification).

**Q24: How do you replay events after fixing a consumer bug?**

A: Reset Kafka consumer offset to the desired point (or use a new consumer group with `earliest`). Consumer `processed_event` table will block re-processing unless you delete relevant `event_id` rows (dangerous) or use a new consumer group name with a fresh dedup table. Prefer: fix bug, deploy, let new events flow; for historical replay, use a dedicated replay tool that bypasses dedup with explicit operator approval. See [Section 7.4](#74-failed-event-manual-replay-runbook).

**Q25: How do you test the outbox pattern in CI?**

A: Testcontainers with PostgreSQL + Kafka. Write order, wait for relay to publish, assert Kafka record, simulate duplicate delivery, assert consumer dedup. See [Section 16](#section-16-testing-strategy--unit-integration-chaos).

---

### Category F: Advanced and Curveball

**Q26: Can you use outbox with MongoDB?**

A: Yes, if you use MongoDB multi-document transactions (replica set required). Insert domain document and outbox document in one session transaction. Relay polls similarly. CDC options exist (Debezium MongoDB connector) but are less mature than PostgreSQL.

**Q27: How does outbox compare to AWS SQS/SNS post-commit?**

A: Same dual-write problem applies. Outbox with SQS as target works: relay polls outbox and calls `sqs.sendMessage()`. For Lambda triggers, idempotency via `event_id` in message attributes is still required.

**Q28: What is the maximum sustainable throughput for polling relay?**

A: Roughly 1–3K events/sec per service with 4 relay instances, batch 200, 1s poll. Beyond that, DB poll load and Kafka producer throughput become bottlenecks — evaluate CDC. Exact number depends on payload size, DB hardware, and network. See [Section 6.6](#66-throughput-tuning).

**Q29: Your team wants to delete the outbox table and "just use Kafka." What do you say?**

A: Kafka is not your system of record unless you are fully event-sourced. If business state lives in PostgreSQL, you need a reliable bridge. Without outbox, you choose between data loss and ghost events. The outbox is that bridge. See [Section 1](#section-1-why-the-outbox-exists--the-dual-write-problem).

**Q30: How do you explain outbox to a product manager?**

A: "When a customer places an order, we save it and notify other systems. If we save the order but the notification fails, warehouses never see it. The outbox makes sure the notification is never lost — it's saved alongside the order and sent automatically, even if our messaging system is temporarily down. Customers see their order immediately; other systems catch up within seconds."

---

## Appendix: SQL Quick Reference

### Fetch pending batch

```sql
select * from outbox_event
where status = 'PENDING'
  and (next_retry_at is null or next_retry_at <= now())
order by created_at
for update skip locked
limit 200;
```

### Mark sent

```sql
update outbox_event
set status = 'SENT', published_at = now(), last_error = null
where event_id = :eventId;
```

### Reset failed for replay

```sql
update outbox_event
set status = 'PENDING', retry_count = 0, next_retry_at = null, last_error = null
where event_id = :eventId and status = 'FAILED';
```

### Ops: pending backlog snapshot

```sql
select status, count(*), min(created_at), max(created_at)
from outbox_event
group by status;
```

### Ops: oldest pending

```sql
select event_id, event_type, created_at,
       extract(epoch from (now() - created_at)) as age_seconds
from outbox_event
where status = 'PENDING'
order by created_at
limit 10;
```

---

## Appendix: Reference Example — OutboxDemo.java Walkthrough

The repository includes `examples/outbox/OutboxDemo.java` — a minimal in-memory demonstration of the full outbox lifecycle.

### Components

| Class | Role |
|-------|------|
| `OrderService` | Writes order + outbox event in one simulated transaction |
| `OutboxRelay` | Polls `PENDING` events, publishes to broker, marks `SENT` |
| `Broker` | Simulates Kafka; first publish fails to demonstrate retry |
| `IdempotentConsumer` | Dedupes by `event_id` using `HashSet` |

### Scenario executed in `main()`

1. `createOrderAndOutbox(orderId)` — prints `TX COMMIT: order + outbox event id=...`
2. First `pollAndPublishOnce()` — broker throws `Transient broker failure`; event stays `PENDING`
3. Second `pollAndPublishOnce()` — publish succeeds, status → `SENT`
4. Consumer receives event, processes once
5. Duplicate delivery → `DUPLICATE DROPPED`

### Mapping to production

| Demo | Production equivalent |
|------|----------------------|
| `InMemoryStore` | PostgreSQL with `outbox_event` table |
| `OrderService.createOrderAndOutbox` | `@Transactional` service method |
| `OutboxRelay.pollAndPublishOnce` | `@Scheduled` relay with `FOR UPDATE SKIP LOCKED` |
| `Broker.failNextPublish` | Transient Kafka `TimeoutException` |
| `IdempotentConsumer.processedEventIds` | `processed_event` table with `ON CONFLICT DO NOTHING` |

Run the demo:

```bash
cd examples/outbox
javac OutboxDemo.java && java OutboxDemo
```

Expected output:

```
=== Scenario: TX write + relay retry + idempotent consume ===
TX COMMIT: order + outbox event id=<uuid>
PUBLISH FAIL eventId=<uuid> attempts=1 reason=Transient broker failure
PUBLISH OK eventId=<uuid> attempts=2
CONSUMED eventId=<uuid> payload={orderId:...,status:CREATED}
DUPLICATE DROPPED eventId=<uuid>
```

---

## How to Talk About the Transactional Outbox Pattern in an Interview

> Short and clear. How you would actually say it.

---

### "What problem does the Outbox pattern solve?"

You need to save something to your database AND send a message to Kafka. These are two separate things.

If you save to the database and then Kafka goes down, the message never gets sent. The order exists but nobody knows about it.

If you send to Kafka first and then the database save fails, Kafka has a message for data that doesn't exist.

The outbox pattern fixes this. You write your data AND the message into your own database in the same transaction. Then a background job reads that message from the database and sends it to Kafka. The transaction makes sure both things happen together or not at all.

---

### "How does the background job work?"

The job checks the outbox table for messages that haven't been sent yet. It grabs a batch of rows, locks them so other instances don't grab the same ones — that's the `SKIP LOCKED` part — and sends each one to Kafka.

If Kafka is down, the message stays in the table. The job retries later with backoff.

If the job crashes after sending but before marking it as sent, it will send the same message again when it restarts. So the consumer might get the same message twice. That's fine — the consumer just needs to check "have I seen this event_id before?" and skip it if yes.

Our repo demo shows exactly this: `OutboxDemo.java` fails the first publish, succeeds the second, and the consumer drops the duplicate.

---

### "Why not just call Kafka inside the @Transactional method?"

Because they're not the same transaction. Your database can commit and Kafka can fail — or the other way around. You'd be back to the same dual-write problem.

The outbox keeps the write path simple: database only. Kafka is someone else's problem — the relay's job.

---

### "CDC vs polling relay — which do you use?"

Polling is simpler. Your job queries the table every few seconds. Easy to build, easy to debug with SQL, your team owns it completely.

CDC reads the database transaction log directly through Debezium. It reacts to changes in milliseconds. Faster, but you need Kafka Connect running reliably, and your platform team usually owns that.

I usually start with polling. If the volume gets high — say past a couple thousand events per second — or we need sub-second delivery, I switch to CDC. Same outbox table, different relay mechanism.

---

### "What metrics do you watch?"

Three big ones:

1. How many events are waiting to be sent — pending count.
2. How old is the oldest waiting event — if this goes past two minutes, something is wrong.
3. How many events hit terminal failure — any number above zero pages someone.

I also watch publish failure rate and duplicate drops at the consumer, because those tell you the relay is crash-looping or Kafka is unhappy.

---

### "What if an event goes to FAILED?"

Something is permanently wrong — maybe the topic doesn't exist, or the payload is too big. It won't fix itself on retry.

You fix the root cause, reset the row to PENDING, and let the relay try again. The consumer's idempotency check makes it safe to replay.

---

### Quick Answers

| Question | Say this |
|---|---|
| What problem does it solve? | You need to save data and publish an event atomically — this does both in one DB transaction |
| How does it work? | Write event to outbox table in same transaction, relay sends it to Kafka separately |
| What if relay crashes? | Message stays in table, relay retries on restart |
| Why handle duplicates on consumer side? | Relay can send the same message twice — consumer must ignore the second one by `event_id` |
| CDC or polling? | Polling for simplicity, CDC for speed and high volume |
| Can you call Kafka in @Transactional? | No — that reintroduces the dual-write problem |
| What is FOR UPDATE SKIP LOCKED? | Lets multiple relay instances grab different rows without blocking each other |
| What metrics matter most? | Pending count, oldest pending age, terminal FAILED count |
| Where is the demo? | `examples/outbox/OutboxDemo.java` in this repo |
| Does outbox give exactly-once? | No — at-least-once to broker; consumer dedup gives exactly-once effects |
