# Transactional Outbox Pattern: Production Playbook

## Why This Pattern Exists

Any service that both writes local state and publishes an event has a dual-write problem.

1. Database commit succeeds, event publish fails: downstream systems never learn about the change.
2. Event publish succeeds, database commit fails: downstream systems process an event for data that does not exist.

Transactional Outbox solves this by writing business data and event record in one local database transaction, then publishing asynchronously from the outbox table.

## Real Production Scenarios

### Scenario 1: E-commerce Order Placement

1. `order-service` writes `orders` row (`PLACED`) and outbox event `OrderPlaced`.
2. Relay publishes `OrderPlaced` to Kafka topic `orders.events`.
3. `payment-service` and `inventory-service` consume and act.
4. If Kafka is down, order write still succeeds; event is delayed but not lost.

Business impact:

1. Customer order is durable immediately.
2. Dependent systems converge eventually.
3. Operations can monitor backlog and recover without data repair scripts.

### Scenario 2: Payment Capture in a Saga

1. `payment-service` captures payment and writes outbox `PaymentCaptured`.
2. Saga orchestrator consumes and triggers shipment.
3. If relay crashes after publishing but before marking `SENT`, same event can be re-published.
4. Orchestrator dedupes by `event_id` and proceeds once.

Business impact:

1. No missed saga transitions.
2. Duplicate events do not produce duplicate shipments.

### Scenario 3: User Profile Update + Search Index Sync

1. `profile-service` updates canonical user profile.
2. Outbox event `UserProfileUpdated` is written in same transaction.
3. Search-index updater consumes and updates Elastic/OpenSearch index.
4. Index lag is tolerated; correctness is maintained.

Business impact:

1. Source of truth remains consistent.
2. Read models catch up asynchronously and safely.

---

## Step-by-Step Implementation

## Step 1: Define Event Envelope

Minimum fields for cross-service safety:

1. `event_id` (UUID, immutable, globally unique)
2. `event_type` (semantic name, versioned)
3. `aggregate_type` and `aggregate_id`
4. `partition_key` (ordering key)
5. `payload` (JSON/Avro/Protobuf)
6. `headers` (`trace_id`, `tenant_id`, schema version)
7. `occurred_at` and `created_at`

Rule:

1. Never mutate payload after insert.

## Step 2: Create Outbox Table

```sql
create table outbox_event (
  event_id           uuid primary key,
  aggregate_type     varchar(100) not null,
  aggregate_id       varchar(100) not null,
  event_type         varchar(200) not null,
  payload            jsonb not null,
  headers            jsonb not null,
  status             varchar(20) not null, -- PENDING|SENT|FAILED
  partition_key      varchar(100) not null,
  created_at         timestamptz not null default now(),
  occurred_at        timestamptz not null default now(),
  published_at       timestamptz,
  retry_count        int not null default 0,
  next_retry_at      timestamptz,
  last_error         text
);

create index idx_outbox_pending
  on outbox_event(status, next_retry_at, created_at);

create index idx_outbox_aggregate
  on outbox_event(aggregate_type, aggregate_id, created_at);
```

Operational note:

1. Keep only `PENDING` rows hot.
2. Archive or partition old `SENT` rows.

## Step 3: Write Domain Data + Outbox in One Transaction

```java
@Transactional
public OrderId placeOrder(PlaceOrderCommand cmd) {
    Order order = orderRepository.save(Order.place(cmd));

    OutboxEvent outboxEvent = OutboxEvent.pending(
        UUID.randomUUID(),
        "Order",
        order.getId().toString(),
        "OrderPlaced.v1",
        toJson(Map.of(
            "orderId", order.getId().toString(),
            "customerId", order.getCustomerId().toString(),
            "totalAmount", order.getTotalAmount().toString()
        )),
        Map.of(
            "traceId", currentTraceId(),
            "schemaVersion", "1"
        ),
        order.getId().toString()
    );

    outboxRepository.save(outboxEvent);
    return order.getId();
}
```

Hard rule:

1. Do not call Kafka producer from this transaction.

## Step 4: Implement Relay (Polling Pattern)

Batch flow:

1. Start DB transaction.
2. Select `PENDING` due rows using `FOR UPDATE SKIP LOCKED`.
3. Publish each event to Kafka with `event_id` header.
4. Mark `SENT` + `published_at` on success.
5. On failure, increment `retry_count`, set exponential `next_retry_at`, store `last_error`.
6. Commit and repeat.

Fetch query:

```sql
select *
from outbox_event
where status = 'PENDING'
  and (next_retry_at is null or next_retry_at <= now())
order by created_at
for update skip locked
limit 200;
```

## Step 5: Backoff and Terminal Failure Policy

Recommended retry policy:

1. Attempts `1..8` with exponential backoff + jitter.
2. Transient errors stay `PENDING`.
3. After max attempts, mark `FAILED` and page on-call.

Formula example:

1. `delay = min(base * 2^retry_count, max_delay) + jitter`
2. `base = 1s`, `max_delay = 15m`, jitter `0..500ms`

## Step 6: Consumer Idempotency (Non-Negotiable)

Consumer flow:

1. Check `event_id` in processed-events store.
2. If seen, acknowledge and skip side effects.
3. If new, apply side effects in transaction and store `event_id`.
4. Commit offset only after durable commit.

Outbox solves producer consistency, not consumer exactly-once effects.

## Step 7: Monitor and Operate

Track:

1. `outbox_pending_count`
2. `outbox_oldest_pending_age_seconds`
3. `outbox_publish_success_total`
4. `outbox_publish_failure_total`
5. `outbox_retry_count` histogram
6. `outbox_failed_terminal_count`
7. `consumer_duplicate_dropped_total`

Alert thresholds (example):

1. Oldest pending age > 120s for 10m.
2. Publish failures > 5% over 5m.
3. Any terminal failed event > 0.

---

## Relay Choice: Polling vs CDC

### Polling Relay

Use when:

1. You want simple ownership in service code.
2. Throughput is moderate.
3. Team is not operating Kafka Connect/Debezium yet.

### CDC Relay (Debezium Outbox)

Use when:

1. Throughput is high.
2. Central platform team already runs Connect reliably.
3. You want to avoid custom relay worker lifecycle management.

Tradeoff:

1. CDC reduces app code but increases platform dependency.

---

## Common Production Mistakes

1. Publishing from request transaction and treating outbox as optional fallback.
2. Missing consumer dedupe because "producer is idempotent".
3. Unbounded outbox growth with no archival strategy.
4. Bad partition key causing reordering across related events.
5. No alerting on pending age, only on error count.

---

## Minimal Readiness Checklist

1. Domain + outbox in same DB transaction.
2. Relay supports locking, retry, and backoff.
3. Consumer dedupe implemented and tested.
4. Pending-age SLO defined and alerted.
5. Replay runbook documented for `FAILED` events.
6. Event schema versioning strategy agreed.

## Reference Example in Repo

See `examples/outbox/OutboxDemo.java` for concept demonstration.

---

## How to Talk About the Transactional Outbox Pattern in an Interview (Human English)

---

### "What is the Transactional Outbox pattern and why does it exist?"

> "The problem it solves is called the dual-write problem. In a microservices system, a service often needs to do two things atomically: save data to its database AND publish an event to Kafka. But these are two separate systems — there's no global transaction spanning both. So if you write to the DB and then Kafka goes down, the event never gets published. Or you publish to Kafka first, and then the DB write fails — now downstream services processed an event for data that doesn't exist. The Outbox pattern solves this by writing the event record into the same database as your business data, in the same transaction. Then a separate relay process reads from that outbox table and publishes to Kafka. The DB transaction is your atomicity guarantee. The relay handles eventual delivery."

**Good analogy:**
> "Imagine writing a letter — you need to both record in your journal that you sent it AND actually mail it. If you mail it first and then your journal is lost, you don't know you sent it. If you write in the journal first and then forget to mail it, the recipient never gets it. The outbox pattern says: write the letter and the journal entry at the same time (one transaction), then separately go mail it."

---

### "What does the relay do and how does it handle failures?"

> "The relay polls the outbox table for `PENDING` records, publishes each to Kafka, and marks them `SENT`. It uses `FOR UPDATE SKIP LOCKED` so multiple relay instances don't double-process the same row. If Kafka is down, the event stays `PENDING` and the relay retries with exponential backoff. After a configurable number of retries, it marks the event `FAILED` and alerts ops. The consumer side must be idempotent because the relay may publish the same event twice (e.g., relay crashes after publishing but before marking `SENT`). The `event_id` header on every message lets consumers deduplicate."

---

### "CDC vs polling relay — which do you use?"

> "If the team already runs Debezium and Kafka Connect, CDC is cleaner — it reads the DB binlog (transaction log), so there's no polling latency and no extra queries on the main DB. But if you're a small team without platform infrastructure for Connect, a polling relay inside your own service is simpler to own and debug. I've built both. My default for a new service is polling relay — it's 50 lines of code and you own it. I graduate to CDC when the throughput outgrows polling or when a platform team takes over the infra."

---

### Quick Cheat Sheet

| Question | One-line answer |
|---|---|
| What problem does it solve? | Dual-write: writing to DB and publishing to Kafka atomically |
| How? | Write event to outbox table in same DB transaction; relay publishes asynchronously |
| What if relay fails? | Event stays PENDING, relay retries with backoff |
| Consumer idempotency needed? | Yes — relay can publish duplicates on crash/restart |
| CDC vs polling? | CDC = low latency, needs Debezium. Polling = simpler, good for moderate throughput |
| What to monitor? | Pending event age, retry count, terminal failures |

