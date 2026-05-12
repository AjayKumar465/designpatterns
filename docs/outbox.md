# Transactional Outbox Pattern (Production Reference)

## Problem

One service must do both:

1. Commit local business data
2. Publish integration event to Kafka

Naive dual write:

1. DB commit succeeds, Kafka publish fails -> downstream never sees state change
2. Kafka publish succeeds, DB commit fails -> downstream sees phantom event

## Core Idea

Persist domain data and outbound event in one local DB transaction.
A separate relay publishes outbox rows to Kafka and marks them delivered.

---

## Production Stack (Typical)

1. Spring Boot 3
2. PostgreSQL (domain + outbox table)
3. Kafka producer with idempotence enabled
4. Relay: polling worker or Debezium CDC
5. Avro/Protobuf + Schema Registry
6. Micrometer + OpenTelemetry for metrics/traces

---

## Recommended Outbox Table

```sql
create table outbox_event (
  event_id           uuid primary key,
  aggregate_type     varchar(100) not null,
  aggregate_id       varchar(100) not null,
  event_type         varchar(200) not null,
  payload            jsonb not null,
  headers            jsonb not null,
  status             varchar(20) not null, -- PENDING|SENT|FAILED
  partition_key      varchar(100) not null, -- often aggregate_id or saga_id
  created_at         timestamptz not null default now(),
  published_at       timestamptz,
  retry_count        int not null default 0,
  next_retry_at      timestamptz,
  last_error         text
);

create index idx_outbox_pending
  on outbox_event(status, next_retry_at, created_at);
```

## Write Path (Inside One Transaction)

```java
@Transactional
public void createOrder(CreateOrderCommand cmd) {
    Order order = orderRepository.save(Order.create(cmd));

    OutboxEvent e = OutboxEvent.pending(
        UUID.randomUUID(),
        "Order",
        order.getId().toString(),
        "OrderCreated",
        toJson(orderCreatedPayload(order)),
        Map.of("traceId", currentTraceId()),
        order.getId().toString() // partition key
    );

    outboxRepository.save(e);
}
```

Key rule:

- Never publish directly to Kafka in this method.

---

## Relay Strategies

## 1) Polling Relay

Worker loop:

1. Fetch due `PENDING` rows in small batches
2. Lock rows (`FOR UPDATE SKIP LOCKED`) for multi-worker safety
3. Publish to Kafka
4. Mark `SENT` with `published_at`
5. On error, increment `retry_count`, set `next_retry_at`, capture `last_error`

Typical fetch query:

```sql
select *
from outbox_event
where status = 'PENDING'
  and (next_retry_at is null or next_retry_at <= now())
order by created_at
for update skip locked
limit 200;
```

## 2) CDC Relay (Debezium Outbox)

1. App only writes to outbox table
2. Debezium streams DB change events to Kafka
3. Kafka Connect handles delivery and offsets

When to prefer CDC:

1. High throughput
2. Need lower relay maintenance burden
3. Strong platform support for Kafka Connect

---

## Kafka Producer Baseline Settings

1. `acks=all`
2. `enable.idempotence=true`
3. `retries` high (bounded by delivery timeout)
4. `compression.type=zstd` (or snappy)
5. Stable keying by `partition_key`

Ordering note:

- Use same key for related events (`aggregate_id` or `saga_id`) to preserve per-entity order.

---

## Consumer Contract (Still Mandatory)

Outbox prevents producer-side dual-write inconsistency.
It does not guarantee exactly-once business effect on consumers.

Consumer must:

1. Dedupe by `event_id` (processed-events table/cache)
2. Be idempotent for side effects
3. Commit offsets only after durable local handling

---

## Failure Scenarios and Handling

1. Relay crashes after Kafka publish but before marking row `SENT`
- Row is republished on restart
- Consumer dedupe must absorb duplicate

2. Kafka unavailable for long duration
- Outbox backlog grows
- Alert on oldest pending age and queue size

3. Poison payload
- Repeated publish failures
- Move to `FAILED` after threshold, raise operational incident

4. DB hot spot on outbox index
- Partition table or archive old `SENT` rows
- Keep pending index selective

---

## Operational Metrics (Required)

1. `outbox_pending_count`
2. `outbox_oldest_pending_age_seconds`
3. `outbox_publish_success_total`
4. `outbox_publish_failure_total`
5. `outbox_retry_count` (distribution)
6. `outbox_relay_batch_latency_ms`
7. `outbox_failed_terminal_count`
8. `consumer_duplicate_dropped_total`

Alert examples:

1. Oldest pending age > SLO threshold
2. Publish failure ratio > baseline
3. Terminal failed events > 0

---

## Tradeoffs

Pros:

1. Eliminates producer dual-write race
2. Strong durability with restart safety
3. Works well with Saga and event-driven integration

Cons:

1. Extra infra and operational complexity
2. Eventual consistency window
3. Requires rigorous consumer idempotency anyway

---

## Relationship to Saga

In production, orchestrator/service steps often use:

1. Local state update + outbox write in one transaction
2. Relay to publish saga command/reply events safely

This is the standard way to avoid lost saga events during crashes.

## Reference Example in Repo

See `examples/outbox/OutboxDemo.java` for concept demonstration.
