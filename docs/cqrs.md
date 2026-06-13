# CQRS Pattern: Production Playbook

**Expert playbook:** [`docs/cqrs-expert-playbook.md`](cqrs-expert-playbook.md) — full lead/architect depth (projectors, read-your-writes, rebuild, 35+ interview Q&A).

## What CQRS Means

CQRS stands for Command Query Responsibility Segregation.

The core idea:

1. Commands change state.
2. Queries read state.
3. The write model and read model are allowed to be different.

That sounds simple, but the production value is deeper: you stop forcing one database model to serve two very different workloads.

In real systems, writes usually need invariants, validation, transactions, and business rules. Reads usually need fast lookup, filtering, search, denormalized joins, pagination, and caching. CQRS lets each side be optimized for its own job.

---

## The Production Problem

Consider an e-commerce order service.

The write path must enforce:

1. Customer exists.
2. Items are valid.
3. Prices are calculated consistently.
4. Inventory reservation is requested.
5. Order state transitions are legal.
6. Duplicate command retries do not create duplicate orders.

The read path must support:

1. Order detail page.
2. Customer order history.
3. Admin search by status/date/customer/email.
4. Dashboard counts by region and status.
5. Mobile app order card with denormalized payment and shipment status.

A normalized transactional schema is good for correctness, but often poor for read-heavy views. A denormalized read schema is good for queries, but dangerous as the source of truth.

CQRS separates those concerns.

---

## High-Level Architecture

```text
Client
  |
  | POST /orders
  v
Command API
  |
  | validates command
  | writes authoritative state
  v
Write Database
  |
  | domain event through outbox or CDC
  v
Event Broker
  |
  | projectors consume events
  v
Read Model Database
  ^
  |
Query API
  ^
  |
  | GET /orders/{id}
Client
```

Production rule:

1. The write database is the source of truth.
2. The read model is disposable and rebuildable.

---

## Real Production Example: Order Platform

Services:

1. `order-command-service`: accepts commands and owns order state.
2. `order-query-service`: serves read-optimized APIs.
3. `order-projector`: consumes order events and builds read models.
4. Kafka topic: `order.events`.
5. Write store: PostgreSQL normalized tables.
6. Read store: PostgreSQL denormalized tables, Elasticsearch/OpenSearch, Redis, or Cassandra depending on access pattern.

Example command:

```json
{
  "commandId": "7d6bb27e-20ef-4d25-a5f3-b1edcf9b0e83",
  "customerId": "C-1001",
  "items": [
    {
      "sku": "SKU-9",
      "quantity": 2
    }
  ]
}
```

Example events:

```json
{
  "eventId": "01HR...",
  "eventType": "OrderPlaced.v1",
  "aggregateType": "Order",
  "aggregateId": "O-9001",
  "sequence": 1,
  "occurredAt": "2026-06-02T10:15:30Z",
  "payload": {
    "orderId": "O-9001",
    "customerId": "C-1001",
    "status": "PLACED",
    "totalAmount": "2499.00"
  }
}
```

```json
{
  "eventId": "01HS...",
  "eventType": "OrderPaid.v1",
  "aggregateType": "Order",
  "aggregateId": "O-9001",
  "sequence": 2,
  "occurredAt": "2026-06-02T10:16:05Z",
  "payload": {
    "orderId": "O-9001",
    "paymentId": "P-7001",
    "status": "PAID"
  }
}
```

---

## Step 1: Model Commands Explicitly

A command is an intent to change state.

Examples:

1. `PlaceOrderCommand`
2. `CancelOrderCommand`
3. `MarkOrderPaidCommand`
4. `ShipOrderCommand`

Java example:

```java
public record PlaceOrderCommand(
    UUID commandId,
    String customerId,
    List<OrderLineRequest> items
) {
}
```

Production rules:

1. Every command should have an idempotency key, usually `commandId`.
2. Validate authorization before state change.
3. Validate business invariants inside the write model.
4. Commands should not return read-model data.

Common mistake:

1. Treating commands as CRUD DTOs.
2. A command should describe business intent, not database columns.

---

## Step 2: Build the Write Model for Correctness

The write model protects invariants.

Example aggregate:

```java
public final class Order {

    private final String id;
    private OrderStatus status;
    private final List<OrderLine> lines;
    private long version;

    public static Order place(String id, String customerId, List<OrderLine> lines) {
        if (lines == null || lines.isEmpty()) {
            throw new IllegalArgumentException("Order must contain at least one item");
        }
        return new Order(id, OrderStatus.PLACED, lines, 0);
    }

    public void markPaid() {
        if (status != OrderStatus.PLACED) {
            throw new IllegalStateException("Only placed orders can be paid");
        }
        status = OrderStatus.PAID;
        version++;
    }

    public void cancel() {
        if (status == OrderStatus.SHIPPED) {
            throw new IllegalStateException("Shipped orders cannot be cancelled");
        }
        status = OrderStatus.CANCELLED;
        version++;
    }
}
```

Write schema example:

```sql
create table orders (
  order_id       varchar(64) primary key,
  customer_id    varchar(64) not null,
  status         varchar(32) not null,
  version        bigint not null,
  created_at     timestamptz not null,
  updated_at     timestamptz not null
);

create table order_line (
  order_id       varchar(64) not null,
  line_id        varchar(64) not null,
  sku            varchar(64) not null,
  quantity       int not null,
  unit_price     numeric(12, 2) not null,
  primary key (order_id, line_id)
);

create table processed_command (
  command_id     uuid primary key,
  result_id      varchar(64),
  processed_at   timestamptz not null
);
```

The `processed_command` table makes command retries safe.

---

## Step 3: Publish Domain Events Safely

CQRS almost always needs events to update read models.

Do not publish to Kafka directly inside the command transaction. Use the Transactional Outbox pattern.

Command transaction:

1. Insert or update write model.
2. Insert `processed_command`.
3. Insert outbox event.
4. Commit.

Example:

```java
@Transactional
public OrderId placeOrder(PlaceOrderCommand command) {
    if (processedCommandRepository.exists(command.commandId())) {
        return processedCommandRepository.resultOf(command.commandId());
    }

    Order order = Order.place(newOrderId(), command.customerId(), toLines(command.items()));
    orderRepository.save(order);

    processedCommandRepository.save(command.commandId(), order.id());

    outboxRepository.save(OutboxEvent.pending(
        UUID.randomUUID(),
        "Order",
        order.id(),
        "OrderPlaced.v1",
        order.version(),
        order.id(),
        toJson(orderPlacedPayload(order))
    ));

    return new OrderId(order.id());
}
```

Important:

1. CQRS does not replace Outbox.
2. CQRS tells you to separate write/read models.
3. Outbox helps move events between them reliably.

---

## Step 4: Design Read Models for Queries

A read model is shaped for a specific query.

Order detail read model:

```sql
create table order_detail_view (
  order_id          varchar(64) primary key,
  customer_id       varchar(64) not null,
  customer_name     varchar(200) not null,
  status            varchar(32) not null,
  payment_status    varchar(32) not null,
  shipment_status   varchar(32) not null,
  total_amount      numeric(12, 2) not null,
  lines             jsonb not null,
  last_event_id     uuid not null,
  last_sequence     bigint not null,
  updated_at        timestamptz not null
);
```

Customer order history read model:

```sql
create table customer_order_history_view (
  customer_id       varchar(64) not null,
  order_id          varchar(64) not null,
  status            varchar(32) not null,
  total_amount      numeric(12, 2) not null,
  placed_at         timestamptz not null,
  updated_at        timestamptz not null,
  primary key (customer_id, placed_at, order_id)
);
```

Admin search read model:

1. Elasticsearch/OpenSearch index keyed by `order_id`.
2. Indexed fields: `status`, `customerId`, `customerEmail`, `placedAt`, `totalAmount`, `sku`.
3. Used only for search, not for authoritative decisions.

Production rule:

1. Build multiple read models when access patterns differ.
2. Do not force one read table to support every query forever.

---

## Step 5: Implement Projectors

A projector consumes events and updates read models.

Example:

```java
@KafkaListener(topics = "order.events", groupId = "order-detail-projector")
public void onOrderEvent(OrderEvent event) {
    if (processedEventRepository.exists(event.eventId())) {
        return;
    }

    transactionTemplate.executeWithoutResult(status -> {
        switch (event.eventType()) {
            case "OrderPlaced.v1" -> orderDetailViewRepository.insert(event);
            case "OrderPaid.v1" -> orderDetailViewRepository.markPaid(event);
            case "OrderShipped.v1" -> orderDetailViewRepository.markShipped(event);
            case "OrderCancelled.v1" -> orderDetailViewRepository.markCancelled(event);
            default -> throw new UnsupportedOperationException(event.eventType());
        }

        processedEventRepository.save(event.eventId(), event.aggregateId());
    });
}
```

Projector rules:

1. Projectors must be idempotent.
2. Store `event_id` to drop duplicates.
3. Track `last_sequence` per aggregate to reject stale events.
4. Commit Kafka offset only after read model update commits.
5. Poison events go to DLQ or terminal failure workflow.

---

## Step 6: Handle Ordering

For Kafka:

1. Use `aggregate_id` as message key.
2. Events for one aggregate go to the same partition.
3. Kafka preserves order within a partition.

Still protect the read model:

```sql
update order_detail_view
set status = 'PAID',
    payment_status = 'PAID',
    last_event_id = :event_id,
    last_sequence = :sequence,
    updated_at = now()
where order_id = :order_id
  and last_sequence < :sequence;
```

Why this matters:

1. Replays happen.
2. Duplicates happen.
3. Bad manual publishes happen.
4. Consumers restart mid-batch.

At staff level, do not say "Kafka keeps order" and stop there. Tell me what your database update does when an old event arrives.

---

## Step 7: Query API Reads Only Read Models

Example controller:

```java
@RestController
class OrderQueryController {

    private final OrderDetailViewRepository repository;

    OrderQueryController(OrderDetailViewRepository repository) {
        this.repository = repository;
    }

    @GetMapping("/orders/{orderId}")
    OrderDetailResponse getOrder(@PathVariable String orderId) {
        return repository.findById(orderId)
            .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND));
    }
}
```

Query side rules:

1. Query handlers must not mutate authoritative state.
2. Query handlers should not call command-side repositories.
3. Query API should expose read freshness when users care.

Example response:

```json
{
  "orderId": "O-9001",
  "status": "PAID",
  "paymentStatus": "PAID",
  "shipmentStatus": "PENDING",
  "totalAmount": "2499.00",
  "viewUpdatedAt": "2026-06-02T10:16:08Z",
  "readModelLagMs": 3000
}
```

---

## Eventual Consistency

CQRS introduces a consistency gap.

Example:

1. Client calls `POST /orders`.
2. Command succeeds and returns `orderId`.
3. Client immediately calls `GET /orders/{orderId}`.
4. Read model may not contain the order yet.

Production options:

1. Return `202 Accepted` with operation id when read projection is not immediate.
2. Return command result from the write side for the immediate response.
3. Use read-your-writes token with minimum expected sequence.
4. Poll query endpoint until `last_sequence >= expected_sequence`.
5. For critical views, serve a fallback from write store for a short period.

Read-your-writes example:

```http
POST /orders
```

```json
{
  "orderId": "O-9001",
  "expectedSequence": 1
}
```

```http
GET /orders/O-9001?minSequence=1
```

If projection has not caught up:

```http
HTTP/1.1 202 Accepted
Retry-After: 1
```

---

## Rebuild Strategy

Read models must be rebuildable.

Rebuild sources:

1. Event log if events are retained forever.
2. Write database snapshot plus events after snapshot.
3. CDC stream from write database.
4. Batch export from authoritative service.

Rebuild process:

1. Create new read model table or index with version suffix.
2. Replay source data into new model.
3. Validate counts and checksums.
4. Dual-write projector to old and new model for a short period.
5. Switch query API to new model.
6. Keep old model for rollback window.
7. Drop old model after confidence period.

Production warning:

1. If you cannot rebuild the read model, it has quietly become another source of truth.

---

## Failure Scenarios and Handling

### Projector Down

Impact:

1. Writes continue.
2. Read model becomes stale.

Handling:

1. Alert on projection lag.
2. Scale projector consumers.
3. Keep event retention long enough for recovery.

### Duplicate Event

Impact:

1. Count fields may double.
2. Status transitions may repeat.

Handling:

1. Dedupe by `event_id`.
2. Use sequence-aware updates.
3. Make increments idempotent by deriving state where possible.

### Out-of-Order Event

Impact:

1. Read model may show older status.

Handling:

1. Partition by aggregate id.
2. Store `last_sequence`.
3. Ignore events with `sequence <= last_sequence`.

### Poison Event

Impact:

1. Consumer repeatedly fails on one event.
2. Lag grows behind the poison event.

Handling:

1. Retry transient errors.
2. Send poison event to DLQ after threshold.
3. Page on-call for schema or data bug.
4. Provide replay tooling after fix.

### Read Store Lost

Impact:

1. Query API degraded or unavailable.

Handling:

1. Rebuild from event log or write snapshot.
2. Serve limited fallback from write side only if business requires it.
3. Communicate stale or degraded state explicitly.

---

## Metrics You Must Emit

Command side:

1. `cqrs_command_received_total`
2. `cqrs_command_success_total`
3. `cqrs_command_failure_total`
4. `cqrs_command_idempotent_replay_total`
5. `cqrs_command_latency_ms`

Event/projector side:

1. `cqrs_projection_lag_seconds`
2. `cqrs_projection_event_processed_total`
3. `cqrs_projection_event_failed_total`
4. `cqrs_projection_duplicate_dropped_total`
5. `cqrs_projection_stale_event_dropped_total`
6. `cqrs_projection_dlq_total`

Query side:

1. `cqrs_query_latency_ms`
2. `cqrs_query_not_found_total`
3. `cqrs_read_model_age_seconds`
4. `cqrs_query_cache_hit_ratio`

Alert examples:

1. Projection lag > 2 minutes for 10 minutes.
2. DLQ events > 0.
3. Query p99 latency above SLO.
4. Read model age above business freshness threshold.

---

## Logging and Tracing

Every command log should include:

1. `command_id`
2. `aggregate_id`
3. `customer_id` or tenant id
4. `trace_id`
5. Result status

Every event should include:

1. `event_id`
2. `event_type`
3. `aggregate_id`
4. `sequence`
5. `correlation_id`
6. `causation_id`

Trace flow:

```text
HTTP command request
  -> command handler
  -> write DB transaction
  -> outbox row
  -> relay publish
  -> projector consume
  -> read model update
  -> query request
```

Without correlation, CQRS incidents become archaeology.

---

## CQRS vs Event Sourcing

CQRS and Event Sourcing are different.

CQRS:

1. Separates command model from query model.
2. Write state can be stored as normal relational rows.
3. Events may be integration events only.

Event Sourcing:

1. Stores state changes as the primary source of truth.
2. Reconstructs aggregate state by replaying events.
3. Usually pairs well with CQRS, but is not required.

Use CQRS without Event Sourcing when:

1. You need read/write separation.
2. You do not need full historical event replay as source of truth.
3. Your team wants lower operational complexity.

Use CQRS with Event Sourcing when:

1. Audit history is core to the domain.
2. Temporal reconstruction matters.
3. Business state transitions are naturally event-based.

---

## When To Use CQRS

Good fit:

1. Read and write workloads are very different.
2. Read traffic is much larger than write traffic.
3. You need denormalized views for performance.
4. You need multiple query shapes from one write model.
5. Different teams own command and query scaling separately.

Bad fit:

1. Simple CRUD application.
2. Team cannot operate asynchronous pipelines.
3. Business requires immediate consistent reads everywhere.
4. Read models cannot tolerate rebuild or replay.
5. The split is introduced before real complexity exists.

Common staff-level pushback:

1. If your entire app has five tables and one admin UI, CQRS is probably ceremony.
2. If your order table is serving checkout, search, dashboards, exports, and mobile views, CQRS starts paying rent.

---

## Production Readiness Checklist

1. Commands have idempotency keys.
2. Write model enforces invariants.
3. Events are published through outbox or CDC.
4. Event payloads are versioned.
5. Projectors are idempotent.
6. Read models track `last_event_id` and `last_sequence`.
7. Projection lag is measured and alerted.
8. Read model rebuild runbook exists.
9. Query API handles stale or missing projections explicitly.
10. DLQ and replay tooling exist.
11. Compatibility and schema evolution rules are documented.
12. Consumer-facing APIs do not hide eventual consistency assumptions.

## Reference Relationship to Existing Docs

CQRS commonly appears with:

1. Transactional Outbox: safely moves write-side events to projectors.
2. Saga: coordinates multi-service business workflows.
3. Event Sourcing: optional source-of-truth event store, not mandatory for CQRS.

See `docs/outbox.md` and `docs/saga.md` for the reliability patterns that usually surround production CQRS.

---

## How to Talk About CQRS in an Interview

> Say it like this. Casual and clear.

---

### "What is CQRS?"

CQRS just means — reading and writing are different things, so don't use the same code for both.

When you save data, you need validation, business rules, and transactions. When you fetch data, you just need it fast. Forcing one model to do both makes it messy.

CQRS says — have a write side that handles saving, and a read side that handles fetching. Each can be optimized for what it does.

---

### "Why split into separate databases?"

Once the read and write models are separate, you can use different databases for each.

The write side can use a normal relational database — good for transactions and rules. The read side can use whatever is fastest — maybe a read replica, maybe Redis, maybe Elasticsearch.

The catch is they are not always in sync right away. After you write something, the read side updates a moment later. That's usually fine. A product listing being 2 seconds stale is okay. A bank balance being stale is not.

---

### "How do you keep the read side in sync?"

Events. When something is written, you publish an event. Like "order was placed". The read side listens for that and updates its own data.

If the read side gets out of sync or you change its structure, you just replay all the events and rebuild it from scratch.

---

### "When does CQRS make things worse?"

For a simple app, CQRS is way too much. You end up with two models, event publishing, and a message bus — all for something that was a basic CRUD app.

Use it when reads and writes really are different. Like a dashboard that needs very fast reads on data that gets written rarely. That's where it pays off.

---

### Quick Answers

| Question | Say this |
|---|---|
| What is CQRS? | Separate your read model from your write model — each can be optimized differently |
| Why split databases? | Read and write have different needs — you optimize each separately |
| How do they stay in sync? | Domain events — the write side publishes, the read side updates itself |
| Main tradeoff? | Reads might be a little behind writes — eventual consistency |
| When to use? | High read traffic, multiple read formats needed, complex write rules |
| When not to use? | Simple CRUD — adds complexity for no real benefit |

