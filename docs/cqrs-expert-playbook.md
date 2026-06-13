# CQRS — Expert Playbook (Lead/Architect, Java, 10+ Years)

A comprehensive end-to-end reference for Command Query Responsibility Segregation in production Java systems. Covers write-model aggregates, read-model projectors, outbox integration, ordering guards, eventual consistency UX, rebuild strategies, Spring Boot command/query services, Kafka projectors, metrics, DLQ handling, and operational runbooks. Cross-references the [Transactional Outbox Expert Playbook](outbox-expert-playbook.md) and [Saga pattern docs](saga.md) where CQRS commonly appears in real architectures.

---

## Table of Contents

1. [CQRS Fundamentals (Beyond Textbook Definitions)](#1-cqrs-fundamentals-beyond-textbook-definitions)
2. [The Production Problem — Why One Model Breaks](#2-the-production-problem--why-one-model-breaks)
3. [Architecture — Command, Event, and Query Paths](#3-architecture--command-event-and-query-paths)
4. [Modeling Commands Explicitly](#4-modeling-commands-explicitly)
5. [Write Model Aggregates and Invariants](#5-write-model-aggregates-and-invariants)
6. [Transactional Outbox Integration](#6-transactional-outbox-integration)
7. [Read Models and Query Shapes](#7-read-models-and-query-shapes)
8. [Projectors — Design and Implementation](#8-projectors--design-and-implementation)
9. [Ordering, Sequence Guards, and Idempotency](#9-ordering-sequence-guards-and-idempotency)
10. [Eventual Consistency and Read-Your-Writes UX](#10-eventual-consistency-and-read-your-writes-ux)
11. [Read Model Rebuild Strategy](#11-read-model-rebuild-strategy)
12. [CQRS vs Event Sourcing — Decision Framework](#12-cqrs-vs-event-sourcing--decision-framework)
13. [Spring Boot Command and Query Services](#13-spring-boot-command-and-query-services)
14. [Kafka Projectors — Production Configuration](#14-kafka-projectors--production-configuration)
15. [Metrics, Logging, and Tracing](#15-metrics-logging-and-tracing)
16. [DLQ, Poison Events, and Replay Tooling](#16-dlq-poison-events-and-replay-tooling)
17. [Production Issue Runbook](#17-production-issue-runbook)
18. [When NOT to Use CQRS](#18-when-not-to-use-cqrs)
19. [Lead Interview Questions — Logical and Production Scenarios](#19-lead-interview-questions--logical-and-production-scenarios)
20. [How to Talk About CQRS in an Interview](#20-how-to-talk-about-cqrs-in-an-interview)

---

## 1. CQRS Fundamentals (Beyond Textbook Definitions)

### 1.1 What CQRS Actually Means

CQRS stands for **Command Query Responsibility Segregation**.

The core idea is deceptively simple:

1. **Commands** change state — they express business intent and enforce invariants.
2. **Queries** read state — they return data optimized for a specific access pattern.
3. The **write model** and **read model** are allowed — and often should — be different.

The production value is not the acronym. The value is that you stop forcing one database schema, one service layer, and one API to serve two workloads that have fundamentally different needs.

| Workload | Primary concerns | Optimization target |
|----------|------------------|---------------------|
| Write path | Invariants, validation, transactions, idempotency, audit | Correctness, consistency within aggregate |
| Read path | Lookup speed, filtering, search, pagination, denormalization, caching | Latency, throughput, UX responsiveness |

### 1.2 CQRS Is Not CRUD With Extra Steps

A common anti-pattern: rename `POST /orders` to `PlaceOrderCommand` but still read and write the same normalized `orders` table from one monolithic service.

That is not CQRS. That is ceremony.

Real CQRS means:

1. Command handlers mutate an **authoritative write store** through a domain model.
2. Domain events (or CDC) propagate changes asynchronously.
3. **Projectors** build one or more **read-optimized stores**.
4. Query handlers read **only** from read models (with explicit stale-read handling).

### 1.3 CQRS vs Traditional Layered Architecture

```
Traditional (single model):
  Controller -> Service -> Repository -> Single DB -> Same tables for read and write

CQRS (separated models):
  CommandController -> CommandHandler -> WriteRepository -> Write DB
                                                              |
                                                         Outbox/CDC
                                                              |
                                                         Event Broker
                                                              |
                                                         Projector -> Read DB
  QueryController -> QueryHandler -> ReadRepository -> Read DB
```

### 1.4 The Golden Production Rules

1. The **write database is the source of truth**.
2. The **read model is disposable and rebuildable**.
3. Commands do not return read-model-shaped DTOs unless you explicitly accept coupling.
4. Queries never mutate authoritative state.
5. Every async boundary assumes **at-least-once delivery** — design for duplicates and reordering.

### 1.5 CQRS in the Microservices Landscape

CQRS often appears alongside:

| Pattern | Relationship | See also |
|---------|--------------|----------|
| Transactional Outbox | Reliably publishes write-side events to projectors and downstream services | [outbox-expert-playbook.md](outbox-expert-playbook.md), [outbox.md](outbox.md) |
| Saga | Coordinates multi-service workflows; each service may have its own CQRS split | [saga.md](saga.md), [saga-orchestrator-sync-vs-async.md](saga-orchestrator-sync-vs-async.md) |
| Event Sourcing | Optional — stores state as events; pairs well but is not required | Section 12 |
| Kafka | Common transport for domain events between write side and projectors | [kafka-expert-playbook.md](kafka-expert-playbook.md) |

---

## 2. The Production Problem — Why One Model Breaks

### 2.1 E-Commerce Order Platform Example

Consider an order service at scale.

**Write path must enforce:**

1. Customer exists and is not blocked.
2. Items are valid and priced consistently at order time.
3. Inventory reservation is requested (possibly via saga).
4. Order state transitions are legal (`PLACED -> PAID -> SHIPPED`, not `SHIPPED -> PLACED`).
5. Duplicate command retries do not create duplicate orders.
6. Payment capture is idempotent under network retries.

**Read path must support:**

1. Order detail page (single lookup, denormalized lines + payment + shipment).
2. Customer order history (paginated, sorted by date).
3. Admin search by status, date, customer email, SKU.
4. Dashboard aggregates by region and status.
5. Mobile order card with cached summary fields.

A normalized transactional schema (`orders`, `order_line`, `payment`, `shipment`) is excellent for write correctness. It is often poor for:

- Admin full-text search across customer email and SKU.
- Mobile list views that need 6 joins per row.
- Dashboard rollups that hammer the primary write database.

A denormalized read schema is excellent for queries. It is **dangerous** as the source of truth because invariants span multiple entities and concurrent writes become nightmares.

CQRS separates those concerns deliberately.

### 2.2 Symptoms That CQRS Might Help

| Symptom | Root cause CQRS addresses |
|---------|---------------------------|
| Read replicas still too slow for search/dashboards | Dedicated read models shaped per query |
| Complex JOIN queries on the write DB during peak checkout | Reads offloaded to denormalized views |
| Team debates "one big table vs many views" endlessly | Multiple read models, one write model |
| Scaling reads requires scaling the write-primary | Independent scaling of query services |
| Mobile and admin need incompatible query shapes | Separate projectors per access pattern |

### 2.3 Symptoms That CQRS Will Hurt You

| Symptom | Why CQRS makes it worse |
|---------|-------------------------|
| 5-table CRUD admin app | Operational overhead exceeds benefit |
| Business demands strong consistency on every read | CQRS introduces intentional lag |
| Team has no experience operating async pipelines | Incidents become opaque |
| No event retention or rebuild plan | Read model becomes shadow source of truth |

---

## 3. Architecture — Command, Event, and Query Paths

### 3.1 End-to-End Flow

```text
Client
  |
  | POST /commands/orders  (PlaceOrder)
  v
Command API  (order-command-service)
  |
  | validate auth + command
  | load aggregate, enforce invariants
  | persist write model + processed_command + outbox
  v
Write Database (PostgreSQL — normalized, authoritative)
  |
  | outbox relay (polling or Debezium CDC)
  v
Event Broker (Kafka topic: order.events)
  |
  | projectors consume (consumer groups per read model)
  v
Read Model Stores
  |-- order_detail_view (PostgreSQL)
  |-- customer_order_history_view (PostgreSQL)
  |-- order_search_index (OpenSearch)
  |
  v
Query API  (order-query-service)
  ^
  |
  | GET /orders/{id}
Client
```

### 3.2 Service Boundaries

Production-grade decomposition:

| Service | Responsibility | Scaling knob |
|---------|----------------|--------------|
| `order-command-service` | Accept commands, own write model, emit events | Write throughput, transaction latency |
| `order-query-service` | Serve read APIs from read models only | Read QPS, cache hit ratio |
| `order-detail-projector` | Maintain `order_detail_view` | Consumer lag, partition count |
| `order-search-projector` | Maintain OpenSearch index | Indexing throughput |
| `outbox-relay` | Publish pending outbox rows to Kafka | Relay backlog |

You may colocate command + outbox relay early in product life. **Never** colocate command handlers with query handlers reading the write DB as a shortcut — that coupling erodes the pattern within two sprints.

### 3.3 Deployment Topology (Kubernetes)

```text
┌─────────────────────────────────────────────────────────────────┐
│  Ingress / API Gateway                                          │
│    /commands/*  -> order-command-service (Deployment, HPA)      │
│    /queries/*   -> order-query-service   (Deployment, HPA)        │
└─────────────────────────────────────────────────────────────────┘
         |                                    ^
         v                                    |
┌─────────────────┐    Kafka     ┌──────────────────────────┐
│  Write PG       │ ── outbox ──>│  order-detail-projector  │
│  (StatefulSet   │              │  order-search-projector  │
│   or managed)   │              │  (Deployments, KEDA/lag) │
└─────────────────┘              └──────────────────────────┘
                                           |
                                           v
                                 ┌─────────────────┐
                                 │  Read stores    │
                                 │  PG + OpenSearch│
                                 └─────────────────┘
```

### 3.4 Event Envelope (Production Minimum)

Every domain event published from the write side should carry:

```json
{
  "eventId": "01HRZK3N8X9Y2M4P6Q8R0S2T4V6",
  "eventType": "OrderPlaced.v1",
  "aggregateType": "Order",
  "aggregateId": "O-9001",
  "sequence": 1,
  "occurredAt": "2026-06-02T10:15:30Z",
  "correlationId": "trace-abc-123",
  "causationId": "cmd-7d6bb27e-20ef-4d25-a5f3-b1edcf9b0e83",
  "payload": {
    "orderId": "O-9001",
    "customerId": "C-1001",
    "status": "PLACED",
    "totalAmount": "2499.00",
    "lines": [
      { "sku": "SKU-9", "quantity": 2, "unitPrice": "1249.50" }
    ]
  }
}
```

Field rules:

1. `eventId` — globally unique, used for idempotent projection.
2. `sequence` — monotonically increasing **per aggregate**, used for ordering guards.
3. `eventType` — versioned (`OrderPlaced.v1`) for schema evolution.
4. `aggregateId` — Kafka message key for partition ordering.
5. `correlationId` / `causationId` — distributed tracing across command → event → projection.

---

## 4. Modeling Commands Explicitly

### 4.1 Commands Express Business Intent

A command is an **intent to change state**, not a database row DTO.

| Good command | Bad "command" |
|--------------|---------------|
| `PlaceOrderCommand` | `InsertOrderRequest` with every column |
| `CancelOrderCommand` | `PATCH /orders/{id}` with partial JSON |
| `MarkOrderPaidCommand` | `UpdateOrderStatusRequest` |

### 4.2 Java Command Records

```java
public record PlaceOrderCommand(
    UUID commandId,
    String customerId,
    List<OrderLineRequest> items
) {
    public PlaceOrderCommand {
        Objects.requireNonNull(commandId, "commandId");
        Objects.requireNonNull(customerId, "customerId");
        Objects.requireNonNull(items, "items");
        if (items.isEmpty()) {
            throw new IllegalArgumentException("Order must contain at least one item");
        }
    }
}

public record CancelOrderCommand(
    UUID commandId,
    String orderId,
    String reason
) {}

public record MarkOrderPaidCommand(
    UUID commandId,
    String orderId,
    String paymentId,
    BigDecimal capturedAmount
) {}
```

### 4.3 Command Handler Contract

Production rules:

1. Every command carries an **idempotency key** — usually `commandId` (UUID v4 or ULID).
2. **Authorize** before loading the aggregate (fail fast on auth).
3. Validate **business invariants** inside the aggregate, not in the controller.
4. Commands return **minimal write-side result** — typically aggregate id + version/sequence, not a full read DTO.
5. Command handlers are **transactional** — write model + idempotency record + outbox in one commit.

```java
public sealed interface CommandResult permits CommandAccepted, CommandDuplicate, CommandRejected {}

public record CommandAccepted(String aggregateId, long sequence) implements CommandResult {}
public record CommandDuplicate(String aggregateId) implements CommandResult {}
public record CommandRejected(String code, String message) implements CommandResult {}
```

### 4.4 Idempotency Table

```sql
create table processed_command (
  command_id     uuid primary key,
  aggregate_type varchar(64) not null,
  result_id      varchar(64) not null,
  result_sequence bigint,
  processed_at   timestamptz not null default now()
);

create index idx_processed_command_result
  on processed_command (result_id);
```

Handler pattern:

```java
@Transactional
public CommandResult placeOrder(PlaceOrderCommand command) {
    var existing = processedCommandRepository.findResult(command.commandId());
    if (existing.isPresent()) {
        meterRegistry.counter("cqrs.command.idempotent_replay").increment();
        return new CommandDuplicate(existing.get().resultId());
    }

    var order = Order.place(newOrderId(), command.customerId(), toLines(command.items()));
    orderRepository.save(order);

    var sequence = order.version();
    outboxRepository.save(buildOrderPlacedEvent(order, command.commandId(), sequence));

    processedCommandRepository.save(
        command.commandId(), "Order", order.id(), sequence);

    return new CommandAccepted(order.id(), sequence);
}
```

### 4.5 Command API Response Codes

| Scenario | HTTP | Body |
|----------|------|------|
| First successful execution | `201 Created` or `200 OK` | `{ "orderId": "...", "expectedSequence": 1 }` |
| Idempotent retry | `200 OK` | Same ids, flag optional: `"duplicate": true` |
| Business rule violation | `409 Conflict` or `422 Unprocessable` | `{ "code": "INVALID_TRANSITION", ... }` |
| Unauthorized | `403 Forbidden` | — |

**Do not** return the full order detail read model from the command API unless you have explicitly decided to accept write/read coupling for that endpoint.

### 4.6 Common Command Modeling Mistakes

1. **Anemic commands** — flat DTOs with no validation; all logic in a 400-line service class.
2. **Query smuggling** — `PlaceOrderCommand` returns enriched customer profile from a join.
3. **Missing idempotency** — mobile clients retry POST; you create duplicate orders.
4. **Command = HTTP verb** — using PATCH semantics for state transitions that have business meaning.
5. **Cross-aggregate commands** — `PlaceOrderAndShipCommand` that should be a saga instead (see [saga.md](saga.md)).

---

## 5. Write Model Aggregates and Invariants

### 5.1 Aggregate Design

An aggregate is a consistency boundary. All invariants inside it are enforced synchronously in one transaction.

For orders:

- **Aggregate root**: `Order`
- **Entities/value objects inside boundary**: `OrderLine`
- **References outside boundary**: `CustomerId`, `ProductSku` (validated via ACL or synchronous call, not owned)

```java
public final class Order {

    private final String id;
    private final String customerId;
    private OrderStatus status;
    private final List<OrderLine> lines;
    private long version;

    private Order(String id, String customerId, OrderStatus status,
                  List<OrderLine> lines, long version) {
        this.id = id;
        this.customerId = customerId;
        this.status = status;
        this.lines = List.copyOf(lines);
        this.version = version;
    }

    public static Order place(String id, String customerId, List<OrderLine> lines) {
        if (lines == null || lines.isEmpty()) {
            throw new DomainException("ORDER_EMPTY", "Order must contain at least one item");
        }
        return new Order(id, customerId, OrderStatus.PLACED, lines, 0);
    }

    public OrderPaid markPaid(String paymentId, BigDecimal capturedAmount) {
        if (status != OrderStatus.PLACED) {
            throw new DomainException("INVALID_TRANSITION",
                "Only placed orders can be paid; current status: " + status);
        }
        if (capturedAmount.compareTo(totalAmount()) < 0) {
            throw new DomainException("INSUFFICIENT_CAPTURE",
                "Captured amount less than order total");
        }
        status = OrderStatus.PAID;
        version++;
        return new OrderPaid(id, paymentId, version);
    }

    public void cancel(String reason) {
        if (status == OrderStatus.SHIPPED) {
            throw new DomainException("INVALID_TRANSITION",
                "Shipped orders cannot be cancelled");
        }
        if (status == OrderStatus.CANCELLED) {
            return; // idempotent cancel
        }
        status = OrderStatus.CANCELLED;
        version++;
    }

    public BigDecimal totalAmount() {
        return lines.stream()
            .map(OrderLine::lineTotal)
            .reduce(BigDecimal.ZERO, BigDecimal::add);
    }

    public String id() { return id; }
    public String customerId() { return customerId; }
    public OrderStatus status() { return status; }
    public long version() { return version; }
    public List<OrderLine> lines() { return lines; }
}
```

### 5.2 Write Schema (Normalized, Authoritative)

```sql
create table orders (
  order_id       varchar(64) primary key,
  customer_id    varchar(64) not null,
  status         varchar(32) not null,
  version        bigint not null,
  created_at     timestamptz not null,
  updated_at     timestamptz not null,
  constraint chk_order_status check (
    status in ('PLACED','PAID','SHIPPED','CANCELLED')
  )
);

create table order_line (
  order_id       varchar(64) not null references orders(order_id),
  line_id        varchar(64) not null,
  sku            varchar(64) not null,
  quantity       int not null check (quantity > 0),
  unit_price     numeric(12, 2) not null,
  primary key (order_id, line_id)
);

-- Optimistic locking guard
create unique index uq_orders_id_version on orders (order_id, version);
```

Repository save with version check:

```java
@Transactional
public void save(Order order) {
    int updated = jdbcTemplate.update("""
        update orders
        set status = ?, version = ?, updated_at = now()
        where order_id = ? and version = ?
        """,
        order.status().name(), order.version(), order.id(), order.version() - 1);

    if (updated == 0 && !exists(order.id())) {
        insertNew(order);
    } else if (updated == 0) {
        throw new OptimisticLockException("Concurrent modification: " + order.id());
    }
}
```

### 5.3 Write Model vs Event Sourcing Write Model

In **CQRS without Event Sourcing**:

- Current state is stored as relational rows.
- Events are **integration events** — derived from state change, published for projectors.

In **CQRS with Event Sourcing**:

- Events **are** the write model.
- Aggregate state is reconstructed by replaying events.
- Snapshots optional for performance.

Section 12 covers when to choose each.

### 5.4 Saga Integration on the Write Side

Multi-service workflows (place order → reserve inventory → capture payment → create shipment) should **not** be one giant distributed transaction.

Use a [Saga](saga.md):

- Each service exposes **commands** on its write model.
- Saga orchestrator (or choreography via events) drives forward steps and compensations.
- Each service publishes events through **outbox** so projectors and saga participants see consistent notifications.

```text
OrderCommandService          PaymentCommandService
      |                              |
      | OrderPlaced (outbox)         |
      +----------------------------->|
      |                              | CapturePaymentCommand
      |                              | PaymentCaptured (outbox)
      |<-----------------------------+
      | MarkOrderPaidCommand         |
```

See [saga-orchestrator-sync-vs-async.md](saga-orchestrator-sync-vs-async.md) and [saga-prod-examples-sync-async.md](saga-prod-examples-sync-async.md) for orchestration trade-offs.

---

## 6. Transactional Outbox Integration

### 6.1 Why CQRS Requires Reliable Event Publication

Projectors learn about write-side changes through events. If you publish to Kafka **outside** the database transaction:

1. DB commits, publish fails → read models never update (silent divergence).
2. Publish succeeds, DB rolls back → ghost events corrupt read models.

**Transactional Outbox** solves this: business write + outbox insert in **one local transaction**; a relay publishes asynchronously.

Deep dive: [outbox-expert-playbook.md](outbox-expert-playbook.md).

### 6.2 Outbox Table

```sql
create table outbox_event (
  event_id           uuid primary key,
  aggregate_type     varchar(100) not null,
  aggregate_id       varchar(100) not null,
  event_type         varchar(200) not null,
  sequence           bigint not null,
  partition_key      varchar(200) not null,
  payload            jsonb not null,
  headers            jsonb not null default '{}',
  status             varchar(20) not null default 'PENDING',
  created_at         timestamptz not null default now(),
  published_at       timestamptz,
  constraint chk_outbox_status check (status in ('PENDING','SENT','FAILED'))
);

create index idx_outbox_pending on outbox_event (created_at)
  where status = 'PENDING';
```

### 6.3 Command Transaction (Single Commit)

```java
@Transactional
public CommandResult placeOrder(PlaceOrderCommand command) {
    // idempotency check ...
    Order order = Order.place(newOrderId(), command.customerId(), toLines(command.items()));
    orderRepository.save(order);

    OutboxEvent event = OutboxEvent.pending(
        UlidCreator.getUlid().toUuid(),
        "Order",
        order.id(),
        "OrderPlaced.v1",
        order.version(),
        order.id(), // partition key
        Map.of("trace_id", MDC.get("trace_id")),
        toJson(orderPlacedPayload(order))
    );
    outboxRepository.insert(event);

    processedCommandRepository.save(command.commandId(), "Order", order.id(), order.version());

    return new CommandAccepted(order.id(), order.version());
}
```

Transaction contents:

1. Insert/update write model.
2. Insert `processed_command`.
3. Insert `outbox_event` with `status = PENDING`.
4. Commit.

### 6.4 Outbox Relay Options

| Strategy | Pros | Cons |
|----------|------|------|
| Polling publisher (Spring `@Scheduled`) | Simple, works everywhere | Poll latency, DB load |
| Debezium CDC | Near-real-time, no poll loop | Infra complexity, ops skill |
| PostgreSQL `LISTEN/NOTIFY` | Low latency | Not durable notification alone |

Polling relay (production baseline):

```java
@Scheduled(fixedDelayString = "${outbox.relay.poll-ms:500}")
@Transactional
public void publishPendingEvents() {
    List<OutboxEvent> batch = outboxRepository.lockPendingBatch(100);
    for (OutboxEvent event : batch) {
        try {
            kafkaTemplate.send(
                "order.events",
                event.partitionKey(),
                toAvro(event)
            ).get(5, TimeUnit.SECONDS);
            outboxRepository.markSent(event.eventId());
        } catch (Exception ex) {
            outboxRepository.markFailed(event.eventId(), ex.getMessage());
            meterRegistry.counter("outbox.relay.failed").increment();
        }
    }
}
```

Use `SELECT ... FOR UPDATE SKIP LOCKED` for concurrent relay instances.

### 6.5 CQRS + Outbox + Saga Together

Typical order platform:

1. **Order command service** writes order + `OrderPlaced` outbox event.
2. **Relay** publishes to `order.events`.
3. **Inventory / payment services** consume (saga participants).
4. **Projectors** consume same topic (or derived topics) for read models.

The outbox guarantees: if the order exists, the event will eventually publish. Saga and projectors must both **dedupe by eventId**.

---

## 7. Read Models and Query Shapes

### 7.1 One Write Model, Many Read Models

Each read model is shaped for a **specific query** — not a generic mirror of the write schema.

| Read model | Query served | Store choice |
|------------|--------------|--------------|
| `order_detail_view` | GET `/orders/{id}` | PostgreSQL JSONB |
| `customer_order_history_view` | GET `/customers/{id}/orders` | PostgreSQL |
| `order_search_index` | Admin search | OpenSearch |
| `order_dashboard_daily` | BI dashboard | ClickHouse / materialized view |
| `order_summary_cache` | Mobile list card | Redis (TTL, rebuilt from events) |

Production rule: **build multiple read models when access patterns differ**. Do not force one denormalized table to serve every future query.

### 7.2 Order Detail Read Model

```sql
create table order_detail_view (
  order_id          varchar(64) primary key,
  customer_id       varchar(64) not null,
  customer_name     varchar(200) not null,
  status            varchar(32) not null,
  payment_status    varchar(32) not null default 'PENDING',
  shipment_status   varchar(32) not null default 'PENDING',
  total_amount      numeric(12, 2) not null,
  lines             jsonb not null,
  last_event_id     uuid not null,
  last_sequence     bigint not null,
  updated_at        timestamptz not null
);

create index idx_order_detail_customer on order_detail_view (customer_id);
```

### 7.3 Customer Order History Read Model

Optimized for `(customer_id, placed_at DESC)` pagination:

```sql
create table customer_order_history_view (
  customer_id       varchar(64) not null,
  order_id          varchar(64) not null,
  status            varchar(32) not null,
  total_amount      numeric(12, 2) not null,
  placed_at         timestamptz not null,
  updated_at        timestamptz not null,
  last_sequence     bigint not null,
  primary key (customer_id, placed_at, order_id)
);
```

### 7.4 Admin Search Read Model (OpenSearch)

Index document (denormalized at projection time):

```json
{
  "orderId": "O-9001",
  "customerId": "C-1001",
  "customerEmail": "user@example.com",
  "status": "PAID",
  "totalAmount": 2499.00,
  "skus": ["SKU-9", "SKU-12"],
  "placedAt": "2026-06-02T10:15:30Z",
  "lastSequence": 2
}
```

Search queries never touch the write database.

### 7.5 Read Model Freshness Metadata

Always store projection metadata:

1. `last_event_id` — dedupe + audit.
2. `last_sequence` — ordering guard.
3. `updated_at` — expose to clients as `viewUpdatedAt`.

Query API may compute:

```java
long readModelLagMs = Duration.between(view.updatedAt(), Instant.now()).toMillis();
```

Return in response for UX decisions (Section 10).

---

## 8. Projectors — Design and Implementation

### 8.1 Projector Responsibilities

A projector:

1. Consumes domain events from Kafka (or CDC).
2. Transforms event payload into read-model rows/documents.
3. Applies updates **idempotently** with sequence guards.
4. Commits read-model update **before** Kafka offset commit.
5. Routes poison events to DLQ after retry threshold.

### 8.2 Projector Implementation (Spring Boot + Kafka)

```java
@Component
@Slf4j
public class OrderDetailProjector {

    private final OrderDetailViewRepository viewRepository;
    private final ProcessedEventRepository processedEventRepository;
    private final TransactionTemplate transactionTemplate;
    private final MeterRegistry meterRegistry;

    @KafkaListener(
        topics = "${cqrs.order.events-topic:order.events}",
        groupId = "${cqrs.projector.order-detail.group-id:order-detail-projector}",
        containerFactory = "manualAckKafkaListenerContainerFactory"
    )
    public void onOrderEvent(
            ConsumerRecord<String, OrderEvent> record,
            Acknowledgment ack) {

        OrderEvent event = record.value();

        if (processedEventRepository.exists(event.eventId())) {
            meterRegistry.counter("cqrs.projection.duplicate_dropped").increment();
            ack.acknowledge();
            return;
        }

        try {
            transactionTemplate.executeWithoutResult(status -> {
                switch (event.eventType()) {
                    case "OrderPlaced.v1" -> viewRepository.insertFromPlaced(event);
                    case "OrderPaid.v1" -> viewRepository.markPaid(event);
                    case "OrderShipped.v1" -> viewRepository.markShipped(event);
                    case "OrderCancelled.v1" -> viewRepository.markCancelled(event);
                    default -> throw new UnsupportedEventTypeException(event.eventType());
                }
                processedEventRepository.save(event.eventId(), event.aggregateId());
            });
            ack.acknowledge();
            meterRegistry.counter("cqrs.projection.event_processed",
                "event_type", event.eventType()).increment();
        } catch (Exception ex) {
            // DefaultErrorHandler + DLQ — see Section 16
            throw ex;
        }
    }
}
```

### 8.3 Processed Events Table

```sql
create table processed_event (
  event_id       uuid primary key,
  aggregate_id   varchar(64) not null,
  event_type     varchar(200) not null,
  processed_at   timestamptz not null default now()
);

create index idx_processed_event_aggregate
  on processed_event (aggregate_id, processed_at desc);
```

### 8.4 One Consumer Group Per Read Model

| Read model | Consumer group | Topic |
|------------|----------------|-------|
| `order_detail_view` | `order-detail-projector` | `order.events` |
| `customer_order_history_view` | `order-history-projector` | `order.events` |
| OpenSearch index | `order-search-projector` | `order.events` |

Same topic, independent lag, independent scaling, independent failure domains.

### 8.5 Projection Strategies

| Strategy | When to use |
|----------|-------------|
| **Upsert from event payload** | Event carries full state needed for view |
| **Delta update** | Event carries partial change (`markPaid`) |
| **Event accumulation → snapshot** | Complex aggregations over event stream |
| **Dual-write during migration** | Rebuild new model while old still serves queries |

### 8.6 Projector Failure Modes

| Failure | Symptom | Mitigation |
|---------|---------|------------|
| Projector down | Lag grows, queries stale | HPA on lag, page on SLO breach |
| Bug in projection logic | Systematic bad data | Pause consumer, rebuild from checkpoint |
| Schema mismatch | Poison events | DLQ, fix schema, replay |
| Slow read DB | Consumer max.poll exceeded | Batch size tuning, async indexing |

---

## 9. Ordering, Sequence Guards, and Idempotency

### 9.1 Kafka Ordering Reality

Kafka guarantees order **within a partition**.

Production setup:

1. Use `aggregateId` as message **key**.
2. All events for one order land in the same partition.
3. Consumer processes sequentially per partition assignment.

**Staff-level rule:** Do not say "Kafka keeps order" and stop. Explain what your **database update** does when an old event arrives.

### 9.2 Sequence Guard SQL

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

If zero rows updated:

1. **Duplicate** (same sequence) — likely already applied; check `last_event_id`.
2. **Stale event** (sequence older than current) — drop and metric `stale_event_dropped`.
3. **Missing prior event** (sequence gap) — alert; may need quarantine or replay from write side.

### 9.3 Insert-If-Absent for First Event

```sql
insert into order_detail_view (
  order_id, customer_id, customer_name, status,
  payment_status, shipment_status, total_amount, lines,
  last_event_id, last_sequence, updated_at
)
values (...)
on conflict (order_id) do update
set status = excluded.status,
    last_event_id = excluded.last_event_id,
    last_sequence = excluded.last_sequence,
    updated_at = excluded.updated_at
where order_detail_view.last_sequence < excluded.last_sequence;
```

### 9.4 Idempotency Layers (Defense in Depth)

| Layer | Mechanism | Protects against |
|-------|-----------|------------------|
| Command side | `processed_command` | Duplicate HTTP POST |
| Outbox relay | `eventId` uniqueness | Duplicate publish |
| Projector | `processed_event` | Duplicate consume |
| Read model | `last_sequence` guard | Out-of-order apply |

### 9.5 When Ordering Breaks Anyway

Ordering breaks when:

1. Manual event republish without sequence discipline.
2. Replay tool publishes batch out of order.
3. Multiple topics merged without per-aggregate sorting.
4. Key=null messages round-robin across partitions.

Runbook: quarantine aggregate, rebuild read model row from write DB snapshot + event replay in sequence order.

---

## 10. Eventual Consistency and Read-Your-Writes UX

### 10.1 The Consistency Gap

Classic user flow:

1. Client `POST /orders` → command succeeds, returns `orderId`.
2. Client immediately `GET /orders/{orderId}`.
3. Read model may **not** contain the order yet (projection lag 50ms–5s+).

This is not a bug. It is the explicit trade-off of CQRS. **Product and API design must acknowledge it.**

### 10.2 UX Strategies (Production Menu)

| Strategy | Mechanism | Best for |
|----------|-----------|----------|
| **Return write result** | Command response includes authoritative fields | Mobile create flows |
| **202 + poll** | Query returns `202 Accepted` until caught up | API-first clients |
| **minSequence query param** | Client passes `?minSequence=1` | SPA read-your-writes |
| **Read-your-writes token** | Cookie/header with expected sequence | Web sessions |
| **Write-side fallback (short TTL)** | Query service reads write DB if projection lag > threshold | Critical financial views |
| **Optimistic UI** | Client shows pending state until poll succeeds | Consumer apps |

### 10.3 minSequence Pattern

Command response:

```json
{
  "orderId": "O-9001",
  "expectedSequence": 1,
  "status": "PLACED"
}
```

Query:

```http
GET /orders/O-9001?minSequence=1
```

Query handler:

```java
@GetMapping("/orders/{orderId}")
ResponseEntity<OrderDetailResponse> getOrder(
        @PathVariable String orderId,
        @RequestParam(required = false) Long minSequence) {

    var view = repository.findById(orderId);

    if (view.isEmpty()) {
        if (minSequence != null) {
            return ResponseEntity.status(HttpStatus.ACCEPTED)
                .header("Retry-After", "1")
                .build();
        }
        throw new ResponseStatusException(HttpStatus.NOT_FOUND);
    }

    if (minSequence != null && view.get().lastSequence() < minSequence) {
        return ResponseEntity.status(HttpStatus.ACCEPTED)
            .header("Retry-After", "1")
            .body(toResponse(view.get())); // optional partial stale body
    }

    return ResponseEntity.ok(toResponse(view.get()));
}
```

### 10.4 Response Freshness Fields

```json
{
  "orderId": "O-9001",
  "status": "PAID",
  "paymentStatus": "PAID",
  "shipmentStatus": "PENDING",
  "totalAmount": "2499.00",
  "viewUpdatedAt": "2026-06-02T10:16:08Z",
  "lastSequence": 2,
  "readModelLagMs": 120
}
```

Mobile apps can show: "Updating..." when `readModelLagMs > 2000`.

### 10.5 Write-Side Fallback (Use Sparingly)

```java
public Optional<OrderDetailResponse> findOrder(String orderId, Long minSequence) {
    var view = readRepository.findById(orderId);
    if (view.isPresent() && (minSequence == null || view.get().lastSequence() >= minSequence)) {
        return Optional.of(toResponse(view.get()));
    }
    // Fallback: authoritative read from write service (internal gRPC/REST)
    if (minSequence != null && minSequence <= 2) {
        return writeSideReadClient.fetchOrderSnapshot(orderId);
    }
    return view.map(this::toResponse);
}
```

Rules:

1. Fallback is **read-only**, narrow, and internal.
2. Time-box or sequence-box (`minSequence <= 2`) — not permanent bypass.
3. Metric `cqrs.query.write_fallback_total` — if this grows, fix projection lag.

### 10.6 Business Consistency Tiers

| Domain | Acceptable lag | UX approach |
|--------|----------------|-------------|
| Product catalog | 1–30 seconds | Stale badge optional |
| Order status after checkout | 0–2 seconds | minSequence + poll |
| Account balance | Zero | Do not use CQRS for balance reads without strong guarantees |
| Analytics dashboard | 1–5 minutes | Timestamp disclaimer |

---

## 11. Read Model Rebuild Strategy

### 11.1 Core Principle

If you cannot rebuild a read model, it has quietly become **another source of truth** — and you no longer have CQRS.

### 11.2 Rebuild Sources

| Source | Requirement | Retention |
|--------|-------------|-----------|
| Kafka event log | Events retained long enough | `retention.ms` >= max downtime + rebuild time |
| Event store (Event Sourcing) | Full history | Permanent or policy-driven |
| Write DB snapshot + delta events | Snapshot tooling | Snapshot frequency defined |
| CDC stream | Debezium from write tables | Connector lag monitored |
| Batch export | One-time migration | Ad hoc |

### 11.3 Zero-Downtime Rebuild Process

```text
Phase 1: CREATE order_detail_view_v2 (empty)
Phase 2: Replay events from offset 0 (or snapshot point) into v2
Phase 3: Validate counts + checksums (v1 vs v2)
Phase 4: Dual-write projector → v1 AND v2 (short window)
Phase 5: Switch query API feature flag to v2
Phase 6: Monitor error rate + lag 24–72 hours
Phase 7: Drop v1 table / stop dual-write
```

Validation queries:

```sql
-- Row count parity (approximate — v2 may have extra tombstones)
select count(*) from order_detail_view;
select count(*) from order_detail_view_v2;

-- Aggregate checksum
select sum(last_sequence) from order_detail_view;
select sum(last_sequence) from order_detail_view_v2;

-- Sample diff
select v1.order_id
from order_detail_view v1
join order_detail_view_v2 v2 using (order_id)
where v1.status <> v2.status
   or v1.last_sequence <> v2.last_sequence
limit 100;
```

### 11.4 Rebuild Tooling (CLI)

```bash
# Replay from earliest offset into v2 (dedicated consumer group)
java -jar order-projector.jar \
  --mode=rebuild \
  --target-table=order_detail_view_v2 \
  --topic=order.events \
  --from-offset=earliest \
  --rate-limit=5000

# Validate and cutover
java -jar order-projector.jar --mode=validate --source=v1 --target=v2
```

### 11.5 Schema Evolution During Rebuild

When event schema changes (`OrderPlaced.v1` → `OrderPlaced.v2`):

1. Projector handles **both** types during transition.
2. Rebuild replays historical v1 events through v2 mapper.
3. Upcasters transform old payload → canonical internal model.

### 11.6 Disaster: Read Store Lost

1. Stop query service (or return `503` with clear message).
2. Projectors continue or pause — depends on whether store is empty vs corrupted.
3. Recreate empty read tables/indexes.
4. Replay from Kafka earliest retained offset.
5. If retention expired — restore write DB snapshot + CDC, or cold export from write side.
6. Gradual traffic restore with lag monitoring.

---

## 12. CQRS vs Event Sourcing — Decision Framework

### 12.1 They Are Different Concerns

| Aspect | CQRS | Event Sourcing |
|--------|------|----------------|
| Primary question | Should reads and writes use different models? | Should state be stored as events? |
| Write storage | Often relational current-state | Append-only event log |
| Read side | Projections from events or CDC | Typically projections from event log |
| Replay | Rebuild read models | Rebuild aggregates AND read models |
| Complexity | Medium | High |

**CQRS does not imply Event Sourcing.**  
**Event Sourcing often implies CQRS** for query performance.

### 12.2 CQRS Without Event Sourcing

Use when:

1. Read/write separation is the main goal.
2. Full historical replay as source of truth is not required.
3. Team wants lower operational complexity.
4. Integration events derived from state change are sufficient.

Write path stores `orders` row. Outbox publishes `OrderPlaced.v1` **after** state is persisted.

### 12.3 CQRS With Event Sourcing

Use when:

1. Audit history is core to the domain (finance, healthcare).
2. Temporal queries ("what was the balance on date X?") are first-class.
3. State transitions are naturally modeled as events.
4. Team can operate event store, snapshots, upcasting.

Write path appends to `order_events` stream. Aggregate loaded via replay. Snapshots every N events.

### 12.4 Comparison Table for Interviews

| Question | CQRS only | CQRS + ES |
|----------|-----------|-----------|
| Source of truth | Relational write DB | Event log |
| Bug in projection | Rebuild from events/CDC | Replay events |
| Bug in aggregate logic | Fix forward + migration | Fix + replay (harder) |
| Storage growth | Moderate | Events accumulate — plan compaction/archival |
| Hiring/ops | Moderate skill bar | High skill bar |

### 12.5 Hybrid (Pragmatic Enterprise)

Common in Java shops:

- **Core transactional services**: CQRS + outbox, no ES.
- **Audit-heavy subdomain**: Event Sourcing for one bounded context.
- **Analytics**: CDC to warehouse, not ES.

---

## 13. Spring Boot Command and Query Services

### 13.1 Module / Service Split

```
order-platform/
  order-command-service/     # Spring Boot — writes only
  order-query-service/       # Spring Boot — reads only
  order-projector/           # Spring Boot — Kafka consumers
  order-domain/              # Shared domain types (commands, events — NOT repositories)
  order-contracts/           # Avro/JSON schemas, API DTOs
```

**Do not share JPA entities between command and query services.** Share contracts and event schemas only.

### 13.2 Command Service Skeleton

```java
@SpringBootApplication
@EnableScheduling  // outbox relay
public class OrderCommandApplication {
    public static void main(String[] args) {
        SpringApplication.run(OrderCommandApplication.class, args);
    }
}

@RestController
@RequestMapping("/commands/orders")
@Validated
class OrderCommandController {

    private final PlaceOrderHandler placeOrderHandler;

    @PostMapping
    ResponseEntity<CommandResponse> placeOrder(@Valid @RequestBody PlaceOrderCommand command) {
        CommandResult result = placeOrderHandler.handle(command);
        return switch (result) {
            case CommandAccepted(var id, var seq) -> ResponseEntity
                .status(HttpStatus.CREATED)
                .body(new CommandResponse(id, seq, false));
            case CommandDuplicate(var id) -> ResponseEntity
                .ok(new CommandResponse(id, null, true));
            case CommandRejected(var code, var msg) -> ResponseEntity
                .status(HttpStatus.CONFLICT)
                .body(CommandResponse.error(code, msg));
        };
    }
}
```

### 13.3 Query Service Skeleton

```java
@SpringBootApplication
public class OrderQueryApplication {
    public static void main(String[] args) {
        SpringApplication.run(OrderQueryApplication.class, args);
    }
}

@RestController
@RequestMapping("/queries/orders")
class OrderQueryController {

    private final OrderDetailQueryService queryService;

    @GetMapping("/{orderId}")
    ResponseEntity<OrderDetailResponse> getOrder(
            @PathVariable String orderId,
            @RequestParam(required = false) Long minSequence) {
        return queryService.findOrder(orderId, minSequence)
            .map(ResponseEntity::ok)
            .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @GetMapping
    Page<CustomerOrderSummary> customerOrders(
            @RequestParam String customerId,
            Pageable pageable) {
        return queryService.customerOrderHistory(customerId, pageable);
    }
}
```

### 13.4 Separate DataSources

```java
@Configuration
public class QueryDataSourceConfig {

    @Bean
    @ConfigurationProperties("spring.datasource.read")
    public DataSourceProperties readDataSourceProperties() {
        return new DataSourceProperties();
    }

    @Bean
    public DataSource readDataSource() {
        return readDataSourceProperties()
            .initializeDataSourceBuilder()
            .build();
    }

    @Bean
    public JdbcTemplate readJdbcTemplate(@Qualifier("readDataSource") DataSource ds) {
        return new JdbcTemplate(ds);
    }
}
```

Command service owns write datasource + outbox. Query service connects **only** to read replicas / read store.

### 13.5 Security Boundaries

| Service | AuthZ focus |
|---------|-------------|
| Command | Role must allow mutation (`PLACE_ORDER`, `CANCEL_ORDER`) |
| Query | Role may allow read-only scopes; field-level filtering |

Query service should not expose internal projection tables directly — use repository layer.

### 13.6 Testing Strategy

| Test type | Command service | Query service | Projector |
|-----------|-----------------|-----------------|-----------|
| Unit | Aggregate invariants | Query mappers | Event → SQL mapping |
| Integration | `@SpringBootTest` + Testcontainers PG | Read repo queries | `@EmbeddedKafka` + Testcontainers |
| Contract | Command API OpenAPI | Query API OpenAPI | Avro compatibility |
| E2E | Post command → wait → assert query | — | Lag-aware awaitility |

```java
@Test
void placeOrderEventuallyVisibleInQuery() {
    var cmd = new PlaceOrderCommand(UUID.randomUUID(), "C-1001", List.of(line("SKU-9", 2)));
    var accepted = commandClient.placeOrder(cmd);

    await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
        var response = queryClient.getOrder(accepted.orderId(), accepted.expectedSequence());
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody().status()).isEqualTo("PLACED");
    });
}
```

---

## 14. Kafka Projectors — Production Configuration

See [kafka-expert-playbook.md](kafka-expert-playbook.md) for broker internals. This section covers **CQRS projector-specific** Kafka settings.

### 14.1 Topic Design

```properties
# order.events
partitions=24                    # scale projectors horizontally
replication.factor=3
min.insync.replicas=2
retention.ms=604800000           # 7 days — must exceed rebuild window
cleanup.policy=delete            # not compact — we need full history for rebuild
```

Key = `aggregateId` (order id). Hot keys (large customers) may need dedicated handling — see kafka playbook Section 6.

### 14.2 Consumer Configuration (Projector)

```properties
spring.kafka.consumer.group-id=order-detail-projector
spring.kafka.consumer.enable-auto-commit=false
spring.kafka.consumer.auto-offset-reset=earliest
spring.kafka.consumer.isolation-level=read_committed
spring.kafka.consumer.max-poll-records=100
spring.kafka.consumer.max-poll-interval-ms=300000
spring.kafka.listener.ack-mode=MANUAL_IMMEDIATE
spring.kafka.listener.concurrency=6
```

### 14.3 Error Handler + DLQ (Spring Boot 3.x)

```java
@Bean
public ConcurrentKafkaListenerContainerFactory<String, OrderEvent> manualAckKafkaListenerContainerFactory(
        ConsumerFactory<String, OrderEvent> consumerFactory,
        KafkaTemplate<String, OrderEvent> kafkaTemplate) {

    var factory = new ConcurrentKafkaListenerContainerFactory<String, OrderEvent>();
    factory.setConsumerFactory(consumerFactory);
    factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

    var recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
        (record, ex) -> new TopicPartition("order.events.dlq", record.partition()));

    var errorHandler = new DefaultErrorHandler(recoverer,
        BackOff.fixedBackOff(1000L, 3)); // 3 retries, 1s apart

    errorHandler.addNotRetryableExceptions(
        UnsupportedEventTypeException.class,
        JsonParseException.class
    );

    factory.setCommonErrorHandler(errorHandler);
    return factory;
}
```

### 14.4 Multiple Projectors, Same Topic

Each projector = separate consumer group. Kafka delivers a copy per group. Lag is independent.

Monitor:

```
kafka.consumer_group.lag{group="order-detail-projector"}
kafka.consumer_group.lag{group="order-search-projector"}
```

### 14.5 Throughput Tuning

| Knob | Effect |
|------|--------|
| `concurrency` | Should be ≤ partition count |
| Batch JDBC inserts | Reduces DB round trips for history projector |
| Async OpenSearch bulk | Higher search projector throughput |
| Rate limit on rebuild | Protects read store during replay |

---

## 15. Metrics, Logging, and Tracing

### 15.1 Command Side Metrics

```java
// Micrometer examples
Counter.builder("cqrs.command.received")
    .tag("command", "PlaceOrder")
    .register(meterRegistry);

Timer.builder("cqrs.command.latency")
    .tag("command", "PlaceOrder")
    .publishPercentiles(0.5, 0.95, 0.99)
    .register(meterRegistry);

Counter.builder("cqrs.command.idempotent_replay")
    .tag("command", "PlaceOrder")
    .register(meterRegistry);
```

| Metric | Type | Alert |
|--------|------|-------|
| `cqrs.command.received_total` | Counter | — |
| `cqrs.command.success_total` | Counter | Success rate drop |
| `cqrs.command.failure_total` | Counter | Spike by error code |
| `cqrs.command.idempotent_replay_total` | Counter | Informational |
| `cqrs.command.latency_ms` | Timer | p99 > SLO |

### 15.2 Projector Metrics

| Metric | Type | Alert |
|--------|------|-------|
| `cqrs.projection.lag_seconds` | Gauge | > 120s for 10 min |
| `cqrs.projection.event_processed_total` | Counter | — |
| `cqrs.projection.event_failed_total` | Counter | > 0 sustained |
| `cqrs.projection.duplicate_dropped_total` | Counter | Informational |
| `cqrs.projection.stale_event_dropped_total` | Counter | Spike = ordering bug |
| `cqrs.projection.dlq_total` | Counter | **Any increment → page** |

Lag gauge derivation:

```java
@Scheduled(fixedRate = 30_000)
public void recordProjectionLag() {
    long lag = kafkaAdminClient.consumerGroupLag("order-detail-projector", "order.events");
    meterRegistry.gauge("cqrs.projection.lag_seconds", lag);
}
```

### 15.3 Query Side Metrics

| Metric | Type | Alert |
|--------|------|-------|
| `cqrs.query.latency_ms` | Timer | p99 > SLO |
| `cqrs.query.not_found_total` | Counter | Spike after deploy |
| `cqrs.read_model.age_seconds` | Gauge | Freshness SLO |
| `cqrs.query.cache.hit_ratio` | Gauge | Drop below threshold |
| `cqrs.query.write_fallback_total` | Counter | Should be near zero |

### 15.4 Structured Logging

Command log (JSON):

```json
{
  "level": "INFO",
  "message": "Command processed",
  "command_id": "7d6bb27e-20ef-4d25-a5f3-b1edcf9b0e83",
  "command_type": "PlaceOrder",
  "aggregate_id": "O-9001",
  "aggregate_type": "Order",
  "sequence": 1,
  "result": "ACCEPTED",
  "trace_id": "abc123",
  "duration_ms": 45
}
```

Event / projection log:

```json
{
  "level": "INFO",
  "message": "Event projected",
  "event_id": "01HR...",
  "event_type": "OrderPlaced.v1",
  "aggregate_id": "O-9001",
  "sequence": 1,
  "projector": "order-detail-projector",
  "correlation_id": "abc123",
  "rows_updated": 1
}
```

### 15.5 Distributed Trace Flow

```text
HTTP POST /commands/orders          [trace_id: abc]
  -> PlaceOrderHandler              [span: command.handle]
  -> PostgreSQL commit              [span: db.transaction]
  -> Outbox insert                  [span: outbox.insert]
Outbox relay                        [trace_id: abc, parent: command]
  -> Kafka publish                  [span: kafka.produce]
Projector consume                   [trace_id: abc]
  -> Read model update              [span: projection.apply]
HTTP GET /queries/orders/O-9001     [trace_id: def, linked: abc]
```

Without correlation IDs, CQRS incidents become archaeology.

OpenTelemetry: inject `trace_id` into outbox `headers` and Kafka record headers.

---

## 16. DLQ, Poison Events, and Replay Tooling

### 16.1 Poison Event Definition

A poison event causes the projector to **fail deterministically** on every retry — schema mismatch, bad enum, null required field, logic bug.

Symptom: consumer stuck, lag infinite, all partitions behind one bad offset.

### 16.2 DLQ Workflow

```text
order.events  -->  projector  --(fail 3x)-->  order.events.dlq
                                                    |
                                              ops dashboard
                                                    |
                                              fix code/schema
                                                    |
                                              replay tool  --> order.events (or dedicated replay topic)
```

DLQ message retains:

1. Original payload + headers.
2. Failure reason + stack hash.
3. Source topic/partition/offset.
4. `event_id` for idempotency on replay.

### 16.3 DLQ Consumer (Ops Visibility)

```java
@KafkaListener(topics = "order.events.dlq", groupId = "dlq-monitor")
public void onDlq(ConsumerRecord<String, OrderEvent> record) {
    alertService.page("CQRS DLQ", Map.of(
        "event_id", record.value().eventId(),
        "event_type", record.value().eventType(),
        "error", new String(record.headers().lastHeader("exception").value())
    ));
    dlqRepository.save(record);
}
```

### 16.4 Replay Tool Requirements

Production replay CLI / admin API:

1. Replay by `event_id`, `aggregate_id`, offset range, or time range.
2. Target specific projector consumer group (or rebuild table).
3. Rate limiting to protect read DB.
4. Dry-run mode (validate mapping without write).
5. Audit log of who triggered replay.

```java
public void replayEvent(UUID eventId, String targetProjector) {
    var event = dlqRepository.findByEventId(eventId)
        .orElseThrow();
    // Re-invoke projector handler directly OR republish to topic
    projectorRegistry.get(targetProjector).project(event);
}
```

### 16.5 Safe Replay Checklist

1. Root cause fixed (code deployed).
2. Idempotency tables intact — replay will hit `processed_event` guard.
3. Sequence guards prevent stale overwrites.
4. Monitor lag during replay.
5. Validate sample aggregates post-replay.

### 16.6 When NOT to Replay

If bug caused **wrong but valid** projections (e.g., wrong tax calculation):

1. Pause projector.
2. Identify affected aggregate range (SQL on read model).
3. Delete or quarantine affected rows.
4. Selective replay or full rebuild.

---

## 17. Production Issue Runbook

### 17.1 Projection Lag Spiking

**Symptoms:** `cqrs.projection.lag_seconds` > SLO, users report stale data.

**Diagnosis:**

1. Check consumer group lag per projector.
2. Check read DB CPU/IO saturation.
3. Check for poison event (single partition lag anomalously high).
4. Check recent deploy (slow handler code path).

**Mitigation:**

1. Scale projector pods (concurrency ≤ partitions).
2. If poison — route to DLQ (Section 16).
3. If read DB hot — throttle rebuild/replay, add read replica.
4. Temporary: enable write-side fallback for critical queries (Section 10.5).

**Escalation:** Lag > 15 min and growing → incident bridge.

---

### 17.2 Query Returns 404 After Successful Command

**Symptoms:** Client creates order, immediate GET returns 404.

**Diagnosis:**

1. Expected eventual consistency vs actual bug.
2. Check projection lag — is event consumed?
3. Check outbox relay backlog — event never published?
4. Check `processed_event` — projector dropped as duplicate incorrectly?

**Mitigation:**

1. If lag normal — fix client UX (minSequence poll).
2. If outbox backlog — scale relay, check Kafka availability.
3. If projector error — check DLQ.

---

### 17.3 Read Model Corruption (Wrong Status)

**Symptoms:** Support tickets, status mismatch between admin and customer view.

**Diagnosis:**

1. Compare write DB vs read model for sample `order_id`.
2. Check `last_sequence` gaps.
3. Review recent replay/deploy for mapping bug.

**Mitigation:**

1. Pause affected projector.
2. Delete corrupted read rows for affected aggregates OR full rebuild.
3. Replay events in order from known good offset.
4. Post-incident: add integration test for event type.

---

### 17.4 Outbox Relay Stalled

**Symptoms:** `outbox_event` PENDING rows growing, projectors idle.

**Diagnosis:**

1. Kafka broker health.
2. Relay pod crashes / DB lock contention.
3. Serialization errors on publish.

**Mitigation:**

1. Fix Kafka connectivity.
2. Scale relay instances (`SKIP LOCKED` polling).
3. Move failed rows to FAILED status, alert, fix schema.

See [outbox-expert-playbook.md](outbox-expert-playbook.md) runbook section.

---

### 17.5 Duplicate Orders (Idempotency Failure)

**Symptoms:** Same `commandId` or client retry created two orders.

**Diagnosis:**

1. `processed_command` table missing or not checked.
2. Transaction boundary wrong — command partial commit.
3. Client generates new `commandId` on retry.

**Mitigation:**

1. Hotfix idempotency check.
2. Manual merge/cancel duplicate orders.
3. Client SDK fix — stable idempotency key per user action.

---

### 17.6 OpenSearch Index Drift

**Symptoms:** Admin search missing orders that detail view shows.

**Diagnosis:**

1. Search projector lag vs detail projector lag.
2. Bulk indexing failures silently swallowed?
3. Mapping conflict on new field.

**Mitigation:**

1. Reindex from `order_detail_view` (batch) or replay Kafka.
2. Fix mapping version, deploy projector fix.
3. Dual-run index alias swap.

---

### 17.7 Incident Communication Template

```text
Impact: Order query API showing stale status up to X minutes
Root cause: order-detail-projector lag due to [poison event / DB saturation]
Write path: UNAFFECTED — orders can still be placed
Mitigation: [Scaled projectors / DLQ routed / replay in progress]
ETA: Read models caught up to < 60s lag by HH:MM UTC
Workaround: Clients should retry GET with minSequence or poll Retry-After
```

---

## 18. When NOT to Use CQRS

### 18.1 Hard Disqualifiers

1. **Simple CRUD** — few entities, one admin UI, low traffic.
2. **Strong consistency required on all reads** — banking ledger displays without careful design.
3. **Team cannot operate async pipelines** — no DLQ discipline, no lag alerts.
4. **Read models cannot be rebuilt** — projection becomes shadow source of truth.
5. **Premature split** — complexity introduced before read/write pain is real.

### 18.2 Soft Warning Signs

1. Single team owns everything — operational split adds friction without scaling benefit.
2. No event retention policy — rebuild impossible after 7-day Kafka retention.
3. Product not willing to design for eventual consistency UX.
4. Regulatory audit requires synchronous cross-service reads.

### 18.3 Staff-Level Pushback (Interview Gold)

> "If your entire app has five tables and one admin UI, CQRS is ceremony.  
> If your order table is serving checkout, search, dashboards, exports, and mobile views, CQRS starts paying rent."

### 18.4 Alternatives to Consider First

| Need | Try first |
|------|-----------|
| Read scaling | Read replicas + caching |
| Search | CDC to OpenSearch without full CQRS |
| Reporting | Read replica + materialized views |
| Complex writes | Domain model in monolith |
| Cross-service workflow | Saga without separate query service |

### 18.5 Production Readiness Checklist

Before adopting CQRS in production:

- [ ] Commands have idempotency keys (`commandId`).
- [ ] Write model enforces invariants in aggregates.
- [ ] Events published through outbox or reliable CDC ([outbox-expert-playbook.md](outbox-expert-playbook.md)).
- [ ] Event payloads versioned (`EventType.v1`).
- [ ] Projectors idempotent (`processed_event` + sequence guards).
- [ ] Read models track `last_event_id` and `last_sequence`.
- [ ] Projection lag measured and alerted.
- [ ] Read model rebuild runbook documented and tested.
- [ ] Query API handles stale/missing projections (`202`, `minSequence`).
- [ ] DLQ and replay tooling exist.
- [ ] Schema evolution rules documented.
- [ ] Consumer-facing APIs document eventual consistency assumptions.
- [ ] Saga workflows use outbox per service ([saga.md](saga.md)).

---

## 19. Lead Interview Questions — Logical and Production Scenarios

### Category A: Fundamentals

**Q1: What is CQRS and what problem does it solve?**

A: CQRS separates the model used for writes (commands) from the model used for reads (queries). It solves the problem of one database schema and one API being forced to serve conflicting workloads — transactional correctness on writes vs performance and flexibility on reads. The write side enforces invariants; the read side is optimized for specific query shapes, updated asynchronously via events.

**Q2: Is CQRS the same as Event Sourcing?**

A: No. CQRS is about separating read and write models. Event Sourcing stores state as an append-only event log. You can do CQRS without ES (relational write DB + integration events). ES usually implies CQRS because replaying events is not a query-friendly primary interface.

**Q3: Does CQRS require two physical databases?**

A: Not strictly at small scale — you can start with separate tables in one PostgreSQL instance. Production at scale typically separates write primary from read replicas or dedicated read stores (OpenSearch, Redis). The logical separation matters more than the physical topology early on.

**Q4: What is a command vs a query in API design?**

A: A command expresses intent to mutate state (`PlaceOrder`, `CancelOrder`) and should be handled idempotently. A query returns data without side effects (`GetOrderDetail`, `SearchOrders`). In REST, commands often map to POST on resource-specific command endpoints; queries map to GET. Queries never call command repositories.

**Q5: What is an aggregate in CQRS?**

A: A consistency boundary — a cluster of domain objects updated together in one transaction. The aggregate root enforces invariants. External references are by ID only. One command typically mutates one aggregate instance per transaction.

---

### Category B: Architecture and Boundaries

**Q6: How do you split command and query services without duplicating all business logic?**

A: Share the **domain model** and **event contracts**, not repositories or persistence entities. Command service owns aggregates + write repos + outbox. Query service owns read repos + query DTOs. Projectors own event → read model mapping. Validation rules live in the domain layer on the write side; read side trusts projected data.

**Q7: Can the query service ever read the write database?**

A: Only as a **narrow, temporary fallback** for read-your-writes with strict guards — not as the default path. Permanent bypass means you don't have CQRS. If fallback metrics climb, fix projection lag instead.

**Q8: How does CQRS interact with Saga?**

A: Each saga participant typically has its own write model and publishes events via outbox. The orchestrator sends commands to participants; projectors on each service build local read models. Saga compensations are commands too. See [saga.md](saga.md).

**Q9: One Kafka topic or many for domain events?**

A: Start with one domain topic per bounded context (`order.events`) keyed by aggregate ID. Split into derived topics when consumers need different retention, ACLs, or fan-out patterns. Avoid topic explosion per event type.

**Q10: Where does API Gateway route — command or query service?**

A: Route `POST/PUT/DELETE` command endpoints to command service. Route `GET` to query service. Some teams use separate subdomains: `commands.api.example.com` vs `queries.api.example.com` for clarity and independent rate limits.

---

### Category C: Outbox and Event Publishing

**Q11: Why not publish to Kafka directly in the command handler after DB commit?**

A: Crash between commit and publish loses the event. Publish before commit creates ghost events. Outbox co-locates event insert with business write in one transaction — see [outbox-expert-playbook.md](outbox-expert-playbook.md).

**Q12: Polling outbox vs Debezium CDC — how do you choose?**

A: Polling is simpler, good for MVP and moderate throughput. Debezium gives lower latency and avoids poll load but adds Kafka Connect ops. Many teams start polling, migrate to CDC when relay lag or DB load becomes painful.

**Q13: What if the outbox relay publishes duplicate events?**

A: Expected under at-least-once. Projectors dedupe by `eventId`. Read models guard by `last_sequence`. Downstream saga steps must be idempotent too.

**Q14: Should domain events contain full aggregate state or deltas?**

A: Trade-off. Full state simplifies projectors and rebuilds but increases payload size. Deltas are lean but require projectors to handle partial updates and more event types. For rebuild-friendly systems, bias toward events that carry enough data to project without reading write DB.

---

### Category D: Projectors and Idempotency

**Q15: How do you make projectors idempotent?**

A: Three layers: (1) `processed_event` table skip if `eventId` seen, (2) SQL updates with `WHERE last_sequence < :sequence`, (3) design projections so re-application is safe (upserts, derived state from sequence).

**Q16: Kafka preserves order — why do you still need sequence guards?**

A: Replays, manual republishes, consumer restarts, multiple publishers, and bugs all deliver duplicates or out-of-order events. Partition ordering is necessary but not sufficient. The read model must defend itself.

**Q17: One projector or many for multiple read models?**

A: Separate consumer group per read model — independent lag, scaling, failure. Never one projector updating five stores in one giant transaction if you can avoid it — partial failure becomes hard to reason about.

**Q18: When would you use Kafka Streams instead of a Spring `@KafkaListener` projector?**

A: When projection requires streaming joins, windowed aggregations, or stateful transforms across events. Simple CRUD-style read model updates are clearer as listener + JDBC.

---

### Category E: Eventual Consistency and UX

**Q19: User creates order and gets 404 on immediate refresh — how do you fix it?**

A: Product + API fix, not "make Kafka faster" alone. Return `expectedSequence` from command, support `minSequence` on GET with `202 Retry-After`, or optimistic UI. Optionally short-lived write-side fallback for critical paths.

**Q20: Is eventual consistency acceptable for payment status?**

A: Depends on domain. Order **placement** confirmation can come from command response (write authoritative). **Payment captured** display may need sub-second projection or read-your-writes token. Bank **balance** usually should not use naive CQRS without strong guarantees.

**Q21: How do you explain stale reads to product managers?**

A: "After you save, the listing page updates in under 2 seconds" — set SLO, show `viewUpdatedAt`, design UI for pending states. Match consistency tier to business risk.

---

### Category F: Rebuild and Operations

**Q22: How do you rebuild a read model without downtime?**

A: Build `view_v2`, replay events, validate checksums, dual-write projectors briefly, feature-flag query API to v2, monitor, drop v1. See Section 11.

**Q23: Kafka retention is 7 days and you lost the read database — can you recover?**

A: Only if events are still in Kafka or you have write DB snapshot + CDC. If neither — rebuild from write side export (slow, lossy for denormalized enrichments). This is why retention and rebuild runbooks are non-negotiable.

**Q24: What metrics do you alert on first for CQRS?**

A: `projection.lag_seconds`, `dlq_total > 0`, command failure rate spike, outbox PENDING backlog age, query p99 latency. Lag is the heartbeat of the system.

**Q25: Walk through a poison event incident.**

A: Projector fails on one message, partition stuck, lag grows. DefaultErrorHandler retries, then DLQ. Ops paged, inspect DLQ payload, find schema bug, deploy fix, replay event by `eventId`, verify read model, confirm lag drains. Document post-mortem.

---

### Category G: Advanced / Staff Level

**Q26: How do you version events (`OrderPlaced.v1` → `v2`)?**

A: Projector handles both during migration. Prefer additive schema changes. Upcasters for ES. Never mutate published events. Consumer-driven contract tests in CI.

**Q27: CQRS in a modular monolith — worth it?**

A: Yes, as a **logical** split first — separate packages, separate tables, internal event bus, one deployable. Physical split into services when scaling or team boundaries justify ops cost.

**Q28: How do you test projection logic?**

A: Unit test: given event X, assert SQL/state change. Integration: `@EmbeddedKafka` + Testcontainers, publish event, assert read row. Property-based: random event sequences, assert invariants on read model.

**Q29: What is the biggest CQRS failure mode you've seen?**

A: Read model becomes de facto source of truth — teams make business decisions from stale/wrong projections, no rebuild path, no sequence guards. Or: CQRS adopted on day one of a todo app — team drowns in ops.

**Q30: When would you NOT use CQRS?**

A: Simple CRUD, strong consistency everywhere, no async ops maturity, no rebuild story. See Section 18.

---

### Category H: Curveball Scenarios

**Q31: Two projectors consume same event — one fails, one succeeds. Is that OK?**

A: Yes — independent consumer groups, independent failure domains. Fix the failing projector without stopping the healthy one. Business may accept search lag while detail view is fresh.

**Q32: Command succeeds but outbox event has wrong payload. How bad is it?**

A: Very bad — write DB correct, read models corrupt. Requires replay from corrected events or rebuild from write DB. Prevention: integration tests, schema validation before outbox insert, canary projector in staging.

**Q33: Customer asks for "real-time dashboard" — CQRS or not?**

A: CQRS read model fed by events often **is** the real-time dashboard path — projectors maintain rollups. Define "real-time" (1s vs 1min). May use Kafka Streams windows or materialized views.

**Q34: How does CQRS affect GDPR right-to-erasure?**

A: Erasure is a **command** on write model + cascade to read models and search indexes. Projectors must handle `CustomerErased.v1`. Rebuild archives may retain tombstones — legal review required.

**Q35: Monolith migrating to CQRS — first step?**

A: Identify hottest read query. Build one read model + projector. Command path unchanged except outbox. Query path reads new view. Do not split services until pattern proves value.

---

## 20. How to Talk About CQRS in an Interview

> Say it like this. Casual and clear. Short sentences.

---

### "What is CQRS?"

CQRS just means — reading and writing are different jobs, so don't force one model to do both.

When you save data, you need validation, business rules, and transactions. When you fetch data, you just need it fast — maybe search, filters, dashboards.

CQRS says — have a write side that handles saving correctly, and a read side that's shaped for how people actually query. They stay in sync through events, not by sharing the same tables.

---

### "Why split into separate databases?"

Once read and write are separate concepts, you can store them differently.

Write side — normal relational DB, good for rules and transactions. Read side — whatever fits: same DB with different tables, a replica, Redis, Elasticsearch.

The catch — they're not always in sync instantly. You write an order, the read view updates a moment later. For a product catalog, two seconds stale is fine. For a bank balance, you'd design that part differently.

---

### "How do you keep the read side in sync?"

Events. When something changes on the write side, you publish an event — like "order was placed." A projector listens and updates the read tables.

You do this reliably with the **outbox pattern** — write the business data and the event in the same database transaction, then a relay publishes to Kafka. See our [outbox expert playbook](outbox-expert-playbook.md).

If the read side gets wrong or you change the query shape, you replay events and rebuild it. The read model is disposable — the write DB is truth.

---

### "What about ordering and duplicates?"

Kafka keeps order per partition if you use the order ID as the key. But stuff still goes wrong — retries, replays, bugs.

So the read model stores a sequence number per order. When updating, you only apply the event if its sequence is newer than what's already there. And you track processed event IDs so duplicates don't double-count.

Don't just say "Kafka handles it." Say what your database does when an old event shows up.

---

### "What happens when the user saves and immediately refreshes?"

Classic CQRS moment. Command succeeded, but the read model might lag 500 milliseconds.

You handle it in the API and UI — return the order ID and expected version from the POST, let GET accept a minimum sequence and return 202 Retry-After if not caught up yet, or show a "saving..." state in the app.

You don't pretend it's instantly consistent unless you add a controlled fallback to the write DB for that one screen.

---

### "CQRS vs Event Sourcing — what's the difference?"

CQRS — separate read and write models. Your write DB can still be normal rows.

Event Sourcing — the events **are** the database. You rebuild state by replaying them.

You can do CQRS without Event Sourcing. That's what most teams should start with. Event Sourcing is heavier — great for audit-heavy domains, overkill for a lot of apps.

---

### "When does CQRS make things worse?"

Simple CRUD app — five tables, one admin screen — CQRS adds two models, projectors, Kafka, lag monitoring. All pain, no gain.

Also bad if the business says every read must be instantly consistent everywhere, and they're not willing to compromise on UX.

Use it when reads and writes are genuinely different — high read traffic, multiple query formats, complex write rules. Like orders serving checkout, mobile, admin search, and dashboards at once.

---

### "How does this relate to Saga and Outbox?"

**Outbox** — makes sure events actually get published when the write succeeds. CQRS projectors depend on that.

**Saga** — coordinates multi-service workflows without a giant distributed transaction. Each service has its own write model and publishes events. Your read models are separate per service.

They stack: command → outbox → Kafka → projectors on the read side; saga orchestrator drives cross-service commands. See [saga.md](saga.md).

---

### Quick Answers

| Question | Say this |
|----------|----------|
| What is CQRS? | Separate read model from write model — optimize each for its job |
| Why split databases? | Writes need correctness; reads need speed and query shapes |
| How do they stay in sync? | Domain events — write side publishes, projectors update read stores |
| What makes sync reliable? | Transactional outbox — event saved in same DB transaction as the write |
| Main tradeoff? | Reads can lag writes — eventual consistency |
| How handle immediate refresh? | Return sequence from command; poll with minSequence or optimistic UI |
| How handle duplicates? | Idempotent commands + processed event IDs + sequence guards on read model |
| CQRS vs Event Sourcing? | CQRS = split models; ES = events are the source of truth (optional add-on) |
| When to use? | High read load, multiple query formats, complex write rules |
| When not to use? | Simple CRUD — adds complexity for no real benefit |
| What is a projector? | Consumer that turns events into read-optimized tables or indexes |
| What is the source of truth? | Write database — read models are rebuildable |
| Key metric? | Projection lag — if it grows, users see stale data |
| Poison event? | Bad event blocks projector — retry, then DLQ, fix, replay |
| Related patterns? | Outbox for publish reliability; Saga for cross-service workflows |

---

## Appendix A: Reference Documentation Map

| Document | Relevance to CQRS |
|----------|-------------------|
| [outbox-expert-playbook.md](outbox-expert-playbook.md) | Reliable event publish from command service |
| [outbox.md](outbox.md) | Outbox pattern overview |
| [saga.md](saga.md) | Multi-service command coordination |
| [saga-orchestrator-sync-vs-async.md](saga-orchestrator-sync-vs-async.md) | Orchestrator latency vs reliability |
| [saga-prod-examples-sync-async.md](saga-prod-examples-sync-async.md) | Production saga examples |
| [kafka-expert-playbook.md](kafka-expert-playbook.md) | Projector consumer config, DLQ, ordering |
| [cqrs.md](cqrs.md) | Original production playbook (shorter form) |

---

## Appendix B: SQL and Schema Quick Reference

### Write Side

```sql
-- orders, order_line, processed_command, outbox_event
-- See Sections 4, 5, 6
```

### Read Side

```sql
-- order_detail_view, customer_order_history_view, processed_event
-- See Sections 7, 8, 9
```

### Sequence Guard Pattern

```sql
update <read_view>
set ..., last_sequence = :sequence, last_event_id = :event_id, updated_at = now()
where <aggregate_id_col> = :aggregate_id
  and last_sequence < :sequence;
```

---

## Appendix C: Spring Boot Property Summary

```properties
# Command service
spring.datasource.url=jdbc:postgresql://write-db/orders
outbox.relay.poll-ms=500
outbox.relay.batch-size=100

# Query service
spring.datasource.read.url=jdbc:postgresql://read-db/orders_read

# Projector
cqrs.order.events-topic=order.events
cqrs.projector.order-detail.group-id=order-detail-projector
spring.kafka.consumer.enable-auto-commit=false
spring.kafka.listener.ack-mode=manual_immediate
spring.kafka.listener.concurrency=6
```

---

*End of CQRS Expert Playbook*
