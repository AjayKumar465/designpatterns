# Java System Design — Revision Playbook (Lead/Architect, Interviews, Production)

> **Revision map** — scan before lead interviews, architecture reviews, or on-call escalations. Deep production depth with plain language. Targets Java Backend Lead, Staff Engineer, and Architect roles designing distributed systems on Spring Boot, PostgreSQL, Kafka, and Kubernetes.

A comprehensive end-to-end **revision hub** linking every expert playbook in this repo. Covers fundamentals, consistency models, API design, problem→pattern mapping, decision flowcharts, four end-to-end scenario walkthroughs, scalability, security/ops, 40+ lead-level Q&As, plain-English interview framing, and revision cheat sheets. Sourced from production war stories, Martin Fowler / Chris Richardson patterns, Spring ecosystem guides, and CNCF operational practice.

**Related playbooks in this repo — deep dives:**

| Playbook | When to open |
|----------|--------------|
| [Saga Expert Playbook](saga-expert-playbook.md) | Multi-service business transactions, compensation, orchestration |
| [Transactional Outbox Expert Playbook](outbox-expert-playbook.md) | Dual-write problem, reliable event publishing from DB |
| [CQRS Expert Playbook](cqrs-expert-playbook.md) | Separate read/write models, dashboards, search |
| [Kafka Expert Playbook](kafka-expert-playbook.md) | Event backbone, ordering, consumer groups, DLQ |
| [Circuit Breaker Expert Playbook](circuit-breaker-expert-playbook.md) | Cascading failure, fail-fast, Resilience4j |
| [Bulkhead Expert Playbook](bulkhead-expert-playbook.md) | Thread/connection pool isolation per dependency |
| [Strangler Fig Playbook](strangler-fig-playbook.md) | Monolith → microservices incremental migration |
| [Metrics & Observability Playbook](metrics-observability-playbook.md) | RED/USE, SLOs, Micrometer, on-call signals |
| [Kubernetes Expert Playbook](kubernetes-expert-playbook.md) | Runtime platform, probes, rollouts, debugging |
| [Two-Phase Commit (2PC)](two-phase-commit-2pc.md) | When (not) to use distributed atomic commit |
| [LRU Cache Expert Playbook](lru-cache-expert-playbook.md) | In-process caching, eviction, stampede prevention |
| [Spring Boot Production Revision Playbook](spring-boot-production-revision-playbook.md) | `@Transactional`, JPA, Actuator, graceful shutdown |
| [PostgreSQL & JPA Revision Playbook](postgresql-jpa-revision-playbook.md) | Indexes, locking, connection pools, query tuning |
| [JVM Performance Revision Playbook](jvm-performance-revision-playbook.md) | GC, heap sizing, profiling, virtual threads |
| [Java Concurrency & Streams Playbook](java-modern-concurrency-streams-playbook.md) | Virtual threads, `CompletableFuture`, parallel streams |
| [Custom Spring Boot Starter Playbook](custom-spring-boot-starter-expert-playbook.md) | Auto-configuration, internal platform libraries |

**Runnable examples in this repo:**

- `examples/saga/` — saga orchestration, compensation, idempotency
- `examples/circuit-breaker/` — CLOSED/OPEN/HALF_OPEN state machine
- `examples/custom-spring-boot-starter/` — Spring Boot 3 auto-configuration patterns

---

## Table of Contents

1. [System Design Fundamentals for Java Backend Leads](#section-1-system-design-fundamentals-for-java-backend-leads)
2. [Consistency Models — CAP, ACID, BASE, and What to Promise Users](#section-2-consistency-models--cap-acid-base-and-what-to-promise-users)
3. [API Design — Idempotency, Pagination, Versioning, Errors](#section-3-api-design--idempotency-pagination-versioning-errors)
4. [Problem → Pattern Matrix — Quick Routing Guide](#section-4-problem--pattern-matrix--quick-routing-guide)
5. [Decision Flowcharts — ASCII Decision Trees](#section-5-decision-flowcharts--ascii-decision-trees)
6. [Scenario Walkthrough 1 — Place Order (Happy Path)](#section-6-scenario-walkthrough-1--place-order-happy-path)
7. [Scenario Walkthrough 2 — Payment Failure and Compensation](#section-7-scenario-walkthrough-2--payment-failure-and-compensation)
8. [Scenario Walkthrough 3 — Read Dashboard Under Load](#section-8-scenario-walkthrough-3--read-dashboard-under-load)
9. [Scenario Walkthrough 4 — Monolith Migration (Strangler Fig)](#section-9-scenario-walkthrough-4--monolith-migration-strangler-fig)
10. [Scalability — Horizontal, Vertical, Data, and Cost](#section-10-scalability--horizontal-vertical-data-and-cost)
11. [Security and Operations — Production Baseline](#section-11-security-and-operations--production-baseline)
12. [Five-Layer Production Debugging Framework](#section-12-five-layer-production-debugging-framework)
13. [Lead Interview Questions — 40+ Logical and Production Scenarios](#section-13-lead-interview-questions--40-logical-and-production-scenarios)
14. [How to Talk About System Design in an Interview (Plain English)](#section-14-how-to-talk-about-system-design-in-an-interview-plain-english)
15. [Revision Cheat Sheet — Tables, One-Liners, and Checklists](#section-15-revision-cheat-sheet--tables-one-liners-and-checklists)
16. [Appendix — Cross-Playbook Study Order and Revision Calendar](#section-16-appendix--cross-playbook-study-order-and-revision-calendar)

---

## Section 1: System Design Fundamentals for Java Backend Leads

### 1.1 What "System Design" Means at Lead Level

System design is not drawing boxes on a whiteboard. At lead level it is:

1. **Clarifying requirements** — functional, non-functional (latency, availability, consistency, cost).
2. **Choosing boundaries** — services, databases, sync vs async integration.
3. **Making trade-offs explicit** — CAP, operational complexity, team topology.
4. **Designing for failure** — timeouts, idempotency, observability, runbooks.
5. **Shipping incrementally** — strangler fig, feature flags, reversible decisions.

```
┌────────────────────────────────────────────────────────────────────┐
│                    LEAD SYSTEM DESIGN LAYERS                       │
├──────────────┬──────────────┬──────────────┬─────────────────────────┤
│  Product &   │  Domain &    │  Integration │  Platform & Ops         │
│  SLAs        │  Data Model  │  Patterns    │  (K8s, metrics, CI/CD)  │
├──────────────┴──────────────┴──────────────┴─────────────────────────┤
│  Cross-cutting: security, idempotency, correlation IDs, cost model   │
└────────────────────────────────────────────────────────────────────┘
```

**Interview one-liner:** "I start with user-visible guarantees — what must be immediate, what can be eventual — then pick integration patterns that match those guarantees and our team's operational maturity."

### 1.2 Functional vs Non-Functional Requirements

| Category | Examples | Lead-level questions |
|----------|----------|----------------------|
| **Functional** | Place order, refund, search products | What is the aggregate root? Who owns the data? |
| **Latency** | p99 < 200ms reads, < 2s writes | Which path is sync? Where is caching allowed? |
| **Availability** | 99.9% vs 99.99% | RTO/RPO? Multi-AZ? Degraded mode? |
| **Consistency** | Strong inventory, eventual dashboard | Per-operation guarantees, not global slogans |
| **Durability** | No lost orders after ACK | Outbox? Kafka acks? DB fsync? |
| **Scalability** | 10x peak (Black Friday) | Stateless app tier? DB bottleneck? |
| **Security** | PCI scope, PII, audit | Token validation, encryption, least privilege |
| **Cost** | $/order at scale | Over-provisioned Kafka? N+1 queries? |
| **Operability** | On-call can debug in 15 min | Metrics, traces, runbooks, feature flags |

### 1.3 The Java/Spring Production Stack (Typical)

```
Client (Web/Mobile)
    → API Gateway / Ingress (auth, rate limit, routing)
    → Spring Boot services (REST/gRPC)
        → PostgreSQL (authoritative state, outbox table)
        → Redis (cache, rate limit, session — not source of truth)
        → Kafka (async integration, CQRS projection feed)
    → Observability (Micrometer → Prometheus, structured logs, traces)
    → Kubernetes (scheduling, rollouts, secrets)
```

Deep dives: [Spring Boot Production](spring-boot-production-revision-playbook.md), [PostgreSQL/JPA](postgresql-jpa-revision-playbook.md), [Kafka](kafka-expert-playbook.md), [Kubernetes](kubernetes-expert-playbook.md).

### 1.4 Monolith vs Microservices — Decision Lens

| Factor | Favor monolith (modular) | Favor microservices |
|--------|--------------------------|---------------------|
| Team size | < 10 engineers, one deploy train | Multiple autonomous teams |
| Domain clarity | Bounded contexts still fuzzy | Clear ownership per service |
| Operational maturity | Limited SRE/K8s experience | Mature platform team |
| Scale profile | Uniform scaling | Independent scale per domain |
| Consistency needs | Mostly local transactions | Accept eventual + sagas |
| Migration state | Greenfield or early product | Legacy strangler in flight |

**Production truth:** Most successful "microservices" shops started with a **well-modularized monolith** and extracted services when pain justified operational cost. See [Strangler Fig Playbook](strangler-fig-playbook.md).

### 1.5 Sync vs Async Integration

| Sync (HTTP/gRPC) | Async (Kafka/events) |
|------------------|----------------------|
| Immediate response needed | Fire-and-forget or long-running |
| Simple request-response | Decouple peak load |
| Tight coupling to dependency latency | Buffer spikes, replay |
| Circuit breaker required | Consumer idempotency required |
| Harder to scale fan-out | Natural fan-out via consumer groups |

**Rule of thumb:** User-facing **command** path stays sync until ACK of acceptance; **side effects** and **projections** go async via outbox → Kafka. See [Outbox](outbox-expert-playbook.md).

### 1.6 Data Ownership — Database-per-Service

Each service owns its schema. **No shared mutable tables** across services.

```
order-service     → orders_db
inventory-service → inventory_db
payment-service   → payment_db
```

Cross-service reads:

- **Sync API** — for rare, latency-tolerant lookups (with circuit breaker).
- **Cached read model** — CQRS projection (preferred for dashboards).
- **Duplicate data** — accept staleness with explicit TTL/version.

Violating ownership → distributed joins, deployment coupling, schema change paralysis.

### 1.7 Idempotency — Foundational (Not Optional)

Any retried operation (HTTP client, Kafka consumer, saga step) must be idempotent.

| Layer | Mechanism |
|-------|-----------|
| HTTP POST | `Idempotency-Key` header + unique DB constraint |
| Kafka consumer | Processed-events table keyed by `(consumer, eventId)` |
| Saga step | `sagaId + stepName` unique constraint |
| Payment | PSP idempotency key (Stripe-style) |

Details: Section 3 and [Saga §7](saga-expert-playbook.md#section-7-idempotency--the-non-negotiable-foundation).

### 1.8 Observability Is Part of Design

Design metrics and traces **with** the architecture, not after launch.

| Signal | Use |
|--------|-----|
| **RED** (Rate, Errors, Duration) | Per API and dependency |
| **USE** (Utilization, Saturation, Errors) | DB pool, thread pools, CPU |
| **Business metrics** | Orders placed, payments failed, saga compensations |
| **SLOs** | Error budget drives release risk |

See [Metrics & Observability Playbook](metrics-observability-playbook.md).

### 1.9 Cost-Aware Design (Lead Responsibility)

| Expensive habit | Cheaper alternative |
|-----------------|---------------------|
| Chatty sync microservices | Batch + async events |
| Kafka with over-partitioning | Right-size partitions; compaction where fit |
| N+1 JPA in hot path | Fetch join, denormalized read model |
| Always-strong cross-service reads | CQRS + stale-OK UI |
| Giant K8s clusters "just in case" | HPA + load test driven sizing |

### 1.10 Anti-Patterns to Name in Interviews

1. **Distributed monolith** — microservices deployed together, shared DB, circular sync calls.
2. **Dual write without outbox** — DB commit + Kafka publish in separate try/catch.
3. **2PC across cloud APIs** — XA fantasy; use saga instead ([2PC doc](two-phase-commit-2pc.md)).
4. **Cache as source of truth** — Redis holds inventory counts without DB authority.
5. **Infinite retry without DLQ** — poison messages block partition forever.
6. **Circuit breaker on everything** — hides systemic bugs; tune with metrics.
7. **Big-bang rewrite** — multi-year risk; use strangler fig.

---

## Section 2: Consistency Models — CAP, ACID, BASE, and What to Promise Users

### 2.1 CAP Theorem (Practical Reading)

In a network partition, you choose between **Consistency** (all nodes see same data) and **Availability** (every request gets a response). **Partition tolerance** is not optional in distributed systems.

```
         Network partition happens
                    │
        ┌───────────┴───────────┐
        ▼                       ▼
   Choose CP                 Choose AP
   (reject writes             (accept writes,
    or stale reads)            serve possibly stale reads)
        │                       │
   etcd, ZooKeeper            Cassandra-style,
   strong DB primary          DNS caches, CDN
```

**Interview framing:** "We don't pick CAP once for the whole system — we pick **per operation**. Payment capture is CP-leaning; product catalog browse is AP-leaning."

### 2.2 ACID (Single Database — Your Default Friend)

| Property | Meaning | Spring/JPA note |
|----------|---------|-----------------|
| **Atomicity** | All or nothing | `@Transactional` boundary |
| **Consistency** | Constraints hold | DB constraints + domain validation |
| **Isolation** | Concurrent TX visibility | Isolation level, optimistic `@Version` |
| **Durability** | Committed survives crash | WAL, replication |

**Lead rule:** Maximize work inside **one service's local transaction**. See [Spring Boot §2](spring-boot-production-revision-playbook.md#section-2-transactional-deep-dive--propagation-rollback-self-invocation).

### 2.3 BASE (Distributed Default)

**Basically Available, Soft state, Eventual consistency.**

- **Basically Available** — system responds even if parts degraded.
- **Soft state** — state may change without new input (async projectors).
- **Eventual consistency** — given no new updates, replicas converge.

This is the mental model for **outbox → Kafka → projectors** and **sagas**.

### 2.4 Consistency Spectrum — Name the Guarantee Per Operation

| Level | User-visible example | Mechanism |
|-------|---------------------|-----------|
| **Strong (linearizable)** | Withdraw $100 once only | Single DB TX, or distributed lock (rare) |
| **Sequential** | Order events processed in order | Kafka single partition per key |
| **Causal** | Reply sees own prior writes | Sticky routing, read-your-writes token |
| **Eventual** | Dashboard totals catch up in seconds | CQRS projector |
| **Best-effort** | Recommendation tiles | Cache TTL, stale OK |

### 2.5 Distributed Transactions — 2PC vs Saga vs Outbox

```
Need atomicity across 2+ service databases?
        │
        ├─ Participants support XA + same TM + low latency?
        │       └─ Rare: see two-phase-commit-2pc.md (usually NO in cloud microservices)
        │
        ├─ Long-running business process with compensations?
        │       └─ Saga (orchestration or choreography) → saga-expert-playbook.md
        │
        └─ Single service write + notify others?
                └─ Transactional Outbox → outbox-expert-playbook.md
```

| Pattern | Atomicity scope | Blocking? | Typical Java stack |
|---------|-----------------|-----------|-------------------|
| **Local TX** | One DB | No | Spring `@Transactional` |
| **2PC/XA** | Multiple RM | Yes (prepare locks) | Atomikos — avoid in microservices |
| **Outbox** | DB + event record | No | Same TX insert + relay |
| **Saga** | Semantic all-or-nothing | No | Orchestrator + idempotent steps |

### 2.6 Read-Your-Writes and Stale Reads

Problem: User places order (write to `orders_db`), immediately opens dashboard (reads `dashboard_db` projection) — order missing.

Solutions (pick one, document in API contract):

1. **Routing token** — client sends `afterSequence` from write response; query waits until projector catches up (bounded wait).
2. **Sync read fallback** — if critical, read authoritative write model for that entity.
3. **UX tolerance** — "Processing…" state for N seconds.

See [CQRS §10](cqrs-expert-playbook.md#10-eventual-consistency-and-read-your-writes-ux).

### 2.7 Inventory Consistency — Classic Hard Problem

**Overselling** happens when two orders read `stock=1` concurrently.

| Approach | Pros | Cons |
|----------|------|------|
| **Pessimistic lock** (`SELECT FOR UPDATE`) | Simple, strong | Contention at hot SKUs |
| **Optimistic lock** (`@Version`) | Better throughput | Retry storms on conflict |
| **Reserve in saga step 1** | Clear compensation story | Requires saga infrastructure |
| **Event sourcing + single writer per SKU** | Ordered decisions | High complexity |

Production combo: optimistic or `FOR UPDATE` in **inventory-service** + idempotent reserve/release in saga ([Saga Playbook](saga-expert-playbook.md)).

### 2.8 Duplicates vs Lost Messages — Pick Your Poison

In distributed messaging you realistically get **at-least-once delivery**. Design for:

- **Idempotent consumers** — dedupe table.
- **Monotonic state** — `status` only advances forward (PLACED → PAID, never backward without compensation).
- **Outbox relay** — retries publish; duplicates possible at broker.

Exactly-once end-to-end is a **marketing term** unless the whole pipeline is engineered for it (Kafka transactions + idempotent consumer + compacted topics — still has footguns). See [Kafka §7](kafka-expert-playbook.md#section-7-delivery-guarantees).

### 2.9 Consistency Checklist for Design Reviews

- [ ] What is the **aggregate root** and **source of truth**?
- [ ] What happens on **duplicate request**?
- [ ] What does user see **immediately after submit**?
- [ ] Maximum **staleness** for each read path?
- [ ] **Compensation** story if step 3 fails?
- [ ] **Ordering** guarantees per entity key?
- [ ] **Failure isolation** — one bad tenant/partition?

---

## Section 3: API Design — Idempotency, Pagination, Versioning, Errors

### 3.1 REST API Design Principles (Production)

| Principle | Implementation |
|-----------|----------------|
| **Nouns for resources** | `POST /orders`, `GET /orders/{id}` |
| **Commands as verbs when needed** | `POST /orders/{id}/cancel` |
| **Explicit status codes** | 201 created, 409 conflict, 422 validation |
| **Problem Details** | RFC 7807 `application/problem+json` |
| **Correlation** | `X-Correlation-Id` / `traceparent` |
| **Versioning** | URL `/v1/` or header `Accept-Version` |

Spring: [Spring Boot §10, §15](spring-boot-production-revision-playbook.md).

### 3.2 Idempotency — HTTP Layer

**Safe methods:** GET, HEAD, OPTIONS — naturally idempotent.

**PUT/DELETE** — idempotent by semantics (same result if repeated).

**POST** — **not** idempotent unless you design it:

```
POST /orders
Headers:
  Idempotency-Key: 550e8400-e29b-41d4-a716-446655440000
```

Server flow:

```
1. BEGIN TX
2. INSERT idempotency_keys (key, request_hash, response_body, status) ON CONFLICT DO NOTHING
3. If conflict and same hash → return stored response (200/201)
4. If conflict and different hash → 409 Conflict
5. Else process order, store response, COMMIT
```

```java
@Transactional
public PlaceOrderResponse placeOrder(PlaceOrderRequest req, String idempotencyKey) {
    var existing = idempotencyRepo.findByKey(idempotencyKey);
    if (existing != null) {
        if (!existing.requestHash().equals(hash(req))) {
            throw new IdempotencyConflictException();
        }
        return existing.response();
    }
    var order = orderService.create(req);
    idempotencyRepo.save(new IdempotencyRecord(idempotencyKey, hash(req), toResponse(order)));
    return toResponse(order);
}
```

**Retention:** keep keys 24–72 hours (payment networks often 24h).

### 3.3 Idempotency — Event Consumers

```sql
CREATE TABLE processed_events (
  consumer_name   VARCHAR(64) NOT NULL,
  event_id        UUID NOT NULL,
  processed_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (consumer_name, event_id)
);
```

```java
@KafkaListener(topics = "order-events")
@Transactional
public void onOrderEvent(OrderEvent event) {
    if (processedEvents.exists("inventory-service", event.id())) return;
    inventoryService.apply(event);
    processedEvents.mark("inventory-service", event.id());
}
```

### 3.4 Pagination — Offset vs Cursor

| Style | Query | Pros | Cons |
|-------|-------|------|------|
| **Offset** | `?page=3&size=20` | Simple | Slow on large offsets (`OFFSET 100000`) |
| **Keyset (cursor)** | `?after_id=xyz&limit=20` | Stable under inserts | No random page jump |
| **Seek (timestamp)** | `?after=2024-01-01T00:00:00Z` | Feed-style | Clock skew awareness |

**Production default for feeds and ops tables:** keyset cursor on indexed `(created_at, id)`:

```sql
SELECT * FROM orders
WHERE (created_at, id) > (:cursorTime, :cursorId)
ORDER BY created_at, id
LIMIT 21;  -- fetch 21 to detect hasNext
```

Response envelope:

```json
{
  "items": [...],
  "nextCursor": "eyJjcmVhdGVkQXQiOi4uLn0=",
  "hasNext": true
}
```

### 3.5 Filtering, Sorting, and Index Discipline

Every filter in public API must map to **indexed column(s)**. Document allowed sort fields. Reject arbitrary `sort=foo` to prevent full table scans.

See [PostgreSQL/JPA Playbook](postgresql-jpa-revision-playbook.md) for index design.

### 3.6 Rate Limiting and Backpressure

| Layer | Tool |
|-------|------|
| Edge | API Gateway, Ingress rate limit |
| App | Bucket4j, Resilience4j rate limiter |
| Dependency | Bulkhead max concurrency ([Bulkhead Playbook](bulkhead-expert-playbook.md)) |

Return **429** with `Retry-After` header. Prefer shedding load early over timeout cascades.

### 3.7 API Versioning and Deprecation

1. Ship `/v2` alongside `/v1` with shared domain layer where possible.
2. Sunset headers: `Deprecation: true`, `Sunset: Sat, 01 Jan 2027 00:00:00 GMT`.
3. Metrics on `/v1` traffic — strangler for APIs too.

### 3.8 Error Model — Problem Details

```json
{
  "type": "https://api.example.com/problems/insufficient-stock",
  "title": "Insufficient stock",
  "status": 409,
  "detail": "SKU-42 has 0 units available",
  "instance": "/orders/req-abc",
  "correlationId": "7f3e2a1b-..."
}
```

**Never** expose stack traces. Map domain exceptions in `@ControllerAdvice`.

### 3.9 Webhooks and Callback Idempotency

Payment PSP webhooks arrive multiple times. Store `psp_event_id` unique; respond 200 on duplicate.

### 3.10 gRPC vs REST — When Which

| REST/JSON | gRPC |
|-----------|------|
| Public APIs, browsers | Internal east-west, high throughput |
| OpenAPI ecosystem | Schema evolution via protobuf |
| Human debugging with curl | Binary, HTTP/2 multiplexing |

Many Java shops: REST at edge, gRPC inside mesh.

---

## Section 4: Problem → Pattern Matrix — Quick Routing Guide

Use this matrix in design reviews and interviews. Each cell links to the deep playbook.

### 4.1 Primary Matrix

| Problem / Symptom | First pattern to consider | Deep dive |
|-------------------|---------------------------|-----------|
| DB commits but Kafka publish fails (or reverse) | **Transactional Outbox** | [outbox-expert-playbook.md](outbox-expert-playbook.md) |
| Multi-step order across order/inventory/payment | **Saga** (orchestrated) | [saga-expert-playbook.md](saga-expert-playbook.md) |
| Dashboard/search slow; writes contending with reads | **CQRS** + read models | [cqrs-expert-playbook.md](cqrs-expert-playbook.md) |
| Need reliable async integration backbone | **Kafka** + outbox | [kafka-expert-playbook.md](kafka-expert-playbook.md) |
| Downstream timeout causes thread pool exhaustion | **Circuit Breaker** + timeout | [circuit-breaker-expert-playbook.md](circuit-breaker-expert-playbook.md) |
| One slow dependency blocks all features | **Bulkhead** (separate pools) | [bulkhead-expert-playbook.md](bulkhead-expert-playbook.md) |
| Legacy monolith must become services | **Strangler Fig** facade | [strangler-fig-playbook.md](strangler-fig-playbook.md) |
| Cross-DB atomic commit "across everything" | **Avoid 2PC**; saga or redesign boundaries | [two-phase-commit-2pc.md](two-phase-commit-2pc.md) |
| Hot read path hammering DB | **Cache** (LRU/local or Redis) | [lru-cache-expert-playbook.md](lru-cache-expert-playbook.md) |
| Cannot debug production latency/errors | **RED/USE metrics + traces** | [metrics-observability-playbook.md](metrics-observability-playbook.md) |
| Rollouts cause 502 / OOM on deploy | **K8s probes + graceful shutdown** | [kubernetes-expert-playbook.md](kubernetes-expert-playbook.md) |
| `@Transactional` not rolling back / N+1 queries | **Spring + JPA tuning** | [spring-boot-production-revision-playbook.md](spring-boot-production-revision-playbook.md) |
| Lock contention, slow queries, pool exhaustion | **PostgreSQL indexes/locking** | [postgresql-jpa-revision-playbook.md](postgresql-jpa-revision-playbook.md) |
| GC pauses, heap OOMKilled | **JVM sizing/profiling** | [jvm-performance-revision-playbook.md](jvm-performance-revision-playbook.md) |

### 4.2 Secondary Matrix — Combinations

| Combined problem | Pattern stack |
|------------------|---------------|
| Place order + notify 5 services reliably | Outbox → Kafka → idempotent consumers |
| Place order + payment + inventory consistency | Saga orchestrator + outbox per step |
| Real-time dashboard on saga data | CQRS projector consuming saga events |
| Migration off monolith order module | Strangler facade + outbox sync to new service |
| Black Friday traffic spike | Bulkhead + cache + HPA ([K8s §10](kubernetes-expert-playbook.md)) |
| Payment provider flaky | Circuit breaker + saga compensation on timeout |
| Replay events after bug fix | Kafka offset reset + idempotent projector rebuild ([CQRS §11](cqrs-expert-playbook.md)) |

### 4.3 Symptom → Investigation → Pattern

| Production symptom | Likely root cause | Pattern/fix doc |
|--------------------|-------------------|-----------------|
| Duplicate charges | Missing idempotency key | §3.2, [Saga §7](saga-expert-playbook.md) |
| Ghost orders (paid, no shipment) | Saga stuck without timeout/DLQ | [Saga §9–10](saga-expert-playbook.md) |
| Orders in DB, never in warehouse | Outbox relay down / lag | [Outbox §14](outbox-expert-playbook.md) |
| Dashboard 30s behind | Projector lag / consumer paused | [CQRS §17](cqrs-expert-playbook.md), [Kafka §16](kafka-expert-playbook.md) |
| All APIs slow when one DB slow | Shared pool / no bulkhead | [Bulkhead](bulkhead-expert-playbook.md) |
| Retry storm after outage | Circuit breaker still CLOSED | [Circuit Breaker §7](circuit-breaker-expert-playbook.md) |
| p99 spikes after deploy | Probe too aggressive / no preStop | [K8s §8–9](kubernetes-expert-playbook.md) |

---

## Section 5: Decision Flowcharts — ASCII Decision Trees

### 5.1 Master Pattern Selection

```
                    START: New integration or consistency problem
                                    │
                                    ▼
              ┌─────────────────────────────────────────┐
              │  Can entire operation complete in ONE    │
              │  service + ONE database transaction?     │
              └────────────────────┬────────────────────┘
                          YES      │      NO
                           ▼       │       ▼
                    Local @Transactional   │
                    + return response      │
                           │               │
                           │               ▼
                           │     Need MULTIPLE services to commit atomically?
                           │               │
                           │        YES    │    NO (one writer, many readers)
                           │         ▼     │         ▼
                           │    Avoid 2PC  │    Outbox + async events
                           │    Use SAGA   │    (outbox-expert-playbook.md)
                           │         │     │
                           │         ▼     │
                           │   Long-running│
                           │   + compens-│
                           │   ations?   │
                           │    YES→Saga │
                           │    NO→Rethink│
                           │    boundaries│
                           ▼             ▼
                    Need to notify others?
                           │
                    YES ───┴─── NO → done
                     ▼
              Outbox in same TX
                     │
                     ▼
              Kafka (or queue) → idempotent consumers
```

### 5.2 Sync vs Async Path

```
              User waiting on HTTP response?
                        │
               YES      │      NO
                ▼       │       ▼
         Keep path     │    Publish command/event
         minimal       │    return 202 Accepted
                │      │       │
                ▼      │       ▼
    How many sync deps?│   Worker / consumer
         │             │   processes async
    ┌────┴────┐        │
   0-1       2+         │
    │         │        │
    ▼         ▼        │
  Direct   Orchestrator│
  call     or parallel │
           with        │
           timeouts +  │
           circuit     │
           breakers    │
```

### 5.3 Read Path Optimization

```
                    READ request (GET /dashboard, /search)
                                │
                                ▼
              Can answer from cache with acceptable staleness?
                         │
                  YES ───┴─── NO
                   ▼           ▼
            LRU / Redis    Is write model query cheap
            (lru-cache)    AND traffic low?
                                │
                         YES ───┴─── NO
                          ▼           ▼
                    Query write DB   CQRS read model
                    (careful)        + projector
                                     (cqrs-expert-playbook.md)
```

### 5.4 Failure Handling at Dependency

```
                Call to downstream service
                            │
                            ▼
                   Configure TIMEOUT
                   (always, no exceptions)
                            │
                            ▼
              Transient errors (5xx, timeout)?
                            │
                     YES ───┴─── NO (4xx business)
                      ▼           ▼
              Limited retry      Map to domain error
              (idempotent only)  no retry
                      │
                      ▼
              Failure rate high?
                      │
               YES ───┴─── NO
                ▼           ▼
         CIRCUIT BREAKER   Normal path
         fail-fast +       (circuit-breaker-expert-playbook.md)
         fallback
                │
                ▼
         Is dependency slow but others must not wait?
                │
         YES ───┴─── NO
          ▼           ▼
      BULKHEAD      Shared pool OK?
      separate pool (bulkhead-expert-playbook.md)
```

### 5.5 Monolith Extraction Decision

```
           Extract feature X from monolith?
                      │
                      ▼
        Is domain boundary clear + data separable?
                      │
               NO ───┴─── YES
                ▼           ▼
         Refactor module   Strangler Fig:
         inside monolith   facade routes traffic
         first             (strangler-fig-playbook.md)
                              │
                              ▼
                    Dual-write or CDC for data sync?
                              │
                       Outbox/CDC ───┴─── Big-bang cutover?
                              │              │
                              ▼              ▼
                         Incremental     HIGH RISK
                         traffic %       avoid
```

### 5.6 Caching Decision

```
                  Add cache?
                      │
                      ▼
           Is DB the source of truth for this data?
                      │
               NO ───┴─── YES
                ▼           ▼
            Fix design   Read-heavy + stale OK?
            first              │
                          YES ─┴─ NO
                           ▼      ▼
                    LRU/local   Strong consistency
                    or Redis    required — no cache
                    + TTL       (or very short TTL +
                    + stampede  explicit invalidation)
                    protection
                    (lru-cache-expert-playbook.md)
```

### 5.7 Observability Instrumentation Decision

```
              New service or critical path?
                      │
                      ▼
         Emit RED metrics per endpoint
         + dependency timer metrics
                      │
                      ▼
         Business KPI counters
         (orders_placed, payment_failed)
                      │
                      ▼
         Structured logs with correlationId
                      │
                      ▼
         Distributed trace propagation
         (W3C traceparent)
                      │
                      ▼
         SLO + alert on burn rate
         (metrics-observability-playbook.md)
```

---

## Section 6: Scenario Walkthrough 1 — Place Order (Happy Path)

### 6.1 Requirements Snapshot

| Requirement | Target |
|-------------|--------|
| User submits cart | Sync response < 500ms p99 |
| Order must not be lost after 201 Created | Durability |
| Inventory must not oversell | Strong per-SKU in inventory-service |
| Payment captured before shipment | Saga ordering |
| Warehouse notified | Async, at-least-once OK with idempotency |

### 6.2 Service Boundaries

```
┌─────────────┐     ┌──────────────────┐     ┌─────────────────┐
│ order-svc   │     │ inventory-svc    │     │ payment-svc     │
│ orders_db   │     │ inventory_db     │     │ payments_db     │
└──────┬──────┘     └────────┬─────────┘     └────────┬────────┘
       │                     │                          │
       └─────────────────────┼──────────────────────────┘
                             ▼
                    ┌─────────────────┐
                    │ Kafka (events)  │
                    └────────┬────────┘
                             ▼
                    ┌─────────────────┐
                    │ shipping-svc    │
                    │ (+ CQRS         │
                    │  dashboard)     │
                    └─────────────────┘
```

### 6.3 Happy Path Sequence (Orchestrated Saga)

```
Client          API GW       order-svc      saga-orch      inventory    payment     shipping
  │                │             │              │             │           │            │
  │ POST /orders   │             │              │             │           │            │
  │ Idempotency-Key│             │              │             │           │            │
  ├───────────────►│────────────►│              │             │           │            │
  │                │             │ BEGIN TX     │             │           │            │
  │                │             │ insert order │             │           │            │
  │                │             │ PLACED       │             │           │            │
  │                │             │ insert outbox│             │           │            │
  │                │             │ COMMIT       │             │           │            │
  │                │             ├─────────────►│ start saga  │           │            │
  │◄───────────────┴─────────────┤ 201 {orderId}│             │           │            │
  │                              │              ├───────────►│ reserve   │            │
  │                              │              │            │ COMMIT    │            │
  │                              │              ├───────────────────────►│ authorize  │
  │                              │              │            │           │ COMMIT     │
  │                              │              │ emit OrderPaid (outbox)│            │
  │                              │              │            │           ├───────────►│
  │                              │              │            │           │ (async)    │
  │                              │              │ saga DONE  │           │            │
```

### 6.4 order-service Code Sketch

```java
@RestController
@RequestMapping("/v1/orders")
public class OrderController {
    private final PlaceOrderHandler handler;

    @PostMapping
    public ResponseEntity<OrderResponse> place(
            @Valid @RequestBody PlaceOrderRequest req,
            @RequestHeader("Idempotency-Key") String idempotencyKey) {
        var result = handler.handle(req, idempotencyKey);
        return ResponseEntity.status(HttpStatus.CREATED).body(result);
    }
}
```

### 6.5 Outbox in order-service

Same transaction as order insert — see [Outbox §5](outbox-expert-playbook.md):

```java
@Transactional
public Order place(PlaceOrderRequest req) {
    var order = orderRepo.save(Order.placed(req));
    outboxRepo.save(OutboxEvent.orderPlaced(order));
    return order;
}
```

Relay publishes `OrderPlaced` → saga orchestrator consumes.

### 6.6 Key Design Decisions (Interview Talking Points)

1. **201 returned before payment** — user sees "order received"; payment async in saga (or sync if product requires — trade latency).
2. **Idempotency-Key** on POST — mobile clients retry on network blur.
3. **Outbox** — saga never starts on ghost events.
4. **Partition key** = `orderId` — all events for one order ordered.
5. **Metrics** — `saga_started`, `step_duration`, `orders_placed` counters.

### 6.7 What Can Still Go Wrong (Happy Path Assumptions)

- Inventory reserved but payment slow — saga pending; UI shows "processing".
- Relay lag 200ms — acceptable if SLA documented.
- Duplicate `OrderPlaced` — orchestrator idempotent on `sagaId=orderId`.

---

## Section 7: Scenario Walkthrough 2 — Payment Failure and Compensation

### 7.1 Failure Trigger

Payment gateway returns `DECLINED` or times out after inventory reserved.

### 7.2 Compensation Flow

```
Forward completed:  ReserveInventory ✓
Failed at:          CapturePayment ✗
Compensate:         ReleaseInventory (semantic undo)
Order status:       PAYMENT_FAILED (not deleted — audit trail)
```

```
saga-orch          inventory-svc       payment-svc        order-svc
    │                    │                  │                  │
    │── CapturePayment ──┼─────────────────►│                  │
    │◄── timeout/decline─┼──────────────────┤                  │
    │                    │                  │                  │
    │── COMPENSATE ─────►│ release()        │                  │
    │◄── released ───────┤                  │                  │
    │                    │                  │                  │
    │── update status ───┼──────────────────┼─────────────────►│
    │                    │                  │                  │ PAYMENT_FAILED
```

### 7.3 Compensation Is Not a Rollback

| Forward | Compensation |
|---------|--------------|
| `INSERT reservation` | `UPDATE reservation RELEASED` or `INSERT release record` |
| `CAPTURE payment` | `REFUND` (if capture succeeded partially — edge case) |
| `SHIP order` | `CANCEL shipment` (may fail if already shipped — manual ops) |

**Semantic undo** — business meaning reversed, not `ROLLBACK` across services.

Deep dive: [Saga §4](saga-expert-playbook.md#section-4-forward-steps-and-compensating-transactions).

### 7.4 Idempotency During Compensation

Compensation steps retried on Kafka redelivery:

```java
@Transactional
public void releaseInventory(String sagaId, String sku, int qty) {
    if (compensationLog.exists(sagaId, "RELEASE_INVENTORY")) return;
    inventoryRepo.release(sku, qty);
    compensationLog.record(sagaId, "RELEASE_INVENTORY");
}
```

### 7.5 Payment Timeout vs Decline

| Outcome | Saga action |
|---------|-------------|
| **Hard decline** | Immediate compensation |
| **Timeout** | Retry with backoff (idempotent payment ref); after N tries → compensate |
| **Unknown** | Query payment status API (reconciliation job); never double-capture |

Circuit breaker on payment client: [Circuit Breaker Playbook](circuit-breaker-expert-playbook.md).

### 7.6 User Experience

- Push notification / email: "Payment failed, items returned to cart."
- Order record remains with `PAYMENT_FAILED` for support lookup.
- Do not delete order row — breaks audit and analytics.

### 7.7 DLQ and Stuck Sagas

If compensation fails (inventory release DB down):

1. Saga state → `COMPENSATING_FAILED`.
2. Alert on-call — manual playbook.
3. Event to DLQ with `sagaId` for tooling replay.

[Saga §10](saga-expert-playbook.md#section-10-dead-letter-queue-dlq-patterns).

### 7.8 Reconciliation Job (Safety Net)

Nightly batch compares: orders `PAYMENT_FAILED` vs inventory reservations vs payment PSP ledger. Fixes drift.

---

## Section 8: Scenario Walkthrough 3 — Read Dashboard Under Load

### 8.1 Requirements Snapshot

| Requirement | Target |
|-------------|--------|
| Ops dashboard: orders/min, revenue, failures | p99 < 300ms |
| 50 concurrent ops users | Not on critical write path |
| Data from order, payment, inventory | Cross-service aggregation |
| Acceptable staleness | 5–15 seconds |

**Pattern:** CQRS with denormalized read model — [CQRS Expert Playbook](cqrs-expert-playbook.md).

### 8.2 Architecture

```
order-svc ──outbox──► Kafka topic: order-events ──┐
payment-svc ──outbox──► payment-events ────────────┼──► dashboard-projector ──► dashboard_db
inventory-svc ──outbox──► inventory-events ────────┘
                                                          │
                                                          ▼
                                                   GET /v1/dashboard
                                                   (query-svc, read-only)
```

### 8.3 Read Model Schema (Denormalized)

```sql
CREATE TABLE dashboard_order_facts (
  order_id        UUID PRIMARY KEY,
  customer_id     UUID NOT NULL,
  status          VARCHAR(32) NOT NULL,
  total_cents     BIGINT NOT NULL,
  payment_status  VARCHAR(32),
  items_count     INT NOT NULL,
  placed_at       TIMESTAMPTZ NOT NULL,
  updated_at      TIMESTAMPTZ NOT NULL,
  version         BIGINT NOT NULL  -- monotonic event sequence
);

CREATE INDEX idx_dashboard_placed_at ON dashboard_order_facts (placed_at DESC);
CREATE INDEX idx_dashboard_status ON dashboard_order_facts (status, placed_at DESC);
```

### 8.4 Projector (Idempotent)

```java
@Component
public class DashboardProjector {
    @KafkaListener(topics = "order-events", groupId = "dashboard-projector")
    @Transactional
    public void onOrderEvent(OrderEvent event) {
        if (event.sequence() <= facts.currentVersion(event.orderId())) return;
        facts.upsert(DashboardOrderFacts.from(event));
    }
}
```

Ordering: partition by `orderId`. Sequence guard prevents out-of-order replay corruption — [CQRS §9](cqrs-expert-playbook.md#9-ordering-sequence-guards-and-idempotency).

### 8.5 Query API with Cursor Pagination

```java
@GetMapping("/v1/dashboard/orders")
public CursorPage<DashboardOrderRow> list(
        @RequestParam(required = false) String cursor,
        @RequestParam(defaultValue = "20") int limit,
        @RequestParam(required = false) String status) {
    return queryService.fetchOrders(cursor, Math.min(limit, 100), status);
}
```

### 8.6 Caching Layer (Optional Second Tier)

For aggregate KPI tiles (`revenue today`, `orders/min`):

```
Query ──► Redis key dashboard:kpi:2024-06-13 TTL 10s
              │
              miss ▼
         dashboard_db ROLLUP table (maintained by projector)
```

See [LRU Cache Playbook](lru-cache-expert-playbook.md) for stampede protection (`sync load` or jittered TTL).

### 8.7 Load Test Expectations

| Load | Bottleneck | Mitigation |
|------|------------|------------|
| 100 RPS reads | PostgreSQL index scan | Connection pool sizing ([PostgreSQL playbook](postgresql-jpa-revision-playbook.md)) |
| 1k RPS reads | DB CPU | Read replica for dashboard_db; cache KPIs |
| Projector lag | Kafka consumer slow | Scale consumer instances ≤ partition count |
| Large payloads | Network | Field projection; don't return full order JSON |

### 8.8 Failure Modes

| Symptom | Cause | Fix |
|---------|-------|-----|
| Dashboard empty after deploy | Projector offset wrong | Reset with rebuild procedure ([CQRS §11](cqrs-expert-playbook.md#11-read-model-rebuild-strategy)) |
| Stale for hours | Consumer stuck on poison message | DLQ + skip; alert `consumer_lag` |
| Double-count revenue | Non-idempotent projector | Sequence guard + primary key upsert |
| Spike after cache expiry | Cache stampede | Single-flight lock |

### 8.9 Read vs Write Path Isolation (Bulkhead)

Dashboard query service uses **separate** HikariCP pool from command services — [Bulkhead Playbook](bulkhead-expert-playbook.md). Analytics traffic cannot exhaust pools for `POST /orders`.

### 8.10 Interview Summary (30 seconds)

"We separated reads with CQRS. Write services emit facts through the outbox to Kafka; a projector builds a denormalized dashboard table. Queries never join across live microservices. We accept 10-second staleness and protect the write path with isolated connection pools."

---

## Section 9: Scenario Walkthrough 4 — Monolith Migration (Strangler Fig)

### 9.1 Starting Point

```
┌──────────────────────────────────────────────┐
│           LEGACY MONOLITH (WAR/JAR)          │
│  Orders │ Payments │ Inventory │ Users │ ... │
│              single Oracle DB                │
└──────────────────────────────────────────────┘
```

Problems: 45-minute builds, all-hands deploys, scaling entire JVM for one hot endpoint.

### 9.2 Target State (Incremental)

```
                    ┌─────────────────┐
                    │ Strangler Facade │  Spring Cloud Gateway / NGINX
                    │ (route rules)    │
                    └────────┬─────────┘
              ┌──────────────┼──────────────┐
              ▼              ▼              ▼
      order-svc (new)   monolith      payment-svc (later)
      PostgreSQL        (shrinking)   ...
```

Deep dive: [Strangler Fig Playbook](strangler-fig-playbook.md).

### 9.3 Phase 1 — Facade Without Behavior Change

1. Deploy gateway in front of monolith (100% traffic → monolith).
2. Add correlation IDs, metrics, shadow routing capability.
3. No user-visible change — validates ops path.

### 9.4 Phase 2 — Extract Order Read API

1. Build `order-query-svc` with CQRS read model fed by **CDC/Debezium** from monolith `orders` table OR monolith publishes `OrderUpdated` to Kafka.
2. Route `GET /orders/*` to new service via facade (5% canary → 100%).
3. Monolith still owns writes.

### 9.5 Phase 3 — Extract Order Write Path

1. Build `order-command-svc` + `orders_db`.
2. **Dual-write period** (dangerous) OR **change data capture** one-way sync — prefer:
   - Strangler: new writes go to new service only for **new feature** (e.g., subscription orders).
   - Or: outbox from monolith + sync to new DB until cutover.

```
Monolith write ──► outbox ──► relay ──► order-svc (replicate for read parity)
                     │
              feature flag: new POST /orders → order-svc
```

### 9.6 Anti-Corruption Layer (ACL)

New service must not leak monolith's `OrderDTO` with 80 fields. ACL translates:

```java
public class LegacyOrderAdapter {
    public Order domainFrom(LegacyOrderRow row) {
        return Order.builder()
            .id(OrderId.of(row.getOrdId()))  // legacy column name
            .status(mapStatus(row.getStsCd()))
            .build();
    }
}
```

### 9.7 Traffic Migration Checklist

- [ ] Feature flag per route (% canary)
- [ ] Circuit breaker fallback to monolith if new service fails ([Circuit Breaker §17](circuit-breaker-expert-playbook.md))
- [ ] Compare metrics: error rate, p99, business counts
- [ ] Rollback = flip flag (no redeploy)
- [ ] Data reconciliation job during dual-run

### 9.8 Phase 4 — Decommission Monolith Module

When `POST/GET /orders` stable 30+ days:

1. Stop monolith order code paths.
2. Remove ACL if no longer needed.
3. Archive legacy tables after backup.

### 9.9 Event-Driven Strangler

Kafka as integration hub during migration — [Strangler §10](strangler-fig-playbook.md#section-10-event-driven-strangler-fig-with-kafka):

- Monolith publishes domain events (via outbox).
- New services subscribe without monolith knowing consumers.
- Enables parallel extraction of inventory, payment teams.

### 9.10 Kubernetes and GitOps During Migration

- New services deploy to K8s; monolith may stay VM initially.
- Gateway routes unify path — [Kubernetes Playbook](kubernetes-expert-playbook.md).
- Observability compares old vs new on same dashboards ([Metrics Playbook](metrics-observability-playbook.md)).

### 9.11 Migration Interview Story Arc

"We never stopped the world. We put a gateway in front, extracted read models first with CDC, then shifted write traffic for new order types behind feature flags, with circuit breaker fallback to legacy. Each slice had reconciliation jobs and explicit rollback."

---

## Section 10: Scalability — Horizontal, Vertical, Data, and Cost

### 10.1 Scalability Layers

```
┌─────────────────────────────────────────────────────────────┐
│ L7: Product caching, CDN, read replicas, materialized views │
├─────────────────────────────────────────────────────────────┤
│ L6: Async buffering (Kafka), sagas, background workers      │
├─────────────────────────────────────────────────────────────┤
│ L5: App horizontal scale (K8s HPA, stateless pods)          │
├─────────────────────────────────────────────────────────────┤
│ L4: Connection pools, bulkheads, circuit breakers           │
├─────────────────────────────────────────────────────────────┤
│ L3: DB scaling (indexes, partitioning, read replicas)       │
├─────────────────────────────────────────────────────────────┤
│ L2: JVM tuning (heap, GC, virtual threads)                  │
├─────────────────────────────────────────────────────────────┤
│ L1: Hardware / node scale (Cluster Autoscaler)            │
└─────────────────────────────────────────────────────────────┘
```

### 10.2 Stateless Application Tier

Spring Boot pods hold **no session state** in memory. Session → Redis or JWT. Uploads → S3. Any pod handles any request.

```
        HPA watches CPU or custom metric (request rate)
                    │
                    ▼
        Replica count 3 → 12 during peak
                    │
        Each pod: same image, config from K8s Secret/ConfigMap
```

### 10.3 Database — The Usual Bottleneck

| Technique | When |
|-----------|------|
| **Indexes** | Every WHERE/ORDER BY in hot path |
| **Read replicas** | CQRS read models, reporting |
| **Partitioning** | Time-series events > 100M rows |
| **Connection pool sizing** | `pool size ≈ (core_count * 2) + spindle` per instance — tune with metrics |
| **Avoid cross-shard joins** | Design aggregates per shard key |

[PostgreSQL/JPA Playbook](postgresql-jpa-revision-playbook.md).

### 10.4 Kafka Scalability

- Throughput ∝ partition count (for single key, one partition = serial).
- Consumer instances ≤ partitions per group.
- Hot key problem → sub-key partitioning or aggregate in stream.

[Kafka §6](kafka-expert-playbook.md#section-6-partition-strategy-and-data-skew).

### 10.5 Caching Strategy Tiers

| Tier | Latency | Consistency |
|------|---------|-------------|
| L1: Caffeine in-process LRU | μs | Per-pod stale |
| L2: Redis cluster | ms | Shared, TTL-based |
| L3: CDN edge | ms | Static/assets only |

[lru-cache-expert-playbook.md](lru-cache-expert-playbook.md).

### 10.6 Virtual Threads (Java 21+, Spring Boot 3.2+)

Good for **I/O-bound** sync calls (many blocking HTTP clients). Bad for CPU-bound work. Still need bulkheads — million virtual threads calling one dying DB still kills it.

[JVM Performance Playbook](jvm-performance-revision-playbook.md).

### 10.7 Backpressure and Load Shedding

When overloaded:

1. Return 503 fast at gateway (rate limit).
2. Shed non-critical endpoints (recommendations off, dashboard degraded).
3. Never infinite queue in memory.

### 10.8 Capacity Planning Formula (Interview)

```
Peak RPS × avg latency (s) = concurrent requests
Concurrent requests / requests per pod capacity = pods needed
Add 30% headroom + PDB minAvailable constraints
```

Always validate with load test (Gatling, k6) — not spreadsheet only.

### 10.9 Scaling Anti-Patterns

| Anti-pattern | Why it fails |
|--------------|--------------|
| Scale pods without fixing N+1 | DB melts at any replica count |
| Add Kafka partitions without rekey strategy | Ordering breaks |
| Cache everything | Invalidation nightmare |
| 2PC to "fix" consistency | Throughput ceiling |

---

## Section 11: Security and Operations — Production Baseline

### 11.1 Security Layers

```
Internet
   │
   ▼
WAF / DDoS protection
   │
   ▼
API Gateway (OAuth2/OIDC validate JWT)
   │
   ▼
Service mesh / mTLS (optional, east-west)
   │
   ▼
Spring Security Resource Server
   │
   ▼
Service account → DB credentials (least privilege)
```

[Spring Boot §13](spring-boot-production-revision-playbook.md#section-13-security-overview--oauth2-resource-server-sketch).

### 11.2 Authentication vs Authorization

| | AuthN | AuthZ |
|---|-------|-------|
| Question | Who are you? | What can you do? |
| Mechanism | JWT, mTLS | RBAC, scopes, `@PreAuthorize` |
| Lead concern | Token expiry, rotation | Tenant isolation |

### 11.3 Secrets Management

- **Never** in git. K8s Secrets + external vault (AWS SM, HashiCorp Vault).
- Rotate DB passwords without downtime — dual-credential window.
- Kafka SASL/SCRAM credentials per service.

### 11.4 Data Protection

| Data class | Control |
|------------|---------|
| PII | Encrypt at rest; mask in logs |
| PCI (cards) | Never store PAN; use PSP tokenization |
| Audit | Append-only audit log for admin actions |

### 11.5 Network Security

- **NetworkPolicy** default-deny in K8s namespace.
- DB not public internet — private subnet only.
- Egress allow-list for PSP APIs.

[Kubernetes §11](kubernetes-expert-playbook.md#section-11-security--rbac-serviceaccount-networkpolicy-pod-security-standards).

### 11.6 Supply Chain

- Pin image digests in prod.
- Scan CI images (Trivy).
- Dependabot for Spring CVEs.

### 11.7 Operations — Deployment

| Practice | Tool |
|----------|------|
| Rolling deploy | K8s Deployment `maxUnavailable: 0` |
| Canary | Argo Rollouts / flag |
| GitOps | Argo CD — [K8s §16](kubernetes-expert-playbook.md) |
| Graceful shutdown | `server.shutdown=graceful` + preStop |

### 11.8 Operations — Incident Response

1. **Triage** — user impact, SLO burn.
2. **Mitigate** — scale, circuit open, disable feature flag.
3. **Diagnose** — traces, logs, RED metrics (§12).
4. **Fix** — patch, rollback, replay.
5. **Postmortem** — blameless, action items.

### 11.9 SLO Example

| SLI | SLO | Error budget (30d) |
|-----|-----|-------------------|
| Order API availability | 99.9% | ~43 min downtime |
| Order API p99 latency | < 500ms | Alert at 400ms sustained |

[Metrics Playbook](metrics-observability-playbook.md).

### 11.10 Backup and DR

| Asset | RPO | RTO |
|-------|-----|-----|
| PostgreSQL | 5 min (PITR) | 1 hour |
| Kafka | Cluster policy | Replay from retention |
| etcd (K8s) | 1 hour snapshot | 2 hours |

### 11.11 Compliance and Audit for Sagas

Store saga state transitions immutable. Support "show me why order X failed" for regulators/support.

### 11.12 On-Call Runbook Links

| Alert | First doc to open |
|-------|-------------------|
| `outbox_lag_seconds` high | [Outbox §14](outbox-expert-playbook.md) |
| `kafka_consumer_lag` | [Kafka §16](kafka-expert-playbook.md) |
| `circuit_breaker_open` | [Circuit Breaker §15](circuit-breaker-expert-playbook.md) |
| `pod_oomkilled` | [JVM](jvm-performance-revision-playbook.md) + [K8s §14](kubernetes-expert-playbook.md) |
| `hikari_pool_waiting` | [PostgreSQL/JPA](postgresql-jpa-revision-playbook.md) |

---

## Section 12: Five-Layer Production Debugging Framework

When production misbehaves, **do not random kubectl**. Walk layers systematically from user symptom to root cause.

> **Full depth with kubectl commands, decision trees, and 25+ K8s scenarios:** [Kubernetes Expert Playbook §13 — Five-Layer Production Debugging Framework](kubernetes-expert-playbook.md#section-13-five-layer-production-debugging-framework)

### 12.1 Framework Overview (Application + Platform Stack)

This revision map uses a **top-down business-first** view. The [Kubernetes playbook](kubernetes-expert-playbook.md#section-13-five-layer-production-debugging-framework) uses the same five layers from **application up through cloud** — map between them when triaging:

| This doc (top-down) | K8s playbook (bottom-up) |
|---------------------|--------------------------|
| Layer 5: Business / SLO | (cross-cutting — metrics, saga state) |
| Layer 4: Dependencies | Layer 1: Application (JVM, Spring, pools) |
| Layer 3: Service runtime | Layer 1: Application |
| Layer 2: Platform (K8s) | Layers 2–4: Workload, Networking, Cluster |
| Layer 1: Edge | Layer 5: Infra / Cloud (LB, RDS, IAM) |

```
Layer 5: Business / SLO     — Are orders completing? Saga stuck count?
Layer 4: Dependencies       — Kafka lag, PSP errors, DB replica lag
Layer 3: Service runtime    — JVM heap, thread pools, circuit state
Layer 2: Platform (K8s)     — Pods ready? Endpoints? NetworkPolicy?
Layer 1: Edge               — Gateway 502? TLS? Rate limit?
```

Start Layer 5 (user symptom), drill down; fix at lowest layer that explains symptom. For Pod/Ingress/LB issues, switch to the K8s playbook decision tree immediately.

### 12.2 Layer 5 — Business Signals

```promql
# Orders placed rate drop
rate(orders_placed_total[5m]) < 0.5 * rate(orders_placed_total[1h] offset 1d)

# Saga compensations spike
rate(saga_compensation_total[5m]) > threshold
```

### 12.3 Layer 4 — Dependencies

- Kafka: `kafka_consumer_lag_sum` by group.
- PostgreSQL: `pg_stat_activity`, long queries, locks.
- External API: circuit breaker metrics, timeout ratio.

### 12.4 Layer 3 — JVM / Spring

- Actuator `/actuator/metrics/jvm.memory.used`.
- Hikari `pool.Active`, `pool.Pending`.
- Thread dump if CPU high.

### 12.5 Layer 2 — Kubernetes

```bash
kubectl get pods -l app=order-svc
kubectl describe pod <pod>  # events: OOMKilled, probe failed
kubectl get endpointslices
```

### 12.6 Layer 1 — Edge

- Ingress controller logs.
- JWT validation failures (401 spike).
- WAF blocked requests.

### 12.7 Correlation ID Trace

```
grep correlationId=abc123 across:
  gateway access log → order-svc log → saga-orch log → payment-client log
```

OpenTelemetry trace links spans across services.

### 12.8 Common False Trails

| Red herring | Actual cause |
|-------------|--------------|
| "Kafka is down" | Consumer exception loop, lag only one group |
| "DB is slow" | Missing index on new query param |
| "K8s is broken" | Liveness probe kills during GC pause |
| "Need more pods" | Connection pool exhaustion |

---

## Section 13: Lead Interview Questions — 40+ Logical and Production Scenarios

> Scenario-based questions for Java Backend Lead / Architect interviews. Answers are revision-length — expand with your war stories.

### Fundamentals

**Q1: How do you start a system design interview?**

**A:** Clarify functional scope, scale (users, RPS), consistency expectations, and latency SLAs. Identify read-heavy vs write-heavy paths. Then draw boundaries and data ownership before picking patterns. *(§1)*

**Q2: Monolith vs microservices — how do you decide?**

**A:** Team topology and operational maturity matter as much as scale. I favor modular monolith until independent deploy units and clear bounded contexts justify microservices ops cost. *(§1.4, [Strangler](strangler-fig-playbook.md))*

**Q3: What is the hardest part of microservices?**

**A:** Data consistency and operations — not HTTP. Distributed transactions, observability, and deployment coordination. Patterns: saga, outbox, CQRS. *(§2, §4)*

**Q4: Explain CAP in one sentence.**

**A:** During a network partition you cannot have perfect consistency and perfect availability simultaneously; real systems choose per operation, not globally. *(§2.1)*

**Q5: What is an aggregate root?**

**A:** The entity that enforces invariants for a cluster of related objects; all changes go through it in one transaction boundary. Example: `Order` owns `OrderLine` items. *(§2.9)*

### Consistency and Transactions

**Q6: Why not use 2PC across microservices?**

**A:** Blocking locks, coordinator SPOF, poor support in cloud DBs and SaaS APIs, fragile under latency. Use saga for cross-service workflows; outbox for single-writer notification. *([2PC](two-phase-commit-2pc.md), [Saga](saga-expert-playbook.md))*

**Q7: What is eventual consistency? Give a user-facing example.**

**A:** Replicas converge without simultaneous strong consistency. Dashboard totals update seconds after order placed; UI shows "processing" or uses read-your-writes token. *(§2.4, §8)*

**Q8: How do you prevent overselling inventory?**

**A:** Strong consistency inside inventory-service: `SELECT FOR UPDATE` or optimistic `@Version` on stock row; expose reserve/release as idempotent saga steps. *(§2.7)*

**Q9: What is the outbox pattern?**

**A:** Write business row and outbox event in one DB transaction; separate relay publishes to Kafka. Avoids dual-write where DB commits and broker fails. *([Outbox](outbox-expert-playbook.md))*

**Q10: Saga vs outbox — when which?**

**A:** Outbox: one service commits state and notifies others. Saga: multi-service workflow with compensations. Often combined — each saga step uses local TX + outbox. *(§4)*

**Q11: What is a compensating transaction?**

**A:** Semantic undo of a completed forward step — e.g., `releaseInventory` after failed payment — not a distributed rollback. *([Saga §4](saga-expert-playbook.md))*

**Q12: How do you guarantee ordering of events?**

**A:** Kafka partition key per entity (`orderId`); single consumer per partition; sequence numbers in projector for out-of-order detection. *([Kafka §9](kafka-expert-playbook.md), [CQRS §9](cqrs-expert-playbook.md))*

### API Design

**Q13: How do you make POST /orders idempotent?**

**A:** `Idempotency-Key` header; store key + request hash + response; return cached response on replay; 409 if same key different body. *(§3.2)*

**Q14: Offset vs cursor pagination?**

**A:** Offset simple but O(n) on large pages; cursor (keyset) uses indexed `(created_at, id)` for stable performant feeds. *(§3.4)*

**Q15: What HTTP status for validation errors vs conflicts?**

**A:** 422 or 400 for validation; 409 for business conflicts (insufficient stock, idempotency mismatch). Problem Details body. *(§3.8)*

### Resilience

**Q16: What does a circuit breaker do?**

**A:** Stops calling a failing dependency after threshold; fails fast locally; probes recovery in HALF_OPEN. Prevents thread exhaustion. *([Circuit Breaker](circuit-breaker-expert-playbook.md))*

**Q17: Circuit breaker vs retry?**

**A:** Retry helps transient errors on healthy dependency; breaker stops calls when dependency systematically failing. Retry inside breaker only while CLOSED. *(§5.4)*

**Q18: What is a bulkhead?**

**A:** Isolated resource pool (threads, connections) per dependency so one slow service cannot exhaust shared pool. *([Bulkhead](bulkhead-expert-playbook.md))*

**Q19: How do you handle payment gateway timeouts in a saga?**

**A:** Idempotent payment reference; bounded retries; query reconciliation API; compensate (release inventory) if unknown too long. *(§7)*

**Q20: What is a DLQ?**

**A:** Dead letter queue/topic for messages that fail processing after retries — prevents blocking partition; enables manual replay. *([Kafka §8](kafka-expert-playbook.md))*

### Data and CQRS

**Q21: When do you use CQRS?**

**A:** Read patterns differ materially from writes; dashboard/search needs denormalized models; write path must stay fast. Not for simple CRUD. *([CQRS](cqrs-expert-playbook.md))*

**Q22: CQRS vs Event Sourcing?**

**A:** CQRS separates read/write models; event sourcing stores state as event log. ES implies CQRS often; CQRS does not require ES. *([CQRS §12](cqrs-expert-playbook.md))*

**Q23: How do you rebuild a corrupted read model?**

**A:** Stop projector; truncate read table; reset consumer offset to beginning (or snapshot + delta); replay with idempotent upserts. *([CQRS §11](cqrs-expert-playbook.md))*

**Q24: How do you solve N+1 queries in Spring?**

**A:** `@EntityGraph`, fetch join in JPQL, `@BatchSize`, or don't use entities on hot read path — use DTO projection/CQRS. *([Spring Boot §4](spring-boot-production-revision-playbook.md))*

### Kafka and Messaging

**Q25: Why Kafka over RabbitMQ for event backbone?**

**A:** Replay, retention, high throughput, log-based ordering per partition — fits event-driven microservices and projectors. *([Kafka §1](kafka-expert-playbook.md))*

**Q26: At-least-once vs exactly-once?**

**A:** At-least-once + idempotent consumer is production default. True exactly-once end-to-end is rare and expensive. *(§2.8)*

**Q27: Consumer lag growing — what do you check?**

**A:** Processing exceptions, slow DB writes, insufficient consumers (≤ partitions), rebalance storm, poison message. *([Kafka §16](kafka-expert-playbook.md))*

**Q28: How do you choose Kafka partition count?**

**A:** Target throughput per partition (~10MB/s rule of thumb); max parallel consumers; hot key risk. Cannot decrease easily — plan headroom. *([Kafka §6](kafka-expert-playbook.md))*

### Scalability and Performance

**Q29: Stateless vs stateful services for scaling?**

**A:** Stateless app tier scales horizontally freely; stateful (DB, Kafka) needs sharding, partitioning, replicas. *(§10)*

**Q30: How do you size HikariCP?**

**A:** Not `connections = pods * 50`. Formula from DB max connections, pod count, workload; monitor `Pending threads`. *([PostgreSQL/JPA](postgresql-jpa-revision-playbook.md))*

**Q31: When are virtual threads useful?**

**A:** I/O-bound workloads with many blocking calls; not CPU-bound computation. Still need backpressure and pool limits. *([JVM](jvm-performance-revision-playbook.md))*

**Q32: How do you cache without stale inventory?**

**A:** Don't cache authoritative stock for checkout; cache product catalog with TTL; invalidate on write for admin views. *(§5.6, [LRU](lru-cache-expert-playbook.md))*

### Migration and Architecture

**Q33: How does strangler fig work?**

**A:** Facade routes traffic incrementally from legacy to new services; ACL translates models; feature flags; decommission when 100% migrated. *([Strangler](strangler-fig-playbook.md))*

**Q34: Big-bang rewrite risks?**

**A:** Multi-year delivery, no business value until end, knowledge loss, high defect rate. Strangler delivers incremental value. *(§9)*

**Q35: Anti-corruption layer purpose?**

**A:** Translates legacy model to new domain model so new service isn't polluted by legacy schema/API quirks. *([Strangler §6](strangler-fig-playbook.md))*

### Security and Ops

**Q36: How do you secure service-to-service calls?**

**A:** JWT or mTLS; network policies; least-privilege DB users per service; no secrets in images. *(§11)*

**Q37: What is graceful shutdown on K8s?**

**A:** preStop sleep → SIGTERM → finish in-flight requests → stop accepting new → exit before `terminationGracePeriodSeconds`. *([Spring Boot §11](spring-boot-production-revision-playbook.md), [K8s §15](kubernetes-expert-playbook.md))*

**Q38: How do you define an SLO?**

**A:** SLI (metric) + target (99.9%) + window (30d); error budget drives release policy. *([Metrics](metrics-observability-playbook.md))*

### Scenario Questions

**Q39: Design place order across inventory, payment, shipping.**

**A:** order-service accepts with idempotency; local TX + outbox; saga orchestrator: reserve → pay → emit shipped command; compensate on failure; CQRS for status dashboard. *(§6–7)*

**Q40: Payment succeeded but order shows failed — debug?**

**A:** Check saga state machine, idempotency logs, outbox relay, duplicate compensation, PSP webhook idempotency; reconciliation job. *(§7, §12)*

**Q41: Dashboard 5 minutes behind during peak — fix?**

**A:** Scale projector consumers (≤ partitions), optimize upsert, check poison consumer, add KPI cache, consider read replica. *(§8)*

**Q42: Extract orders from monolith — first step?**

**A:** Strangler facade; extract read path or new order types first; CDC/outbox sync; never big-bang. *(§9)*

**Q43: All microservices slow when payment is down — why?**

**A:** Missing circuit breaker/bulkhead; shared thread pool blocked on payment timeouts. *([Circuit Breaker](circuit-breaker-expert-playbook.md), [Bulkhead](bulkhead-expert-playbook.md))*

**Q44: How do you test sagas?**

**A:** Unit test state transitions; integration with Testcontainers (DB + Kafka); chaos inject payment timeout; verify compensation idempotency. *([Spring Boot §14](spring-boot-production-revision-playbook.md))*

**Q45: Difference between orchestration and choreography sagas?**

**A:** Orchestration: central coordinator directs steps. Choreography: services react to events. Orchestration easier to debug; choreography looser coupling. *([Saga §5](saga-expert-playbook.md))*

**Q46: How do you debug p99 latency spike after deploy?**

**A:** Five-layer framework §12; compare canary vs stable metrics; check new N+1 query, pool size change, probe killing pods mid-request. *([K8s §13](kubernetes-expert-playbook.md#section-13-five-layer-production-debugging-framework), [PostgreSQL/JPA](postgresql-jpa-revision-playbook.md))*

**Q47: Webhook idempotency for payment PSP (Stripe-style)?**

**A:** Store `event_id` unique; return 200 on duplicate; verify signature; process in TX with business idempotency. *(§3.9)*

**Q48: When to build a custom Spring Boot starter?**

**A:** When 10+ internal services repeat the same auto-config (metrics, security, outbox). *([Custom Starter Playbook](custom-spring-boot-starter-expert-playbook.md))*

**Q49: Virtual threads vs platform threads for microservices?**

**A:** Virtual threads help I/O-bound blocking workloads (many HTTP clients); still need bulkheads and pool limits; not a substitute for DB scale. *([Java Concurrency Playbook](java-modern-concurrency-streams-playbook.md), [JVM](jvm-performance-revision-playbook.md))*

**Q50: Biggest distributed systems mistake you have seen?**

**A:** (Prepare your war story.) Template: dual-write without outbox → lost warehouse events; fixed with outbox + reconciliation job + `outbox_lag` alert. Interviewers want specifics and learning.

---

## Section 14: How to Talk About System Design in an Interview (Plain English)

### 14.1 The 8-Minute Story Structure

1. **Requirements (1 min)** — "Users place orders; we can't oversell; payment can fail; dashboard can lag 10 seconds."
2. **High-level diagram (2 min)** — boxes: API, order service, inventory, payment, Kafka, dashboard.
3. **Deep dive on hardest part (3 min)** — usually consistency: saga + outbox + idempotency.
4. **Failure and ops (1 min)** — circuit breakers, metrics, stuck saga alerts.
5. **Trade-offs (1 min)** — what you optimized for and what you deferred.

### 14.2 Plain English Phrases (Use These)

| Jargon | Say instead |
|--------|-------------|
| Transactional outbox | "We save the order and the 'notify everyone' message in one database commit so they can't get out of sync." |
| Saga | "Each step can succeed or undo itself; if payment fails we release the stock we held." |
| CQRS | "Writes go to the system of record; reads come from a dashboard-optimized copy updated in the background." |
| Circuit breaker | "If the payment API is melting down, we stop calling it and fail fast instead of hanging every checkout." |
| Eventual consistency | "The dashboard might be a few seconds behind — we tell users or show a processing state." |
| Idempotency key | "If the phone retries the request, we don't double-charge — we return the same result." |
| Strangler fig | "We peel off one feature at a time from the old system without a risky big-bang rewrite." |

### 14.3 Signals You Are Senior

- You ask clarifying questions before drawing.
- You name **failure modes** without being prompted.
- You say "it depends" with **explicit trade-offs**.
- You reference **operational** concerns: deploy, debug, on-call.
- You don't over-engineer CRUD into Kafka + CQRS + saga.

### 14.4 Red Flags to Avoid

- Jumping to Kubernetes before defining data model.
- Claiming "exactly-once everywhere."
- No idempotency on POST or consumers.
- Shared database across microservices.
- Caching inventory during checkout.

### 14.5 Closing Strong

"I'd ship the monolithic write path with outbox first, add saga orchestration for payment failures, then CQRS for the ops dashboard — each layer independently observable with an SLO and rollback plan."

---

## Section 15: Revision Cheat Sheet — Tables, One-Liners, and Checklists

### 15.1 Pattern One-Liners

| Pattern | One-liner |
|---------|-----------|
| Outbox | Same TX: row + event; relay publishes later |
| Saga | Local TX steps + semantic compensations |
| CQRS | Write model ≠ read model; async projection |
| Circuit breaker | Trip on failures; fail fast; probe recovery |
| Bulkhead | Separate pools so one bad dep doesn't drown all |
| Strangler | Facade migrates traffic slice by slice |
| 2PC | Avoid in microservices; blocking distributed commit |
| LRU cache | Evict least-recently-used; watch stampede |

### 15.2 Consistency Quick Pick

| Need | Pick |
|------|------|
| Single service write | Local `@Transactional` |
| Notify after write | Outbox |
| Multi-service workflow | Saga |
| Fast dashboard | CQRS + cache |
| Cross-DB atomic | Redesign boundaries (not 2PC) |

### 15.3 HTTP Status Cheat Sheet

| Code | Use |
|------|-----|
| 200 | GET/PUT success |
| 201 | POST created |
| 202 | Accepted async processing |
| 400/422 | Validation |
| 401/403 | Auth |
| 409 | Conflict / idempotency mismatch |
| 429 | Rate limited |
| 503 | Overloaded / dependency down |

### 15.4 Kafka Producer/Consumer Defaults to Remember

| Setting | Production note |
|---------|-----------------|
| `acks=all` | Durability; wait all ISR |
| `enable.idempotence=true` | Producer dedup |
| `max.poll.interval.ms` | Must exceed max processing time |
| Consumer group | One consumer per partition max parallelism |

### 15.5 Resilience Stack Order

```
Timeout → Retry (idempotent only) → Circuit Breaker → Bulkhead → Fallback
```

### 15.6 Pre-Production Checklist

- [ ] Idempotency on all POST and consumers
- [ ] Outbox or saga — no naked dual-write
- [ ] Timeouts on every external call
- [ ] Circuit breaker on critical dependencies
- [ ] RED metrics + business counters
- [ ] Correlation ID end-to-end
- [ ] Load test at 2× expected peak
- [ ] Rollback / feature flag documented
- [ ] Runbook for top 3 failure modes
- [ ] DB indexes match query API filters

### 15.7 Playbook Cross-Reference by Interview Topic

| Interview topic | Read first |
|-----------------|------------|
| Order + payment flow | This doc §6–7 → [Saga](saga-expert-playbook.md) |
| Dual-write / events | [Outbox](outbox-expert-playbook.md) |
| Dashboard scale | [CQRS](cqrs-expert-playbook.md) |
| Black Friday | [Bulkhead](bulkhead-expert-playbook.md) + [K8s HPA](kubernetes-expert-playbook.md) |
| Monolith migration | [Strangler](strangler-fig-playbook.md) |
| JVM OOM on deploy | [JVM](jvm-performance-revision-playbook.md) + [K8s probes](kubernetes-expert-playbook.md) |
| Slow queries | [PostgreSQL/JPA](postgresql-jpa-revision-playbook.md) |

### 15.8 Key `@Transactional` Pitfalls (Spring)

| Pitfall | Fix |
|---------|-----|
| Self-invocation bypasses proxy | Inject self or move TX to another bean |
| Checked exception no rollback | `rollbackFor = Exception.class` |
| Long TX holds connections | Keep TX short; outbox for async |
| `REQUIRES_NEW` abuse | Understand nested TX cost |

### 15.9 Observability Minimum Viable

```
Metrics: http_server_requests_seconds + custom business counters
Logs:    JSON + correlationId + traceId
Traces:  W3C traceparent across gateway and Kafka consumers
Alerts:  SLO burn rate + consumer lag + outbox lag
```

### 15.10 Revision Mnemonics

- **O-S-C-K-C-B-S** — Outbox, Saga, CQRS, Kafka, Circuit breaker, Bulkhead, Strangler.
- **IDI** — Idempotency, Durability (outbox), Isolation (bulkhead).
- **RED USE** — Rate Errors Duration; Utilization Saturation Errors.

---

## Section 16: Appendix — Cross-Playbook Study Order and Revision Calendar

### 16.1 Recommended Study Order (2 Weeks)

| Day | Focus | Playbook |
|-----|-------|----------|
| 1–2 | This revision map + place order scenario | This doc §1–7 |
| 3 | Outbox + idempotency | [outbox-expert-playbook.md](outbox-expert-playbook.md) |
| 4 | Saga compensation | [saga-expert-playbook.md](saga-expert-playbook.md) |
| 5 | CQRS + dashboard | [cqrs-expert-playbook.md](cqrs-expert-playbook.md) |
| 6 | Kafka delivery + lag | [kafka-expert-playbook.md](kafka-expert-playbook.md) |
| 7 | Circuit breaker + bulkhead | [circuit-breaker](circuit-breaker-expert-playbook.md), [bulkhead](bulkhead-expert-playbook.md) |
| 8 | Spring Boot production | [spring-boot-production-revision-playbook.md](spring-boot-production-revision-playbook.md) |
| 9 | PostgreSQL + JPA | [postgresql-jpa-revision-playbook.md](postgresql-jpa-revision-playbook.md) |
| 10 | JVM + K8s runtime | [jvm-performance-revision-playbook.md](jvm-performance-revision-playbook.md), [kubernetes-expert-playbook.md](kubernetes-expert-playbook.md) |
| 11 | Observability | [metrics-observability-playbook.md](metrics-observability-playbook.md) |
| 12 | Strangler migration | [strangler-fig-playbook.md](strangler-fig-playbook.md) |
| 13 | 2PC (why not) + LRU cache | [two-phase-commit-2pc.md](two-phase-commit-2pc.md), [lru-cache-expert-playbook.md](lru-cache-expert-playbook.md) |
| 14 | Concurrency + platform libs | [java-modern-concurrency-streams-playbook.md](java-modern-concurrency-streams-playbook.md), [custom-spring-boot-starter-expert-playbook.md](custom-spring-boot-starter-expert-playbook.md) |
| 15 | Mock interviews §13–14 | This doc |

### 16.2 Night-Before-Interview Scan (30 min)

1. §4 Problem → Pattern matrix (5 min)
2. §5 Decision flowcharts (5 min)
3. §6–9 One scenario walkthrough matching job domain (10 min)
4. §15 Cheat sheet (5 min)
5. §14 Plain English phrases (5 min)

### 16.3 On-Call Pocket Card

```
User impact? → SLO dashboard
Kafka lag?   → kafka-expert-playbook §16
Outbox lag?  → outbox-expert-playbook §14
502 spike?   → kubernetes-expert-playbook §13
Pool wait?   → postgresql-jpa-revision-playbook
OOMKill?     → jvm-performance + k8s §14
CB open?     → circuit-breaker-expert-playbook §15
```

### 16.4 Glossary

| Term | Definition |
|------|------------|
| **Aggregate** | Consistency boundary for writes |
| **Compensation** | Semantic undo in saga |
| **Projector** | CQRS read model updater |
| **Relay** | Outbox publisher process |
| **ISR** | In-sync Kafka replicas |
| **HALF_OPEN** | Circuit breaker probe state |
| **ACL** | Anti-corruption layer in migration |
| **DLQ** | Dead letter queue for poison messages |
| **SLO** | Service level objective |
| **Keyset pagination** | Cursor-based paging |

### 16.5 Document Map — All Linked Playbooks

```
java-system-design-revision-playbook.md (YOU ARE HERE — hub)
├── saga-expert-playbook.md
├── outbox-expert-playbook.md
├── cqrs-expert-playbook.md
├── kafka-expert-playbook.md
├── circuit-breaker-expert-playbook.md
├── bulkhead-expert-playbook.md
├── strangler-fig-playbook.md
├── metrics-observability-playbook.md
├── kubernetes-expert-playbook.md
├── two-phase-commit-2pc.md
├── lru-cache-expert-playbook.md
├── spring-boot-production-revision-playbook.md
├── postgresql-jpa-revision-playbook.md
├── jvm-performance-revision-playbook.md
├── java-modern-concurrency-streams-playbook.md
└── custom-spring-boot-starter-expert-playbook.md
```

---

*End of Java System Design Revision Playbook. All sections complete — no placeholders. Last updated: 2026-06-13.*

