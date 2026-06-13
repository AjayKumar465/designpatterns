# Saga Pattern — Expert Playbook (Lead/Architect, Java, 10+ Years)

A comprehensive end-to-end reference covering the Saga pattern from theory to production-grade implementation. Includes choreography vs orchestration, sync vs async orchestration, durable state machines, idempotency, compensation design, transactional outbox integration, DLQ handling, observability, production runbooks, and 25+ lead-level interview Q&As. Sourced from Chris Richardson's microservices patterns, Hector Garcia-Molina's original saga paper, Spring Boot production patterns, Confluent event-driven architecture guides, and real-world war stories from order, payment, and logistics systems.

**Runnable examples in this repo:**

- `examples/saga/SagaDemo.java` — happy path, failure compensation, idempotency guard
- `examples/saga/SagaOrchestratorModesDemo.java` — sync vs async orchestration with A→B→C→D flow

---

## Table of Contents

1. [What Is the Saga Pattern?](#section-1-what-is-the-saga-pattern)
2. [The Problem: Why Not 2PC?](#section-2-the-problem-why-not-2pc)
3. [Saga vs 2PC vs Local Transaction](#section-3-saga-vs-2pc-vs-local-transaction)
4. [Forward Steps and Compensating Transactions](#section-4-forward-steps-and-compensating-transactions)
5. [Choreography vs Orchestration](#section-5-choreography-vs-orchestration)
6. [Synchronous vs Asynchronous Orchestration](#section-6-synchronous-vs-asynchronous-orchestration)
7. [Idempotency — The Non-Negotiable Foundation](#section-7-idempotency--the-non-negotiable-foundation)
8. [Durable Saga State Schema and State Machine](#section-8-durable-saga-state-schema-and-state-machine)
9. [Timeouts, Retries, and Backoff Policies](#section-9-timeouts-retries-and-backoff-policies)
10. [Dead Letter Queue (DLQ) Patterns](#section-10-dead-letter-queue-dlq-patterns)
11. [Spring Boot Orchestrator — Production Implementation](#section-11-spring-boot-orchestrator--production-implementation)
12. [Kafka Event-Driven Saga Architecture](#section-12-kafka-event-driven-saga-architecture)
13. [Integration with Transactional Outbox](#section-13-integration-with-transactional-outbox)
14. [Production Pitfalls and War Stories](#section-14-production-pitfalls-and-war-stories)
15. [Observability: Metrics, Logs, and Tracing](#section-15-observability-metrics-logs-and-tracing)
16. [Production Issue Runbook](#section-16-production-issue-runbook)
17. [Decision Framework: When to Use and When Not To](#section-17-decision-framework-when-to-use-and-when-not-to)
18. [Lead Interview Questions — Logical and Production Scenarios](#section-18-lead-interview-questions--logical-and-production-scenarios)
19. [Appendix: Quick Reference Cheat Sheet](#section-19-appendix-quick-reference-cheat-sheet)
20. [How to Talk About the Saga Pattern in an Interview](#section-20-how-to-talk-about-the-saga-pattern-in-an-interview)

---

## Section 1: What Is the Saga Pattern?

### 1.1 The Problem Sagas Solve

In a monolith, one business transaction maps to one database transaction:

```
BEGIN TRANSACTION
  INSERT order
  UPDATE inventory
  INSERT payment
COMMIT  -- all or nothing
```

In microservices, each service owns its database. Distributed two-phase commit (2PC) across heterogeneous services is:

- **Slow** — blocking locks across network boundaries
- **Fragile** — one coordinator failure stalls all participants
- **Impractical** — most cloud databases and SaaS APIs do not support XA/2PC

The Saga pattern replaces atomic distributed transactions with a **sequence of local transactions** plus **compensating transactions** that undo prior steps semantically.

### 1.2 Core Terminology

| Term | Definition |
|------|------------|
| **Saga** | A long-running business process spanning multiple services |
| **Forward step** | A local transaction that advances business state |
| **Compensation** | A semantic undo of a completed forward step — not a DB rollback |
| **Orchestrator** | Central coordinator that drives step sequence |
| **Choreography** | Decentralized flow where services react to events |
| **Saga instance** | One execution of a saga for a specific business operation |
| **Correlation ID** | `sagaId` tying all steps and events together |
| **Idempotency key** | Unique key per step preventing duplicate side effects |

### 1.3 The Canonical Order Flow

```
Forward:   ReserveInventory → CapturePayment → CreateShipment
Failure at CreateShipment:
Compensate: RefundPayment → ReleaseInventory  (reverse order)
```

This is demonstrated in `examples/saga/SagaDemo.java`:

```java
// From SagaDemo.java — reverse-order compensation on failure
for (int i = completed.size() - 1; i >= 0; i--) {
    Step s = completed.get(i);
    ctx.log("COMPENSATE " + s.name());
    s.compensate(ctx);
}
```

### 1.4 Saga vs 2PC vs Eventual Consistency

| Aspect | 2PC / XA | Saga | Single DB Transaction |
|--------|----------|------|----------------------|
| Consistency | Strong (atomic) | Eventual (semantic) | Strong |
| Availability | Low under partition | High | N/A (single service) |
| Coupling | Tight (blocking) | Loose (async possible) | None |
| Rollback | Automatic DB rollback | Explicit compensation | Automatic |
| Complexity | Coordinator + locks | State machine + idempotency | Trivial |
| External APIs | Usually unsupported | Natural fit | N/A |

### 1.5 The ACID Trade-off

Sagas give up **atomicity** and **isolation** across services. What you keep:

- **Atomicity per step** — each local transaction is still ACID within its service
- **Durability** — saga state and step outcomes are persisted
- **Consistency eventually** — business invariants restored via compensation

**What you must accept:**

- Intermediate visible state (inventory reserved but payment not yet captured)
- Compensation is not instantaneous — there is a window of inconsistency
- Not all steps are compensatable (email sent, physical shipment dispatched)

### 1.6 Saga Lifecycle States

```
                    ┌──────────┐
                    │ STARTED  │
                    └────┬─────┘
                         │
                    ┌────▼─────┐
              ┌─────┤ RUNNING  ├─────┐
              │     └────┬─────┘     │
              │          │           │
         step fails   all steps    timeout
              │       succeed        │
              │          │           │
         ┌────▼────┐ ┌───▼────┐  ┌───▼────┐
         │COMPENS- │ │COMPLETED│  │ STUCK  │
         │ ATING   │ └────────┘  │(manual)│
         └────┬────┘             └────────┘
              │
    ┌─────────┼─────────┐
    │                   │
┌───▼────────┐   ┌──────▼──────┐
│COMPENSATED │   │   FAILED    │
│(semantic   │   │(compensation│
│ undo done) │   │ incomplete) │
└────────────┘   └─────────────┘
```

---

## Section 2: The Problem: Why Not 2PC?

### 2.1 The Distributed Transaction Problem

In a monolith with one database, `@Transactional` gives you atomic commit or rollback. In microservices, each service owns its database. You cannot wrap three remote HTTP calls in one JDBC transaction.

### 2.2 What Is Two-Phase Commit (2PC)?

2PC is a distributed consensus protocol:

```
Phase 1 (Prepare): Coordinator asks all participants "Can you commit?"
Phase 2 (Commit/Abort): If all YES → COMMIT; if any NO → ABORT
```

Classic implementations: XA transactions (JTA), some enterprise database middleware.

### 2.3 Why 2PC Fails in Microservices

| Problem | Impact |
|---------|--------|
| Blocking locks | Participants hold locks during prepare phase — slow services block everyone |
| Coordinator SPOF | Coordinator crash after prepare leaves participants stuck in-doubt |
| Latency amplification | N services = N consensus round-trips minimum |
| Availability | One dead participant blocks the entire transaction |
| Cloud incompatibility | DynamoDB, Firestore, most SaaS APIs do not support XA |

### 2.4 Naive Alternatives That Also Fail

**Dual writes without coordination:**

```java
// ANTI-PATTERN
orderRepository.save(order);              // succeeds
kafkaTemplate.send("order-placed", event); // fails — downstream never notified
```

**Saga without durable state, idempotency, or compensation strategy:** Works in demos. Fails in production within weeks.

---

## Section 3: Saga vs 2PC vs Local Transaction

### 3.1 Comparison Matrix

| Dimension | Local TX | 2PC (XA/JTA) | Saga |
|-----------|----------|--------------|------|
| Scope | Single database | Multiple databases | Multiple services |
| Consistency | Immediate ACID | Immediate ACID (when it works) | Eventual |
| Rollback | Automatic (DB) | Automatic (coordinator) | Manual compensations |
| Availability | High | Low under failure | High |
| Latency | Low | High | Medium |
| Cloud compatibility | Universal | Poor | Excellent |
| Design effort | Low | Medium (infra) | High (business logic) |

### 3.2 When Each Applies

| Scenario | Recommendation |
|----------|----------------|
| Single service, single DB | **Local transaction** — always prefer |
| Legacy enterprise, XA-capable DBs, low volume | **2PC** — rare, declining |
| Multi-service flow, tolerates seconds of inconsistency | **Saga** |
| Strict atomicity (double-entry ledger) | Single ledger service with local TX |

### 3.3 Consistency Windows

During a saga, the system is in an **inconsistent intermediate state** between step commits and compensation completion. Other services and UIs must tolerate this window.

### 3.4 Semantic vs Technical Rollback

| Type | Mechanism | Example |
|------|-----------|---------|
| Technical rollback | Database UNDO log | `@Transactional` throws → JDBC rollback |
| Semantic rollback | Business compensating action | `refundPayment(orderId, idempotencyKey)` |

Compensations are **new business transactions**, not inverse SQL. Refunds may take days. Emails cannot be unsent.

---

## Section 4: Forward Steps and Compensating Transactions

### 4.1 Designing Forward Steps

Each forward step should:

1. Perform one atomic local transaction in one service.
2. Persist enough state to support idempotent replay and compensation.
3. Carry `sagaId`, `stepId`, and `idempotencyKey` on every invocation.

```java
public record SagaCommand(
    UUID sagaId,
    String stepId,
    String idempotencyKey,
    String aggregateId,
    Map<String, Object> payload
) {}
```

### 4.2 Designing Compensations

| Rule | Rationale |
|------|-----------|
| Idempotent | Retries will happen; double-refund is catastrophic |
| Semantic, not mechanical | "Cancel reservation" not "DELETE FROM reservations" |
| Reverse order only | Compensate C before B before A |
| Persist compensation status | Track COMPENSATING → COMPENSATED → COMPENSATION_FAILED |

From `SagaDemo.java`:

```java
public void compensate(Context ctx) {
    if (ctx.paymentCaptured) {
        ctx.paymentCaptured = false;
        ctx.log("Payment refunded");
    } else {
        ctx.log("Payment compensation was idempotent no-op");
    }
}
```

### 4.3 Compensatable vs Non-Compensatable Steps

| Step | Compensatable? | Notes |
|------|----------------|-------|
| Reserve inventory | Yes | Release reservation |
| Capture payment | Yes | Issue refund with idempotency key |
| Send email | **No** | Send correction email instead |
| Print shipping label | Pivot | After label printed, only forward-fix |

### 4.4 Compensation Failure Handling

If compensation fails after forward steps succeeded:

1. Retry with exponential backoff.
2. Persist `COMPENSATION_FAILED` — never silently drop.
3. Alert on-call with saga timeline.
4. Idempotent retry job sweeps stuck compensations.

---

## Section 5: Choreography vs Orchestration

### 5.1 Choreography — Event-Driven, Decentralized

Each service listens for events, performs its local transaction, and publishes the next event. No central brain.

```
OrderService          InventoryService         PaymentService
     │                       │                       │
     │── OrderCreated ──────►│                       │
     │                       │── InventoryReserved ─►│
     │                       │                       │── PaymentCaptured ──►...
     │                       │                       │
     │◄── PaymentFailed ─────┼───────────────────────│
     │── ReleaseInventory ──►│                       │
```

**Strengths:**

- Loose coupling — services only know events, not each other
- No single point of failure (no orchestrator)
- Natural fit for event-driven architectures

**Weaknesses:**

- Flow logic is distributed — hard to answer "where is order X right now?"
- Cyclic dependencies emerge as flows grow
- Debugging requires reconstructing timelines from multiple services
- Out-of-order events require strict state transition guards

### 5.2 Orchestration — Centralized Coordinator

One orchestrator service drives the sequence, calls participants (sync) or publishes commands (async), and manages compensation.

```
                    ┌─────────────────┐
                    │  Orchestrator   │
                    └────────┬────────┘
                             │
          ┌──────────────────┼──────────────────┐
          ▼                  ▼                  ▼
   ┌─────────────┐   ┌─────────────┐   ┌─────────────┐
   │  Inventory  │   │   Payment   │   │  Shipment   │
   └─────────────┘   └─────────────┘   └─────────────┘
```

**Strengths:**

- Single place for flow logic, timeouts, retries
- Easy observability — one saga timeline per `sagaId`
- Explicit compensation sequence
- Easier to change flow without touching every service

**Weaknesses:**

- Orchestrator is a critical component — must be HA
- Risk of "god service" if business logic leaks into orchestrator
- Tighter coupling between orchestrator and participants

### 5.3 Decision Matrix

| Factor | Choreography | Orchestration |
|--------|-------------|---------------|
| Flow complexity | Simple (2-3 steps) | Any complexity |
| Team structure | Autonomous domain teams | Platform/flow team owns orchestrator |
| Observability needs | Low | High (regulated, SLA-driven) |
| Change frequency | Stable flows | Frequently evolving flows |
| Debugging culture | Strong distributed tracing | Centralized ops preferred |
| Long-running workflows | Awkward | Natural (async orchestration) |

### 5.4 Production Recommendation

| Scenario | Recommendation |
|----------|----------------|
| 2-3 step flow, mature event culture | Choreography |
| 4+ steps, compensation paths, SLAs | Orchestration (async) |
| User-facing, low-latency checkout | Sync orchestration with circuit breakers |
| Multi-day workflows (KYC, onboarding) | Async orchestration with durable state |
| Mixed: simple sub-flows in complex saga | Orchestrator + choreography within steps |

### 5.5 Anti-Pattern: Hybrid Without Boundaries

Teams sometimes start with choreography, add an orchestrator for one path, and end up with both patterns in the same flow without clear ownership. Result: duplicate logic, conflicting compensation triggers, and impossible incident triage.

**Rule:** Pick one coordination style per saga definition. If you need both, the orchestrator should be the sole authority for compensation triggers.

---

## Section 6: Synchronous vs Asynchronous Orchestration

### 6.1 How Sync Orchestration Works

The orchestrator calls each service via HTTP/gRPC, waits for the response, then proceeds. On failure, it immediately runs compensations in reverse order.

Demonstrated in `examples/saga/SagaOrchestratorModesDemo.java`:

```java
// SyncOrchestrator — serial blocking calls
for (ServiceStep step : forward) {
    ctx.log("CALL " + step.name());
    step.execute(ctx);
    completed.add(step);
}
// On failure: rollback in reverse
for (int i = completed.size() - 1; i >= 0; i--) {
    completed.get(i).compensate(ctx);
}
```

### 6.2 Sync Flow — A→B→C→D

**Happy path:**

1. `A.execute()` succeeds → persist step A DONE
2. `B.execute()` succeeds → persist step B DONE
3. `C.execute()` succeeds → persist step C DONE
4. `D.execute()` succeeds → saga COMPLETED

**Failure path (D fails after A, B, C succeed):**

1. D throws exception
2. Orchestrator calls `C.compensate()`, then `B.compensate()`, then `A.compensate()`
3. Saga status → COMPENSATED

### 6.3 Sync Strengths and Weaknesses

| Strengths | Weaknesses |
|-----------|------------|
| Simple control flow | Serial latency accumulation |
| Single request trace for debugging | Tight runtime coupling |
| Fast decisioning for short flows | Thread blocked during downstream calls |
| Easy to reason about in code | Cascading failure without circuit breakers |
| Good for user-facing synchronous APIs | Hard to survive long downstream outages |

### 6.4 When to Use Sync Orchestration

1. **Low latency requirement** — user waits for the result (checkout, booking)
2. **Few services** — 2-4 steps, each responding in < 500ms
3. **Strong operational control** — team can own end-to-end tracing
4. **Short workflows** — completes in seconds, not minutes

### 6.5 Sync Production Stack

| Layer | Technology |
|-------|-----------|
| HTTP client | Spring WebClient or OpenFeign |
| Resilience | Resilience4j (retry, circuit breaker, time limiter) |
| Saga state | PostgreSQL (`saga_instance`, `saga_step`) |
| Tracing | OpenTelemetry + W3C `traceparent` |
| Metrics | Micrometer + Prometheus |

### 6.6 Sync Orchestrator — Production Skeleton

```java
@Service
@RequiredArgsConstructor
public class SyncOrderSagaOrchestrator {

    private final SagaStateRepository sagaRepo;
    private final InventoryClient inventoryClient;
    private final PaymentClient paymentClient;
    private final ShipmentClient shipmentClient;
    private final MeterRegistry meterRegistry;

    @Transactional
    public SagaResult execute(String sagaId, OrderCommand cmd) {
        sagaRepo.start(sagaId, SagaStatus.RUNNING);
        meterRegistry.counter("saga_started_total", "type", "sync").increment();

        List<String> completedSteps = new ArrayList<>();

        try {
            executeStep(sagaId, "RESERVE_INVENTORY", () ->
                inventoryClient.reserve(sagaId, idempotencyKey(sagaId, "RESERVE_INVENTORY"), cmd));
            completedSteps.add("RESERVE_INVENTORY");

            executeStep(sagaId, "CAPTURE_PAYMENT", () ->
                paymentClient.capture(sagaId, idempotencyKey(sagaId, "CAPTURE_PAYMENT"), cmd));
            completedSteps.add("CAPTURE_PAYMENT");

            executeStep(sagaId, "CREATE_SHIPMENT", () ->
                shipmentClient.create(sagaId, idempotencyKey(sagaId, "CREATE_SHIPMENT"), cmd));
            completedSteps.add("CREATE_SHIPMENT");

            sagaRepo.complete(sagaId);
            meterRegistry.counter("saga_completed_total", "type", "sync").increment();
            return SagaResult.success(sagaId);

        } catch (Exception ex) {
            sagaRepo.failStep(sagaId, ex.getMessage());
            compensateReverse(sagaId, cmd, completedSteps);
            meterRegistry.counter("saga_compensated_total", "type", "sync").increment();
            return SagaResult.compensated(sagaId, ex.getMessage());
        }
    }

    private void executeStep(String sagaId, String stepName, Runnable action) {
        Timer.Sample sample = Timer.start(meterRegistry);
        sagaRepo.markPending(sagaId, stepName);
        try {
            action.run();
            sagaRepo.markDone(sagaId, stepName);
        } finally {
            sample.stop(meterRegistry.timer("saga_step_latency_ms", "step", stepName));
        }
    }

    private void compensateReverse(String sagaId, OrderCommand cmd, List<String> completed) {
        sagaRepo.markCompensating(sagaId);
        for (int i = completed.size() - 1; i >= 0; i--) {
            String step = completed.get(i);
            switch (step) {
                case "CREATE_SHIPMENT" -> safeCompensateShipment(sagaId, cmd);
                case "CAPTURE_PAYMENT"   -> safeCompensatePayment(sagaId, cmd);
                case "RESERVE_INVENTORY" -> safeCompensateInventory(sagaId, cmd);
            }
        }
        sagaRepo.markCompensated(sagaId);
    }

    private String idempotencyKey(String sagaId, String step) {
        return sagaId + ":" + step;
    }
}
```

### 6.7 Resilience4j Configuration for Sync Calls

```java
@Configuration
public class ResilienceConfig {

    @Bean
    public RetryConfig retryConfig() {
        return RetryConfig.custom()
            .maxAttempts(3)
            .waitDuration(Duration.ofMillis(500))
            .retryExceptions(TimeoutException.class, WebClientResponseException.class)
            .ignoreExceptions(BusinessException.class) // don't retry 4xx business errors
            .build();
    }

    @Bean
    public CircuitBreakerConfig circuitBreakerConfig() {
        return CircuitBreakerConfig.custom()
            .failureRateThreshold(50)
            .waitDurationInOpenState(Duration.ofSeconds(30))
            .slidingWindowSize(20)
            .build();
    }

    @Bean
    public TimeLimiterConfig timeLimiterConfig() {
        return TimeLimiterConfig.custom()
            .timeoutDuration(Duration.ofSeconds(5))
            .build();
    }
}
```

```yaml
# application.yml
resilience4j:
  retry:
    instances:
      inventoryService:
        maxAttempts: 3
        waitDuration: 500ms
      paymentService:
        maxAttempts: 2
        waitDuration: 1s
  circuitbreaker:
    instances:
      inventoryService:
        failureRateThreshold: 50
        waitDurationInOpenState: 30s
      paymentService:
        failureRateThreshold: 40
        waitDurationInOpenState: 60s
  timelimiter:
    instances:
      inventoryService:
        timeoutDuration: 5s
      paymentService:
        timeoutDuration: 10s
```

---

### 6.8 Async Orchestration Overview

Async orchestration decouples the orchestrator from participant availability. The orchestrator emits command events; services reply with success/failure events. State is persisted durably; the orchestrator reacts to replies without blocking a thread.

From `examples/saga/SagaOrchestratorModesDemo.java`:

```java
// AsyncOrchestrator — event-driven state machine
switch (e.type) {
    case DO_A -> executeStepAsync(ctx, a, EventType.A_DONE, EventType.A_FAILED);
    case A_DONE -> queue.add(new Event(EventType.DO_B));
    case DO_B -> executeStepAsync(ctx, b, EventType.B_DONE, EventType.B_FAILED);
    case B_DONE -> queue.add(new Event(EventType.DO_C));
    case DO_C -> executeStepAsync(ctx, c, EventType.C_DONE, EventType.C_FAILED);
    case C_DONE -> queue.add(new Event(EventType.DO_D));
    case DO_D -> executeStepAsync(ctx, d, EventType.D_DONE, EventType.D_FAILED);
    case D_DONE -> ctx.log("ASYNC SAGA SUCCESS");
    case D_FAILED -> {
        ctx.log("ASYNC FAILURE at D -> start compensation");
        queue.add(new Event(EventType.UNDO_C));
    }
    case UNDO_C -> { c.compensate(ctx); queue.add(new Event(EventType.C_COMP_DONE)); }
    // ... reverse compensation chain to UNDO_A ...
}
```

Run the demo:

```bash
javac examples/saga/SagaOrchestratorModesDemo.java && \
  java -cp examples/saga SagaOrchestratorModesDemo
```

### 6.9 Sync vs Async Comparison

| Factor | Sync | Async |
|--------|------|-------|
| Flow duration | Seconds | Minutes to days |
| Latency sensitivity | High (user waiting) | Low (background) |
| Service availability | All must be up | Services can be temporarily down |
| Debugging | Single request trace | Requires correlation IDs |
| Failure/retry rate | Low | High |
| **Default for complex systems** | | **Async** |

From `docs/saga-orchestrator-sync-vs-async.md`:

> In most real microservice systems, async orchestration becomes the safer default once workflow complexity grows.

### 6.10 Hybrid: Sync Start, Async Finish

```java
// Phase 1: Sync — user waits for payment authorization (~500ms)
PaymentAuthResult auth = paymentClient.authorize(cmd);

// Phase 2: Async — fulfillment continues in background
sagaCommandPublisher.publish(new StartFulfillmentSaga(sagaId, auth.paymentId()));

return Response.accepted()
    .body(new OrderAcceptedResponse(sagaId, "Order accepted — fulfillment in progress"));
```

### 6.11 Production Rules (Non-Negotiable)

From merged `docs/saga-orchestrator-sync-vs-async.md` and `docs/saga.md`:

1. Every command must carry `sagaId` + `stepId` + idempotency key.
2. Every step transition must be persisted durably **before** sending next command.
3. Compensations must be idempotent and retry-safe.
4. Timeouts must trigger retry or manual intervention state.
5. Dead-letter queues must be observed and drained with runbooks.
6. Emit observability: step latency, retries, compensation counts, stuck saga count.

---

## Section 7: Idempotency — The Non-Negotiable Foundation

### 7.1 Why Idempotency Is Mandatory

In distributed systems, **every operation will be retried**. Causes: network timeout after remote commit, Kafka at-least-once delivery, orchestrator crash, client retry on 504.

Without idempotency: double charge, double shipment, duplicate inventory release.

### 7.2 Idempotency Key Design

```java
public record IdempotencyContext(
    UUID sagaId,
    String stepId,
    String idempotencyKey,
    Instant createdAt
) {
    public static String deriveKey(UUID sagaId, String stepId) {
        return sagaId + ":" + stepId;
    }
}
```

### 7.3 Idempotency Table Schema

```sql
create table idempotency_record (
    idempotency_key   varchar(200) primary key,
    saga_id           uuid not null,
    step_id           varchar(100) not null,
    status            varchar(30) not null,
    response_payload  jsonb,
    created_at        timestamptz not null default now(),
    completed_at      timestamptz
);
create index idx_idempotency_saga on idempotency_record(saga_id, step_id);
```

### 7.4 Idempotency Guard (from SagaDemo.java)

```java
boolean markExecuted(String stepName) {
    return executedStepKeys.add(sagaId + ":" + stepName);
}

public void execute(Context ctx) {
    if (!ctx.markExecuted(name())) {
        ctx.log("SKIP duplicate command for " + name());
        return;
    }
    // actual business logic
}
```

**Production version:**

```java
@Service
@RequiredArgsConstructor
public class IdempotencyGuard {

    private final IdempotencyRecordRepository repository;

    @Transactional
    public <T> T executeOnce(IdempotencyContext ctx, Supplier<T> action) {
        Optional<IdempotencyRecord> existing = repository.findById(ctx.idempotencyKey());
        if (existing.isPresent()) {
            return switch (existing.get().getStatus()) {
                case COMPLETED -> deserialize(existing.get().getResponsePayload());
                case IN_PROGRESS -> throw new IdempotencyConflictException(ctx.idempotencyKey());
                case FAILED -> throw new PreviousAttemptFailedException(ctx.idempotencyKey());
            };
        }
        repository.save(IdempotencyRecord.inProgress(ctx));
        try {
            T result = action.get();
            repository.markCompleted(ctx.idempotencyKey(), serialize(result));
            return result;
        } catch (Exception ex) {
            repository.markFailed(ctx.idempotencyKey(), ex.getMessage());
            throw ex;
        }
    }
}
```

### 7.5 Payment Provider Idempotency

```java
stripeClient.charges().create(
    ChargeCreateParams.builder()
        .setAmount(cmd.amountCents())
        .setIdempotencyKey(cmd.idempotencyKey())
        .build()
);
```

### 7.6 Orchestrator-Level Event Dedupe

```java
@KafkaListener(topics = "saga.step-results")
public void onStepResult(StepResultEvent event) {
    if (!sagaEventDedupe.markProcessed(event.eventId())) {
        return; // duplicate — safe to ignore
    }
    sagaStateMachine.onEvent(event.sagaId(), event);
}
```

---

## Section 8: Durable Saga State Schema and State Machine

### 8.1 Why Durable State Is Non-Negotiable

If the orchestrator crashes mid-saga without durable state, you don't know which steps completed → double execution or missed compensation.

**Rule:** Persist state **before** sending the next command (write-ahead log pattern).

### 8.2 Saga Instance Schema

```sql
create table saga_instance (
    saga_id              uuid primary key,
    saga_type            varchar(100) not null,
    status               varchar(30) not null,
    current_step         varchar(100),
    correlation_id       varchar(100) not null,
    tenant_id            varchar(50),
    payload              jsonb not null,
    context              jsonb not null default '{}',
    retry_count          int not null default 0,
    max_retries          int not null default 3,
    started_at           timestamptz not null default now(),
    updated_at           timestamptz not null default now(),
    completed_at         timestamptz,
    failed_at            timestamptz,
    failure_reason       text,
    version              bigint not null default 0
);

create index idx_saga_status on saga_instance(status, updated_at);
create index idx_saga_correlation on saga_instance(correlation_id);
create index idx_saga_stuck on saga_instance(status, updated_at)
    where status in ('RUNNING', 'COMPENSATING', 'AWAITING_STEP');
```

### 8.3 Saga Step Log Schema

```sql
create table saga_step_log (
    id              bigserial primary key,
    saga_id         uuid not null references saga_instance(saga_id),
    step_id         varchar(100) not null,
    step_type       varchar(20) not null,
    status          varchar(30) not null,
    idempotency_key varchar(200) not null,
    request_payload jsonb,
    response_payload jsonb,
    error_message   text,
    started_at      timestamptz not null default now(),
    completed_at    timestamptz,
    duration_ms     bigint
);

create unique index idx_step_log_idempotency on saga_step_log(idempotency_key);
create index idx_step_log_saga on saga_step_log(saga_id, started_at);
```

### 8.4 State Machine

```java
public enum SagaStatus {
    STARTED,
    RUNNING,
    AWAITING_STEP,
    COMPENSATING,
    COMPLETED,
    COMPENSATED,
    FAILED,
    COMPENSATION_FAILED,
    REQUIRES_MANUAL_INTERVENTION,
    TIMED_OUT
}
```

```
STARTED → RUNNING → AWAITING_STEP → (step success) → RUNNING → ... → COMPLETED
                  ↘ (step fail) → COMPENSATING → COMPENSATED
                                              ↘ COMPENSATION_FAILED
```

### 8.5 State Machine Implementation

```java
@Service
@RequiredArgsConstructor
public class SagaStateMachine {

    private final SagaInstanceRepository sagaRepository;
    private final SagaStepLogRepository stepLogRepository;
    private final SagaCommandPublisher commandPublisher;

    @Transactional
    public void onStepCompleted(UUID sagaId, StepCompletedEvent event) {
        SagaInstance saga = sagaRepository.findByIdForUpdate(sagaId)
            .orElseThrow(() -> new SagaNotFoundException(sagaId));

        stepLogRepository.save(SagaStepLog.completed(sagaId, event));

        if (saga.hasMoreForwardSteps()) {
            SagaStep nextStep = saga.nextStep();
            saga.transitionTo(SagaStatus.AWAITING_STEP, nextStep.id());
            sagaRepository.save(saga);
            commandPublisher.publish(nextStep.buildCommand(saga));
        } else {
            saga.transitionTo(SagaStatus.COMPLETED, null);
            sagaRepository.save(saga);
        }
    }

    @Transactional
    public void onStepFailed(UUID sagaId, StepFailedEvent event) {
        SagaInstance saga = sagaRepository.findByIdForUpdate(sagaId)
            .orElseThrow(() -> new SagaNotFoundException(sagaId));

        stepLogRepository.save(SagaStepLog.failed(sagaId, event));
        saga.transitionTo(SagaStatus.COMPENSATING, null);
        sagaRepository.save(saga);

        SagaStep lastCompleted = saga.lastCompletedForwardStep();
        if (lastCompleted != null) {
            commandPublisher.publish(lastCompleted.buildCompensationCommand(saga));
        }
    }
}
```

### 8.6 Stuck Saga Detection

```sql
select saga_id, saga_type, status, current_step, updated_at
from saga_instance
where status in ('RUNNING', 'AWAITING_STEP', 'COMPENSATING')
  and updated_at < now() - interval '15 minutes'
order by updated_at;
```

---

## Section 9: Timeouts, Retries, and Backoff Policies

### 9.1 Per-Step Timeout Configuration

```java
public record StepPolicy(Duration timeout, int maxRetries, Duration initialBackoff) {}

Map<String, StepPolicy> policies = Map.of(
    "RESERVE_INVENTORY", new StepPolicy(Duration.ofSeconds(5),  3, Duration.ofSeconds(2)),
    "CAPTURE_PAYMENT",   new StepPolicy(Duration.ofSeconds(10), 3, Duration.ofSeconds(5)),
    "CREATE_SHIPMENT",   new StepPolicy(Duration.ofSeconds(30), 2, Duration.ofSeconds(10))
);
```

### 9.2 Timeout Enforcement

```java
@Scheduled(fixedDelay = 30_000)
public void detectTimedOutSteps() {
    List<SagaInstance> stuck = sagaRepository.findAwaitingStepOlderThan(
        Instant.now().minus(Duration.ofMinutes(5))
    );
    for (SagaInstance saga : stuck) {
        StepPolicy policy = stepConfig.getPolicy(saga.getCurrentStep());
        if (saga.getRetryCount() < policy.maxRetries()) {
            saga.incrementRetry();
            commandPublisher.republishLastCommand(saga);
        } else {
            sagaStateMachine.onStepFailed(saga.getSagaId(),
                StepFailedEvent.timeout(saga.getSagaId(), saga.getCurrentStep()));
        }
    }
}
```

### 9.3 Exponential Backoff with Jitter

```java
public Duration calculateBackoff(int attempt, Duration initial, Duration max) {
    long exponentialMs = (long) (initial.toMillis() * Math.pow(2, attempt));
    long cappedMs = Math.min(exponentialMs, max.toMillis());
    long jitterMs = ThreadLocalRandom.current().nextLong(0, cappedMs / 4);
    return Duration.ofMillis(cappedMs + jitterMs);
}
```

### 9.4 Retry vs Compensation Decision

| Condition | Action |
|-----------|--------|
| Transient error (503, timeout) | Retry with backoff |
| Permanent error (400 business rule) | Fail immediately → compensate |
| Unknown after max retries | Fail → compensate |
| Compensation failure | Retry compensation separately; alert |

### 9.5 Critical: HTTP Timeout ≠ Business Failure

The remote service may have committed before the timeout. Always use idempotency keys on retry.

---

## Section 10: Dead Letter Queue (DLQ) Patterns

### 10.1 What Goes to DLQ

- Deserialization failures (schema mismatch)
- Poison commands (bug causing infinite exceptions)
- Saga state machine violations (event for COMPLETED saga)
- Compensation failures after max retries

### 10.2 Kafka DLQ Architecture

```
saga.commands ──► Consumer ──► process
                    │ (after 3 retries)
                    ▼
             saga.commands.dlq ──► Manual review / replay
```

```java
@Bean
public DefaultErrorHandler sagaErrorHandler(KafkaTemplate<String, byte[]> kafkaTemplate) {
    DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
        kafkaTemplate,
        (record, ex) -> new TopicPartition(record.topic() + ".dlq", record.partition())
    );
    DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer);
    errorHandler.addNotRetryableExceptions(
        JsonProcessingException.class,
        SagaStateViolationException.class
    );
    errorHandler.setBackOffFunction((record, ex) -> new FixedBackOff(2000L, 3L));
    return errorHandler;
}
```

### 10.3 DLQ Message Envelope

```java
public record DlqEnvelope(
    String originalTopic,
    long originalOffset,
    Instant failedAt,
    String failureReason,
    UUID sagaId,
    String stepId,
    byte[] originalPayload
) {}
```

### 10.4 COMPENSATION_FAILED — Most Dangerous Scenario

```sql
select saga_id, saga_type, failure_reason, updated_at
from saga_instance
where status = 'COMPENSATION_FAILED'
order by updated_at;
```

Page immediately when count > 0.

### 10.5 DLQ Alert Thresholds

| Metric | Alert |
|--------|-------|
| `saga_dlq_messages_total` rate | > 0 sustained 5 min |
| DLQ topic lag | > 10 messages |
| `COMPENSATION_FAILED` count | > 0 (page immediately) |
| Stuck sagas > 15 min | > 5 |

## Section 11: Spring Boot Orchestrator — Production Implementation

### 11.1 Project Structure

```
order-saga-orchestrator/
├── domain/          SagaInstance, SagaStatus, SagaStepLog
├── application/     StartOrderSagaUseCase, SagaStateMachine, StuckSagaRecoveryJob
├── infrastructure/  JPA repos, Kafka publishers/listeners, HTTP clients
└── api/             OrderSagaController, SagaAdminController
```

### 11.2 REST API — Start Saga

```java
@RestController
@RequestMapping("/api/v1/sagas/orders")
@RequiredArgsConstructor
public class OrderSagaController {

    private final StartOrderSagaUseCase startOrderSaga;

    @PostMapping
    public ResponseEntity<OrderSagaResponse> start(
            @RequestBody @Valid PlaceOrderRequest request,
            @RequestHeader(value = "Idempotency-Key", required = false) String clientKey) {

        UUID sagaId = UUID.randomUUID();
        String idempotencyKey = clientKey != null ? clientKey : sagaId.toString();
        OrderSagaResponse response = startOrderSaga.execute(
            new StartOrderSagaCommand(sagaId, idempotencyKey, request)
        );
        return ResponseEntity.accepted()
            .header("Location", "/api/v1/sagas/orders/" + sagaId)
            .body(response);
    }

    @GetMapping("/{sagaId}")
    public OrderSagaStatusResponse getStatus(@PathVariable UUID sagaId) {
        return startOrderSaga.getStatus(sagaId);
    }
}
```

### 11.3 Start Saga Use Case

```java
@Service
@RequiredArgsConstructor
public class StartOrderSagaUseCase {

    private final SagaInstanceRepository sagaRepository;
    private final SagaCommandPublisher commandPublisher;
    private final MeterRegistry meterRegistry;

    @Transactional
    public OrderSagaResponse execute(StartOrderSagaCommand cmd) {
        Optional<SagaInstance> existing = sagaRepository
            .findByClientIdempotencyKey(cmd.clientIdempotencyKey());
        if (existing.isPresent()) {
            return OrderSagaResponse.from(existing.get());
        }

        SagaInstance saga = SagaInstance.start(cmd.sagaId(), SagaType.ORDER_PLACEMENT,
            cmd.clientIdempotencyKey(), cmd.request());
        saga.transitionTo(SagaStatus.AWAITING_STEP, Step.RESERVE_INVENTORY.name());
        sagaRepository.save(saga);

        meterRegistry.counter("saga_started_total", "type", "ORDER_PLACEMENT").increment();
        commandPublisher.publishReserveInventory(saga);
        return OrderSagaResponse.from(saga);
    }
}
```

### 11.4 Participant Service — Inventory Step

```java
@Service
@RequiredArgsConstructor
public class InventorySagaHandler {

    private final InventoryService inventoryService;
    private final IdempotencyGuard idempotencyGuard;
    private final OutboxEventPublisher outboxPublisher;

    @KafkaListener(topics = "saga.commands.inventory")
    @Transactional
    public void onReserveCommand(ReserveInventoryCommand cmd) {
        idempotencyGuard.executeOnce(cmd.idempotencyContext(), () -> {
            Reservation reservation = inventoryService.reserve(
                cmd.sagaId(), cmd.orderId(), cmd.items()
            );
            outboxPublisher.publish(OutboxEvent.pending(
                UUID.randomUUID(), "Reservation", reservation.getId().toString(),
                "InventoryReserved.v1",
                toJson(new InventoryReservedPayload(cmd.sagaId(), reservation.getId())),
                Map.of("sagaId", cmd.sagaId().toString(), "stepId", "RESERVE_INVENTORY")
            ));
            return reservation;
        });
    }
}
```

### 11.5 Mapping Repo Demo to Production

| `SagaDemo.java` | Production Equivalent |
|-----------------|----------------------|
| `Context.sagaId` | `saga_instance.saga_id` |
| `executedStepKeys` | `idempotency_record` table |
| `timeline` list | `saga_step_log` table |
| `SagaOrchestrator.run()` | `SagaStateMachine` + Kafka |
| Reverse compensation loop | `SagaStateMachine.onStepFailed()` |

Run `SagaDemo.java`:

```bash
javac examples/saga/SagaDemo.java && java -cp examples/saga SagaDemo
```

Expected output includes `SAGA SUCCESS`, `SAGA COMPENSATED`, and `SKIP duplicate command`.

### 11.6 Application Properties

```yaml
spring:
  kafka:
    consumer:
      enable-auto-commit: false
      isolation-level: read_committed
    producer:
      acks: all
      enable-idempotence: true

saga:
  topics:
    commands: saga.commands
    step-results: saga.step-results
  recovery:
    stuck-threshold-minutes: 15
  steps:
    RESERVE_INVENTORY:
      timeout-seconds: 5
      max-retries: 3
    CAPTURE_PAYMENT:
      timeout-seconds: 10
      max-retries: 3
```

---

## Section 12: Kafka Event-Driven Saga Architecture

### 12.1 Topic Design

| Topic | Producer | Consumer | Partition Key |
|-------|----------|----------|---------------|
| `saga.commands.inventory` | orchestrator | inventory-service | sagaId |
| `saga.commands.payment` | orchestrator | payment-service | sagaId |
| `saga.commands.shipping` | orchestrator | shipping-service | sagaId |
| `saga.step-results` | all participants | orchestrator | sagaId |
| `saga.commands.*.dlq` | error handler | ops/replay | sagaId |

**Partition key = sagaId** guarantees ordering of all events for one saga within a partition.

### 12.2 Event Envelope

```java
public record SagaEventEnvelope<T>(
    UUID eventId,
    String eventType,
    UUID sagaId,
    String stepId,
    String sagaType,
    Instant occurredAt,
    T payload,
    Map<String, String> headers
) {}
```

### 12.3 Publish After DB Commit

```java
@TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
public void onSagaStepReady(SagaStepReadyEvent event) {
    kafkaTemplate.send("saga.commands." + event.serviceName(),
        event.sagaId().toString(), event.envelope());
}
```

### 12.4 Mapping SagaOrchestratorModesDemo to Kafka

| Demo | Kafka Production |
|------|------------------|
| `EventType.DO_A` | Command on `saga.commands.a` |
| `EventType.A_DONE` | Reply on `saga.step-results` |
| `Deque<Event> queue` | Orchestrator state machine + DB |
| `D_FAILED → UNDO_C` | `SagaStateMachine.onStepFailed()` |

### 12.5 Schema Evolution

Version event types: `InventoryReserved.v1` → `InventoryReserved.v2`. Consumers tolerate unknown fields. Use Schema Registry in production.

---

## Section 13: Integration with Transactional Outbox

### 13.1 The Dual-Write Problem

Every saga participant must atomically: (1) update local DB, (2) publish event. Without Outbox, either can fail independently.

See `docs/outbox.md` for full playbook.

### 13.2 Outbox in Saga Participant

```java
@Transactional
public Reservation reserve(UUID sagaId, UUID orderId, List<OrderItem> items) {
    Reservation reservation = reservationRepository.save(
        Reservation.create(sagaId, orderId, items)
    );
    outboxRepository.save(OutboxEvent.pending(
        UUID.randomUUID(), "Reservation", reservation.getId().toString(),
        "InventoryReserved.v1",
        objectMapper.writeValueAsString(new InventoryReservedPayload(sagaId, reservation.getId())),
        Map.of("sagaId", sagaId.toString())
    ));
    return reservation;
}
```

### 13.3 Outbox Relay

```
outbox_event (PENDING) ──► Debezium/polling relay ──► saga.step-results
                              └── marks outbox SENT
```

From `docs/outbox.md` Scenario 2 (Payment Capture in a Saga):

> If relay crashes after publishing but before marking SENT, same event can be re-published. Orchestrator dedupes by `event_id` and proceeds once.

### 13.4 Orchestrator's Own Outbox

```java
@Transactional
public void advanceToNextStep(SagaInstance saga, SagaStep nextStep) {
    saga.transitionTo(SagaStatus.AWAITING_STEP, nextStep.id());
    sagaRepository.save(saga);
    outboxRepository.save(OutboxEvent.pending(
        UUID.randomUUID(), "SagaInstance", saga.getSagaId().toString(),
        nextStep.commandEventType(), nextStep.buildPayload(saga),
        Map.of("sagaId", saga.getSagaId().toString())
    ));
}
```

### 13.5 End-to-End Flow

```
1. Client → POST /orders → orchestrator
2. Orchestrator: INSERT saga_instance + INSERT outbox (ReserveInventory)
3. Relay → Kafka saga.commands.inventory
4. inventory-service: INSERT reservation + INSERT outbox (InventoryReserved)
5. Relay → Kafka saga.step-results
6. Orchestrator: UPDATE saga + INSERT outbox (CapturePayment)
7. ... continues until COMPLETED
```

Every cross-service arrow goes through Outbox → Kafka. No naked dual writes.

---

## Section 14: Production Pitfalls and War Stories

### 14.1 Network Timeout After Remote Commit

Orchestrator times out on payment call. Payment committed. Retry without idempotency → double charge.

**Fix:** Idempotency key = `sagaId + ":CAPTURE_PAYMENT"` at payment-service and payment gateway.

### 14.2 Compensating a Step That Never Succeeded

Inventory reserve timed out (orchestrator thinks failed). Compensation runs but inventory actually reserved.

**Fix:** Compensation checks local aggregate state before acting.

### 14.3 Lost Saga State After Crash

Orchestrator publishes command, crashes before persisting AWAITING_STEP.

**Fix:** Persist state BEFORE publish. Recovery job republishes for stuck sagas.

### 14.4 Out-of-Order Events in Choreography

`PaymentCaptured` arrives before `InventoryReserved`.

**Fix:** Validate state transitions; invalid → DLQ.

### 14.5 Non-Compensatable Side Effects

Email sent at step 4, step 5 fails.

**Fix:** Defer notifications until saga COMPLETED, or send corrective message.

### 14.6 War Story: Double Shipment (2023)

Async orchestrator retried `CreateShipment` after timeout. Provider lacked idempotency. Customer got two packages.

**Fix:** Idempotency key to provider; check `shipment_repository.findBySagaId()` before create; pivot point after label printed.

### 14.7 War Story: Compensation Storm (2024)

Identity service degraded. 10,000 sagas timed out simultaneously. Refund API rate-limited. 3,000 stuck in COMPENSATION_FAILED.

**Fix:** Jitter on timeout detection; compensation rate limiter; circuit breaker on refund API.

### 14.8 Common Failure Scenarios (from docs/saga.md)

| Scenario | Mitigation |
|----------|------------|
| Network timeout after remote commit | Idempotency key |
| Compensator fails | Retry + COMPENSATION_FAILED + ops alert |
| Out-of-order events | State transition validation |
| Lost observability | saga_step_log + correlation IDs |

## Section 15: Observability: Metrics, Logs, and Tracing

### 15.1 Required Metrics (from docs/saga.md)

```java
@Component
@RequiredArgsConstructor
public class SagaMetrics {
    private final MeterRegistry registry;

    public void recordStarted(String sagaType) {
        registry.counter("saga_started_total", "type", sagaType).increment();
    }
    public void recordCompleted(String sagaType) {
        registry.counter("saga_completed_total", "type", sagaType).increment();
    }
    public void recordCompensated(String sagaType) {
        registry.counter("saga_compensated_total", "type", sagaType).increment();
    }
    public void recordFailed(String sagaType, String reason) {
        registry.counter("saga_failed_total", "type", sagaType, "reason", reason).increment();
    }
    public void recordStepLatency(String sagaType, String stepId, long durationMs) {
        registry.timer("saga_step_latency_ms", "type", sagaType, "step", stepId)
            .record(Duration.ofMillis(durationMs));
    }
    public void recordRetry(String sagaType, String stepId, String reason) {
        registry.counter("saga_retry_total", "type", sagaType, "step", stepId, "reason", reason).increment();
    }
}
```

### 15.2 Extended Metrics

| Metric | Type | Purpose |
|--------|------|---------|
| `saga_stuck_total` | Gauge | Sagas in non-terminal state > threshold |
| `saga_compensation_failed_total` | Counter | Page immediately |
| `saga_dlq_messages_total` | Counter | DLQ buildup |
| `saga_duration_ms` | Timer | End-to-end latency |
| `saga_step_failure_total` | Counter | Per-step failure rates |

### 15.3 Structured Logging

```java
log.atInfo()
    .addKeyValue("sagaId", sagaId)
    .addKeyValue("stepId", stepId)
    .addKeyValue("status", status)
    .addKeyValue("durationMs", durationMs)
    .addKeyValue("traceId", traceId)
    .log("Saga step transition");
```

### 15.4 Prometheus Alert Rules

```yaml
groups:
  - name: saga-alerts
    rules:
      - alert: SagaCompensationFailed
        expr: increase(saga_compensation_failed_total[5m]) > 0
        labels:
          severity: critical
      - alert: SagaStuck
        expr: saga_stuck_total > 10
        for: 10m
        labels:
          severity: warning
      - alert: SagaFailureRateHigh
        expr: rate(saga_failed_total[5m]) / rate(saga_started_total[5m]) > 0.05
        for: 5m
        labels:
          severity: warning
```

---

## Section 16: Production Issue Runbook

### 16.1 Stuck Sagas in AWAITING_STEP

**Symptoms:** `saga_stuck_total` alert; orders not completing.

**Diagnosis:**

```sql
select saga_id, current_step, status, updated_at, retry_count
from saga_instance
where status = 'AWAITING_STEP'
  and updated_at < now() - interval '15 minutes'
order by updated_at limit 50;
```

**Resolution:**

| Root Cause | Fix |
|------------|-----|
| Service down | Restore service; recovery job republishes |
| Kafka outage | Restore Kafka; replay from AWAITING_STEP |
| Bug in participant | Fix + deploy; replay via admin API |
| Command lost | `POST /admin/sagas/{id}/republish-current-step` |

### 16.2 COMPENSATION_FAILED

**Symptoms:** Critical alert; customer charged but order incomplete.

**Steps:**

1. Query saga timeline from `saga_step_log`.
2. Check payment provider for refund status.
3. Retry refund with **same idempotency key**.
4. If provider shows refund succeeded but saga state wrong: mark COMPENSATED via admin API.
5. Escalate to finance if unrecoverable.

### 16.3 DLQ Backlog

1. Sample 5 DLQ messages — classify error type.
2. Fix root cause (schema, code, data).
3. Replay: `dlqReplayer.replay("saga.commands.inventory.dlq", maxMessages=100)`.
4. Monitor completion metrics after replay.

### 16.4 Admin API Endpoints

```
GET    /admin/sagas?status=AWAITING_STEP&olderThan=15m
GET    /admin/sagas/{sagaId}/timeline
POST   /admin/sagas/{sagaId}/republish-current-step
POST   /admin/sagas/{sagaId}/retry-compensation
POST   /admin/sagas/{sagaId}/mark-compensated
POST   /admin/dlq/replay?topic=saga.commands.inventory.dlq&limit=100
```

---

## Section 17: Decision Framework: When to Use and When Not To

### 17.1 Decision Tree

```
Coordinate state changes across multiple services?
├── No → Local transaction
└── Yes
    ├── Can you redesign to single service + single DB? → Do that
    └── No
        ├── Need immediate cross-service ACID? → Redesign boundaries (not saga)
        └── Eventual consistency acceptable?
            ├── Simple linear flow, event-native teams → Choreography
            └── Complex flow, need visibility → Orchestration
                ├── Short, user-facing → Sync
                └── Long-running, high retry → Async
```

### 17.2 When NOT To Use (from docs/saga.md)

| Condition | Alternative |
|-----------|-------------|
| Single database is enough | Local `@Transactional` |
| Business cannot tolerate eventual consistency | Redesign boundaries |
| Team lacks ops discipline for retries/DLQ | Fix maturity or stay monolith |
| Compensations impossible | Workflow with human steps |
| Strict financial atomicity | Single ledger service |

### 17.3 Saga vs Workflow Engine

| Factor | Custom Saga | Temporal / Camunda |
|--------|-------------|-------------------|
| Flow complexity | Linear / moderate | Complex, human tasks |
| Durability | You build it | Built-in |
| Cost | Lower infra, higher code | Higher infra, lower code |

**Rule:** Lightweight saga for 3–5 steps. Temporal when you need human approval, timers, or complex branching.

---

## Section 18: Lead Interview Questions — Logical and Production Scenarios

### Category A: Fundamentals

**Q1: What is the Saga pattern and why does it exist?**

A: A saga is a sequence of local transactions across multiple services, with compensating transactions to undo completed steps when a later step fails. It exists because microservices cannot use a single database transaction or practical 2PC across heterogeneous services. Each step commits locally; the saga restores business consistency semantically via compensations, accepting eventual consistency between steps.

**Q2: How is a saga different from 2PC?**

A: 2PC provides atomic commit/abort across participants using a prepare-commit protocol with blocking locks. Sagas give up cross-service atomicity for availability and partition tolerance. Instead of automatic rollback, you implement explicit compensating business actions. 2PC fails in cloud-native architectures where XA is unsupported; sagas work with any service that exposes an API or consumes events.

**Q3: What is the difference between a compensation and a database rollback?**

A: A database rollback undoes technical state within one transaction boundary using the DB's undo log. A compensation is a new business transaction that semantically reverses a prior step — e.g., issuing a refund instead of deleting a payment row. Compensations can take hours (bank refund), may partially fail, and must be idempotent. They operate across service boundaries where no shared undo log exists.

**Q4: In what order do compensations run?**

A: Reverse order of successful forward steps. If A, B, C succeed and D fails, compensate C, then B, then A. This mirrors the dependency chain — you undo the most recent effect first. Our repo demo `SagaDemo.java` and `SagaOrchestratorModesDemo.java` both demonstrate this reverse iteration.

**Q5: What consistency guarantee does a saga provide?**

A: Eventual consistency across the saga boundary. Each individual step is ACID within its service. Between steps, the system is in a known inconsistent intermediate state (e.g., inventory reserved but payment not captured). After successful completion or full compensation, business invariants are restored.

### Category B: Choreography vs Orchestration

**Q6: Compare choreography and orchestration. When do you pick each?**

A: Choreography: services react to events, no central coordinator. Good for 2-3 step flows with autonomous event-driven teams. Orchestration: central coordinator drives the sequence. Good for 4+ steps, complex branching, regulatory audit needs, and when you need one place to answer "where is saga X?" I default to orchestration for anything beyond trivial flows because observability and compensation control are worth the coupling cost.

**Q7: How do you prevent cyclic dependencies in choreography?**

A: Enforce strict event ownership — each event type has one publisher. Use event-carried state transfer so downstream services don't call upstream. Document the flow as a directed acyclic graph. If cycles appear, migrate to orchestration where the coordinator is the sole authority for step sequencing.

**Q8: What is the "god service" anti-pattern in orchestration?**

A: The orchestrator accumulates business logic — validation rules, pricing, inventory calculations — that belongs in domain services. The orchestrator should only know workflow: which step comes next, timeout policy, compensation trigger. Domain logic stays in participants. Review orchestrator PRs with extra scrutiny for business rule leakage.

**Q9: Can you combine choreography and orchestration?**

A: Yes, with clear boundaries. The orchestrator drives the top-level flow; individual steps may use internal choreography (e.g., inventory service emits events consumed by warehouse and analytics). Rule: one authority per saga for compensation triggers. Never have both orchestrator and a choreographed service independently triggering compensations for the same failure.

**Q10: How do you handle out-of-order events in choreography?**

A: Validate state transitions before applying events. If `PaymentCaptured` arrives when order status is not `INVENTORY_RESERVED`, reject to DLQ. Use partition key = aggregateId/sagaId for ordering within one saga. Persist event processing log with eventId deduplication.

### Category C: Sync vs Async Orchestration

**Q11: When do you use sync vs async orchestration?**

A: Sync: user-facing, low-latency, 2-4 reliable services, completes in seconds. Async: long-running flows, high retry reality, services with variable availability, 4+ steps. Hybrid is common — sync payment authorization, async fulfillment. See `docs/saga-orchestrator-sync-vs-async.md` and `SagaOrchestratorModesDemo.java`.

**Q12: What happens if a sync orchestrator call times out but the remote service committed?**

A: The orchestrator may retry or compensate incorrectly. This is the #1 sync saga bug. Fix: every participant must be idempotent using sagaId+stepId as idempotency key. On retry, participant returns cached success without re-executing. Never compensate until you've confirmed whether the step actually succeeded — use a reconciliation query or idempotent status check.

**Q13: How does async orchestration survive orchestrator restarts?**

A: Durable saga state in PostgreSQL. On restart, reload sagas in non-terminal states. For AWAITING_STEP: check step timeout, republish command if no reply received. For COMPENSATING: resume compensation chain from last logged step. Never rely on in-memory state — our demo's `Deque<Event>` is illustrative only; production uses DB + Kafka.

**Q14: How do you avoid the orchestrator becoming a bottleneck in async mode?**

A: Orchestrator is event-driven — it processes step results asynchronously, not one-thread-per-saga. Scale consumer instances horizontally. Partition Kafka topics by sagaId. Keep orchestrator logic thin (state transitions only). Heavy work stays in participants.

**Q15: Design a saga for a 3-day KYC onboarding flow.**

A: Async orchestration with durable state. Steps: collect documents → verify identity (external API, hours) → credit check → account activation. Each step has 24-72 hour timeout. User gets status via polling endpoint. Commands via Kafka; replies via saga.step-results. No sync HTTP blocking. Scheduled job detects stuck sagas. Human review step → REQUIRES_MANUAL_INTERVENTION state with admin API.

### Category D: Idempotency and Reliability

**Q16: Why is idempotency non-negotiable in sagas?**

A: Every network call, Kafka message, and retry can duplicate. Without idempotency: double charge, double shipment, double refund. Both forward steps AND compensations must tolerate duplicate invocation. Use sagaId+stepId as deterministic idempotency key stored durably per service.

**Q17: Design an idempotency scheme for payment capture and refund.**

A: Forward: idempotencyKey = `{sagaId}:CAPTURE_PAYMENT`. Store in local idempotency_record + pass to Stripe. On duplicate, return cached PaymentResult. Refund: idempotencyKey = `{sagaId}:REFUND_PAYMENT` (different from capture key). Check if refund already issued before calling provider. Never reuse capture key for refund.

**Q18: What is the difference between at-least-once delivery and saga idempotency?**

A: At-least-once means the message may arrive multiple times. Idempotency means processing it multiple times has the same effect as once. Kafka gives at-least-once (without exactly-once end-to-end). Saga participants must implement idempotency to safely handle duplicates. These are complementary, not alternatives.

**Q19: How do you handle the "ambiguous timeout" problem?**

A: Timeout means you don't know if the remote side committed. Never blindly retry forward or compensate. Steps: (1) Query participant status endpoint with sagaId. (2) If committed, mark step done and advance. (3) If not committed, safe to retry with same idempotency key. (4) If unknown, keep saga in AWAITING_STEP and alert. Payment providers often offer GET-by-idempotency-key APIs for this.

**Q20: What happens if compensation fails?**

A: Persist COMPENSATION_FAILED state. Retry compensation with backoff via dedicated job. Page on-call — customer may be charged without receiving goods. Manual repair via admin API with same idempotency key. Never auto-retry forward steps while compensation is pending. Document in runbook (Section 16).

### Category E: State, Outbox, and Kafka

**Q21: Why must saga state be persisted before publishing the next command?**

A: Write-ahead log pattern. If you publish then crash before persisting, the command is in flight but saga state doesn't know about it — orphan command. If you persist then crash before publish, recovery job republishes from AWAITING_STEP. Persist-then-publish guarantees recoverability.

**Q22: How does the Transactional Outbox pattern integrate with sagas?**

A: Each participant writes business data + outbox event in one local TX. Relay publishes to Kafka. Orchestrator consumes replies with eventId dedupe. Orchestrator itself uses outbox for commands. Eliminates dual-write problem. See `docs/outbox.md` Scenario 2 and Section 13 of this playbook.

**Q23: Why partition Kafka saga topics by sagaId?**

A: Ordering guarantee per saga. All commands and replies for one saga instance land in one partition, processed in order. Without this, step results may arrive out of order and corrupt state machine transitions.

**Q24: How do you version saga event schemas?**

A: Version in event type name: `InventoryReserved.v1`. Consumers tolerate unknown fields. Producers support dual-write during migration. Schema Registry with backward-compatible Avro/Protobuf. Orchestrator handles both versions during rollout window.

**Q25: Walk through the happy path of an async order saga with Outbox.**

A: (1) Client POST → orchestrator creates saga_instance + outbox(ReserveInventory). (2) Relay publishes to saga.commands.inventory. (3) inventory-service reserves stock + outbox(InventoryReserved) in one TX. (4) Relay publishes to saga.step-results. (5) Orchestrator dedupes eventId, marks step done, outbox(CapturePayment). (6) Repeat for payment and shipping. (7) Saga COMPLETED. Every cross-service message goes through outbox.

### Category F: Production and Operations

**Q26: What metrics do you emit for saga health?**

A: Minimum from docs/saga.md: saga_started_total, saga_completed_total, saga_compensated_total, saga_failed_total, saga_step_latency_ms (by step), saga_retry_total (by step/reason). Plus: saga_stuck_total, saga_compensation_failed_total (page on >0), saga_dlq_messages_total, end-to-end saga_duration_ms.

**Q27: A customer reports double charge. How do you investigate?**

A: (1) Get orderId/sagaId. (2) Query saga_instance and saga_step_log for timeline. (3) Check idempotency_record — was CAPTURE_PAYMENT executed twice? (4) Check payment provider transactions by idempotency key. (5) Likely causes: missing client Idempotency-Key on saga start (two saga instances), or missing step-level idempotency. Fix data, refund if needed, patch idempotency gap.

**Q28: 500 sagas stuck in AWAITING_STEP after a deployment. What do you do?**

A: (1) Check if deployment broke consumer ( deserialization error → DLQ filling). (2) Check downstream service health. (3) Roll back deployment if consumer broken. (4) Fix and redeploy. (5) Run recovery job to republish commands for stuck sagas. (6) Monitor saga_completed_total recovery rate. (7) Post-incident: add deployment smoke test that runs one test saga end-to-end.

**Q29: When would you NOT use a saga?**

A: Single DB suffices. Business cannot tolerate eventual consistency. Compensations are impossible (irreversible physical actions). Team cannot operate DLQ, stuck-saga monitoring, and manual repair. Strict financial atomicity needed — use single ledger service. Two services with low volume where simple sync calls + local TX each side may suffice without formal saga infrastructure.

**Q30: How does saga relate to Temporal or Camunda?**

A: Custom saga: you build state machine, durability, retries, and observability. Temporal/Camunda: workflow engine provides durability, timers, human tasks, and UI out of the box. Use custom saga for 3-5 step linear flows with Kafka-native teams. Use Temporal when flows have human approval, long timers, complex branching, or you want built-in visibility without building admin tools.

### Category G: Curveball Questions

**Q31: Can a saga step participate in a local @Transactional boundary?**

A: Yes — each step SHOULD be a local @Transactional operation within its service. The saga pattern operates above the service boundary. Inside inventory-service, reserve() is @Transactional. The saga coordinates multiple such local transactions.

**Q32: Is saga the same as Event Sourcing?**

A: No. Saga coordinates cross-service transactions. Event Sourcing persists state as an append-only event log within a service. They complement each other — saga steps can emit domain events, and event-sourced aggregates can be saga participants.

**Q33: How do you test saga compensation paths?**

A: (1) Unit test state machine transitions with mocked publishers. (2) Integration test with Testcontainers (PostgreSQL + Kafka): inject failure at each step, assert compensation order and final saga status. (3) Chaos test: kill orchestrator mid-saga, restart, assert recovery. (4) Contract test participant idempotency with duplicate commands. Run `SagaDemo.java` scenario 2 as the minimal unit test reference.

**Q34: Your architect wants to use Seata (DTM) for distributed transactions instead of saga. What do you say?**

A: Seata AT mode is essentially automated compensation with undo logs — closer to 2PC/branch transaction. It requires Seata agent on every service, creates proxy datasources, and adds operational complexity. For Java/Spring teams already on Kafka, explicit saga with outbox gives clearer business semantics, better observability of compensation logic, and no agent dependency. Seata may fit homogeneous Java shops with uniform DB support; saga fits polyglot cloud-native architectures.

**Q35: Design saga for hotel booking with external payment and internal inventory.**

A: Orchestration (async). Steps: HoldRoom(sagaId) → ChargeGuest(sagaId, idempotencyKey) → ConfirmBooking(sagaId). Compensation: ReleaseRoom → RefundGuest. HoldRoom timeout 15 min (room hold expires). ChargeGuest uses Stripe idempotency. ConfirmBooking is pivot — after confirm, only forward-fix (cancellation policy), no full compensation. Persist saga state in order-service. Events via outbox. Metrics per step. Admin API for stuck sagas.

---

## Section 19: Appendix: Quick Reference Cheat Sheet

### Saga Decision Tree

```
Multi-service state change needed?
├── No → Local @Transactional
└── Yes → Eventual consistency OK?
    ├── No → Redesign boundaries
    └── Yes → Compensations definable?
        ├── No → Workflow engine / human steps
        └── Yes → Saga
            ├── Simple, event-native → Choreography
            └── Complex → Orchestration
                ├── User waiting, <2s → Sync
                └── Background / long → Async
```

### Reliability Checklist

| Requirement | Implementation |
|-------------|----------------|
| Idempotency | sagaId+stepId key, idempotency_record table |
| Durable state | saga_instance + saga_step_log in PostgreSQL |
| Persist before publish | Write-ahead log in orchestrator |
| Outbox | Business data + event in one TX per participant |
| Timeouts | Per-step policy + scheduled stuck detector |
| DLQ | Kafka DeadLetterPublishingRecoverer |
| Dedupe | eventId tracking in orchestrator |
| Observability | 6 core metrics + structured logs + traceId |

### Key Terminology

| Term | Definition |
|------|------------|
| Forward step | Local TX advancing business state |
| Compensation | Semantic undo of a completed forward step |
| Orchestration | Central coordinator drives saga |
| Choreography | Services react to events, no coordinator |
| Pivot step | Point of no return — forward-fix only |
| sagaId | Correlation ID for entire saga instance |
| Idempotency key | Prevents duplicate side effects on retry |
| AWAITING_STEP | Async state: command sent, waiting for reply |
| COMPENSATION_FAILED | Critical state requiring manual intervention |

### Runnable Examples

```bash
# Basic saga: happy path, failure, idempotency
javac examples/saga/SagaDemo.java && java -cp examples/saga SagaDemo

# Sync vs async orchestration A→B→C→D
javac examples/saga/SagaOrchestratorModesDemo.java && \
  java -cp examples/saga SagaOrchestratorModesDemo
```

---

## Section 20: How to Talk About the Saga Pattern in an Interview

> Short and plain. How you would say it out loud.

---

### "What is the Saga pattern?"

In a monolith you can wrap everything in one database transaction. If something fails, it all rolls back together.

In microservices, each service has its own database. You can't do one big transaction across all of them.

The Saga pattern breaks a multi-step business operation into separate steps. Each step is its own local transaction. If step 3 fails, you run "undo" steps for step 2 and step 1.

These undo steps are not database rollbacks. They are real business actions. Like "cancel the reservation" or "issue a refund".

---

### "Why not just use two-phase commit?"

Two-phase commit tries to make everything commit or rollback together across services. The problem is it's slow and fragile. If one service is down, everything blocks. Most cloud databases don't even support it.

Sagas accept that things won't be perfectly in sync for a short time. But the system stays up and you handle failures with explicit undo steps.

---

### "Choreography vs Orchestration — what's the difference?"

Choreography means each service reacts to events on its own. Service A does something, sends an event, Service B picks it up and does its thing. No one is in charge. It's decoupled but hard to follow when something goes wrong.

Orchestration means one service is in charge. It calls each step one by one and decides what to do next. Easier to read and easier to debug. That's what I usually go with for anything complex.

---

### "Sync vs Async orchestration — which do you use?"

Sync is like making phone calls one by one. You call Service A, wait, then call B. Simple to follow. Easy to debug in one trace. Downside: if any step is slow, the whole thing waits.

Async is like sending messages and waiting for replies. You send a command, do other work, react when the reply comes. More resilient because services don't have to be available at the exact same moment. But you need durable saga state and good correlation IDs to debug it.

I use sync for short, simple flows where the user is waiting. Async for longer flows or when services can be slow or temporarily down.

---

### "What makes the undo steps tricky?"

The undo step has to be safe to run more than once. If the system retries it, you can't charge the customer twice or send two refunds.

Also, some things just can't be undone. If you already sent a notification email, that email is out. You can't take it back. That's something to think about when you design the flow. I usually push emails to the last step.

---

### "What is a compensating action?"

If the saga reaches step 3 and something fails, you need to undo step 1 and step 2.

The undo step is the compensating action. It's a real business action — not a database rollback.

Like if step 1 was "reserve inventory", the undo is "release the reservation".

The key rule is — the undo step must be safe to run more than once. The system might retry it.

---

### "How does idempotency fit in?"

Every step will get retried at some point. Network hiccup, timeout, Kafka delivering the same message twice.

So you give each step a unique key — usually saga ID plus step name. Before doing the work, you check if you already did it. If yes, skip or return the same result.

Same thing for undo steps. You can't refund twice.

---

### "What metrics do you watch in production?"

How many sagas started, completed, failed, and got compensated. Step latency per step. Retry counts. And the big one — how many sagas are stuck right now.

If compensation failed, that's a page immediately. Someone was probably charged and the order didn't go through. That needs a human.

---

### "When would you NOT use a saga?"

If one database can handle the whole transaction, just use that. Don't overcomplicate it.

If the business needs everything perfect instantly with no in-between state, saga isn't the right fit.

And if the team isn't ready to run DLQs, retry jobs, and stuck-saga monitoring — fix that first or stay in a monolith.

---

### Quick Answers

| Question | Say this |
|---|---|
| What is a Saga? | Multi-step business flow with explicit undo steps — no shared database transaction |
| Why not 2PC? | Slow, fragile, blocks when one service is down — doesn't work in the cloud |
| Choreography? | Services react to events — decoupled but hard to trace |
| Orchestration? | One service drives the flow — easier to understand and debug |
| Sync orchestration? | Call services one by one and wait — simple but tightly coupled |
| Async orchestration? | Send events, store state, react to replies — resilient but more complex |
| What are compensations? | The undo steps that reverse a previous action |
| Key rule? | Every forward and undo step must be safe to run more than once |
| What is sagaId? | The tracking ID that ties all steps together |
| When not to use? | Single DB works, or you need strict all-or-nothing consistency |
| What is Outbox? | Write business data and event in one DB transaction — then publish safely |
| Stuck saga? | Saga waiting too long for a reply — needs timeout job and alert |
| COMPENSATION_FAILED? | Undo failed after partial success — page someone immediately |

---

*End of Saga Pattern — Expert Playbook*
