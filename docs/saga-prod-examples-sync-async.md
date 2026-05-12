# Saga Pattern: Production-Style Examples (Non-Executable)

This document gives realistic reference implementations for:

1. Sync orchestration with HTTP/gRPC
2. Async orchestration with Kafka

Scenario:

- Services: `ServiceA`, `ServiceB`, `ServiceC`, `ServiceD`
- Forward: `A -> B -> C -> D`
- If `D` fails: compensate `C -> B -> A`

## Shared Design Contracts

### Correlation + Idempotency Headers

Every inter-service command must carry:

1. `X-Saga-Id`
2. `X-Step-Id`
3. `Idempotency-Key`
4. `Traceparent` (OpenTelemetry / W3C trace context)

### Saga State Model (Orchestrator DB)

`saga_instance`:

1. `saga_id`
2. `status` (`RUNNING`, `COMPLETED`, `COMPENSATING`, `FAILED`, `COMPENSATED`)
3. `current_step`
4. `last_error`
5. `updated_at`

`saga_step`:

1. `saga_id`
2. `step_name`
3. `status` (`PENDING`, `DONE`, `FAILED`, `COMPENSATED`)
4. `retry_count`
5. `updated_at`

---

## 1) Sync Orchestration (Prod-style, non-executable)

### Typical stack

1. Spring Boot 3
2. Spring Cloud OpenFeign or WebClient
3. Resilience4j (retry, timeout, circuit breaker)
4. PostgreSQL for saga state
5. OpenTelemetry + Micrometer

### Orchestrator flow

```java
@Transactional
public void executeOrderSaga(String sagaId, OrderCommand cmd) {
    sagaRepo.start(sagaId, "A");

    try {
        callA(sagaId, cmd);  sagaRepo.markDone(sagaId, "A");
        callB(sagaId, cmd);  sagaRepo.markDone(sagaId, "B");
        callC(sagaId, cmd);  sagaRepo.markDone(sagaId, "C");
        callD(sagaId, cmd);  sagaRepo.markDone(sagaId, "D");

        sagaRepo.complete(sagaId);
    } catch (Exception ex) {
        sagaRepo.failStep(sagaId, ex.getMessage());
        compensateReverse(sagaId, cmd);
        sagaRepo.markCompensated(sagaId);
    }
}
```

### HTTP client wrappers with production policies

```java
@Retry(name = "serviceA")
@CircuitBreaker(name = "serviceA")
@TimeLimiter(name = "serviceA")
public CompletionStage<Void> callA(String sagaId, OrderCommand cmd) {
    return serviceAClient.reserve(
        sagaId,
        "A",
        buildIdempotencyKey(sagaId, "A"),
        cmd
    );
}
```

Apply same shape for `callB`, `callC`, `callD`.

### Compensation sequence when D fails

```java
private void compensateReverse(String sagaId, OrderCommand cmd) {
    if (sagaRepo.isDone(sagaId, "C")) safeCompensateC(sagaId, cmd);
    if (sagaRepo.isDone(sagaId, "B")) safeCompensateB(sagaId, cmd);
    if (sagaRepo.isDone(sagaId, "A")) safeCompensateA(sagaId, cmd);
}
```

### Key production behaviors

1. Each `safeCompensateX` is idempotent and retried.
2. Timeout on downstream call does not imply downstream did nothing.
3. Step transition is persisted before moving to next step.
4. If compensation repeatedly fails, saga goes `FAILED` and requires operator runbook.

---

## 2) Async Orchestration with Kafka (Prod-style, non-executable)

### Typical stack

1. Spring Boot 3 + Spring for Apache Kafka
2. Kafka topics for commands + replies
3. Transactional Outbox for reliable command publishing
4. Schema Registry (Avro/Protobuf recommended)
5. DLQ + retry topics

### Topic model

Commands:

1. `saga.cmd.a`
2. `saga.cmd.b`
3. `saga.cmd.c`
4. `saga.cmd.d`
5. `saga.cmd.compensate.a`
6. `saga.cmd.compensate.b`
7. `saga.cmd.compensate.c`

Replies:

1. `saga.reply.a`
2. `saga.reply.b`
3. `saga.reply.c`
4. `saga.reply.d`
5. `saga.reply.compensate.a`
6. `saga.reply.compensate.b`
7. `saga.reply.compensate.c`

### Event contract (example)

```json
{
  "eventId": "uuid",
  "sagaId": "uuid",
  "step": "A|B|C|D|COMP_A|COMP_B|COMP_C",
  "status": "DONE|FAILED",
  "timestamp": "2026-05-12T12:00:00Z",
  "payload": {}
}
```

### Orchestrator state machine (reply-driven)

```java
@KafkaListener(topics = {"saga.reply.a", "saga.reply.b", "saga.reply.c", "saga.reply.d"})
public void onReply(SagaReply reply) {
    SagaState state = sagaRepo.load(reply.getSagaId());

    if (state.isTerminal()) return; // idempotent guard

    switch (reply.getStep()) {
        case "A":
            if (reply.isDone()) sendCommandB(state);
            else failSaga(state, "A failed");
            break;
        case "B":
            if (reply.isDone()) sendCommandC(state);
            else startCompensationFromB(state);
            break;
        case "C":
            if (reply.isDone()) sendCommandD(state);
            else startCompensationFromC(state);
            break;
        case "D":
            if (reply.isDone()) completeSaga(state);
            else startCompensationFromD(state); // C -> B -> A
            break;
        default:
            throw new IllegalStateException("Unknown step " + reply.getStep());
    }
}
```

### Compensation after D failure

1. Publish `COMPENSATE_C` command
2. On `COMP_C_DONE`, publish `COMPENSATE_B`
3. On `COMP_B_DONE`, publish `COMPENSATE_A`
4. On `COMP_A_DONE`, mark saga `COMPENSATED`

### Kafka reliability settings (baseline)

Producer:

1. `acks=all`
2. `enable.idempotence=true`
3. `max.in.flight.requests.per.connection=1` (if strict ordering is required per key)
4. `retries` high with bounded delivery timeout

Consumer:

1. Manual ack after state persistence
2. Dedupe by `eventId` (processed-events table)
3. Retry topic strategy + DLQ for poison messages

### Partitioning

Use `sagaId` as message key so all events of a saga keep relative ordering within a partition.

---

## Happy Path vs D Failure Summary

### Happy path

1. `A_DONE`
2. `B_DONE`
3. `C_DONE`
4. `D_DONE`
5. Saga status `COMPLETED`

### D failure path

1. `A_DONE`, `B_DONE`, `C_DONE`
2. `D_FAILED`
3. `COMP_C_DONE`
4. `COMP_B_DONE`
5. `COMP_A_DONE`
6. Saga status `COMPENSATED` (or `FAILED` if compensation cannot be finished)

---

## What Teams Commonly Miss

1. Compensation is business undo, not physical rollback.
2. Timeout ambiguity: downstream may have succeeded despite caller timeout.
3. Missing dedupe store causes double side effects under retries.
4. No operator runbook for stuck compensation.
5. No per-step SLA metrics means hidden backlog and latent incidents.
