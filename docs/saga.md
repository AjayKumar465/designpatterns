# Saga Pattern (Distributed Transaction Without 2PC)

## Problem

You need one business transaction to span multiple services, each with its own database.

Example order flow:

1. Reserve inventory
2. Capture payment
3. Create shipment

If step 3 fails after steps 1 and 2 already succeeded, the overall business action must be undone semantically, not via database rollback across services.

## Core Idea

A Saga breaks one long transaction into local transactions plus compensating actions.

- Forward step: move business state ahead
- Compensation step: undo business intent of a previous forward step

Compensations run in reverse order of successful forward steps.

## Styles

### Choreography

- Services react to events and emit new events
- No central coordinator
- Good for loose coupling
- Risk: flow logic becomes distributed and hard to debug

### Orchestration

- Central orchestrator drives the sequence
- Easier observability and control flow
- Risk: orchestrator can become a bottleneck or a "brain service"

## Reliability Rules (Non-Negotiable)

1. Idempotency for both forward and compensation actions
2. Durable saga state (step, status, retries, correlationId)
3. Timeout and retry policy per step
4. Dead-letter handling for poison events
5. Explicit compensation failure handling strategy

## Common Failure Scenarios

1. Network timeout after remote side committed
- Retry may duplicate effect unless idempotent key is used

2. Compensator fails
- Needs retry/backoff and operator visibility

3. Out-of-order events in choreography
- Must validate state transitions before applying events

4. Lost observability
- If you cannot reconstruct saga timeline, incident response will stall

## When Not To Use Saga

1. Single database transaction is enough
2. Business cannot tolerate eventual consistency
3. Team lacks operational discipline for retries, DLQ, and state repair

## Metrics You Must Emit

1. `saga_started_total`
2. `saga_completed_total`
3. `saga_compensated_total`
4. `saga_failed_total`
5. `saga_step_latency_ms` (tagged by step)
6. `saga_retry_total` (tagged by step and reason)

## Runnable Example

See `examples/saga/SagaDemo.java`.

It demonstrates:

1. Happy path (all steps succeed)
2. Failed path (shipment fails)
3. Reverse-order compensations
4. Idempotency guard for duplicate command execution

---

## How to Talk About the Saga Pattern in an Interview (Human English)

---

### "What is the Saga pattern?"

> "The problem it solves: in a microservices world, each service has its own database. You can't do a traditional 2PC (two-phase commit) across multiple databases — it's too slow, too coupled, and most databases don't even support it. But sometimes business operations span multiple services — place an order, which means reserving inventory, capturing payment, and creating a shipment. If payment succeeds but shipment fails, you need to undo the payment. How do you do that without a global transaction? A Saga breaks the business operation into a sequence of local transactions with compensating actions — explicit undo steps. If step 3 fails, you run the compensations for steps 2 and 1 in reverse. No 2PC. No distributed lock. Just choreographed or orchestrated local transactions plus compensating logic."

---

### "Choreography vs Orchestration — what's the difference?"

> "In choreography, services react to events and emit new events. Service A emits 'InventoryReserved', service B picks it up and charges payment, emits 'PaymentCaptured', service C picks that up and creates the shipment. No central brain — services know what to do when they see certain events. It's highly decoupled but debugging is painful — the flow is spread across services and you need strong correlation IDs and event timelines to understand what happened. In orchestration, there's a central orchestrator that says 'call A, then call B, then call C'. The flow is explicit and visible in one place. Debugging is easier. The risk is the orchestrator becomes a monolith brain. My default is orchestration for complex flows — it's much easier to reason about and observe."

---

### "What makes compensations tricky?"

> "Compensations must be idempotent — if the orchestrator retries 'undo payment', the payment service shouldn't charge the customer twice or throw an error because it was already undone. And there are failure scenarios compensations can't handle: a 'compensatable' action might have caused downstream side effects that can't be undone — like a notification already sent to the customer. That's called a 'pivot transaction' — once it happens, you can't roll back cleanly, only forward. That's why sagas work for eventually consistent systems, not for systems that need strict ACID semantics."

---

### Quick Cheat Sheet

| Question | One-line answer |
|---|---|
| What is a Saga? | A sequence of local transactions with compensating actions — no 2PC needed |
| Choreography? | Services react to events — decoupled but hard to debug |
| Orchestration? | Central orchestrator drives the flow — easier to observe and control |
| What are compensations? | Explicit undo actions for previously completed steps |
| Key requirement? | Idempotency — retried compensations must not cause double-effects |
| When not to use? | When strong ACID consistency is needed across services |

