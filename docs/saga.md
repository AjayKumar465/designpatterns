# Saga Pattern (Distributed Transaction Without 2PC)

**Expert playbook:** [`docs/saga-expert-playbook.md`](saga-expert-playbook.md) — full lead/architect depth (orchestration, idempotency, outbox, 35+ interview Q&A).

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

## How to Talk About the Saga Pattern in an Interview

> Short and plain. How you would say it out loud.

---

### "What is the Saga pattern?"

In a monolith you can wrap everything in one database transaction. If something fails, it all rolls back together.

In microservices, each service has its own database. You can't do one big transaction across all of them.

The Saga pattern breaks a multi-step business operation into separate steps. Each step is its own local transaction. If step 3 fails, you run "undo" steps for step 2 and step 1.

These undo steps are not database rollbacks. They are real business actions. Like "cancel the reservation" or "issue a refund".

---

### "Choreography vs Orchestration — what's the difference?"

Choreography means each service reacts to events on its own. Service A does something, sends an event, Service B picks it up and does its thing. No one is in charge. It's decoupled but hard to follow when something goes wrong.

Orchestration means one service is in charge. It calls each step one by one and decides what to do next. Easier to read and easier to debug. That's what I usually go with for anything complex.

---

### "What makes the undo steps tricky?"

The undo step has to be safe to run more than once. If the system retries it, you can't charge the customer twice or send two refunds.

Also, some things just can't be undone. If you already sent a notification email, that email is out. You can't take it back. That's something to think about when you design the flow.

---

### Quick Answers

| Question | Say this |
|---|---|
| What is a Saga? | Multi-step business flow with explicit undo steps — no shared database transaction |
| Choreography? | Services react to events — decoupled but hard to trace |
| Orchestration? | One service drives the flow — easier to understand and debug |
| What are compensations? | The undo steps that reverse a previous action |
| Key rule? | Every undo step must be safe to run more than once |
| When not to use? | When you need strict all-or-nothing consistency — sagas are eventually consistent |

