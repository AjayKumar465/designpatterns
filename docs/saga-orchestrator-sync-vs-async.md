# Advanced Saga Orchestrator: Sync vs Async (Service A/B/C/D)

## Scenario

We have one business flow across four services:

1. Service A
2. Service B
3. Service C
4. Service D

Forward sequence:

`A -> B -> C -> D`

Compensation sequence (if D fails after A/B/C succeeded):

`C_COMPENSATE -> B_COMPENSATE -> A_COMPENSATE`

## 1. Synchronous Orchestration

### How it works

- Orchestrator calls each service over request/response.
- It waits for each call result before moving to next step.
- On failure at step `D`, orchestrator immediately invokes compensations in reverse order.

### Happy path

1. `A.execute()` succeeds
2. `B.execute()` succeeds
3. `C.execute()` succeeds
4. `D.execute()` succeeds
5. Saga completes

### Failure path (D fails)

1. `A.execute()` succeeds
2. `B.execute()` succeeds
3. `C.execute()` succeeds
4. `D.execute()` fails
5. Orchestrator triggers:
- `C.compensate()`
- `B.compensate()`
- `A.compensate()`

### Strengths

1. Simple control flow
2. Easy to debug in one request trace
3. Fast decisioning for short workflows

### Weaknesses

1. Tight runtime coupling to all services
2. Increased latency across serial blocking calls
3. Harder to survive transient failures at high scale without robust retries/circuit breakers

## 2. Asynchronous Orchestration

### How it works

- Orchestrator emits command events, services reply with success/failure events.
- Orchestrator stores durable saga state and reacts to replies.
- It does not block waiting in one thread/request.

### Happy path

1. Orchestrator sends `DoA`
2. `A` emits `A_DONE`
3. Orchestrator sends `DoB`
4. `B` emits `B_DONE`
5. Orchestrator sends `DoC`
6. `C` emits `C_DONE`
7. Orchestrator sends `DoD`
8. `D` emits `D_DONE`
9. Saga completes

### Failure path (D fails)

1. `A_DONE`, `B_DONE`, `C_DONE` received
2. Orchestrator sends `DoD`
3. `D` emits `D_FAILED`
4. Orchestrator emits compensation commands:
- `UndoC`
- `UndoB`
- `UndoA`
5. Saga marked compensated/failed

### Strengths

1. Better resilience and decoupling
2. Better long-running saga support
3. Easier to absorb retries and temporary outages

### Weaknesses

1. More moving parts and state machine complexity
2. Requires idempotency and dedupe rigor
3. Debugging requires strong correlation IDs and event timelines

## 3. Production Rules You Must Enforce

1. Every command must carry `sagaId` + `stepId` + idempotency key.
2. Every step transition must be persisted durably before sending next command.
3. Compensations must be idempotent and retry-safe.
4. Timeouts must trigger retry or manual intervention state.
5. Dead-letter queues must be observed and drained with runbooks.
6. Add observability: step latency, retries, compensation counts, stuck saga count.

## 4. Practical Guidance

Use sync orchestration when:

1. Low latency path
2. Few services
3. Tight operational control

Use async orchestration when:

1. Long-running workflows
2. High failure/retry reality
3. Loose service availability requirements

In most real microservice systems, async orchestration becomes the safer default once workflow complexity grows.

## Runnable Demo

See:

- `examples/saga/SagaOrchestratorModesDemo.java`

It demonstrates both:

1. Sync happy path and D-failure rollback
2. Async happy path and D-failure rollback

---

## How to Talk About Saga Sync vs Async Orchestration in an Interview (Human English)

---

### "How do you decide between synchronous and asynchronous saga orchestration?"

> "The short answer is: sync for simple, low-latency flows. Async for long-running, failure-heavy, resilient flows. In sync orchestration, the orchestrator calls each service and waits — it's basically a blocking chain of HTTP calls. Simple to implement, easy to trace in one request span, good when services are fast and reliable. The problem is you're tightly coupled at runtime — if service B is slow, service A's thread is blocked waiting. Doesn't scale well with many steps or unreliable dependencies. In async orchestration, the orchestrator sends command events to Kafka and services reply with result events. The orchestrator stores its state durably and reacts to replies. No blocking threads. You can absorb retries, partial failures, and service restarts transparently. The tradeoff is complexity — you need state machines, durable saga state, idempotency everywhere, and strong correlation IDs to trace what happened."

---

### "What's a compensating transaction and when does it run?"

> "A compensating transaction is the 'undo' action for a step. If the saga reaches step D and D fails, the orchestrator runs compensations in reverse order: undo C, undo B, undo A. The key thing to understand is that compensations are NOT the same as a database rollback — they're new forward business actions that semantically reverse the previous action. 'Undo reserve inventory' means 'release the reservation'. 'Undo capture payment' means 'issue a refund'. And they must be idempotent — if the compensation is retried, running it twice should be safe."

---

### Quick Cheat Sheet

| Question | One-line answer |
|---|---|
| Sync orchestration? | Sequential HTTP calls — simple but tightly coupled at runtime |
| Async orchestration? | Command events + durable state — resilient but complex |
| When sync? | Short flows, fast services, low failure rate |
| When async? | Long-running, high failure rate, need retry/resume capability |
| What is compensation? | Business-level undo action — NOT a DB rollback, must be idempotent |
| Key requirement for async? | Durable saga state + idempotency + strong correlation IDs |

