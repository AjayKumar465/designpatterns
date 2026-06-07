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

## How to Talk About Saga Sync vs Async Orchestration in an Interview

> Short and plain. How you would say it.

---

### "Sync vs Async orchestration — which do you use and why?"

Sync orchestration is like making phone calls one by one. You call Service A, wait for the answer, then call Service B, wait, then C. Simple to follow. Easy to debug in one trace.

The downside is if any step is slow, the whole thing waits. And if a service is down when you call it, you have a problem right there.

Async orchestration is like sending messages and waiting for replies. You send a command, continue doing other things, and react when the reply comes. More resilient because services don't have to be available at the exact same moment.

The tradeoff is complexity. You need to store the saga state durably. You need to handle retries and duplicates. Debugging needs good correlation IDs so you can trace the full flow.

I use sync for short, simple flows where services are reliable. Async for longer flows or when services can be slow or temporarily down.

---

### "What is a compensating action?"

If the saga reaches step 3 and something fails, you need to undo step 1 and step 2.

The undo step is the compensating action. It's a real business action — not a database rollback.

Like if step 1 was "reserve inventory", the undo is "release the reservation".

The key rule is — the undo step must be safe to run more than once. The system might retry it, and you can't accidentally run it twice and cause a double-refund or something like that.

---

### Quick Answers

| Question | Say this |
|---|---|
| Sync orchestration? | Call services one by one and wait — simple but tightly coupled |
| Async orchestration? | Send events, store state, react to replies — resilient but more complex |
| When to use sync? | Short flows, fast services, few steps |
| When to use async? | Long flows, services can be slow or offline, high retry rate |
| What is compensation? | The undo step for a completed action — must be safe to repeat |

