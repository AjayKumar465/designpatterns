# Two-Phase Commit (2PC) — How It Works, Pros & Cons in Microservices

A practical reference for understanding distributed two-phase commit, why it dominated enterprise databases for decades, and why it is usually the wrong default for microservices — with when it still makes sense and what to use instead.

**Related docs:**

- [Saga Pattern](saga.md) — the common microservices alternative to 2PC
- [Saga Expert Playbook](saga-expert-playbook.md) — Section 2–3 cover 2PC vs Saga in depth
- [Transactional Outbox](outbox.md) — reliable single-service writes + events (not cross-service atomicity)

---

## What Is Two-Phase Commit?

**Two-Phase Commit (2PC)** is a distributed consensus protocol that makes multiple participants **agree to commit or abort together**. Either all participants persist their changes, or none do.

It is the standard way databases implement **distributed transactions** when more than one resource manager (database, message broker, etc.) must participate in one logical transaction.

In Java enterprise stacks you often see this as:

- **XA transactions** — the X/Open standard for distributed transaction processing
- **JTA (Java Transaction API)** — Java’s interface to XA coordinators
- **Atomikos, Narayana (JBoss), Bitronix** — transaction manager implementations

---

## The Players

| Role | Responsibility |
|------|----------------|
| **Transaction coordinator (TM)** | Runs the 2PC protocol; decides commit or abort |
| **Resource manager (RM)** | Each database, queue, or resource that joins the transaction |
| **Application** | Starts the global transaction, does work, asks TM to commit |

In a monolith with one app server and two XA-capable databases, the app server’s transaction manager is usually the coordinator.

In microservices, **each service owns its DB** — so a “global transaction” would require every service’s database (and often the network between them) to participate in one XA transaction. That is where the pain starts.

---

## How 2PC Works — Step by Step

### Normal happy path

```
Application                    Coordinator                 Participant A (DB)    Participant B (DB)
     |                              |                              |                    |
     |-- begin global TX ---------->|                              |                    |
     |-- SQL update ---------------->|-- Phase 1: PREPARE --------->|                    |
     |                              |<-- "YES, prepared" ----------|                    |
     |-- SQL update ---------------->|-- Phase 1: PREPARE ------------------------------>|
     |                              |<-- "YES, prepared" --------------------------------|
     |-- commit ------------------->|                              |                    |
     |                              |-- Phase 2: COMMIT ----------->|                    |
     |                              |<-- ACK ----------------------|                    |
     |                              |-- Phase 2: COMMIT -------------------------------->|
     |                              |<-- ACK ---------------------------------------------|
     |<-- success ------------------|                              |                    |
```

### Phase 1 — Prepare (vote)

1. Coordinator asks every participant: **“Can you commit?”**
2. Each participant:
   - Writes changes to a **durable prepare log** (not yet visible as committed to other transactions)
   - Acquires and **holds locks** until Phase 2 completes
   - Votes **YES (prepared)** or **NO (abort)**
3. If **any** participant votes NO or times out, coordinator goes to **abort**.

### Phase 2 — Commit or abort (decision)

1. If **all** voted YES → coordinator sends **COMMIT** to all
2. If **any** voted NO → coordinator sends **ROLLBACK** to all
3. Participants make the decision durable and release locks

**Key property:** Atomicity across participants — no partial commit in the success case.

### Failure path (one participant cannot prepare)

```
Coordinator sends PREPARE to A and B
A votes YES
B votes NO (disk full, constraint violation, timeout)

Coordinator sends ROLLBACK to A and B
A rolls back prepared work
B rolls back (or no-op)
Global transaction aborted — neither side committed
```

---

## What “Prepared” Means

During Phase 1, a participant that voted YES is in **prepared** (or **in-doubt**) state:

- Changes are forced to disk in a way that allows either commit or rollback later
- **Locks are still held** — other transactions may block waiting for those rows
- The participant **cannot decide on its own** — it must wait for the coordinator’s Phase 2 message

If the coordinator crashes after all participants prepared but before sending COMMIT, participants stay prepared and locked until:

- Coordinator recovers and completes Phase 2, or
- An operator runs **heuristic recovery** (manual commit/rollback — dangerous if wrong)

This **blocking** behavior is central to 2PC’s problems at scale.

---

## 2PC in Java — XA / JTA Sketch

```java
// Conceptual — requires XA datasources + JTA transaction manager
UserTransaction tx = (UserTransaction) new InitialContext().lookup("java:comp/UserTransaction");

try {
    tx.begin();

    // Both connections enlist in the same global XA transaction
    orderRepository.save(order);      // XA connection to orders DB
    inventoryRepository.reserve(sku); // XA connection to inventory DB

    tx.commit();   // Coordinator runs 2PC across both RMs
} catch (Exception e) {
    tx.rollback();
    throw e;
}
```

Production requirements for this to work:

1. Both databases support **XA** and are configured with XA drivers
2. A **transaction manager** (Spring `@Transactional` with JTA, Atomikos, etc.) coordinates both
3. Network and timeouts configured for prepare/commit phases
4. Both resources available for the **entire** duration of the global transaction

In microservices, `inventoryRepository` would live in another **process** — its database is not in your JVM’s XA enlistment unless you use a distributed transaction product (Seata, etc.) that proxies remote participation. That adds another layer of complexity.

---

## 2PC vs Local Transaction vs Saga

| | Local `@Transactional` | 2PC / XA | Saga |
|---|------------------------|----------|------|
| **Scope** | One service, one DB | Multiple RMs, often one app | Multiple services, each local TX |
| **Consistency** | Strong (ACID) | Strong (when it completes) | Eventual (semantic) |
| **Rollback** | Automatic | Automatic (abort) | Compensating actions |
| **Blocking** | Short | Yes — prepared locks | No cross-service locks |
| **Coordinator failure** | N/A | Participants can block | Saga state + retry |
| **Cloud / polyglot** | Universal | Poor | Good |
| **Typical microservices fit** | **Best default** | Rare | Common for cross-service flows |

---

## Pros of 2PC

### 1. Strong atomicity

All participants commit or all abort. No “order created but inventory not reserved” window in the success/failure model of the protocol itself.

### 2. Familiar mental model

Developers used to `@Transactional` get the same all-or-nothing semantics extended across resources — when it works, reasoning is simple.

### 3. Works in homogeneous enterprise setups

Same vendor stack (e.g., two Oracle DBs, WebLogic TM, corporate network) with ops teams who run XA recovery procedures — 2PC can be reliable at **moderate scale**.

### 4. No compensating logic required

Unlike sagas, you do not design `refundPayment()` or `releaseInventory()` — rollback is infrastructure-level.

### 5. Useful inside a single platform boundary

2PC **within one service** across two XA resources (DB + JMS in one monolith) was a legitimate pattern before outbox became popular.

---

## Cons of 2PC in Microservices

### 1. Blocking locks (availability killer)

Prepared participants hold locks until Phase 2. Slow or failed coordinator → locks held → other requests block → **cascading slowness** across services.

Microservices assume **partial failure** and **high concurrency**; long-held distributed locks fight that model.

### 2. Coordinator is a single point of failure

Coordinator crash in the “all prepared, not yet committed” window leaves **in-doubt transactions**. Recovery requires transaction manager tooling and sometimes DBA intervention.

### 3. Latency stacks across services

Each participant adds prepare + commit round-trips. A flow touching five services serially under 2PC pays **five prepare phases** before any commit — user-facing latency explodes.

### 4. Poor fit for cloud and managed services

Many cloud databases and SaaS APIs **do not support XA**:

- DynamoDB, Cosmos DB, Firestore
- Most third-party REST payment/shipping APIs
- Kafka (classic 2PC with DB is awkward; Kafka transactions are a different model)

You cannot 2PC your PostgreSQL order service with Stripe’s HTTP API.

### 5. Tight coupling at runtime

All participants must be **up and reachable** during the entire global transaction. One service restarting → whole transaction fails. Microservices want **independent deployability and failure domains**.

### 6. Heuristic decisions and data corruption risk

If automated recovery fails, ops may force commit or rollback on a prepared branch (**heuristic commit/rollback**). Wrong choice → **duplicate or lost** business effects.

### 7. Operational complexity

XA logs, timeout tuning, orphan transaction monitoring, version skew across TM and drivers — most platform teams **avoid** enabling XA in new systems.

### 8. Does not scale with organization structure

Each team owns a service and database. Global 2PC implies **shared fate** on every release and outage — opposite of bounded context autonomy.

---

## 2PC in Microservices — Summary Table

| Concern | 2PC impact |
|---------|------------|
| Availability | Low under partition or slow nodes |
| Latency | High — multiple synchronous phases |
| Scalability | Limited by lock duration and coordinator |
| Autonomy | Low — all services coupled in one TX window |
| Observability | Hard — in-doubt TX across teams |
| Cloud-native | Usually unsupported or discouraged |
| Team velocity | Cross-team coordination for TX boundaries |

---

## When 2PC Might Still Be Considered

| Scenario | Verdict |
|----------|---------|
| Single monolith, two XA databases, low QPS | Acceptable legacy; consider outbox for new work |
| Homogeneous Java cluster, Seata/Narayana AT mode | Possible; weigh ops cost vs explicit saga |
| Strong cross-DB atomicity in **one** deployable | Local 2PC or redesign to one DB |
| Cross-service e-commerce order flow | **Prefer saga** |
| Payment via external HTTP API | **2PC impossible** — saga or idempotent API |
| Read-heavy, write-rare financial ledger in one service | Local TX + careful modeling, not distributed 2PC |

---

## Modern Alternatives in Microservices

### 1. Local transaction only (best default)

Keep each business step atomic **inside one service**. If the whole flow fits one bounded context, **do not distribute**.

### 2. Saga + compensations

Multi-step flows across services with **semantic undo**. See [saga.md](saga.md).

### 3. Transactional outbox + events

Atomic **write + event** in one DB; **no** cross-service atomicity, but reliable propagation. See [outbox.md](outbox.md).

### 4. Idempotency + at-least-once

Accept retries; design every handler to be safe when run twice.

### 5. “Best effort” + reconciliation

Run steps; nightly jobs fix mismatches. Common in payments at scale when strict 2PC is impossible.

---

## 2PC vs Kafka Transactions (Do Not Confuse)

**Kafka transactions** (produce/consume atomically within Kafka) solve **broker-internal** consistency — not “order DB + payment DB + Kafka” in one XA transaction.

**Outbox pattern** is the usual way to atomically update a DB and eventually publish to Kafka without 2PC across systems.

---

## Decision Flowchart (Text)

```
Need to update multiple services' databases in one atomic step?
│
├─ Can the whole flow live in ONE service / ONE database?
│   └─ YES → Use local @Transactional. Stop here.
│
├─ Are all participants XA-capable AND under your ops control AND low volume?
│   └─ YES → 2PC *might* work (legacy enterprise). Document recovery runbooks.
│
└─ NO (typical microservices)
    └─ Use Saga (with compensations) OR redesign boundaries OR eventual consistency + outbox
```

---

## Metrics and Operations (If You Run XA)

If 2PC is in production, monitor:

1. **In-doubt transaction count** — should be zero sustained
2. **Prepare phase duration** — p99 per resource
3. **Rollback rate** — business vs infrastructure failures
4. **Lock wait time** on participant databases
5. **Coordinator recovery events**

Alert immediately on any in-doubt transaction older than a few minutes.

---

## How to Talk About 2PC in an Interview

> Plain English. Short sentences.

---

### "What is two-phase commit?"

It’s a way to make two or more databases all commit together or all roll back together.

Phase one — the coordinator asks everyone “can you commit?” and they lock their changes and say yes or no.

Phase two — if everyone said yes, the coordinator tells them all to commit. If anyone said no, everyone rolls back.

---

### "Why don’t microservices use 2PC?"

A few big reasons.

While waiting for phase two, databases hold locks. If something is slow or the coordinator dies, locks stick around and other requests pile up.

Also, in microservices each team owns their own database — often in the cloud without XA support. You can’t wrap a Postgres update and a call to Stripe’s API in one 2PC transaction anyway.

Microservices prefer each service to commit locally, then use patterns like saga or outbox to stay consistent over time.

---

### "When would you use 2PC?"

Honestly, rarely in new microservice designs.

Maybe in a legacy monolith with two Oracle databases and a team that already runs XA recovery. For new cross-service flows, I’d use saga with compensations or rethink service boundaries so one local transaction is enough.

---

### Quick Answers

| Question | Say this |
|---|---|
| What is 2PC? | Coordinator runs prepare then commit/abort so all participants agree |
| Phase 1? | Prepare — vote yes/no, hold locks |
| Phase 2? | Commit or rollback based on votes |
| Main microservices problem? | Blocking locks, coordinator failure, latency, cloud doesn’t support XA |
| Main benefit? | Strong atomicity — all commit or all abort |
| Better alternative? | Local TX per service; saga for multi-service; outbox for events |
| 2PC vs saga? | 2PC = automatic rollback, blocks; saga = compensations, eventual consistency |
| Kafka transactions same as 2PC? | No — Kafka TX is internal to Kafka; use outbox for DB + Kafka |

---

## Further Reading in This Repo

| Doc | Why |
|-----|-----|
| [saga-expert-playbook.md](saga-expert-playbook.md) | Sections 2–3: detailed 2PC vs Saga |
| [saga.md](saga.md) | Short saga reference |
| [outbox-expert-playbook.md](outbox-expert-playbook.md) | Section 15: outbox vs 2PC vs Kafka transactions |
| [circuit-breaker-expert-playbook.md](circuit-breaker-expert-playbook.md) | Protect calls when distributed TX is not an option |

---

*2PC teaches why “just make it one transaction” breaks across service boundaries — that motivates saga, outbox, and clear bounded contexts.*
