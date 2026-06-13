# PostgreSQL + JDBC/JPA/Hibernate — Expert Revision Playbook (Production, Interviews, Java/Spring)

> **Revision guide** — scan sections before interviews or on-call. Deep production depth with plain language. Targets Java Lead/Architect roles in microservices shops using Spring Boot, PostgreSQL, and Hibernate.

A comprehensive end-to-end reference covering PostgreSQL schema design for microservices, indexing and query tuning, transaction isolation, deadlocks, connection pooling, JPA/Hibernate production patterns, migrations, pagination, read replicas, idempotency and outbox tables, JSONB, audit columns, production runbooks, and 40+ lead-level interview Q&As. Sourced from PostgreSQL official docs, Hibernate reference, production war stories, r/java and r/PostgreSQL themes, and Spring Data JPA best practices.

**Related playbooks in this repo:**

- [Custom Spring Boot Starter Expert Playbook](custom-spring-boot-starter-expert-playbook.md) — auto-configure DataSource, Flyway, JPA in platform starters
- [Transactional Outbox Expert Playbook](outbox-expert-playbook.md) — outbox table schema, `FOR UPDATE SKIP LOCKED` relay, idempotency at consumers
- [CQRS Expert Playbook](cqrs-expert-playbook.md) — separate write/read stores, projectors, eventual consistency
- [Saga Expert Playbook](saga-expert-playbook.md) — local transactions per step, compensation, durable saga state in PostgreSQL
- [Metrics & Observability Playbook](metrics-observability-playbook.md) — HikariCP metrics, slow query logging, DB connection pool alerts

---

## Table of Contents

1. [PostgreSQL in Microservices — One Database Per Service](#section-1-postgresql-in-microservices--one-database-per-service)
2. [Schema Design — Types, Constraints, Naming, Soft Deletes](#section-2-schema-design--types-constraints-naming-soft-deletes)
3. [Indexing Deep Dive — B-Tree, Partial, Covering, GIN/GiST](#section-3-indexing-deep-dive--b-tree-partial-covering-gingist)
4. [Query Analysis — EXPLAIN, EXPLAIN ANALYZE, pg_stat_statements](#section-4-query-analysis--explain-explain-analyze-pg_stat_statements)
5. [Transactions — ACID, Isolation Levels, MVCC](#section-5-transactions--acid-isolation-levels-mvcc)
6. [Deadlocks, Lock Contention, and Advisory Locks](#section-6-deadlocks-lock-contention-and-advisory-locks)
7. [SELECT FOR UPDATE, SKIP LOCKED, and Outbox Relay](#section-7-select-for-update-skip-locked-and-outbox-relay)
8. [Flyway and Liquibase — Migrations in Production](#section-8-flyway-and-liquibase--migrations-in-production)
9. [HikariCP — Pool Sizing, Leaks, and JDBC Tuning](#section-9-hikaricp--pool-sizing-leaks-and-jdbc-tuning)
10. [JPA/Hibernate Fundamentals — EntityManager, Session, Flush](#section-10-jpahibernate-fundamentals--entitymanager-session-flush)
11. [N+1 Problem — Deep Dive and All Fixes](#section-11-n1-problem--deep-dive-and-all-fixes)
12. [Pagination — Offset vs Keyset/Cursor](#section-12-pagination--offset-vs-keysetcursor)
13. [Read Replicas — Routing, Lag, and Consistency](#section-13-read-replicas--routing-lag-and-consistency)
14. [Idempotency Tables and Outbox Schema](#section-14-idempotency-tables-and-outbox-schema)
15. [JSONB Columns — When, How, Indexing](#section-15-jsonb-columns--when-how-indexing)
16. [Audit Columns — created_at, updated_at, versioning](#section-16-audit-columns--created_at-updated_at-versioning)
17. [Spring Data JPA — Repositories, Projections, Specifications](#section-17-spring-data-jpa--repositories-projections-specifications)
18. [Hibernate Performance — Batch Writes, Statelessness, Caching](#section-18-hibernate-performance--batch-writes-statelessness-caching)
19. [Connection and Transaction Management in Spring](#section-19-connection-and-transaction-management-in-spring)
20. [Production Runbook — Slow Queries, Locks, Pool Exhaustion, Replication Lag](#section-20-production-runbook--slow-queries-locks-pool-exhaustion-replication-lag)
21. [Revision Cheat Sheets — SQL, Hibernate, Spring Config](#section-21-revision-cheat-sheets--sql-hibernate-spring-config)
22. [Lead Interview Questions — Logical and Production Scenarios](#section-22-lead-interview-questions--logical-and-production-scenarios)
23. [How to Talk About PostgreSQL and JPA in an Interview](#section-23-how-to-talk-about-postgresql-and-jpa-in-an-interview)
24. [Appendix — Decision Trees and Quick Reference](#section-24-appendix--decision-trees-and-quick-reference)

---


## Section 1: PostgreSQL in Microservices — One Database Per Service

### 1.1 The Core Principle

In microservices, **each service owns its data**. That ownership is enforced at the database boundary:

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│ order-svc   │     │ payment-svc │     │ inventory   │
│  order_db   │     │ payment_db  │     │ inventory_db│
└─────────────┘     └─────────────┘     └─────────────┘
       │                   │                   │
       └───────────────────┴───────────────────┘
                    Events (Kafka)
              No cross-DB foreign keys
```

**Rules:**

| Rule | Why |
|------|-----|
| One PostgreSQL database (or cluster) per service | Independent deploy, scale, backup, failure isolation |
| No shared tables across services | Shared schema = shared fate; coupling at DB layer |
| No cross-service foreign keys | FK implies synchronous coupling; breaks autonomy |
| Reference other services by ID only | `customer_id` is a value, not a FK to `customers` table in another DB |
| Communicate via APIs or events | Saga, outbox, CQRS — see [saga-expert-playbook.md](saga-expert-playbook.md), [outbox-expert-playbook.md](outbox-expert-playbook.md) |

### 1.2 Shared Database Anti-Pattern

```
❌ WRONG — "modular monolith database"

  orders table          payments table          inventory table
  ─────────────────────────────────────────────────────────────
  All in one PostgreSQL instance, accessed by three services

  Problems:
  - Schema changes require coordinated releases
  - One slow query affects all services
  - No clear ownership — "who owns the payments migration?"
  - Cannot scale read/write per domain
```

**Interview one-liner:** "Shared database is the fastest way to build a distributed monolith with network partitions."

### 1.3 Database Per Service — Practical Variants

| Variant | Description | When to use |
|---------|-------------|-------------|
| **Dedicated DB instance** | Separate RDS/Cloud SQL per service | Strong isolation, regulated workloads |
| **Shared cluster, separate database** | One PostgreSQL cluster, `order_db`, `payment_db` schemas isolated | Cost-conscious, small teams |
| **Shared cluster, separate schema** | `order_service.orders`, `payment_service.payments` | Acceptable for small orgs; weaker blast radius |
| **CQRS read DB** | Write DB per service + separate read-optimized store | High read load, search, dashboards — [cqrs-expert-playbook.md](cqrs-expert-playbook.md) |

**Production default for Java shops:** Shared managed PostgreSQL cluster with **one database per service** + connection limits per DB user.

### 1.4 Cross-Service Data — Patterns

When `order-service` needs customer name:

| Approach | Consistency | Complexity |
|----------|-------------|------------|
| **API call** at read time | Strong if customer-svc available | Simple; adds latency and coupling |
| **Local cache/replica** (event-driven copy) | Eventual | CQRS projector copies `customer_name` into `orders` read model |
| **Saga correlation ID** | Per saga instance | Store only IDs in write model; enrich at query time |

Never: `JOIN customers ON orders.customer_id` across databases.

### 1.5 Schema Ownership and Migration Discipline

Each service:

1. Owns its Flyway/Liquibase scripts in its repo (Section 8).
2. Runs migrations on deploy (init container or app startup with care).
3. Never runs DDL against another service's database.
4. Publishes **events** when data other services need changes — not direct DB writes.

### 1.6 Connection Identity per Service

```yaml
# order-service application.yml
spring:
  datasource:
    url: jdbc:postgresql://pg-cluster:5432/order_db
    username: order_svc_app        # NOT postgres superuser
    hikari:
      maximum-pool-size: 20
      pool-name: order-svc-pool
```

Grant only required privileges:

```sql
GRANT CONNECT ON DATABASE order_db TO order_svc_app;
GRANT USAGE ON SCHEMA public TO order_svc_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO order_svc_app;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
  GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO order_svc_app;
```

### 1.7 When One DB Per Service Is Too Heavy

For **very small teams** or **early MVP**:

- Start with modular monolith + single DB if team < 5 and no scale pressure.
- Split databases when: independent deploy cadence needed, different scaling profiles, or organizational boundaries align.

**Lead answer:** "We chose DB-per-service when order and payment teams needed independent release trains and order DB was 80% of IOPS."

---

## Section 2: Schema Design — Types, Constraints, Naming, Soft Deletes

### 2.1 Primary Keys — BIGINT vs UUID

| Type | Pros | Cons |
|------|------|------|
| **BIGSERIAL / BIGINT** | Compact (8 bytes), fast B-tree inserts, human-debuggable | Predictable; needs sequence coordination if merging DBs |
| **UUID (gen_random_uuid())** | Globally unique, safe for distributed ID generation, no sequence hotspot | 16 bytes, random UUIDs fragment B-tree index (use `uuid` type, not VARCHAR) |
| **UUID v7 (PG 18+ / app-generated)** | Time-ordered UUID — better index locality | Requires library or PG 18 |

**Production default for Spring/JPA:**

```java
@Entity
public class Order {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)  // BIGSERIAL
    private Long id;
}
```

Use UUID when: IDs exposed externally, merging shards, or generating IDs before insert (outbox correlation).

```java
@Id
@Column(columnDefinition = "uuid")
@GeneratedValue
private UUID id;  // Hibernate 6 + @JdbcTypeCode(SqlTypes.UUID)
```

### 2.2 Timestamps — Always WITH TIME ZONE

```sql
CREATE TABLE orders (
    id          BIGSERIAL PRIMARY KEY,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

**Never** use `TIMESTAMP WITHOUT TIME ZONE` for audit columns — ambiguous across regions and DST.

JPA mapping:

```java
@Column(nullable = false, updatable = false)
private Instant createdAt;

@Column(nullable = false)
private Instant updatedAt;
```

Use `Instant` in Java, not `LocalDateTime`, for server-side timestamps.

### 2.3 Money and Decimals

```sql
amount NUMERIC(19, 4) NOT NULL  -- exact decimal, no float rounding
currency CHAR(3) NOT NULL DEFAULT 'USD'
```

**Never** use `float`/`double` or PostgreSQL `REAL` for money. JPA: `BigDecimal` with `@Column(precision = 19, scale = 4)`.

### 2.4 Enums — PostgreSQL ENUM vs VARCHAR vs Java Enum

| Approach | When |
|----------|------|
| PostgreSQL `ENUM` type | Stable, small set; migration needed to add values |
| `VARCHAR` + check constraint | Flexible; app validates |
| Java `@Enumerated(STRING)` | Default Spring choice; VARCHAR column |

```sql
CREATE TYPE order_status AS ENUM ('PLACED', 'CONFIRMED', 'CANCELLED');
-- Adding value: ALTER TYPE order_status ADD VALUE 'SHIPPED';
```

### 2.5 Constraints — Enforce at DB Layer

```sql
CREATE TABLE orders (
    id           BIGSERIAL PRIMARY KEY,
    customer_id  BIGINT NOT NULL,
    status       VARCHAR(32) NOT NULL,
    total_amount NUMERIC(19,4) NOT NULL CHECK (total_amount >= 0),
    CONSTRAINT orders_status_check
        CHECK (status IN ('PLACED','CONFIRMED','CANCELLED','SHIPPED'))
);

CREATE UNIQUE INDEX orders_idempotency_key_uq
    ON orders (idempotency_key)
    WHERE idempotency_key IS NOT NULL;
```

**Application validation is not enough** — concurrent requests and bugs bypass app layer.

### 2.6 Soft Deletes

```sql
deleted_at TIMESTAMPTZ NULL  -- NULL = active, non-null = soft deleted
```

```java
@SQLRestriction("deleted_at IS NULL")  // Hibernate 6
@Entity
public class Customer { ... }
```

**Index implication:** Partial index for active rows only:

```sql
CREATE INDEX customers_email_active_idx
    ON customers (email)
    WHERE deleted_at IS NULL;
```

### 2.7 Naming Conventions

| Element | Convention | Example |
|---------|------------|---------|
| Tables | snake_case, plural | `order_items` |
| Columns | snake_case | `created_at` |
| PK | `id` or `{table}_id` | `id` |
| FK (within service) | `{referenced}_id` | `order_id` |
| Indexes | `{table}_{cols}_{type}_idx` | `orders_customer_id_status_idx` |
| JPA | `@Table(name = "orders")` explicit when camelCase entity |

Spring Boot default: `SpringPhysicalNamingStrategy` maps `OrderItem` → `order_item` (singular). Set explicitly if you prefer plural tables.

### 2.8 Normalization vs Denormalization Within a Service

- **Write model:** normalized (3NF) for integrity — orders + order_items separate tables.
- **Read model within same DB:** denormalized views or materialized views for dashboards.
- **Cross-service denormalization:** via CQRS projector, not JOIN across DBs.

---

## Section 3: Indexing Deep Dive — B-Tree, Partial, Covering, GIN/GiST

### 3.1 B-Tree — Default and Workhorse

PostgreSQL default index type. Supports: `=`, `<`, `>`, `<=`, `>=`, `BETWEEN`, `IN`, `IS NULL`, `IS NOT NULL`.

```sql
CREATE INDEX orders_customer_id_idx ON orders (customer_id);
```

**Leftmost prefix rule:** Index on `(customer_id, status)` helps:

- `WHERE customer_id = ?`
- `WHERE customer_id = ? AND status = ?`

Does **not** help: `WHERE status = ?` alone (unless partial index).

### 3.2 Composite Index Column Order

Order columns by:

1. **Equality filters first** (`customer_id = ?`)
2. **Range filters next** (`created_at > ?`)
3. **ORDER BY / GROUP BY** columns

```sql
-- Good for: customer orders by date
CREATE INDEX orders_customer_created_idx
    ON orders (customer_id, created_at DESC);
```

### 3.3 Partial Indexes — Smaller, Faster

Index only rows that queries need:

```sql
-- Only open orders — most queries filter status = 'PLACED' or 'CONFIRMED'
CREATE INDEX orders_open_customer_idx
    ON orders (customer_id, created_at DESC)
    WHERE status IN ('PLACED', 'CONFIRMED');

-- Active users only
CREATE INDEX users_email_active_idx
    ON users (email)
    WHERE deleted_at IS NULL;

-- Outbox pending events — critical for relay performance
CREATE INDEX outbox_pending_idx
    ON outbox_events (created_at)
    WHERE status = 'PENDING';
```

**Benefits:** Smaller index, faster scans, less write amplification. **Requirement:** Query `WHERE` must match index predicate (or be stricter).

### 3.4 Covering Indexes (Index-Only Scans)

Include columns in index so PostgreSQL avoids heap fetch:

```sql
CREATE INDEX orders_customer_status_covering_idx
    ON orders (customer_id)
    INCLUDE (status, total_amount, created_at);
```

Query:

```sql
SELECT status, total_amount, created_at
FROM orders
WHERE customer_id = 12345;
```

`EXPLAIN` shows `Index Only Scan` if visibility map allows (VACUUM keeps this healthy).

**JPA implication:** Projections that only need indexed columns are dramatically faster.

### 3.5 Unique Indexes vs Unique Constraints

```sql
CREATE UNIQUE INDEX orders_idempotency_key_uq
    ON orders (idempotency_key)
    WHERE idempotency_key IS NOT NULL;
```

Partial unique index allows multiple NULL idempotency keys while enforcing uniqueness when present.

### 3.6 GIN Indexes for JSONB and Full-Text

```sql
-- JSONB containment queries
CREATE INDEX products_metadata_gin_idx
    ON products USING GIN (metadata jsonb_path_ops);

-- Query: WHERE metadata @> '{"brand": "Nike"}'

-- Full-text search
CREATE INDEX articles_search_idx
    ON articles USING GIN (to_tsvector('english', title || ' ' || body));
```

### 3.7 Index Anti-Patterns

| Anti-pattern | Problem |
|--------------|---------|
| Index every column | Write slowdown, wasted storage |
| `%column%` LIKE with B-tree | Cannot use B-tree; use `pg_trgm` GIN or full-text |
| Function on column without expression index | `WHERE lower(email) = ?` needs `INDEX ON lower(email)` |
| Unused indexes | Check `pg_stat_user_indexes.idx_scan = 0` over weeks |
| Duplicate/redundant indexes | `(a)` redundant if `(a, b)` exists for equality on `a` |

### 3.8 Monitoring Index Health

```sql
SELECT schemaname, relname, indexrelname, idx_scan, idx_tup_read, idx_tup_fetch
FROM pg_stat_user_indexes
ORDER BY idx_scan ASC;

-- Bloated indexes
SELECT * FROM pgstatindex('orders_customer_id_idx');
```

Remove indexes with `idx_scan = 0` over 30+ days (verify no rare critical query).

---


## Section 4: Query Analysis — EXPLAIN, EXPLAIN ANALYZE, pg_stat_statements

### 4.1 EXPLAIN — Reading the Plan

```sql
EXPLAIN (FORMAT TEXT)
SELECT o.id, o.status, o.total_amount
FROM orders o
WHERE o.customer_id = 12345
  AND o.status = 'PLACED'
ORDER BY o.created_at DESC
LIMIT 20;
```

**Key nodes:**

| Node | Meaning |
|------|---------|
| **Seq Scan** | Full table scan — bad on large tables unless most rows match |
| **Index Scan** | B-tree lookup + heap fetch per row |
| **Index Only Scan** | Index covers query — best case |
| **Bitmap Index Scan** | Multiple index conditions combined |
| **Nested Loop** | For each row in A, scan B — OK for small inner |
| **Hash Join** | Build hash table on one side — good for equality joins |
| **Merge Join** | Both sides sorted — good for large sorted inputs |

### 4.2 EXPLAIN ANALYZE — Real Execution

```sql
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT ...
```

**Adds:** actual row counts, actual time, buffer hits/reads.

**Critical skill:** Compare **estimated rows** vs **actual rows**. Large mismatch → stale statistics → `ANALYZE table_name`.

```sql
ANALYZE orders;
-- Or increase statistics target for skewed column:
ALTER TABLE orders SET (autovacuum_analyze_scale_factor = 0.02);
```

### 4.3 Red Flags in EXPLAIN Output

```
Seq Scan on orders  (cost=0.00..500000.00 rows=1000000 width=64)
  Filter: (status = 'PLACED')
  Rows Removed by Filter: 950000
```

→ Need index on `status` or partial index on `status = 'PLACED'`.

```
Nested Loop  (actual time=0.05..5000.00 rows=10000 loops=1)
  -> Index Scan on orders ...
  -> Seq Scan on order_items  (actual time=0.01..0.50 rows=5 loops=10000)
```

→ N+1 at SQL level or missing FK index on `order_items.order_id`.

### 4.4 pg_stat_statements — Production Slow Query Discovery

```sql
-- Enable in postgresql.conf:
-- shared_preload_libraries = 'pg_stat_statements'
-- pg_stat_statements.track = all

SELECT
    calls,
    round(total_exec_time::numeric, 2) AS total_ms,
    round(mean_exec_time::numeric, 2) AS mean_ms,
    round((100 * total_exec_time / sum(total_exec_time) OVER ())::numeric, 2) AS pct_total,
    rows,
    query
FROM pg_stat_statements
ORDER BY total_exec_time DESC
LIMIT 20;
```

**Use for:** Finding queries that consume disproportionate DB time — not just slowest single execution.

### 4.5 log_min_duration_statement

```yaml
# For targeted slow query logging (avoid logging everything in prod)
spring.jpa.properties.hibernate.session.events.log.LOG_QUERIES_SLOWER_THAN_MS: 500
```

PostgreSQL side:

```
log_min_duration_statement = 500ms
log_line_prefix = '%t [%p] %u %d '
```

Correlate with application trace ID via JDBC `application_name` or OpenTelemetry.

### 4.6 Query Tuning Workflow (On-Call)

```
1. Identify slow query (APM, pg_stat_statements, logs)
2. EXPLAIN (ANALYZE, BUFFERS) on production-like data volume
3. Check: wrong plan? missing index? stale stats? lock wait?
4. Fix: index, rewrite query, increase stats, partition, cache
5. Verify: EXPLAIN again + deploy + monitor p95 latency
6. Document in runbook if recurring pattern
```

---

## Section 5: Transactions — ACID, Isolation Levels, MVCC

### 5.1 ACID in PostgreSQL

| Property | PostgreSQL mechanism |
|----------|---------------------|
| **Atomicity** | Single transaction commit/rollback |
| **Consistency** | Constraints checked at statement or transaction end |
| **Isolation** | MVCC + optional stronger locks |
| **Durability** | WAL flushed to disk (`synchronous_commit`) |

### 5.2 MVCC — How PostgreSQL Sees Rows

Each row has `xmin` (creating transaction) and `xmax` (deleting/updating transaction).

- Readers don't block writers; writers don't block readers.
- **Old row versions** remain until VACUUM cleans dead tuples.
- Long transactions → bloat → performance degradation.

```sql
SELECT pid, state, xact_start, query
FROM pg_stat_activity
WHERE state != 'idle'
ORDER BY xact_start;
```

**Kill long idle-in-transaction sessions** — they hold back VACUUM and bloat tables.

### 5.3 Isolation Levels

PostgreSQL supports:

| Level | Dirty read | Non-repeatable read | Phantom read |
|-------|------------|---------------------|--------------|
| Read Uncommitted | — (PG treats as Read Committed) | | |
| **Read Committed** (default) | No | Yes | Yes |
| **Repeatable Read** | No | No | No (in PG) |
| **Serializable** | No | No | No |

**Default Read Committed:** Each statement sees a snapshot at statement start.

```sql
BEGIN;
SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
SELECT balance FROM accounts WHERE id = 1;  -- sees 100
-- another transaction commits balance = 50
SELECT balance FROM accounts WHERE id = 1;  -- sees 50 (non-repeatable)
COMMIT;
```

### 5.4 Repeatable Read in Spring

```java
@Transactional(isolation = Isolation.REPEATABLE_READ)
public void transfer(Long from, Long to, BigDecimal amount) {
    Account a = accountRepo.findById(from).orElseThrow();
    Account b = accountRepo.findById(to).orElseThrow();
    // Both reads see same snapshot for entire transaction
    a.debit(amount);
    b.credit(amount);
}
```

PostgreSQL Repeatable Read prevents phantom reads for most cases via MVCC snapshot isolation.

**Serialization failure:** Concurrent updates may throw:

```
ERROR: could not serialize access due to concurrent update
```

Spring: `OptimisticLockException` or `PessimisticLockException` depending on strategy.

### 5.5 Serializable — Strongest, Highest Contention

Use when: financial ledger invariants that must hold under all concurrency patterns.

Expect more retries. Application must retry on `SQLSTATE 40001`.

### 5.6 Spring @Transactional Defaults

```java
@Transactional  // propagation=REQUIRED, isolation=DEFAULT (Read Committed), readOnly=false
```

| Setting | Production note |
|---------|-----------------|
| `readOnly=true` | Hibernate skips dirty checking; PG may optimize; use on queries |
| `propagation=REQUIRED` | Joins existing transaction or creates new |
| `REQUIRES_NEW` | Suspends current, new transaction — use carefully (audit log) |
| `rollbackFor` | Default: RuntimeException and Error only — **checked exceptions don't rollback** |

```java
@Transactional(rollbackFor = Exception.class)  // if you need all exceptions
```

### 5.7 Transaction Boundaries — Service Layer Rule

```
Controller → Service (@Transactional) → Repository
```

**Never** `@Transactional` on controllers. **Avoid** `@Transactional` on repositories (Spring Data handles read operations).

**Keep transactions short:**

- No HTTP calls inside `@Transactional`
- No Kafka publish inside `@Transactional` without outbox
- Load data, mutate, save, commit — then publish event via outbox in same transaction

See [outbox-expert-playbook.md](outbox-expert-playbook.md).

### 5.8 Auto-Commit and JDBC

HikariCP connections default to auto-commit true outside Spring transaction.

Inside `@Transactional`, Spring sets `autoCommit=false` and manages commit/rollback.

**Leak symptom:** Connection returned with autoCommit false or uncommitted transaction — see Section 9.

---

## Section 6: Deadlocks, Lock Contention, and Advisory Locks

### 6.1 Row-Level Locks in PostgreSQL

| Lock | SQL | Use |
|------|-----|-----|
| `FOR UPDATE` | `SELECT ... FOR UPDATE` | Pessimistic write lock |
| `FOR NO KEY UPDATE` | weaker row lock | FK checks |
| `FOR SHARE` | shared lock | Prevent writes |
| `FOR KEY SHARE` | weakest | FK on referenced row |

### 6.2 Deadlock Scenario

```
Transaction A: UPDATE orders WHERE id = 1  (locks row 1)
Transaction B: UPDATE orders WHERE id = 2  (locks row 2)
Transaction A: UPDATE orders WHERE id = 2  (waits for B)
Transaction B: UPDATE orders WHERE id = 1  (waits for A)
→ DEADLOCK detected
```

PostgreSQL picks **victim** and aborts one transaction:

```
ERROR: deadlock detected
DETAIL: Process 12345 waits for ShareLock on transaction 67890...
```

### 6.3 Deadlock Prevention

1. **Consistent lock ordering** — always lock rows in same order (e.g., by `id ASC`).
2. **Smaller transactions** — less overlap window.
3. **Optimistic locking** — `@Version` column, no row lock until update.
4. **Retry on deadlock** — Spring `@Retryable` on `CannotAcquireLockException`.

```java
@Entity
public class Order {
    @Version
    private Long version;
}
```

```java
@Retryable(retryFor = CannotAcquireLockException.class, maxAttempts = 3)
@Transactional
public void updateOrder(Long id, OrderUpdate cmd) { ... }
```

### 6.4 Detecting Lock Contention

```sql
SELECT
    blocked.pid AS blocked_pid,
    blocked.query AS blocked_query,
    blocking.pid AS blocking_pid,
    blocking.query AS blocking_query,
    now() - blocked.query_start AS blocked_duration
FROM pg_stat_activity blocked
JOIN pg_stat_activity blocking
    ON blocking.pid = ANY(pg_blocking_pids(blocked.pid))
WHERE blocked.wait_event_type = 'Lock';
```

```sql
-- pg_locks detail
SELECT l.locktype, l.mode, l.granted, a.query, a.pid
FROM pg_locks l
JOIN pg_stat_activity a ON l.pid = a.pid
WHERE NOT l.granted;
```

### 6.5 Advisory Locks

Application-level locks without row data:

```sql
SELECT pg_advisory_xact_lock(12345);  -- released at transaction end
-- or session-level:
SELECT pg_advisory_lock(12345);
SELECT pg_advisory_unlock(12345);
```

**Use for:** Single-flight migration, one relay worker per partition, cron job deduplication.

```java
@Transactional
public void runNightlyJob() {
    Boolean acquired = jdbcTemplate.queryForObject(
        "SELECT pg_try_advisory_xact_lock(?)", Boolean.class, JOB_LOCK_ID);
    if (!Boolean.TRUE.equals(acquired)) return;
    // run job
}
```

### 6.6 Lock Timeout and Statement Timeout

```sql
SET lock_timeout = '2s';      -- fail fast waiting for lock
SET statement_timeout = '30s'; -- kill long queries
```

Spring:

```yaml
spring.datasource.hikari.data-source-properties:
  lock_timeout: 2000
  statement_timeout: 30000
```

**Production:** Set `statement_timeout` at DB user or role level to prevent runaway queries.

---

## Section 7: SELECT FOR UPDATE, SKIP LOCKED, and Outbox Relay

### 7.1 SELECT FOR UPDATE — Pessimistic Locking

```sql
BEGIN;
SELECT * FROM inventory
WHERE sku = 'ABC-123'
FOR UPDATE;
-- row locked until COMMIT
UPDATE inventory SET quantity = quantity - 1 WHERE sku = 'ABC-123';
COMMIT;
```

JPA:

```java
@Lock(LockModeType.PESSIMISTIC_WRITE)
@Query("SELECT i FROM Inventory i WHERE i.sku = :sku")
Optional<Inventory> findBySkuForUpdate(@Param("sku") String sku);
```

**Warning:** Locks block other writers. Keep transaction minimal.

### 7.2 SKIP LOCKED — Work Queue Pattern

```sql
SELECT id, payload
FROM outbox_events
WHERE status = 'PENDING'
ORDER BY created_at
FOR UPDATE SKIP LOCKED
LIMIT 100;
```

**Behavior:**

- Rows already locked by another worker are **skipped** (not waited on).
- Multiple relay instances process concurrently without duplicate work on same row.
- Requires `FOR UPDATE` — plain `SELECT` does not lock.

### 7.3 Outbox Relay Query (Production)

```sql
WITH batch AS (
    SELECT id
    FROM outbox_events
    WHERE status = 'PENDING'
      AND next_attempt_at <= now()
    ORDER BY created_at
    FOR UPDATE SKIP LOCKED
    LIMIT 50
)
UPDATE outbox_events e
SET status = 'PROCESSING', locked_at = now(), locked_by = :workerId
FROM batch
WHERE e.id = batch.id
RETURNING e.*;
```

Full schema and relay patterns: [outbox-expert-playbook.md](outbox-expert-playbook.md) Section 4 and 6.

### 7.4 NOWAIT vs SKIP LOCKED

| Clause | Behavior |
|--------|----------|
| `FOR UPDATE NOWAIT` | Error immediately if row locked |
| `FOR UPDATE SKIP LOCKED` | Skip locked rows, return available |
| `FOR UPDATE` (default) | Wait until lock released |

### 7.5 Spring Implementation Sketch

```java
@Transactional
public List<OutboxEvent> claimBatch(String workerId, int limit) {
    return outboxRepository.claimPending(workerId, limit);
}

// Native query in repository:
@Query(value = """
    SELECT * FROM outbox_events
    WHERE status = 'PENDING' AND next_attempt_at <= now()
    ORDER BY created_at
    FOR UPDATE SKIP LOCKED
    LIMIT :limit
    """, nativeQuery = true)
List<OutboxEvent> findPendingForUpdate(@Param("limit") int limit);
```

**Critical:** Claim and status update must be **same transaction** — otherwise two workers can read same row.

### 7.6 Saga Step Locking

Saga orchestrator locks saga instance row:

```sql
SELECT * FROM saga_instances WHERE id = :id FOR UPDATE;
```

Prevents two orchestrator threads advancing same saga. See [saga-expert-playbook.md](saga-expert-playbook.md).

---

## Section 8: Flyway and Liquibase — Migrations in Production

### 8.1 Why Migrations Belong in the Service Repo

Each microservice owns its schema evolution. Migrations are **versioned, reviewed, and deployed with the application** — never applied manually in production except emergency break-glass.

```
deploy pipeline:
  build jar → run flyway migrate (init container or app) → start app → health check
```

**Rules:**

| Rule | Why |
|------|-----|
| Forward-only in prod | Rollback = new migration, not `flyway undo` |
| Idempotent where possible | `IF NOT EXISTS`, defensive checks for re-runs |
| One concern per migration | Easier review and bisect |
| Never edit applied migrations | Checksum mismatch breaks deploy |
| Test on production-like volume | Index creation on 100M rows ≠ dev |

### 8.2 Flyway — Spring Boot Default

```xml
<!-- pom.xml -->
<dependency>
    <groupId>org.flywaydb</groupId>
    <artifactId>flyway-core</artifactId>
</dependency>
<dependency>
    <groupId>org.flywaydb</groupId>
    <artifactId>flyway-database-postgresql</artifactId>
</dependency>
```

```yaml
spring:
  flyway:
    enabled: true
    locations: classpath:db/migration
    baseline-on-migrate: true        # existing DB without history
    validate-on-migrate: true
    out-of-order: false              # prod: strict ordering
```

Naming convention:

```
src/main/resources/db/migration/
  V1__create_orders_table.sql
  V2__add_orders_idempotency_key.sql
  V3__outbox_events_partial_index.sql
```

### 8.3 Safe DDL Patterns in PostgreSQL

**Adding a column (nullable first):**

```sql
-- V10__add_orders_priority.sql
ALTER TABLE orders ADD COLUMN priority SMALLINT NULL;

-- Backfill in app or batch job, then:
-- V11__orders_priority_not_null.sql
UPDATE orders SET priority = 0 WHERE priority IS NULL;
ALTER TABLE orders ALTER COLUMN priority SET NOT NULL;
ALTER TABLE orders ALTER COLUMN priority SET DEFAULT 0;
```

**Adding index without blocking writes (PG 11+):**

```sql
CREATE INDEX CONCURRENTLY IF NOT EXISTS orders_customer_status_idx
    ON orders (customer_id, status);
```

**Note:** `CONCURRENTLY` cannot run inside a Flyway transaction. Use Flyway `executeInTransaction=false`:

```java
// Optional: Flyway Java migration for CONCURRENTLY
public class V12__orders_customer_status_idx extends BaseJavaMigration {
    @Override
    public void migrate(Context context) throws Exception {
        context.getConnection()
            .createStatement()
            .execute("""
                CREATE INDEX CONCURRENTLY IF NOT EXISTS orders_customer_status_idx
                    ON orders (customer_id, status)
                """);
    }
    @Override
    public boolean canExecuteInTransaction() { return false; }
}
```

**Renaming columns:** Prefer add-new → dual-write → backfill → drop-old over in-place rename during active traffic.

### 8.4 Liquibase — When Teams Choose It

Liquibase offers XML/YAML/SQL changelogs, preconditions, and rollback scripts. Common in enterprises with DBA review gates.

```yaml
# db/changelog/db.changelog-master.yaml
databaseChangeLog:
  - include:
      file: db/changelog/changes/V001-orders-table.yaml
```

**Flyway vs Liquibase:**

| Aspect | Flyway | Liquibase |
|--------|--------|-----------|
| Learning curve | Lower | Higher |
| SQL-first | Yes | SQL, XML, YAML |
| Rollback | Community undo | Built-in rollback blocks |
| Spring Boot default | Native | Supported |
| Team preference | Java/Spring shops | Enterprise DBA workflows |

**Lead answer:** "We use Flyway because migrations are plain SQL in the service repo, reviewed in PRs, and run on deploy. Rollback is always a forward migration."

### 8.5 Migration Ordering with Multiple Instances

**Problem:** Two pods start simultaneously; both run Flyway.

**Solution:** Flyway uses a **schema history table** (`flyway_schema_history`) with advisory locking — only one migrator wins.

**Still risky:** Long-running migration blocks deploy. Use:

- Init container runs migrate before app pods start
- Blue/green: migrate before traffic switch
- `CREATE INDEX CONCURRENTLY` for large tables

### 8.6 Breaking Changes — Expand/Contract Pattern

```
Phase 1 (expand):  Add new column/table; app writes both old and new
Phase 2 (migrate): Backfill data
Phase 3 (contract): App reads new only; drop old column
```

Never drop a column the **currently deployed** app still reads.

### 8.7 Seed Data vs Migrations

| Type | Location | Runs in prod? |
|------|----------|---------------|
| Schema migrations | `db/migration/V*.sql` | Yes |
| Reference/lookup data | `R__reference_data.sql` (Flyway repeatable) | Carefully |
| Test fixtures | `src/test/resources` | No |

### 8.8 Migration Failure Runbook

```
1. Deploy fails: "Migration V15 failed"
2. Check flyway_schema_history — success=false row
3. Fix root cause (syntax, lock timeout, disk full)
4. If partial DDL applied: write repair migration V15_1, NOT edit V15
5. flyway repair only for checksum fixes on unapplied scripts
6. Re-deploy
```

---

## Section 9: HikariCP — Pool Sizing, Leaks, and JDBC Tuning

### 9.1 Why HikariCP

Spring Boot 2+ defaults to HikariCP — fast, minimal overhead, production-proven. The pool is the **gate** between your JVM threads and PostgreSQL's finite connection capacity.

```
App threads (200)  →  HikariCP pool (20)  →  PostgreSQL max_connections (100)
```

### 9.2 Pool Sizing Formula

**Classic rule:**

```
pool_size = ((core_count * 2) + effective_spindle_count)
```

For SSD/NVMe and cloud PostgreSQL, practical starting point:

```
maximum-pool-size = 10 to 20 per service instance
```

**Total connections budget:**

```
total = (instances × pool_size) + admin + migration + BI tools
total < PostgreSQL max_connections × 0.8
```

Example: 10 pods × 20 connections = 200 → need RDS with `max_connections` ≥ 250 or use PgBouncer.

### 9.3 Spring Boot HikariCP Configuration

```yaml
spring:
  datasource:
    url: jdbc:postgresql://pg-host:5432/order_db?ApplicationName=order-svc
    username: order_svc_app
    hikari:
      pool-name: order-svc-pool
      maximum-pool-size: 20
      minimum-idle: 5
      connection-timeout: 30000       # 30s wait for connection
      idle-timeout: 600000            # 10m — retire idle connections
      max-lifetime: 1800000           # 30m — below PG/RDS idle cutoff
      keepalive-time: 300000          # 5m — detect dead connections
      leak-detection-threshold: 60000 # log if held > 60s (dev/staging)
      data-source-properties:
        reWriteBatchedInserts: true   # PG batch insert optimization
        prepareThreshold: 5
        ApplicationName: order-svc
```

### 9.4 Connection Leaks — Symptoms and Fixes

**Symptoms:**

```
HikariPool-1 - Connection is not available, request timed out after 30000ms
HikariPool-1 - Thread starvation or clock leap detected
App threads blocked waiting for connection
```

**Common causes:**

| Cause | Fix |
|-------|-----|
| Stream/ResultSet not closed | Use try-with-resources; Spring Data handles for repos |
| `@Transactional` too long | Shorten TX; no HTTP/Kafka inside |
| Manual `EntityManager` not closed | Don't manage EM manually in Spring |
| Async without `@Transactional` propagation | TX ends before async completes — connection leak |
| Forgotten `@Transactional` on write | Connection held in weird state |

**Debug:**

```yaml
spring.datasource.hikari.leak-detection-threshold: 60000
logging.level.com.zaxxer.hikari: DEBUG
```

Stack trace in logs shows who borrowed connection.

### 9.5 PgBouncer — When You Need It

Use PgBouncer (transaction pooling mode) when:

- Many service instances × moderate pool = connection explosion
- Serverless/Lambda with frequent connect/disconnect
- Shared cluster with strict connection limits

**JPA caveat:** Transaction pooling breaks **prepared statement caching** and some session-level features. Use `pool_mode = transaction` with:

```yaml
spring.jpa.properties.hibernate.jdbc.lob.non_contextual_creation: true
# Disable server-side prepared statements if issues:
spring.datasource.hikari.data-source-properties.prepareThreshold: 0
```

### 9.6 JDBC URL Tuning for PostgreSQL

```
jdbc:postgresql://host:5432/db?
  ApplicationName=order-svc&
  reWriteBatchedInserts=true&
  tcpKeepAlive=true&
  connectTimeout=10&
  socketTimeout=60
```

| Parameter | Purpose |
|-----------|---------|
| `reWriteBatchedInserts` | Batch INSERT becomes multi-value INSERT |
| `ApplicationName` | Shows in `pg_stat_activity` |
| `socketTimeout` | Prevent hung reads |
| `targetServerType=preferSecondary` | Read replica routing (driver-level) |

### 9.7 Monitoring HikariCP

Micrometer metrics (Spring Boot Actuator):

```
hikaricp.connections.active
hikaricp.connections.idle
hikaricp.connections.pending
hikaricp.connections.timeout
```

**Alert when:** `pending > 0` sustained, or `active == maximum` for > 1 minute.

See [metrics-observability-playbook.md](metrics-observability-playbook.md).

---

## Section 10: JPA/Hibernate Fundamentals — EntityManager, Session, Flush

### 10.1 JPA vs Hibernate vs Spring Data

```
JPA (specification)
  └── Hibernate (implementation)
        └── Spring Data JPA (repository abstraction)
              └── Your @Entity + Repository interface
```

**Interview one-liner:** "JPA is the API, Hibernate is the engine, Spring Data JPA is the repository layer — I still need to understand Hibernate behavior for production debugging."

### 10.2 Entity Lifecycle States

| State | Description | In persistence context? |
|-------|-------------|---------------------------|
| **Transient** | `new Order()` — not persisted | No |
| **Managed** | Loaded or persisted in active session | Yes |
| **Detached** | Was managed; session closed | No |
| **Removed** | `entityManager.remove()` scheduled delete | Yes until flush |

```java
Order order = new Order();           // transient
order = orderRepo.save(order);       // managed (persist + flush may defer)
// end @Transactional
order.setStatus(CANCELLED);          // detached — change NOT persisted unless merge/save
```

### 10.3 Persistence Context (Session)

One persistence context per transaction (default `EntityManager` scope).

```java
@Transactional
public void example() {
    Order a = orderRepo.findById(1L).get();  // SELECT
    Order b = orderRepo.findById(1L).get();  // same instance from L1 cache — no SELECT
    assert a == b;  // identity equality
}
```

**First-level cache:** Per-session. **Second-level cache:** Shared across sessions (Section 18) — use sparingly.

### 10.4 Flush Modes

| FlushMode | Behavior |
|-----------|----------|
| `AUTO` (default) | Flush before query if dirty entities might affect results |
| `COMMIT` | Flush only at commit |
| `MANUAL` | Explicit `entityManager.flush()` |

```java
@Transactional
public void bulkUpdate() {
    entityManager.setFlushMode(FlushModeType.COMMIT);
    for (Order o : orders) {
        o.setProcessed(true);
    }
    // flush once at commit — fewer round trips
}
```

### 10.5 Dirty Checking

Hibernate tracks managed entity field changes via bytecode enhancement or reflection. At flush:

```sql
UPDATE orders SET status = ?, updated_at = ? WHERE id = ? AND version = ?
```

**Cost:** Hibernate compares all managed entities at flush. Large persistence context → slow flush.

**Fix:** `entityManager.clear()` in batch jobs; `@Modifying` bulk updates; stateless session.

### 10.6 Lazy Loading — The Proxy Trap

```java
@Entity
public class Order {
    @OneToMany(mappedBy = "order", fetch = FetchType.LAZY)
    private List<OrderItem> items;
}
```

```java
Order order = orderRepo.findById(1L).get();
// transaction ends
order.getItems().size();  // LazyInitializationException
```

**Fixes:**

1. `@Transactional` on service method that uses collection
2. `JOIN FETCH` in query
3. `@EntityGraph`
4. DTO projection — don't expose entities outside service layer

### 10.7 equals/hashCode with JPA Entities

**Rule:** Use **business key** or **id only** (if assigned):

```java
@Override
public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof Order other)) return false;
    return id != null && id.equals(other.id);
}

@Override
public int hashCode() {
    return getClass().hashCode();
}
```

Never include mutable fields or collections in `hashCode`.

### 10.8 Mapping Best Practices

```java
@Entity
@Table(name = "orders")
@Getter
@Setter
public class Order {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "customer_id", nullable = false)
    private Long customerId;

    @Column(name = "status", nullable = false, length = 32)
    private String status;

    @OneToMany(mappedBy = "order", cascade = CascadeType.ALL, orphanRemoval = true)
    private List<OrderItem> items = new ArrayList<>();

    @Version
    private Long version;
}
```

| Setting | Guidance |
|---------|----------|
| `CascadeType.ALL` | Only on true composition (order → items) |
| `orphanRemoval = true` | Removing from collection deletes row |
| `FetchType.EAGER` | Avoid on collections — N+1 and over-fetch |
| Lombok `@Data` on entities | Avoid — breaks equals/hashCode |

---

## Section 11: N+1 Problem — Deep Dive and All Fixes

### 11.1 What N+1 Looks Like

```java
List<Order> orders = orderRepo.findByCustomerId(customerId);  // 1 query
for (Order order : orders) {
    log.info("{}", order.getItems().size());  // N queries
}
```

Hibernate SQL log:

```sql
SELECT * FROM orders WHERE customer_id = ?;
SELECT * FROM order_items WHERE order_id = ?;  -- × 100 orders
```

**101 queries** for 100 orders. Latency and connection pool exhaustion at scale.

### 11.2 Detecting N+1

| Method | How |
|--------|-----|
| Hibernate stats | `hibernate.generate_statistics=true` |
| Datasource proxy | logback JDBC proxy, p6spy |
| APM | Datadog/New Relic JDBC span count per request |
| Tests | `@Sql` + assert query count with `@DataJpaTest` + statistics |

```yaml
spring.jpa.properties:
  hibernate.generate_statistics: true
logging.level.org.hibernate.stat: DEBUG
```

### 11.3 Fix 1 — JOIN FETCH

```java
@Query("""
    SELECT DISTINCT o FROM Order o
    JOIN FETCH o.items
    WHERE o.customerId = :customerId
    """)
List<Order> findByCustomerIdWithItems(@Param("customerId") Long customerId);
```

**Caution:** `DISTINCT` needed — cartesian product from multiple bags. **Cannot** `JOIN FETCH` two `List` collections (MultipleBagFetchException) — use Set or two queries.

### 11.4 Fix 2 — @EntityGraph

```java
@EntityGraph(attributePaths = {"items"})
List<Order> findByCustomerId(Long customerId);
```

Cleaner than JPQL for simple cases. Same bag limitation applies.

### 11.5 Fix 3 — Batch Fetching (@BatchSize)

```java
@Entity
public class Order {
    @OneToMany(mappedBy = "order")
    @BatchSize(size = 25)
    private List<OrderItem> items;
}
```

Or global:

```yaml
spring.jpa.properties.hibernate.default_batch_fetch_size: 25
```

Hibernate issues:

```sql
SELECT * FROM order_items WHERE order_id IN (?,?,?,...);  -- batches of 25
```

**Trade-off:** 1 + ceil(N/25) queries vs 1 query with JOIN FETCH. Simpler, avoids cartesian product.

### 11.6 Fix 4 — DTO Projection (Best for Read APIs)

```java
@Query("""
    SELECT new com.example.OrderSummaryDto(o.id, o.status, COUNT(i))
    FROM Order o LEFT JOIN o.items i
    WHERE o.customerId = :customerId
    GROUP BY o.id, o.status
    """)
List<OrderSummaryDto> findSummariesByCustomer(@Param("customerId") Long customerId);
```

One query, no entity graph, no lazy load surprises.

### 11.7 Fix 5 — Subselect Fetch (Hibernate-specific)

```java
@Fetch(FetchMode.SUBSELECT)
@OneToMany(mappedBy = "order")
private List<OrderItem> items;
```

One extra query for all items of all orders in persistence context — good for list pages.

### 11.8 N+1 in Spring Data Derived Queries

```java
List<Order> findByStatus(String status);  // 1 query
// then access lazy collection → N+1
```

**Prevention:** Service layer always uses fetch-optimized repository method for use cases that need associations.

### 11.9 Decision Matrix

| Scenario | Recommended fix |
|----------|-----------------|
| Single order detail page | `JOIN FETCH` or `@EntityGraph` |
| Order list (no items) | Don't fetch items; DTO without association |
| Order list with item count | DTO with `COUNT` aggregation |
| Many orders, need all items | `@BatchSize` or `SUBSELECT` |
| Report/export | Native SQL or JDBC, not entity graph |

---

## Section 12: Pagination — Offset vs Keyset/Cursor

### 12.1 Offset Pagination — Simple but O(n)

```java
Page<Order> findByCustomerId(Long customerId, Pageable pageable);
```

```sql
SELECT * FROM orders
WHERE customer_id = ?
ORDER BY created_at DESC
LIMIT 20 OFFSET 100000;
```

**Problem:** PostgreSQL must scan and discard 100,000 rows. Page 5000 gets slower linearly.

**Use when:** Small tables, admin UI with random page jumps, OFFSET < few thousand.

### 12.2 Keyset (Cursor) Pagination — Production Default

```sql
SELECT * FROM orders
WHERE customer_id = :customerId
  AND (created_at, id) < (:lastCreatedAt, :lastId)
ORDER BY created_at DESC, id DESC
LIMIT 20;
```

**Requires:** Stable sort key — `(created_at, id)` tiebreaker for non-unique timestamps.

```java
public record OrderCursor(Instant createdAt, Long id) {}

@Query(value = """
    SELECT * FROM orders
    WHERE customer_id = :customerId
      AND (created_at, id) < (:cursorCreatedAt, :cursorId)
    ORDER BY created_at DESC, id DESC
    LIMIT :limit
    """, nativeQuery = true)
List<Order> findNextPage(
    @Param("customerId") Long customerId,
    @Param("cursorCreatedAt") Instant cursorCreatedAt,
    @Param("cursorId") Long cursorId,
    @Param("limit") int limit);
```

**Index required:**

```sql
CREATE INDEX orders_customer_created_id_idx
    ON orders (customer_id, created_at DESC, id DESC);
```

### 12.3 Spring Data Page vs Slice

| Type | Extra COUNT query? | Use |
|------|-------------------|-----|
| `Page<T>` | Yes — `SELECT count(*)` | UI showing "Page 3 of 47" |
| `Slice<T>` | No | Infinite scroll, "load more" |
| Custom cursor | No | High-volume feeds |

```java
Slice<Order> findByCustomerId(Long customerId, Pageable pageable);
// hasNext() without total count
```

### 12.4 Avoid COUNT(*) on Large Tables

`Page<Order>` triggers:

```sql
SELECT count(*) FROM orders WHERE customer_id = ?;  -- full index/table scan
```

**Alternatives:**

- Return `Slice` without total
- Approximate count: `pg_class.reltuples` for dashboards
- Cache total with TTL
- Keyset only — no page numbers

### 12.5 Sorting Pitfalls

| Pitfall | Fix |
|---------|-----|
| Non-unique ORDER BY column | Add `id` as tiebreaker |
| Sorting on unindexed column | Add index matching ORDER BY |
| `LOWER(email)` sort | Expression index or stored generated column |

### 12.6 JPA and Keyset in APIs

API response:

```json
{
  "items": [...],
  "nextCursor": "2024-06-01T12:00:00Z|12345"
}
```

Encode cursor as opaque Base64 of `(created_at, id)` — prevents client manipulation of sort.

---

## Section 13: Read Replicas — Routing, Lag, and Consistency

### 13.1 Architecture

```
                    ┌─────────────┐
  Write traffic ──► │   Primary   │
                    └──────┬──────┘
                           │ streaming replication
              ┌────────────┼────────────┐
              ▼            ▼            ▼
        ┌──────────┐ ┌──────────┐ ┌──────────┐
        │ Replica 1│ │ Replica 2│ │ Replica 3│
        └──────────┘ └──────────┘ └──────────┘
              ▲
  Read traffic ─┘ (reports, list queries, CQRS projectors)
```

### 13.2 Replication Lag

```sql
-- On primary: replication lag per standby
SELECT application_name, state, sync_state,
       pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn) AS lag_bytes
FROM pg_stat_replication;
```

**Typical lag:** milliseconds to seconds. Under load or large transactions: seconds to minutes.

**User-visible bug:** Create order → redirect to detail → 404 because read replica hasn't caught up.

### 13.3 Routing Strategies

| Strategy | Implementation |
|----------|----------------|
| **Primary only** | Simplest; all traffic to writer |
| **Replica for @Transactional(readOnly=true)** | Spring AbstractRoutingDataSource |
| **CQRS read DB** | Separate database fed by projector — [cqrs-expert-playbook.md](cqrs-expert-playbook.md) |
| **Driver-level** | JDBC `targetServerType=preferSecondary` |

### 13.4 Spring Routing DataSource Sketch

```java
public class RoutingDataSource extends AbstractRoutingDataSource {
    @Override
    protected Object determineCurrentLookupKey() {
        return TransactionSynchronizationManager.isCurrentTransactionReadOnly()
            ? "replica" : "primary";
    }
}
```

```java
@Transactional(readOnly = true)
public List<Order> listOrders(Long customerId) {
    return orderRepo.findByCustomerId(customerId);  // routes to replica
}
```

**Caution:** Replication lag → stale reads. Not for "read-your-writes" immediately after mutation unless routed to primary.

### 13.5 Read-Your-Writes Patterns

| Pattern | How |
|---------|-----|
| **Sticky primary after write** | Session flag: next read from primary for 2s |
| **Wait for replay** | `pg_current_wal_lsn()` compare — rarely in app code |
| **Return DTO from write** | Create response includes full object — no immediate re-read |
| **Eventual consistency UX** | "Processing..." state until projector catches up |

### 13.6 Replica Failover

Managed PostgreSQL (RDS, Cloud SQL): automatic failover promotes replica.

**App impact:** Connection strings with cluster endpoint; HikariCP `max-lifetime` refreshes connections; brief errors during failover — retry with backoff.

### 13.7 What NOT to Put on Replicas

- Writes (will fail or route wrong)
- `@Transactional` saga steps requiring strong consistency
- Outbox relay claiming (must be primary — locks are on primary)
- Serializable isolation depending on latest state

---

## Section 14: Idempotency Tables and Outbox Schema

### 14.1 Why Idempotency Keys

HTTP retries, Kafka at-least-once delivery, and client double-clicks cause **duplicate processing**. Idempotency keys make handlers safe to retry.

```
Client: POST /orders  Idempotency-Key: abc-123
Server: first request → create order 201
        retry same key → return same order 200 (no duplicate)
```

### 14.2 Idempotency Table Schema

```sql
CREATE TABLE idempotency_records (
    id              BIGSERIAL PRIMARY KEY,
    idempotency_key VARCHAR(128) NOT NULL,
    request_hash    VARCHAR(64)  NOT NULL,   -- hash of body + route
    response_status SMALLINT     NOT NULL,
    response_body   JSONB        NOT NULL,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    expires_at      TIMESTAMPTZ  NOT NULL,
    CONSTRAINT idempotency_key_uq UNIQUE (idempotency_key)
);

CREATE INDEX idempotency_expires_idx ON idempotency_records (expires_at);
```

**Flow:**

```
BEGIN;
  SELECT * FROM idempotency_records WHERE idempotency_key = ? FOR UPDATE;
  -- if exists and hash matches → return cached response
  -- if exists and hash differs → 409 Conflict
  -- else process request, INSERT idempotency_record, COMMIT
```

### 14.3 Idempotency vs Unique Business Constraint

| Layer | Example |
|-------|---------|
| Idempotency key | Same request retried → same outcome |
| Unique constraint | `payment_id` unique — duplicate insert fails |

Use **both**: idempotency for API layer, unique constraints for domain invariants.

```sql
CREATE UNIQUE INDEX payments_provider_ref_uq
    ON payments (provider, provider_reference);
```

### 14.4 Outbox Table Schema (Production)

```sql
CREATE TABLE outbox_events (
    id              BIGSERIAL PRIMARY KEY,
    aggregate_type  VARCHAR(64)  NOT NULL,
    aggregate_id    VARCHAR(64)  NOT NULL,
    event_type      VARCHAR(128) NOT NULL,
    payload         JSONB        NOT NULL,
    status          VARCHAR(16)  NOT NULL DEFAULT 'PENDING',
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    next_attempt_at TIMESTAMPTZ  NOT NULL DEFAULT now(),
    locked_at       TIMESTAMPTZ,
    locked_by       VARCHAR(64),
    attempts        INT          NOT NULL DEFAULT 0,
    last_error      TEXT
);

CREATE INDEX outbox_pending_idx
    ON outbox_events (created_at)
    WHERE status = 'PENDING';
```

Full relay, DLQ, and monitoring: [outbox-expert-playbook.md](outbox-expert-playbook.md).

### 14.5 Same Transaction — Write + Outbox

```java
@Transactional
public Order placeOrder(PlaceOrderCommand cmd, String idempotencyKey) {
    // 1. Check idempotency
    // 2. INSERT order + order_items
    // 3. INSERT outbox_events (same TX)
    // 4. INSERT idempotency_record with response
    return order;
}
// Relay publishes to Kafka AFTER commit — never before
```

**Golden rule:** Message broker and database cannot participate in one 2PC in most microservices. Outbox bridges this.

### 14.6 Consumer Idempotency

```sql
CREATE TABLE processed_events (
    event_id    UUID PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

```java
@Transactional
public void handle(Event e) {
    if (processedRepo.existsById(e.getId())) return;
    // business logic
    processedRepo.save(new ProcessedEvent(e.getId()));
}
```

Or rely on natural idempotency: `UPSERT ... ON CONFLICT DO NOTHING`.

### 14.7 TTL and Cleanup

Idempotency records expire after 24–72 hours. Scheduled job:

```sql
DELETE FROM idempotency_records WHERE expires_at < now() LIMIT 10000;
```

Partition `outbox_events` by month if volume is high; archive `PUBLISHED` events after 7 days.

---

## Section 15: JSONB Columns — When, How, Indexing

### 15.1 When to Use JSONB

| Use JSONB | Use normalized columns |
|----------|------------------------|
| Semi-structured metadata (product attributes) | Core domain fields queried/filtered heavily |
| Event payload snapshot in outbox | Foreign keys, amounts, status |
| Provider-specific response blob | Columns needing strong constraints |
| Schema varies by tenant/type | Reporting joins |

**Rule:** Promote frequently queried JSON fields to real columns via migration when access pattern stabilizes.

### 15.2 Column Definition and JPA Mapping

```sql
ALTER TABLE products ADD COLUMN metadata JSONB NOT NULL DEFAULT '{}';
```

```java
@Entity
public class Product {
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(columnDefinition = "jsonb")
    private Map<String, Object> metadata;
}
```

Or typed POJO:

```java
@JdbcTypeCode(SqlTypes.JSON)
private ProductMetadata metadata;
```

### 15.3 Querying JSONB

```sql
-- Containment
SELECT * FROM products WHERE metadata @> '{"brand": "Nike"}';

-- Path extraction
SELECT * FROM products WHERE metadata->>'brand' = 'Nike';

-- Nested
SELECT * FROM products WHERE metadata #>> '{dimensions,weight}' > '500';
```

Spring Data native query or `@Query` with native SQL for complex JSONB — JPQL doesn't support `@>`.

### 15.4 Indexing JSONB

```sql
-- GIN for containment (@>, ?, ?|, ?&)
CREATE INDEX products_metadata_gin_idx
    ON products USING GIN (metadata jsonb_path_ops);

-- Expression index for specific key
CREATE INDEX products_brand_idx
    ON products ((metadata->>'brand'));
```

| Index | Best for |
|-------|----------|
| `jsonb_ops` (default GIN) | Many keys, `@>` containment |
| `jsonb_path_ops` | Smaller index, `@>` only |
| Expression B-tree | Equality on one extracted field |

### 15.5 JSONB Anti-Patterns

| Anti-pattern | Problem |
|--------------|---------|
| Entire entity as JSONB | No constraints, no FK, hard migrations |
| `metadata->>'field' LIKE '%x%'` | No index; full scan |
| Huge documents (>1MB) | TOAST overhead, slow updates |
| Frequent partial updates | Rewrites whole JSONB value — consider normalized table |

### 15.6 Validation

```sql
ALTER TABLE products ADD CONSTRAINT metadata_is_object
    CHECK (jsonb_typeof(metadata) = 'object');
```

App-layer validation with JSON Schema before persist.

---

## Section 16: Audit Columns — created_at, updated_at, versioning

### 16.1 Standard Audit Columns

```sql
CREATE TABLE orders (
    id          BIGSERIAL PRIMARY KEY,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_by  VARCHAR(64),
    updated_by  VARCHAR(64),
    version     BIGINT NOT NULL DEFAULT 0
);
```

### 16.2 JPA @CreatedDate / @LastModifiedDate

```java
@EntityEntityListeners(AuditingEntityListener.class)
public class Order {

    @CreatedDate
    @Column(nullable = false, updatable = false)
    private Instant createdAt;

    @LastModifiedDate
    @Column(nullable = false)
    private Instant updatedAt;

    @CreatedBy
    private String createdBy;

    @LastModifiedBy
    private String updatedBy;
}
```

```java
@Configuration
@EnableJpaAuditing
public class JpaConfig { }
```

Provide `AuditorAware<String>` from SecurityContext.

### 16.3 Optimistic Locking with @Version

```java
@Version
private Long version;
```

Hibernate adds `WHERE version = ?` to UPDATE. Concurrent update → `OptimisticLockException`.

**Interview answer:** "We use `@Version` for concurrent order updates; client gets 409 and refreshes. For inventory decrement under contention, we combine optimistic or short `FOR UPDATE`."

### 16.4 updated_at Without Triggers

**Option A — JPA `@PreUpdate`:**

```java
@PreUpdate
void onUpdate() {
    this.updatedAt = Instant.now();
}
```

**Option B — PostgreSQL trigger (works for non-JPA writers too):**

```sql
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER orders_updated_at
    BEFORE UPDATE ON orders
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();
```

Use trigger when ETL, admin scripts, or multiple apps write same table.

### 16.5 Soft Delete Audit

```sql
deleted_at  TIMESTAMPTZ NULL,
deleted_by  VARCHAR(64) NULL
```

```java
@SQLDelete(sql = "UPDATE customers SET deleted_at = now() WHERE id = ?")
@SQLRestriction("deleted_at IS NULL")
public class Customer { }
```

### 16.6 Full Audit History (Separate Table)

For compliance (who changed what field when):

```sql
CREATE TABLE orders_audit (
    audit_id    BIGSERIAL PRIMARY KEY,
    order_id    BIGINT NOT NULL,
    action      VARCHAR(8) NOT NULL,
    old_data    JSONB,
    new_data    JSONB,
    changed_by  VARCHAR(64),
    changed_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

Hibernate Envers (`@Audited`) or Debezium CDC capture changes without app code.

---

## Section 17: Spring Data JPA — Repositories, Projections, Specifications

### 17.1 Repository Hierarchy

```java
public interface OrderRepository extends JpaRepository<Order, Long> {
    List<Order> findByCustomerIdAndStatus(Long customerId, String status);
    Optional<Order> findByIdempotencyKey(String key);
}
```

Spring generates implementation at runtime from method name or `@Query`.

### 17.2 Derived Query Pitfalls

```java
// Generates: ... WHERE customerId = ? AND status = ? ORDER BY createdAt DESC
List<Order> findByCustomerIdAndStatusOrderByCreatedAtDesc(
    Long customerId, String status);
```

| Pitfall | Issue |
|---------|-------|
| Unindexed sort/filter | Seq scan |
| `Containing` | `%value%` — often no index |
| `First10` vs `Top10` | Limit without guarantee without Pageable |
| Returning `List` for unique lookup | Use `Optional` |

### 17.3 @Query — JPQL and Native

```java
@Query("SELECT o FROM Order o WHERE o.customerId = :cid AND o.status IN :statuses")
List<Order> findActiveForCustomer(
    @Param("cid") Long customerId,
    @Param("statuses") Collection<String> statuses);

@Modifying
@Query("UPDATE Order o SET o.status = :status WHERE o.id = :id")
int updateStatus(@Param("id") Long id, @Param("status") String status);
```

`@Modifying` queries need `@Transactional` and often `clearAutomatically = true` to refresh persistence context.

### 17.4 Projections — Interface and DTO

**Closed projection (interface):**

```java
public interface OrderSummary {
    Long getId();
    String getStatus();
    Instant getCreatedAt();
}

List<OrderSummary> findByCustomerId(Long customerId);
```

**DTO constructor:**

```java
@Query("SELECT new com.example.OrderSummaryDto(o.id, o.status, o.totalAmount) FROM Order o WHERE ...")
List<OrderSummaryDto> findDtos(...);
```

Projections avoid loading full entity and lazy associations.

### 17.5 Specifications (Dynamic Queries)

```java
public class OrderSpecs {
    public static Specification<Order> hasCustomer(Long customerId) {
        return (root, q, cb) -> cb.equal(root.get("customerId"), customerId);
    }
    public static Specification<Order> statusIn(Collection<String> statuses) {
        return (root, q, cb) -> root.get("status").in(statuses);
    }
}

orderRepo.findAll(Specification.where(hasCustomer(id)).and(statusIn(statuses)), pageable);
```

Use for admin search with optional filters. **Ensure indexes match** common filter combinations.

### 17.6 Custom Repository Fragment

```java
public interface OrderRepositoryCustom {
    List<Order> claimPendingOutbox(int limit);
}

public class OrderRepositoryImpl implements OrderRepositoryCustom {
    @PersistenceContext
    private EntityManager em;
    // native query implementation
}

public interface OrderRepository extends JpaRepository<Order, Long>, OrderRepositoryCustom { }
```

### 17.7 @EntityGraph on Repository Method

```java
@EntityGraph(attributePaths = {"items"})
Optional<Order> findWithItemsById(Long id);
```

Prefer explicit fetch methods over default `findById` when items always needed.

---

## Section 18: Hibernate Performance — Batch Writes, Statelessness, Caching

### 18.1 JDBC Batch Inserts/Updates

```yaml
spring.jpa.properties:
  hibernate.jdbc.batch_size: 25
  hibernate.order_inserts: true
  hibernate.order_updates: true
  hibernate.jdbc.batch_versioned_data: true
```

Plus PostgreSQL driver:

```yaml
spring.datasource.hikari.data-source-properties.reWriteBatchedInserts: true
```

**Verify in logs:**

```sql
INSERT INTO order_items (order_id, sku, qty) VALUES (?,?,?), (?,?,?), ...
```

Without batching: one INSERT per row.

### 18.2 Batch Insert Entity Pattern

```java
@Transactional
public void importItems(List<OrderItem> items) {
    for (int i = 0; i < items.size(); i++) {
        entityManager.persist(items.get(i));
        if (i % 25 == 0) {
            entityManager.flush();
            entityManager.clear();
        }
    }
}
```

`clear()` detaches entities — prevents persistence context bloat.

### 18.3 StatelessSession for Bulk ETL

```java
SessionFactory sf = entityManagerFactory.unwrap(SessionFactory.class);
try (StatelessSession session = sf.openStatelessSession()) {
    Transaction tx = session.beginTransaction();
    for (OrderItem item : items) {
        session.insert(item);
    }
    tx.commit();
}
```

No dirty checking, no L1 cache — maximum throughput for bulk loads.

### 18.4 Read-Only Optimization

```java
@Transactional(readOnly = true)
public List<OrderDto> listOrders() { ... }
```

```yaml
spring.jpa.properties.hibernate.connection.provider_disables_autocommit: true
```

Hibernate skips dirty checking on read-only transactions.

### 18.5 Second-Level Cache — Use Sparingly

```java
@Entity
@Cacheable
@org.hibernate.annotations.Cache(usage = CacheConcurrencyStrategy.READ_WRITE)
public class Product { }
```

Requires JCache/Ehcache region factory. **Only for:**

- Rarely changing reference data (country codes, tax rates)
- High read, low write

**Never** cache frequently updated entities — cache invalidation complexity exceeds benefit.

### 18.6 Query Cache

Rarely enabled in microservices. Prefer application-level Caffeine/Redis for hot keys with explicit TTL.

### 18.7 Statement Logging in Production

```yaml
# NEVER log all SQL at INFO in prod
logging.level.org.hibernate.SQL: WARN
spring.jpa.properties.hibernate.session.events.log.LOG_QUERIES_SLOWER_THAN_MS: 500
```

Use APM JDBC instrumentation instead of full SQL logs.

### 18.8 Hibernate DDL — Never in Production

```yaml
spring.jpa.hibernate.ddl-auto: validate   # prod
# spring.jpa.hibernate.ddl-auto: update    # dev only — NEVER prod
```

Schema changes via Flyway only.

---

## Section 19: Connection and Transaction Management in Spring

### 19.1 Transaction Propagation Cheat Sheet

| Propagation | Behavior | Use case |
|-------------|----------|----------|
| `REQUIRED` (default) | Join or create | Normal service methods |
| `REQUIRES_NEW` | Always new TX | Audit log that must commit even if outer rolls back |
| `NESTED` | Savepoint nested | Rare; partial rollback within TX |
| `NOT_SUPPORTED` | Suspend TX | Call external non-transactional API |
| `MANDATORY` | Must exist | Enforce TX started by caller |
| `NEVER` | Must not exist | Assert no TX |

### 19.2 REQUIRES_NEW — Danger and Use

```java
@Transactional(propagation = Propagation.REQUIRES_NEW)
public void writeAuditLog(AuditEntry entry) {
    auditRepo.save(entry);
}
```

**Use:** Audit trail survives business rollback.

**Danger:** Holds second connection from pool while outer TX holds first — doubles pool pressure.

### 19.3 Self-Invocation Trap

```java
@Service
public class OrderService {
    public void placeOrder() {
        this.saveOrder();  // @Transactional NOT applied — same bean, no proxy
    }

    @Transactional
    public void saveOrder() { ... }
}
```

**Fix:** Inject self, move to another bean, or use `@Transactional` on caller.

### 19.4 @Transactional on Private Methods

**Does not work** — Spring AOP proxies only intercept public methods on proxied beans.

### 19.5 Programmatic Transactions

```java
@Autowired TransactionTemplate transactionTemplate;

public void run() {
    transactionTemplate.executeWithoutResult(status -> {
        orderRepo.save(order);
        outboxRepo.save(event);
    });
}
```

Use when annotation model doesn't fit (conditional TX boundaries).

### 19.6 Connection-Transaction Lifecycle

```
1. @Transactional method entered
2. Spring opens connection from HikariCP, sets autoCommit=false
3. EntityManager joins transaction
4. Queries execute on same connection
5. Flush on commit
6. Commit or rollback
7. Connection returned to pool (reset autoCommit)
```

### 19.7 LazyInitializationException Root Cause

Transaction ended before lazy collection accessed. Fix at architectural level — don't extend TX to controller; fetch in service.

### 19.8 Integration Test Transaction Rollback

```java
@DataJpaTest
@AutoConfigureTestDatabase(replace = NONE)
class OrderRepositoryTest {
    @Autowired OrderRepository repo;
    // each test @Transactional rollback by default
}
```

### 19.9 @TransactionalEventListener

```java
@TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
public void onOrderPlaced(OrderPlacedEvent event) {
    // runs after successful commit — safe for non-outbox side effects
    // still not atomic with DB — use outbox for broker publish
}
```

**AFTER_COMMIT** for: cache eviction, email (if acceptable to miss on crash). **Not** for Kafka without outbox.

### 19.10 Multiple DataSources

CQRS write vs read DB:

```java
@EnableJpaRepositories(
    basePackages = "com.example.write",
    entityManagerFactoryRef = "writeEmf",
    transactionManagerRef = "writeTxManager"
)
```

Separate pools, separate Flyway locations.

### 19.11 Testcontainers Integration Test

```java
@SpringBootTest
@Testcontainers
class OrderRepositoryTest {
    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
    }
}
```

Real PostgreSQL — catches PG-specific SQL, JSONB, `SKIP LOCKED`.

---

## Section 20: Production Runbook — Slow Queries, Locks, Pool Exhaustion, Replication Lag

### 20.0 Alert Thresholds (Starting Points)

| Metric | Warning | Critical |
|--------|---------|----------|
| Query p95 latency | > 200ms | > 1s |
| Active connections | > 70% max | > 85% |
| Replication lag | > 5s | > 30s |
| Deadlocks per hour | > 5 | > 20 |
| `idle in transaction` count | > 5 | > 20 |
| HikariCP pending threads | > 0 sustained | > 10 |

### 20.1 Alert: Database p95 Latency Spike

```
Triage checklist:
□ APM: which endpoint/query?
□ pg_stat_statements: top total_exec_time last hour
□ Deploy correlation: new release?
□ EXPLAIN ANALYZE on suspect query (staging replica)
□ pg_stat_activity: waiting on Lock? IO? CPU?
```

### 20.2 Kill Runaway Query

```sql
SELECT pid, usename, state, query_start, query
FROM pg_stat_activity
WHERE state = 'active' AND query_start < now() - interval '5 minutes';

SELECT pg_cancel_backend(:pid);   -- graceful
SELECT pg_terminate_backend(:pid); -- force if cancel fails
```

**Caution:** Terminating writer may rollback large transaction — spike in WAL and replication lag.

### 20.3 Pool Exhaustion Playbook

```
Symptoms: Connection timed out, threads blocked, Hikari pending > 0

Steps:
1. Confirm active connections: pg_stat_activity count by application_name
2. Check idle in transaction: state = 'idle in transaction'
3. Kill long idle-in-transaction sessions
4. Review recent deploy for @Transactional scope leaks
5. Temporary: scale pods OR increase pool (if PG headroom)
6. Root fix: shorten transactions, fix leak, add PgBouncer
```

### 20.4 Deadlock Spike

```sql
-- PostgreSQL logs deadlocks if log_lock_waits = on
grep "deadlock detected" /var/log/postgresql/...
```

Review lock ordering in hot code paths. Add retry. Consider optimistic locking.

### 20.5 Replication Lag Critical

```
1. Check pg_stat_replication lag_bytes on primary
2. Identify long transactions on primary blocking WAL replay
3. Check replica disk IO saturation
4. Temporary: route critical reads to primary
5. Scale replica CPU/IO; reduce large batch writes on primary
```

### 20.6 Disk Full / Bloat

```sql
SELECT pg_size_pretty(pg_database_size(current_database()));

SELECT schemaname, relname, n_dead_tup, last_autovacuum
FROM pg_stat_user_tables
ORDER BY n_dead_tup DESC;
```

Manual `VACUUM (ANALYZE)` on hot tables if autovacuum falling behind.

### 20.7 Failover Procedure (Managed PG)

```
1. Confirm primary unhealthy via health check
2. Managed service auto-failover OR manual promote
3. Verify app using cluster writer endpoint
4. HikariCP recycles connections within max-lifetime
5. Monitor error rate for 15 minutes
6. Post-incident: verify replica rebuilt
```

### 20.8 Communication Template

```
INCIDENT: order-svc DB degradation
Impact: Order placement p95 3s (normal 200ms)
Cause: Missing index on orders(status) after V14 migration
Mitigation: CREATE INDEX CONCURRENTLY orders_status_idx
Status: Monitoring — p95 recovered to 250ms
```

### 20.9 Scenario: Migration Failed Mid-Deploy

```
1. Check flyway_schema_history — failed migration version
2. Flyway repair if checksum mismatch (understand why)
3. Manual DDL completion if migration partially applied
4. Never delete history row without understanding state
5. Roll forward with compensating migration — not backward in prod
```

### 20.10 Scenario: Outbox Relay Stalled

See [outbox-expert-playbook.md](outbox-expert-playbook.md) Section 14.

Quick checks:

```sql
SELECT status, count(*) FROM outbox_events GROUP BY status;
SELECT count(*) FROM outbox_events WHERE status = 'PENDING' AND next_attempt_at <= now();
```

Stuck `PROCESSING` rows → relay crashed mid-batch — reset with timeout policy.

### 20.11 Five-Layer DB Debugging Framework

```
Layer 1: Application — pool metrics, transaction boundaries, N+1
Layer 2: SQL — EXPLAIN, pg_stat_statements, slow log
Layer 3: Locks — pg_locks, blocking pids, deadlocks
Layer 4: PostgreSQL instance — connections, vacuum, disk, memory
Layer 5: Infrastructure — replication, network, storage IOPS, failover state
```

---

## Section 21: Revision Cheat Sheets — SQL, Hibernate, Spring Config

### 21.1 PostgreSQL Quick Reference

```sql
-- Lock inspection
SELECT * FROM pg_locks WHERE NOT granted;
SELECT pg_blocking_pids(pid) FROM pg_stat_activity WHERE pid = :blocked;

-- Active queries
SELECT pid, usename, application_name, state, wait_event_type, query
FROM pg_stat_activity WHERE datname = current_database();

-- Table size
SELECT relname, pg_size_pretty(pg_total_relation_size(relid))
FROM pg_stat_user_tables ORDER BY pg_total_relation_size(relid) DESC;

-- Index usage
SELECT indexrelname, idx_scan FROM pg_stat_user_indexes WHERE relname = 'orders';

-- Slow queries (pg_stat_statements)
SELECT calls, mean_exec_time, query FROM pg_stat_statements
ORDER BY total_exec_time DESC LIMIT 10;

-- Replication lag
SELECT application_name, pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn) AS lag
FROM pg_stat_replication;

-- Outbox claim
SELECT id FROM outbox_events WHERE status = 'PENDING'
ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT 50;
```

### 21.2 Hibernate Quick Reference

```yaml
# application.yml essentials
spring.jpa:
  open-in-view: false
  properties.hibernate:
    jdbc.batch_size: 25
    order_inserts: true
    order_updates: true
    default_batch_fetch_size: 25
    format_sql: false
    generate_statistics: false
  hibernate.ddl-auto: validate
```

```java
// Fetch fixes
@EntityGraph(attributePaths = {"items"})
@Query("SELECT DISTINCT o FROM Order o JOIN FETCH o.items WHERE o.id = :id")
@BatchSize(size = 25)  // on collection field

// Locking
@Lock(LockModeType.PESSIMISTIC_WRITE)
@Version  // optimistic
```

### 21.3 Spring Transaction Quick Reference

```java
@Transactional(readOnly = true)                    // queries → replica OK
@Transactional(isolation = Isolation.REPEATABLE_READ)
@Transactional(propagation = Propagation.REQUIRES_NEW)
@Transactional(rollbackFor = Exception.class)
@Modifying(clearAutomatically = true)              // bulk update queries
```

### 21.4 HikariCP Quick Reference

```yaml
spring.datasource.hikari:
  maximum-pool-size: 20
  connection-timeout: 30000
  max-lifetime: 1800000
  leak-detection-threshold: 0   # 0 = disabled (prod default)
```

**Sizing:** `(instances × pool_size) < max_connections × 0.8`

### 21.5 Migration Quick Reference

```
V{version}__{description}.sql
CREATE INDEX CONCURRENTLY → executeInTransaction=false
Never edit applied migration
Rollback = new forward migration
```

### 21.6 Pagination Quick Reference

| Pattern | SQL | When |
|---------|-----|------|
| Offset | `LIMIT n OFFSET m` | Small tables, admin |
| Keyset | `WHERE (ts,id) < (?,?) ORDER BY ts DESC,id DESC LIMIT n` | Feeds, large tables |
| Slice | Spring `Slice<T>` | No total count needed |

---

## Section 22: Lead Interview Questions — Logical and Production Scenarios

### 22.1 Architecture and Schema (Q1–Q10)

**Q1: Why one database per service?**

Each service owns schema evolution, scaling, and failure isolation. Shared databases create distributed monolith coupling — coordinated migrations, unclear ownership, and blast radius across teams.

**Q2: How do you reference data in another service?**

Store foreign IDs only (`customer_id`), never cross-DB FKs. Enrich via API, cached replica, or CQRS read model updated by events.

**Q3: BIGINT vs UUID for primary keys?**

BIGINT: compact, sequential, fast inserts. UUID: global uniqueness, safe client-generated IDs, no sequence coordination — at cost of index size and random insert fragmentation. UUID v7 improves locality.

**Q4: Why TIMESTAMPTZ over TIMESTAMP?**

Stores absolute instant; displays correctly across zones. TIMESTAMP WITHOUT TIME ZONE is ambiguous for distributed systems and DST.

**Q5: When would you denormalize within a service?**

Read-heavy dashboards, materialized views, or CQRS read models in same DB. Write model stays normalized for integrity.

**Q6: How do soft deletes affect indexing?**

Partial index `WHERE deleted_at IS NULL` keeps index small and matches active-row queries. Hibernate `@SQLRestriction` applies filter automatically.

**Q7: How do you handle money in PostgreSQL + JPA?**

`NUMERIC(19,4)` + Java `BigDecimal`. Never float/double.

**Q8: PostgreSQL ENUM vs VARCHAR for status?**

ENUM: DB-enforced, compact; adding values needs migration. VARCHAR + check or app enum: flexible. Most Spring shops use VARCHAR + `@Enumerated(STRING)`.

**Q9: What's wrong with shared database microservices?**

Coupled release trains, no per-domain scaling, schema ownership disputes, one slow query impacts all services.

**Q10: When is schema-per-service acceptable vs dedicated DB?**

Small org, cost constraints, moderate blast radius acceptance. Regulated or high-scale workloads prefer dedicated database or cluster per service.

### 22.2 Indexing and Queries (Q11–Q20)

**Q11: How do you choose composite index column order?**

Equality columns first, then range, then ORDER BY columns. Index `(customer_id, created_at DESC)` for `WHERE customer_id = ? ORDER BY created_at DESC`.

**Q12: What is a partial index and when use it?**

Index subset of rows matching predicate — e.g., `WHERE status = 'PENDING'` on outbox. Smaller, faster, less write amplification.

**Q13: EXPLAIN shows Seq Scan on 10M row table — first steps?**

Check WHERE clause selectivity, existing indexes, `ANALYZE` freshness, and whether partial/covering index fits query pattern.

**Q14: Estimated rows vs actual rows mismatch — meaning?**

Stale statistics. Run `ANALYZE`; consider higher statistics target for skewed columns.

**Q15: What is index-only scan?**

Query columns all in index INCLUDE list; heap fetch avoided if visibility map current. Requires regular VACUUM.

**Q16: When use GIN vs B-tree?**

B-tree: equality, range, sorting. GIN: JSONB containment, full-text, arrays.

**Q17: How find unused indexes?**

`pg_stat_user_indexes` where `idx_scan = 0` over meaningful period; verify against rare reporting queries before drop.

**Q18: N+1 at SQL level in EXPLAIN?**

Nested loop with inner seq scan repeated — missing index on FK or ORM N+1 loading lazy collections.

**Q19: How monitor slow queries in production?**

pg_stat_statements, APM JDBC spans, `log_min_duration_statement`, Hibernate slow query log threshold.

**Q20: Can you use OFFSET pagination at scale?**

Poor beyond few thousand offset — keyset/cursor pagination on indexed sort key is production default for feeds.

### 22.3 Transactions, Locks, Concurrency (Q21–Q30)

**Q21: Explain MVCC in one minute.**

Each row version visible to snapshots by transaction ID. Readers don't block writers. Dead tuples need VACUUM. Long transactions cause bloat.

**Q22: Default isolation in PostgreSQL and Spring?**

Read Committed. Each statement sees committed data as of statement start.

**Q23: When use REPEATABLE READ or SERIALIZABLE?**

Financial invariants, report consistency within TX. Serializable strongest; expect retries on serialization failure (40001).

**Q24: What causes deadlocks and how prevent?**

Circular lock wait on rows. Consistent lock ordering, smaller TX, optimistic locking, retry on `CannotAcquireLockException`.

**Q25: SELECT FOR UPDATE vs optimistic @Version?**

Pessimistic: locks row, prevents concurrent write — use for inventory. Optimistic: no lock on read, fails on conflicting update — better for low contention.

**Q26: What does SKIP LOCKED do?**

Skips rows locked by other transactions — work queue pattern for outbox relay without blocking.

**Q27: Why keep transactions short?**

Holds connections, row locks, blocks VACUUM, increases deadlock and pool exhaustion risk. No external I/O inside TX.

**Q28: What is idle in transaction and why dangerous?**

Connection open after BEGIN but app idle — holds locks, blocks VACUUM, eats pool. Set `idle_in_transaction_session_timeout`.

**Q29: When use advisory locks?**

App-level mutex: single-flight job, migration guard, one relay per partition — without locking business rows.

**Q30: Can you publish to Kafka inside @Transactional?**

Risky — DB may commit but Kafka fails (or vice versa). Use transactional outbox in same DB transaction; relay publishes after commit.

### 22.4 JPA, Hibernate, Spring Data (Q31–Q40)

**Q31: What is open-in-view and why disable?**

`OpenEntityManagerInViewFilter` keeps session open through HTTP render — hides LazyInitializationException but encourages N+1 and long sessions. Set `spring.jpa.open-in-view=false`.

**Q32: Fix N+1 for order list with items?**

DTO without items, `@BatchSize`, JOIN FETCH (single association), or aggregate COUNT query — depends on UI need.

**Q33: Difference between persist and merge?**

`persist`: new entity, must be transient. `merge`: detached entity → copy to managed. `save()` in Spring Data handles both via `SimpleJpaRepository`.

**Q34: Why flush before commit?**

Synchronize persistence context to DB — constraint checks, ID generation visibility, ordering with native queries in same TX.

**Q35: How batch inserts work in Hibernate?**

`hibernate.jdbc.batch_size`, `order_inserts`, PostgreSQL `reWriteBatchedInserts=true`. Flush/clear periodically in large imports.

**Q36: When use native query over JPQL?**

JSONB operators, `FOR UPDATE SKIP LOCKED`, PostgreSQL-specific optimizations, complex CTEs.

**Q37: What does @Modifying do?**

Executes UPDATE/DELETE JPQL/SQL — bypasses dirty checking. Needs `@Transactional` and often `clearAutomatically`.

**Q38: How Spring Data pagination works internally?**

Adds `LIMIT/OFFSET` (or keyset in custom impl). `Page` adds COUNT query; `Slice` does not.

**Q39: Entity vs DTO in API layer?**

DTO/projections prevent lazy load leaks, over-fetching, and accidental exposure of internal fields. Entities stay in service/persistence layer.

**Q40: validate vs update for ddl-auto?**

Prod: `validate` or `none`. Dev: `update` optionally. Schema via Flyway — never Hibernate auto-update in prod.

### 22.5 Production Scenarios (Q41–Q45)

**Q41: Deploy causes connection pool timeout — diagnosis?**

Check Hikari metrics, pg_stat_activity by app name, idle-in-transaction count, recent code adding long @Transactional or missing connection release. leak-detection-threshold in staging.

**Q42: Outbox relay duplicates messages — how?**

Claim and mark PROCESSING not atomic, or relay commits before Kafka ack and reprocesses. Fix: same TX for claim+update; idempotent consumer; unique event ID.

**Q43: Read replica returns stale data after create — fix?**

Read-your-writes: route post-mutation reads to primary, return created DTO from write response, or client-side retry with backoff.

**Q44: Migration locked production table 20 minutes — prevention?**

`CREATE INDEX CONCURRENTLY`, add nullable column first, expand/contract pattern, run heavy DDL in maintenance window with init container gating deploy.

**Q45: pg_stat_statements shows ORM query with 50 joins — action?**

Likely cartesian product from multiple JOIN FETCH bags. Split queries, use @BatchSize, or DTO projection.

### 22.6 Rapid-Fire One-Liners (Q46–Q50)

**Q46: HikariCP pool size per instance?** Start 10–20; total across instances must stay under PostgreSQL `max_connections` budget.

**Q47: Best pagination for infinite scroll?** Keyset/cursor on indexed `(sort_col, id)` — not OFFSET.

**Q48: JSONB index for `@>` queries?** GIN with `jsonb_path_ops` or `jsonb_ops`.

**Q49: Idempotency key storage duration?** 24–72 hours TTL with scheduled cleanup; unique constraint on key.

**Q50: First thing in EXPLAIN ANALYZE?** Compare estimated vs actual rows; check for Seq Scan, nested loop disasters, and buffer hits.


---

## Section 23: How to Talk About PostgreSQL and JPA in an Interview

### 23.1 The 60-Second Pitch

> "I treat PostgreSQL as the **system of record** per microservice — one database per service, migrations via Flyway, no cross-service foreign keys. On the Java side I keep transactions **short**: business write plus outbox in one `@Transactional`, never HTTP or Kafka inside the transaction unless it's the outbox table.
>
> For performance I lean on **EXPLAIN ANALYZE** and `pg_stat_statements`, partial and covering indexes, keyset pagination, and fixing N+1 with projections or fetch plans — not `open-in-view`. Connection pools are sized so total connections across pods stay under PostgreSQL limits, and I watch HikariCP pending threads and `idle in transaction` as early warnings.
>
> For distributed patterns I use **idempotency keys**, transactional **outbox** with `SKIP LOCKED` relay, and sagas with durable state in PostgreSQL — at-least-once everywhere, exactly-once side effects via deduplication at consumers."

### 23.2 Framing Trade-offs (Shows Seniority)

| Topic | Don't say | Say instead |
|-------|-----------|-------------|
| ORM | "Hibernate is slow" | "ORM is fine when fetch plan matches access pattern; I use projections and native SQL for hot paths" |
| Microservices DB | "Never share data" | "No shared schema; replicate via events when needed — eventual consistency with explicit UX" |
| Indexes | "Add index on everything" | "Index for proven query patterns; partial indexes for hot subsets; remove unused indexes" |
| Transactions | "Always @Transactional" | "Short boundaries, outbox for cross-system effects, optimistic lock for low contention" |
| Replicas | "Reads go to replica" | "Replicas for analytics; critical reads primary or read-your-writes strategy" |

### 23.3 Story Templates (STAR)

**Performance win:**

> "Order history API p95 was 2s — pg_stat_statements showed OFFSET pagination at page 200. We switched to keyset cursor on `(customer_id, created_at, id)` with covering index; p95 dropped to 80ms."

**Incident:**

> "Connection pool exhaustion during peak — `idle in transaction` from OSIV plus slow external call inside service method. We set `open-in-view: false`, moved HTTP out of transaction, killed stale sessions; pools stabilized."

**Distributed correctness:**

> "Payment double-charge on retry — added idempotency table with unique `(key, scope)` in same transaction as payment row; retries return cached response."

### 23.4 Questions to Ask the Interviewer

- One DB per service or shared cluster boundaries?
- Outbox vs Debezium CDC for events?
- Read replica routing strategy?
- Flyway ownership — per service or platform?
- PostgreSQL version and managed provider?

### 23.5 Red Flags to Avoid in Answers

- "We use `ddl-auto=update` in prod"
- "Lazy loading works — we have `@Transactional` on controller"
- "We publish to Kafka inside `@Transactional`" (without outbox)
- "OFFSET pagination is fine for our 50M row table"
- "Pool size 100 per instance — Postgres handles it"

### 23.6 Plain English Glossary

| Term | Plain English |
|------|---------------|
| MVCC | Readers don't block writers; old row versions stick around until cleanup |
| N+1 | One query becomes hundreds because ORM loads related data lazily one row at a time |
| Keyset pagination | "Give me items after this cursor" instead of "skip 10,000 rows" |
| Outbox | Write the event to a table in the same commit as business data; separate process publishes to Kafka |
| SKIP LOCKED | Workers grab different rows without waiting on each other |
| Connection pool | Reuse DB connections — opening one per request exhausts the database |
| Read replica lag | Copy of database is seconds behind — new data may not show yet |
| Idempotency key | Same request ID twice → same result, no double charge |
| Flyway | Versioned SQL migration scripts applied automatically |
| Bloat | Dead row versions make tables bigger and slower until VACUUM |
| Covering index | Index holds all columns the query needs so the DB skips reading the full table |
| Deadlock | Two transactions waiting on each other — PostgreSQL kills one |
| HikariCP | Fast JDBC connection pool used by default in Spring Boot |
| JSONB | Binary JSON in PostgreSQL — queryable and indexable |
| Pessimistic lock | `SELECT FOR UPDATE` — hold row until commit |
| Optimistic lock | `@Version` column — fail update if someone else changed row first |

### 23.7 Plain English — Common Interview Prompts

**"How do microservices share data?"**

Each service keeps its own database. If the order service needs a customer name, it either calls the customer API, stores a copy updated by events, or shows only the customer ID. No shared tables.

**"What goes wrong with lazy loading?"**

Hibernate loads an order in one query. When the code asks for line items, it runs another query per order — 100 orders means 101 queries. Fix by fetching what you need upfront or returning a DTO without hidden database calls.

**"Why not publish to Kafka in the same method that saves to DB?"**

The database might commit but Kafka fails — or Kafka succeeds and the database rolls back. You get inconsistent systems. The outbox pattern writes the message to a table in the **same** database transaction; a separate relay sends it to Kafka later.

**"Why is my connection pool empty?"**

Connections are stuck in long transactions, leaked because a stream wasn't closed, or you have too many app instances each opening too many connections. Check `pg_stat_activity` and HikariCP metrics.

**"When would you not use JPA?"**

Bulk ETL, complex reporting with window functions, heavy analytics, or when EXPLAIN shows ORM generates bad SQL. Use JDBC, jOOQ, or native queries for those paths; keep JPA for domain CRUD.

---

## Section 24: Appendix — Decision Trees and Quick Reference

### 24.1 Index Decision Tree

```
Query slow?
├─ EXPLAIN shows Seq Scan on large table?
│  ├─ Filter on subset? → partial index
│  ├─ Few columns returned? → covering index INCLUDE
│  ├─ JSONB containment? → GIN index
│  └─ Function on column? → expression index
├─ Wrong row estimate? → ANALYZE, extended stats
└─ Index exists but unused? → predicate mismatch or wrong column order
```

### 24.2 N+1 Fix Decision Tree

```
N+1 detected?
├─ List API, few fields? → DTO projection
├─ Detail, one association? → JOIN FETCH / EntityGraph
├─ Many call sites? → @BatchSize + default_batch_fetch_size
├─ Report / millions of rows? → JDBC / jOOQ, not entity graph
└─ Read-heavy separate shape? → CQRS read model
```

### 24.3 Transaction Boundary Decision Tree

```
Side effect needed?
├─ Same DB only? → @Transactional service method
├─ Message broker? → outbox table same transaction
├─ External HTTP? → AFTER_COMMIT or saga step (not in DB tx)
├─ Read only? → @Transactional(readOnly=true), replica OK
└─ Hot row contention? → @Version or FOR UPDATE (short tx)
```

### 24.4 Pagination Decision Tree

```
Pagination needed?
├─ User jumps to arbitrary page? → offset (small data only)
├─ Feed / infinite scroll? → keyset cursor
├─ Need total count? → separate expensive COUNT or approximate
└─ Deep pages performance critical? → keyset mandatory
```

### 24.5 PostgreSQL vs MySQL (Interview Quick Compare)

| Aspect | PostgreSQL | MySQL (InnoDB) |
|--------|------------|----------------|
| MVCC | Row versions, vacuum | Undo log |
| JSON | JSONB binary + GIN | JSON functional |
| Partial indexes | Yes | Limited |
| SKIP LOCKED | Yes | Yes (8+) |
| Replication | Streaming physical/logical | Binlog async/sync |
| Typical Java shop | Strong default for new microservices | Legacy adjacency |

### 24.6 Spring Boot Data Stack Map

```
Controller
  → Service (@Transactional)
    → Spring Data JPA Repository
      → Hibernate Session / EntityManager
        → HikariCP
          → PostgreSQL JDBC Driver
            → PostgreSQL
```

Platform starter wiring: [custom-spring-boot-starter-expert-playbook.md](custom-spring-boot-starter-expert-playbook.md).

### 24.7 Cross-Playbook Pattern Map

| Pattern | PostgreSQL role | Playbook |
|---------|-----------------|----------|
| Transactional outbox | `outbox_events` + SKIP LOCKED | [outbox-expert-playbook.md](outbox-expert-playbook.md) |
| CQRS | Write DB + read store / projector | [cqrs-expert-playbook.md](cqrs-expert-playbook.md) |
| Saga | `saga_instances` durable state | [saga-expert-playbook.md](saga-expert-playbook.md) |
| Platform JDBC config | HikariCP, Flyway auto-config | [custom-spring-boot-starter-expert-playbook.md](custom-spring-boot-starter-expert-playbook.md) |
| Pool / slow query alerts | Micrometer metrics | [metrics-observability-playbook.md](metrics-observability-playbook.md) |

### 24.8 Production Readiness Checklist

- [ ] One DB per service, least-privilege DB user
- [ ] Flyway migrations, ddl-auto validate/none
- [ ] HikariCP sized with cluster connection budget
- [ ] statement_timeout and lock_timeout configured
- [ ] pg_stat_statements enabled
- [ ] Key indexes on FK columns and query filters
- [ ] Outbox partial index on PENDING
- [ ] Idempotency on write APIs
- [ ] open-in-view false
- [ ] Batch insert settings for bulk paths
- [ ] Read replica routing with read-your-writes strategy
- [ ] Runbook for pool exhaustion, locks, lag
- [ ] Testcontainers PostgreSQL in CI

### 24.9 Version Notes

| PostgreSQL | Feature |
|------------|---------|
| 11+ | Faster ADD COLUMN with DEFAULT |
| 12+ | CREATE INDEX CONCURRENTLY improvements |
| 13+ | B-tree duplicate data removal |
| 14+ | Multirange types |
| 15+ | MERGE statement |
| 16+ | Logical replication from standby |

| Spring Boot / Hibernate | Feature |
|-------------------------|---------|
| Boot 3.x | Jakarta EE, Hibernate 6 |
| Hibernate 6 | `@JdbcTypeCode`, `@SQLRestriction` |
| Spring Data 3.2+ | Scroll/keyset APIs |

---

*End of PostgreSQL + JDBC/JPA/Hibernate Expert Revision Playbook. Pair with [outbox-expert-playbook.md](outbox-expert-playbook.md), [cqrs-expert-playbook.md](cqrs-expert-playbook.md), [saga-expert-playbook.md](saga-expert-playbook.md), and [custom-spring-boot-starter-expert-playbook.md](custom-spring-boot-starter-expert-playbook.md) for distributed Java architecture interviews.*

