# Spring Boot 3 — Production Revision Playbook (Java Lead, Interviews, Microservices)

> **Revision guide** — scan sections before interviews or on-call. Deep production depth with plain language. Targets Java Lead, Staff Engineer, and Architect roles shipping Spring Boot 3 on Kubernetes.

A comprehensive end-to-end reference covering `@Transactional` pitfalls, JPA/Hibernate performance, HikariCP sizing, validation, configuration, Actuator health probes, error handling, graceful shutdown, async pitfalls, OAuth2 resource server basics, testing with Testcontainers, production runbooks, and 40+ lead-level interview Q&As. Sourced from Spring Boot 3.x docs, Hibernate 6 guides, production war stories, and patterns in this repo.

**Related playbooks in this repo:**

- [Kubernetes Expert Playbook](kubernetes-expert-playbook.md) — probes, graceful shutdown, JVM memory on K8s, deployment strategies
- [Metrics & Observability Playbook](metrics-observability-playbook.md) — Micrometer, Prometheus, RED/USE, SLOs for Spring Boot apps
- [Transactional Outbox Expert Playbook](outbox-expert-playbook.md) — dual-write problem, `@Transactional` + outbox in one DB transaction
- [CQRS Expert Playbook](cqrs-expert-playbook.md) — read/write model separation in Spring services
- [Custom Spring Boot Starter Expert Playbook](custom-spring-boot-starter-expert-playbook.md) — platform auto-configuration for shared production wiring
- [Circuit Breaker Expert Playbook](circuit-breaker-expert-playbook.md) — Resilience4j fail-fast when dependencies fail
- [Bulkhead Expert Playbook](bulkhead-expert-playbook.md) — isolate thread pools and connection pools per dependency
- [Kafka Expert Playbook](kafka-expert-playbook.md) — event-driven Spring Boot consumers and producers

**Runnable demo:** `examples/custom-spring-boot-starter/` — minimal Spring Boot 3 consumer with auto-configuration patterns.

---

## Table of Contents

1. [Spring Boot 3 Production Mindset — What "Production Ready" Means](#section-1-spring-boot-3-production-mindset--what-production-ready-means)
2. [@Transactional Deep Dive — Propagation, Rollback, Self-Invocation](#section-2-transactional-deep-dive--propagation-rollback-self-invocation)
3. [Transaction Boundaries in Microservices — Sagas, Outbox, Idempotency](#section-3-transaction-boundaries-in-microservices--sagas-outbox-idempotency)
4. [JPA/Hibernate — N+1, Fetch Join, EntityGraph, Batch Size](#section-4-jpahibernate--n1-fetch-join-entitygraph-batch-size)
5. [Lazy Loading, Optimistic Locking, Pessimistic Locking](#section-5-lazy-loading-optimistic-locking-pessimistic-locking)
6. [HikariCP Connection Pool Sizing with JPA](#section-6-hikaricp-connection-pool-sizing-with-jpa)
7. [Validation — @Valid, Groups, Custom Validators](#section-7-validation--valid-groups-custom-validators)
8. [Configuration — Profiles, @ConfigurationProperties, Secrets](#section-8-configuration--profiles-configurationproperties-secrets)
9. [Spring Boot Actuator — Health Groups, K8s Probes, Metrics](#section-9-spring-boot-actuator--health-groups-k8s-probes-metrics)
10. [Error Handling — @ControllerAdvice, Problem Details (RFC 7807)](#section-10-error-handling--controlleradvice-problem-details-rfc-7807)
11. [Graceful Shutdown and Lifecycle](#section-11-graceful-shutdown-and-lifecycle)
12. [@Async Pitfalls — Thread Pools, Transactions, Context Loss](#section-12-async-pitfalls--thread-pools-transactions-context-loss)
13. [Security Overview — OAuth2 Resource Server Sketch](#section-13-security-overview--oauth2-resource-server-sketch)
14. [Testing — @SpringBootTest vs Slice Tests, Testcontainers PostgreSQL](#section-14-testing--springboottest-vs-slice-tests-testcontainers-postgresql)
15. [REST API, Pagination, and OpenAPI in Production](#section-15-rest-api-pagination-and-openapi-in-production)
16. [Logging, Correlation IDs, and Structured Logs](#section-16-logging-correlation-ids-and-structured-logs)
17. [Database Migrations — Flyway/Liquibase in Production](#section-17-database-migrations--flywayliquibase-in-production)
18. [Five-Layer Production Debugging Framework for Spring Boot](#section-18-five-layer-production-debugging-framework-for-spring-boot)
19. [Production Scenario Runbook — 25+ Scenarios](#section-19-production-scenario-runbook--25-scenarios)
20. [Spring Boot on Kubernetes — Cross-Cutting Concerns](#section-20-spring-boot-on-kubernetes--cross-cutting-concerns)
21. [Revision Cheat Sheets — Properties, Annotations, Quick Tables](#section-21-revision-cheat-sheets--properties-annotations-quick-tables)
22. [Lead Interview Questions — Logical and Production Scenarios](#section-22-lead-interview-questions--logical-and-production-scenarios)
23. [How to Talk About Spring Boot in Production in an Interview](#section-23-how-to-talk-about-spring-boot-in-production-in-an-interview)
24. [Appendix — Decision Trees and Production Readiness Checklist](#section-24-appendix--decision-trees-and-production-readiness-checklist)

---

## Section 1: Spring Boot 3 Production Mindset — What "Production Ready" Means

### 1.1 Spring Boot Is Not "Just CRUD"

Spring Boot accelerates development, but production readiness is **operational**: predictable failure modes, observable behavior, safe deploys, and data consistency under concurrency.

```
┌─────────────────────────────────────────────────────────────────┐
│                    PRODUCTION SPRING BOOT APP                   │
├─────────────┬─────────────┬─────────────┬───────────────────────┤
│   HTTP/API  │  Domain +   │ Persistence │  Platform (K8s,       │
│   Layer     │  Services   │  JPA/JDBC   │  metrics, secrets)    │
├─────────────┴─────────────┴─────────────┴───────────────────────┤
│  Cross-cutting: security, validation, transactions, async,      │
│  error handling, graceful shutdown, correlation IDs             │
└─────────────────────────────────────────────────────────────────┘
```

**Interview one-liner:** "Spring Boot gives you defaults; production is about choosing the right overrides — pool sizes, transaction boundaries, health probes, and failure semantics."

### 1.2 Spring Boot 3 Baseline (Know This Cold)

| Change from Boot 2 | Production impact |
|--------------------|-------------------|
| **Java 17+** required | Virtual threads optional (Boot 3.2+); records, sealed types |
| **Jakarta EE 9+** (`jakarta.*`) | All `javax.persistence` → `jakarta.persistence` |
| **Hibernate 6** | Stricter typing, changed SQL generation; test migrations |
| **Native compilation** optional | GraalVM — not default for most Java shops |
| **Observability API** | Micrometer Observation unifies metrics + tracing |
| **Problem Details** support | `spring.mvc.problemdetails.enabled=true` |

### 1.3 Layered Architecture in Production

```java
@RestController
@RequestMapping("/api/v1/orders")
@Validated
public class OrderController {

    private final OrderApplicationService orderService;

    public OrderController(OrderApplicationService orderService) {
        this.orderService = orderService;
    }

    @PostMapping
    public ResponseEntity<OrderResponse> placeOrder(@Valid @RequestBody PlaceOrderRequest request) {
        OrderResponse response = orderService.placeOrder(request);
        return ResponseEntity.status(HttpStatus.CREATED).body(response);
    }
}

@Service
public class OrderApplicationService {

    private final OrderRepository orderRepository;
    private final OutboxEventPublisher outboxPublisher;

    @Transactional
    public OrderResponse placeOrder(PlaceOrderRequest request) {
        Order order = Order.create(request);
        orderRepository.save(order);
        outboxPublisher.publish(new OrderPlacedEvent(order.getId()));
        return OrderResponse.from(order);
    }
}
```

**Rules:**

- **Controller** — HTTP mapping, validation trigger, status codes. No business logic.
- **Service** — transaction boundary, orchestration, domain rules.
- **Repository** — persistence only. No cross-aggregate business rules.

See [outbox-expert-playbook.md](outbox-expert-playbook.md) for atomic domain + event writes.

### 1.4 What Breaks First in Production

| Symptom | Common Spring Boot root cause |
|---------|------------------------------|
| 503 under load | HikariCP pool exhausted, thread pool saturated |
| Slow API after deploy | N+1 queries, missing index, fetch join abuse |
| Random 500s | Unchecked exceptions, lazy init outside session |
| Data corruption | Wrong `@Transactional` propagation, no optimistic lock |
| Duplicate side effects | Retries without idempotency, async after commit race |
| OOM on K8s | JVM heap vs container limit mismatch |
| Stuck pods on deploy | Missing graceful shutdown, long-running requests |

### 1.5 Production Configuration Layers

```
1. application.yml           — defaults safe for all envs
2. application-{profile}.yml — env-specific (staging, prod)
3. Env vars / K8s ConfigMap  — non-secret overrides
4. K8s Secrets / Vault       — credentials, API keys
5. Feature flags             — runtime toggles without redeploy
```

Never commit secrets. Use `@ConfigurationProperties` with validation — Section 8.

### 1.6 Dependency Management

Pin Spring Boot BOM via parent POM or `spring-boot-dependencies`. Align:

- Hibernate ORM with Boot-managed version
- PostgreSQL driver
- Testcontainers BOM
- Resilience4j (if used) with Boot 3 starter

**Anti-pattern:** Mixing Spring Framework versions manually across modules.

---

## Section 2: @Transactional Deep Dive — Propagation, Rollback, Self-Invocation

### 2.1 How Spring Transactions Actually Work

Spring uses **AOP proxies**. `@Transactional` on a method is intercepted by a proxy that:

1. Opens (or joins) a transaction
2. Calls your method
3. Commits on success or rolls back on configured exceptions

```
Client → OrderController → OrderService$$SpringProxy → OrderServiceImpl.placeOrder()
                                    │
                                    └── TransactionInterceptor (begin/commit/rollback)
```

**Critical:** The proxy wraps the **bean**, not arbitrary internal calls.

### 2.2 Propagation — When to Use What

| Propagation | Behavior | Production use |
|-------------|----------|----------------|
| **REQUIRED** (default) | Join existing TX or create new | 95% of service methods |
| **REQUIRES_NEW** | Suspend current TX; always new TX | Audit log that must commit even if outer rolls back |
| **NESTED** | Savepoint within outer TX | Rare; DB must support savepoints |
| **MANDATORY** | Must run inside existing TX | Enforce caller provides TX; fails fast if not |
| **SUPPORTS** | Join if exists, else non-transactional | Read-only helpers |
| **NOT_SUPPORTED** | Suspend TX | Long-running non-DB work inside TX boundary |
| **NEVER** | Fail if TX exists | Guard methods that must not run in TX |

```java
@Service
public class AuditService {

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void recordFailure(String orderId, String reason) {
        auditRepository.save(new AuditEntry(orderId, reason));
        // Commits independently — survives outer rollback
    }
}

@Service
public class OrderApplicationService {

    private final OrderRepository orderRepository;
    private final AuditService auditService;

    @Transactional
    public void placeOrder(PlaceOrderRequest request) {
        try {
            orderRepository.save(Order.create(request));
            throw new RuntimeException("payment gateway timeout");
        } catch (RuntimeException ex) {
            auditService.recordFailure(request.orderId(), ex.getMessage());
            throw ex; // outer TX rolls back; audit row remains
        }
    }
}
```

**Interview trap:** `REQUIRES_NEW` opens a **new connection** from the pool — can exhaust HikariCP under error storms.

### 2.3 rollbackFor and rollback Rules

**Default:** Roll back on **unchecked** exceptions (`RuntimeException`, `Error`). **Do not** roll back on checked exceptions unless configured.

```java
@Transactional(rollbackFor = Exception.class)
public void importOrders(InputStream csv) throws IOException {
    // Checked IOException now triggers rollback
}

@Transactional(noRollbackFor = BusinessWarningException.class)
public void processWithWarnings() {
    // BusinessWarningException commits the TX
}
```

**Production bug:** Service catches `Exception`, logs, returns 200 — transaction **commits** partial work.

```java
// BAD — swallows failure, TX commits
@Transactional
public void updateInventory(String sku, int qty) {
    try {
        inventoryRepository.decrement(sku, qty);
    } catch (Exception e) {
        log.error("failed", e);
        // no rethrow — TX commits!
    }
}

// GOOD — let exception propagate or mark rollback
@Transactional
public void updateInventory(String sku, int qty) {
    inventoryRepository.decrement(sku, qty);
}
```

Use `TransactionAspectSupport.currentTransactionStatus().setRollbackOnly()` when you must handle locally but still roll back.

### 2.4 Self-Invocation — The #1 @Transactional Bug

Calling `@Transactional` method from **another method on the same class** bypasses the proxy:

```java
@Service
public class OrderService {

    public void placeOrderExternal(PlaceOrderRequest req) {
        // NO TRANSACTION — proxy not involved
        this.placeOrderInternal(req);
    }

    @Transactional
    public void placeOrderInternal(PlaceOrderRequest req) {
        orderRepository.save(Order.create(req));
    }
}
```

**Fixes:**

1. **Move** transactional method to another bean (preferred).
2. **Inject self** via `@Lazy` proxy (works but smells).
3. Use `TransactionTemplate` programmatically.

```java
@Service
public class OrderService {

    private final OrderRepository orderRepository;
    private final TransactionTemplate transactionTemplate;

    public OrderService(OrderRepository orderRepository,
                        PlatformTransactionManager txManager) {
        this.orderRepository = orderRepository;
        this.transactionTemplate = new TransactionTemplate(txManager);
    }

    public void placeOrderExternal(PlaceOrderRequest req) {
        transactionTemplate.executeWithoutResult(status ->
            orderRepository.save(Order.create(req))
        );
    }
}
```

**Detection:** Enable `logging.level.org.springframework.transaction=DEBUG` in staging — missing "Creating new transaction" log on expected path.

### 2.5 readOnly = true

```java
@Transactional(readOnly = true)
public OrderDetails getOrder(UUID id) {
    return orderRepository.findById(id)
        .map(OrderDetails::from)
        .orElseThrow(() -> new OrderNotFoundException(id));
}
```

**Benefits:**

- Hibernate skips dirty checking flush at end of TX
- Hint to DB driver: read-only connection (PostgreSQL)
- Documents intent

**Pitfalls:**

- `readOnly=true` with write operations — undefined behavior; may fail at flush or silently misbehave depending on DB.
- Class-level `@Transactional(readOnly=true)` with one write method — that write method needs `@Transactional(readOnly=false)` explicitly.

```java
@Service
@Transactional(readOnly = true) // default for all methods
public class OrderQueryService {

    public OrderDetails getOrder(UUID id) { /* ... */ }

    @Transactional(readOnly = false)
    public void archiveOrder(UUID id) { /* write */ }
}
```

### 2.6 Transaction Timeout and Isolation

```java
@Transactional(timeout = 5) // seconds — default rollback if exceeded
public void quickUpdate(UUID id) { /* ... */ }

@Transactional(isolation = Isolation.READ_COMMITTED) // PostgreSQL default
public void standardWrite() { /* ... */ }
```

**Production notes:**

- Avoid `SERIALIZABLE` unless you understand retry logic for serialization failures.
- Long TX hold connections — keep service methods short.
- `@Transactional` on controller — **anti-pattern** (TX spans HTTP layer).

### 2.7 @Transactional on Private/Final Methods

**Does not work** — CGLIB/JDK proxy cannot intercept private methods. `final` methods on CGLIB-proxied classes also fail.

Keep `@Transactional` on **public** service methods.

### 2.8 Transaction Synchronization and afterCommit

For side effects that must run **only after successful commit** (email, Kafka without outbox):

```java
@Service
public class NotificationService {

    @Transactional
    public void confirmOrder(UUID orderId) {
        orderRepository.markConfirmed(orderId);

        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                emailClient.sendConfirmation(orderId);
            }
        });
    }
}
```

**Better for events:** [outbox-expert-playbook.md](outbox-expert-playbook.md) — survives crash between commit and publish.

---

## Section 3: Transaction Boundaries in Microservices — Sagas, Outbox, Idempotency

### 3.1 No Distributed Transactions (Usually)

**Two-Phase Commit (2PC)** across PostgreSQL + Kafka + Stripe is not the default microservices pattern. Spring Boot services use:

- **Local transactions** per service
- **Saga** (orchestration/choreography) for cross-service workflows
- **Outbox** for reliable event publish
- **Idempotency keys** for safe retries

```
Order Service                    Payment Service
     │                                │
     │ 1. TX: save order + outbox     │
     │ 2. relay publishes event  ────►│ 3. TX: charge + save payment
     │                                │
     │◄──── PaymentFailed event ──────│ (compensating saga step)
```

See [saga-expert-playbook.md](saga-expert-playbook.md) and [outbox-expert-playbook.md](outbox-expert-playbook.md).

### 3.2 Anti-Pattern: @Transactional Spanning Remote Calls

```java
// BAD — holds DB connection open during HTTP call
@Transactional
public void placeOrder(PlaceOrderRequest req) {
    Order order = orderRepository.save(Order.create(req));
    paymentClient.charge(order.getId(), req.amount()); // 30s timeout = 30s connection held
}
```

**Fix:** Short TX for local write → publish event/outbox → async payment processing.

### 3.3 Idempotency for HTTP APIs

```java
@Entity
@Table(uniqueConstraints = @UniqueConstraint(columnNames = "idempotency_key"))
public class IdempotencyRecord {
    @Id
    private UUID id;
    private String idempotencyKey;
    private String responseBody;
    private HttpStatus httpStatus;
}

@Service
public class IdempotentOrderService {

    @Transactional
    public ResponseEntity<OrderResponse> placeOrder(String idempotencyKey, PlaceOrderRequest req) {
        return idempotencyRepository.findByKey(idempotencyKey)
            .map(record -> ResponseEntity.status(record.getHttpStatus())
                .body(deserialize(record.getResponseBody())))
            .orElseGet(() -> {
                OrderResponse response = doPlaceOrder(req);
                idempotencyRepository.save(IdempotencyRecord.of(idempotencyKey, response));
                return ResponseEntity.status(HttpStatus.CREATED).body(response);
            });
    }
}
```

Client sends `Idempotency-Key` header — Stripe-style pattern.

### 3.4 Read Models vs Write Models

For high-read endpoints, avoid heavy JPA graphs in request path. See [cqrs-expert-playbook.md](cqrs-expert-playbook.md):

- **Write side:** entities, transactions, outbox
- **Read side:** denormalized projections, `@Transactional(readOnly=true)`, optional separate DB

### 3.5 Chained @Transactional Calls Across Beans

```java
@Service
public class OrderFacade {

    private final InventoryService inventoryService;
    private final OrderService orderService;

    // NO @Transactional here — each service owns its boundary
    public void placeOrder(PlaceOrderRequest req) {
        orderService.createPending(req);      // TX 1
        inventoryService.reserve(req.sku());  // TX 2 — separate service, separate TX
    }
}
```

If both must be atomic locally → single service method, single TX. If cross-service → saga compensation.

---

## Section 4: JPA/Hibernate — N+1, Fetch Join, EntityGraph, Batch Size

### 4.1 The N+1 Problem

```java
@Entity
public class Order {
    @OneToMany(mappedBy = "order", fetch = FetchType.LAZY)
    private List<OrderLine> lines;
}

// Controller calls service
List<Order> orders = orderRepository.findAll(); // 1 query
for (Order o : orders) {
    o.getLines().size(); // N queries — one per order
}
```

**Symptoms:** 1 API call → 101 SQL queries. Connection pool spike. P99 latency explosion.

**Detect:**

```yaml
# application-dev.yml — NEVER leave on in prod at INFO
logging.level.org.hibernate.SQL: DEBUG
logging.level.org.hibernate.orm.jdbc.bind: TRACE
```

Or Hibernate statistics / datasource-proxy / p6spy in staging.

Micrometer: watch `jdbc.connections.active` and `http.server.requests` P99 — see [metrics-observability-playbook.md](metrics-observability-playbook.md).

### 4.2 JOIN FETCH

```java
public interface OrderRepository extends JpaRepository<Order, UUID> {

    @Query("""
        SELECT DISTINCT o FROM Order o
        LEFT JOIN FETCH o.lines
        WHERE o.customerId = :customerId
        """)
    List<Order> findWithLinesByCustomerId(@Param("customerId") UUID customerId);
}
```

**Rules:**

- Use `DISTINCT` when collection fetch causes duplicate root rows
- Cannot paginate reliably with collection fetch join — use batch fetching or separate query
- Multiple `JOIN FETCH` on collections → Cartesian product explosion

### 4.3 @EntityGraph

```java
@EntityGraph(attributePaths = {"lines", "customer"})
Optional<Order> findWithDetailsById(UUID id);

@NamedEntityGraph(
    name = "Order.summary",
    attributeNodes = {
        @NamedAttributeNode("lines"),
        @NamedAttributeNode(value = "customer", subgraph = "customerAddress")
    },
    subgraphs = @NamedSubgraph(name = "customerAddress", attributeNodes = @NamedAttributeNode("address"))
)
@Entity
public class Order { /* ... */ }
```

EntityGraph generates fetch plan without JPQL string — cleaner for repositories.

### 4.4 @BatchSize — Hibernate Batch Fetching

```java
@Entity
public class Order {
    @OneToMany(mappedBy = "order")
    @BatchSize(size = 25)
    private List<OrderLine> lines;
}
```

Global default:

```yaml
spring.jpa.properties.hibernate.default_batch_fetch_size: 25
```

Loads lines for up to 25 orders in **one** query instead of N:

```sql
-- IN clause batch
SELECT * FROM order_line WHERE order_id IN (?, ?, ?, ...);
```

**When to use:** Lists of parent entities where JOIN FETCH causes pagination pain.

### 4.5 JDBC Batch Inserts/Updates

```yaml
spring.jpa.properties.hibernate.jdbc.batch_size: 25
spring.jpa.properties.hibernate.order_inserts: true
spring.jpa.properties.hibernate.order_updates: true
spring.jpa.properties.hibernate.jdbc.batch_versioned_data: true
```

Requires **ID generation strategy** that allows batching — `GenerationType.SEQUENCE` preferred over `IDENTITY` (MySQL auto-increment disables batching for inserts).

```java
@Entity
public class OrderLine {
    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "order_line_seq")
    @SequenceGenerator(name = "order_line_seq", sequenceName = "order_line_seq", allocationSize = 50)
    private Long id;
}
```

### 4.6 DTO Projections — Avoid Entity Over-Fetching

```java
public interface OrderSummaryProjection {
    UUID getId();
    Instant getCreatedAt();
    BigDecimal getTotal();
}

@Query("""
    SELECT o.id AS id, o.createdAt AS createdAt, o.total AS total
    FROM Order o WHERE o.customerId = :customerId
    """)
List<OrderSummaryProjection> findSummariesByCustomer(@Param("customerId") UUID customerId);
```

No lazy loading risk — read-only interface projection.

### 4.7 Pagination Done Right

```java
@GetMapping
public Page<OrderSummaryResponse> list(
        @RequestParam(defaultValue = "0") int page,
        @RequestParam(defaultValue = "20") int size) {

    Pageable pageable = PageRequest.of(page, Math.min(size, 100)); // cap page size
    return orderQueryService.findSummaries(pageable);
}
```

**Never** `findAll()` then filter in memory for API endpoints.

---

## Section 5: Lazy Loading, Optimistic Locking, Pessimistic Locking

### 5.1 LazyInitializationException

```
org.hibernate.LazyInitializationException: failed to lazily initialize a collection of role:
Order.lines: could not initialize proxy - no Session
```

**Cause:** Access lazy association **outside** an open persistence context (after `@Transactional` method returns, or in controller without OSIV).

**Fixes (pick intentionally):**

| Approach | Trade-off |
|----------|-----------|
| Fetch join / EntityGraph in query | Best for known use cases |
| `@Transactional` on read method wrapping access | Simple; watch TX length |
| DTO mapping inside TX | Clean API boundary |
| `spring.jpa.open-in-view=false` (recommended prod) | Forces explicit fetch — no hidden N+1 in views |

```yaml
# Production recommendation
spring.jpa.open-in-view: false
```

With OSIV disabled, you **must** fetch data inside service layer — this is correct architecture.

### 5.2 Open Session In View (OSIV)

Default `true` in Spring Boot — session open for entire HTTP request.

**Pros:** Lazy loading works in controller/serialization.
**Cons:** Hides N+1; extends connection hold time through JSON serialization.

**Lead interview answer:** "We disable OSIV in production and fetch explicitly in the service layer — predictable query count per endpoint."

### 5.3 Optimistic Locking with @Version

```java
@Entity
public class InventoryItem {
    @Id
    private UUID id;

    private int quantity;

    @Version
    private Long version;
}

@Service
public class InventoryService {

    @Transactional
    public void decrement(UUID skuId, int amount) {
        InventoryItem item = inventoryRepository.findById(skuId)
            .orElseThrow();
        item.decrement(amount);
        // Hibernate: UPDATE ... SET quantity=?, version=version+1 WHERE id=? AND version=?
    }
}
```

**On conflict:** `OptimisticLockException` / `StaleObjectStateException`.

**Handle:**

```java
@Retryable(retryFor = OptimisticLockException.class, maxAttempts = 3)
public void decrementWithRetry(UUID skuId, int amount) { /* ... */ }
```

Or return 409 Conflict to client with refresh instruction.

### 5.4 Pessimistic Locking

When you must **serialize** access (limited stock, seat booking):

```java
@Lock(LockModeType.PESSIMISTIC_WRITE)
@Query("SELECT i FROM InventoryItem i WHERE i.id = :id")
Optional<InventoryItem> findByIdForUpdate(@Param("id") UUID id);
```

PostgreSQL: `SELECT ... FOR UPDATE`.

**Production warnings:**

- Holds row lock until TX ends — keep TX **short**
- Deadlock risk under concurrent updates — consistent lock ordering
- Do not combine with remote HTTP calls inside same TX

```java
@Transactional
public void reserveStock(UUID skuId, int qty) {
    InventoryItem item = inventoryRepository.findByIdForUpdate(skuId)
        .orElseThrow();
    if (item.getQuantity() < qty) {
        throw new InsufficientStockException(skuId);
    }
    item.decrement(qty);
}
```

### 5.5 Lock Mode Selection Guide

| Scenario | Lock type |
|----------|-----------|
| Concurrent edits to user profile | `@Version` optimistic |
| Flash sale inventory | Pessimistic or atomic SQL `UPDATE ... WHERE qty >= ?` |
| Financial ledger | Optimistic + idempotency or SERIALIZABLE + retry |
| Read-heavy, rare writes | Optimistic |

### 5.6 Atomic Update Without Loading Entity

```java
@Modifying
@Query("""
    UPDATE InventoryItem i SET i.quantity = i.quantity - :qty, i.version = i.version + 1
    WHERE i.id = :id AND i.quantity >= :qty
    """)
int decrementIfAvailable(@Param("id") UUID id, @Param("qty") int qty);

// In service:
if (repository.decrementIfAvailable(id, qty) == 0) {
    throw new InsufficientStockException(id);
}
```

Single round-trip, no lazy loading, minimal lock time.

---

## Section 6: HikariCP Connection Pool Sizing with JPA

### 6.1 Why Pool Exhaustion Is the #1 Production Incident

Every `@Transactional` method borrows a connection from HikariCP. When all connections are checked out, new requests block until `connectionTimeout` — then fail with:

```
java.sql.SQLTransientConnectionException: HikariPool-1 - Connection is not available, request timed out after 30000ms
```

**Symptoms:** 503 errors, thread pile-up on Tomcat worker threads, cascading timeouts across the mesh.

```
Request Thread                    HikariCP Pool (max=10)
     │                                  │
     ├── getConnection() ──────────────►│ conn 1..10 all in use
     │   (blocks 30s)                   │
     └── timeout → 500/503              │
```

See [bulkhead-expert-playbook.md](bulkhead-expert-playbook.md) for isolating pools per dependency.

### 6.2 Default Spring Boot Hikari Settings

```yaml
spring:
  datasource:
    hikari:
      maximum-pool-size: 10          # Boot default
      minimum-idle: 10
      connection-timeout: 30000      # ms — how long to wait for a conn
      idle-timeout: 600000           # 10 min
      max-lifetime: 1800000          # 30 min — rotate before DB/firewall kills
      pool-name: order-service-pool
      leak-detection-threshold: 60000  # staging only — logs stack if held > 60s
```

**Production:** Set `pool-name` per service for clear metrics. Enable `leak-detection-threshold` in staging, not prod (overhead).

### 6.3 Sizing Formula (Starting Point)

```
pool_size ≈ (core_count × 2) + effective_spindle_count   # legacy disk rule

For cloud PostgreSQL (no local spindles):
pool_size ≈ (expected_concurrent_db_requests_per_pod) + small_buffer

More practical:
  connections_per_pod = min( (tomcat_max_threads × avg_db_time_ratio), postgres_max / num_pods )
```

**Example:** 4 pods, PostgreSQL `max_connections=200`, reserve 20 for admin/replication → 180 app connections → **45 per pod max**. If Tomcat `max=200` but only 10% of requests hit DB concurrently → **~20 per pod** is reasonable.

**Interview one-liner:** "I size the pool from PostgreSQL's global limit divided by replicas, then cap by actual concurrent transactional work — not Tomcat thread count."

### 6.4 JPA-Specific Pool Drains

| Cause | Fix |
|-------|-----|
| Long `@Transactional` with HTTP inside | Short TX; outbox/event for async |
| N+1 queries | Fetch join, batch size — Section 4 |
| OSIV holding conn through serialization | `spring.jpa.open-in-view=false` |
| `REQUIRES_NEW` in loops | Batch or single TX |
| Missing `@Transactional(readOnly=true)` on reads | Reduces unnecessary write locks |
| Streaming large `ResultSet` without fetch size | Set `hibernate.jdbc.fetch_size` |

### 6.5 Multi-Datasource Pools

```yaml
spring:
  datasource:
    primary:
      jdbc-url: jdbc:postgresql://primary:5432/orders
      hikari:
        maximum-pool-size: 15
        pool-name: orders-primary
    readonly:
      jdbc-url: jdbc:postgresql://replica:5432/orders
      hikari:
        maximum-pool-size: 25
        pool-name: orders-readonly
        read-only: true
```

Each datasource = separate Hikari pool. Size independently. Read replicas can often be larger.

### 6.6 Monitoring HikariCP

Actuator + Micrometer expose:

| Metric | Alert on |
|--------|----------|
| `hikaricp.connections.active` | Sustained near `maximum-pool-size` |
| `hikaricp.connections.pending` | > 0 for > 30s |
| `hikaricp.connections.timeout` | Any increase |
| `hikaricp.connections.usage` | P99 > 5s (long-held connections) |

See [metrics-observability-playbook.md](metrics-observability-playbook.md).

### 6.7 Connection Validation and RDS Failover

```yaml
spring:
  datasource:
    hikari:
      connection-test-query: SELECT 1   # rarely needed with JDBC4 isValid()
      keepalive-time: 30000             # Boot 3.2+ — ping idle connections
```

After Aurora/RDS failover, stale connections fail fast on next use. Hikari evicts and replaces. Tune `max-lifetime` below DB/proxy idle timeout.

---

## Section 7: Validation — @Valid, Groups, Custom Validators

### 7.1 Validation Layers in Production

```
HTTP Request
    │
    ├── @Valid on @RequestBody     — Bean Validation (JSR 380)
    ├── @Validated on controller   — enables method-level constraints
    ├── @PathVariable @Min/@Max    — parameter constraints
    └── Service-level invariants   — business rules (not annotations)
```

**Rule:** Annotations for **structural** validation; service layer for **business** validation ("coupon expired", "insufficient stock").

### 7.2 Request DTO Validation

```java
public record PlaceOrderRequest(
    @NotNull UUID customerId,
    @NotEmpty @Size(max = 20) List<@Valid OrderLineRequest> lines,
    @Positive BigDecimal totalAmount
) {}

public record OrderLineRequest(
    @NotBlank @Size(max = 64) String sku,
    @Min(1) @Max(999) int quantity
) {}

@RestController
@Validated
public class OrderController {

    @PostMapping("/orders")
    public ResponseEntity<OrderResponse> placeOrder(@Valid @RequestBody PlaceOrderRequest request) {
        return ResponseEntity.status(HttpStatus.CREATED).body(orderService.placeOrder(request));
    }

    @GetMapping("/orders/{id}")
    public OrderResponse getOrder(@PathVariable @NotNull UUID id) {
        return orderService.getOrder(id);
    }
}
```

### 7.3 Validation Groups — Create vs Update

```java
public interface OnCreate {}
public interface OnUpdate {}

public record UpdateCustomerRequest(
    @Null(groups = OnCreate.class)
    @NotNull(groups = OnUpdate.class)
    UUID id,

    @NotBlank(groups = {OnCreate.class, OnUpdate.class})
    String name
) {}

@PostMapping
public CustomerResponse create(@Validated(OnCreate.class) @RequestBody UpdateCustomerRequest req) { /* ... */ }

@PutMapping("/{id}")
public CustomerResponse update(@Validated(OnUpdate.class) @RequestBody UpdateCustomerRequest req) { /* ... */ }
```

### 7.4 Custom Validators

```java
@Target({ElementType.FIELD, ElementType.PARAMETER})
@Retention(RetentionPolicy.RUNTIME)
@Constraint(validatedBy = SkuExistsValidator.class)
public @interface ValidSku {
    String message() default "SKU does not exist";
    Class<?>[] groups() default {};
    Class<? extends Payload>[] payload() default {};
}

@Component
public class SkuExistsValidator implements ConstraintValidator<ValidSku, String> {

    private final ProductCatalogClient catalogClient;

    @Override
    public boolean isValid(String sku, ConstraintValidatorContext context) {
        if (sku == null) return true; // @NotBlank handles null/empty
        return catalogClient.exists(sku);
    }
}
```

**Production warning:** Remote validation in `@ConstraintValidator` adds latency and failure modes. Prefer local DB check or cache for hot paths.

### 7.5 @Validated on @ConfigurationProperties

```java
@ConfigurationProperties(prefix = "app.payment")
@Validated
public record PaymentProperties(
    @NotBlank String apiUrl,
    @Min(1000) @Max(60000) int timeoutMs,
    @NotEmpty List<@NotBlank String> allowedCurrencies
) {}
```

Invalid config → **fail fast at startup**, not at 3 AM under traffic.

### 7.6 Method-Level Validation

```java
@Service
@Validated
public class TransferService {

    public void transfer(
            @NotNull UUID fromAccount,
            @NotNull UUID toAccount,
            @Positive BigDecimal amount) {
        // ...
    }
}
```

Requires `@Validated` on the class (not just `@Valid`).

### 7.7 Validation Error Response Shape

Default: 400 with `MethodArgumentNotValidException` — field errors array. In production, map via `@ControllerAdvice` to RFC 7807 Problem Details — Section 10.

**Never** return 500 for validation failures.

---

## Section 8: Configuration — Profiles, @ConfigurationProperties, Secrets

### 8.1 Profile Strategy

| Profile | Purpose |
|---------|---------|
| `default` | Safe local dev defaults |
| `staging` | Prod-like; real integrations with test credentials |
| `prod` | Production overrides only via env/secrets |
| `test` | `@SpringBootTest`, Testcontainers |

```yaml
# application.yml — shared
spring:
  application:
    name: order-service
  profiles:
    active: ${SPRING_PROFILES_ACTIVE:default}

---
spring:
  config:
    activate:
      on-profile: prod
  jpa:
    open-in-view: false
  datasource:
    hikari:
      maximum-pool-size: ${HIKARI_MAX_POOL_SIZE:20}
```

**Anti-pattern:** `application-prod.yml` with hardcoded passwords.

### 8.2 @ConfigurationProperties vs @Value

```java
// PREFERRED — typed, validated, testable
@ConfigurationProperties(prefix = "app.orders")
@Validated
public record OrderProperties(
    Duration reservationTtl,
    int maxLinesPerOrder,
    boolean outboxEnabled
) {}

@Configuration
@EnableConfigurationProperties(OrderProperties.class)
public class OrderConfig {}
```

```java
// AVOID for clusters of related properties
@Value("${app.orders.max-lines-per-order}")
private int maxLines;
```

**Why:** `@ConfigurationProperties` binds once at startup; supports lists, maps, nested objects, relaxed binding (`max-lines-per-order` ↔ `maxLinesPerOrder`).

### 8.3 Secrets Management

```
┌──────────────┐     ┌─────────────┐     ┌──────────────────┐
│ K8s Secret   │────►│ Env var     │────►│ @Configuration   │
│ / Vault      │     │ DB_PASSWORD │     │ Properties       │
└──────────────┘     └─────────────┘     └──────────────────┘
```

```yaml
spring:
  datasource:
    url: ${DATABASE_URL}
    username: ${DATABASE_USERNAME}
    password: ${DATABASE_PASSWORD}
```

**Never** log secrets. Use Spring Cloud Vault or External Secrets Operator for rotation.

### 8.4 Feature Flags

```java
@ConfigurationProperties(prefix = "feature")
public record FeatureFlags(
    boolean newCheckoutFlow,
    boolean kafkaOutboxRelay
) {}
```

Toggle via ConfigMap without redeploy. For complex flags, integrate LaunchDarkly/Unleash.

### 8.5 Configuration Precedence (Know for Interviews)

1. Devtools global properties (if present)
2. `@TestPropertySource` / `@DynamicPropertySource`
3. Command line args
4. `SPRING_APPLICATION_JSON`
5. OS environment variables
6. `application-{profile}.yml`
7. `application.yml`

Later sources override earlier (within same source type, more specific wins).

### 8.6 Fail-Fast Validation at Startup

```java
@Component
public class StartupValidator implements ApplicationRunner {

    private final DataSource dataSource;

    @Override
    public void run(ApplicationArguments args) throws Exception {
        try (Connection c = dataSource.getConnection()) {
            log.info("Database connectivity verified");
        }
    }
}
```

Or rely on Actuator `readiness` health — Section 9.

---

## Section 9: Spring Boot Actuator — Health Groups, K8s Probes, Metrics

### 9.1 Actuator Endpoints in Production

```yaml
management:
  endpoints:
    web:
      exposure:
        include: health,info,metrics,prometheus
      base-path: /actuator
  endpoint:
    health:
      show-details: when_authorized
      probes:
        enabled: true
  health:
    livenessstate:
      enabled: true
    readinessstate:
      enabled: true
```

**Security:** Do not expose all endpoints publicly. Restrict `/actuator/*` to internal network or auth.

### 9.2 Kubernetes Probes

Spring Boot 3 exposes:

| Endpoint | K8s probe | Purpose |
|----------|-----------|---------|
| `/actuator/health/liveness` | `livenessProbe` | JVM alive; restart if deadlocked |
| `/actuator/health/readiness` | `readinessProbe` | Ready for traffic (DB up, etc.) |
| `/actuator/health/startup` | `startupProbe` | Slow-start apps (optional) |

```yaml
# Kubernetes deployment snippet
livenessProbe:
  httpGet:
    path: /actuator/health/liveness
    port: 8080
  initialDelaySeconds: 30
  periodSeconds: 10
  failureThreshold: 3

readinessProbe:
  httpGet:
    path: /actuator/health/readiness
    port: 8080
  initialDelaySeconds: 10
  periodSeconds: 5
  failureThreshold: 3

startupProbe:
  httpGet:
    path: /actuator/health/readiness
    port: 8080
  failureThreshold: 30
  periodSeconds: 10
```

See [kubernetes-expert-playbook.md](kubernetes-expert-playbook.md).

### 9.3 Custom Health Indicators

```java
@Component
public class PaymentGatewayHealthIndicator implements HealthIndicator {

    private final PaymentClient paymentClient;

    @Override
    public Health health() {
        try {
            paymentClient.ping();
            return Health.up().withDetail("gateway", "reachable").build();
        } catch (Exception ex) {
            return Health.down(ex).withDetail("gateway", "unreachable").build();
        }
    }
}
```

**Production nuance:** Expensive health checks on liveness → restart loops. Put dependency checks on **readiness**, not liveness.

### 9.4 Health Groups

```yaml
management:
  endpoint:
    health:
      group:
        readiness:
          include: readinessState,db,diskSpace
        liveness:
          include: livenessState
        deep:
          include: db,redis,paymentGateway
          show-details: always
```

Custom group for ops deep-check without affecting K8s probes.

### 9.5 Metrics and Prometheus

```xml
<dependency>
    <groupId>io.micrometer</groupId>
    <artifactId>micrometer-registry-prometheus</artifactId>
</dependency>
```

Key RED metrics for Spring Boot:

| Metric | Type |
|--------|------|
| `http.server.requests` | Timer — rate, errors, duration |
| `hikaricp.connections.active` | Gauge |
| `jvm.memory.used` | Gauge |
| `logback.events` | Counter by level |
| `spring.data.repository.invocations` | Timer (if enabled) |

```yaml
management:
  metrics:
    tags:
      application: ${spring.application.name}
      environment: ${spring.profiles.active}
  prometheus:
    metrics:
      export:
        enabled: true
```

### 9.6 Info Endpoint

```yaml
management:
  info:
    env:
      enabled: true
info:
  app:
    version: '@project.version@'
    git-commit: ${GIT_COMMIT:unknown}
```

Useful for verifying deployed artifact version during incidents.

---

## Section 10: Error Handling — @ControllerAdvice, Problem Details (RFC 7807)

### 10.1 Global Exception Handling

```java
@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(OrderNotFoundException.class)
    public ResponseEntity<ProblemDetail> handleNotFound(OrderNotFoundException ex, HttpServletRequest request) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.NOT_FOUND, ex.getMessage());
        problem.setTitle("Order Not Found");
        problem.setType(URI.create("https://api.example.com/problems/order-not-found"));
        problem.setInstance(URI.create(request.getRequestURI()));
        problem.setProperty("orderId", ex.getOrderId());
        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(problem);
    }

    @ExceptionHandler(InsufficientStockException.class)
    public ResponseEntity<ProblemDetail> handleInsufficientStock(InsufficientStockException ex) {
        ProblemDetail problem = ProblemDetail.forStatus(HttpStatus.CONFLICT);
        problem.setDetail(ex.getMessage());
        problem.setTitle("Insufficient Stock");
        return ResponseEntity.status(HttpStatus.CONFLICT).body(problem);
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ProblemDetail> handleValidation(MethodArgumentNotValidException ex) {
        ProblemDetail problem = ProblemDetail.forStatus(HttpStatus.BAD_REQUEST);
        problem.setTitle("Validation Failed");
        List<Map<String, String>> errors = ex.getBindingResult().getFieldErrors().stream()
            .map(fe -> Map.of("field", fe.getField(), "message", fe.getDefaultMessage()))
            .toList();
        problem.setProperty("errors", errors);
        return ResponseEntity.badRequest().body(problem);
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ProblemDetail> handleUnexpected(Exception ex, HttpServletRequest request) {
        log.error("Unhandled exception on {} {}", request.getMethod(), request.getRequestURI(), ex);
        ProblemDetail problem = ProblemDetail.forStatus(HttpStatus.INTERNAL_SERVER_ERROR);
        problem.setTitle("Internal Server Error");
        problem.setDetail("An unexpected error occurred"); // never leak stack trace
        return ResponseEntity.internalServerError().body(problem);
    }
}
```

### 10.2 Enable RFC 7807 Problem Details

```yaml
spring:
  mvc:
    problemdetails:
      enabled: true
```

Spring Boot 3 generates `ProblemDetail` for standard errors (404, 405, etc.) automatically when enabled.

### 10.3 Domain Exceptions vs HTTP in Services

```java
// GOOD — service throws domain exception
public class OrderNotFoundException extends RuntimeException {
    private final UUID orderId;
    // ...
}

// BAD — service returns ResponseEntity or sets HTTP codes
```

Keep services HTTP-agnostic. Map exceptions in `@ControllerAdvice`.

### 10.4 Error Response Rules for Production

| Scenario | Status | Body |
|----------|--------|------|
| Validation failure | 400 | Field errors |
| Missing auth | 401 | Problem detail, no hint |
| Forbidden | 403 | Generic message |
| Not found | 404 | Resource type + id |
| Optimistic lock conflict | 409 | Retry guidance |
| Rate limited | 429 | Retry-After header |
| Unexpected | 500 | Generic message; log full trace server-side |

### 10.5 Correlation with Logging

Include `traceId` / `correlationId` in ProblemDetail properties — Section 16.

```java
problem.setProperty("correlationId", MDC.get("correlationId"));
```

### 10.6 @ControllerAdvice Scope

```java
@RestControllerAdvice(assignableTypes = OrderController.class)
public class OrderExceptionHandler { /* order-specific */ }

@RestControllerAdvice
public class GlobalExceptionHandler { /* catch-all */ }
```

Order matters: most specific handler wins.

---

## Section 11: Graceful Shutdown and Lifecycle

### 11.1 Why Graceful Shutdown Matters on K8s

When a pod receives SIGTERM:

```
1. K8s removes pod from Service endpoints (if preStop + readiness handled)
2. SIGTERM sent to container
3. Spring Boot begins graceful shutdown
4. In-flight requests must complete within terminationGracePeriodSeconds
5. SIGKILL if still running
```

Without graceful shutdown: mid-transaction kills, duplicate processing on retry, angry users.

### 11.2 Spring Boot Graceful Shutdown Config

```yaml
server:
  shutdown: graceful

spring:
  lifecycle:
    timeout-per-shutdown-phase: 30s
```

```yaml
# Kubernetes
spec:
  terminationGracePeriodSeconds: 45
```

Ensure K8s grace period > Spring shutdown timeout + buffer.

### 11.3 Shutdown Sequence

```
SmartLifecycle beans stop (reverse order)
    │
    ├── Tomcat stops accepting new connections
    ├── In-flight HTTP requests complete
    ├── @PreDestroy methods run
    ├── TaskExecutor shutdown (@Async pools)
    └── HikariCP pool closes
```

### 11.4 preStop Hook Pattern

```yaml
lifecycle:
  preStop:
    exec:
      command: ["sh", "-c", "sleep 5"]
```

Brief sleep allows endpoint propagation before SIGTERM — endpoints controller delay.

### 11.5 Kafka Listener Graceful Stop

```yaml
spring:
  kafka:
    listener:
      ack-mode: manual
```

Spring Kafka stops consumers and waits for in-flight records during shutdown phase.

### 11.6 @PreDestroy for Custom Resources

```java
@Component
public class OutboxRelayScheduler {

    private ScheduledExecutorService scheduler;

    @PreDestroy
    public void shutdown() {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(10, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
```

### 11.7 Testing Graceful Shutdown

```bash
# Send SIGTERM to running app
kill -SIGTERM $(pgrep -f order-service)

# Verify in logs:
# "Commencing graceful shutdown"
# "Graceful shutdown complete"
```

In K8s: `kubectl delete pod <pod> --grace-period=45` and watch request success rate.

---

## Section 12: @Async Pitfalls — Thread Pools, Transactions, Context Loss

### 12.1 Enabling @Async

```java
@Configuration
@EnableAsync
public class AsyncConfig implements AsyncConfigurer {

    @Override
    public Executor getAsyncExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(4);
        executor.setMaxPoolSize(16);
        executor.setQueueCapacity(100);
        executor.setThreadNamePrefix("async-");
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        executor.initialize();
        return executor;
    }
}
```

**Never** use default single-thread executor in production.

### 12.2 @Transactional and @Async Don't Mix (Naively)

```java
@Service
public class NotificationService {

    @Async
    @Transactional  // NEW transaction on async thread — separate connection
    public void sendOrderConfirmation(UUID orderId) {
        Order order = orderRepository.findById(orderId).orElseThrow();
        emailClient.send(order);
    }
}
```

Caller returns before async completes. Failures are silent to caller unless `CompletableFuture` returned.

### 12.3 Returning CompletableFuture

```java
@Async
public CompletableFuture<SendResult> sendAsync(UUID orderId) {
    try {
        emailClient.send(orderId);
        return CompletableFuture.completedFuture(SendResult.success());
    } catch (Exception ex) {
        return CompletableFuture.failedFuture(ex);
    }
}
```

Caller can compose, timeout, or log failures.

### 12.4 Security Context and MDC Loss

Async threads don't inherit:

- `SecurityContextHolder` (auth)
- MDC (correlation ID)
- Request scope beans

**Fix — TaskDecorator:**

```java
executor.setTaskDecorator(runnable -> {
    SecurityContext context = SecurityContextHolder.getContext();
    Map<String, String> mdc = MDC.getCopyOfContextMap();
    return () -> {
        try {
            SecurityContextHolder.setContext(context);
            if (mdc != null) MDC.setContextMap(mdc);
            runnable.run();
        } finally {
            SecurityContextHolder.clearContext();
            MDC.clear();
        }
    };
});
```

Or use Micrometer Context Propagation (Boot 3.2+ observability).

### 12.5 Self-Invocation (Again)

`@Async` on method called from same class → **not async** (proxy bypass). Same fix as `@Transactional` — separate bean.

### 12.6 Virtual Threads (Boot 3.2+)

```yaml
spring:
  threads:
    virtual:
      enabled: true
```

```java
@Async
public void process() { /* runs on virtual thread */ }
```

Good for I/O-bound async. Still doesn't solve transaction boundary confusion.

### 12.7 When NOT to Use @Async

| Use @Async | Use outbox/message broker |
|------------|---------------------------|
| Fire-and-forget email inside monolith | Cross-service events |
| Local CPU offload | Must survive restart |
| Caller doesn't need result | Retry + DLQ needed |

See [outbox-expert-playbook.md](outbox-expert-playbook.md) and [kafka-expert-playbook.md](kafka-expert-playbook.md).


## Section 13: Security Overview — OAuth2 Resource Server Sketch

> Not a full security bible — enough to discuss production JWT validation in interviews.

### 13.1 Resource Server Basics

Your API validates JWT access tokens issued by an IdP (Keycloak, Auth0, Okta, Cognito).

```xml
<dependency>
  <groupId>org.springframework.boot</groupId>
  <artifactId>spring-boot-starter-oauth2-resource-server</artifactId>
</dependency>
```

```yaml
spring:
  security:
    oauth2:
      resourceserver:
        jwt:
          issuer-uri: https://auth.example.com/realms/production
          # jwk-set-uri auto-discovered from issuer
```

### 13.2 Security Filter Chain

```java
@Configuration
@EnableWebSecurity
public class SecurityConfig {

    @Bean
    SecurityFilterChain securityFilterChain(HttpSecurity http) throws Exception {
        return http
            .csrf(csrf -> csrf.disable()) // stateless API
            .sessionManagement(sm -> sm.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
            .authorizeHttpRequests(auth -> auth
                .requestMatchers("/actuator/health/**").permitAll()
                .requestMatchers(HttpMethod.GET, "/api/v1/public/**").permitAll()
                .requestMatchers("/api/v1/admin/**").hasRole("ADMIN")
                .anyRequest().authenticated()
            )
            .oauth2ResourceServer(oauth2 -> oauth2.jwt(Customizer.withDefaults()))
            .build();
    }
}
```

### 13.3 Method Security

```java
@Service
public class OrderService {

    @PreAuthorize("hasRole('CUSTOMER') and #customerId == authentication.principal.subject")
    public List<OrderResponse> getOrdersForCustomer(UUID customerId) { /* ... */ }
}
```

Enable: `@EnableMethodSecurity`.

### 13.4 JWT Claims to Principal

```java
@Bean
JwtAuthenticationConverter jwtAuthenticationConverter() {
    JwtGrantedAuthoritiesConverter granted = new JwtGrantedAuthoritiesConverter();
    granted.setAuthoritiesClaimName("roles");
    granted.setAuthorityPrefix("ROLE_");

    JwtAuthenticationConverter converter = new JwtAuthenticationConverter();
    converter.setJwtGrantedAuthoritiesConverter(granted);
    return converter;
}
```

### 13.5 Production Security Checklist (API)

- [ ] HTTPS only (ingress TLS)
- [ ] Short token TTL + refresh flow (client-side)
- [ ] Scope/role checks on sensitive endpoints
- [ ] Actuator not public
- [ ] CORS restricted to known origins
- [ ] Rate limiting at gateway
- [ ] Secrets not in Git
- [ ] Dependency scanning (OWASP dependency-check, Snyk)

---

## Section 14: Testing — @SpringBootTest vs Slice Tests, Testcontainers PostgreSQL

### 14.1 Test Pyramid for Spring Boot

```
        ┌─────────────┐
        │  E2E (few)  │
        ├─────────────┤
        │ @SpringBootTest + Testcontainers │
        ├─────────────────────────┤
        │ @WebMvcTest / @DataJpaTest (many) │
        ├─────────────────────────┤
        │ Unit tests (most)       │
        └─────────────────────────┘
```

### 14.2 @SpringBootTest — Full Context

```java
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
class OrderIntegrationTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine")
        .withDatabaseName("orders_test");

    @DynamicPropertySource
    static void configure(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
    }

    @Autowired
    TestRestTemplate restTemplate;

    @Test
    void placeOrder_persistsAndReturns201() {
        var request = new PlaceOrderRequest(/* ... */);
        ResponseEntity<OrderResponse> response =
            restTemplate.postForEntity("/api/v1/orders", request, OrderResponse.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
    }
}
```

**Use when:** Testing full flow — controller → service → DB → security.

**Cost:** Slow — use sparingly.

### 14.3 @WebMvcTest — Controller Slice

```java
@WebMvcTest(OrderController.class)
@Import(OrderControllerTestConfig.class)
class OrderControllerTest {

    @Autowired MockMvc mockMvc;
    @MockBean OrderApplicationService orderService;

    @Test
    void placeOrder_returns201() throws Exception {
        when(orderService.placeOrder(any())).thenReturn(new OrderResponse(/* ... */));

        mockMvc.perform(post("/api/v1/orders")
                .contentType(MediaType.APPLICATION_JSON)
                .content("""
                    {"customerId":"550e8400-e29b-41d4-a716-446655440000","lines":[],"totalAmount":10.00}
                    """))
            .andExpect(status().isCreated());
    }
}
```

No DB, no full context — fast.

### 14.4 @DataJpaTest — Repository Slice

```java
@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Testcontainers
class OrderRepositoryTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @DynamicPropertySource
    static void configure(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
    }

    @Autowired OrderRepository orderRepository;

    @Test
    void findWithLinesByCustomerId_fetchesInOneQuery() {
        // seed data, assert query behavior
    }
}
```

Use Testcontainers for PostgreSQL-specific SQL (JSONB, SKIP LOCKED) — H2 diverges.

### 14.5 @MockBean vs @MockitoBean (Boot 3.4+)

Prefer `@MockitoBean` when available — cleaner test context. `@MockBean` still widely used.

### 14.6 Testcontainers Tips

```java
// Shared container across test class — static @Container
// Or reuse with singleton pattern for faster CI

@Testcontainers(disabledWithoutDocker = true)
```

CI must have Docker available. Use `@ServiceConnection` (Boot 3.1+) for simpler wiring:

```java
@Container
@ServiceConnection
static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");
```

### 14.7 Testing @Transactional Behavior

```java
@SpringBootTest
@Transactional // rolls back after each test — default in @DataJpaTest
class OrderServiceTransactionTest {

    @Autowired OrderApplicationService orderService;
    @Autowired OrderRepository orderRepository;

    @Test
    void placeOrder_rollsBackOnFailure() {
        assertThatThrownBy(() -> orderService.placeOrder(invalidRequest()))
            .isInstanceOf(InvalidOrderException.class);
        assertThat(orderRepository.count()).isZero();
    }
}
```

For testing commit behavior, use `@Commit` or `TransactionTemplate` without test rollback.

---

## Section 15: REST API, Pagination, and OpenAPI in Production

### 15.1 API Versioning

```java
@RequestMapping("/api/v1/orders")
public class OrderControllerV1 { }

@RequestMapping("/api/v2/orders")
public class OrderControllerV2 { }
```

Or header-based versioning for internal APIs. Document deprecation timeline.

### 15.2 SpringDoc OpenAPI

```xml
<dependency>
  <groupId>org.springdoc</groupId>
  <artifactId>springdoc-openapi-starter-webmvc-ui</artifactId>
</dependency>
```

Disable swagger UI in production or protect with auth:

```yaml
springdoc:
  swagger-ui:
    enabled: ${SWAGGER_ENABLED:false}
```

### 15.3 Idempotent PUT/PATCH

Use `@Version` or ETag headers for conditional updates — pairs with optimistic locking.

---

## Section 16: Logging, Correlation IDs, and Structured Logs

### 16.1 Structured JSON Logging

```xml
<dependency>
  <groupId>net.logstash.logback</groupId>
  <artifactId>logstash-logback-encoder</artifactId>
</dependency>
```

```xml
<!-- logback-spring.xml -->
<appender name="JSON" class="ch.qos.logback.core.ConsoleAppender">
  <encoder class="net.logstash.logback.encoder.LogstashEncoder"/>
</appender>
```

### 16.2 Correlation ID Filter

```java
@Component
public class CorrelationIdFilter extends OncePerRequestFilter {

    public static final String CORRELATION_ID_HEADER = "X-Correlation-Id";

    @Override
    protected void doFilterInternal(HttpServletRequest request,
                                    HttpServletResponse response,
                                    FilterChain filterChain) throws ServletException, IOException {
        String correlationId = Optional.ofNullable(request.getHeader(CORRELATION_ID_HEADER))
            .filter(s -> !s.isBlank())
            .orElse(UUID.randomUUID().toString());
        MDC.put("correlationId", correlationId);
        response.setHeader(CORRELATION_ID_HEADER, correlationId);
        try {
            filterChain.doFilter(request, response);
        } finally {
            MDC.remove("correlationId");
        }
    }
}
```

Integrates with Micrometer tracing (`traceId`, `spanId`) — [metrics-observability-playbook.md](metrics-observability-playbook.md) Section 14.

### 16.3 Logging Levels in Production

```yaml
logging:
  level:
    root: INFO
    com.yourcompany.orders: INFO
    org.hibernate.SQL: WARN
    org.springframework.web: INFO
```

Never DEBUG Hibernate SQL in prod unless short-lived troubleshooting.

---

## Section 17: Database Migrations — Flyway/Liquibase in Production

### 17.1 Flyway Setup

```yaml
spring:
  flyway:
    enabled: true
    locations: classpath:db/migration
    baseline-on-migrate: true
```

```sql
-- V1__create_orders.sql
CREATE TABLE orders (
    id UUID PRIMARY KEY,
    customer_id UUID NOT NULL,
    total_amount NUMERIC(19,2) NOT NULL,
    version BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

### 17.2 Production Migration Rules

- **Backward-compatible** migrations only for zero-downtime deploys (expand-contract pattern)
- Never edit applied migration scripts — add new version
- Test migrations against copy of prod schema in CI
- Long-running migrations: run as separate Job, not blocking app startup

### 17.3 Expand-Contract Example

```
Phase 1: ADD COLUMN nullable
Phase 2: Deploy app writing to new column
Phase 3: Backfill data
Phase 4: ADD NOT NULL constraint
Phase 5: Remove old column
```

---

## Section 18: Five-Layer Production Debugging Framework for Spring Boot

```
Layer 5: Infrastructure     K8s, network, DNS, node pressure
Layer 4: Platform           JVM, pool, threads, GC
Layer 3: Spring Boot        Actuator health, config, profiles
Layer 2: Application        Logs, traces, business metrics
Layer 1: Data               SQL, locks, replication lag
```

### 18.1 Debugging Flow

```
Alert: P99 latency spike on POST /api/v1/orders
  │
  ├─ Layer 2: Grafana RED — error rate up? which status?
  ├─ Layer 4: hikaricp.connections.pending > 0?
  ├─ Layer 1: pg_stat_activity — long running queries?
  ├─ Layer 2: Trace — N+1 in Hibernate spans?
  └─ Layer 5: Recent deploy? pod restarts?
```

### 18.2 Essential Commands

```bash
# Health
curl -s localhost:8080/actuator/health/readiness | jq

# Metrics
curl -s localhost:8080/actuator/prometheus | grep hikaricp

# Thread dump (K8s)
kubectl exec deploy/order-service -- jcmd 1 Thread.print

# Heap (careful in prod)
kubectl exec deploy/order-service -- jcmd 1 GC.heap_info
```

Full K8s debug: [kubernetes-expert-playbook.md](kubernetes-expert-playbook.md) Section 13.

---

## Section 19: Production Scenario Runbook — 25+ Scenarios

### 19.1 HikariCP Connection Pool Exhausted

**Symptoms:** `SQLTransientConnectionException`, 503, `hikaricp.connections.pending` > 0.

**Investigate:**

```bash
curl -s localhost:8080/actuator/metrics/hikaricp.connections.active | jq
curl -s localhost:8080/actuator/metrics/hikaricp.connections.timeout | jq
```

| Root Cause | Fix |
|------------|-----|
| Long `@Transactional` with remote calls | Shorten TX; move HTTP out |
| Pool too small for load | Increase pool OR scale pods OR reduce per-pod pool |
| Connection leak | Enable leak detection in staging; fix unclosed resources |
| DB slow | Fix queries, indexes |
| Too many pod replicas × pool > DB max | Reduce pool per instance |

### 19.2 N+1 Query Latency Spike After Deploy

**Symptoms:** P99 up, DB QPS 10× HTTP QPS.

**Fix:** Enable SQL logging in staging; add JOIN FETCH / `@BatchSize` / DTO projection; disable OSIV.

### 19.3 LazyInitializationException in Production

**Symptoms:** 500 with `LazyInitializationException` in logs.

**Fix:** Fetch inside `@Transactional` service; disable OSIV; don't serialize entities directly.

### 19.4 @Transactional Not Rolling Back

**Symptoms:** Partial data committed despite error.

**Check:** Self-invocation? Swallowed exception? Checked exception without `rollbackFor`?

### 19.5 OptimisticLockException Storm

**Symptoms:** 409 spikes on hot entity.

**Fix:** Retry with backoff; atomic SQL update; reduce contention window.

### 19.6 Deadlock in PostgreSQL

**Symptoms:** `PSQLException: deadlock detected`.

**Fix:** Consistent lock ordering; shorter TX; `@Retryable` on deadlock.

### 19.7 Readiness Probe Failing — DB Down

**Symptoms:** Pod Running but not Ready; all replicas out of rotation.

**Fix:** Fix PostgreSQL; verify credentials; don't put DB on liveness.

### 19.8 Liveness Probe Killing Healthy JVM

**Symptoms:** Restart loop during GC pause.

**Fix:** Increase liveness timeout; use `/actuator/health/liveness` not full health; tune GC.

### 19.9 OOMKilled on Kubernetes

**Symptoms:** Pod `OOMKilled`, exit 137.

**Fix:** `-XX:MaxRAMPercentage=75`; increase limit; heap dump analysis.

See [kubernetes-expert-playbook.md](kubernetes-expert-playbook.md) Section 15.1.

### 19.10 Graceful Shutdown Dropping Requests

**Symptoms:** 502 during deploy; incomplete orders.

**Fix:** `server.shutdown=graceful`; preStop sleep; increase `terminationGracePeriodSeconds`.

### 19.11 Duplicate Event Processing

**Symptoms:** Double emails, double charges.

**Fix:** Consumer idempotency; outbox relay dedup; idempotency keys on API.

See [outbox-expert-playbook.md](outbox-expert-playbook.md) Section 9.

### 19.12 Outbox Relay Lag Growing

**Symptoms:** `outbox_pending_count` metric rising.

**Fix:** Scale relay; index on `(status, created_at)`; SKIP LOCKED batch size; check Kafka.

### 19.13 Validation Errors Return 500 Instead of 400

**Fix:** `@Valid` on request body; `@Validated` on controller; handle `MethodArgumentNotValidException`.

### 19.14 Wrong Profile Active in Production

**Symptoms:** Dev config in prod; H2 driver loaded.

**Fix:** Assert `SPRING_PROFILES_ACTIVE=production` at startup; fail if missing required env vars.

```java
@PostConstruct
void validateProductionProfile() {
    if (Arrays.asList(environment.getActiveProfiles()).contains("production")
            && environment.getProperty("DB_HOST") == null) {
        throw new IllegalStateException("DB_HOST required in production");
    }
}
```

### 19.15 Secrets Not Loaded — Empty Password

**Fix:** K8s Secret key names match env vars; External Secrets sync; never rely on default empty string.

### 19.16 @Async Not Running Async

**Fix:** Separate bean; `@EnableAsync`; check proxy; inspect thread name prefix in logs.

### 19.17 MDC Empty in Async Logs

**Fix:** TaskDecorator propagates MDC — Section 12.5.

### 19.18 JWT Validation Failing — 401 on All Requests

**Fix:** Verify `issuer-uri`; clock skew; audience claim; JWK rotation cache.

### 19.19 Flyway Migration Failed on Deploy

**Symptoms:** Pod CrashLoop on startup.

**Fix:** `flyway repair` only with understanding; restore from backup; test migration in staging.

### 19.20 Hibernate Schema Validation Failed

```yaml
spring.jpa.hibernate.ddl-auto: validate  # prod only
```

**Fix:** Align entities with DB; run missing migration.

### 19.21 Circuit Breaker Open — Cascading Failures

**Symptoms:** All calls to payment service fail fast.

**Fix:** [circuit-breaker-expert-playbook.md](circuit-breaker-expert-playbook.md) — fix downstream; tune thresholds.

### 19.22 Thread Pool Saturation (Tomcat)

**Symptoms:** Requests queue; `tomcat.threads.busy` maxed.

**Fix:** Scale pods; increase `server.tomcat.threads.max`; fix slow endpoints.

### 19.23 GC Pause Latency Spikes

**Fix:** G1/ZGC tuning; reduce heap; object allocation profiling; increase CPU limit.

### 19.24 Read Replica Lag — Stale Reads

**Symptoms:** User sees old data after update.

**Fix:** Read-your-writes routing to primary; eventual consistency messaging; CQRS sync lag metric.

See [cqrs-expert-playbook.md](cqrs-expert-playbook.md).

### 19.25 Metric Cardinality Explosion

**Symptoms:** Prometheus OOM; scrape timeout.

**Fix:** Don't tag metrics with userId/orderId; use MeterFilter — [metrics-observability-playbook.md](metrics-observability-playbook.md) Section 9-10.

### 19.26 Testcontainers Pass — Production Fails

**Cause:** H2 vs PostgreSQL divergence; Testcontainers used H2 locally without Docker.

**Fix:** Always Testcontainers PostgreSQL in CI for SQL-specific features.

### 19.27 ClassNotFoundException After Boot 3 Migration

**Fix:** `javax.*` → `jakarta.*`; update Spring Security 6 config DSL.

### 19.28 Scheduled Job Runs on Every Pod

**Fix:** ShedLock, Quartz cluster, or K8s CronJob for singleton work.

### 19.29 Memory Leak — Metaspace Growth

**Fix:** Classloader leak from hot deploy; limit dynamic proxies; restart policy; analyze metaspace dumps.

### 19.30 HTTP 415 Unsupported Media Type

**Fix:** Client `Content-Type: application/json`; `@RequestBody` DTO matches payload.

---

## Section 20: Spring Boot on Kubernetes — Cross-Cutting Concerns

### 20.1 The Production Stack

```
Ingress → Service → Pod (Spring Boot)
                      ├─ Actuator probes
                      ├─ Prometheus scrape
                      ├─ ConfigMap + Secret env
                      └─ JVM container-aware memory
```

Full K8s guide: [kubernetes-expert-playbook.md](kubernetes-expert-playbook.md) Section 15.

### 20.2 Deployment Checklist

- [ ] `SPRING_PROFILES_ACTIVE=production`
- [ ] `server.shutdown=graceful`
- [ ] `spring.jpa.open-in-view=false`
- [ ] Actuator liveness/readiness probes configured
- [ ] Resource requests/limits set (memory critical for JVM)
- [ ] preStop hook
- [ ] Prometheus scrape annotations
- [ ] Secrets via K8s Secret / External Secrets
- [ ] Flyway migrations compatible with rolling deploy
- [ ] PDB `minAvailable: 1`

### 20.3 Platform Starter for Consistency

Internal teams ship shared auto-config for metrics, security, logging — [custom-spring-boot-starter-expert-playbook.md](custom-spring-boot-starter-expert-playbook.md).

---

## Section 21: Revision Cheat Sheets — Properties, Annotations, Quick Tables

### 21.1 Essential application-production.yml

```yaml
spring:
  profiles:
    active: production
  jpa:
    open-in-view: false
    hibernate:
      ddl-auto: validate
    properties:
      hibernate.jdbc.batch_size: 25
      hibernate.default_batch_fetch_size: 25
  datasource:
    hikari:
      maximum-pool-size: 15
      connection-timeout: 5000
  mvc:
    problemdetails:
      enabled: true
  flyway:
    enabled: true

server:
  shutdown: graceful

management:
  endpoints:
    web:
      exposure:
        include: health,prometheus
  endpoint:
    health:
      probes:
        enabled: true
  metrics:
    tags:
      application: ${spring.application.name}
```

### 21.2 @Transactional Quick Reference

| Scenario | Setting |
|----------|---------|
| Default write | `@Transactional` |
| Read query | `@Transactional(readOnly = true)` |
| Audit must survive rollback | `propagation = REQUIRES_NEW` |
| Checked exception rollback | `rollbackFor = Exception.class` |
| Self-invocation bug | Move to another bean |

### 21.3 JPA Fetch Strategy

| Need | Use |
|------|-----|
| Single entity + associations | `@EntityGraph` or JOIN FETCH |
| List of entities + child | `@BatchSize` |
| API response | DTO projection |
| Hot write path | Atomic `@Modifying` query |

### 21.4 Test Annotation Selection

| Annotation | Loads |
|------------|-------|
| `@WebMvcTest` | Controller + MVC |
| `@DataJpaTest` | JPA + repositories |
| `@SpringBootTest` | Full application |
| `@MockBean` | Replace bean in context |

### 21.5 HTTP Status Mapping

| Condition | Status |
|-----------|--------|
| Validation failed | 400 |
| Unauthorized | 401 |
| Forbidden | 403 |
| Not found | 404 |
| Optimistic lock conflict | 409 |
| Unexpected error | 500 |

---

## Section 22: Lead Interview Questions — Logical and Production Scenarios

### 22.1 @Transactional and Data Integrity

**Q1: Why doesn't `@Transactional` work when I call the method from the same class?**

A: Spring AOP proxies intercept **external** calls only. Self-invocation bypasses the proxy. Fix: move to another bean, use `TransactionTemplate`, or inject self via `@Lazy`.

**Q2: When would you use `REQUIRES_NEW`?**

A: When a sub-operation must commit independently — audit logs, idempotency record persistence, or compensating side effects that must survive outer rollback. Watch pool usage.

**Q3: Does `@Transactional(readOnly=true)` guarantee no writes?**

A: No — it's a hint. Hibernate skips dirty checking; PostgreSQL may use read-only connection. Writes can still occur if code mutates entities — enforce in code review.

**Q4: What happens if you catch an exception inside a `@Transactional` method and don't rethrow?**

A: Transaction **commits** unless you call `setRollbackOnly()`. Common production bug.

**Q5: Should `@Transactional` be on the controller?**

A: No. Transaction boundary belongs in the service layer. Controller TX spans HTTP serialization and holds connections too long.

**Q6: How do you run code only after successful commit?**

A: `TransactionSynchronization.afterCommit()`, outbox pattern (preferred for events), or `@TransactionalEventListener(phase = AFTER_COMMIT)`.

**Q7: What's wrong with `@Transactional` on a private method?**

A: Proxies cannot intercept private methods — annotation is ignored silently.

**Q8: How do isolation levels affect production PostgreSQL apps?**

A: Default `READ_COMMITTED` suits most cases. `SERIALIZABLE` needs retry logic. Higher isolation = more locking and deadlocks.

### 22.2 JPA, Hibernate, and Performance

**Q9: Explain the N+1 problem and three ways to fix it.**

A: One query for parents, N for children. Fixes: JOIN FETCH, `@BatchSize`, DTO projection / EntityGraph. Detect via SQL logging or tracing.

**Q10: Why disable Open Session In View in production?**

A: Hides N+1; holds DB connection through JSON serialization; unpredictable query count per request.

**Q11: JOIN FETCH vs `@BatchSize` — when to use each?**

A: JOIN FETCH for single-entity detail views. `@BatchSize` for lists where collection JOIN FETCH breaks pagination or causes Cartesian products.

**Q12: Why does `GenerationType.IDENTITY` break JDBC batching?**

A: ID must be generated per row before batch flush. Use `SEQUENCE` with `allocationSize` for batch inserts.

**Q13: What is `LazyInitializationException` and root cause?**

A: Accessing lazy association outside persistence context. Fix: fetch in service TX, use DTOs, or (not recommended) OSIV.

**Q14: Optimistic vs pessimistic locking — flash sale scenario?**

A: Pessimistic `FOR UPDATE` or atomic `UPDATE ... WHERE qty >= ?` to serialize inventory. Optimistic with retry for low contention.

**Q15: Can you paginate with JOIN FETCH on a collection?**

A: Unreliably — duplicate roots break page counts. Use batch fetch or separate query.

**Q16: What does `hibernate.jdbc.batch_size` do?**

A: Batches INSERT/UPDATE statements. Requires ordered inserts/updates and non-IDENTITY ID strategy.

**Q17: How do you count queries per API endpoint in staging?**

A: Hibernate statistics, datasource-proxy, p6spy, or tracing with JDBC spans in Micrometer/OTel.

### 22.3 Connection Pools and Infrastructure

**Q18: How do you size HikariCP for 8 pods on PostgreSQL with max_connections=200?**

A: Reserve ~20 for admin → 180 for apps → ~22 per pod max. Tune down based on actual concurrent DB usage, not thread count.

**Q19: What does `hikaricp.connections.pending` tell you?**

A: Threads waiting for a connection — pool undersized, slow queries, or connection leaks.

**Q20: Why fail-fast with low `connection-timeout`?**

A: Queuing threads for 30s causes cascading latency. Fail fast → circuit breaker → shed load.

**Q21: How does graceful shutdown interact with Kubernetes?**

A: SIGTERM → stop accepting requests → complete in-flight → close pool. K8s `terminationGracePeriodSeconds` must exceed Spring shutdown timeout.

**Q22: What's the difference between liveness and readiness for Spring Boot?**

A: Liveness = JVM alive (restart if dead). Readiness = can serve traffic (DB up). Never put DB check on liveness.

### 22.4 Validation, Errors, and API Design

**Q23: `@Valid` vs `@Validated`?**

A: `@Valid` — JSR 380 on objects. `@Validated` — Spring's variant enabling method-level constraints and validation groups.

**Q24: Where do business validation rules belong?**

A: Service layer. Annotations for structural constraints (not null, size). "Coupon expired" is domain logic.

**Q25: Why RFC 7807 Problem Details?**

A: Consistent machine-readable errors with type, title, status, detail, and extension fields — better than ad-hoc JSON.

**Q26: Should domain exceptions know about HTTP status codes?**

A: No. Domain throws `OrderNotFoundException`; `@ControllerAdvice` maps to 404.

**Q27: How do you validate configuration at startup?**

A: `@Validated` on `@ConfigurationProperties` — fail fast before accepting traffic.

### 22.5 Security, Async, and Microservices

**Q28: Sketch OAuth2 resource server flow.**

A: Client sends Bearer JWT → Spring validates signature via JWK Set from issuer → extracts authorities → authorization checks.

**Q29: What breaks with `@Async` in a secured app?**

A: SecurityContext and MDC don't propagate. Use TaskDecorator or context propagation.

**Q30: Why not use `@Async` for order confirmation email in microservices?**

A: Fire-and-forget loses message on crash. Outbox or message broker gives durability and retry.

**Q31: How do you prevent duplicate order creation on client retry?**

A: Idempotency-Key header stored in unique DB column; return cached response on replay.

**Q32: Can one `@Transactional` span two microservices?**

A: Not without 2PC (avoid). Use saga + outbox + idempotency.

**Q33: What's the dual-write problem?**

A: DB commit succeeds, message publish fails (or vice versa). Outbox solves atomically — see [outbox-expert-playbook.md](outbox-expert-playbook.md).

### 22.6 Testing and Deployment

**Q34: `@WebMvcTest` vs `@SpringBootTest`?**

A: `@WebMvcTest` — controller slice, fast. `@SpringBootTest` — full context, integration. Use slices by default.

**Q35: Why Testcontainers over H2?**

A: PostgreSQL-specific SQL, JSONB, locking behavior, and Flyway scripts differ. H2 gives false confidence.

**Q36: How do you test `@Transactional` rollback?**

A: `@Transactional` on test method with `@Rollback`, or `@Sql` setup + assert DB state after service call.

**Q37: `@MockBean` vs `@Mock`?**

A: `@MockBean` replaces Spring context bean. `@Mock` is plain Mockito — use with `@ExtendWith(MockitoExtension.class)` for unit tests without context.

**Q38: How do Flyway migrations work with rolling deploy on K8s?**

A: Expand-contract pattern — backward-compatible migrations only. Never drop column until all pods on new code.

**Q39: What is expand-contract migration?**

A: Phase 1 add nullable column → deploy app writing both → backfill → add constraint → remove old column.

**Q40: How do you verify health probes before go-live?**

A: `kubectl describe pod` probe failures; curl readiness from inside cluster; simulate DB down — readiness fails, liveness passes.

### 22.7 Architecture and Lead-Level Scenarios

**Q41: Design an order service that publishes events reliably.**

A: Single TX: save order + outbox row. Relay process publishes to Kafka. Consumer idempotency. See outbox playbook.

**Q42: P99 latency doubled after a harmless code deploy — your process?**

A: Five-layer debug — Section 18. Check deploy diff, metrics (pool, GC), SQL count, traces, infra.

**Q43: Team wants `@Transactional` on repository methods — your response?**

A: Service layer owns TX boundaries. Repository TX per method causes partial commits and unclear orchestration.

**Q44: When would you choose virtual threads in Spring Boot 3.2+?**

A: I/O-bound workloads with many blocking calls. Still size DB pool for DB capacity. Less benefit for CPU-bound.

**Q45: How do you prevent metric cardinality explosion?**

A: Never high-cardinality tags (userId). Use MeterFilter. Aggregate at query time.

**Q46: What's your production readiness checklist for a new Spring Boot service?**

A: Section 24 appendix — probes, graceful shutdown, OSIV off, pool sized, validation, Problem Details, structured logs, migrations, secrets externalized, SLO metrics.

**Q47: Explain `@ConfigurationProperties` vs `@Value` for a platform team.**

A: Properties class = typed, validated, documented, testable bundle. `@Value` scatters config, no validation, harder to refactor.

**Q48: How does Spring Boot 3 differ from Boot 2 for production?**

A: Java 17+, Jakarta namespace, Hibernate 6, Problem Details, observability API, optional virtual threads.

**Q49: Customer reports intermittent 409 on checkout — investigation?**

A: Optimistic lock conflicts on inventory. Check concurrent updates, retry logic, consider atomic SQL decrement.

**Q50: When is `@PreDestroy` not enough for shutdown?**

A: Long-running tasks need cooperative cancellation; Kafka consumers need listener container stop; custom thread pools need explicit awaitTermination.

---

## Section 23: How to Talk About Spring Boot in Production in an Interview

### 23.1 The Lead-Level Framing

Don't list features. Tell **stories with trade-offs**:

> "We run Spring Boot 3 on Kubernetes — 12 pods, PostgreSQL, Kafka outbox. I focus on operational correctness: transaction boundaries, pool sizing against `max_connections`, OSIV disabled, and readiness probes that check DB without putting that on liveness."

Interviewers want: **judgment**, not trivia recitation.

### 23.2 Structure Every Answer (STAR-Technical)

```
Situation  — production context (scale, constraints)
Trade-off  — what you considered
Action     — what you configured/changed
Result     — metric or incident outcome
```

**Example prompt:** "Tell me about transactions in Spring."

**Weak answer:** "I use `@Transactional` on service methods."

**Strong answer:**

> "In our order service, the transaction boundary is one service method: save order and outbox event atomically. I avoid remote HTTP inside the transaction because we saw pool exhaustion when payment gateway latency spiked — connections were held 30 seconds. We moved payment to a saga step. For audit logging that must survive rollback, we use `REQUIRES_NEW` sparingly. We also hit the self-invocation bug in a code review — transactional method called from same class — caught by DEBUG transaction logging in staging."

### 23.3 Topics to Weave In Naturally

| Topic | One sentence that impresses |
|-------|----------------------------|
| Transactions | "Service-layer boundaries; no HTTP inside TX; outbox for events." |
| JPA | "OSIV off; explicit fetch; DTO projections on list endpoints." |
| Pools | "Pool sized from PostgreSQL global limit ÷ replicas, not Tomcat threads." |
| K8s | "Graceful shutdown + preStop; readiness checks DB, liveness checks JVM." |
| Errors | "RFC 7807 Problem Details; domain exceptions; never leak stack traces." |
| Testing | "Testcontainers PostgreSQL in CI; slice tests for speed." |
| Observability | "RED metrics on `http.server.requests`; alert on pool pending." |

### 23.4 Red Flags Interviewers Listen For

| Red flag | Better signal |
|----------|---------------|
| "We use `@Transactional` everywhere" | "Intentional boundaries per use case" |
| "Lazy loading just works" | "We fetch explicitly; OSIV disabled" |
| "Default pool is fine" | "Sized against DB max_connections and concurrency" |
| "We catch exceptions and return 200" | "Exceptions propagate or setRollbackOnly" |
| "We test with H2" | "Testcontainers for SQL parity" |
| "Actuator exposes everything" | "Health + prometheus only; secured" |

### 23.5 Handling "What Would You Do?" Scenarios

**Prompt:** "API slow under load — walk me through."

```
1. Grafana: RED metrics — latency vs errors vs traffic
2. HikariCP pending/active — pool issue?
3. Traces/logs: query count per request — N+1?
4. PostgreSQL: pg_stat_activity — slow queries, locks
5. Recent deploy: regression?
6. K8s: CPU throttling, OOM, pod count
```

Narrate **breadth first, then depth** — shows systematic thinking.

### 23.6 Questions to Ask the Interviewer

Shows seniority:

- "Do you disable OSIV across services or per-team?"
- "How do you handle cross-service consistency — outbox, saga, or something else?"
- "What's your Flyway strategy for zero-downtime deploys?"
- "Are virtual threads on the roadmap, or pinned thread pools?"

### 23.7 Whiteboard-Friendly Diagrams

**Transaction + Outbox (draw in 30 seconds):**

```
Client → Controller → Service [@Transactional]
                          ├─ INSERT order
                          └─ INSERT outbox_event
                      COMMIT
Relay (separate process) → Kafka → Payment Service
```

**K8s probe split:**

```
liveness  → /actuator/health/liveness  → JVM up?
readiness → /actuator/health/readiness → DB up? ready for traffic?
```

### 23.8 Closing Statement Template

> "Spring Boot gets you to production fast, but production hardening is deliberate: short transactions, explicit data fetching, pool math against the database, observable health boundaries, and consistency patterns like outbox instead of dual writes. That's what I optimize for on my teams."

---

## Section 24: Appendix — Decision Trees and Production Readiness Checklist

### 24.1 Transaction Boundary Decision Tree

```
Need atomic DB write?
  ├─ YES → Single service @Transactional method
  │         ├─ Remote call needed?
  │         │    ├─ YES → Outbox/event AFTER local TX (not inside)
  │         │    └─ NO  → Keep TX short
  │         └─ Must survive outer rollback?
  │              ├─ YES → REQUIRES_NEW (audit only — watch pool)
  │              └─ NO  → REQUIRED
  └─ NO  → No @Transactional (or readOnly for queries)
```

### 24.2 Fetch Strategy Decision Tree

```
Returning data to API?
  ├─ Single entity detail?
  │    └─ JOIN FETCH or @EntityGraph
  ├─ List of entities with children?
  │    └─ @BatchSize (avoid collection JOIN FETCH + pagination)
  ├─ List for table/grid?
  │    └─ DTO projection (no entities)
  └─ Write path only?
       └─ @Modifying query (no load entity)
```

### 24.3 Lock Strategy Decision Tree

```
Concurrent updates to same row?
  ├─ Low contention → @Version optimistic + retry
  ├─ High contention inventory → Pessimistic FOR UPDATE or atomic UPDATE
  └─ Financial ledger → Optimistic + idempotency key on API
```

### 24.4 Test Type Decision Tree

```
What are you testing?
  ├─ Controller mapping + validation → @WebMvcTest
  ├─ Repository query → @DataJpaTest + Testcontainers
  ├─ Full flow → @SpringBootTest + Testcontainers
  └─ Pure logic → JUnit + Mockito (no Spring context)
```

### 24.5 Production Readiness Checklist

#### Application

- [ ] Java 17+ / Spring Boot 3.x pinned via BOM
- [ ] `spring.jpa.open-in-view=false`
- [ ] `spring.jpa.hibernate.ddl-auto=validate` (prod)
- [ ] `@ConfigurationProperties` validated at startup
- [ ] No secrets in source control or ConfigMaps
- [ ] RFC 7807 Problem Details enabled
- [ ] `@ControllerAdvice` maps domain exceptions
- [ ] Idempotency on mutating APIs where clients retry
- [ ] Correlation ID in logs (MDC)

#### Database

- [ ] HikariCP pool sized: `pods × pool < PG max_connections`
- [ ] Flyway/Liquibase migrations tested in staging
- [ ] Expand-contract for breaking schema changes
- [ ] Indexes on FK and query filter columns
- [ ] N+1 audited on top 10 endpoints

#### Operations

- [ ] Actuator: health, prometheus exposed (secured)
- [ ] K8s liveness + readiness + startup probes
- [ ] `server.shutdown=graceful` + preStop hook
- [ ] Resource limits (memory for JVM `-XX:MaxRAMPercentage`)
- [ ] PDB configured
- [ ] Alerts: error rate, P99 latency, pool pending, OOM
- [ ] Runbook linked in on-call wiki

#### Security

- [ ] OAuth2 resource server or equivalent
- [ ] Actuator endpoints not public
- [ ] Dependency scan (OWASP/Snyk) in CI

#### Testing

- [ ] Testcontainers PostgreSQL in CI
- [ ] Slice tests for controllers and repositories
- [ ] Migration tests against prod-like schema

### 24.6 Quick Reference — Files in This Repo

| Topic | Playbook |
|-------|----------|
| K8s probes, JVM memory | [kubernetes-expert-playbook.md](kubernetes-expert-playbook.md) |
| Metrics, SLOs | [metrics-observability-playbook.md](metrics-observability-playbook.md) |
| Reliable events | [outbox-expert-playbook.md](outbox-expert-playbook.md) |
| Read/write split | [cqrs-expert-playbook.md](cqrs-expert-playbook.md) |
| Platform starters | [custom-spring-boot-starter-expert-playbook.md](custom-spring-boot-starter-expert-playbook.md) |
| Resilience | [circuit-breaker-expert-playbook.md](circuit-breaker-expert-playbook.md) |
| Pool isolation | [bulkhead-expert-playbook.md](bulkhead-expert-playbook.md) |
| Kafka integration | [kafka-expert-playbook.md](kafka-expert-playbook.md) |
| Runnable demo | `examples/custom-spring-boot-starter/` |

### 24.7 Revision Order (Suggested 2-Hour Scan)

```
Hour 1: Sections 1-2 (mindset + transactions) → 4-6 (JPA + pools)
Hour 2: Sections 9-12 (actuator, errors, shutdown, async) → 19 + 22-23
```

---

*End of Spring Boot 3 Production Revision Playbook*
