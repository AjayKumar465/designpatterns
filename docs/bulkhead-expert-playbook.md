# Bulkhead Pattern — Expert Playbook (Lead/Architect, Java, 10+ Years)

A comprehensive end-to-end reference covering the Bulkhead pattern from theory to production-grade implementation in Java and Spring Boot. Includes semaphore bulkheads, thread pool isolation, connection pool limits, Hystrix history, Resilience4j configuration, layered resilience with circuit breakers and rate limiters, observability, production runbooks, and 25+ lead-level interview Q&As.

**Runnable examples in this repo:**

- `examples/bulkhead/BulkheadDemo.java` — shared pool exhaustion, semaphore bulkhead fail-fast, thread pool isolation per dependency
- `examples/circuit-breaker/CircuitBreakerDemo.java` — circuit breaker lifecycle (companion pattern; see cross-reference in Section 3 and Section 12)

**Related playbooks:**

- Circuit Breaker — `docs/circuit-breaker-expert-playbook.md` (companion resilience pattern; use bulkhead + circuit breaker together)
- Saga — `docs/saga-expert-playbook.md` (orchestrators should bulkhead outbound calls to participants)
- Metrics & Observability — `docs/metrics-observability-playbook.md`

---

## Table of Contents

1. [What Is the Bulkhead Pattern?](#section-1-what-is-the-bulkhead-pattern)
2. [The Problem: Resource Exhaustion and Cascading Failure](#section-2-the-problem-resource-exhaustion-and-cascading-failure)
3. [Bulkhead vs Circuit Breaker vs Rate Limiting](#section-3-bulkhead-vs-circuit-breaker-vs-rate-limiting)
4. [Semaphore Bulkhead — Limit Concurrent Calls](#section-4-semaphore-bulkhead--limit-concurrent-calls)
5. [Thread Pool Bulkhead — Isolation by Executor](#section-5-thread-pool-bulkhead--isolation-by-executor)
6. [Connection Pool Bulkhead — JDBC, HTTP, and gRPC](#section-6-connection-pool-bulkhead--jdbc-http-and-grpc)
7. [Hystrix History and the Resilience4j Bulkhead](#section-7-hystrix-history-and-the-resilience4j-bulkhead)
8. [Resilience4j Bulkhead — Semaphore and Thread Pool Modes](#section-8-resilience4j-bulkhead--semaphore-and-thread-pool-modes)
9. [Spring Boot Integration with Resilience4j](#section-9-spring-boot-integration-with-resilience4j)
10. [Production Java Patterns — Shared Pool Anti-Patterns](#section-10-production-java-patterns--shared-pool-anti-patterns)
11. [Sizing Bulkheads — Capacity Planning and Load Testing](#section-11-sizing-bulkheads--capacity-planning-and-load-testing)
12. [Layered Resilience — Bulkhead + Circuit Breaker + Timeout + Retry](#section-12-layered-resilience--bulkhead--circuit-breaker--timeout--retry)
13. [Metrics and Observability](#section-13-metrics-and-observability)
14. [Production Pitfalls and War Stories](#section-14-production-pitfalls-and-war-stories)
15. [Production Issue Runbook](#section-15-production-issue-runbook)
16. [Decision Framework: When to Use and When NOT To](#section-16-decision-framework-when-to-use-and-when-not-to)
17. [Lead Interview Questions & Answers](#section-17-lead-interview-questions--answers)
18. [Appendix: Quick Reference Cheat Sheet](#section-18-appendix-quick-reference-cheat-sheet)
19. [How to Talk About Bulkheads in an Interview](#section-19-how-to-talk-about-bulkheads-in-an-interview)

---

## Section 1: What Is the Bulkhead Pattern?

### 1.1 The Ship Metaphor

On a ship, **bulkheads** are watertight walls dividing the hull into compartments. If one compartment floods, the others stay dry and the ship stays afloat.

In software, a **bulkhead** partitions **shared resources** so that failure or overload in one area cannot exhaust resources for the entire system.

| Without bulkhead | With bulkhead |
|------------------|---------------|
| One slow dependency consumes all threads | Slow dependency capped at N concurrent calls |
| One bad tenant floods DB connections | Per-tenant or per-service pool limits |
| Entire API becomes unresponsive | Only the affected feature degrades |

### 1.2 Core Terminology

| Term | Definition |
|------|------------|
| **Bulkhead** | A resource partition that limits concurrent use of a dependency or subsystem |
| **Semaphore bulkhead** | Limits concurrent in-flight calls using a counting semaphore; caller thread may block or fail fast |
| **Thread pool bulkhead** | Dedicated `ExecutorService` per dependency; work runs on isolated threads |
| **Connection pool bulkhead** | HikariCP / HTTP client max connections cap concurrent I/O to a backend |
| **Blast radius** | How much of the system is affected when one component fails or slows down |
| **Fail fast** | Reject immediately when bulkhead is full instead of queueing indefinitely |
| **Fairness** | Ensuring one noisy neighbor cannot starve other workloads |

### 1.3 What Bulkheads Protect

Bulkheads protect **finite resources**:

- Tomcat / Jetty worker threads
- Application thread pools (`ExecutorService`)
- JDBC connection pools (HikariCP)
- HTTP client connection pools (Apache HttpClient, OkHttp, WebClient)
- File descriptors and socket limits
- CPU time on constrained containers
- Downstream service capacity (you don't overwhelm a fragile partner)

### 1.4 Repo Example Overview

`examples/bulkhead/BulkheadDemo.java` demonstrates three scenarios:

1. **No bulkhead** — 8 concurrent calls to a 400ms dependency with pool size 4 → queue buildup, timeouts, errors
2. **Semaphore bulkhead** — max 2 concurrent calls to slow dependency → excess requests fail fast with `BulkheadRejectedException`
3. **Thread pool bulkhead** — separate pools for payment vs catalog → catalog stays responsive while payment pool is busy

Run:

```bash
javac examples/bulkhead/BulkheadDemo.java && java -cp examples/bulkhead BulkheadDemo
```

---

## Section 2: The Problem: Resource Exhaustion and Cascading Failure

### 2.1 The Shared Thread Pool Trap

Most Java services use a **shared** thread pool for async work or rely on the servlet container's **fixed worker thread pool** (e.g., Tomcat default 200 threads).

When a downstream dependency slows down:

```
Request arrives → servlet thread (or pool thread) blocks on HTTP call
                → thread held for seconds instead of milliseconds
                → pool exhausts
                → new requests queue or timeout
                → health checks fail
                → load balancer removes instance
                → remaining instances take more load
                → cascade
```

This is **not** a bug in the dependency alone — it is **resource coupling**. Every caller shares the same finite thread budget.

### 2.2 Latency Amplification

| Normal state | Under slow dependency |
|--------------|----------------------|
| 50ms p99 per request | 2000ms p99 per request |
| 200 threads × 50ms = 4000 RPS capacity | 200 threads × 2000ms = 100 RPS capacity |
| Headroom for spikes | No headroom — everything queues |

A **20× latency increase** can reduce effective throughput by **20×** if threads block synchronously.

### 2.3 The Anti-Pattern: One Pool for Everything

```java
// ANTI-PATTERN — single pool for all outbound integrations
@Bean
public ExecutorService integrationExecutor() {
    return Executors.newFixedThreadPool(50);
}

// Payment, catalog, fraud, email — all share 50 threads
paymentService.charge(...);   // uses pool thread, blocks 3s on PSP
catalogService.lookup(...);   // starved — waits for pool slot
fraudService.score(...);      // starved
```

When payment gateway latency spikes, **catalog lookups fail** even though the catalog service is healthy. Users cannot browse products because checkout is slow.

### 2.4 What Bulkhead Does NOT Fix

| Problem | Bulkhead helps? | Better tool |
|---------|-----------------|-------------|
| Dependency returns 500 errors | Partially (limits concurrent bad calls) | Circuit breaker |
| Too many requests globally | No | Rate limiter / API gateway |
| Bug causing infinite loop in your code | No | Fix the bug |
| Database query missing index | No | Query optimization |
| Network partition | No | Circuit breaker + fallback |

Bulkhead addresses **resource exhaustion**, not **logical failure**. Use it alongside circuit breakers, timeouts, and rate limiters.

### 2.5 Failure Timeline — Realistic Incident

```
T+0s   Payment provider deploys bad version — p99 latency 50ms → 8000ms
T+30s  Checkout threads blocked; Tomcat active threads climb to max
T+45s  Product search (same JVM) starts timing out — unrelated code path
T+60s  Connection pool to catalog DB exhausted (threads hold connections while waiting on payment)
T+90s  Kubernetes liveness probe fails — pod restarted
T+120s Remaining pods absorb traffic — same cascade
T+5m   Full site outage despite catalog and search backends being healthy
```

**Root cause:** no bulkhead between checkout/payment and the rest of the service.

---

## Section 3: Bulkhead vs Circuit Breaker vs Rate Limiting

These three patterns are complementary. Interviewers often conflate them. Know the distinction cold.

### 3.1 Comparison Matrix

| Dimension | Bulkhead | Circuit Breaker | Rate Limiter |
|-----------|----------|-----------------|--------------|
| **Primary goal** | Limit concurrent resource use | Stop calling a failing dependency | Limit requests per time window |
| **Trigger** | Bulkhead full (max concurrent reached) | Failure rate / slow call threshold | Rate exceeded (e.g., 100 req/s) |
| **Protects** | Your threads, connections, memory | Downstream + your resources (by not calling) | Your service + downstream (ingress control) |
| **Behavior when tripped** | Reject / block immediately | Fail fast (OPEN state) | Reject or delay (429) |
| **Self-healing** | Immediate when slot frees | HALF_OPEN probe after cooldown | Next time window |
| **Scope** | Per dependency partition | Per dependency call path | Per client, tenant, or endpoint |
| **Typical library** | Resilience4j Bulkhead, Semaphore | Resilience4j CircuitBreaker | Resilience4j RateLimiter, Bucket4j |

### 3.2 When They Work Together

```
Incoming request
    │
    ▼
Rate Limiter (protect service from traffic spike)
    │
    ▼
Bulkhead (cap concurrent calls to payment-service)
    │
    ▼
Circuit Breaker (skip calls if payment-service is down)
    │
    ▼
Timeout (don't wait forever)
    │
    ▼
Payment HTTP call
```

**Order matters in Resilience4j:** decorators are applied inside-out. Typical stack:

```java
Supplier<String> decorated = Decorators.ofSupplier(paymentClient::charge)
    .withBulkhead(bulkhead)
    .withCircuitBreaker(circuitBreaker)
    .withTimeLimiter(timeLimiter, scheduler)
    .decorate();
```

Bulkhead **outside** circuit breaker: you don't waste bulkhead slots on calls the breaker would reject anyway — but some teams prefer bulkhead first to cap concurrency regardless of breaker state. Document your choice.

See `docs/circuit-breaker-expert-playbook.md` for circuit breaker state machine (`CLOSED → OPEN → HALF_OPEN`) and `examples/circuit-breaker/CircuitBreakerDemo.java`.

### 3.3 Bulkhead vs Circuit Breaker — Interview Sound Bite

> **Bulkhead:** "I'll only let 10 requests talk to payment at once, so the other 190 threads can still serve catalog."
>
> **Circuit breaker:** "Payment is failing 80% of the time — stop calling it for 30 seconds and fail fast."

### 3.4 Bulkhead vs Rate Limiter

Rate limiter: **100 requests per second** to an API — smooth traffic shape.

Bulkhead: **10 concurrent in-flight** calls — protects against slow responses that hold threads for seconds.

A rate limiter does not help when 10 slow calls each take 30 seconds — you could have only 10 RPS effective throughput with no "rate" violation. Bulkhead caps **concurrency**; rate limiter caps **velocity**.

---

## Section 4: Semaphore Bulkhead — Limit Concurrent Calls

### 4.1 How It Works

A `java.util.concurrent.Semaphore` maintains a fixed number of **permits**. Before calling a dependency, acquire a permit; release in `finally`.

```
Semaphore(permits=2)

Call 1: acquire ✓  — in flight
Call 2: acquire ✓  — in flight
Call 3: tryAcquire ✗ — bulkhead full, reject immediately
Call 1: release    — permit returned
Call 3: tryAcquire ✓ — now succeeds
```

### 4.2 From BulkheadDemo.java

```java
static final class BulkheadClient {
    private final SlowDependency dependency;
    private final ExecutorService pool;
    private final Semaphore bulkhead;

    BulkheadClient(SlowDependency dependency, int poolSize, int maxConcurrentToDependency) {
        this.dependency = dependency;
        this.pool = Executors.newFixedThreadPool(poolSize);
        this.bulkhead = new Semaphore(maxConcurrentToDependency);
    }

    Future<String> callAsync(String id) {
        return pool.submit(() -> {
            if (!bulkhead.tryAcquire()) {
                throw new BulkheadRejectedException("Bulkhead full for slow dependency");
            }
            try {
                return dependency.call(id);
            } finally {
                bulkhead.release();
            }
        });
    }
}
```

**Key design choices:**

- `tryAcquire()` — **fail fast**, no indefinite blocking
- `release()` in `finally` — permit always returned even on exception
- Semaphore limits **calls to dependency**, not pool threads — pool may still queue tasks, but only N actually invoke the dependency

### 4.3 Blocking vs Fail-Fast Semaphore

| API | Behavior | Use when |
|-----|----------|----------|
| `acquire()` | Blocks until permit available | You want backpressure, bounded wait acceptable |
| `tryAcquire()` | Returns false immediately if full | Fail fast — preferred for HTTP APIs |
| `tryAcquire(timeout, unit)` | Waits up to N ms | Bounded queueing — middle ground |

For user-facing synchronous APIs, **`tryAcquire()` + immediate 503/429** is usually correct. Blocking `acquire()` on servlet threads just moves the queue from the pool to the semaphore — threads still pile up.

### 4.4 Fair vs Unfair Semaphore

`new Semaphore(n, fair=true)` — FIFO permit acquisition. Slightly lower throughput, better starvation resistance.

`fair=false` (default) — higher throughput, possible starvation under extreme load.

Production default: **unfair** unless you observe starvation in load tests.

### 4.5 Semaphore Bulkhead on Servlet Thread (Sync Path)

For synchronous REST controllers calling a fragile dependency:

```java
@Service
public class LegacyReportService {

    private final Semaphore reportBulkhead = new Semaphore(5);
    private final ReportApiClient client;

    public ReportDto fetchReport(String id) {
        if (!reportBulkhead.tryAcquire()) {
            throw new ServiceUnavailableException("Report service at capacity — retry later");
        }
        try {
            return client.fetch(id);
        } finally {
            reportBulkhead.release();
        }
    }
}
```

The servlet thread is held during the call — but at most **5** concurrent calls to `ReportApiClient` regardless of total Tomcat threads.

---

## Section 5: Thread Pool Bulkhead — Isolation by Executor

### 5.1 Concept

Instead of sharing one pool, assign **dedicated thread pools** per dependency or subsystem:

```
┌─────────────────────────────────────────┐
│              Order Service              │
├─────────────┬─────────────┬─────────────┤
│ paymentPool │ catalogPool │  fraudPool  │
│  (max 10)   │  (max 20)   │   (max 5)   │
└──────┬──────┴──────┬──────┴──────┬──────┘
       ▼             ▼             ▼
   Payment API   Catalog API   Fraud API
```

Slow payment calls consume **payment pool** threads only. Catalog pool remains available.

### 5.2 From BulkheadDemo.java — Isolation Proof

```java
ExecutorService paymentPool = Executors.newFixedThreadPool(2);
ExecutorService catalogPool = Executors.newFixedThreadPool(2);

Future<String> payment = paymentPool.submit(() -> {
    paymentInflight.incrementAndGet();
    latch.await(2, TimeUnit.SECONDS);  // simulate slow payment
    return "payment-ok";
});

Future<String> catalog = catalogPool.submit(() -> "catalog-ok");

// Catalog completes while payment pool is blocked
System.out.println("Catalog still works: " + catalog.get(1, TimeUnit.SECONDS));
```

### 5.3 Thread Pool Sizing Guidelines

| Pool | Sizing heuristic |
|------|------------------|
| CPU-bound work | `cores + 1` |
| I/O-bound (HTTP, DB) | Higher — but cap per dependency, not unbounded |
| Mixed workload | Separate pools — never one `CachedThreadPool` for everything |

**Little's Law:** `concurrency = throughput × latency`

If payment API handles 50 RPS at 200ms latency sustainably: `concurrency ≈ 50 × 0.2 = 10`. Size bulkhead around **downstream capacity**, not your max traffic.

### 5.4 Spring `@Async` with Named Executors

```java
@Configuration
@EnableAsync
public class BulkheadExecutorConfig {

    @Bean(name = "paymentExecutor")
    public Executor paymentExecutor() {
        ThreadPoolTaskExecutor ex = new ThreadPoolTaskExecutor();
        ex.setCorePoolSize(4);
        ex.setMaxPoolSize(8);
        ex.setQueueCapacity(20);
        ex.setThreadNamePrefix("payment-");
        ex.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        ex.initialize();
        return ex;
    }

    @Bean(name = "catalogExecutor")
    public Executor catalogExecutor() {
        ThreadPoolTaskExecutor ex = new ThreadPoolTaskExecutor();
        ex.setCorePoolSize(8);
        ex.setMaxPoolSize(16);
        ex.setQueueCapacity(100);
        ex.setThreadNamePrefix("catalog-");
        ex.setRejectedExecutionHandler(new ThreadPoolExecutor.AbortPolicy());
        ex.initialize();
        return ex;
    }
}

@Service
public class CheckoutFacade {

    @Async("paymentExecutor")
    public CompletableFuture<PaymentResult> chargeAsync(Order order) { ... }

    @Async("catalogExecutor")
    public CompletableFuture<Product> loadProductAsync(String sku) { ... }
}
```

**RejectedExecutionHandler choice:**

| Policy | Behavior | Risk |
|--------|----------|------|
| `AbortPolicy` | Throw — fail fast | Good for bulkhead |
| `CallerRunsPolicy` | Run on caller thread | **Dangerous** — blocks servlet thread, defeats isolation |
| `DiscardPolicy` | Silent drop | Never in production |

Prefer **`AbortPolicy`** or custom handler that returns structured error.

### 5.5 Virtual Threads (Java 21+) — Nuance

Virtual threads make **blocking I/O cheap** — millions of virtual threads can block on HTTP without exhausting OS threads.

**Bulkheads still matter with virtual threads:**

- Downstream capacity is still finite — you can overwhelm payment provider with 10,000 concurrent calls
- Connection pools (JDBC, HTTP) still have max size
- Memory for in-flight request state still grows linearly
- CPU for serialization/deserialization still bounded

Virtual threads reduce the **need for thread pool bulkheads on the caller side**, but **semaphore bulkheads on downstream concurrency** remain essential.

```java
// Still cap concurrent calls to fragile partner even with virtual threads
Semaphore partnerBulkhead = new Semaphore(20);
```

---

## Section 6: Connection Pool Bulkhead — JDBC, HTTP, and gRPC

### 6.1 Connection Pools Are Implicit Bulkheads

Every connection pool is a bulkhead — max connections = max concurrent I/O to that backend.

| Pool | Default trap |
|------|--------------|
| HikariCP | `maximumPoolSize=10` but 200 Tomcat threads → 190 threads block waiting for connection |
| Apache HttpClient | `maxTotal=200` shared across all hosts — one slow host consumes all |
| WebClient (Reactor Netty) | `maxConnections` per remote |

### 6.2 HikariCP — Per-Service Pool Strategy

**Anti-pattern:** Monolith with one giant DB and `maximumPoolSize=100`.

**Better:** Microservices each own a small pool sized to actual DB capacity.

```
PostgreSQL max_connections = 200
5 service instances × Hikari maximumPoolSize=15 = 75 app connections
Headroom for admin, migrations, replicas
```

Rule of thumb: **`pool_size = (core_count × 2) + effective_spindle_count`** (HikariCP wiki) for OLTP — then validate with load test.

### 6.3 Separate Pools Per Database / Schema

In modular monoliths transitioning to services:

```yaml
# application.yml — separate datasources = connection bulkheads
spring:
  datasource:
    orders:
      hikari:
        maximum-pool-size: 10
        pool-name: orders-pool
    reporting:
      hikari:
        maximum-pool-size: 3   # heavy analytics queries capped
        pool-name: reporting-pool
```

Heavy reporting queries cannot exhaust the orders pool.

### 6.4 HTTP Client — Per-Host Bulkhead

```java
@Bean
public CloseableHttpClient paymentHttpClient() {
    PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
    cm.setMaxTotal(50);
    cm.setDefaultMaxPerRoute(10);  // bulkhead per route/host
    return HttpClients.custom()
        .setConnectionManager(cm)
        .evictIdleConnections(30, TimeUnit.SECONDS)
        .build();
}

@Bean
public CloseableHttpClient catalogHttpClient() {
    PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
    cm.setMaxTotal(100);
    cm.setDefaultMaxPerRoute(30);
    return HttpClients.custom().setConnectionManager(cm).build();
}
```

**Never share one `maxTotal=200` pool** across payment (fragile, slow) and catalog (fast, internal).

### 6.5 WebClient / Reactor Netty

```java
ConnectionProvider paymentProvider = ConnectionProvider.builder("payment")
    .maxConnections(20)
    .pendingAcquireMaxCount(50)   // fail when queue full — bulkhead behavior
    .pendingAcquireTimeout(Duration.ofMillis(200))
    .build();

HttpClient httpClient = HttpClient.create(paymentProvider);
WebClient paymentWebClient = WebClient.builder()
    .clientConnector(new ReactorClientHttpConnector(httpClient))
    .baseUrl("https://payment.example.com")
    .build();
```

---

## Section 7: Hystrix History and the Resilience4j Bulkhead

### 7.1 Netflix Hystrix (2012–2018)

Hystrix pioneered **bulkhead + circuit breaker + timeout** in the JVM ecosystem:

- **Thread isolation** — each dependency got its own thread pool (`coreSize`, `maxQueueSize`)
- **Semaphore isolation** — lighter-weight concurrency limit without extra threads
- **Circuit breaker** — integrated failure detection
- **Fallback** — degrade gracefully

```java
// Hystrix (deprecated) — historical reference
@HystrixCommand(
    commandKey = "getPayment",
    threadPoolKey = "paymentPool",
    threadPoolProperties = {
        @HystrixProperty(name = "coreSize", value = "10"),
        @HystrixProperty(name = "maxQueueSize", value = "20")
    },
    fallbackMethod = "getPaymentFallback"
)
public Payment getPayment(String id) { ... }
```

### 7.2 Why Hystrix Was Deprecated

| Factor | Detail |
|--------|--------|
| Maintenance | Netflix placed Hystrix in maintenance mode (2018) |
| Servlet model shift | Reactive / WebFlux / virtual threads reduced thread-isolation value |
| Modularity | Hystrix bundled everything — hard to compose |
| Successor | **Resilience4j** — modular, functional, no AOP requirement |

### 7.3 Hystrix → Resilience4j Mapping

| Hystrix | Resilience4j |
|---------|--------------|
| `@HystrixCommand` thread pool | `ThreadPoolBulkhead` |
| `@HystrixCommand` semaphore | `Bulkhead` (semaphore mode) |
| Circuit breaker | `CircuitBreaker` |
| Timeout | `TimeLimiter` |
| Metrics | Micrometer integration |

### 7.4 Lessons Carried Forward

1. **Thread pool per dependency** was powerful but expensive (thread overhead)
2. **Semaphore isolation** is often sufficient for I/O-bound calls on servlet stacks
3. **Queue size = 0** (fail fast) was Hystrix best practice for user-facing paths — still valid
4. **Metrics dashboard** (Hystrix Turbine) → now Grafana + Micrometer

---

## Section 8: Resilience4j Bulkhead — Semaphore and Thread Pool Modes

### 8.1 Semaphore Bulkhead (Default)

```java
BulkheadConfig config = BulkheadConfig.custom()
    .maxConcurrentCalls(10)
    .maxWaitDuration(Duration.ZERO)  // fail fast — no waiting
    .build();

Bulkhead bulkhead = Bulkhead.of("paymentService", config);

Supplier<PaymentResult> supplier = Bulkhead.decorateSupplier(bulkhead, () ->
    paymentClient.charge(order));

Try<PaymentResult> result = Try.ofSupplier(supplier);
result.onFailure(BulkheadFullException.class, ex ->
    log.warn("Payment bulkhead full — rejecting"));
```

### 8.2 Thread Pool Bulkhead

```java
ThreadPoolBulkheadConfig config = ThreadPoolBulkheadConfig.custom()
    .coreThreadPoolSize(4)
    .maxThreadPoolSize(4)
    .queueCapacity(10)
    .keepAliveDuration(Duration.ofMillis(500))
    .build();

ThreadPoolBulkhead poolBulkhead = ThreadPoolBulkhead.of("paymentPool", config);

Supplier<CompletableFuture<PaymentResult>> futureSupplier =
    ThreadPoolBulkhead.decorateSupplier(poolBulkhead, () ->
        CompletableFuture.supplyAsync(() -> paymentClient.charge(order)));
```

Thread pool bulkhead runs work on **dedicated threads** — useful when caller must not block (e.g., reactive pipeline feeding into blocking adapter).

### 8.3 Semaphore vs Thread Pool Bulkhead — Decision

| Factor | Semaphore | Thread Pool |
|--------|-----------|-------------|
| Overhead | Minimal | Extra threads |
| Caller blocks? | Yes (on servlet thread) | No — work on pool thread |
| Timeout on caller thread | Natural | Need composition with Future/TimeLimiter |
| Servlet / blocking stack | **Preferred** | Use when isolation from caller thread required |
| Reactive (WebFlux) | Preferred | Thread pool bulkhead rarely needed |

### 8.4 BulkheadFullException Handling

```java
@RestControllerAdvice
public class ResilienceExceptionHandler {

    @ExceptionHandler(BulkheadFullException.class)
    public ResponseEntity<ProblemDetail> bulkheadFull(BulkheadFullException ex) {
        ProblemDetail pd = ProblemDetail.forStatus(HttpStatus.SERVICE_UNAVAILABLE);
        pd.setTitle("Dependency at capacity");
        pd.setDetail("Too many concurrent requests to " + ex.getMessage());
        pd.setProperty("retryable", true);
        return ResponseEntity.status(503)
            .header("Retry-After", "5")
            .body(pd);
    }
}
```

Return **503 Service Unavailable** with `Retry-After` — not 500. Clients and load balancers behave differently.

### 8.5 Registry and Shared Configuration

```java
BulkheadRegistry registry = BulkheadRegistry.of(defaultConfig);

Bulkhead paymentBulkhead = registry.bulkhead("payment");
Bulkhead catalogBulkhead = registry.bulkhead("catalog", catalogSpecificConfig);
```

Central registry enables consistent metrics tagging and config hot-reload via Spring Cloud Config.

---

## Section 9: Spring Boot Integration with Resilience4j

### 9.1 Dependencies

```xml
<dependency>
    <groupId>io.github.resilience4j</groupId>
    <artifactId>resilience4j-spring-boot3</artifactId>
</dependency>
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-aop</artifactId>
</dependency>
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-actuator</artifactId>
</dependency>
```

### 9.2 application.yml Configuration

```yaml
resilience4j:
  bulkhead:
    instances:
      paymentService:
        max-concurrent-calls: 10
        max-wait-duration: 0ms
      catalogService:
        max-concurrent-calls: 25
        max-wait-duration: 50ms
  thread-pool-bulkhead:
    instances:
      reportGeneration:
        core-thread-pool-size: 2
        max-thread-pool-size: 2
        queue-capacity: 5
  circuitbreaker:
    instances:
      paymentService:
        failure-rate-threshold: 50
        wait-duration-in-open-state: 30s
        sliding-window-size: 20
```

### 9.3 Annotation-Based Usage

```java
@Service
public class PaymentGatewayService {

    @Bulkhead(name = "paymentService", type = Bulkhead.Type.SEMAPHORE,
              fallbackMethod = "chargeFallback")
    @CircuitBreaker(name = "paymentService", fallbackMethod = "chargeFallback")
    @TimeLimiter(name = "paymentService")
    public CompletableFuture<PaymentResult> charge(Order order) {
        return CompletableFuture.supplyAsync(() -> stripeClient.charge(order));
    }

    private CompletableFuture<PaymentResult> chargeFallback(Order order, Throwable t) {
        if (t instanceof BulkheadFullException) {
            return CompletableFuture.failedFuture(
                new ServiceCapacityException("Payment at capacity"));
        }
        return CompletableFuture.completedFuture(PaymentResult.deferred(order.id()));
    }
}
```

### 9.4 Programmatic — Preferred for Library Code

Annotations hide composition order. For lead-level codebases, prefer explicit decoration:

```java
@Component
public class ResilientPaymentClient {

    private final PaymentClient delegate;
    private final Bulkhead bulkhead;
    private final CircuitBreaker circuitBreaker;

    public ResilientPaymentClient(PaymentClient delegate, BulkheadRegistry bulkheadRegistry,
                                  CircuitBreakerRegistry cbRegistry) {
        this.delegate = delegate;
        this.bulkhead = bulkheadRegistry.bulkhead("paymentService");
        this.circuitBreaker = cbRegistry.circuitBreaker("paymentService");
    }

    public PaymentResult charge(Order order) {
        Supplier<PaymentResult> decorated = Decorators.ofSupplier(() -> delegate.charge(order))
            .withBulkhead(bulkhead)
            .withCircuitBreaker(circuitBreaker)
            .decorate();
        return Try.ofSupplier(decorated).get();
    }
}
```

Testable, explicit, no AOP proxy surprises.

### 9.5 WebClient Filter Integration

```java
@Bean
public WebClient paymentWebClient(BulkheadRegistry bulkheadRegistry) {
    Bulkhead bulkhead = bulkheadRegistry.bulkhead("paymentService");
    return WebClient.builder()
        .baseUrl("${payment.base-url}")
        .filter((request, next) -> {
            return Mono.fromCallable(() -> {
                Bulkhead.decorateCallable(bulkhead, () -> {
                    return next.exchange(request).block();
                }).call();
            }).flatMap(m -> m);
        })
        .build();
}
```

For WebFlux, prefer `transformDeferred` with reactive bulkhead patterns — avoid `.block()` in production reactive chains.

### 9.6 Actuator Endpoints

```yaml
management.endpoints.web.exposure.include: health,metrics,prometheus,bulkhevents,circuitbreakerevents
management.endpoint.health.show-details: always
```

Health contributor shows bulkhead state when configured. Use for debugging — not as primary alert source (use metrics).

---

## Section 10: Production Java Patterns — Shared Pool Anti-Patterns

### 10.1 `ForkJoinPool.commonPool()` for Blocking I/O

```java
// ANTI-PATTERN
CompletableFuture.supplyAsync(() -> httpClient.call());  // uses common pool
```

`commonPool()` is sized for **CPU parallelism**, not blocking I/O. Under load, all async work starves.

**Fix:** Explicit executor per domain.

```java
CompletableFuture.supplyAsync(() -> httpClient.call(), paymentExecutor);
```

### 10.2 `Executors.newCachedThreadPool()` as "Bulkhead"

Unbounded thread creation — under spike, creates thousands of threads → OOM or context switch thrashing. **Not a bulkhead** — no upper bound.

### 10.3 `@Async` Default Executor

Spring's default `@Async` uses `SimpleAsyncTaskExecutor` (new thread per task in some configs) or a single unconfigured pool. Always specify executor name.

### 10.4 CompletableFuture.allOf Without Per-Dependency Limits

```java
// ANTI-PATTERN — 50 parallel downstream calls
List<CompletableFuture<Result>> futures = ids.stream()
    .map(id -> CompletableFuture.supplyAsync(() -> client.fetch(id), sharedPool))
    .toList();
CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
```

Fan-out of 50 concurrent calls bypasses bulkhead if pool is large enough.

**Fix:** Bulkhead + batched processing, or semaphore around `client.fetch`.

### 10.5 Blocking on Reactive Pipeline

```java
// ANTI-PATTERN in WebFlux controller
@GetMapping("/order/{id}")
public Order getOrder(@PathVariable String id) {
    return orderService.findReactive(id).block();  // blocks event loop thread
}
```

Bulkhead the blocking call or move to MVC with bounded servlet threads + semaphore.

### 10.6 Good Pattern — Gateway Service with Bulkheads

```java
@Service
public class OrderOrchestrationService {

    private final Bulkhead paymentBulkhead;
    private final Bulkhead inventoryBulkhead;
    private final Bulkhead shippingBulkhead;

    public OrderResult placeOrder(PlaceOrderCommand cmd) {
        InventoryReservation inv = invoke(inventoryBulkhead,
            () -> inventoryClient.reserve(cmd));
        PaymentCapture pay = invoke(paymentBulkhead,
            () -> paymentClient.capture(cmd, inv));
        ShipmentLabel ship = invoke(shippingBulkhead,
            () -> shippingClient.createLabel(cmd, pay));
        return OrderResult.of(inv, pay, ship);
    }

    private <T> T invoke(Bulkhead bulkhead, Supplier<T> action) {
        return Bulkhead.decorateSupplier(bulkhead, action).get();
    }
}
```

Each dependency independently capped — one slow step does not expand concurrency to others.

---

## Section 11: Sizing Bulkheads — Capacity Planning and Load Testing

### 11.1 Sizing Inputs

| Input | Source |
|-------|--------|
| Downstream max sustainable concurrency | Partner SLA, load test, documentation |
| p99 latency at target load | APM, historical metrics |
| Your thread budget | Tomcat max threads, container CPU |
| Acceptable reject rate | Product decision (503 vs wait) |
| Connection pool size | Hikari / HTTP client config |

### 11.2 Formula Starting Point

```
bulkhead_max_concurrent = downstream_max_rps × downstream_p99_seconds × safety_factor

Example:
  Partner SLA: 100 RPS
  p99 latency: 0.3s
  safety_factor: 0.8 (leave 20% headroom for partner)
  bulkhead = 100 × 0.3 × 0.8 = 24 concurrent calls
```

Round down. Start conservative — easier to increase than recover from cascade.

### 11.3 Load Test Protocol

1. **Baseline** — measure p99 with no bulkhead at target RPS
2. **Introduce bulkhead** — set to 50% of estimated capacity
3. **Ramp** — increase traffic until bulkhead rejections appear
4. **Verify isolation** — confirm unrelated endpoints stay healthy
5. **Chaos** — inject 5s latency on one dependency; verify bulkhead absorbs

Tools: Gatling, k6, JMeter with per-scenario metrics.

### 11.4 Dynamic Sizing — Advanced

Most teams use **static config** with periodic review. Advanced options:

- Spring Cloud Config refresh
- Feature flag for bulkhead limit tuning without redeploy
- Autoscaling downstream → revisit bulkhead quarterly

Avoid autoscaling bulkhead limits in real-time without strong feedback loops — oscillation risk.

### 11.5 Per-Tenant Bulkheads (Multi-Tenant SaaS)

```java
BulkheadRegistry tenantBulkheads = BulkheadRegistry.of(
    BulkheadConfig.custom().maxConcurrentCalls(5).build());

Bulkhead bulkhead = tenantBulkheads.bulkhead("tenant-" + tenantId);
```

Prevents one enterprise customer from exhausting shared downstream capacity.

**Caution:** Unbounded number of tenants → unbounded registry entries. Use LRU cache of bulkhead instances or hash tenant into fixed buckets (e.g., 64 shards).

---

## Section 12: Layered Resilience — Bulkhead + Circuit Breaker + Timeout + Retry

### 12.1 Defense in Depth Diagram

```
                    ┌──────────────────────────────────────┐
  Client request ──►│ Rate Limiter (ingress)               │
                    ├──────────────────────────────────────┤
                    │ Bulkhead (concurrency cap)           │
                    ├──────────────────────────────────────┤
                    │ Circuit Breaker (failure detection)  │
                    ├──────────────────────────────────────┤
                    │ TimeLimiter / Timeout                │
                    ├──────────────────────────────────────┤
                    │ Retry (idempotent ops only)        │
                    ├──────────────────────────────────────┤
                    │ HTTP call → downstream               │
                    └──────────────────────────────────────┘
```

### 12.2 Interaction — Bulkhead Full vs Circuit Open

| State | HTTP response | Client action |
|-------|---------------|---------------|
| Bulkhead full | 503 + Retry-After | Retry with backoff |
| Circuit OPEN | 503 or fallback | Use cache / degraded mode |
| Timeout | 504 Gateway Timeout | Retry if idempotent |
| Success | 200 | Normal |

Differentiate in logs and metrics — triage differs.

### 12.3 Retry + Bulkhead Danger

Retries multiply concurrent attempts:

```
bulkhead=10, retry=3 → effective burst up to 30 attempts if timed out
```

**Fix:**

- Count retry attempts against bulkhead OR
- Use smaller bulkhead when retries enabled OR
- Retry only after bulkhead slot released (outside bulkhead scope)

```java
// Safer: retry wraps bulkhead, not inside it
Retry.decorateSupplier(retry, () ->
    Bulkhead.decorateSupplier(bulkhead, delegate::call).get()
);
```

### 12.4 Full Spring Boot Stack Example

```java
@Configuration
public class PaymentResilienceConfig {

    @Bean
    public PaymentService resilientPaymentService(
            StripePaymentClient client,
            BulkheadRegistry bulkheadRegistry,
            CircuitBreakerRegistry cbRegistry,
            RetryRegistry retryRegistry,
            TimeLimiterRegistry tlRegistry,
            ScheduledExecutorService scheduler) {

        Bulkhead bulkhead = bulkheadRegistry.bulkhead("paymentService");
        CircuitBreaker cb = cbRegistry.circuitBreaker("paymentService");
        Retry retry = retryRegistry.retry("paymentService");
        TimeLimiter tl = tlRegistry.timeLimiter("paymentService");

        Function<Order, CompletableFuture<PaymentResult>> fn = order ->
            CompletableFuture.supplyAsync(() -> client.charge(order));

        return order -> {
            Supplier<CompletableFuture<PaymentResult>> supplier = () -> fn.apply(order);
            Supplier<CompletableFuture<PaymentResult>> withTimeout =
                TimeLimiter.decorateFutureSupplier(tl, supplier);
            Supplier<CompletableFuture<PaymentResult>> withRetry =
                Retry.decorateCompletionStage(retry, scheduler, () -> withTimeout.get());
            Supplier<CompletableFuture<PaymentResult>> withCb =
                CircuitBreaker.decorateCompletionStage(cb, withRetry);
            Supplier<CompletableFuture<PaymentResult>> withBulkhead =
                Bulkhead.decorateCompletionStage(bulkhead, withCb);
            return withBulkhead.get().join();
        };
    }
}
```

Tune order per team standards — document in ADR.

### 12.5 Cross-Reference: Circuit Breaker Playbook

For circuit breaker state machine, half-open probe design, failure rate windows, and slow-call detection, see:

- **`docs/circuit-breaker-expert-playbook.md`**
- **`examples/circuit-breaker/CircuitBreakerDemo.java`**

**Combined incident:** Circuit OPEN on payment + bulkhead full on catalog = two different root causes. Dashboards must tag `resilience4j_bulkhead_available_concurrent_calls` separately from `resilience4j_circuitbreaker_state`.

---

## Section 13: Metrics and Observability

### 13.1 Mandatory Bulkhead Metrics

| Metric | Type | Alert |
|--------|------|-------|
| `bulkhead_concurrent_calls` | Gauge | — |
| `bulkhead_available_concurrent_calls` | Gauge | Sustained zero |
| `bulkhead_max_allowed_concurrent_calls` | Gauge | Config drift detection |
| `bulkhead_rejected_total` | Counter | Spike > baseline |
| `bulkhead_wait_duration_seconds` | Timer | p99 > SLO |

Resilience4j Micrometer binder exposes these automatically with Spring Boot Actuator.

### 13.2 Prometheus Queries

```promql
# Rejection rate per bulkhead (5m window)
rate(resilience4j_bulkhead_calls_total{name="paymentService",kind="failed"}[5m])

# Utilization percentage
1 - (resilience4j_bulkhead_available_concurrent_calls{name="paymentService"}
     / resilience4j_bulkhead_max_allowed_concurrent_calls{name="paymentService"})

# Rejection ratio
sum(rate(resilience4j_bulkhead_calls_total{kind="failed"}[5m]))
/ sum(rate(resilience4j_bulkhead_calls_total[5m]))
```

### 13.3 Structured Logging

```java
@Slf4j
public class BulkheadEventPublisher implements Consumer<BulkheadEvent> {

    @Override
    public void accept(BulkheadEvent event) {
        switch (event.getEventType()) {
            case CALL_REJECTED -> log.warn("bulkhead_rejected name={} type={}",
                event.getBulkheadName(), event.getEventType());
            case CALL_PERMITTED -> { /* debug only — high volume */ }
            default -> log.debug("bulkhead_event name={} type={}",
                event.getBulkheadName(), event.getEventType());
        }
    }
}
```

Correlate with `traceId`, `dependency`, `tenantId` in MDC.

### 13.4 Dashboard Panels

1. **Bulkhead utilization** — heatmap per dependency
2. **Rejection rate** — stacked by service instance
3. **Tomcat active threads** — overlay with bulkhead rejections (prove isolation)
4. **Downstream latency** — compare bulkhead-saturated vs healthy periods
5. **503 rate** — split by `bulkhead_full` vs `circuit_open` tag

### 13.5 SLIs and SLOs

| SLI | Definition |
|-----|------------|
| Bulkhead rejection ratio | `rejected / (permitted + rejected)` |
| Isolation effectiveness | Unrelated endpoint error rate during dependency degradation |
| Time-to-recover | Duration from bulkhead saturation to <5% rejection after dependency heals |

**SLO example:** Catalog search p99 < 200ms even when payment bulkhead rejection rate > 50%.

### 13.6 Distributed Tracing

Tag spans when bulkhead rejects:

```java
span.setAttribute("bulkhead.name", "paymentService");
span.setAttribute("bulkhead.rejected", true);
span.setStatus(StatusCode.ERROR, "BulkheadFull");
```

Jaeger/Tempo trace shows rejected calls without polluting downstream traces.

---

## Section 14: Production Pitfalls and War Stories

### 14.1 Pitfall: Bulkhead Too Large — No Protection

Setting `maxConcurrentCalls=200` when Tomcat has 200 threads — bulkhead is meaningless.

**Symptom:** "We added bulkhead but still had cascade."

**Fix:** Size relative to **downstream capacity**, not thread count.

### 14.2 Pitfall: Bulkhead Too Small — False 503s

`maxConcurrentCalls=2` for a dependency handling 500 RPS at 50ms → needs ~25 concurrent.

**Symptom:** Constant 503s under normal load.

**Fix:** Load test, increase limit, verify downstream headroom.

### 14.3 Pitfall: Blocking acquire on Servlet Threads

`bulkhead.acquire()` with no timeout on user-facing path — threads block, pool still exhausts.

**Fix:** `tryAcquire()` or `tryAcquire(50ms)` + 503.

### 14.4 Pitfall: CallerRunsPolicy Hides Rejection

Queue fills → policy runs task on servlet thread → **bulkhead bypassed**.

**Fix:** `AbortPolicy` + explicit exception handling.

### 14.5 Pitfall: Shared HTTP Connection Pool

All WebClients share one connection provider — classic bulkhead miss.

**Fix:** Per-dependency `ConnectionProvider` (Section 6).

### 14.6 Pitfall: Retry Storm Inside Bulkhead

3 retries × 10 bulkhead slots = 30 effective calls during timeout storm.

**Fix:** Retry outside bulkhead or reduce retry count when bulkhead utilization > 80%.

### 14.7 War Story — Black Friday Checkout

**Context:** E-commerce monolith, single `ExecutorService(100)` for all integrations.

**Event:** Payment provider latency 50ms → 3000ms under load.

**Result:** All 100 threads blocked on payment. Product page (reads catalog from cache + DB) failed — DB connections held by blocked checkout threads. Revenue loss 40 minutes.

**Fix deployed:** Semaphore bulkhead (15) on payment client + separate `catalogExecutor(30)` + Hikari pool split. Next peak: checkout degraded (503 with retry), browse remained healthy.

### 14.8 War Story — Reporting Job Ate the Database

Nightly report job used same Hikari pool as OLTP. Long-running queries held 40 connections. Checkout failed with "Connection is not available."

**Fix:** Separate read replica datasource with `maximumPoolSize=5` for reporting — connection bulkhead by topology.

### 14.9 Pitfall: No Fallback UX

Bulkhead rejects with generic 500 — client retries aggressively, amplifying load.

**Fix:** 503 + `Retry-After` + user message "High demand — try again in a few seconds" + client exponential backoff.

---

## Section 15: Production Issue Runbook

### 15.1 Symptom: Spike in `bulkhead_rejected_total`

**Severity:** Medium (may be expected under load) or High (if sustained)

**Steps:**

1. Check downstream dependency latency and error rate in APM
2. Compare bulkhead utilization: `available / max` → if zero sustained, bulkhead is bottleneck OR downstream is slow
3. If downstream healthy → bulkhead undersized; consider temporary limit increase via config refresh
4. If downstream slow → do NOT only increase bulkhead; engage circuit breaker, contact partner, enable degraded mode
5. Verify unrelated endpoints healthy — if not, isolation failed; check for shared pools

### 15.2 Symptom: Full site slow — one dependency degraded

**Severity:** Critical

**Steps:**

1. Identify shared resource: thread dump (`jstack`) — look for blocked threads on same external call
2. Confirm missing bulkhead: grep codebase for shared `ExecutorService`, shared `WebClient`, shared `HttpClient`
3. **Mitigation:** Enable/emergency lower bulkhead limit on bad dependency (fail fast frees threads faster than slow calls)
4. Open circuit breaker manually if supported
5. Scale horizontally — **temporary** relief only if bulkhead exists; without bulkhead, scaling amplifies downstream load

### 15.3 Symptom: 503 only on one feature

**Severity:** Low-Medium (isolation working as designed)

**Steps:**

1. Confirm bulkhead rejections correlate with feature
2. Communicate degraded mode to stakeholders
3. Tune bulkhead or fix downstream
4. Verify fallback path works

### 15.4 Thread Dump Analysis Cheatsheet

```
# Look for:
"pool-N-thread-M" blocked on socketRead0  → HTTP wait
"HikariPool-1 connection adder" waiting   → pool exhaustion
"http-nio-8080-exec-N" BLOCKED on Semaphore.acquire  → blocking bulkhead — consider fail-fast
```

### 15.5 Emergency Config Change

```yaml
# Spring Cloud Config / actuator refresh
resilience4j.bulkhead.instances.paymentService.max-concurrent-calls: 5  # reduce to fail faster
```

Post-incident: restore after dependency recovers; document in change log.

### 15.6 Post-Incident Checklist

- [ ] Bulkhead metrics in incident timeline?
- [ ] Was rejection distinguishable from circuit open?
- [ ] Did isolation protect unrelated features?
- [ ] Sizing validated against downstream SLA?
- [ ] Runbook step gaps documented?

---

## Section 16: Decision Framework: When to Use and When NOT To

### 16.1 Decision Tree

```
Multiple dependencies or subsystems share finite resources?
├── No → Bulkhead optional (still use connection pool limits)
└── Yes
    ├── Can one slow/failing dependency block others today?
    │   └── Yes → ADD BULKHEAD
    └── Dependency mix
        ├── I/O-bound blocking calls → Semaphore or thread pool bulkhead
        ├── Reactive non-blocking → Semaphore on concurrent subscriptions
        └── Database → Separate pools / read replicas
```

### 16.2 When to Use Bulkhead

| Scenario | Bulkhead type |
|----------|---------------|
| Microservice calls 5+ downstream APIs | Semaphore per client |
| Modular monolith with varied SLAs | Thread pool per domain |
| Multi-tenant SaaS | Per-tenant or sharded bulkhead |
| Batch + OLTP same JVM | Separate executors + datasource pools |
| Partner API with strict concurrency SLA | Semaphore sized to SLA |
| Saga orchestrator calling participants | Bulkhead per participant (see `docs/saga-expert-playbook.md`) |

### 16.3 When NOT To Use Bulkhead

| Condition | Why | Alternative |
|-----------|-----|-------------|
| Single dependency, single thread pool, low traffic | Overhead > benefit | Timeout + circuit breaker only |
| Strict "never reject" SLA | Bulkhead rejects by design | Queue + backpressure (different tradeoff) |
| CPU-bound work on fixed cores | Threads aren't the bottleneck | Partition workloads, K8s CPU limits |
| Downstream scales elastically with your traffic | Rare in practice | Auto-scale + monitor |
| Team cannot operate 503/degraded UX | Rejections need product handling | Fix UX maturity first |
| Problem is logic bugs / data corruption | Bulkhead doesn't help | Fix root cause |

### 16.4 Bulkhead vs Horizontal Scale

Scaling **out** adds capacity but does not isolate faults. 10 pods without bulkhead → 10× concurrent hammering of slow dependency.

**Rule:** Bulkhead first for isolation; scale for throughput.

### 16.5 Container Limits as Bulkheads

Kubernetes `resources.limits.cpu` and `memory` are **process-level bulkheads**. Complement application bulkheads:

```yaml
resources:
  limits:
    cpu: "2"
    memory: 1Gi
  requests:
    cpu: "1"
    memory: 512Mi
```

 JVM heap must fit within memory limit minus native overhead.

---

## Section 17: Lead Interview Questions & Answers

### Category A: Fundamentals

**Q1: What is the Bulkhead pattern?**

A: Bulkhead partitions shared resources (threads, connections, memory budget) so overload or failure in one compartment cannot sink the entire system. Named after ship watertight compartments. In Java, implemented via semaphores, dedicated thread pools, or connection pool limits. Our repo demo `BulkheadDemo.java` shows semaphore and thread pool isolation.

**Q2: What problem does bulkhead solve that circuit breaker does not?**

A: Circuit breaker stops calling a **failing** dependency after error threshold. Bulkhead limits **concurrent calls** regardless of success/failure — critical when dependency is **slow but not erroring**. A dependency with 100% success and 10s latency can exhaust all threads; circuit breaker may never open if slow-call threshold isn't configured. Use both.

**Q3: Explain semaphore vs thread pool bulkhead.**

A: Semaphore caps concurrent executions — caller thread often runs the work (blocking model). Thread pool bulkhead runs work on a dedicated pool — isolates caller threads from blocking. Semaphore: lower overhead, good for servlet stacks. Thread pool: when caller must not block or you need queue-based rejection. Resilience4j supports both.

**Q4: What is blast radius?**

A: The portion of the system impacted when one component fails. Without bulkhead, one slow payment API can take down product search — blast radius is the whole service. With bulkhead, blast radius is checkout only. Lead architects minimize blast radius by default.

**Q5: How does bulkhead relate to connection pooling?**

A: Connection pools are implicit bulkheads — `maximumPoolSize` caps concurrent DB connections. Problem: 200 servlet threads contending for 10 connections — 190 block. Combine connection pool limits with application bulkhead so threads fail fast instead of blocking on `getConnection()`. HikariCP `connectionTimeout` should be short.

### Category B: Implementation

**Q6: Walk through BulkheadDemo.java scenarios.**

A: Scenario 1: 8 calls, pool 4, 400ms latency — without bulkhead, tasks queue, some timeout. Scenario 2: semaphore max 2 — only 2 calls in flight to slow dependency, others get `BulkheadRejectedException` immediately. Scenario 3: payment pool blocked on latch, catalog pool on separate executor completes — proves thread pool isolation. Run: `javac examples/bulkhead/BulkheadDemo.java && java -cp examples/bulkhead BulkheadDemo`.

**Q7: Why use tryAcquire() instead of acquire()?**

A: `acquire()` blocks until permit available — on servlet thread, you're still consuming Tomcat thread while waiting. Under load, blocked threads pile up and mimic the original problem. `tryAcquire()` fails fast → 503 → frees thread immediately → rest of service stays healthy.

**Q8: How do you size a bulkhead?**

A: Start from downstream capacity: `max_concurrent ≈ partner_max_rps × p99_latency × 0.8`. Cross-check against your thread budget and connection pool. Load test with chaos (injected latency). Prefer conservative initial value. Section 11 details the protocol.

**Q9: Configure bulkhead in Spring Boot with Resilience4j.**

A: Add `resilience4j-spring-boot3`, define `resilience4j.bulkhead.instances.{name}.max-concurrent-calls` in YAML, apply `@Bulkhead(name="...")` or programmatic `Bulkhead.decorateSupplier`. Enable actuator metrics. Return 503 on `BulkheadFullException`. See Section 9.

**Q10: What rejected execution handler for thread pool bulkhead?**

A: `AbortPolicy` — throws `RejectedExecutionException`, map to 503. Avoid `CallerRunsPolicy` on servlet-bound work — it executes on caller thread, defeating isolation. Never `DiscardPolicy` silently.

### Category C: Comparisons and Composition

**Q11: Bulkhead vs rate limiter — when use which?**

A: Rate limiter: max requests per second (velocity). Bulkhead: max concurrent in-flight (concurrency). Slow calls tie up concurrency without high RPS — bulkhead catches this; rate limiter doesn't. Use rate limiter at API gateway for ingress; bulkhead per downstream dependency.

**Q12: What order should resilience decorators use?**

A: Team-dependent; common: RateLimiter → Bulkhead → CircuitBreaker → TimeLimiter → Retry (retries often outermost or carefully scoped). Document in ADR. Key rule: don't retry inside bulkhead without accounting for multiplied concurrency. Section 12.

**Q13: How does bulkhead interact with circuit breaker OPEN state?**

A: When circuit is OPEN, calls fail fast without consuming bulkhead slot (if breaker is inner). If bulkhead is inner, OPEN calls may still acquire bulkhead briefly — prefer breaker to short-circuit before bulkhead when possible. Monitor both metrics separately.

**Q14: Compare Hystrix thread isolation to Resilience4j semaphore bulkhead.**

A: Hystrix spawned dedicated threads per dependency — strong isolation, high thread overhead. Resilience4j semaphore bulkhead on modern stacks (virtual threads, improved async) achieves concurrency limiting with minimal overhead. Thread pool bulkhead in Resilience4j when you need Hystrix-like isolation.

**Q15: Do virtual threads eliminate need for bulkheads?**

A: No. Virtual threads reduce OS thread exhaustion on caller side, but downstream capacity, connection pools, memory, and partner SLAs still require concurrency limits. Semaphore bulkhead on outbound calls remains necessary.

### Category D: Production and Operations

**Q16: What metrics alert on for bulkhead health?**

A: `bulkhead_rejected_total` spike, sustained `available_concurrent_calls == 0`, rejection ratio > SLO, correlation with downstream latency. Also track isolation SLI: unrelated endpoint error rate during dependency degradation. Section 13.

**Q17: Site-wide outage but one dependency slow — diagnosis?**

A: Thread dump — likely all servlet threads blocked on same downstream call. Check for shared executor, shared HttpClient pool, missing bulkhead. Mitigation: emergency fail-fast bulkhead limit, circuit breaker open, scale only if bulkheads exist. Section 15.

**Q18: Customer sees 503 on checkout but browse works — good or bad?**

A: Good — bulkhead isolation working. Checkout bulkhead saturated or circuit open on payment; catalog path unaffected. Tune bulkhead size, improve degraded UX, fix downstream. Better than full site down.

**Q19: How handle bulkhead rejection in UX?**

A: HTTP 503 with `Retry-After`, structured error body `{ "retryable": true }`, client exponential backoff. Never generic 500. Product copy explaining high demand. Track rejection rate in analytics.

**Q20: Per-tenant bulkhead design for SaaS?**

A: Cap concurrent calls per tenant to prevent noisy neighbor. Use bulkhead registry keyed by tenantId — bucket into fixed shards (e.g., 64) to avoid unbounded registry. Combine with rate limiter at gateway. Section 11.5.

### Category E: Architecture and Design

**Q21: Where bulkhead in saga orchestrator?**

A: Each outbound participant call wrapped in named bulkhead — payment, inventory, shipping independently capped. Prevents one stuck participant from blocking threads needed to compensate others. Complements saga timeout policies in `docs/saga-expert-playbook.md`.

**Q22: Bulkhead in API Gateway vs service?**

A: Both layers — gateway: tenant/global rate limits. Service: per-dependency concurrency knowing downstream capacity. Gateway doesn't know internal dependency topology. Defense in depth.

**Q23: Design bulkheads for order service calling 4 downstream APIs.**

A: Semaphore bulkhead per client: payment(10), inventory(20), fraud(5), shipping(15). Separate HTTP connection providers matching limits. Circuit breaker + timeout on each. Orchestration in `OrderOrchestrationService` with explicit decoration. Load test with 5s latency injected on payment only — verify catalog path p99 unchanged.

**Q24: When would you NOT add bulkhead?**

A: Single downstream, low traffic, team can't handle 503 UX, or problem is bugs not resource coupling. Don't add complexity without shared resource risk. Section 16.

**Q25: Connection pool sizing vs bulkhead sizing — conflict?**

A: Bulkhead max concurrent should be ≤ connection pool max for that dependency. If bulkhead=30 but Hikari max=10, 20 threads block on getConnection — false sense of security. Align: bulkhead ≤ pool ≤ downstream capacity.

### Category F: Curveball and Advanced

**Q26: Bulkhead vs backpressure in reactive streams?**

A: Reactive backpressure (Request(n)) controls in-flight elements in a pipeline. Bulkhead is explicit partition across dependencies. In WebFlux, use `flatMap(concurrency)` as bulkhead-like limit. Semaphore bulkhead when bridging reactive-to-blocking adapter.

**Q27: How test bulkhead behavior?**

A: Unit: mock slow dependency, assert N concurrent max, assert N+1 rejected. Integration: WireMock delay, load test with Gatling asserting 503 rate and healthy endpoint p99. Chaos: toxiproxy latency injection. Repo demo is minimal unit reference.

**Q28: Bulkhead for Kafka consumers?**

A: Limit concurrent message processing per topic/partition — separate thread pools per consumer group handler. Prevents one slow handler from blocking partition consumption across message types. Semaphore on processing logic per dependency invoked from consumer.

**Q29: Your team wants unbounded queue instead of bulkhead reject — opinion?**

A: Unbounded queue hides backpressure until OOM or multi-minute latency — worse UX than fast 503. Bounded queue + fail fast = predictable degradation. If business rejects 503, negotiate queued response with **visible** wait time and queue depth metric — still bounded.

**Q30: Design resilience for payment provider with 429 rate limits AND concurrency SLA.**

A: Rate limiter (respect 429 headers), semaphore bulkhead (concurrency SLA), circuit breaker ( sustained 5xx), timeout (2s), idempotent retry with jitter on 429 only. Separate metrics per layer. Fallback: queue payment for async processing.

**Q31: jstack shows threads blocked on Hikari getConnection — bulkhead fix?**

A: Shorten Hikari `connectionTimeout`, add semaphore bulkhead before DB access path, split pools for heavy vs light queries, consider read replica for reads. Root cause: threads >> pool size without fail-fast.

**Q32: Difference between bulkhead and pool isolation in Kubernetes?**

A: K8s resource limits bulkhead the **pod**. Application bulkhead partitions **within** the pod across dependencies. Both needed — pod limit prevents one service consuming cluster; app bulkhead prevents one dependency consuming pod.

---

## Section 18: Appendix: Quick Reference Cheat Sheet

### Bulkhead Type Selection

```
Blocking servlet + outbound HTTP?
  └── Semaphore bulkhead (fail-fast tryAcquire)

Need caller thread isolation?
  └── Thread pool bulkhead (AbortPolicy)

Database access?
  └── Hikari maximumPoolSize + short connectionTimeout

Reactive WebFlux?
  └── flatMap concurrency + ConnectionProvider maxConnections

Virtual threads?
  └── Semaphore on downstream concurrency (still required)
```

### Resilience Stack Checklist

| Layer | Tool | Config starting point |
|-------|------|----------------------|
| Ingress | Rate limiter / API GW | 100 req/s per client |
| Concurrency | Bulkhead semaphore | 10–25 per dependency |
| Failure | Circuit breaker | 50% failure, 30s open |
| Latency | TimeLimiter | p99 × 2 |
| Retry | Retry (idempotent only) | 3 attempts, exponential backoff |
| Pool | Hikari / HTTP client | bulkhead ≤ pool size |

### HTTP Status Codes

| Condition | Status |
|-----------|--------|
| Bulkhead full | 503 Service Unavailable |
| Circuit open | 503 or 200 with fallback body |
| Timeout | 504 Gateway Timeout |
| Rate limited | 429 Too Many Requests |

### Key Files in This Repo

```bash
# Bulkhead demo
javac examples/bulkhead/BulkheadDemo.java && java -cp examples/bulkhead BulkheadDemo

# Circuit breaker companion
javac examples/circuit-breaker/CircuitBreakerDemo.java && \
  java -cp examples/circuit-breaker CircuitBreakerDemo
```

### Related Documentation

| Topic | Location |
|-------|----------|
| Circuit breaker deep dive | `docs/circuit-breaker-expert-playbook.md` |
| Saga orchestrator resilience | `docs/saga-expert-playbook.md` |
| Metrics and alerting | `docs/metrics-observability-playbook.md` |
| Runnable bulkhead example | `examples/bulkhead/BulkheadDemo.java` |

---

## Section 19: How to Talk About Bulkheads in an Interview

> Plain English. Short sentences. How you'd explain it to a teammate over coffee.

---

### "What is the bulkhead pattern?"

It's like walls on a ship. If one part floods, the rest stays dry.

In software, it means you split up your resources — threads, connections — so one slow or broken dependency can't eat everything.

Without it, one slow payment API can block all your threads and take down product search too. Even though search has nothing to do with payments.

---

### "How is that different from a circuit breaker?"

Circuit breaker says — "this service is failing a lot, stop calling it for a while."

Bulkhead says — "I'll only allow 10 calls to this service at the same time, period. Success or failure."

If payment is slow but still returning 200 OK, the circuit breaker might never trip. But bulkhead stops 500 threads from all waiting on payment at once.

Use both. They solve different problems.

See `docs/circuit-breaker-expert-playbook.md` for the circuit breaker side.

---

### "Semaphore vs thread pool bulkhead?"

Semaphore is a counter. Before you call payment, you grab a slot. When done, you release it. If no slots left, reject immediately.

Thread pool bulkhead means payment gets its own small pool of threads. Catalog gets a different pool. They can't steal from each other.

Semaphore is lighter. Thread pool is stronger isolation. For most Spring REST apps, semaphore fail-fast is enough.

---

### "Why fail fast instead of waiting?"

If you wait for a bulkhead slot on a servlet thread, you're still stuck. Threads pile up. The whole server slows down.

Better to say "sorry, we're at capacity" right away with a 503. User can retry. Meanwhile other features keep working.

---

### "How do you pick the bulkhead size?"

Look at what the downstream can handle. If they say 100 requests per second and latency is around 200 milliseconds, you need roughly 20 concurrent slots.

Then load test it. Start conservative. Easier to bump up than to recover from a cascade outage.

Also — your bulkhead can't be bigger than your connection pool. Otherwise threads block waiting for connections.

---

### "What about virtual threads — do we still need bulkheads?"

Virtual threads help with blocking without running out of OS threads. That's nice.

But the payment provider still can't handle 10,000 simultaneous calls. Your database still has a connection limit. Your memory still fills up.

So yes — you still cap concurrent calls to fragile partners.

---

### "What metrics do you watch?"

How many calls are in flight. How many got rejected. When rejections spike.

The big proof it worked: during a payment outage, catalog latency stays normal. That's the isolation test.

---

### "When would you NOT use a bulkhead?"

If you only call one backend and traffic is low — probably overkill.

If the business can't tolerate any 503 ever — you need a different design, like async queueing. Bulkhead rejects by design.

If the real problem is a bug or missing database index — fix that first. Bulkhead won't help.

---

### "Tell me about a production incident."

We had one pool for every outbound integration. Payment slowed down during a sale. Every thread blocked waiting on payment. Product pages died too.

Fix was semaphore on payment — max 15 concurrent — and a separate thread pool for catalog calls. Next sale, checkout showed "try again" for some users, but browsing worked fine. Revenue on browse path saved.

That's the tradeoff bulkhead gives you — partial degradation instead of total outage.

---

### Quick Answers

| Question | Say this |
|----------|----------|
| What is bulkhead? | Split shared resources so one failure doesn't sink everything |
| Ship metaphor? | Watertight compartments — one floods, others stay dry |
| vs Circuit breaker? | Breaker stops calling failures; bulkhead limits concurrent calls always |
| vs Rate limiter? | Limiter caps requests per second; bulkhead caps in-flight at same time |
| Semaphore bulkhead? | Counter — grab slot, call, release; reject if full |
| Thread pool bulkhead? | Separate pool per dependency — can't steal threads |
| Fail fast or wait? | Fail fast — 503 + Retry-After; don't block servlet threads |
| How to size? | Downstream capacity × latency × safety factor; then load test |
| Spring Boot? | Resilience4j — YAML config + `@Bulkhead` or programmatic decorate |
| Virtual threads? | Still need bulkhead for downstream and connection limits |
| Key metric? | Rejection rate + proof unrelated endpoints stay healthy |
| HTTP status? | 503 Service Unavailable — not 500 |
| With circuit breaker? | Layer both — see circuit-breaker playbook |
| Connection pools? | They're implicit bulkheads — align pool size with bulkhead |
| Hystrix? | Deprecated — use Resilience4j instead |
| When NOT to use? | Single dependency, low traffic, or rejection unacceptable without UX plan |
| Repo example? | `examples/bulkhead/BulkheadDemo.java` |
| Run command? | `javac examples/bulkhead/BulkheadDemo.java && java -cp examples/bulkhead BulkheadDemo` |

---

*End of Bulkhead Pattern — Expert Playbook*
