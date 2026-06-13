# Circuit Breaker — Expert Playbook (Lead/Architect, Java, 10+ Years)

A comprehensive end-to-end reference covering the Circuit Breaker pattern from theory to production-grade implementation. Includes the CLOSED → OPEN → HALF_OPEN state machine, fail-fast semantics, half-open probe design, Resilience4j + Spring Boot integration (`@CircuitBreaker`, fallbacks), interaction with retry and timeout, bulkhead isolation, Micrometer metrics, production runbooks, and 25+ lead-level interview Q&As. Sourced from Michael Nygard's *Release It!*, Netflix Hystrix (historical), Resilience4j documentation, Spring Cloud Circuit Breaker guides, and real-world war stories from payment, inventory, and third-party API integrations.

**Runnable example in this repo:**

- `examples/circuit-breaker/CircuitBreakerDemo.java` — minimal CLOSED/OPEN/HALF_OPEN state machine with fail-fast and caller-side fallback

**Related playbooks:**

- [Saga Pattern — Expert Playbook](saga-expert-playbook.md) — long-running flows where circuit breakers protect sync orchestration steps
- [Strangler Fig — Expert Playbook](strangler-fig-playbook.md) — facade routing with circuit breaker fallback to legacy
- [Java Modern Concurrency & Streams Playbook](java-modern-concurrency-streams-playbook.md) — bulkhead with semaphore pattern
- [Metrics & Observability Playbook](metrics-observability-playbook.md) — circuit breaker state gauges in Grafana

---

## Table of Contents

1. [What Is a Circuit Breaker?](#1-what-is-a-circuit-breaker)
2. [The Problem: Cascading Failure](#2-the-problem-cascading-failure)
3. [Circuit Breaker States — CLOSED, OPEN, HALF_OPEN](#3-circuit-breaker-states--closed-open-half_open)
4. [State Machine Semantics and Transitions](#4-state-machine-semantics-and-transitions)
5. [From Scratch — CircuitBreakerDemo Walkthrough](#5-from-scratch--circuitbreakerdemo-walkthrough)
6. [Fail-Fast and Graceful Degradation](#6-fail-fast-and-graceful-degradation)
7. [Half-Open Probes — Recovery Without Thundering Herd](#7-half-open-probes--recovery-without-thundering-herd)
8. [Resilience4j Fundamentals](#8-resilience4j-fundamentals)
9. [Spring Boot Integration — @CircuitBreaker and Fallbacks](#9-spring-boot-integration--circuitbreaker-and-fallbacks)
10. [Interaction with Retry and Timeout](#10-interaction-with-retry-and-timeout)
11. [Bulkhead — Isolation and Relationship to Circuit Breaker](#11-bulkhead--isolation-and-relationship-to-circuit-breaker)
12. [Production Configuration Tuning](#12-production-configuration-tuning)
13. [Metrics, Observability, and Alerting (Micrometer)](#13-metrics-observability-and-alerting-micrometer)
14. [Production Pitfalls and War Stories](#14-production-pitfalls-and-war-stories)
15. [Production Issue Runbook](#15-production-issue-runbook)
16. [Decision Framework: When to Use and When NOT To](#16-decision-framework-when-to-use-and-when-not-to)
17. [Circuit Breaker in Larger Architectures](#17-circuit-breaker-in-larger-architectures)
18. [Lead Interview Questions & Answers](#18-lead-interview-questions--answers)
19. [How to Talk About Circuit Breakers in an Interview](#19-how-to-talk-about-circuit-breakers-in-an-interview)

---

## 1. What Is a Circuit Breaker?

### 1.1 Definition

A **circuit breaker** is a stability pattern that wraps calls to an unreliable dependency. It monitors outcomes (success, failure, slow responses) and **stops forwarding traffic** when failure rates exceed a threshold — just like an electrical circuit breaker trips to prevent fire.

Instead of every caller hammering a dying service and waiting for timeouts, the breaker **fails fast** locally and optionally returns a **fallback** response.

### 1.2 Core Terminology

| Term | Definition |
|------|------------|
| **Protected call** | The remote invocation wrapped by the breaker (HTTP, gRPC, DB, message publish) |
| **CLOSED** | Normal operation — calls pass through; failures are counted |
| **OPEN** | Breaker tripped — calls rejected immediately without hitting the dependency |
| **HALF_OPEN** | Recovery probe state — limited calls allowed to test if dependency recovered |
| **Failure threshold** | Count or rate of failures/slow calls that trips OPEN |
| **Wait duration in OPEN** | How long to stay OPEN before allowing a HALF_OPEN probe |
| **Fallback** | Degraded response when call is not permitted or fails |
| **CallNotPermittedException** | Thrown when breaker is OPEN (or HALF_OPEN saturated) |

### 1.3 Where Circuit Breakers Live

```
┌──────────────┐     ┌─────────────────┐     ┌──────────────────┐
│   Client     │────▶│ Circuit Breaker │────▶│ Remote Dependency│
│ (your service)│     │  (in-process)   │     │ (HTTP/gRPC/DB)   │
└──────────────┘     └─────────────────┘     └──────────────────┘
                              │
                              ▼
                     Fallback / cached data
```

Circuit breakers are **client-side** (or gateway-side) — they protect **your** threads, connection pools, and user experience. They do not fix the downstream service; they **contain blast radius**.

### 1.4 Circuit Breaker vs Related Patterns

| Pattern | Purpose | Relationship |
|---------|---------|--------------|
| **Retry** | Transient failure recovery | Runs *inside* or *before* breaker depending on stack order |
| **Timeout** | Bound wait time per attempt | Defines what "slow" means for slow-call rate |
| **Bulkhead** | Limit concurrent calls | Isolates resource pools; complements breaker |
| **Rate limiter** | Cap requests per time window | Throttles traffic; breaker reacts to *outcomes* |
| **Fallback / cache** | Degraded UX | Invoked when breaker rejects or call fails |

---

## 2. The Problem: Cascading Failure

### 2.1 The Death Spiral

Consider a checkout service calling inventory, payment, and shipping APIs. Inventory DB is slow (disk full, lock contention). Each checkout thread blocks 30 seconds waiting for inventory. Thread pool exhausts. Checkout stops accepting new requests — **even though payment and shipping are healthy**.

```
Inventory DB slow (500ms → 30s)
        ↓
Checkout threads blocked waiting
        ↓
Thread pool saturated (200/200)
        ↓
Checkout returns 503 to ALL users
        ↓
Retries from clients amplify load
        ↓
Inventory receives MORE requests while recovering LESS
        ↓
CASCADE: entire platform appears down
```

This is **cascading failure** — one weak dependency takes down callers, which take down their callers.

### 2.2 Why Timeouts Alone Are Not Enough

Timeouts prevent infinite waits, but under load:

- 10,000 requests × 5s timeout = 50,000 thread-seconds of pain
- Each timed-out request still **hit** the dying service
- Retry storms multiply effective load by retry count

Circuit breakers add **proactive load shedding**: once failure rate is high, stop calling entirely for a cooling-off period.

### 2.3 Real-World Triggers

| Trigger | Symptom | Without breaker |
|---------|---------|-----------------|
| DB connection pool exhausted | Every query waits | All API threads blocked |
| Third-party API 500 storm | Timeouts everywhere | Your service OOM or 503 |
| Network partition to one AZ | Half your pods fail calls | Retry loop amplifies |
| Deployment bad config | Dependency returns 503 | Callers retry → DDoS themselves |
| GC pause on dependency | Latency spikes | Caller thread pools fill |

### 2.4 The Lead Architect Framing

> "We don't add circuit breakers because we expect dependencies to fail. We add them because **when** they fail, we refuse to let that failure define our availability."

Measure **dependency health per outbound integration**, not just service-level uptime.

---

## 3. Circuit Breaker States — CLOSED, OPEN, HALF_OPEN

### 3.1 State Overview

```
                    failures exceed threshold
         ┌──────────────────────────────────────┐
         │                                      ▼
    ┌─────────┐   wait duration elapsed   ┌─────────┐
    │ CLOSED  │◀──── probe succeeds ──────│HALF_OPEN│
    │ (normal)│                           │ (probe) │
    └─────────┘                           └─────────┘
         │                                      │
         │ failures exceed threshold            │ probe fails
         └──────────────────────────────────────┼──────────▶ ┌──────┐
                                                └──────────▶ │ OPEN │
                                                             │(reject)│
                                                             └──────┘
```

### 3.2 CLOSED — Business as Usual

- All calls pass through to the dependency.
- Successes reset or decay failure counters (implementation-dependent).
- Failures and slow calls increment counters.
- When sliding window shows failure rate ≥ threshold → transition to **OPEN**.

**Key insight:** CLOSED is not "healthy" — it is "still willing to try."

### 3.3 OPEN — Fail Fast

- Calls are **rejected immediately** with `CallNotPermittedException` (Resilience4j) or equivalent.
- Zero network I/O to the dependency for rejected calls.
- Protects downstream from retry amplification and protects caller thread pools.
- After `waitDurationInOpenState` → transition to **HALF_OPEN**.

**User experience:** Fast error or fallback — not 30-second spinner.

### 3.4 HALF_OPEN — Cautious Recovery

- Allows a **limited number** of trial calls (often 1, sometimes N).
- **Probe succeeds** → CLOSED (dependency recovered).
- **Probe fails** → OPEN (back to cooling-off).
- Prevents **thundering herd** when dependency flickers online.

Our repo demo enforces single probe:

```java
// From CircuitBreakerDemo.java — only one HALF_OPEN probe at a time
if (state == State.HALF_OPEN) {
    if (halfOpenProbeInFlight) {
        throw new CallNotPermittedException("Circuit HALF_OPEN — probe already running");
    }
    halfOpenProbeInFlight = true;
}
```

### 3.5 State Comparison Table

| State | Calls permitted? | Hits dependency? | Typical latency |
|-------|------------------|--------------------|-----------------|
| CLOSED | Yes (all) | Yes | Normal + timeout bound |
| OPEN | No | No | Microseconds (local reject) |
| HALF_OPEN | Limited (probe) | Yes (probe only) | Normal for probe; others rejected |

---

## 4. State Machine Semantics and Transitions

### 4.1 What Counts as a Failure?

Configurable per integration:

| Failure type | Typical treatment |
|--------------|-------------------|
| HTTP 5xx | Failure |
| HTTP 429 (rate limited) | Failure or separate policy |
| HTTP 4xx (client error) | Usually **not** a failure (don't trip on bad requests) |
| Timeout / slow call | Failure (slow-call rate threshold) |
| Connection refused | Failure |
| `IOException` | Failure |
| Business validation error | Usually excluded |

**Production rule:** Only trip on failures that indicate **dependency distress**, not caller mistakes.

### 4.2 Sliding Window vs Count-Based

| Window type | Behavior | Best for |
|-------------|----------|----------|
| **Count-based** | Last N calls | Low traffic integrations |
| **Time-based** | Calls in last T seconds | High traffic, smooths bursts |
| **Slow call rate** | % calls slower than threshold | Latency degradation without hard errors |

Resilience4j default: count-based sliding window with configurable size.

### 4.3 Transition Rules (Canonical)

| From | Event | To |
|------|-------|-----|
| CLOSED | Failure rate ≥ threshold | OPEN |
| OPEN | `waitDurationInOpenState` elapsed | HALF_OPEN |
| HALF_OPEN | Probe success (permitted calls succeed) | CLOSED |
| HALF_OPEN | Probe failure | OPEN |
| CLOSED | Success after prior failures | Reset failure counter |

### 4.4 Edge Cases Architects Must Decide

**Flapping dependency:** OPEN → HALF_OPEN → OPEN → HALF_OPEN rapidly.

- Fix: exponential backoff on open duration, or minimum time in CLOSED before re-trip.
- Resilience4j: `enableAutomaticTransitionFromOpenToHalfOpen()`.

**Partial recovery:** Dependency handles 1 req/sec but you need 1000 req/sec.

- HALF_OPEN probe succeeds → CLOSED → immediate overload → OPEN again.
- Fix: ramp traffic gradually (client-side rate limit) or combine with bulkhead.

**Shared breaker across endpoints:** One bad endpoint trips breaker for entire service host.

- Fix: **one breaker per dependency operation** (e.g., `inventory-get` vs `inventory-reserve`).

---

## 5. From Scratch — CircuitBreakerDemo Walkthrough

The repo ships a zero-dependency implementation in `examples/circuit-breaker/CircuitBreakerDemo.java`. Study this before reaching for Resilience4j — interviews often ask you to implement the state machine.

### 5.1 Core Structure

```java
enum State { CLOSED, OPEN, HALF_OPEN }

static final class SimpleCircuitBreaker {
    private final int failureThreshold;
    private final Duration openDuration;
    private State state = State.CLOSED;
    private int consecutiveFailures;
    private Instant openedAt;
    private boolean halfOpenProbeInFlight;

    String execute(String input) {
        transitionIfOpenWindowElapsed();

        if (state == State.OPEN) {
            throw new CallNotPermittedException("Circuit OPEN — fail fast");
        }
        // HALF_OPEN probe gating ...
        try {
            String result = remote.call(input);
            onSuccess();
            return result;
        } catch (RuntimeException ex) {
            onFailure();
            throw ex;
        }
    }
}
```

### 5.2 Scenario 1 — Failures Trip Breaker

Configuration: `failureThreshold = 3`, `openDuration = 500ms`, remote fails first 3 calls.

```
call-1 FAILED   [state=CLOSED]
call-2 FAILED   [state=CLOSED]
call-3 FAILED   → STATE -> OPEN
call-4 REJECTED (fail fast)
call-5 REJECTED (fail fast)
call-6 REJECTED (fail fast)
```

After 3 consecutive failures in CLOSED, breaker trips OPEN. Calls 4–6 never hit the remote.

### 5.3 Scenario 2 — Recovery via HALF_OPEN

After 600ms sleep (> 500ms open duration):

```
STATE -> HALF_OPEN (probe allowed)
probe-1 SUCCESS → STATE -> CLOSED
after-recovery SUCCESS [state=CLOSED]
```

Remote has recovered (failUntilCall exhausted). Single probe succeeds → CLOSED.

### 5.4 Scenario 3 — Caller-Side Fallback

```java
static String callWithFallback(SimpleCircuitBreaker breaker, String userId) {
    try {
        return breaker.execute(userId);
    } catch (CallNotPermittedException | RuntimeException ex) {
        return "FALLBACK:cached-profile:" + userId;
    }
}
```

Fallback is **caller responsibility** in this demo. In Spring + Resilience4j, `@CircuitBreaker(fallbackMethod = "...")` wires this declaratively.

### 5.5 Run the Demo

```bash
cd /Volumes/Work/designpatterns
javac examples/circuit-breaker/CircuitBreakerDemo.java
java -cp examples/circuit-breaker CircuitBreakerDemo
```

---

## 6. Fail-Fast and Graceful Degradation

### 6.1 Fail-Fast Semantics

When OPEN, the breaker throws immediately:

```java
throw new CallNotPermittedException("Circuit OPEN — fail fast");
```

**Benefits:**

- Thread returned to pool in microseconds
- No socket/file descriptor consumption on doomed calls
- Predictable latency for users (error page vs hang)
- Downstream gets **zero** load during recovery window

### 6.2 Fallback Strategies

| Strategy | When to use | Example |
|----------|-------------|---------|
| **Cached stale data** | Read-heavy, eventual staleness OK | Product catalog from Redis |
| **Default value** | Non-critical enrichment | Empty recommendations list |
| **Queued async retry** | Write can be deferred | "Order received, confirming inventory" |
| **Alternate dependency** | Multi-provider | Secondary payment gateway |
| **Legacy system** | Strangler Fig migration | Route to monolith when new service OPEN |

From `CircuitBreakerDemo.java`:

```
Timeline: [OK:user-1, FALLBACK:cached-profile:user-2, ...]
```

### 6.3 Fallback Design Rules

1. **Fallback must be faster than primary** — otherwise pointless.
2. **Fallback must not depend on the same failing dependency** — common bug: fallback calls same DB.
3. **Log and metric fallback invocations** — high fallback rate = degraded UX even if "up."
4. **Fallback is not silent failure** — emit `circuit_breaker_fallback_total` counter.

### 6.4 HTTP Response Mapping

| Breaker state / outcome | HTTP status | Body |
|-------------------------|-------------|------|
| OPEN + no fallback | 503 Service Unavailable | `{"error":"dependency_unavailable"}` |
| OPEN + fallback | 200 with degraded flag | `{"data":..., "degraded":true}` |
| HALF_OPEN reject | 503 or fallback | Same as OPEN |
| CLOSED + remote 500 | 502/504 | Propagate or map |

Document degraded responses in API contracts — mobile clients may show stale badge.

---

## 7. Half-Open Probes — Recovery Without Thundering Herd

### 7.1 The Thundering Herd Problem

Dependency recovers at T=0. Without HALF_OPEN:

- 10,000 waiting clients simultaneously retry
- Recovery service instantly overloaded
- Back to failure → perpetual flapping

HALF_OPEN allows **controlled probing** before full traffic restoration.

### 7.2 Probe Count Configuration

| `permittedNumberOfCallsInHalfOpenState` | Behavior |
|----------------------------------------|----------|
| 1 | Conservative — one success closes circuit |
| 5–10 | Requires multiple successes — safer for flaky deps |
| >10 | Approaches full traffic — rarely appropriate |

Resilience4j example:

```java
CircuitBreakerConfig config = CircuitBreakerConfig.custom()
    .failureRateThreshold(50)
    .slidingWindowSize(10)
    .waitDurationInOpenState(Duration.ofSeconds(30))
    .permittedNumberOfCallsInHalfOpenState(3)
    .automaticTransitionFromOpenToHalfOpenEnabled(true)
    .build();
```

### 7.3 Single Probe In Flight

Our demo uses `halfOpenProbeInFlight` boolean — only one concurrent probe. Resilience4j handles this via permitted call count in half-open state.

**Why it matters:** Without serialization, two threads could both probe, one fail and one succeed — ambiguous state transition.

### 7.4 Gradual Recovery Patterns (Beyond Basic HALF_OPEN)

For high-traffic systems, consider:

1. **HALF_OPEN success → CLOSED with rate limiter** — cap outbound RPS for 60s after close.
2. **Canary probe from dedicated job** — synthetic health check opens circuit, not user traffic.
3. **Per-instance breakers** — each pod probes independently (acceptable; breaks sync across fleet).

---

## 8. Resilience4j Fundamentals

### 8.1 Why Resilience4j (Not Hystrix)

Netflix Hystrix is in maintenance mode. **Resilience4j** is the modern Java choice:

- Lightweight, no thread pool per command (uses decorators)
- Functional composition (Vavr-style)
- Native Micrometer metrics
- Spring Boot 3 / Spring Cloud Circuit Breaker integration

### 8.2 Maven Dependencies

```xml
<dependency>
    <groupId>io.github.resilience4j</groupId>
    <artifactId>resilience4j-spring-boot3</artifactId>
    <version>2.2.0</version>
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

### 8.3 Programmatic Usage

```java
@Service
public class InventoryClient {

    private final CircuitBreaker circuitBreaker;
    private final RestClient restClient;

    public InventoryClient(CircuitBreakerRegistry registry, RestClient restClient) {
        this.circuitBreaker = registry.circuitBreaker("inventory-service");
        this.restClient = restClient;
    }

    public InventoryResponse getInventory(String sku) {
        Supplier<InventoryResponse> supplier = CircuitBreaker
            .decorateSupplier(circuitBreaker, () ->
                restClient.get()
                    .uri("/inventory/{sku}", sku)
                    .retrieve()
                    .body(InventoryResponse.class));

        return Try.ofSupplier(supplier)
            .recover(CallNotPermittedException.class, ex -> getCachedInventory(sku))
            .recover(HttpServerErrorException.class, ex -> getCachedInventory(sku))
            .get();
    }

    private InventoryResponse getCachedInventory(String sku) {
        // Stale-while-error cache lookup
        return cache.get(sku, InventoryResponse.empty(sku));
    }
}
```

### 8.4 Registry and Shared Configuration

```java
@Configuration
public class Resilience4jConfig {

    @Bean
    public CircuitBreakerRegistry circuitBreakerRegistry() {
        CircuitBreakerConfig defaults = CircuitBreakerConfig.custom()
            .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
            .slidingWindowSize(20)
            .failureRateThreshold(50)
            .slowCallRateThreshold(80)
            .slowCallDurationThreshold(Duration.ofSeconds(2))
            .waitDurationInOpenState(Duration.ofSeconds(30))
            .permittedNumberOfCallsInHalfOpenState(5)
            .minimumNumberOfCalls(10)
            .build();

        return CircuitBreakerRegistry.of(defaults);
    }
}
```

### 8.5 Event Listeners (Debugging)

```java
circuitBreaker.getEventPublisher()
    .onStateTransition(event ->
        log.warn("Breaker {} transitioned {} -> {}",
            circuitBreaker.getName(),
            event.getStateTransition().getFromState(),
            event.getStateTransition().getToState()))
    .onFailureRateExceeded(event ->
        log.error("Breaker {} failure rate exceeded: {}",
            circuitBreaker.getName(), event.getFailureRate()));
```

---

## 9. Spring Boot Integration — @CircuitBreaker and Fallbacks

### 9.1 application.yml Configuration

```yaml
resilience4j:
  circuitbreaker:
    instances:
      inventory-service:
        slidingWindowSize: 20
        minimumNumberOfCalls: 10
        failureRateThreshold: 50
        slowCallRateThreshold: 80
        slowCallDurationThreshold: 2s
        waitDurationInOpenState: 30s
        permittedNumberOfCallsInHalfOpenState: 5
        automaticTransitionFromOpenToHalfOpenEnabled: true
        recordExceptions:
          - org.springframework.web.client.HttpServerErrorException
          - java.io.IOException
          - java.util.concurrent.TimeoutException
        ignoreExceptions:
          - com.example.NotFoundException
      payment-service:
        slidingWindowSize: 50
        failureRateThreshold: 30
        waitDurationInOpenState: 60s
```

### 9.2 @CircuitBreaker Annotation

```java
@Service
public class PaymentServiceClient {

    private final RestClient paymentRestClient;

    public PaymentServiceClient(RestClient paymentRestClient) {
        this.paymentRestClient = paymentRestClient;
    }

    @CircuitBreaker(name = "payment-service", fallbackMethod = "chargeFallback")
    public PaymentResult charge(ChargeRequest request) {
        return paymentRestClient.post()
            .uri("/payments/charge")
            .body(request)
            .retrieve()
            .body(PaymentResult.class);
    }

    // Fallback signature must match: same return type, original args + Throwable
    private PaymentResult chargeFallback(ChargeRequest request, Throwable cause) {
        log.warn("Payment circuit fallback for order {}: {}",
            request.orderId(), cause.toString());
        return PaymentResult.deferred(request.orderId(),
            "Payment confirmation pending — we will notify you");
    }
}
```

### 9.3 Controller Layer — Propagating Degraded State

```java
@RestController
@RequestMapping("/api/checkout")
public class CheckoutController {

    private final CheckoutService checkoutService;

    @PostMapping
    public ResponseEntity<CheckoutResponse> checkout(@RequestBody CheckoutRequest req) {
        CheckoutResult result = checkoutService.checkout(req);
        if (result.isDegraded()) {
            return ResponseEntity.ok()
                .header("X-Degraded", "true")
                .body(result.toResponse());
        }
        return ResponseEntity.ok(result.toResponse());
    }
}
```

### 9.4 Spring Cloud Circuit Breaker Abstraction

For vendor-neutral code:

```java
@Service
public class ShippingClient {

    private final CircuitBreakerFactory circuitBreakerFactory;
    private final RestClient restClient;

    public ShippingQuote getQuote(ShippingRequest req) {
        CircuitBreaker cb = circuitBreakerFactory.create("shipping-service");
        return cb.run(
            () -> restClient.post().uri("/quotes").body(req).retrieve().body(ShippingQuote.class),
            throwable -> ShippingQuote.unavailable(req.destination())
        );
    }
}
```

### 9.5 WebClient + Reactive Circuit Breaker

```java
@Service
public class ReactiveInventoryClient {

    private final WebClient webClient;
    private final ReactiveCircuitBreaker circuitBreaker;

    public Mono<InventoryResponse> getInventory(String sku) {
        return circuitBreaker.run(
            webClient.get()
                .uri("/inventory/{sku}", sku)
                .retrieve()
                .bodyToMono(InventoryResponse.class),
            throwable -> Mono.just(InventoryResponse.cached(sku))
        );
    }
}
```

Enable reactive support:

```xml
<dependency>
    <groupId>io.github.resilience4j</groupId>
    <artifactId>resilience4j-reactor</artifactId>
</dependency>
```

---

## 10. Interaction with Retry and Timeout

### 10.1 Stack Order Matters

**Recommended order (outer → inner):**

```
Timeout → Circuit Breaker → Retry → Actual call
```

Or with Resilience4j decorators:

```java
Retry retry = retryRegistry.retry("inventory-service");
CircuitBreaker cb = circuitBreakerRegistry.circuitBreaker("inventory-service");
TimeLimiter timeLimiter = timeLimiterRegistry.timeLimiter("inventory-service");

Supplier<CompletableFuture<InventoryResponse>> futureSupplier =
    () -> CompletableFuture.supplyAsync(() -> callRemote(sku));

Callable<InventoryResponse> decorated = CircuitBreaker
    .decorateCallable(cb,
        Retry.decorateCallable(retry,
            TimeLimiter.decorateFutureSupplier(timeLimiter, futureSupplier)));
```

### 10.2 Why Retry Before Breaker Is Dangerous

If retry wraps breaker (retry outer):

- Breaker OPEN → retry retries `CallNotPermittedException` → pointless loops
- Multiplies rejected call handling latency

If breaker wraps retry (breaker outer):

- OPEN state rejects before retry executes — correct fail-fast

### 10.3 Retry + Breaker Interaction Table

| Configuration | Effect |
|---------------|--------|
| Breaker OPEN | Retry never runs — immediate reject/fallback |
| Breaker CLOSED, transient 503 | Retry may succeed; failures count toward trip |
| Breaker HALF_OPEN | Probe call may retry internally — **avoid**; keep probe as single attempt |
| Max retries = 3, failure threshold = 50% | One user request = up to 3 dependency calls |

**Rule:** Exclude `CallNotPermittedException` from retry policies.

```yaml
resilience4j:
  retry:
    instances:
      inventory-service:
        maxAttempts: 3
        waitDuration: 200ms
        enableExponentialBackoff: true
        retryExceptions:
          - org.springframework.web.client.HttpServerErrorException
        ignoreExceptions:
          - io.github.resilience4j.circuitbreaker.CallNotPermittedException
```

### 10.4 Timeout Configuration

```yaml
resilience4j:
  timelimiter:
    instances:
      inventory-service:
        timeoutDuration: 3s
        cancelRunningFuture: true
```

Slow calls exceeding `slowCallDurationThreshold` count toward **slow call rate** — trips breaker on latency degradation even without exceptions.

### 10.5 Saga Orchestrator Integration

Sync saga orchestrators (see [saga-expert-playbook.md](saga-expert-playbook.md)) benefit from circuit breakers on each step:

```java
@CircuitBreaker(name = "inventory-service", fallbackMethod = "reserveFallback")
public StepResult reserveInventory(SagaCommand cmd) {
    return inventoryClient.reserve(cmd);
}

private StepResult reserveFallback(SagaCommand cmd, Throwable t) {
    // Trigger compensation path or mark saga AWAITING_RETRY
    throw new DependencyUnavailableException("inventory", cmd.sagaId());
}
```

When breaker is OPEN during saga step, fail fast → saga moves to compensation or retry queue instead of blocking orchestrator thread pool.

---

## 11. Bulkhead — Isolation and Relationship to Circuit Breaker

### 11.1 What Bulkhead Does

Named after ship compartments — **bulkhead** limits concurrent access to a dependency so one slow integration cannot consume all threads.

```
Without bulkhead:
  All 200 threads → Inventory (slow) → pool exhausted

With bulkhead (max 20 concurrent to inventory):
  20 threads → Inventory
  180 threads → available for Payment, Shipping, other work
```

See [java-modern-concurrency-streams-playbook.md](java-modern-concurrency-streams-playbook.md) Pattern 3 for semaphore-based bulkhead.

### 11.2 Circuit Breaker vs Bulkhead

| Aspect | Circuit Breaker | Bulkhead |
|--------|-----------------|----------|
| Trigger | Failure/slow **rate** | Concurrent call **limit** |
| Action | Stop all calls (OPEN) | Reject excess concurrent calls |
| Protects | Dependency + caller from retry storm | Caller thread pool from one dependency |
| Recovery | HALF_OPEN probe | Immediate when slot frees |

**Use both.** They solve different problems.

### 11.3 Resilience4j Bulkhead

```yaml
resilience4j:
  bulkhead:
    instances:
      inventory-service:
        maxConcurrentCalls: 25
        maxWaitDuration: 0   # fail immediately if full — don't queue
  thread-pool-bulkhead:
    instances:
      report-generation:
        maxThreadPoolSize: 10
        coreThreadPoolSize: 5
        queueCapacity: 20
```

```java
@Bulkhead(name = "inventory-service", type = Bulkhead.Type.SEMAPHORE)
@CircuitBreaker(name = "inventory-service", fallbackMethod = "getInventoryFallback")
public InventoryResponse getInventory(String sku) {
    return inventoryClient.fetch(sku);
}
```

### 11.4 Combined Decorator Order

```
Bulkhead → CircuitBreaker → TimeLimiter → Retry → Call
```

Bulkhead outermost — even OPEN breaker shouldn't queue threads waiting for bulkhead slot on other code paths.

### 11.5 Bulkhead Full → Circuit Breaker?

When bulkhead rejects (`BulkheadFullException`), decide:

- **Count as failure** toward breaker trip — aggressive, may OPEN during legitimate spikes
- **Do not count** — bulkhead handles backpressure; breaker handles dependency health

Most teams: **do not** trip breaker on bulkhead reject — different semantics (self-protection vs dependency failure).

---

## 12. Production Configuration Tuning

### 12.1 Starting Points by Integration Type

| Integration | slidingWindowSize | failureRateThreshold | waitDurationInOpenState | Notes |
|-------------|-------------------|----------------------|-------------------------|-------|
| Critical sync (payment) | 50 | 30% | 60s | Conservative trip, longer recovery |
| Read-heavy cacheable | 20 | 50% | 15s | Aggressive fallback |
| Internal microservice | 30 | 50% | 30s | Baseline |
| Batch / async worker | 10 | 60% | 10s | Lower traffic — use time-based window |
| Third-party SaaS API | 100 | 25% | 120s | External, unpredictable |

### 12.2 minimumNumberOfCalls

Prevents tripping on first 2 failures in low traffic:

```yaml
minimumNumberOfCalls: 10  # need 10 calls in window before rate calculated
```

Set relative to expected RPS × window duration.

### 12.3 Tuning Workflow

1. **Deploy with permissive settings** (high threshold, long open duration).
2. **Observe metrics** for 1–2 weeks: failure rate, slow call rate, state transitions.
3. **Tighten** until fallback rate acceptable under synthetic failure tests.
4. **Chaos test**: kill dependency pod, verify breaker trips < 5s, recovers cleanly.

### 12.4 Per-Endpoint vs Per-Service Breakers

```yaml
resilience4j:
  circuitbreaker:
    instances:
      inventory-get:
        baseConfig: default
      inventory-reserve:
        failureRateThreshold: 20   # writes more critical
        waitDurationInOpenState: 90s
```

**Anti-pattern:** One breaker for entire monolith host — one bad endpoint disables all.

### 12.5 Environment-Specific Overrides

```yaml
# application-prod.yml
resilience4j:
  circuitbreaker:
    instances:
      inventory-service:
        waitDurationInOpenState: 45s

# application-dev.yml — disable or loosen for local dev
resilience4j:
  circuitbreaker:
    instances:
      inventory-service:
        failureRateThreshold: 90
```

---

## 13. Metrics, Observability, and Alerting (Micrometer)

### 13.1 Key Resilience4j Metrics

| Metric | Type | Tags | Meaning |
|--------|------|------|---------|
| `resilience4j.circuitbreaker.calls` | Counter | `kind=successful\|failed\|not_permitted` | Call outcomes |
| `resilience4j.circuitbreaker.state` | Gauge | `state=closed\|open\|half_open` | Current state |
| `resilience4j.circuitbreaker.failure.rate` | Gauge | — | Current failure rate |
| `resilience4j.circuitbreaker.slow.calls` | Counter | — | Slow call count |
| `resilience4j.circuitbreaker.buffered.calls` | Gauge | — | Window fill level |

Actuator endpoint: `/actuator/circuitbreakers` and `/actuator/circuitbreakerevents`.

### 13.2 Custom State Gauge (Grafana)

From [metrics-observability-playbook.md](metrics-observability-playbook.md):

```java
@Bean
public MeterBinder circuitBreakerStateBinder(CircuitBreakerRegistry registry) {
    return meterRegistry -> registry.getAllCircuitBreakers().forEach(cb ->
        Gauge.builder("circuit.breaker.state.numeric", cb, breaker -> {
                return switch (breaker.getState()) {
                    case CLOSED -> 0;
                    case HALF_OPEN -> 1;
                    case OPEN -> 2;
                    case DISABLED, FORCED_OPEN, METRICS_ONLY -> -1;
                };
            })
            .tag("name", cb.getName())
            .description("0=CLOSED, 1=HALF_OPEN, 2=OPEN")
            .register(meterRegistry)
    );
}
```

Grafana value mappings: 0=green CLOSED, 1=yellow HALF_OPEN, 2=red OPEN.

### 13.3 Structured Logging

```java
@Slf4j
@Aspect
@Component
public class CircuitBreakerLoggingAspect {

    @AfterThrowing(
        pointcut = "@annotation(io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker)",
        throwing = "ex")
    public void logCircuitFailure(JoinPoint jp, Throwable ex) {
        if (ex instanceof CallNotPermittedException) {
            log.warn("circuit_breaker_rejected method={} reason={}",
                jp.getSignature().getName(), ex.getMessage());
        }
    }
}
```

Include `traceId`, `dependency name`, `breaker state` in MDC.

### 13.4 Alert Rules

| Alert | Condition | Severity |
|-------|-----------|----------|
| Breaker OPEN | `state == OPEN` for > 2 min | Warning |
| Breaker flapping | > 5 transitions in 10 min | Critical |
| High not_permitted rate | `not_permitted / total > 10%` for 5 min | Warning |
| Fallback surge | `fallback_total` rate 5× baseline | Warning |
| Slow call rate rising | `slow_call_rate > 60%` in CLOSED | Warning (pre-trip) |

### 13.5 Dashboard Panels

1. Breaker state timeline per dependency (color-coded)
2. Call outcome stacked area (success / failed / not_permitted)
3. Failure rate vs threshold line
4. p99 latency overlay with slow-call threshold
5. Fallback invocation rate

---

## 14. Production Pitfalls and War Stories

### 14.1 Pitfall: Breaker on Wrong Exceptions

Tripping on `404 Not Found` → breaker OPEN during catalog refresh when SKUs legitimately missing.

**Fix:** `ignoreExceptions: [NotFoundException.class]`

### 14.2 Pitfall: Fallback Calls Same Dependency

```java
// ANTI-PATTERN
private Product getProductFallback(String id, Throwable t) {
    return legacyProductService.getProduct(id); // also failing!
}
```

**Fix:** Fallback reads from cache, static default, or unrelated healthy store.

### 14.3 Pitfall: Shared Breaker Across Tenants

Multi-tenant SaaS uses one breaker for payment provider — Tenant A's bad requests trip breaker for all tenants.

**Fix:** Breaker per critical tenant tier, or ensure failures are dependency-level not tenant-validation-level.

### 14.4 Pitfall: Retry Storm Through CLOSED Breaker

3 retries × 1000 RPS = 3000 RPS to dying service while CLOSED.

**Fix:** Exponential backoff with jitter, lower max retries, tighter failure threshold, combine with bulkhead.

### 14.5 Pitfall: HALF_OPEN Success → Immediate Overload

Probe succeeds at 1 RPS; CLOSED restores 5000 RPS instantly; dependency dies again.

**Fix:** Rate limiter after close, or require N successes in HALF_OPEN.

### 14.6 War Story: Black Friday Inventory

**Symptom:** Checkout 503 for all users; inventory DB at 40% CPU.

**Root cause:** No circuit breaker on inventory read. Thread pool exhaustion. Clients retried aggressively.

**Fix:** Resilience4j breaker (50% threshold, 30s open), stale cache fallback for product availability display, bulkhead max 30 concurrent inventory calls. Checkout stayed up with "availability may be delayed" banner.

### 14.7 War Story: Strangler Fig Canary Gone Wrong

During migration (see [strangler-fig-playbook.md](strangler-fig-playbook.md)), 50% traffic routed to new account service. New service deployed with bug → 50% users saw errors.

**Fix:** Spring Cloud Gateway circuit breaker on new service route with automatic fallback to legacy. Error rate dropped to 0% for users; team fixed new service offline.

### 14.8 Pitfall: No Breaker on "Fast Failing" Dependency

Dependency returns 503 in 5ms — threads don't block, but load is still amplified.

**Fix:** Circuit breaker still valuable — stops pointless load and enables fallback.

---

## 15. Production Issue Runbook

### 15.1 Symptom: Circuit Breaker Stuck OPEN

**Impact:** Users see fallback or 503 for one integration.

| Step | Action |
|------|--------|
| 1 | Check `/actuator/circuitbreakers` — confirm OPEN state and name |
| 2 | Check dependency health dashboard (error rate, latency, pod restarts) |
| 3 | Query `resilience4j.circuitbreaker.calls{kind="failed"}` spike timing |
| 4 | If dependency healthy but breaker OPEN → probe may be failing on non-representative error |
| 5 | Fix root cause OR manually transition (see 15.4) |
| 6 | Verify HALF_OPEN → CLOSED transition in metrics |

### 15.2 Symptom: Breaker Flapping (OPEN ↔ HALF_OPEN ↔ CLOSED)

**Impact:** Intermittent user errors, unstable latency.

| Step | Action |
|------|--------|
| 1 | Increase `waitDurationInOpenState` temporarily |
| 2 | Increase `permittedNumberOfCallsInHalfOpenState` — require more successes |
| 3 | Check dependency autoscaling — may be scaling too slowly |
| 4 | Add rate limiter post-close |
| 5 | Review if slow-call threshold too aggressive |

### 15.3 Symptom: High Fallback Rate, Breaker CLOSED

**Impact:** Degraded UX but no fail-fast — dependency failing below trip threshold.

| Step | Action |
|------|--------|
| 1 | Lower `failureRateThreshold` or reduce `minimumNumberOfCalls` |
| 2 | Enable slow-call rate tripping |
| 3 | Investigate dependency partial degradation |
| 4 | Alert on fallback counter, not just breaker state |

### 15.4 Manual Breaker Control (Emergency)

Resilience4j supports forced states via Actuator or code:

```java
// Force OPEN — stop all traffic during known maintenance
circuitBreaker.transitionToForcedOpenState();

// Force CLOSED — override after fix verified (use cautiously)
circuitBreaker.transitionToClosedState();

// Reset metrics and state
circuitBreaker.reset();
```

Document who may force-close in production (lead/on-call only).

### 15.5 Post-Incident Checklist

- [ ] Root cause of dependency failure identified
- [ ] Breaker thresholds appropriate (not too loose/tight)
- [ ] Fallback tested and served acceptable UX
- [ ] Retry policy reviewed for amplification
- [ ] Dashboard/alerts updated if gap found
- [ ] Runbook entry added if novel scenario

---

## 16. Decision Framework: When to Use and When NOT To

### 16.1 Decision Tree

```
Outbound call to dependency?
├── No → No breaker needed
└── Yes
    ├── Can failure be ignored (fire-and-forget analytics)? → Optional breaker
    └── Failure affects user-facing path?
        ├── Yes → Circuit breaker + timeout mandatory
        │   ├── Read path with cache? → Breaker + cache fallback
        │   └── Write path? → Breaker + queue/retry OR fail with clear error
        └── No → Bulkhead may suffice
```

### 16.2 When TO Use

| Scenario | Breaker value |
|----------|---------------|
| HTTP/gRPC to microservices | High |
| Third-party SaaS APIs | High |
| Database read during peak (with fallback cache) | Medium-High |
| Sync saga orchestration steps | High (see saga playbook) |
| API gateway routing to new service (Strangler Fig) | Critical |
| Message broker publish (when sync API wraps it) | Medium |

### 16.3 When NOT To Use

| Condition | Why | Alternative |
|-----------|-----|-------------|
| In-process method call | No network failure domain | Input validation |
| Dependency must **always** succeed (financial ledger write) | Fallback unacceptable | Retry + queue + human escalation — not silent degrade |
| Call volume too low for statistical window | Breaker never trips or trips on noise | Simple retry + timeout |
| Latency SLA < breaker overhead | Breaker overhead negligible anyway — still use for failure | Timeout only (rare) |
| Single-threaded batch job | No pool to protect | Job-level checkpoint/restart |
| You have no fallback story | OPEN = same as error without UX benefit | Design fallback first |

### 16.4 Circuit Breaker vs Cache-Aside

| Need | Pattern |
|------|---------|
| Dependency usually fast, occasionally down | Circuit breaker + cache fallback |
| Dependency always slow but consistent | Cache-aside with TTL, optional breaker |
| Strong consistency on read | No breaker fallback with stale data — fail clearly |

---

## 17. Circuit Breaker in Larger Architectures

### 17.1 API Gateway Layer

Spring Cloud Gateway with Resilience4j:

```yaml
spring:
  cloud:
    gateway:
      routes:
        - id: new-account-service
          uri: lb://account-service-v2
          filters:
            - name: CircuitBreaker
              args:
                name: accountServiceV2
                fallbackUri: forward:/legacy/account
```

During [Strangler Fig migration](strangler-fig-playbook.md), gateway breaker enables **instant rollback** to legacy without redeploy.

### 17.2 Service Mesh (Istio/Linkerd)

Mesh outlier detection ejects unhealthy hosts — **similar but not identical** to client-side breaker:

| Feature | Client-side Resilience4j | Service mesh outlier detection |
|---------|--------------------------|--------------------------------|
| Scope | Per calling service | Per destination subset |
| Fallback | Application-controlled | Load balance to other instances |
| Business-aware | Yes (ignore 404) | No — TCP/HTTP level |

**Best practice:** Use both — mesh for instance health, app breaker for dependency-level degradation and fallbacks.

### 17.3 Saga + Circuit Breaker

Long-running sagas ([saga-expert-playbook.md](saga-expert-playbook.md)):

- **Sync orchestration:** Breaker on each participant call prevents orchestrator thread exhaustion.
- **Async orchestration:** Breaker on command publish or sync status check endpoints.
- **OPEN during compensation:** Separate breaker for refund API — don't block compensation because forward path breaker tripped.

### 17.4 Bulkhead + Breaker + Retry Stack

Production resilience stack for outbound calls:

```
┌─────────────────────────────────────────────┐
│ Bulkhead (max 25 concurrent)                │
│  ┌───────────────────────────────────────┐  │
│  │ Circuit Breaker (50% failure → OPEN)  │  │
│  │  ┌─────────────────────────────────┐    │  │
│  │  │ TimeLimiter (3s)                │    │  │
│  │  │  ┌───────────────────────────┐  │    │  │
│  │  │  │ Retry (3x, exp backoff)   │  │    │  │
│  │  │  │  ┌─────────────────────┐  │  │    │  │
│  │  │  │  │ HTTP call           │  │  │    │  │
│  │  │  │  └─────────────────────┘  │  │    │  │
│  │  │  └───────────────────────────┘  │    │  │
│  │  └─────────────────────────────────┘    │  │
│  └───────────────────────────────────────┘  │
└─────────────────────────────────────────────┘
         │ OPEN / failure
         ▼
    Cache fallback / degraded response
```

---

## 18. Lead Interview Questions & Answers

### Category A: Fundamentals

**Q1: What is the Circuit Breaker pattern and what problem does it solve?**

**A**: A circuit breaker wraps calls to an unreliable dependency and stops forwarding requests when failures exceed a threshold — failing fast locally instead of blocking threads on timeouts. It solves **cascading failure**: one slow or down dependency exhausting caller thread pools and taking down the entire service. It gives the dependency time to recover and optionally returns a fallback to maintain partial UX.

**Q2: Explain the three states: CLOSED, OPEN, and HALF_OPEN.**

**A**: **CLOSED** — normal operation; all calls pass through; failures are counted. **OPEN** — breaker tripped; calls rejected immediately without hitting the dependency (`CallNotPermittedException`). **HALF_OPEN** — after a wait period, a limited number of probe calls test recovery; success → CLOSED, failure → OPEN. Our repo demo `CircuitBreakerDemo.java` implements all three with single-probe gating in HALF_OPEN.

**Q3: How is a circuit breaker different from a retry?**

**A**: Retry handles **transient** failures by re-attempting the same call. Circuit breaker handles **sustained** failure by **stopping** calls entirely for a cooling period. Retry amplifies load on a dying service; breaker reduces load. Production stacks use both: retry for occasional blips inside CLOSED state; breaker to cut off during outages. Never retry `CallNotPermittedException`.

**Q4: What is fail-fast and why does it matter?**

**A**: When OPEN, the breaker rejects in microseconds instead of waiting for TCP timeout (seconds). This returns threads to the pool immediately, preserves capacity for healthy code paths, and prevents clients from staring at spinners. Fail-fast converts dependency outage into controlled local rejection plus optional fallback.

**Q5: What triggers the transition from CLOSED to OPEN?**

**A**: Typically when failure rate or slow-call rate in a sliding window exceeds a configured threshold (e.g., 50% of last 20 calls failed). Some implementations use consecutive failure count (our demo uses 3 consecutive). Configurable: which exceptions count, minimum calls before rate calculation, slow call duration threshold.

### Category B: State Machine Deep Dive

**Q6: Why do we need HALF_OPEN instead of going directly from OPEN to CLOSED?**

**A**: Direct OPEN → CLOSED would allow full traffic the instant wait duration elapses — causing a **thundering herd** that re-overloads a barely-recovered dependency. HALF_OPEN sends a controlled probe first. One success proves recovery; only then restore full traffic. Demo: after 500ms OPEN, single probe succeeds → CLOSED.

**Q7: What happens if two threads hit HALF_OPEN simultaneously?**

**A**: Without gating, both could probe; conflicting outcomes complicate state. Our demo uses `halfOpenProbeInFlight` — second thread gets `CallNotPermittedException`. Resilience4j uses `permittedNumberOfCallsInHalfOpenState` to allow N controlled probes. For N=1, equivalent to single probe.

**Q8: Should HTTP 404 trip the circuit breaker?**

**A**: Usually **no**. 404 often means "resource not found" — a valid business outcome, not dependency distress. Tripping on 404 opens breaker during normal catalog gaps. Configure `ignoreExceptions` for client errors (4xx) that aren't dependency health signals. Trip on 5xx, timeouts, connection failures.

**Q9: Explain sliding window vs consecutive failure count.**

**A**: **Consecutive count** (demo): simple, trips fast on sustained errors, but one success resets counter — may never trip on intermittent failures. **Sliding window rate**: e.g., 50% of last 20 calls — smooths bursty traffic, handles intermittent failure patterns. High-traffic production prefers rate-based with `minimumNumberOfCalls`.

**Q10: What is slow-call rate threshold?**

**A**: Trips breaker when calls **complete** but exceed latency threshold (e.g., 80% of calls > 2s) even without exceptions. Catches dependency **degradation** before hard failures — DB lock contention, GC pauses. Resilience4j: `slowCallRateThreshold` + `slowCallDurationThreshold`.

### Category C: Resilience4j and Spring Boot

**Q11: How do you configure a circuit breaker in Spring Boot with Resilience4j?**

**A**: Add `resilience4j-spring-boot3` + AOP. Configure in `application.yml` under `resilience4j.circuitbreaker.instances.{name}`. Annotate methods with `@CircuitBreaker(name = "...", fallbackMethod = "...")`. Fallback method must match signature: original parameters + `Throwable`. Expose state via `/actuator/circuitbreakers`. See Section 9.

**Q12: What is the fallback method contract in `@CircuitBreaker`?**

**A**: Same class, same return type, same parameter types as protected method **plus** a trailing `Throwable` parameter. Must be accessible (often private). Spring AOP proxies invoke fallback on any exception including `CallNotPermittedException` if not excluded. Fallback must not call the failing dependency.

**Q13: Why Resilience4j over Hystrix?**

**A**: Hystrix is maintenance mode, thread-pool-per-command model is heavy. Resilience4j is lightweight, functional, Micrometer-native, actively maintained, integrates with Spring Boot 3. No mandatory thread pool isolation (use bulkhead separately when needed).

**Q14: How do you test circuit breaker behavior in Spring Boot?**

**A**: (1) Unit test with `CircuitBreakerRegistry` — force state transitions, assert fallback invoked. (2) `@SpringBootTest` + WireMock — simulate 503 responses until breaker opens, assert subsequent calls get `CallNotPermittedException` without WireMock hit. (3) Verify metrics via `/actuator/metrics/resilience4j.circuitbreaker.calls`. (4) Run `CircuitBreakerDemo.java` for state machine logic without Spring.

**Q15: How do you share breaker config across microservices?**

**A**: Spring Cloud Config or shared `application-resilience.yml` in parent POM. Per-service overrides for criticality. Document standard baselines: default (50%/30s), critical-write (30%/60s), read-cacheable (50%/15s). Breaker names must match dependency operation, not generic "httpClient".

### Category D: Retry, Timeout, Bulkhead

**Q16: What is the correct order of retry, timeout, and circuit breaker?**

**A**: Outermost: **Timeout → Circuit Breaker → Retry → Call** (or Bulkhead outermost of all). Breaker must wrap retry so OPEN state fails before retry loops. Exclude `CallNotPermittedException` from retry. HALF_OPEN probes should not retry internally — single attempt.

**Q17: How does circuit breaker interact with bulkhead?**

**A**: **Bulkhead** limits concurrent calls (thread/semaphore isolation). **Breaker** stops calls based on failure rate. Bulkhead protects your pool from one slow dep; breaker protects dep from retry storm and enables fail-fast. Use both. Bulkhead reject (`BulkheadFullException`) usually should NOT count toward breaker failure — different semantics. See Section 11 and [java-modern-concurrency-streams-playbook.md](java-modern-concurrency-streams-playbook.md).

**Q18: A dependency times out at 5s. Breaker is CLOSED. What's the problem?**

**A**: Every call blocks a thread for 5s before counting as failure — pool exhaustion before breaker trips. Fix: reduce timeout to p99 + margin (e.g., 1–2s), enable slow-call rate tripping, add bulkhead to cap concurrent waits. Breaker needs failures to accumulate; timeouts must be shorter than pool drain time.

**Q19: Should you put a circuit breaker on a database call?**

**A**: **Yes** for read paths with cache fallback — prevents connection pool exhaustion when DB degrades. **Careful** for writes — fallback may mean data loss. Pattern: breaker on read replica reads with cache; writes fail clearly without silent fallback. Combine with connection pool sizing and query timeouts.

**Q20: How do retries affect breaker failure rate calculation?**

**A**: Each retry attempt typically counts as separate call in the window. 3 retries on failing service = 3 failures toward trip threshold — trips faster (often desirable). Be aware when tuning: effective dependency load = RPS × (1 + retry count) while CLOSED.

### Category E: Production and Operations

**Q21: What metrics do you alert on for circuit breakers?**

**A**: Breaker state OPEN > 2 min (warning), state transitions > 5 in 10 min (flapping/critical), `not_permitted` call rate > 10% of total, fallback invocation rate spike, failure rate approaching threshold while CLOSED, slow call rate rising. Grafana state gauge: 0=CLOSED, 1=HALF_OPEN, 2=OPEN. See Section 13.

**Q22: Breaker is OPEN but dependency dashboard shows healthy. What do you check?**

**A**: (1) Are probes failing on non-representative path (auth error on probe endpoint)? (2) Is `ignoreExceptions` misconfigured? (3) Network partition between AZs — your pods can't reach healthy pods. (4) Stale breaker state — rare; try manual reset after verifying health. (5) Slow-call tripping on latency slightly above threshold during deploy warmup.

**Q23: How do you prevent flapping?**

**A**: Increase `waitDurationInOpenState`, require multiple successes in HALF_OPEN (`permittedNumberOfCallsInHalfOpenState` > 1), add post-close rate limiter, fix dependency autoscaling lag, use exponential backoff on open duration. Flapping = dependency at edge of capacity.

**Q24: Design circuit breaker strategy for a payment gateway integration.**

**A**: Separate breaker `payment-charge` (writes, 30% threshold, 60s open, **no silent fallback** — queue for retry or clear error) and `payment-status` (reads, 50% threshold, cache last known status). Ignore 4xx validation errors. Bulkhead max 15 concurrent charges. Timeout 5s. Metrics + page on OPEN > 1 min. Idempotency keys on retry. Never fallback to "payment succeeded."

**Q25: How does circuit breaker help in Strangler Fig migration?**

**A**: Gateway routes traffic to new service; breaker on new route with fallback to legacy. New service bug or outage → breaker trips → users transparently hit legacy. Enables safe canary ramp (5% → 50%) without redeploy rollback. See [strangler-fig-playbook.md](strangler-fig-playbook.md) Sections 8.3 and 11.2.

### Category F: Advanced and Curveball

**Q26: Client-side breaker vs service mesh outlier detection?**

**A**: Client-side (Resilience4j): application-aware exception handling, fallback logic, per-operation granularity. Mesh outlier detection: ejects unhealthy **instances** at L4/L7, load balances to peers — no business fallback. Use both: mesh for instance rotation, app breaker for dependency-level trip and degraded UX.

**Q27: Can circuit breaker cause split-brain behavior across pods?**

**A**: Yes — each JVM has independent breaker state. Pod A may be CLOSED while Pod B is OPEN during partial recovery. Acceptable for most systems — gradual traffic restoration. For strict consistency, centralize state (Redis) — rarely worth complexity. Prefer per-instance breakers.

**Q28: Implement circuit breaker from scratch in an interview.**

**A**: Enum `State { CLOSED, OPEN, HALF_OPEN }`. Fields: `consecutiveFailures`, `openedAt`, `failureThreshold`, `openDuration`, `halfOpenProbeInFlight`. `execute()`: if OPEN and duration elapsed → HALF_OPEN; if OPEN → throw; if HALF_OPEN and probe in flight → throw; try call; success → reset/close; failure → increment/trip. Reference: `CircuitBreakerDemo.java` Sections 5, 32–111.

**Q29: What is the difference between circuit breaker OPEN and rate limiter reject?**

**A**: Rate limiter rejects based on **request count per time window** regardless of outcome — proactive throttling. Breaker rejects based on **failure/slow rate** — reactive to dependency health. Rate limiter protects dependency from overload; breaker protects caller from dependency failure. Complementary.

**Q30: When would you NOT use a circuit breaker?**

**A**: No network boundary (in-process call). No fallback and failure must never be masked (critical financial write without queue). Traffic too low for meaningful statistics. Team hasn't designed degraded UX — OPEN just becomes obscure 503. Prefer fixing observability and timeout first for low-risk internal calls.

**Q31: How does circuit breaker relate to saga compensation?**

**A**: In sync saga orchestration, OPEN breaker on step 3 fails fast instead of blocking — saga triggers compensation or marks AWAITING_RETRY. Separate breaker instances for forward steps vs compensation endpoints — compensation must remain reachable when forward breaker is OPEN. See [saga-expert-playbook.md](saga-expert-playbook.md) Section 6 and 14.

**Q32: Your breaker trips during every deployment of dependency. Fix?**

**A**: Dependency drains connections / returns 503 during rolling deploy. Fixes: (1) Increase `minimumNumberOfCalls` and failure threshold temporarily. (2) Retry with jitter handles brief 503. (3) Dependency: preStop hook, graceful shutdown, readiness probe delay. (4) Client: health-check-based routing. (5) Don't count 503 during known maintenance window — operational flag to force CLOSED.

---

## 19. How to Talk About Circuit Breakers in an Interview

> Plain English. Short sentences. How you'd explain it to a teammate or hiring manager.

---

### "What is a circuit breaker?"

It's like the breaker in your house electrical panel. If a dependency starts failing a lot, you stop sending it traffic for a while. Instead of every request waiting 30 seconds and timing out, you fail immediately on your side. That keeps your service alive and gives the broken dependency room to recover.

---

### "Why not just use timeouts?"

Timeouts help — they stop one call from waiting forever. But if you have 200 threads and every call waits 5 seconds before timing out, you still burn through your thread pool. And every timed-out call still hit the dying service. Circuit breaker says — "okay, this thing is clearly down, stop calling it entirely for 30 seconds."

---

### "What are the three states?"

**Closed** means normal. Calls go through.

**Open** means tripped. Calls get rejected immediately. No network call.

**Half-open** means you're testing recovery. You let one or a few calls through. If they succeed, you go back to closed. If they fail, you open again.

We have a working demo in `CircuitBreakerDemo.java` that walks through all three.

---

### "What's a fallback?"

When the breaker is open or the call fails, you return something else instead of an error. Maybe cached data. Maybe a default empty list. Maybe route to the old legacy system during a migration.

Important rule — the fallback can't call the same broken dependency. That sounds obvious but teams mess it up all the time.

---

### "How does it work with retry?"

Retry is for "maybe that was a blip, try again." Circuit breaker is for "this thing is clearly sick, stop trying."

Order matters. Circuit breaker should wrap retry, not the other way around. If the breaker is open, don't retry — just fail fast or use fallback. And never retry a "call not permitted" exception.

---

### "What's a bulkhead and how is it different?"

Bulkhead limits how many calls you make at the same time — like only 20 concurrent calls to inventory. Circuit breaker stops all calls when failure rate is high.

Bulkhead protects your thread pool from one slow dependency. Circuit breaker protects the dependency from you — and protects you from wasting time on a dead service. Use both.

---

### "How do you use it in Spring Boot?"

Resilience4j is the standard now. Add the starter, configure thresholds in `application.yml`, put `@CircuitBreaker` on the method that calls the remote service, and point to a fallback method. Actuator shows you the current state — closed, open, or half-open.

---

### "What metrics do you watch?"

Is the breaker open? How often is it flapping between states? How many calls are getting rejected vs succeeding? How often is fallback running?

If fallback rate is high but breaker is still closed, your thresholds might be too loose. If breaker is flapping open-closed-open-closed, your dependency is at the edge — increase the wait time or require more successful probes.

---

### "When would you NOT use one?"

If there's no remote call — it's just an in-process method — you don't need it.

If you can't define a fallback and the operation must succeed — like recording a payment — you don't silently degrade. You fail clearly and queue for retry.

And if traffic is so low that 2 failures would trip the breaker, you'll get false positives. Fix the stats window first.

---

### "How does it fit with saga and strangler fig?"

In a **saga**, if you're calling services one by one and one is down, the breaker stops your orchestrator from hanging. You can compensate or retry later instead of blocking.

In a **strangler fig** migration, you put a breaker on the new service route. New service breaks? Traffic falls back to the legacy system automatically. That's your safety net during canary rollout.

---

### Quick Answers

| Question | Say this |
|----------|----------|
| What is a circuit breaker? | Stops calling a failing dependency — fail fast instead of timeout storm |
| Three states? | CLOSED (normal), OPEN (reject all), HALF_OPEN (test recovery) |
| Why HALF_OPEN? | Prevents thundering herd when dependency recovers |
| vs Retry? | Retry = try again on blip. Breaker = stop calling when sustained failure |
| vs Timeout? | Timeout bounds one call. Breaker stops all calls after pattern of failures |
| vs Bulkhead? | Bulkhead limits concurrency. Breaker stops calls on failure rate |
| Stack order? | Bulkhead → Breaker → Timeout → Retry → Call |
| Spring Boot? | Resilience4j + `@CircuitBreaker` + fallback method + actuator |
| Key metric? | State OPEN, not_permitted rate, fallback rate, flapping transitions |
| Fallback rule? | Never call the same broken dependency from fallback |
| When not to use? | In-process calls, must-succeed writes with no degrade story, ultra-low traffic |
| Repo demo? | `examples/circuit-breaker/CircuitBreakerDemo.java` |
| Saga link? | Breaker on orchestrator steps — fail fast, compensate or retry queue |
| Strangler link? | Breaker on new service route — auto-fallback to legacy |
| Hystrix? | Legacy. Use Resilience4j for new Java/Spring work |

---

*Runnable example: `examples/circuit-breaker/CircuitBreakerDemo.java`*

```bash
javac examples/circuit-breaker/CircuitBreakerDemo.java && \
  java -cp examples/circuit-breaker CircuitBreakerDemo
```

*Cross-references: [saga-expert-playbook.md](saga-expert-playbook.md) · [strangler-fig-playbook.md](strangler-fig-playbook.md) · [java-modern-concurrency-streams-playbook.md](java-modern-concurrency-streams-playbook.md) · [metrics-observability-playbook.md](metrics-observability-playbook.md)*

*End of Circuit Breaker — Expert Playbook*
