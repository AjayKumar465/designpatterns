# Metrics & Observability — Expert Playbook

> **Goal**: Make any Java/Spring developer production-ready with metrics, monitoring, and observability.
> Covers Micrometer, Prometheus, Grafana, OpenTelemetry, alerting, SLOs, and real-world production patterns.

---

## Table of Contents

1. [Why Metrics Matter](#1-why-metrics-matter)
2. [Observability vs Monitoring — Clearing the Confusion](#2-observability-vs-monitoring--clearing-the-confusion)
3. [The Three Pillars of Observability](#3-the-three-pillars-of-observability)
4. [Monitoring Methodologies — RED, USE, Four Golden Signals](#4-monitoring-methodologies--red-use-four-golden-signals)
5. [SLI, SLO, SLA & Error Budgets](#5-sli-slo-sla--error-budgets)
6. [Micrometer Deep Dive — The Metrics Facade](#6-micrometer-deep-dive--the-metrics-facade)
7. [Meter Types — Counter, Gauge, Timer, DistributionSummary](#7-meter-types--counter-gauge-timer-distributionsummary)
8. [Histograms vs Summaries — publishPercentiles vs publishPercentileHistogram](#8-histograms-vs-summaries--publishpercentiles-vs-publishpercentilehistogram)
9. [Tags, Dimensions & The Cardinality Problem](#9-tags-dimensions--the-cardinality-problem)
10. [MeterFilter — Production Defense Layer](#10-meterfilter--production-defense-layer)
11. [Custom Business Metrics — Production-Grade Patterns](#11-custom-business-metrics--production-grade-patterns)
12. [Annotation-Based Metrics — @Timed, @Counted, @Observed](#12-annotation-based-metrics--timed-counted-observed)
13. [MeterBinder — Reusable Metric Modules](#13-meterbinder--reusable-metric-modules)
14. [Observation API (Spring Boot 3+) — Unified Metrics + Tracing](#14-observation-api-spring-boot-3--unified-metrics--tracing)
15. [Spring Boot Auto-Configured Metrics — What You Get for Free](#15-spring-boot-auto-configured-metrics--what-you-get-for-free)
16. [Prometheus Integration — Scraping, Storage, Architecture](#16-prometheus-integration--scraping-storage-architecture)
17. [PromQL — Essential Queries for Spring Boot](#17-promql--essential-queries-for-spring-boot)
18. [Grafana Dashboards & Alerting](#18-grafana-dashboards--alerting)
19. [OpenTelemetry vs Micrometer — When to Use What](#19-opentelemetry-vs-micrometer--when-to-use-what)
20. [Naming Conventions & Best Practices](#20-naming-conventions--best-practices)
21. [Testing Metrics](#21-testing-metrics)
22. [Production Configuration — Complete Setup](#22-production-configuration--complete-setup)
23. [Production Anti-Patterns & Pitfalls](#23-production-anti-patterns--pitfalls)
24. [Production Issue Runbook](#24-production-issue-runbook)
25. [Lead Interview Questions & Answers](#25-lead-interview-questions--answers)

---

## 1. Why Metrics Matter

Without metrics, you are flying blind:

| Scenario | Without Metrics | With Metrics |
|---|---|---|
| Latency spike | Users complain → you scramble | Alert fires → you see P99 jumped → drill into endpoint |
| Memory leak | OOM crash at 3 AM | Gauge shows steady climb → you catch it during business hours |
| Feature launch | "Is it working?" — nobody knows | Counter shows orders/sec doubled, error rate unchanged |
| Capacity planning | Guess and over-provision | Trend analysis shows you need 3 more pods by Q4 |
| Post-incident review | "Something broke" | Error budget consumed 40% in 2 hours, root cause: DB pool exhaustion |

**Metrics are not optional in production. They are infrastructure.**

---

## 2. Observability vs Monitoring — Clearing the Confusion

| Aspect | Monitoring | Observability |
|---|---|---|
| Question answered | "Is it broken?" | "Why is it broken?" |
| Approach | Pre-defined dashboards for known failures | Explore arbitrary questions about unknown failures |
| Signals | Metrics + alerts | Metrics + Traces + Logs (correlated) |
| Mindset | Reactive — alert when threshold breached | Proactive — understand system behavior from outputs |
| Example | "CPU > 90% → alert" | "Why did P99 spike? → trace shows slow DB → log shows missing index" |

**Observability = Metrics + Traces + Logs, correlated so you can navigate between them.**

```
Metric spike (Grafana) → Click exemplar → Trace (Tempo) → Span shows slow query → Log (Loki) with traceId
```

---

## 3. The Three Pillars of Observability

### Metrics
Aggregated numerical measurements over time. Cheap to store, easy to alert on.
- "Request rate is 500/sec"
- "P99 latency is 200ms"
- "Error rate is 0.1%"

### Traces
A single request's journey across services. Expensive to store, essential for debugging.
- "Request X took 3 seconds because the payment service called Stripe which timed out"
- Each trace has spans (units of work), span IDs, and a trace ID

### Logs
Detailed text events with context. Most verbose, most expensive.
- "2026-06-06 10:00:00 ERROR OrderService: Payment failed for order-12345, reason: CARD_DECLINED"
- Structured (JSON) logs with traceId enable correlation

### The Power of Correlation

```
┌──────────────┐     exemplar      ┌──────────────┐     traceId       ┌──────────────┐
│   METRICS    │ ──────────────── → │    TRACES    │ ──────────────── → │     LOGS     │
│  (Prometheus)│                    │   (Tempo)    │                    │    (Loki)    │
│              │                    │              │                    │              │
│ rate, P99,   │                    │ spans,       │                    │ structured,  │
│ error %      │                    │ waterfall    │                    │ searchable   │
└──────────────┘                    └──────────────┘                    └──────────────┘
```

---

## 4. Monitoring Methodologies — RED, USE, Four Golden Signals

### RED Method (Tom Wilkie, Grafana Labs)
**For services** — measures the user's experience.

| Signal | What | PromQL Example |
|---|---|---|
| **R**ate | Requests per second | `rate(http_server_requests_seconds_count[5m])` |
| **E**rrors | Failed requests per second | `rate(http_server_requests_seconds_count{status=~"5.."}[5m])` |
| **D**uration | Latency distribution (P50/P95/P99) | `histogram_quantile(0.99, sum by (le) (rate(http_server_requests_seconds_bucket[5m])))` |

### USE Method (Brendan Gregg)
**For resources** (CPU, memory, disk, network) — finds infrastructure bottlenecks.

| Signal | What | Example |
|---|---|---|
| **U**tilization | % time resource is busy | CPU at 85% |
| **S**aturation | Work queued beyond capacity | Thread pool queue depth = 200 |
| **E**rrors | Resource-level errors | Disk I/O errors, NIC CRC errors |

### Four Golden Signals (Google SRE)
**Combined perspective** — the most holistic.

| Signal | Description | Maps To |
|---|---|---|
| Latency | Time to serve a request (distinguish success vs error latency) | RED Duration |
| Traffic | Request volume | RED Rate |
| Errors | Rate of failed requests | RED Errors |
| Saturation | How "full" your service is | USE Saturation |

### When to Use What

| Scenario | Use |
|---|---|
| Monitoring a REST API | RED |
| Monitoring a database server | USE |
| Building SRE dashboards | Four Golden Signals |
| Full microservices platform | RED for services + USE for infra |

---

## 5. SLI, SLO, SLA & Error Budgets

### Definitions

| Term | What | Example |
|---|---|---|
| **SLI** (Service Level Indicator) | A quantitative measurement | "99.2% of requests complete in < 200ms" |
| **SLO** (Service Level Objective) | The target for your SLI | "P99 latency < 200ms over 30 days" |
| **SLA** (Service Level Agreement) | A contract with consequences | "99.9% uptime or we refund 10% of monthly bill" |
| **Error Budget** | The allowed failure room | SLO = 99.9% → error budget = 0.1% = ~43 minutes/month |

### The Error Budget Concept

```
Error Budget = 100% - SLO target

SLO = 99.9% availability
Error Budget = 0.1% = 43.2 minutes downtime/month

If you've used 30 minutes this month:
- Remaining budget: 13.2 minutes
- Decision: FREEZE deploys, focus on reliability
```

### Choosing Good SLIs

| Service Type | Good SLI | Bad SLI |
|---|---|---|
| HTTP API | % requests < 200ms | Average latency (hides outliers) |
| Data pipeline | % records processed within SLA window | Total records processed (doesn't show timeliness) |
| Storage system | % reads returning correct data | Read throughput (doesn't show correctness) |

### Multi-Window Burn Rate Alerting

The most production-proven approach to SLO alerting (Google SRE recommended):

```yaml
# Fast burn: consuming 2 hours of budget in 5 minutes
- alert: SLOBurnRateCritical
  expr: |
    (
      job:http_error_ratio:rate1h{job="api"} > (14.4 * 0.001)
      AND
      job:http_error_ratio:rate5m{job="api"} > (14.4 * 0.001)
    )
  for: 2m
  labels:
    severity: critical

# Slow burn: will exhaust budget in 3 days
- alert: SLOBurnRateWarning
  expr: |
    (
      job:http_error_ratio:rate1d{job="api"} > (3 * 0.001)
      AND
      job:http_error_ratio:rate2h{job="api"} > (3 * 0.001)
    )
  for: 60m
  labels:
    severity: warning
```

---

## 6. Micrometer Deep Dive — The Metrics Facade

### What Is Micrometer?

Micrometer is to metrics what SLF4J is to logging — a **vendor-neutral facade**.

```
┌─────────────────────────────────────────┐
│           Your Application Code         │
│         (uses Micrometer API)           │
└─────────────┬───────────────────────────┘
              │
    ┌─────────▼─────────┐
    │   MeterRegistry   │  ← Composite: routes to all backends
    └─────────┬─────────┘
              │
   ┌──────────┼──────────────────┐
   ▼          ▼                  ▼
┌────────┐ ┌─────────┐  ┌──────────────┐
│Promethe│ │ Datadog │  │ CloudWatch   │
│us      │ │         │  │              │
└────────┘ └─────────┘  └──────────────┘
```

### Dependencies (Gradle)

```groovy
dependencies {
    implementation 'org.springframework.boot:spring-boot-starter-actuator'
    runtimeOnly 'io.micrometer:micrometer-registry-prometheus'
}
```

### Dependencies (Maven)

```xml
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-actuator</artifactId>
</dependency>
<dependency>
    <groupId>io.micrometer</groupId>
    <artifactId>micrometer-registry-prometheus</artifactId>
    <scope>runtime</scope>
</dependency>
```

### MeterRegistry

The central component. Spring Boot auto-configures a `CompositeMeterRegistry` that routes to every registry on the classpath.

```java
@Service
public class OrderService {

    private final MeterRegistry registry;

    public OrderService(MeterRegistry registry) {
        this.registry = registry;
    }
}
```

---

## 7. Meter Types — Counter, Gauge, Timer, DistributionSummary

### Decision Matrix

| Meter Type | Behavior | Use Case | Example | Prometheus Type |
|---|---|---|---|---|
| **Counter** | Only increases, resets on restart | Counting events | Orders placed, errors | `_total` suffix |
| **Gauge** | Goes up and down | Current state | Queue depth, active threads | Gauge |
| **Timer** | Duration + count + max | Latency tracking | API response time | Histogram/Summary |
| **DistributionSummary** | Non-time distributions | Size/value distributions | Payload size, order amount | Histogram/Summary |

### Counter

Tracks values that **only increase**. Resets to zero on application restart.

```java
@Service
public class PaymentService {

    private final Counter successCounter;
    private final Counter failureCounter;

    public PaymentService(MeterRegistry registry) {
        this.successCounter = Counter.builder("payments.processed")
            .description("Total payments processed successfully")
            .tag("status", "success")
            .register(registry);

        this.failureCounter = Counter.builder("payments.processed")
            .description("Total payments that failed")
            .tag("status", "failure")
            .register(registry);
    }

    public PaymentResult processPayment(PaymentRequest request) {
        try {
            PaymentResult result = gateway.charge(request);
            successCounter.increment();
            return result;
        } catch (PaymentException e) {
            failureCounter.increment();
            throw e;
        }
    }
}
```

**Critical rule**: Never count something you can time. A `Timer` already includes a count.

### Gauge

Tracks a **current value** that fluctuates up and down. Gauges are sampled — the value is read lazily at scrape time.

```java
@Service
public class QueueMonitor {

    public QueueMonitor(MeterRegistry registry, BlockingQueue<Task> taskQueue) {
        Gauge.builder("task.queue.size", taskQueue, Queue::size)
            .description("Current number of tasks waiting in the queue")
            .register(registry);
    }
}
```

**Using `AtomicLong` for manual gauge control:**

```java
@Component
public class ActiveSessionTracker {

    private final AtomicLong activeSessions;

    public ActiveSessionTracker(MeterRegistry registry) {
        this.activeSessions = registry.gauge(
            "sessions.active",
            new AtomicLong(0)
        );
    }

    public void sessionCreated()   { activeSessions.incrementAndGet(); }
    public void sessionDestroyed() { activeSessions.decrementAndGet(); }
}
```

**When NOT to use a Gauge**: If the value only ever increases, use a Counter. Gauges are for fluctuating state.

### Timer

Measures **short-duration latencies** and their frequency. A Timer is a specialized DistributionSummary aware of time units.

```java
@Service
public class OrderService {

    private final Timer orderTimer;

    public OrderService(MeterRegistry registry) {
        this.orderTimer = Timer.builder("order.processing.duration")
            .description("Time taken to process an order")
            .publishPercentileHistogram()
            .minimumExpectedValue(Duration.ofMillis(1))
            .maximumExpectedValue(Duration.ofSeconds(10))
            .register(registry);
    }

    public Order processOrder(OrderRequest request) {
        return orderTimer.record(() -> {
            // ... order processing logic
            return createOrder(request);
        });
    }
}
```

**Timer automatically records**: count, total time, max, and histogram buckets (if configured).

**Three ways to record:**

```java
// 1. Lambda — cleanest
timer.record(() -> doWork());

// 2. Callable — returns a value
Order result = timer.recordCallable(() -> processOrder(request));

// 3. Timer.Sample — for async or complex flows
Timer.Sample sample = Timer.start(registry);
// ... do work (possibly async) ...
sample.stop(timer);
```

### DistributionSummary

Tracks the **distribution of non-time values** (payload sizes, order amounts, batch sizes).

```java
@Service
public class OrderService {

    private final DistributionSummary orderAmountSummary;

    public OrderService(MeterRegistry registry) {
        this.orderAmountSummary = DistributionSummary
            .builder("order.amount")
            .description("Distribution of order amounts in USD")
            .baseUnit("USD")
            .publishPercentiles(0.5, 0.75, 0.95, 0.99)
            .minimumExpectedValue(1.0)
            .maximumExpectedValue(10000.0)
            .register(registry);
    }

    public void recordOrder(Order order) {
        orderAmountSummary.record(order.getTotalAmount());
    }
}
```

### Quick Reference: Which Meter to Use

```
Is it time-based?
├── Yes → Timer
└── No
    ├── Does it only go up? → Counter
    ├── Does it fluctuate? → Gauge
    └── Is it a distribution of values? → DistributionSummary
```

---

## 8. Histograms vs Summaries — publishPercentiles vs publishPercentileHistogram

This is one of the most misunderstood topics. Getting it wrong causes either **wrong percentiles** or **memory explosions**.

### publishPercentiles (Client-Side Summary)

```java
Timer.builder("api.latency")
    .publishPercentiles(0.5, 0.95, 0.99)
    .register(registry);
```

- Computes percentiles **inside your JVM**
- Exports pre-computed quantile values to Prometheus
- **NOT aggregable** across multiple instances
- Prometheus type: `Summary`

**Problem**: If you have 10 pods, you get 10 separate P99 values. You **cannot** aggregate them into a fleet-wide P99 — that's statistically invalid.

### publishPercentileHistogram (Server-Side Histogram)

```java
Timer.builder("api.latency")
    .publishPercentileHistogram()
    .minimumExpectedValue(Duration.ofMillis(1))
    .maximumExpectedValue(Duration.ofSeconds(10))
    .register(registry);
```

- Exports bucket counts to Prometheus (e.g., `_bucket{le="0.05"}`, `_bucket{le="0.1"}`, ...)
- Percentiles are calculated in PromQL using `histogram_quantile()`
- **Aggregable** across instances, endpoints, regions
- Prometheus type: `Histogram`

### Comparison

| Aspect | `publishPercentiles` | `publishPercentileHistogram` |
|---|---|---|
| Where computed | Client (JVM) | Server (Prometheus) |
| Aggregable across pods | No | Yes |
| Memory impact | Lower | Higher (more time series per bucket) |
| Accuracy | Exact for single instance | Approximation (depends on bucket boundaries) |
| PromQL needed | No — values exported directly | Yes — `histogram_quantile()` |
| Prometheus type | Summary | Histogram |

### Production Recommendation

**Always use `publishPercentileHistogram()` in production** with Prometheus. Always clamp with `minimumExpectedValue` and `maximumExpectedValue` to control bucket count.

```java
Timer.builder("http.server.requests")
    .publishPercentileHistogram()
    .minimumExpectedValue(Duration.ofMillis(1))
    .maximumExpectedValue(Duration.ofSeconds(30))
    .serviceLevelObjectives(
        Duration.ofMillis(100),
        Duration.ofMillis(500),
        Duration.ofSeconds(1)
    )
    .register(registry);
```

### SLO Boundaries

`serviceLevelObjectives()` adds explicit bucket boundaries at your SLO thresholds, giving precise counts at those values:

```
# Prometheus output
http_server_requests_seconds_bucket{le="0.1"}  850   ← requests < 100ms
http_server_requests_seconds_bucket{le="0.5"}  950   ← requests < 500ms
http_server_requests_seconds_bucket{le="1.0"}  990   ← requests < 1 second
http_server_requests_seconds_bucket{le="+Inf"} 1000
```

### Memory Impact

| Configuration | Approximate Buckets per Metric |
|---|---|
| Default (no clamping) | 276 |
| With min=1ms, max=30s | ~73 |
| With min=1ms, max=1min (default Timer clamping) | ~73 |

**Total time series** = buckets × unique tag combinations × number of instances. This grows fast.

---

## 9. Tags, Dimensions & The Cardinality Problem

### What Are Tags?

Tags (labels in Prometheus) add dimensions to metrics, enabling filtering and drill-down.

```java
Counter.builder("http.requests")
    .tag("method", "GET")        // 4-5 values: GET, POST, PUT, DELETE, PATCH
    .tag("status", "200")        // ~10 values: 200, 201, 400, 404, 500...
    .tag("endpoint", "/api/users") // bounded by # of endpoints
    .register(registry);
```

### The Cardinality Problem

**Cardinality** = number of unique time series = product of all unique tag value combinations.

```
4 methods × 10 statuses × 20 endpoints = 800 time series (fine)
4 methods × 10 statuses × 100,000 user IDs = 4,000,000 time series (OOM)
```

### What Causes High Cardinality?

| Tag | Cardinality | Verdict |
|---|---|---|
| `method=GET` | ~5 | Safe |
| `status=200` | ~10 | Safe |
| `region=us-east` | ~5 | Safe |
| `endpoint=/api/users` | ~50 | Safe |
| `userId=abc123` | Unbounded | **DANGEROUS** |
| `orderId=ord-xyz` | Unbounded | **DANGEROUS** |
| `uri=/users/12345/profile` | Unbounded | **DANGEROUS** |
| `email=user@example.com` | Unbounded | **DANGEROUS** |
| `traceId=abc123def` | Unbounded | **DANGEROUS** |

### Real-World Production Incident

> A pentesting tool sent 50,000 unique random URL paths to a service. The Ktor/Micrometer auto-instrumentation created a metric for each path. **50,000 paths × 60 bucket boundaries = 3,000,000 new time series**. VictoriaMetrics (the TSDB) ran out of memory and crashed, taking down monitoring for the entire platform.
> — [Medium: How a Pentesting Tool Broke Our VictoriaMetrics System](https://medium.com/@kavinpon/how-50-000-requests-took-down-our-entire-metrics-system-898b34973ba7)

### Rules of Thumb

1. **Keep cardinality < 10** for each tag
2. **Total unique time series < 100,000** per application
3. **Never use user IDs, request IDs, order IDs, or emails as tags**
4. If you need per-user or per-request data, use **traces** or **logs** — not metrics
5. Normalize dynamic values into bounded categories (e.g., URI templates `/users/{id}`)

### Fixing High Cardinality

```java
// BAD: unbounded user ID as tag
counter.increment(Tags.of("userId", userId)); // millions of values

// GOOD: bounded user tier as tag
String tier = getUserTier(userId); // "free", "premium", "enterprise"
counter.increment(Tags.of("tier", tier)); // 3 values
```

---

## 10. MeterFilter — Production Defense Layer

`MeterFilter` is your last line of defense against cardinality explosions, unnecessary metrics, and runaway memory usage.

### Three Powers of MeterFilter

1. **Deny** — Block metrics from being registered
2. **Transform** — Rename metrics or modify/remove tags
3. **Configure** — Change histogram/percentile settings

### Deny Specific Metrics

```java
@Configuration
public class MetricsFilterConfig {

    @Bean
    public MeterFilter denyJvmCompilationMetrics() {
        return MeterFilter.denyNameStartsWith("jvm.compilation");
    }

    @Bean
    public MeterFilter denyUnnecessaryMetrics() {
        return MeterFilter.deny(id ->
            id.getName().startsWith("tomcat.") ||
            id.getName().startsWith("process.files.")
        );
    }
}
```

### Remove High-Cardinality Tags

```java
@Bean
public MeterFilter removeHighCardinalityTags() {
    return MeterFilter.ignoreTags("userId", "requestId", "traceId");
}
```

### Replace Tag Values to Reduce Cardinality

```java
@Bean
public MeterFilter normalizeUriPaths() {
    return MeterFilter.replaceTagValues("uri",
        uri -> uri.replaceAll("/\\d+", "/{id}")
                  .replaceAll("/[0-9a-f]{8}-[0-9a-f]{4}-.*", "/{uuid}")
    );
}
```

### Set Maximum Allowed Tags (Safety Valve)

```java
@Bean
public MeterFilter limitUriCardinality() {
    return MeterFilter.maximumAllowableTags(
        "http.server.requests",  // metric name prefix
        "uri",                   // tag key
        100,                     // max unique values
        MeterFilter.deny()       // action when exceeded
    );
}
```

### Whitelist Pattern (Deny Everything Except...)

```java
@Bean
public MeterFilter onlyAllowCriticalMetrics() {
    return MeterFilter.denyUnless(id ->
        id.getName().startsWith("http.") ||
        id.getName().startsWith("jvm.memory.") ||
        id.getName().startsWith("order.") ||
        id.getName().startsWith("payment.")
    );
}
```

### Configure Histogram Buckets Globally

```java
@Bean
public MeterFilter histogramConfig() {
    return new MeterFilter() {
        @Override
        public DistributionStatisticConfig configure(
                Meter.Id id,
                DistributionStatisticConfig config) {
            if (id.getName().startsWith("http.server.")) {
                return DistributionStatisticConfig.builder()
                    .percentilesHistogram(true)
                    .minimumExpectedValue(Duration.ofMillis(1).toNanos() * 1.0)
                    .maximumExpectedValue(Duration.ofSeconds(10).toNanos() * 1.0)
                    .build()
                    .merge(config);
            }
            return config;
        }
    };
}
```

### High Cardinality Tags Detector (Micrometer 1.13+)

```java
@Bean
public MeterRegistryCustomizer<MeterRegistry> cardinalityDetector() {
    return registry -> registry.config()
        .highCardinalityTagsDetector(new HighCardinalityTagsDetector(
            registry,
            100,                    // threshold
            Duration.ofMinutes(5),  // check interval
            (meterId, count) -> log.warn(
                "High cardinality detected: {} has {} meters",
                meterId.getName(), count
            )
        ));
}
```

### Filter Evaluation Order

Filters are evaluated **in registration order**. First explicit `ACCEPT` or `DENY` wins. If all filters return `NEUTRAL`, the meter is accepted.

```java
@Bean
@Order(Ordered.HIGHEST_PRECEDENCE) // evaluated first
public MeterFilter criticalFilter() { ... }

@Bean
@Order(Ordered.LOWEST_PRECEDENCE) // evaluated last
public MeterFilter defaultFilter() { ... }
```

---

## 11. Custom Business Metrics — Production-Grade Patterns

### Pattern: Dedicated Metrics Component

Separate metrics concerns from business logic.

```java
@Component
public class OrderMetrics {

    private final Counter ordersCreated;
    private final Counter ordersSucceeded;
    private final Counter ordersFailed;
    private final Timer orderProcessingTime;
    private final AtomicLong pendingOrders;

    public OrderMetrics(MeterRegistry registry) {
        this.ordersCreated = Counter.builder("orders.created.total")
            .description("Total orders created")
            .register(registry);

        this.ordersSucceeded = Counter.builder("orders.completed.total")
            .description("Total orders completed successfully")
            .tag("result", "success")
            .register(registry);

        this.ordersFailed = Counter.builder("orders.completed.total")
            .description("Total orders that failed")
            .tag("result", "failure")
            .register(registry);

        this.orderProcessingTime = Timer.builder("orders.processing.duration")
            .description("Time to process an order end-to-end")
            .publishPercentileHistogram()
            .minimumExpectedValue(Duration.ofMillis(10))
            .maximumExpectedValue(Duration.ofSeconds(30))
            .register(registry);

        this.pendingOrders = registry.gauge(
            "orders.pending.count",
            Tags.empty(),
            new AtomicLong(0)
        );
    }

    public void recordCreated() {
        ordersCreated.increment();
        pendingOrders.incrementAndGet();
    }

    public void recordSuccess() {
        ordersSucceeded.increment();
        pendingOrders.decrementAndGet();
    }

    public void recordFailure() {
        ordersFailed.increment();
        pendingOrders.decrementAndGet();
    }

    public <T> T recordProcessingTime(Callable<T> work) throws Exception {
        return orderProcessingTime.recordCallable(work);
    }
}
```

### Pattern: Dynamic Tags with Bounded Values

```java
@Component
public class PaymentMetrics {

    private final MeterRegistry registry;

    public PaymentMetrics(MeterRegistry registry) {
        this.registry = registry;
        // Pre-register known combinations for zero-value baseline
        for (String method : List.of("credit_card", "debit_card", "paypal", "bank_transfer")) {
            Counter.builder("payments.processed.total")
                .tag("method", method)
                .register(registry);
        }
    }

    public void recordPayment(String paymentMethod, boolean success) {
        String normalizedMethod = normalizePaymentMethod(paymentMethod);
        Counter.builder("payments.processed.total")
            .tag("method", normalizedMethod)
            .tag("result", success ? "success" : "failure")
            .register(registry)
            .increment();
    }

    private String normalizePaymentMethod(String raw) {
        return switch (raw.toLowerCase()) {
            case "visa", "mastercard", "amex" -> "credit_card";
            case "paypal" -> "paypal";
            case "bank", "ach", "wire" -> "bank_transfer";
            default -> "other";
        };
    }
}
```

### Pattern: Async Timer with Timer.Sample

```java
@Service
public class AsyncOrderService {

    private final Timer asyncTimer;
    private final MeterRegistry registry;

    public AsyncOrderService(MeterRegistry registry) {
        this.registry = registry;
        this.asyncTimer = Timer.builder("orders.async.duration")
            .description("End-to-end async order processing time")
            .publishPercentileHistogram()
            .register(registry);
    }

    public CompletableFuture<Order> processOrderAsync(OrderRequest request) {
        Timer.Sample sample = Timer.start(registry);

        return CompletableFuture.supplyAsync(() -> createOrder(request))
            .thenApply(order -> {
                sample.stop(asyncTimer);
                return order;
            })
            .exceptionally(ex -> {
                sample.stop(Timer.builder("orders.async.duration")
                    .tag("error", ex.getClass().getSimpleName())
                    .register(registry));
                throw new CompletionException(ex);
            });
    }
}
```

---

## 12. Annotation-Based Metrics — @Timed, @Counted, @Observed

### Prerequisites

Annotations require **Spring AOP** and explicit aspect bean registration:

```xml
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-aop</artifactId>
</dependency>
```

```java
@Configuration
public class MetricsAspectConfig {

    @Bean
    public TimedAspect timedAspect(MeterRegistry registry) {
        return new TimedAspect(registry);
    }

    @Bean
    public CountedAspect countedAspect(MeterRegistry registry) {
        return new CountedAspect(registry);
    }
}
```

### @Timed

```java
@Service
public class UserService {

    @Timed(value = "users.lookup.duration",
           description = "Time to look up a user",
           percentiles = {0.5, 0.95, 0.99})
    public User findById(Long id) {
        return userRepository.findById(id)
            .orElseThrow(() -> new UserNotFoundException(id));
    }
}
```

### @Counted

```java
@Service
public class NotificationService {

    @Counted(value = "notifications.sent.total",
             description = "Total notifications sent")
    public void sendNotification(Notification notification) {
        // ... send logic
    }
}
```

### Critical AOP Limitations

| Limitation | Explanation |
|---|---|
| **Public methods only** | Spring AOP uses JDK dynamic proxies or CGLIB — only intercepts public methods |
| **No self-invocation** | Calling `this.method()` bypasses the proxy; annotation has no effect |
| **No private/protected** | Annotations on private/protected methods are silently ignored |
| **No static methods** | Static methods are not proxied |

```java
@Service
public class OrderService {

    @Timed("order.process")
    public void processOrder(Order order) {
        validate(order);  // ← @Timed on validate() would NOT work (self-invocation)
    }

    @Timed("order.validate") // silently ignored when called from processOrder()
    public void validate(Order order) { ... }
}
```

### Spring Boot 3.5+ @Timed/@Counted via management property

In Spring Boot 3.5+, you can enable annotation scanning without manually registering aspect beans:

```yaml
management:
  observations:
    annotations:
      enabled: true
```

---

## 13. MeterBinder — Reusable Metric Modules

`MeterBinder` is the standard way to package reusable metric instrumentation. Spring Boot auto-discovers `MeterBinder` beans and binds them to the registry.

### Basic MeterBinder

```java
@Component
public class CacheMetrics implements MeterBinder {

    private final CacheManager cacheManager;

    public CacheMetrics(CacheManager cacheManager) {
        this.cacheManager = cacheManager;
    }

    @Override
    public void bindTo(MeterRegistry registry) {
        cacheManager.getCacheNames().forEach(name -> {
            Cache cache = cacheManager.getCache(name);

            Gauge.builder("cache.size", cache,
                    c -> getCacheSize(c))
                .description("Current cache size")
                .tag("cache", name)
                .register(registry);

            FunctionCounter.builder("cache.hits", cache,
                    c -> getCacheHitCount(c))
                .description("Total cache hits")
                .tag("cache", name)
                .register(registry);

            FunctionCounter.builder("cache.misses", cache,
                    c -> getCacheMissCount(c))
                .description("Total cache misses")
                .tag("cache", name)
                .register(registry);
        });
    }
}
```

### When to Use MeterBinder vs Direct Registration

| Use Case | Approach |
|---|---|
| Metrics depend on other Spring beans | `MeterBinder` (ensures correct initialization order) |
| Reusable across projects | `MeterBinder` (standalone, testable) |
| Simple counter/timer in a service | Direct `MeterRegistry` injection |
| Library/SDK instrumentation | `MeterBinder` (users just add it as a bean) |

---

## 14. Observation API (Spring Boot 3+) — Unified Metrics + Tracing

The Observation API is Micrometer's unified approach: a single instrumentation point that **automatically creates both metrics and trace spans**.

### Core Concept

```java
@Service
public class PaymentService {

    private final ObservationRegistry observationRegistry;

    public PaymentService(ObservationRegistry observationRegistry) {
        this.observationRegistry = observationRegistry;
    }

    public PaymentResult processPayment(PaymentRequest request) {
        return Observation.createNotStarted("payment.process", observationRegistry)
            .contextualName("process-payment")
            .lowCardinalityKeyValue("payment.method", request.getMethod().name())
            .lowCardinalityKeyValue("currency", request.getCurrency())
            .highCardinalityKeyValue("order.id", request.getOrderId())
            .observe(() -> {
                gateway.validate(request);
                return gateway.charge(request);
            });
    }
}
```

### Low vs High Cardinality

| Type | Becomes | Example |
|---|---|---|
| `lowCardinalityKeyValue` | Metric tag + Trace span attribute | `payment.method=CREDIT_CARD` (bounded) |
| `highCardinalityKeyValue` | Trace span attribute only | `order.id=ORD-12345` (unbounded) |

This distinction prevents the cardinality problem at the API level: high-cardinality values are **automatically excluded from metrics** but still available in traces.

### @Observed Annotation

```java
@Configuration
public class ObservationConfig {

    @Bean
    public ObservedAspect observedAspect(ObservationRegistry registry) {
        return new ObservedAspect(registry);
    }
}

@Service
public class InventoryService {

    @Observed(
        name = "inventory.check",
        contextualName = "check-inventory",
        lowCardinalityKeyValues = {"warehouse", "us-east"}
    )
    public boolean checkAvailability(String sku, int quantity) {
        return warehouseClient.check(sku, quantity);
    }
}
```

### What @Observed Produces

For each method call, you get:
- A **Timer** metric: `inventory.check` with tags from `lowCardinalityKeyValues`
- A **Trace span**: `check-inventory` with both low and high cardinality attributes
- **Error tracking**: exceptions automatically recorded

### Observation vs Direct Micrometer

| Feature | Direct Micrometer | Observation API |
|---|---|---|
| Metrics | Yes | Yes (automatic Timer) |
| Traces | No (separate instrumentation) | Yes (automatic span) |
| Error recording | Manual | Automatic |
| Low/High cardinality | All tags go to metrics | Only low-cardinality → metrics |
| When to use | Metrics-only scenarios, fine-grained control | New code, unified observability |

---

## 15. Spring Boot Auto-Configured Metrics — What You Get for Free

Spring Boot Actuator + Micrometer auto-configures these metrics with **zero code**:

### JVM Metrics (`jvm.*`)

| Metric | Description |
|---|---|
| `jvm.memory.used` | Current memory usage by area (heap/non-heap) and pool |
| `jvm.memory.max` | Maximum memory available |
| `jvm.memory.committed` | Committed memory |
| `jvm.gc.pause` | GC pause duration and count (Timer) |
| `jvm.gc.memory.allocated` | Bytes allocated in young gen (Counter) |
| `jvm.gc.memory.promoted` | Bytes promoted to old gen (Counter) |
| `jvm.threads.live` | Current live thread count |
| `jvm.threads.peak` | Peak thread count |
| `jvm.threads.daemon` | Daemon thread count |
| `jvm.classes.loaded` | Number of loaded classes |
| `jvm.buffer.memory.used` | Buffer pool memory (direct/mapped) |

### HTTP Server Metrics (`http.server.requests`)

| Tag | Values |
|---|---|
| `method` | GET, POST, PUT, DELETE, ... |
| `uri` | Templated URI (e.g., `/api/users/{id}`) |
| `status` | HTTP status code |
| `outcome` | SUCCESS, CLIENT_ERROR, SERVER_ERROR |
| `exception` | Exception class name (or "none") |

### HikariCP Connection Pool (`hikaricp.*`)

| Metric | Description |
|---|---|
| `hikaricp.connections.active` | Currently borrowed connections |
| `hikaricp.connections.idle` | Idle connections |
| `hikaricp.connections.pending` | Threads waiting for a connection |
| `hikaricp.connections.timeout` | Connection acquisition timeouts |
| `hikaricp.connections.max` | Maximum pool size |
| `hikaricp.connections.creation` | Connection creation time (Timer) |
| `hikaricp.connections.acquire` | Connection acquire time (Timer) |
| `hikaricp.connections.usage` | Connection usage time (Timer) |

### Spring Data JPA (`spring.data.repository.invocations`)

Auto-instruments repository method calls with timing.

### Kafka (`kafka.consumer.*`, `kafka.producer.*`)

| Metric | Description |
|---|---|
| `kafka.consumer.fetch.manager.records.lag` | Consumer lag per partition |
| `kafka.consumer.fetch.manager.records.consumed.rate` | Consumption rate |
| `kafka.producer.record.send.rate` | Producer send rate |

### System Metrics

| Metric | Description |
|---|---|
| `system.cpu.usage` | System-wide CPU utilization |
| `process.cpu.usage` | JVM process CPU utilization |
| `system.cpu.count` | Available processors |
| `process.uptime` | JVM uptime |
| `disk.free` | Free disk space |

### Tomcat / Undertow / Jetty

| Metric | Description |
|---|---|
| `tomcat.threads.current` | Current thread count |
| `tomcat.threads.busy` | Active/busy threads |
| `tomcat.threads.config.max` | Configured max threads |
| `tomcat.sessions.active.current` | Active HTTP sessions |

---

## 16. Prometheus Integration — Scraping, Storage, Architecture

### Architecture

```
┌────────────────────┐         scrape /actuator/prometheus         ┌──────────────┐
│  Spring Boot App   │ ◄────────────────────────────────────────── │  Prometheus  │
│  (port 8080)       │         every 15s                           │  Server      │
│                    │                                             │              │
│  Management port   │                                             │  TSDB        │
│  (port 8081)       │                                             │  + Rules     │
└────────────────────┘                                             │  + Alerts    │
                                                                   └──────┬───────┘
                                                                          │
                                                                   ┌──────▼───────┐
                                                                   │   Grafana    │
                                                                   │  (port 3000) │
                                                                   │  Dashboards  │
                                                                   │  + Alerts    │
                                                                   └──────────────┘
```

### Prometheus is Pull-Based

Prometheus **scrapes** your `/actuator/prometheus` endpoint at a configured interval. Your app does NOT push metrics.

**Pros**: No coupling to monitoring infrastructure; app works fine even if Prometheus is down.

**Cons**: Short-lived batch jobs can't be scraped — use the **Pushgateway** for those.

### prometheus.yml

```yaml
global:
  scrape_interval: 15s
  evaluation_interval: 15s

scrape_configs:
  - job_name: 'spring-boot-app'
    metrics_path: '/actuator/prometheus'
    scrape_interval: 15s
    static_configs:
      - targets: ['app-host:8081']
        labels:
          environment: 'production'
          team: 'payments'

  # For Kubernetes: use ServiceMonitor instead of static_configs
```

### Kubernetes ServiceMonitor (Prometheus Operator)

```yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: spring-boot-monitor
  labels:
    release: prometheus
spec:
  selector:
    matchLabels:
      app: order-service
  endpoints:
    - port: management
      path: /actuator/prometheus
      interval: 15s
```

### Prometheus Recording Rules

Pre-compute expensive queries as new time series:

```yaml
groups:
  - name: spring-boot-recording-rules
    rules:
      - record: job:http_error_ratio:rate5m
        expr: |
          sum(rate(http_server_requests_seconds_count{status=~"5.."}[5m])) by (job)
          /
          sum(rate(http_server_requests_seconds_count[5m])) by (job)

      - record: job:http_request_rate:rate5m
        expr: sum(rate(http_server_requests_seconds_count[5m])) by (job)
```

### Prometheus Alerting Rules

```yaml
groups:
  - name: spring-boot-alerts
    rules:
      - alert: HighErrorRate
        expr: job:http_error_ratio:rate5m > 0.05
        for: 2m
        labels:
          severity: critical
        annotations:
          summary: "High error rate ({{ $value | humanizePercentage }})"
          runbook: "https://wiki.internal/runbooks/high-error-rate"

      - alert: HighP99Latency
        expr: |
          histogram_quantile(0.99,
            sum by (le, job) (rate(http_server_requests_seconds_bucket[5m]))
          ) > 2
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "P99 latency above 2 seconds"

      - alert: HikariPoolExhaustion
        expr: hikaricp_connections_pending > 5
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "HikariCP pool exhaustion — {{ $value }} threads waiting"

      - alert: JvmHeapHigh
        expr: |
          jvm_memory_used_bytes{area="heap"}
          / jvm_memory_max_bytes{area="heap"} > 0.9
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "JVM heap usage above 90%"
```

---

## 17. PromQL — Essential Queries for Spring Boot

### Request Rate (Throughput)

```promql
# Total requests per second
rate(http_server_requests_seconds_count[5m])

# By endpoint
sum by (uri) (rate(http_server_requests_seconds_count[5m]))

# By status code
sum by (status) (rate(http_server_requests_seconds_count[5m]))
```

### Error Rate

```promql
# Error ratio (5xx / total)
sum(rate(http_server_requests_seconds_count{status=~"5.."}[5m]))
/
sum(rate(http_server_requests_seconds_count[5m]))

# Error rate per endpoint
sum by (uri) (rate(http_server_requests_seconds_count{status=~"5.."}[5m]))
/
sum by (uri) (rate(http_server_requests_seconds_count[5m]))
```

### Latency Percentiles

```promql
# P99 latency (fleet-wide)
histogram_quantile(0.99,
  sum by (le) (rate(http_server_requests_seconds_bucket[5m]))
)

# P95 latency per endpoint
histogram_quantile(0.95,
  sum by (le, uri) (rate(http_server_requests_seconds_bucket[5m]))
)

# P50 (median)
histogram_quantile(0.50,
  sum by (le) (rate(http_server_requests_seconds_bucket[5m]))
)
```

### Average Latency

```promql
# Average over 5 minutes (use increase, not raw division)
increase(http_server_requests_seconds_sum[5m])
/
increase(http_server_requests_seconds_count[5m])
```

### JVM Memory

```promql
# Heap usage percentage
jvm_memory_used_bytes{area="heap"}
/ jvm_memory_max_bytes{area="heap"} * 100

# Non-heap usage
jvm_memory_used_bytes{area="nonheap"}
```

### GC Pressure

```promql
# GC pause rate (seconds/second)
rate(jvm_gc_pause_seconds_sum[5m])

# GC pause count rate
rate(jvm_gc_pause_seconds_count[5m])

# Average GC pause duration
rate(jvm_gc_pause_seconds_sum[5m])
/ rate(jvm_gc_pause_seconds_count[5m])
```

### HikariCP Connection Pool

```promql
# Pool utilization
hikaricp_connections_active
/ hikaricp_connections_max

# Threads waiting for connection
hikaricp_connections_pending

# Average connection acquire time
rate(hikaricp_connections_acquire_seconds_sum[5m])
/ rate(hikaricp_connections_acquire_seconds_count[5m])
```

### Custom Business Metrics

```promql
# Orders per second
rate(orders_created_total[5m])

# Payment success rate
sum(rate(payments_processed_total{result="success"}[5m]))
/
sum(rate(payments_processed_total[5m]))

# Pending order queue
orders_pending_count
```

### rate() vs irate() vs increase()

| Function | What | When |
|---|---|---|
| `rate()` | Per-second average over window | Smooth trends, dashboards |
| `irate()` | Instantaneous rate (last 2 data points) | Volatile, real-time spikes |
| `increase()` | Total increase over window | Human-readable "how many in 5 min" |

---

## 18. Grafana Dashboards & Alerting

### Essential Dashboards

| Dashboard | Purpose | Metrics |
|---|---|---|
| **Fleet Overview** | All services at a glance | RED per service |
| **Service Deep Dive** | Single service breakdown | Latency by endpoint, errors by type |
| **JVM / Infrastructure** | Resource health | Heap, GC, threads, CPU |
| **Business Metrics** | Product health | Orders/sec, revenue, conversion |
| **Connection Pools** | Dependency health | HikariCP, Redis, HTTP client pools |

### Pre-Built Dashboard IDs (Grafana.com)

| Dashboard | ID | Description |
|---|---|---|
| JVM (Micrometer) | 4701 | Standard JVM metrics |
| Spring Boot Statistics | 11378 | HTTP, Tomcat, JVM combined |
| Spring Boot Observability | 17175 | Full Spring Boot 3 observability |

### Grafana Alerting Setup

```yaml
# grafana/provisioning/alerting/rules.yaml
apiVersion: 1
groups:
  - orgId: 1
    name: spring-boot
    folder: Spring Boot
    interval: 1m
    rules:
      - uid: high-error-rate
        title: High Error Rate
        condition: C
        data:
          - refId: A
            datasourceUid: prometheus
            model:
              expr: |
                sum(rate(http_server_requests_seconds_count{status=~"5.."}[5m]))
                / sum(rate(http_server_requests_seconds_count[5m]))
              instant: true
          - refId: C
            datasourceUid: __expr__
            model:
              type: threshold
              conditions:
                - evaluator:
                    type: gt
                    params: [0.05]
        for: 2m
        labels:
          severity: critical
        annotations:
          summary: "Error rate above 5%"
```

### Dashboard Variable Best Practices

```
# Template variables for drill-down
$application = label_values(http_server_requests_seconds_count, application)
$instance    = label_values(http_server_requests_seconds_count{application="$application"}, instance)
$uri         = label_values(http_server_requests_seconds_count{application="$application"}, uri)
```

---

## 19. OpenTelemetry vs Micrometer — When to Use What

### Comparison

| Aspect | Micrometer | OpenTelemetry |
|---|---|---|
| **Primary focus** | Metrics (with Tracing via bridge) | Metrics + Traces + Logs (unified) |
| **Language support** | Java/JVM only | Multi-language (Java, Go, Python, JS, ...) |
| **Spring integration** | Native (default since Boot 2.0) | Via starter or Java agent |
| **Learning curve** | Gentle for Spring devs | Steeper (broader scope) |
| **Distributed tracing** | Via Micrometer Tracing bridge | Full W3C Trace Context, native |
| **Auto-instrumentation** | Spring-specific | Broader library coverage |
| **Vendor lock-in** | Low (facade pattern) | None (OTLP protocol) |
| **Maturity** | Very mature | Stable, rapidly growing (CNCF) |

### Decision Guide

| Your Situation | Choice |
|---|---|
| Spring Boot monolith, need metrics fast | **Micrometer** |
| Polyglot microservices | **OpenTelemetry** |
| Need distributed tracing across services | **OpenTelemetry** or **Micrometer Tracing bridge** |
| Existing Prometheus stack | **Micrometer** (first-class Prometheus support) |
| Want unified metrics + traces + logs | **OpenTelemetry** |
| Spring Boot 4.x, future-proofing | **Micrometer + OTLP export** (best of both) |
| Need to switch backends (Prometheus → Datadog) | Either works — both are facades |

### The Hybrid Approach (Recommended for Spring)

Use Micrometer for instrumentation, export via OTLP:

```groovy
dependencies {
    implementation 'org.springframework.boot:spring-boot-starter-actuator'
    implementation 'io.micrometer:micrometer-registry-otlp'
}
```

```yaml
management:
  otlp:
    metrics:
      export:
        url: http://otel-collector:4318/v1/metrics
        step: 30s
```

This gives you Micrometer's familiar API with OpenTelemetry's protocol and backend flexibility.

---

## 20. Naming Conventions & Best Practices

### Micrometer Naming

Always use **lowercase dot notation** in your code. Micrometer automatically converts to each backend's convention:

| Your Code | Prometheus | Datadog | Atlas |
|---|---|---|---|
| `http.server.requests` | `http_server_requests` | `http.server.requests` | `httpServerRequests` |
| `order.processing.duration` | `order_processing_duration` | `order.processing.duration` | `orderProcessingDuration` |

### Naming Rules

```
✅ Good names:
  orders.created.total          ← descriptive, dot-separated
  payment.processing.duration   ← includes what's being measured
  cache.hit.ratio               ← clear meaning
  http.server.requests          ← namespace.entity.measurement

❌ Bad names:
  orderCounter                  ← camelCase, no context
  count                         ← too generic
  duration                      ← no context, no unit indication
  requests                      ← ambiguous
  myapp_orders_total            ← don't use Prometheus naming in code
```

### Prometheus Naming Conventions

Prometheus converts dot notation to snake_case and adds suffixes:

| Meter Type | Suffix | Example |
|---|---|---|
| Counter | `_total` | `orders_created_total` |
| Gauge | (none) | `orders_pending_count` |
| Timer/Histogram | `_seconds_count`, `_seconds_sum`, `_seconds_bucket` | `order_processing_duration_seconds_*` |
| DistributionSummary | `_count`, `_sum`, `_bucket` | `order_amount_usd_*` |

### Tag Naming

```java
// ✅ Good tags
.tag("method", "GET")           // standard, bounded
.tag("status", "200")           // bounded, known values
.tag("region", "us-east-1")     // bounded, infra context
.tag("payment.method", "card")  // dot notation for multi-word

// ❌ Bad tags
.tag("m", "GET")                // too abbreviated
.tag("httpMethod", "GET")       // camelCase
.tag("userId", "u-12345")       // unbounded = cardinality bomb
```

### Common Tags (Applied to All Metrics)

```java
@Bean
public MeterRegistryCustomizer<MeterRegistry> commonTags() {
    return registry -> registry.config()
        .commonTags(
            "application", applicationName,
            "environment", activeProfile,
            "region", awsRegion
        );
}
```

Or in `application.yml`:

```yaml
management:
  metrics:
    tags:
      application: ${spring.application.name}
      environment: ${SPRING_PROFILES_ACTIVE:dev}
```

---

## 21. Testing Metrics

### Unit Testing with SimpleMeterRegistry

```java
class OrderMetricsTest {

    private SimpleMeterRegistry registry;
    private OrderMetrics orderMetrics;

    @BeforeEach
    void setup() {
        registry = new SimpleMeterRegistry();
        orderMetrics = new OrderMetrics(registry);
    }

    @Test
    void shouldIncrementOrderCreatedCounter() {
        orderMetrics.recordCreated();
        orderMetrics.recordCreated();

        Counter counter = registry.find("orders.created.total").counter();
        assertThat(counter).isNotNull();
        assertThat(counter.count()).isEqualTo(2.0);
    }

    @Test
    void shouldRecordProcessingTime() throws Exception {
        orderMetrics.recordProcessingTime(() -> {
            Thread.sleep(50);
            return new Order();
        });

        Timer timer = registry.find("orders.processing.duration").timer();
        assertThat(timer).isNotNull();
        assertThat(timer.count()).isEqualTo(1);
        assertThat(timer.totalTime(TimeUnit.MILLISECONDS)).isGreaterThan(40);
    }

    @Test
    void shouldTrackPendingOrders() {
        orderMetrics.recordCreated();
        orderMetrics.recordCreated();
        orderMetrics.recordSuccess();

        Gauge gauge = registry.find("orders.pending.count").gauge();
        assertThat(gauge).isNotNull();
        assertThat(gauge.value()).isEqualTo(1.0);
    }
}
```

### Integration Testing

```java
@SpringBootTest
@AutoConfigureMetrics
class OrderServiceIntegrationTest {

    @Autowired
    private MeterRegistry registry;

    @Autowired
    private OrderService orderService;

    @AfterEach
    void cleanup() {
        registry.clear();
    }

    @Test
    void shouldRecordMetricsOnOrderCreation() {
        orderService.createOrder(new OrderRequest("item-1", 2));

        assertThat(registry.find("orders.created.total")
            .counter().count()).isEqualTo(1.0);

        assertThat(registry.find("orders.processing.duration")
            .timer().count()).isEqualTo(1);
    }
}
```

### Testing MeterFilter

```java
class MetricsFilterTest {

    @Test
    void shouldNormalizeUriPaths() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        MetricsFilterConfig config = new MetricsFilterConfig();
        registry.config().meterFilter(config.normalizeUriPaths());

        registry.counter("http.requests", "uri", "/users/12345/orders").increment();
        registry.counter("http.requests", "uri", "/users/67890/orders").increment();

        // Both should map to the same metric
        Counter counter = registry.find("http.requests")
            .tag("uri", "/users/{id}/orders")
            .counter();
        assertThat(counter.count()).isEqualTo(2.0);
    }
}
```

---

## 22. Production Configuration — Complete Setup

### application.yml

```yaml
spring:
  application:
    name: order-service

management:
  server:
    port: 8081                  # separate management port — not exposed to public
  endpoints:
    web:
      exposure:
        include: health, info, prometheus, metrics, loggers
      base-path: /actuator
  endpoint:
    health:
      show-details: when-authorized
      probes:
        enabled: true           # K8s liveness/readiness
    prometheus:
      enabled: true
  metrics:
    tags:
      application: ${spring.application.name}
      environment: ${SPRING_PROFILES_ACTIVE:dev}
    distribution:
      percentiles-histogram:
        http.server.requests: true
        http.client.requests: true
      minimum-expected-value:
        http.server.requests: 1ms
        http.client.requests: 1ms
      maximum-expected-value:
        http.server.requests: 30s
        http.client.requests: 30s
      slo:
        http.server.requests: 100ms, 500ms, 1s, 5s
    enable:
      jvm: true
      process: true
      system: true
      tomcat: true
      hikaricp: true
      all: false                # disable everything first, then enable selectively
  observations:
    annotations:
      enabled: true             # enable @Observed, @Timed, @Counted

# Structured JSON logging with trace context
logging:
  structured:
    format:
      console: logstash
  pattern:
    level: "%5p [${spring.application.name:},%X{traceId:-},%X{spanId:-}]"
```

### Java Configuration

```java
@Configuration
public class MetricsConfig {

    @Value("${spring.application.name}")
    private String applicationName;

    @Bean
    public MeterRegistryCustomizer<MeterRegistry> commonTags(
            @Value("${SPRING_PROFILES_ACTIVE:dev}") String environment) {
        return registry -> registry.config()
            .commonTags(
                "application", applicationName,
                "environment", environment
            );
    }

    // Cardinality protection
    @Bean
    @Order(Ordered.HIGHEST_PRECEDENCE)
    public MeterFilter cardinalitySafetyValve() {
        return MeterFilter.maximumAllowableTags(
            "http.server.requests", "uri", 200, MeterFilter.deny());
    }

    @Bean
    public MeterFilter normalizeUris() {
        return MeterFilter.replaceTagValues("uri",
            uri -> uri.replaceAll("/\\d+", "/{id}")
                      .replaceAll("/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", "/{uuid}"));
    }

    @Bean
    public MeterFilter disableNoiseMetrics() {
        return MeterFilter.deny(id ->
            id.getName().startsWith("jvm.compilation.") ||
            id.getName().startsWith("jvm.classes.") ||
            id.getName().startsWith("process.files.")
        );
    }

    // Observation API support
    @Bean
    public ObservedAspect observedAspect(ObservationRegistry registry) {
        return new ObservedAspect(registry);
    }

    @Bean
    public TimedAspect timedAspect(MeterRegistry registry) {
        return new TimedAspect(registry);
    }

    @Bean
    public CountedAspect countedAspect(MeterRegistry registry) {
        return new CountedAspect(registry);
    }
}
```

### Docker Compose (Local Development Stack)

```yaml
services:
  app:
    build: .
    ports:
      - "8080:8080"
      - "8081:8081"
    environment:
      - SPRING_PROFILES_ACTIVE=dev

  prometheus:
    image: prom/prometheus:latest
    ports:
      - "9090:9090"
    volumes:
      - ./monitoring/prometheus.yml:/etc/prometheus/prometheus.yml
      - ./monitoring/rules.yml:/etc/prometheus/rules.yml
      - prometheus_data:/prometheus

  grafana:
    image: grafana/grafana:latest
    ports:
      - "3000:3000"
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=admin
    volumes:
      - grafana_data:/var/lib/grafana
      - ./monitoring/grafana/provisioning:/etc/grafana/provisioning

volumes:
  prometheus_data:
  grafana_data:
```

---

## 23. Production Anti-Patterns & Pitfalls

### Anti-Pattern 1: High-Cardinality Tags → OOM

```java
// ❌ Creates a new time series for EVERY user
Counter.builder("api.requests")
    .tag("userId", userId)           // unbounded → millions of series
    .register(registry)
    .increment();

// ✅ Bounded tier instead
Counter.builder("api.requests")
    .tag("tier", getUserTier(userId)) // "free", "premium", "enterprise"
    .register(registry)
    .increment();
```

**Impact**: OOM kills, monitoring backend crashes, Prometheus scrape timeouts.

### Anti-Pattern 2: Registering Meters in Hot Paths

```java
// ❌ Creates/looks up meter on every request
public void handleRequest(Request req) {
    Counter.builder("requests.total")
        .tag("path", req.getPath())
        .register(registry)           // lookup cost on every call
        .increment();
}

// ✅ Pre-register at construction time
private final Counter requestCounter;

public MyService(MeterRegistry registry) {
    this.requestCounter = Counter.builder("requests.total")
        .register(registry);
}

public void handleRequest(Request req) {
    requestCounter.increment();       // just an atomic increment
}
```

### Anti-Pattern 3: Using publishPercentiles for Fleet-Wide SLOs

```java
// ❌ Client-side percentiles — NOT aggregable across pods
Timer.builder("api.latency")
    .publishPercentiles(0.99)         // each pod computes its own P99
    .register(registry);

// ✅ Server-side histogram — aggregable
Timer.builder("api.latency")
    .publishPercentileHistogram()     // Prometheus computes fleet-wide P99
    .minimumExpectedValue(Duration.ofMillis(1))
    .maximumExpectedValue(Duration.ofSeconds(10))
    .register(registry);
```

### Anti-Pattern 4: Exposing All Actuator Endpoints

```yaml
# ❌ Exposes thread dumps, heap dumps, env vars to the internet
management:
  endpoints:
    web:
      exposure:
        include: "*"

# ✅ Only expose what's needed, on a separate port
management:
  server:
    port: 8081
  endpoints:
    web:
      exposure:
        include: health, prometheus
  endpoint:
    health:
      show-details: never
```

### Anti-Pattern 5: No Histogram Clamping

```java
// ❌ Ships all 276 default buckets per metric
Timer.builder("api.latency")
    .publishPercentileHistogram()
    .register(registry);              // 276 buckets × tag combos = explosion

// ✅ Clamp to realistic range
Timer.builder("api.latency")
    .publishPercentileHistogram()
    .minimumExpectedValue(Duration.ofMillis(1))
    .maximumExpectedValue(Duration.ofSeconds(10))  // ~73 buckets
    .register(registry);
```

### Anti-Pattern 6: Alerting on Averages

```promql
# ❌ Average hides outliers — 99 users see 50ms, 1 user sees 30 seconds
avg(http_server_requests_seconds_sum / http_server_requests_seconds_count)

# ✅ Alert on percentiles — catches real user impact
histogram_quantile(0.99, sum by (le) (rate(http_server_requests_seconds_bucket[5m]))) > 2
```

### Anti-Pattern 7: Counting What Should Be Timed

```java
// ❌ Separate counter when Timer already counts
Counter requestCounter = Counter.builder("http.requests.count").register(registry);
Timer requestTimer = Timer.builder("http.requests.duration").register(registry);

public void handle() {
    requestCounter.increment();                   // redundant
    requestTimer.record(() -> doWork());
}

// ✅ Timer includes count
Timer requestTimer = Timer.builder("http.requests.duration").register(registry);

public void handle() {
    requestTimer.record(() -> doWork());           // count + duration
}
```

### Anti-Pattern 8: Gauge with Non-Thread-Safe State

```java
// ❌ Race condition — non-atomic reads
private int activeConnections = 0;
Gauge.builder("connections.active", this, s -> s.activeConnections).register(registry);

// ✅ Use AtomicInteger or AtomicLong
private final AtomicInteger activeConnections = new AtomicInteger(0);
Gauge.builder("connections.active", activeConnections, AtomicInteger::get).register(registry);
```

---

## 24. Production Issue Runbook

### Issue: Prometheus Scrape Timeouts

**Symptoms**: Gaps in Grafana dashboards, `up` metric shows 0.

**Diagnosis**:
```promql
up{job="spring-boot-app"} == 0
scrape_duration_seconds{job="spring-boot-app"}
```

**Common causes**:
1. Too many metrics (high cardinality) → scrape takes > 10s
2. Management port not accessible from Prometheus pod
3. Application under heavy GC pressure

**Fixes**:
- Add MeterFilter to deny unnecessary metrics
- Check network connectivity between Prometheus and management port
- Increase `scrape_timeout` in prometheus.yml (default 10s)

### Issue: OOM from Metric Cardinality

**Symptoms**: Heap dump shows `io.micrometer` classes consuming >50% of heap.

**Diagnosis**:
```bash
curl http://localhost:8081/actuator/metrics | jq '.names | length'
curl http://localhost:8081/actuator/prometheus | wc -l
```

**Fixes**:
1. Enable `HighCardinalityTagsDetector`
2. Add `MeterFilter.maximumAllowableTags()` as a safety valve
3. Add `MeterFilter.replaceTagValues()` to normalize URIs
4. Review and remove high-cardinality tags (`userId`, `requestId`)

### Issue: Metrics Show Zero After Restart

**Expected behavior**: Counters reset to 0 on restart. Prometheus uses `rate()` / `increase()` which handles resets gracefully.

**Problem if**: You're using raw counter values instead of `rate()` in dashboards.

**Fix**: Always use `rate()` or `increase()` for counter metrics in PromQL.

### Issue: P99 Latency Shows NaN

**Cause**: Not enough data points in the histogram. `histogram_quantile` returns NaN when there are no samples in the bucket range.

**Fix**:
```promql
# Filter out NaN
histogram_quantile(0.99, sum by (le) (rate(http_server_requests_seconds_bucket[5m]))) > 0
# Or: widen the time range
histogram_quantile(0.99, sum by (le) (rate(http_server_requests_seconds_bucket[15m])))
```

### Issue: Kafka Consumer Lag Always Shows 0

**Cause**: The `kafka.consumer.fetch.manager.records.lag` metric depends on `max.poll.records`. If lag < `max.poll.records` (default 500), it reports 0.

**Fix**: Set `max.poll.records=1` temporarily to verify, or use external lag monitoring (Burrow, kafka-lag-exporter).

### Issue: HikariCP Connection Pool Exhaustion

**Symptoms**: `hikaricp_connections_pending` > 0 and climbing.

**Diagnosis**:
```promql
hikaricp_connections_active / hikaricp_connections_max > 0.9
hikaricp_connections_pending > 0
rate(hikaricp_connections_timeout_total[5m]) > 0
```

**Fixes**:
1. Increase `maximumPoolSize` (carefully — DB has limits too)
2. Reduce connection hold time (optimize queries)
3. Add connection leak detection: `spring.datasource.hikari.leak-detection-threshold=60000`

### Issue: Grafana Dashboard Shows "No Data"

**Checklist**:
1. Is Prometheus scraping? Check `up{job="..."}` in Prometheus UI
2. Is the metric exposed? Check `curl http://host:8081/actuator/prometheus | grep metric_name`
3. Is the metric name correct? Micrometer uses dots, Prometheus uses underscores
4. Is the time range correct? Check Grafana time picker
5. Does the data source point to the right Prometheus instance?

---

## 25. Lead Interview Questions & Answers

### Fundamentals (5 questions)

**Q1: What is the difference between monitoring and observability?**

**A**: Monitoring answers "is it broken?" through pre-defined dashboards and alerts for known failure modes. Observability answers "why is it broken?" by letting you explore arbitrary questions from system outputs (metrics, traces, logs). Monitoring is a subset of observability. In a microservices world, you can't predict all failure modes, so observability — the ability to ask new questions without deploying new instrumentation — is essential. *(Section 2)*

**Q2: Explain the RED, USE, and Four Golden Signals methodologies. When would you use each?**

**A**: RED (Rate, Errors, Duration) measures user-facing service health — use it for API/service monitoring. USE (Utilization, Saturation, Errors) measures infrastructure resource health — use it for CPU, memory, disk, network. The Four Golden Signals (Latency, Traffic, Errors, Saturation) from Google SRE combines both perspectives. In practice, I use RED for every service dashboard and USE for infrastructure, giving complete coverage. *(Section 4)*

**Q3: What is Micrometer and why is it described as "SLF4J for metrics"?**

**A**: Micrometer is a vendor-neutral metrics facade for Java. Just like SLF4J lets you switch logging backends (Logback, Log4j) without changing code, Micrometer lets you switch monitoring backends (Prometheus, Datadog, CloudWatch) by swapping a dependency. You instrument once using the Micrometer API, and the registry implementation handles backend-specific formatting. Spring Boot auto-configures it via Actuator. *(Section 6)*

**Q4: What are the four meter types in Micrometer? Give a real-world example for each.**

**A**: (1) **Counter** — monotonically increasing value, e.g., total orders placed. (2) **Gauge** — fluctuating current state, e.g., queue depth or active connections. (3) **Timer** — latency + count, e.g., API response time — it's a specialized DistributionSummary that understands time units. (4) **DistributionSummary** — distribution of non-time values, e.g., order amounts in USD or payload sizes in bytes. Critical rule: never count something you can time — Timer already includes a count. *(Section 7)*

**Q5: What is an SLO and how does it relate to SLI, SLA, and error budget?**

**A**: An SLI (Service Level Indicator) is the measurement — "P99 latency is 180ms." An SLO (Service Level Objective) is the target — "P99 latency must be < 200ms over 30 days." An SLA (Service Level Agreement) is the contract with business consequences — "99.9% uptime or 10% refund." The error budget is `100% - SLO target` — it's the allowed room for failure. With a 99.9% SLO, you have 43 minutes of downtime per month. If you're burning budget too fast, freeze deploys and focus on reliability. *(Section 5)*

---

### Architecture & Design (5 questions)

**Q6: How would you design a metrics strategy for a microservices platform from scratch?**

**A**: I'd use a layered approach: (1) **Auto-configured metrics** from Spring Boot Actuator — HTTP, JVM, connection pools — zero code. (2) **RED metrics per service** via Micrometer + Prometheus — request rate, error rate, latency percentiles. (3) **Custom business metrics** — orders/sec, payment success rate, conversion funnel — using dedicated `@Component` metric classes. (4) **Common tags** — application name, environment, region — applied via `MeterRegistryCustomizer`. (5) **MeterFilters** — cardinality protection, URI normalization. (6) **Prometheus recording rules** — pre-compute error ratios and SLOs. (7) **Multi-window burn rate alerting** — SLO-based alerts that are precise and low-noise. (8) **Grafana dashboards** — Fleet overview → Service deep dive → JVM → Business. *(Sections 4, 10, 11, 16, 17, 18)*

**Q7: publishPercentiles vs publishPercentileHistogram — which do you use in production and why?**

**A**: Always `publishPercentileHistogram()` with Prometheus. `publishPercentiles()` computes percentiles client-side inside the JVM — they are NOT aggregable across multiple pods. If I have 10 pods, I get 10 separate P99 values that I cannot combine into a fleet-wide P99. `publishPercentileHistogram()` exports cumulative bucket counts that Prometheus can aggregate with `histogram_quantile()`. I always clamp with `minimumExpectedValue` and `maximumExpectedValue` to control the number of buckets (default 276, clamped to ~73). *(Section 8)*

**Q8: How does the Observation API differ from direct Micrometer instrumentation?**

**A**: The Observation API creates a single instrumentation point that automatically produces both metrics (Timer) and trace spans. It distinguishes between `lowCardinalityKeyValue` (→ metric tags + trace attributes) and `highCardinalityKeyValue` (→ trace attributes only), preventing the cardinality problem at the API level. Direct Micrometer gives more control over specific meter types but requires separate instrumentation for tracing. For new code in Spring Boot 3+, I prefer the Observation API for unified observability; I use direct Micrometer for metrics-only scenarios where I need fine-grained control (Counters, Gauges, DistributionSummaries). *(Section 14)*

**Q9: When would you choose OpenTelemetry over Micrometer?**

**A**: I'd choose OpenTelemetry when: (1) I have a polyglot system (Go, Python, Java) and need consistent instrumentation. (2) I need full distributed tracing with W3C Trace Context. (3) I want a unified pipeline for metrics, traces, and logs via OTLP. I'd stick with Micrometer when: (1) It's a Spring-only stack. (2) I only need metrics. (3) I want minimal setup with Actuator. The hybrid approach — Micrometer API with OTLP export (`micrometer-registry-otlp`) — gives the best of both worlds: familiar Spring APIs with OpenTelemetry protocol and backend flexibility. *(Section 19)*

**Q10: How would you handle metrics in a multi-tenant SaaS application?**

**A**: I'd use bounded tenant-tier tags (not tenant IDs): `tier=free|premium|enterprise`. For per-tenant visibility without cardinality explosion, I'd: (1) Use traces with `tenantId` as a high-cardinality span attribute. (2) Use structured logs with `tenantId` in MDC. (3) For aggregate SLOs, define them per tier, not per tenant. (4) If specific tenant metrics are needed, use a separate metric with `MeterFilter.maximumAllowableTags()` as a safety valve. The key insight: metrics are for aggregates, traces are for individual requests, logs are for details. *(Sections 9, 14)*

---

### Production Scenarios (5 questions)

**Q11: Your monitoring system crashes with OOM. Investigation shows Micrometer consuming 50%+ of heap. What happened and how do you fix it?**

**A**: This is the classic high-cardinality problem. Steps: (1) Get a heap dump — look for `io.micrometer` objects. (2) Check `/actuator/prometheus | wc -l` — if >100K lines, cardinality is the issue. (3) Identify the culprit — look for tags with unbounded values (user IDs, full URI paths, request IDs). Real-world case: a pentesting tool sent 50,000 random URLs; auto-instrumentation created 3M time series. (4) Fix immediately: add `MeterFilter.maximumAllowableTags()` as a safety valve. (5) Fix root cause: normalize URIs with `MeterFilter.replaceTagValues()`, remove user-specific tags, use `HighCardinalityTagsDetector` for ongoing monitoring. *(Sections 9, 10, 23)*

**Q12: Your Grafana dashboard shows P99 latency as NaN. What's wrong?**

**A**: `histogram_quantile()` returns NaN when there are no data points in the histogram buckets within the query range. Common causes: (1) The metric was just created and not enough scrapes have occurred. (2) The `rate()` time range is too narrow — widen from `[5m]` to `[15m]`. (3) The histogram buckets don't cover the actual latency range — check `minimumExpectedValue` / `maximumExpectedValue`. (4) No traffic to the endpoint in the query window. Fix: filter out NaN with `> 0`, widen time range, and ensure histogram clamping matches your actual latency distribution. *(Section 24)*

**Q13: After deploying a new version, your order count drops to zero in Grafana. Customers are still ordering. What happened?**

**A**: Counters reset to 0 on application restart. If the dashboard uses raw counter values instead of `rate()` or `increase()`, it shows the raw value which restarted at 0. `rate()` and `increase()` automatically handle counter resets by detecting the decrease and adjusting. Fix: always use `rate()` or `increase()` for counter metrics in PromQL; never display raw counter values. *(Section 24)*

**Q14: Your Spring Boot app has 500 endpoints and Prometheus scrape takes 30 seconds, causing timeouts. How do you reduce metric cardinality?**

**A**: (1) **Deny unnecessary metrics**: `MeterFilter.denyNameStartsWith("jvm.compilation")`, disable tomcat/process metrics you don't need. (2) **Reduce histogram buckets**: clamp all timers with `minimumExpectedValue`/`maximumExpectedValue`. (3) **Normalize URIs**: `MeterFilter.replaceTagValues("uri", ...)` to collapse path variables. (4) **Limit URI cardinality**: `MeterFilter.maximumAllowableTags("http.server.requests", "uri", 200, deny())`. (5) **Whitelist pattern**: use `MeterFilter.denyUnless()` to only allow critical metrics. (6) **Increase scrape_timeout** in Prometheus as a stopgap. (7) **Move to separate management port** to isolate scraping from application traffic. *(Sections 10, 22)*

**Q15: You need to implement multi-window burn rate alerting for your API's 99.9% availability SLO. Walk through the design.**

**A**: (1) Define the SLI: `1 - (5xx requests / total requests)` over 30 days. (2) Error budget: `100% - 99.9% = 0.1% = ~43 min/month`. (3) Create recording rules: `job:http_error_ratio:rate5m`, `rate1h`, `rate6h`, `rate1d`. (4) Fast-burn alert: 14.4x burn rate over 1h AND 5m windows — detects outages that would consume the entire budget in ~2 hours. Severity: critical, pages immediately. (5) Slow-burn alert: 3x burn rate over 1d AND 2h windows — detects gradual degradation that exhausts budget in ~3 days. Severity: warning, ticket creation. (6) Both conditions use AND between long and short windows to prevent false positives. This is the Google SRE recommended approach. *(Section 5)*

---

### Tooling & Implementation (5 questions)

**Q16: Walk me through how a metric goes from your Java code to a Grafana panel.**

**A**: (1) Code calls `counter.increment()` → updates an in-memory `AtomicLong` in the `PrometheusMeterRegistry`. (2) Every 15s, Prometheus HTTP-scrapes `/actuator/prometheus` on the management port. (3) The registry serializes all meters to Prometheus text exposition format (`metric_name{labels} value timestamp`). (4) Prometheus stores the data in its local TSDB as time-series. (5) Grafana queries Prometheus via PromQL, e.g., `rate(orders_created_total[5m])`. (6) Grafana renders the query result as a time-series panel, stat panel, or gauge visualization. (7) If an alert rule is configured, Prometheus evaluates it every `evaluation_interval` and fires to Alertmanager if the condition holds for the `for` duration. *(Sections 6, 16, 17, 18)*

**Q17: How do you test metrics in a Spring Boot application?**

**A**: Unit tests use `SimpleMeterRegistry` — an in-memory registry that doesn't export. Inject it into the component under test, execute logic, then assert against `registry.find("metric.name").counter().count()`. Integration tests use `@SpringBootTest` with `@AutoConfigureMetrics`; call `registry.clear()` in `@AfterEach` to prevent state leaking between tests. For MeterFilter tests, create a `SimpleMeterRegistry`, configure the filter, register a metric, and assert the transformation. I also validate metrics in staging by checking `/actuator/prometheus` output before production deploy. *(Section 21)*

**Q18: What is a MeterBinder and when would you use it over direct MeterRegistry injection?**

**A**: A `MeterBinder` is an interface with a single `bindTo(MeterRegistry)` method. Use it when: (1) Metrics depend on other Spring beans (ensures correct initialization order). (2) You want reusable metric modules across projects. (3) You're building a library/SDK. Spring Boot auto-discovers `MeterBinder` beans and binds them to the registry. For simple counter/timer in a service, direct `MeterRegistry` injection is fine. The key advantage of `MeterBinder` is proper lifecycle management — it guarantees the bean is available when metric values are sampled. *(Section 13)*

**Q19: Explain the difference between rate(), irate(), and increase() in PromQL.**

**A**: `rate()` calculates the per-second average rate over the full time window — smooth trends, ideal for dashboards and alerting. `irate()` uses only the last two data points for an instantaneous rate — shows real-time spikes but is noisy and not suitable for alerting. `increase()` is syntactic sugar for `rate() * window_seconds` — shows the total increase as a human-readable number ("500 requests in the last 5 minutes"). Always use `rate()` for alerting, `rate()` for dashboards, `irate()` for real-time debugging, and `increase()` for human-readable totals. All three handle counter resets gracefully. *(Section 17)*

**Q20: How do you expose metrics securely in a Kubernetes environment?**

**A**: (1) **Separate management port** (8081) — application traffic on 8080. (2) **NetworkPolicy** — allow only Prometheus pods to reach port 8081. (3) **ServiceMonitor** with explicit `endpoints[].port: management`. (4) `show-details: never` on health endpoint publicly. (5) Only expose `health` and `prometheus` endpoints — never `env`, `heapdump`, `threaddump` in production. (6) If using Istio, exclude the management port from the mesh or configure mTLS between Prometheus and pods. (7) RBAC-scoped `loggers` endpoint if runtime log level changes are needed. *(Section 22)*

---

### Advanced & Curveball (10 questions)

**Q21: You have 10 pods behind a load balancer. How do you compute fleet-wide P99 latency correctly?**

**A**: Use `publishPercentileHistogram()` (not `publishPercentiles()`) on every pod. This exports histogram bucket counts. In PromQL, aggregate buckets across all instances with `sum by (le)`, then apply `histogram_quantile()`:

```promql
histogram_quantile(0.99,
  sum by (le) (rate(http_server_requests_seconds_bucket[5m]))
)
```

This works because histogram buckets are additive — you can sum them across dimensions. Pre-computed percentiles (from `publishPercentiles()`) are NOT additive. You cannot average P99 values from 10 pods to get a meaningful fleet P99. *(Section 8)*

**Q22: How would you implement a circuit breaker metric that shows the current state (CLOSED, OPEN, HALF_OPEN)?**

**A**: Use a `Gauge` with a state map. Since metrics must be numeric, encode state as a value:

```java
Gauge.builder("circuit.breaker.state", circuitBreaker, cb -> {
    return switch (cb.getState()) {
        case CLOSED    -> 0;
        case HALF_OPEN -> 1;
        case OPEN      -> 2;
    };
})
.tag("name", circuitBreaker.getName())
.register(registry);
```

In Grafana, use value mappings: 0="CLOSED" (green), 1="HALF_OPEN" (yellow), 2="OPEN" (red). For transitions, also add a Counter: `circuit.breaker.state.transitions.total` with a `to_state` tag.

**Q23: Your team wants to add userId as a tag to every metric for per-user analytics. How do you push back?**

**A**: With 100K users, adding `userId` as a tag on 10 metrics creates 1M+ time series. Each series consumes memory in the app, Prometheus, and Grafana. Concrete consequences: (1) App OOM from Micrometer registry growth. (2) Prometheus scrape timeouts. (3) Prometheus TSDB memory explosion. (4) Grafana query slowdowns. Instead: use traces with `userId` as a high-cardinality span attribute (searchable in Tempo/Jaeger), structured logs with `userId` in MDC (searchable in Loki/Elasticsearch), and metric tags with bounded `userTier` (free/premium/enterprise). Metrics answer "what's the aggregate behavior?" — traces answer "what happened to this user?" *(Section 9)*

**Q24: How do you handle metrics for short-lived batch jobs that can't be scraped by Prometheus?**

**A**: Prometheus is pull-based — if the job finishes before the next scrape, data is lost. Solutions: (1) **Pushgateway** — the job pushes metrics to a gateway that Prometheus scrapes. Caveat: the gateway becomes a single point of failure, and stale metrics persist until explicitly deleted. (2) **OTLP push** — use `micrometer-registry-otlp` to push metrics directly to an OpenTelemetry Collector. (3) **Log-based metrics** — emit structured logs with metric values; process with Loki or CloudWatch Logs Insights. Pushgateway is simplest for small-scale; OTLP is better for production at scale.

**Q25: What happens when Prometheus is down for 5 minutes? Do you lose data?**

**A**: You lose data for those 5 minutes — Prometheus is a pull-based system, and missed scrapes mean missed data points. `rate()` and `increase()` handle gaps gracefully by interpolating. To mitigate: (1) Run Prometheus in HA pairs (Thanos sidecar or Prometheus Agent + remote write). (2) Use Thanos or Cortex for long-term storage with replication. (3) For critical metrics, use WAL-based remote write to ensure data survives Prometheus restarts. (4) Keep scrape intervals reasonable (15s) — shorter intervals mean smaller gaps.

**Q26: How do you correlate a Grafana metric spike with the specific requests that caused it?**

**A**: Using **exemplars**. When `publishPercentileHistogram()` is enabled and tracing is configured, Micrometer attaches trace IDs as exemplars to histogram buckets. In Grafana, hovering over a histogram panel shows exemplar dots; clicking one opens the trace in Tempo/Jaeger. Requirements: (1) Micrometer Tracing or OpenTelemetry configured. (2) `management.metrics.distribution.percentiles-histogram.*=true`. (3) Grafana configured with both Prometheus and Tempo as data sources. (4) Prometheus configured with `--enable-feature=exemplar-storage`.

**Q27: You're migrating from Datadog to Prometheus. How do you handle the transition without losing observability?**

**A**: Micrometer's facade pattern makes this smooth: (1) Add `micrometer-registry-prometheus` alongside `micrometer-registry-datadog` — the `CompositeMeterRegistry` sends to both. (2) Set up Prometheus, Grafana, and alerting in parallel. (3) Rebuild dashboards in Grafana (Prometheus naming differs from Datadog). (4) Validate: compare Datadog and Grafana side-by-side for 2 weeks. (5) Migrate alerts — recreate Datadog monitors as Prometheus alerting rules. (6) Cut over: remove `micrometer-registry-datadog` dependency. (7) Zero application code changes throughout — just dependency swaps. *(Section 6)*

**Q28: How do you prevent a MeterFilter from silently dropping important metrics?**

**A**: (1) **Filter evaluation order matters** — use `@Order(Ordered.HIGHEST_PRECEDENCE)` for critical allows. (2) **Log denied meters** — instead of `MeterFilter.deny()`, implement a custom filter that logs before denying. (3) **Test filters** — unit test with `SimpleMeterRegistry`, register the filter, attempt to register a metric, verify it was denied/allowed. (4) **HighCardinalityTagsDetector** — monitors and warns about high-cardinality metrics in production. (5) **Audit** — periodically check `/actuator/prometheus` output line count and compare against baseline. *(Section 10)*

**Q29: Your team is debating between @Timed annotation and programmatic Timer. Which do you recommend?**

**A**: Use `@Timed` for simple, uniform instrumentation where you don't need conditional tags — controllers, repository methods. Use programmatic `Timer` when: (1) You need dynamic tags based on runtime context. (2) You need the recording to wrap only part of a method. (3) You need `Timer.Sample` for async flows. (4) You need fine-grained control over histogram configuration. Critical caveat: `@Timed` requires `TimedAspect` bean, only works on public methods, and fails silently on self-invocations. In production, I've seen many teams get burned by annotations not working because they forgot the aspect bean or called the method internally. Programmatic is explicit and never silently fails. *(Section 12)*

**Q30: Design a metrics-based canary deployment validation system.**

**A**: (1) Tag metrics with `version` as a common tag: `v1` (stable) vs `v2` (canary). (2) Route 5% of traffic to canary via Istio VirtualService. (3) Create comparison PromQL:

```promql
# Canary error rate vs stable
job:http_error_ratio:rate5m{version="v2"} > 2 * job:http_error_ratio:rate5m{version="v1"}
```

(4) Alert if canary error rate exceeds 2x the stable rate for 5 minutes. (5) Alert if canary P99 latency exceeds 1.5x the stable P99. (6) Use Argo Rollouts or Flagger to automate the decision: promote if within bounds, rollback if not. (7) Include business metrics: canary `orders.created.total` rate shouldn't be significantly lower than expected based on traffic split.

---

## Quick Reference Card

### Meter Type Selection

```
Is it time? → Timer
Is it a count that only goes up? → Counter
Is it a current state that fluctuates? → Gauge
Is it a non-time distribution? → DistributionSummary
Do you need metrics + tracing together? → Observation API
```

### Essential PromQL Patterns

```promql
# Rate of change
rate(metric_total[5m])

# Error ratio
sum(rate(errors_total[5m])) / sum(rate(requests_total[5m]))

# Percentile from histogram
histogram_quantile(0.99, sum by (le) (rate(metric_bucket[5m])))

# Average from summary
increase(metric_sum[5m]) / increase(metric_count[5m])
```

### Production Checklist

- [ ] Separate management port (8081)
- [ ] Only expose health + prometheus endpoints
- [ ] Common tags: application, environment
- [ ] MeterFilter for URI normalization
- [ ] MeterFilter for cardinality safety valve
- [ ] publishPercentileHistogram (not publishPercentiles)
- [ ] Histogram clamping (min/max expected values)
- [ ] Recording rules for error ratios
- [ ] Multi-window burn rate alerts
- [ ] Fleet overview + service deep dive dashboards
- [ ] Metrics tested with SimpleMeterRegistry
- [ ] HighCardinalityTagsDetector enabled

---

*Sources: Micrometer Official Docs, Spring Boot Actuator Reference, Prometheus Documentation, Google SRE Workbook, Grafana Cloud Docs, Medium engineering blogs, Reddit r/java and r/prometheus discussions, Confluent/Uptrace/Better Stack community guides.*
