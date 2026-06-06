# Java Modern Concurrency, Streams & Java 21 — Expert Playbook

> **Goal**: Make any Java developer production-ready with modern Java — CompletableFuture, Virtual Threads, Structured Concurrency, Java 21 features, Streams, and everything in between.
> Covers official docs, Medium engineering blogs, Reddit r/java discussions, and production war stories.

---

## Table of Contents

1. [Future vs CompletableFuture — The Foundation](#1-future-vs-completablefuture--the-foundation)
2. [CompletableFuture API Deep Dive](#2-completablefuture-api-deep-dive)
3. [Composing Async Pipelines — thenApply vs thenCompose](#3-composing-async-pipelines--thenapply-vs-thencompose)
4. [Combining Futures — thenCombine, allOf, anyOf](#4-combining-futures--thencombine-allof-anyof)
5. [Error Handling in CompletableFuture](#5-error-handling-in-completablefuture)
6. [Thread Pools — The Most Critical Decision](#6-thread-pools--the-most-critical-decision)
7. [Timeouts — Never Trust External Services](#7-timeouts--never-trust-external-services)
8. [MDC & Context Propagation in Async Code](#8-mdc--context-propagation-in-async-code)
9. [CompletableFuture Production Patterns](#9-completablefuture-production-patterns)
10. [CompletableFuture Production Pitfalls](#10-completablefuture-production-pitfalls)
11. [Threads Deep Dive — Platform Threads](#11-threads-deep-dive--platform-threads)
12. [Virtual Threads — Project Loom (Java 21)](#12-virtual-threads--project-loom-java-21)
13. [Virtual Threads vs Platform Threads vs WebFlux](#13-virtual-threads-vs-platform-threads-vs-webflux)
14. [Virtual Threads Production Pitfalls & Pinning](#14-virtual-threads-production-pitfalls--pinning)
15. [Structured Concurrency — StructuredTaskScope (Java 21+)](#15-structured-concurrency--structuredtaskscope-java-21)
16. [Java 21 Features — Records](#16-java-21-features--records)
17. [Java 21 Features — Sealed Classes](#17-java-21-features--sealed-classes)
18. [Java 21 Features — Pattern Matching for Switch](#18-java-21-features--pattern-matching-for-switch)
19. [Java 21 Features — Sequenced Collections, Text Blocks & More](#19-java-21-features--sequenced-collections-text-blocks--more)
20. [Java Streams — Pipeline Internals & Laziness](#20-java-streams--pipeline-internals--laziness)
21. [Stream Operations Deep Dive](#21-stream-operations-deep-dive)
22. [Collectors — from groupingBy to Custom](#22-collectors--from-groupingby-to-custom)
23. [Parallel Streams — When and When NOT to Use](#23-parallel-streams--when-and-when-not-to-use)
24. [Stream Performance Traps & Best Practices](#24-stream-performance-traps--best-practices)
25. [Lead Interview Questions & Answers](#25-lead-interview-questions--answers)

---

## 1. Future vs CompletableFuture — The Foundation

### The Problem with `Future` (Java 5)

`Future` was Java's first async abstraction. It works by submitting a task to an `ExecutorService` and getting a handle. But it has a fatal flaw: **to read the result you must block the calling thread with `.get()`**.

```java
// Old-school Future — blocks the calling thread
ExecutorService executor = Executors.newFixedThreadPool(10);
Future<String> future = executor.submit(() -> callRemoteService());

// This BLOCKS until the result is available
String result = future.get(); // ← blocked here
```

### Problems with `Future.get()`

| Problem | Impact |
|---|---|
| Blocks the calling thread | Threads pile up waiting, killing throughput |
| No way to chain tasks | Manual loop/polling required |
| No exception composition | Must catch `ExecutionException` and unwrap cause |
| No timeout-safe defaults | `.get()` with no timeout hangs indefinitely |
| Cannot be completed externally | No way to push a value in |
| No callbacks | Must poll — wastes a thread |

### Real Production War Story

> A team used 200 platform threads with `Future.get()` calls inside a REST controller. Under moderate load, all 200 threads blocked on database + external API calls. Tomcat ran out of threads and the service went down, not from traffic volume but from thread starvation. — (r/java, commonly reported)

### `CompletableFuture` (Java 8) — The Fix

`CompletableFuture` implements both `Future` **and** `CompletionStage`. It adds:
- **Non-blocking callbacks** — define what to do when complete
- **Composable pipelines** — chain transformations
- **Error handling** — functional exception handling
- **External completion** — call `.complete()` or `.completeExceptionally()`
- **Combination** — merge multiple futures

```java
// Modern CompletableFuture — non-blocking pipeline
CompletableFuture<String> result = CompletableFuture
    .supplyAsync(() -> callRemoteService(), executor)
    .thenApply(String::toUpperCase)
    .thenApply(s -> "Processed: " + s)
    .exceptionally(ex -> "Fallback");
```

### Comparison Table

| Feature | `Future` | `CompletableFuture` |
|---|---|---|
| Non-blocking result | No (`.get()` blocks) | Yes (callbacks) |
| Chaining | No | Yes (`thenApply`, `thenCompose`) |
| Error handling | try/catch around `.get()` | `.exceptionally()`, `.handle()` |
| Combining | Manual | `allOf`, `anyOf`, `thenCombine` |
| Timeout | No | Yes (`.orTimeout()`, `.completeOnTimeout()`) |
| External completion | No | Yes (`.complete()`, `.completeExceptionally()`) |
| Thread pool control | Via executor | Via executor per stage |
| Callbacks | No | `thenAccept`, `thenRun`, `whenComplete` |

---

## 2. CompletableFuture API Deep Dive

### Creating CompletableFutures

```java
// 1. Already-completed value (useful for testing/mocking)
CompletableFuture<String> immediate = CompletableFuture.completedFuture("hello");

// 2. Already-failed future
CompletableFuture<String> failed = CompletableFuture.failedFuture(new RuntimeException("oops"));

// 3. Async computation that returns a value (uses ForkJoinPool.commonPool by default)
CompletableFuture<Order> order = CompletableFuture.supplyAsync(
    () -> orderRepository.findById(id),
    ioExecutor  // ← always provide your own executor
);

// 4. Async computation with no return value
CompletableFuture<Void> notification = CompletableFuture.runAsync(
    () -> emailService.send(email),
    ioExecutor
);

// 5. Manual completion (useful for wrapping callback-based APIs)
CompletableFuture<String> manual = new CompletableFuture<>();
asyncCallbackApi.call(result -> manual.complete(result),
                      error  -> manual.completeExceptionally(error));
```

### The Callback Trilogy

| Method | Input | Output | Use Case |
|---|---|---|---|
| `thenApply(Function)` | Result T | New result U | Transform the value |
| `thenAccept(Consumer)` | Result T | Void | Consume the value (side effect) |
| `thenRun(Runnable)` | Nothing | Void | Run something after completion |

```java
CompletableFuture<Order> orderFuture = findOrder(id);

// thenApply — transform
CompletableFuture<Invoice> invoiceFuture = orderFuture.thenApply(Invoice::from);

// thenAccept — side effect (logging, event publishing)
invoiceFuture.thenAccept(invoice -> eventBus.publish(new InvoiceCreated(invoice)));

// thenRun — nothing from the result needed
invoiceFuture.thenRun(() -> log.info("Invoice pipeline complete"));
```

### Async Variants

Every callback has an `*Async` variant that runs the callback on the given executor (or common pool if not provided):

```java
CompletableFuture<Order> order = findOrder(id)
    .thenApplyAsync(Order::enrich, enrichmentExecutor) // runs on enrichmentExecutor
    .thenApplyAsync(Order::validate, validationExecutor); // runs on validationExecutor
```

**Without `Async`**: callback runs on the thread that completed the previous stage (or the calling thread if already complete).
**With `Async`**: callback is submitted to an executor — decouples stages from each other's thread pools.

---

## 3. Composing Async Pipelines — thenApply vs thenCompose

### The Most Important Distinction

This is **the most misunderstood concept** in `CompletableFuture`.

| Method | Function Returns | Result Type | Mental Model |
|---|---|---|---|
| `thenApply(f)` | Plain value `U` | `CompletableFuture<U>` | Like `map` on a stream |
| `thenCompose(f)` | `CompletableFuture<U>` | `CompletableFuture<U>` | Like `flatMap` on a stream |

### The Nesting Problem

```java
// BAD: thenApply when the function itself returns a CompletableFuture
CompletableFuture<CompletableFuture<Payment>> nested = 
    findOrder(orderId)
        .thenApply(order -> processPayment(order)); // nested future!

// You'd have to join twice: result.get().get() — ugly and blocking
```

### The Fix: thenCompose

```java
// GOOD: thenCompose flattens the nested future
CompletableFuture<Payment> payment =
    findOrder(orderId)
        .thenCompose(order -> processPayment(order)); // flat!
```

### Decision Rule

```
Does your lambda return a plain value?       → thenApply
Does your lambda return a CompletableFuture? → thenCompose
```

### Real Pipeline Example

```java
@Service
public class CheckoutService {

    private final OrderRepository orderRepo;
    private final InventoryService inventoryService;
    private final PaymentService paymentService;
    private final NotificationService notificationService;
    private final Executor ioExecutor;

    public CompletableFuture<CheckoutResult> checkout(CheckoutRequest request) {
        return CompletableFuture
            .supplyAsync(() -> orderRepo.create(request), ioExecutor)          // 1. Create order
            .thenComposeAsync(order ->                                           // 2. Reserve inventory (async)
                inventoryService.reserve(order.getItems())                      //    returns CF<Inventory>
                    .thenApply(inventory -> new OrderWithInventory(order, inventory)), ioExecutor)
            .thenComposeAsync(owI ->                                             // 3. Process payment (async)
                paymentService.charge(owI.order(), request.getPaymentToken())   //    returns CF<Payment>
                    .thenApply(payment -> new CompletedOrder(owI.order(), payment)), ioExecutor)
            .thenApplyAsync(completed -> {                                       // 4. Notify (fire-and-forget)
                notificationService.notifyAsync(completed.order());
                return CheckoutResult.success(completed);
            }, ioExecutor)
            .orTimeout(10, TimeUnit.SECONDS)
            .exceptionally(ex -> {
                log.error("Checkout failed for request {}", request.id(), ex);
                return CheckoutResult.failed(ex.getMessage());
            });
    }
}
```

---

## 4. Combining Futures — thenCombine, allOf, anyOf

### `thenCombine` — Merge Two Independent Futures

```java
CompletableFuture<User> userFuture = findUser(userId);
CompletableFuture<List<Order>> ordersFuture = findOrders(userId);

// Both run in parallel; BiFunction called when both complete
CompletableFuture<UserProfile> profile = userFuture.thenCombine(
    ordersFuture,
    (user, orders) -> new UserProfile(user, orders)
);
```

### `allOf` — Wait for All, No Result Aggregation

`allOf` returns `CompletableFuture<Void>` — it signals completion but does NOT aggregate results. You must collect them manually.

```java
List<CompletableFuture<Product>> futures = productIds.stream()
    .map(id -> loadProduct(id))
    .toList();

CompletableFuture<List<Product>> allProducts = CompletableFuture
    .allOf(futures.toArray(new CompletableFuture[0]))
    .thenApply(v -> futures.stream()
        .map(CompletableFuture::join)   // safe: all are complete at this point
        .toList());
```

**Pitfall**: `allOf` fails fast — if ANY future fails, the returned future also fails immediately. Other futures continue running but their results are silently ignored.

### `anyOf` — First to Complete Wins

```java
// Hedged request: call two services, take whoever responds first
CompletableFuture<Object> fastest = CompletableFuture.anyOf(
    priceService1.getPrice(productId),
    priceService2.getPrice(productId)   // backup/redundant call
);
// Type-unsafe: returns CompletableFuture<Object> — you must cast
```

**Warning**: `anyOf` is type-unsafe (returns `CompletableFuture<Object>`). For typed results, use `applyToEither`:

```java
CompletableFuture<Price> fastest = priceService1.getPrice(productId)
    .applyToEither(priceService2.getPrice(productId), Function.identity());
```

### Production Pattern: Fan-Out with Timeout

```java
public CompletableFuture<EnrichedOrder> enrichOrder(Order order) {
    CompletableFuture<CustomerDetails> customerFuture =
        customerService.getDetails(order.customerId())
            .orTimeout(2, TimeUnit.SECONDS)
            .exceptionally(ex -> CustomerDetails.unknown());

    CompletableFuture<ShippingInfo> shippingFuture =
        shippingService.getInfo(order.shippingAddressId())
            .orTimeout(2, TimeUnit.SECONDS)
            .exceptionally(ex -> ShippingInfo.unknown());

    CompletableFuture<PricingInfo> pricingFuture =
        pricingService.calculate(order.items())
            .orTimeout(3, TimeUnit.SECONDS)
            .exceptionally(ex -> PricingInfo.standard());

    return customerFuture.thenCombine(shippingFuture, 
                (customer, shipping) -> new PartialEnrich(order, customer, shipping))
        .thenCombine(pricingFuture,
                (partial, pricing) -> EnrichedOrder.build(partial, pricing))
        .orTimeout(5, TimeUnit.SECONDS);
}
```

---

## 5. Error Handling in CompletableFuture

### The Silent Failure Problem

**The #1 production bug with CompletableFuture**: exceptions are silently swallowed if nobody observes them.

```java
// DANGER: fire-and-forget without error handling
CompletableFuture.supplyAsync(() -> callExpensiveService(), executor);
// Exception thrown inside? Silently discarded. No log, no metric, nothing.
```

### The Three Error Handlers

| Method | Signature | Behavior | Use Case |
|---|---|---|---|
| `exceptionally(Function)` | `Throwable → T` | Handles exception, provides fallback | Simple fallback value |
| `handle(BiFunction)` | `(T, Throwable) → U` | Handles both success and failure | Transform result regardless of outcome |
| `whenComplete(BiConsumer)` | `(T, Throwable) → void` | Side-effect; does NOT change result/exception | Logging, metrics, cleanup |

```java
// exceptionally — provide fallback value
CompletableFuture<Price> price = getPriceFromAPI()
    .exceptionally(ex -> {
        log.error("Price API failed, using default", ex);
        priceFailureCounter.increment();
        return Price.defaultPrice();
    });

// handle — inspect both outcomes
CompletableFuture<Response> response = callService()
    .handle((result, ex) -> {
        if (ex != null) {
            log.error("Service call failed", ex);
            return Response.error(ex.getMessage());
        }
        return Response.success(result);
    });

// whenComplete — side effects only (logging, metrics), does NOT change the result
CompletableFuture<Order> trackedOrder = processOrder(request)
    .whenComplete((order, ex) -> {
        if (ex != null) {
            metrics.recordFailure("order.processing");
        } else {
            metrics.recordSuccess("order.processing");
        }
    });
// trackedOrder still completes with the original value or exception
```

### Error Propagation Rules

```java
// Exception propagates through thenApply — it is skipped
CompletableFuture.failedFuture(new RuntimeException("fail"))
    .thenApply(s -> s.toUpperCase())  // ← skipped, exception propagates
    .thenApply(s -> s + "!")          // ← skipped, exception propagates
    .exceptionally(ex -> "caught: " + ex.getMessage()); // ← only this runs
```

### Production Error Handling Pattern

```java
public CompletableFuture<OrderResult> processOrder(OrderRequest request) {
    return CompletableFuture
        .supplyAsync(() -> validateOrder(request), executor)
        .thenComposeAsync(valid -> chargePayment(valid), executor)
        .thenComposeAsync(payment -> fulfillOrder(payment), executor)
        .whenComplete((result, ex) -> {
            // Always runs — for metrics
            if (ex != null) {
                orderMetrics.recordFailure(request.type(), rootCause(ex));
                log.error("Order {} failed at stage: {}", request.id(), ex.getMessage(), ex);
            } else {
                orderMetrics.recordSuccess(request.type());
            }
        })
        .exceptionally(ex -> {
            // Converts exception to a Result — no naked throws
            return OrderResult.failed(request.id(), ex.getMessage());
        });
}

private Throwable rootCause(Throwable ex) {
    return ex instanceof CompletionException ? ex.getCause() : ex;
}
```

---

## 6. Thread Pools — The Most Critical Decision

### NEVER Use `ForkJoinPool.commonPool()` for I/O

By default, `CompletableFuture.supplyAsync(task)` submits to `ForkJoinPool.commonPool()`. This is a **shared, finite pool** designed for CPU-bound work (fork/join, parallel streams, etc.). If you run I/O tasks there:

1. All common pool threads block on I/O
2. Parallel streams stall (they also use common pool)
3. Other library code stalls
4. The JVM has no way to run anything else

### Sizing Your Thread Pools

```
CPU-bound:   pool size = # CPU cores (Runtime.getRuntime().availableProcessors())
I/O-bound:   pool size = CPU cores × (1 + wait time / compute time)
             Typical: CPU cores × 50 (for 1ms compute, 50ms wait)
```

### Production Thread Pool Configuration

```java
@Configuration
public class AsyncConfig {

    /**
     * For I/O-bound tasks: database calls, external HTTP, Kafka, file I/O.
     * Java 21+ recommendation: use virtual threads instead of this pool.
     */
    @Bean(name = "ioExecutor")
    public ExecutorService ioExecutor() {
        int cores = Runtime.getRuntime().availableProcessors();
        return new ThreadPoolExecutor(
            cores * 10,                         // corePoolSize
            cores * 50,                         // maximumPoolSize
            60L, TimeUnit.SECONDS,              // keepAliveTime
            new LinkedBlockingQueue<>(5000),    // bounded queue — backpressure!
            new ThreadFactory() {
                private final AtomicInteger count = new AtomicInteger(0);
                @Override
                public Thread newThread(Runnable r) {
                    Thread t = new Thread(r);
                    t.setName("io-worker-" + count.getAndIncrement());
                    t.setDaemon(true);
                    return t;
                }
            },
            new ThreadPoolExecutor.CallerRunsPolicy()  // backpressure: caller blocks
        );
    }

    /**
     * For CPU-bound tasks: parsing, transformation, computation.
     * Keep this sized to CPU count.
     */
    @Bean(name = "cpuExecutor")
    public ExecutorService cpuExecutor() {
        int cores = Runtime.getRuntime().availableProcessors();
        return new ThreadPoolExecutor(
            cores, cores,
            0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(1000),
            r -> {
                Thread t = new Thread(r, "cpu-worker");
                t.setDaemon(true);
                return t;
            },
            new ThreadPoolExecutor.AbortPolicy()  // fail fast for CPU tasks
        );
    }

    /**
     * Java 21+: Virtual thread executor for I/O-bound work.
     * Replaces ioExecutor entirely.
     */
    @Bean(name = "virtualThreadExecutor")
    public ExecutorService virtualThreadExecutor() {
        return Executors.newVirtualThreadPerTaskExecutor();
    }
}
```

### Rejection Policies

| Policy | Behavior | When to Use |
|---|---|---|
| `AbortPolicy` (default) | Throws `RejectedExecutionException` | CPU-bound, fail fast |
| `CallerRunsPolicy` | Submitting thread runs the task | I/O-bound, natural backpressure |
| `DiscardPolicy` | Silently drops the task | Fire-and-forget, loss acceptable |
| `DiscardOldestPolicy` | Drops oldest pending task | Sliding-window workloads |

---

## 7. Timeouts — Never Trust External Services

### The Hanging Future Problem

A `CompletableFuture` that never completes **leaks memory**. The stage and all its callbacks are held in memory indefinitely.

```java
// DANGER: no timeout — hangs forever if upstream hangs
CompletableFuture<Payment> payment = paymentGateway.charge(request);
// If gateway never responds, this future is permanently in memory
```

### `orTimeout` (Java 9+)

`orTimeout` completes the future **exceptionally** with `TimeoutException` if it hasn't completed within the given time.

```java
CompletableFuture<Payment> payment = paymentGateway.charge(request)
    .orTimeout(5, TimeUnit.SECONDS)
    .exceptionally(ex -> {
        if (ex instanceof TimeoutException) {
            return Payment.timedOut();
        }
        return Payment.failed(ex.getMessage());
    });
```

### `completeOnTimeout` (Java 9+)

`completeOnTimeout` completes the future **normally** with a fallback value if it hasn't completed within the given time.

```java
CompletableFuture<Price> price = priceService.getPrice(productId)
    .completeOnTimeout(Price.defaultPrice(), 2, TimeUnit.SECONDS);
// Returns default price instead of failing if service is slow
```

### Comparison

| Method | On Timeout | Use Case |
|---|---|---|
| `orTimeout` | Completes exceptionally with `TimeoutException` | Fail loud — you want to know about timeouts |
| `completeOnTimeout` | Completes normally with default value | Graceful degradation with a cached/default value |

### Production Timeout Strategy

```java
// Layered timeouts: individual service + overall pipeline
public CompletableFuture<CheckoutResult> checkout(CartRequest request) {
    return CompletableFuture
        .supplyAsync(() -> inventoryService.check(request.items()), ioExecutor)
        .orTimeout(2, TimeUnit.SECONDS)                     // per-service timeout
        .thenComposeAsync(inv -> paymentService.charge(request.payment()), ioExecutor)
        .orTimeout(5, TimeUnit.SECONDS)                     // per-service timeout
        .thenApplyAsync(payment -> buildResult(payment), ioExecutor)
        .orTimeout(8, TimeUnit.SECONDS)                     // overall pipeline timeout
        .exceptionally(ex -> CheckoutResult.degraded(ex));
}
```

---

## 8. MDC & Context Propagation in Async Code

### The Problem

`MDC` (Mapped Diagnostic Context) and `SecurityContext` are stored in `ThreadLocal`. When a `CompletableFuture` switches threads, `ThreadLocal` is NOT copied to the new thread. This causes:
- Missing correlation IDs in logs
- Missing security context in `@Async` methods
- Broken distributed traces

```java
// Request thread: MDC has traceId = "abc123"
MDC.put("traceId", "abc123");

CompletableFuture.supplyAsync(() -> {
    // Worker thread: MDC is EMPTY — traceId is gone
    log.info("Processing order..."); // logs WITHOUT traceId!
    return processOrder();
}, executor);
```

### Solution 1: `ContextPropagatingTaskDecorator` (Spring Boot 3)

```java
@Bean
public TaskDecorator contextPropagatingDecorator() {
    return new ContextPropagatingTaskDecorator();
}

@Bean(name = "asyncExecutor")
public Executor asyncExecutor() {
    ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
    executor.setCorePoolSize(20);
    executor.setMaxPoolSize(100);
    executor.setTaskDecorator(new ContextPropagatingTaskDecorator());
    executor.initialize();
    return executor;
}
```

### Solution 2: Manual MDC Copy

```java
public static <T> CompletableFuture<T> supplyWithMDC(
        Callable<T> task, Executor executor) {
    Map<String, String> mdcContext = MDC.getCopyOfContextMap();
    return CompletableFuture.supplyAsync(() -> {
        Map<String, String> previous = MDC.getCopyOfContextMap();
        try {
            if (mdcContext != null) MDC.setContextMap(mdcContext);
            return task.call();
        } catch (Exception e) {
            throw new CompletionException(e);
        } finally {
            if (previous != null) MDC.setContextMap(previous);
            else MDC.clear();
        }
    }, executor);
}
```

### Solution 3: Wrap Executor with Context Propagation Library

```java
// Micrometer Context Propagation — wraps executor to propagate MDC, security context, spans
@Bean("tracedExecutor")
public ExecutorService tracedExecutor() {
    ExecutorService raw = Executors.newFixedThreadPool(20);
    return ContextExecutorService.wrap(
        raw,
        ContextSnapshotFactory.builder().build()::captureAll
    );
}
```

### Solution 4: Scoped Values (Java 21+ — Modern Approach)

`ScopedValue` is the Java 21 replacement for `ThreadLocal` in virtual thread contexts. Unlike `ThreadLocal`, `ScopedValue` is **immutable within a scope** and automatically propagated to child virtual threads.

```java
public static final ScopedValue<TraceContext> TRACE_CONTEXT = ScopedValue.newInstance();

public void processRequest(Request request) {
    ScopedValue.where(TRACE_CONTEXT, new TraceContext(request.getTraceId()))
        .run(() -> {
            // TRACE_CONTEXT is available throughout this execution scope
            // AND automatically visible in all virtual threads forked within this scope
            handleRequest(request);
        });
}

public void handleRequest(Request request) {
    TraceContext ctx = TRACE_CONTEXT.get(); // Available here
    // Virtual threads spawned here also inherit the ScopedValue
}
```

---

## 9. CompletableFuture Production Patterns

### Pattern 1: Fire-and-Forget (Safe Version)

```java
// UNSAFE: silent failure
CompletableFuture.runAsync(() -> sendEmail(order));

// SAFE: always log errors, always track metrics
public void sendEmailAsync(Order order) {
    CompletableFuture.runAsync(() -> sendEmail(order), emailExecutor)
        .whenComplete((v, ex) -> {
            if (ex != null) {
                log.error("Email failed for order {}", order.id(), ex);
                emailFailureCounter.increment();
            }
        });
    // Do NOT return the future — caller doesn't need to wait
}
```

### Pattern 2: Retry with Backoff

```java
public <T> CompletableFuture<T> withRetry(
        Supplier<CompletableFuture<T>> taskFactory,
        int maxAttempts,
        Duration initialDelay) {

    return attempt(taskFactory, maxAttempts, initialDelay, 1);
}

private <T> CompletableFuture<T> attempt(
        Supplier<CompletableFuture<T>> factory,
        int remaining, Duration delay, int attempt) {

    return factory.get().exceptionallyComposeAsync(ex -> {
        if (remaining <= 1) return CompletableFuture.failedFuture(ex);

        log.warn("Attempt {} failed, retrying in {}ms: {}", attempt, delay.toMillis(), ex.getMessage());
        return CompletableFuture.delayedExecutor(delay.toMillis(), TimeUnit.MILLISECONDS)
            .execute(() -> {})
            .thenCompose(v -> attempt(factory, remaining - 1, delay.multipliedBy(2), attempt + 1));
    }, executor);
}
```

### Pattern 3: Bulkhead with Semaphore

Prevent one slow dependency from consuming all threads:

```java
@Service
public class ExternalApiClient {

    private final Semaphore semaphore = new Semaphore(20); // max 20 concurrent calls

    public CompletableFuture<Response> call(Request request) {
        if (!semaphore.tryAcquire()) {
            return CompletableFuture.failedFuture(
                new CircuitOpenException("Bulkhead limit reached"));
        }
        return CompletableFuture.supplyAsync(() -> {
            try {
                return httpClient.send(request);
            } finally {
                semaphore.release();
            }
        }, executor);
    }
}
```

### Pattern 4: Testing Async Code

```java
class OrderServiceTest {

    // Use a synchronous executor so tests are deterministic
    private final Executor syncExecutor = Runnable::run;

    @Test
    void shouldProcessOrderSuccessfully() throws Exception {
        // Setup mocks to return completedFuture
        when(inventoryService.reserve(any()))
            .thenReturn(CompletableFuture.completedFuture(inventory));
        when(paymentService.charge(any()))
            .thenReturn(CompletableFuture.completedFuture(payment));

        CheckoutResult result = checkoutService.checkout(request).get(1, TimeUnit.SECONDS);

        assertThat(result.isSuccess()).isTrue();
    }

    @Test
    void shouldHandlePaymentFailure() throws Exception {
        when(inventoryService.reserve(any()))
            .thenReturn(CompletableFuture.completedFuture(inventory));
        when(paymentService.charge(any()))
            .thenReturn(CompletableFuture.failedFuture(new PaymentException("declined")));

        CheckoutResult result = checkoutService.checkout(request).get(1, TimeUnit.SECONDS);

        assertThat(result.isFailed()).isTrue();
        assertThat(result.error()).contains("declined");
    }
}
```

---

## 10. CompletableFuture Production Pitfalls

### Pitfall 1: Calling `.get()` Inside a Pipeline

```java
// ANTI-PATTERN: reintroduces blocking in an async pipeline
CompletableFuture<Result> result = service1.call()
    .thenApply(v1 -> {
        String v2 = service2.call().get(); // ← BLOCKS THE WORKER THREAD
        return combine(v1, v2);
    });

// CORRECT: compose instead
CompletableFuture<Result> result = service1.call()
    .thenCompose(v1 -> service2.call()
        .thenApply(v2 -> combine(v1, v2)));
```

### Pitfall 2: Nested `thenApply` (Wrapping) Instead of `thenCompose` (Flattening)

```java
// WRONG: CompletableFuture<CompletableFuture<Payment>>
CompletableFuture<CompletableFuture<Payment>> nested =
    findOrder(id).thenApply(order -> processPayment(order));

// RIGHT: CompletableFuture<Payment>
CompletableFuture<Payment> flat =
    findOrder(id).thenCompose(order -> processPayment(order));
```

### Pitfall 3: Unhandled Exceptions (Silent Failures)

```java
// DANGEROUS: if processOrder throws, the exception is invisible
CompletableFuture.runAsync(() -> processOrder(id));

// SAFE
CompletableFuture.runAsync(() -> processOrder(id))
    .exceptionally(ex -> { log.error("Order processing failed", ex); return null; });
```

### Pitfall 4: `allOf` Result Collection Race

```java
// WRONG: collecting results inside allOf's completion might see incomplete futures
CompletableFuture.allOf(f1, f2, f3)
    .thenApply(v -> List.of(f1.getNow(null), f2.getNow(null), f3.getNow(null)));

// CORRECT: join() is safe inside allOf's callback — all futures are complete
CompletableFuture.allOf(f1, f2, f3)
    .thenApply(v -> List.of(f1.join(), f2.join(), f3.join()));
```

### Pitfall 5: Missing Cancellation Propagation

```java
// Cancelling the composed future does NOT cancel the source future
CompletableFuture<String> source = slowService.call();
CompletableFuture<String> composed = source.thenApply(String::toUpperCase);

composed.cancel(true);
// source is still running — resource still held!
```

---

## 11. Threads Deep Dive — Platform Threads

### Thread Lifecycle

```
NEW → RUNNABLE → RUNNING → BLOCKED/WAITING/TIMED_WAITING → TERMINATED
```

### Platform Thread Costs

| Resource | Typical Cost |
|---|---|
| Stack memory | ~512KB–1MB per thread (configurable with `-Xss`) |
| OS context switch | ~1–10 microseconds |
| JVM creation overhead | ~1ms |
| Practical max threads | ~2,000–10,000 per JVM (OS limits apply) |

### Executors Reference

```java
// Fixed pool — predictable resource usage, blocks when full
Executors.newFixedThreadPool(20)

// Cached pool — unbounded threads, DANGEROUS for I/O (OOM risk)
Executors.newCachedThreadPool()  // ← never use in production for I/O

// Single thread — serial execution
Executors.newSingleThreadExecutor()

// Scheduled — for repeating tasks
Executors.newScheduledThreadPool(5)

// Fork/Join — for CPU-bound divide-and-conquer (parallel streams use this)
ForkJoinPool.commonPool()
new ForkJoinPool(4) // custom, with parallelism
```

### ThreadLocal — What It Is and Why It Leaks

`ThreadLocal` stores a value per thread. With thread pools (where threads are reused), **a value set in one request persists into the next** unless explicitly removed.

```java
// ThreadLocal leak in thread pools
public class RequestContext {
    private static final ThreadLocal<String> USER_ID = new ThreadLocal<>();

    public static void setUserId(String id) { USER_ID.set(id); }
    public static String getUserId() { return USER_ID.get(); }

    // MUST CLEAR in finally block or request lifecycle ends with stale data
    public static void clear() { USER_ID.remove(); }
}
```

---

## 12. Virtual Threads — Project Loom (Java 21)

### What Are Virtual Threads?

Virtual threads are **lightweight, JVM-managed threads** introduced in Java 21 (JEP 444). They are not OS threads — instead, the JVM multiplexes many virtual threads onto a small number of **carrier (platform) threads**.

```
                    JVM Scheduler
                    /     |     \
    carrier-1   carrier-2  carrier-3  (= # CPU cores)
       |             |         |
  [VT1] [VT2]  [VT3] [VT4]  [VT5] [VT6] ...  (millions possible)
```

### The Magic: Unmounting on Blocking

When a virtual thread blocks on I/O (database call, HTTP request, `Thread.sleep()`):

1. The virtual thread is **unmounted** from its carrier thread
2. The carrier thread picks up another virtual thread
3. When the I/O completes, the virtual thread is **remounted** on any available carrier

This means a small number of carrier threads can serve a huge number of concurrent virtual threads — no more thread pool sizing calculations for I/O.

### Creating Virtual Threads

```java
// 1. Basic creation
Thread vThread = Thread.ofVirtual().start(() -> System.out.println("Hello from virtual thread"));

// 2. Named virtual threads
Thread named = Thread.ofVirtual()
    .name("order-processor-", 0)  // "order-processor-0", "order-processor-1", ...
    .start(() -> processOrder(orderId));

// 3. Virtual thread executor (each task gets its own virtual thread)
ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

// 4. Spring Boot 3.2+ — enable globally with one property
// spring.threads.virtual.enabled=true
```

### Spring Boot Configuration

```yaml
# application.yml — enables virtual threads for Tomcat + @Async + Spring Data
spring:
  threads:
    virtual:
      enabled: true
```

```java
// Or programmatically
@Bean
public TomcatProtocolHandlerCustomizer<?> virtualThreads() {
    return protocolHandler -> protocolHandler.setExecutor(
        Executors.newVirtualThreadPerTaskExecutor()
    );
}
```

### Virtual Thread Performance (Benchmarks)

| Configuration | Throughput (req/s) | Avg Latency | P99 Latency |
|---|---|---|---|
| Platform threads (200 pool) | 1,850 | 108ms | 420ms |
| Virtual threads | 4,200 | 47ms | 180ms |
| WebFlux (12 event loop threads) | 4,350 | 45ms | 175ms |

*Source: devops-monk.com production benchmarks, I/O-bound Spring Boot service*

**Key insight**: Virtual threads on MVC get **near-WebFlux throughput** with **zero reactive complexity**.

---

## 13. Virtual Threads vs Platform Threads vs WebFlux

### Decision Matrix

| Scenario | Recommendation |
|---|---|
| New Spring Boot service, I/O-bound | **Virtual Threads + MVC** |
| High-concurrency HTTP/DB calls | **Virtual Threads + MVC** |
| CPU-bound computation | **Platform threads** (small fixed pool) |
| Existing WebFlux codebase | **Keep WebFlux** (no migration needed) |
| Event-driven/streaming architecture | **WebFlux** or **Kafka** |
| Teams unfamiliar with reactive | **Virtual Threads** (simple blocking code) |
| Need maximum throughput, polyglot team | **WebFlux** or **Kotlin Coroutines** |

### Code Complexity Comparison

```java
// Virtual Threads: plain blocking code — easy to read and debug
@GetMapping("/user/{id}")
public UserProfile getUserProfile(@PathVariable Long id) {
    User user = userRepository.findById(id);          // blocks virtual thread, not carrier
    List<Order> orders = orderRepository.findByUser(id); // blocks virtual thread
    return UserProfile.of(user, orders);
}

// WebFlux: reactive — harder to read, but same throughput
@GetMapping("/user/{id}")
public Mono<UserProfile> getUserProfile(@PathVariable Long id) {
    return userRepository.findById(id)               // returns Mono<User>
        .zipWith(orderRepository.findByUser(id).collectList())
        .map(tuple -> UserProfile.of(tuple.getT1(), tuple.getT2()));
}
```

### Important Caveat

**Virtual threads do NOT help CPU-bound work.** A virtual thread doing computation occupies its carrier thread the entire time — no unmounting occurs. For CPU-bound tasks, use platform threads sized to CPU count.

---

## 14. Virtual Threads Production Pitfalls & Pinning

### Pinning — The Critical Problem (Java 21–23)

In Java 21–23, virtual threads are **pinned** (cannot unmount) when:
1. Inside a `synchronized` block or method
2. Inside a native frame (JNI/FFM calls)

**Pinning means**: the carrier thread is blocked along with the virtual thread, defeating the purpose.

```java
// In Java 21-23: if this blocks inside synchronized, carrier is pinned!
public synchronized String getData() {
    return database.query("SELECT...");  // ← blocks carrier thread if pinned
}
```

### JEP 491 — Pinning Fixed in Java 24 (Production-Ready in Java 25 LTS)

**Java 24 (JEP 491)** fundamentally redesigned the monitor implementation so that virtual threads can unmount even inside `synchronized` blocks. The `jdk.tracePinnedThreads` JVM flag was removed in Java 24 because the problem it monitored is effectively resolved.

| Java Version | synchronized pinning | Recommendation |
|---|---|---|
| 21 LTS | **Pins carrier** (major problem) | Replace `synchronized` with `ReentrantLock` in your code, update dependencies |
| 24 | **Fixed** (JEP 491) | No action needed for `synchronized` |
| 25 LTS | **Fixed** | Recommended production baseline |

```java
// Java 21: workaround — ReentrantLock doesn't pin
private final ReentrantLock lock = new ReentrantLock();

public String getData() {
    lock.lock();
    try {
        return database.query("SELECT...");
    } finally {
        lock.unlock();
    }
}

// Java 24+: synchronized works fine again
public synchronized String getData() {
    return database.query("SELECT..."); // no pinning
}
```

### Monitor Pinning in Java 21 — Real Production Incident

> Netflix reported a deadlock on Java 21 virtual threads: all carrier threads were pinned by `synchronized` blocks in `ConcurrentHashMap.computeIfAbsent()` calls inside heavily-loaded services. When all carriers pinned, no more virtual threads could run — complete service deadlock. Fix: update to libraries that use `ReentrantLock` internally (HikariCP 5.1+, Caffeine, Apache HttpClient 5.x). — JavaCodeGeeks, May 2026

### Pitfall: ThreadLocal as Cache with Virtual Threads

`ThreadLocal` values are created per-thread. With virtual threads (one per request), any `ThreadLocal` used as a **cache** loses its value after each request — hit rate collapses to 0%.

```java
// WRONG: Using ThreadLocal as a request-scoped cache with virtual threads
// Creates new cache for every virtual thread (every request) — no reuse!
private static final ThreadLocal<Map<String, Object>> cache = 
    ThreadLocal.withInitial(HashMap::new);

// CORRECT for virtual threads: ScopedValue for immutable request context
public static final ScopedValue<RequestContext> REQUEST = ScopedValue.newInstance();

// Or: store the cache centrally, not per-thread
private final Map<String, Object> sharedCache = new ConcurrentHashMap<>();
```

### Pitfall: Overwhelming Connection Pools

Virtual threads are cheap — you can create millions. But your **database connection pool** still has limits. 1 million virtual threads trying to use a 20-connection pool causes connection starvation.

```java
// Solution: Semaphore-based throttle
private final Semaphore dbSemaphore = new Semaphore(20); // match pool size

public Optional<Order> findOrder(String id) {
    dbSemaphore.acquireUninterruptibly();
    try {
        return orderRepository.findById(id);
    } finally {
        dbSemaphore.release();
    }
}
```

### Monitoring Virtual Threads

```java
// Detect remaining pinning cases with JFR (Java 21–23)
// JVM flag: -Djdk.tracePinnedThreads=full (removed in Java 24)

// JFR event for pinning (Java 21–24)
// jdk.VirtualThreadPinned

// JFR event for submission failures
// jdk.VirtualThreadSubmitFailed
```

---

## 15. Structured Concurrency — StructuredTaskScope (Java 21+)

### The Problem with `allOf` / Manual Fan-Out

`CompletableFuture.allOf()` has a lifecycle problem: if an exception occurs in one task, other tasks continue running in the background (orphaned tasks). If the parent operation is cancelled, child tasks don't know.

### Structured Concurrency: The Invariant

> When a scope exits, all subtasks it owns are either **done or cancelled**. No subtasks outlive the scope. No orphans. No leaks.

### Java 21–24: `StructuredTaskScope` Classes

```java
// ShutdownOnFailure: all must succeed, cancel all if any fails
public Invoice createInvoice(int orderId, int customerId, String lang)
        throws InterruptedException, ExecutionException {
    try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {
        
        Subtask<Order> orderTask = 
            scope.fork(() -> orderService.getOrder(orderId));
        Subtask<Customer> customerTask =
            scope.fork(() -> customerService.getCustomer(customerId));
        Subtask<InvoiceTemplate> templateTask =
            scope.fork(() -> templateService.getTemplate(lang));

        scope.join()           // wait for all subtasks
             .throwIfFailed(); // throw if any failed (and cancel others)

        return Invoice.generate(orderTask.get(), customerTask.get(), templateTask.get());
    }
}

// ShutdownOnSuccess: first success wins, cancel others
public String fastestResponse(String query) throws InterruptedException {
    try (var scope = new StructuredTaskScope.ShutdownOnSuccess<String>()) {
        scope.fork(() -> primaryService.search(query));
        scope.fork(() -> backupService.search(query));
        scope.fork(() -> cacheService.search(query));

        scope.join();
        return scope.result(); // result of first successful subtask
    }
}
```

### Java 25+: Refactored API with `Joiner`

```java
// JDK 25 refactored API — StructuredTaskScope.open() with Joiner
// Equivalent to ShutdownOnFailure
try (var scope = StructuredTaskScope.open(Joiner.awaitAllSuccessfulOrThrow())) {
    var task1 = scope.fork(() -> service1.call());
    var task2 = scope.fork(() -> service2.call());
    scope.join(); // throws StructuredTaskScope.FailedException if any fails
    return combine(task1.get(), task2.get());
}

// Equivalent to ShutdownOnSuccess
try (var scope = StructuredTaskScope.open(Joiner.anySuccessfulResultOrThrow())) {
    scope.fork(() -> primary.call());
    scope.fork(() -> backup.call());
    return scope.join(); // returns result of first successful subtask
}
```

### With Timeout

```java
try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {
    var kycTask = scope.fork(() -> kycService.check(customer));
    var fraudTask = scope.fork(() -> fraudService.check(customer));
    var creditTask = scope.fork(() -> creditService.check(customer));

    scope.joinUntil(Instant.now().plusSeconds(5))  // global timeout
         .throwIfFailed();

    return new Verification(kycTask.get(), fraudTask.get(), creditTask.get());
}
```

### StructuredTaskScope vs CompletableFuture

| Aspect | `StructuredTaskScope` | `CompletableFuture` |
|---|---|---|
| Blocking | **Yes** — owner thread blocks at `join()` | No — callback-based, non-blocking |
| Child lifetime | Guaranteed: children can't outlive scope | No guarantee — orphaned tasks possible |
| Cancellation propagation | Automatic | Manual (`cancel(true)` + checking) |
| Error propagation | Clean: `throwIfFailed()` | Wrapped in `CompletionException` |
| Context (MDC, ScopedValues) | Auto-inherited by subtasks | Manual propagation required |
| Use case | Request-scoped parallel I/O, fan-out | Non-blocking pipelines, event-driven |
| API status | Preview → Finalized in JDK 25 | Stable since Java 8 |

---

## 16. Java 21 Features — Records

### What Are Records?

Records are **immutable data carrier classes** with a concise declaration. A record automatically generates: canonical constructor, accessor methods (not `getX()` — just `x()`), `equals()`, `hashCode()`, and `toString()`.

```java
// Before Java 16 — 30+ lines of boilerplate
public class Point {
    private final int x;
    private final int y;
    public Point(int x, int y) { this.x = x; this.y = y; }
    public int getX() { return x; }
    public int getY() { return y; }
    @Override public boolean equals(Object o) { ... }
    @Override public int hashCode() { ... }
    @Override public String toString() { ... }
}

// With Records — 1 line
public record Point(int x, int y) {}

Point p = new Point(3, 4);
p.x();    // accessor — NOT p.getX()
p.y();
```

### Record Restrictions (Critical for Interviews)

1. Records are **implicitly final** — cannot be extended
2. Records **cannot extend** any class (implicitly extend `java.lang.Record`)
3. Records **can implement** interfaces
4. No extra **instance fields** allowed beyond the record components
5. Record components are **implicitly final** — immutable

```java
// Implementing interfaces
public interface Describable { String describe(); }

public record Product(String sku, double price) implements Describable {
    // Custom compact constructor for validation
    public Product {
        if (price < 0) throw new IllegalArgumentException("Price cannot be negative");
        sku = sku.strip(); // can transform components in compact constructor
    }

    @Override
    public String describe() {
        return "Product[%s] at $%.2f".formatted(sku, price);
    }

    // Static factory methods — fine
    public static Product free(String sku) { return new Product(sku, 0); }
}
```

### Three Constructor Styles

```java
public record Range(int min, int max) {

    // 1. Compact constructor — validates, no explicit assignments needed
    public Range {
        if (min > max) throw new IllegalArgumentException("min > max");
        // Compiler generates: this.min = min; this.max = max;
    }

    // 2. Custom canonical constructor (explicit assignments required)
    public Range(int min, int max) {
        if (min > max) throw new IllegalArgumentException();
        this.min = min;
        this.max = max;
    }

    // 3. Custom alternative constructor (delegates to canonical)
    public Range(int value) {
        this(value, value); // point range
    }
}
```

### Records in Production

```java
// DTOs / Value Objects
public record OrderRequest(String customerId, List<LineItem> items, String promoCode) {}
public record LineItem(String productId, int quantity, double unitPrice) {}

// Event payloads
public record OrderCreatedEvent(String orderId, Instant createdAt, double totalAmount) {}

// API responses
public record ApiResponse<T>(boolean success, T data, String errorMessage) {
    public static <T> ApiResponse<T> success(T data) {
        return new ApiResponse<>(true, data, null);
    }
    public static <T> ApiResponse<T> error(String message) {
        return new ApiResponse<>(false, null, message);
    }
}

// Tuples / Pairs for stream results
public record MinMax(int min, int max) {}
```

---

## 17. Java 21 Features — Sealed Classes

### What Are Sealed Classes?

Sealed classes restrict which classes can extend them, using the `permits` clause. The compiler knows the **complete set of subtypes** at compile time.

```java
// Sealed hierarchy for payment types
public sealed interface PaymentMethod
    permits CreditCard, BankTransfer, Crypto {}

public record CreditCard(String number, String holder, int expiryYear) implements PaymentMethod {}
public record BankTransfer(String iban, String bicCode) implements PaymentMethod {}
public final class Crypto implements PaymentMethod {
    private final String walletAddress;
    private final String currency;
    // ...
}
```

### Subtype Modifiers

Every permitted subclass must declare exactly one of:

| Modifier | Meaning |
|---|---|
| `final` | Cannot be extended further |
| `sealed` | Can only be extended by its own `permits` list |
| `non-sealed` | Can be extended by anyone (reopens the hierarchy) |

```java
public sealed class Shape permits Circle, Polygon {}

public final class Circle extends Shape {}   // closed — no further extension

public sealed class Polygon extends Shape permits Triangle, Rectangle {} // further sealed

public non-sealed class CustomShape extends Shape {} // open — anyone can extend
```

### Sealed Classes + Pattern Matching = Exhaustiveness

The killer feature: when you switch over a sealed type, the compiler **verifies exhaustiveness** — no `default` needed.

```java
public double calculateFee(PaymentMethod method) {
    return switch (method) {
        case CreditCard cc  -> cc.amount() * 0.029 + 0.30;
        case BankTransfer bt -> 0.25;
        case Crypto c       -> c.amount() * 0.01;
        // NO default needed — compiler ensures all cases are covered
    };
}
// If you add a new type to PaymentMethod, every exhaustive switch fails to compile
// — guaranteeing you handle all cases
```

### Result / Either Pattern

```java
public sealed interface Result<T> permits Result.Success, Result.Failure {
    record Success<T>(T value) implements Result<T> {}
    record Failure<T>(String message, Throwable cause) implements Result<T> {}

    static <T> Result<T> success(T value) { return new Success<>(value); }
    static <T> Result<T> failure(String msg, Throwable cause) { return new Failure<>(msg, cause); }

    default T getOrThrow() {
        return switch (this) {
            case Success<T>(var v) -> v;
            case Failure<T>(var msg, var cause) -> throw new RuntimeException(msg, cause);
        };
    }
}
```

---

## 18. Java 21 Features — Pattern Matching for Switch

### Evolution of Pattern Matching

| Java Version | Feature |
|---|---|
| 14 | `instanceof` pattern matching (preview) |
| 16 | `instanceof` pattern matching (final) |
| 17 | Pattern matching for `switch` (preview) |
| 21 | Pattern matching for `switch` (final — JEP 441) |
| 21 | Record Patterns (final — JEP 440) |

### Type Patterns in Switch

```java
// Old: verbose instanceof chain
String format(Object obj) {
    if (obj instanceof Integer i) return "Integer: " + i;
    else if (obj instanceof String s) return "String: " + s;
    else if (obj instanceof Double d) return "Double: " + d;
    else return "Unknown: " + obj;
}

// Java 21: concise and exhaustive
String format(Object obj) {
    return switch (obj) {
        case Integer i -> "Integer: " + i;
        case String s  -> "String: " + s;
        case Double d  -> "Double: " + d;
        case null      -> "null";           // null handling built-in
        default        -> "Unknown: " + obj;
    };
}
```

### Guard Clauses (`when`)

```java
String classify(Number n) {
    return switch (n) {
        case Integer i when i < 0   -> "Negative integer";
        case Integer i when i == 0  -> "Zero";
        case Integer i              -> "Positive integer: " + i;
        case Double d when d.isNaN()-> "NaN double";
        case Double d               -> "Double: " + d;
        default                     -> "Other number";
    };
}
```

### Record Patterns

Record patterns destructure records directly inside switch/instanceof:

```java
record Point(int x, int y) {}
record Line(Point start, Point end) {}

// Nested record patterns
String describe(Object shape) {
    return switch (shape) {
        case Point(var x, var y) when x == y -> "Diagonal point at " + x;
        case Point(var x, var y)             -> "Point at (" + x + "," + y + ")";
        case Line(Point(var x1, var y1), Point(var x2, var y2)) ->
            "Line from (%d,%d) to (%d,%d)".formatted(x1, y1, x2, y2);
        default -> "Unknown shape";
    };
}
```

### Data-Oriented Programming Pattern

Records + Sealed Classes + Pattern Matching enables functional-style data processing:

```java
sealed interface Shape permits Circle, Rectangle, Triangle {}
record Circle(double radius) implements Shape {}
record Rectangle(double width, double height) implements Shape {}
record Triangle(double base, double height) implements Shape {}

// All branches verified at compile time
double area(Shape shape) {
    return switch (shape) {
        case Circle(var r)         -> Math.PI * r * r;
        case Rectangle(var w, var h) -> w * h;
        case Triangle(var b, var h)  -> 0.5 * b * h;
        // No default — exhaustiveness enforced by sealed hierarchy
    };
}
```

---

## 19. Java 21 Features — Sequenced Collections, Text Blocks & More

### Sequenced Collections (JEP 431)

Java 21 added three new interfaces to unify first/last element access across `List`, `Deque`, and `LinkedHashSet` — previously each had different APIs.

```java
interface SequencedCollection<E> extends Collection<E> {
    E getFirst();              // formerly: list.get(0), deque.peek(), ...
    E getLast();               // formerly: list.get(size-1), deque.peekLast(), ...
    void addFirst(E);
    void addLast(E);
    E removeFirst();
    E removeLast();
    SequencedCollection<E> reversed();
}
```

```java
List<String> list = new ArrayList<>(List.of("a", "b", "c"));
list.getFirst();   // "a" — was: list.get(0)
list.getLast();    // "c" — was: list.get(list.size() - 1)
list.reversed();   // ["c", "b", "a"] — a view, not a copy
list.reversed().removeFirst(); // removes "c" from original!

// LinkedHashSet now has getFirst/getLast too
LinkedHashSet<Integer> set = new LinkedHashSet<>(Set.of(1, 2, 3));
// Note: Set.of doesn't guarantee order — use explicit insertion order
LinkedHashSet<Integer> orderedSet = new LinkedHashSet<>();
orderedSet.addAll(List.of(1, 2, 3));
orderedSet.getFirst();  // 1
```

### Text Blocks (JEP 378, finalized Java 15)

```java
// Old: string concatenation nightmare
String json = "{\n" +
              "  \"name\": \"John\",\n" +
              "  \"age\": 30\n" +
              "}";

// Text block — preserves formatting
String json = """
        {
          "name": "John",
          "age": 30
        }
        """;

// SQL query — super readable
String sql = """
        SELECT u.id, u.name, o.total
        FROM users u
        JOIN orders o ON u.id = o.user_id
        WHERE u.active = true
          AND o.created_at > :since
        ORDER BY o.created_at DESC
        """;

// HTML template
String html = """
        <html>
            <body>
                <h1>%s</h1>
                <p>%s</p>
            </body>
        </html>
        """.formatted(title, body);
```

**Indentation**: text blocks strip common leading whitespace based on the position of the closing `"""`.

### Pattern Matching for `instanceof` (JEP 394, Java 16)

```java
// Old
if (obj instanceof String) {
    String s = (String) obj;
    return s.toUpperCase();
}

// Java 16+ pattern matching
if (obj instanceof String s) {
    return s.toUpperCase(); // s is in scope here — no explicit cast
}

// With guard
if (obj instanceof String s && s.length() > 5) {
    return s.substring(0, 5);
}
```

### `var` (Java 10) — Local Variable Type Inference

```java
// Before
Map<String, List<Integer>> groupedNumbers = new HashMap<>();
List<Map.Entry<String, Integer>> entries = new ArrayList<>();

// With var — type still enforced at compile time
var groupedNumbers = new HashMap<String, List<Integer>>();
var entries = new ArrayList<Map.Entry<String, Integer>>();

// Works in for loops
for (var entry : map.entrySet()) { ... }

// Works with lambdas (needed for annotations on params)
Consumer<String> printer = (@NotNull var s) -> System.out.println(s);
```

**`var` limitations**: cannot be used for fields, method parameters, return types, or catches (except with `catch (var e)` — but this is rarely used).

---

## 20. Java Streams — Pipeline Internals & Laziness

### How a Stream Pipeline Works

A stream pipeline consists of:
1. **Source** — array, collection, generator
2. **Intermediate operations** — lazy, return another stream
3. **Terminal operation** — triggers execution, returns a result

```
Source → filter → map → flatMap → distinct → sorted → limit → terminal
                         ↑
                   intermediate (lazy, no execution yet)
```

### Lazy Evaluation

**Nothing executes until a terminal operation is called.**

```java
Stream<String> stream = List.of("a", "b", "c").stream()
    .filter(s -> { System.out.println("filtering " + s); return true; })
    .map(s -> { System.out.println("mapping " + s); return s.toUpperCase(); });

// No output yet — no execution has happened
System.out.println("--- before terminal ---");
List<String> result = stream.toList(); // ← execution happens here
// Output:
// --- before terminal ---
// filtering a
// mapping a
// filtering b
// mapping b
// filtering c
// mapping c
```

Notice: elements are processed **one at a time through the entire pipeline**, not one stage at a time. This is **pipeline fusion** — the JVM composes all intermediate operations into one pass.

### Short-Circuiting Operations

Some operations **stop early** once the answer is known:

| Operation | Short-circuits? | Stops When |
|---|---|---|
| `findFirst()` | Yes | First element found |
| `findAny()` | Yes | Any element found (parallel-friendly) |
| `anyMatch(pred)` | Yes | First element matches |
| `allMatch(pred)` | Yes | First element doesn't match |
| `noneMatch(pred)` | Yes | First element matches |
| `limit(n)` | Yes | n elements processed |
| `count()` | No | All elements processed |
| `collect()` | No | All elements processed |
| `forEach()` | No | All elements processed |

```java
// Only processes elements until "D" is found
boolean hasD = List.of("A", "B", "C", "D", "E", "F").stream()
    .peek(s -> System.out.println("checking " + s))
    .anyMatch("D"::equals);
// Output: checking A, checking B, checking C, checking D — stops!
```

### The `sorted()` Trap

`sorted()` is an **eager intermediate operation** — it must materialize and sort the entire upstream before it can emit a single element. This breaks laziness and kills short-circuiting:

```java
// WRONG: sorted materializes ALL elements, then findFirst picks one
Optional<Integer> first = numbers.stream()
    .sorted()
    .findFirst(); // O(n log n) + O(n) memory

// CORRECT: min is O(n) with no materialization
Optional<Integer> first = numbers.stream()
    .min(Comparator.naturalOrder()); // O(n), no intermediate collection
```

---

## 21. Stream Operations Deep Dive

### `map` vs `flatMap` vs `mapMulti`

```java
List<String> words = List.of("hello world", "foo bar");

// map: one-to-one transformation
List<String> upper = words.stream()
    .map(String::toUpperCase)
    .toList(); // ["HELLO WORLD", "FOO BAR"]

// flatMap: one-to-many (flatten nested streams)
List<String> tokens = words.stream()
    .flatMap(s -> Arrays.stream(s.split(" ")))
    .toList(); // ["hello", "world", "foo", "bar"]

// mapMulti (Java 16+): imperative consumer-based, no intermediate streams
List<String> tokens2 = words.stream()
    .<String>mapMulti((sentence, consumer) -> {
        for (String word : sentence.split(" ")) {
            consumer.accept(word);
        }
    })
    .toList(); // Same result, potentially faster for small outputs
```

**When to use `mapMulti`**: when the expansion is conditional, small, or you'd need to create many small streams. Use `flatMap` for readability when the mapping naturally returns a `Stream`.

### `reduce` — When and How

```java
// Simple reduce: sum
int sum = IntStream.range(1, 11).reduce(0, Integer::sum);

// Identity matters for parallel correctness — must be true identity
// For sum: identity = 0 (x + 0 = x for all x)
// For product: identity = 1 (x * 1 = x for all x)
// For string concat: DON'T use reduce — O(n²) due to String immutability

// 3-arg reduce for heterogeneous type (parallel-friendly)
int totalLength = Stream.of("hello", "world", "java")
    .reduce(
        0,                             // identity
        (sum2, s) -> sum2 + s.length(), // accumulator
        Integer::sum                   // combiner (for parallel)
    );
```

**Rule**: Use `reduce` only for same-type, simple accumulations (sum, max, min). For complex aggregation into collections, use `collect`.

### `distinct`, `limit`, `skip`

```java
// distinct is O(n) but stateful — tracks all seen elements
List<Integer> unique = Stream.of(1, 2, 2, 3, 3, 3).distinct().toList(); // [1, 2, 3]

// limit + sorted = anti-pattern (sorted is eager)
// For top-N, use limit BEFORE sorted when possible, or use a PriorityQueue
List<Integer> top3Wrong = numbers.stream()
    .sorted(Comparator.reverseOrder())  // ← sorts ALL elements first
    .limit(3)
    .toList();

// For truly large lists, use a min-heap approach
```

---

## 22. Collectors — from groupingBy to Custom

### Essential Collectors

```java
// toList, toSet, toUnmodifiableList
List<String> list = stream.collect(Collectors.toList());       // mutable
List<String> immutable = stream.collect(Collectors.toUnmodifiableList()); // immutable
List<String> modern = stream.toList();  // Java 16+ — shorthand, immutable

// joining
String csv = names.stream().collect(Collectors.joining(", ", "[", "]"));
// "[Alice, Bob, Charlie]"

// counting
long count = orders.stream().collect(Collectors.counting());

// summingInt / averagingInt
double avgPrice = orders.stream()
    .collect(Collectors.averagingDouble(Order::totalAmount));

// summarizingInt — min, max, avg, sum, count in one pass
IntSummaryStatistics stats = orders.stream()
    .collect(Collectors.summarizingInt(Order::quantity));
```

### `groupingBy`

```java
// Basic grouping
Map<String, List<Order>> byCustomer = orders.stream()
    .collect(Collectors.groupingBy(Order::customerId));

// With downstream collector
Map<String, Long> orderCountByCustomer = orders.stream()
    .collect(Collectors.groupingBy(Order::customerId, Collectors.counting()));

Map<String, Double> revenueByCustomer = orders.stream()
    .collect(Collectors.groupingBy(Order::customerId,
        Collectors.summingDouble(Order::totalAmount)));

// Custom map factory + downstream
Map<String, List<String>> orderIdsByCustomer = orders.stream()
    .collect(Collectors.groupingBy(
        Order::customerId,
        TreeMap::new,   // sorted map
        Collectors.mapping(Order::id, Collectors.toList())
    ));

// Multi-level grouping
Map<String, Map<String, List<Order>>> groupedByCustomerAndStatus = orders.stream()
    .collect(Collectors.groupingBy(Order::customerId,
        Collectors.groupingBy(Order::status)));
```

### `partitioningBy`

```java
// Split into two groups: true (passes predicate) and false
Map<Boolean, List<Order>> partitioned = orders.stream()
    .collect(Collectors.partitioningBy(order -> order.totalAmount() > 1000));

List<Order> highValue = partitioned.get(true);
List<Order> regular   = partitioned.get(false);

// With downstream
Map<Boolean, Long> counts = orders.stream()
    .collect(Collectors.partitioningBy(
        order -> order.status().equals("SHIPPED"),
        Collectors.counting()
    ));
```

### `teeing` (Java 12+)

`teeing` runs two collectors in parallel on the same stream, then merges their results:

```java
// Min and max in one pass
record MinMax(Optional<Integer> min, Optional<Integer> max) {}

MinMax result = numbers.stream()
    .collect(Collectors.teeing(
        Collectors.minBy(Comparator.naturalOrder()),
        Collectors.maxBy(Comparator.naturalOrder()),
        MinMax::new
    ));

// More complex: count and sum together
record Stats(long count, double sum) {}
Stats stats = orders.stream()
    .collect(Collectors.teeing(
        Collectors.counting(),
        Collectors.summingDouble(Order::totalAmount),
        Stats::new
    ));
```

### Custom Collector

```java
// A collector that builds a running-total list
public class RunningTotalCollector implements Collector<Double, List<Double>, List<Double>> {

    @Override
    public Supplier<List<Double>> supplier() {
        return ArrayList::new;
    }

    @Override
    public BiConsumer<List<Double>, Double> accumulator() {
        return (list, value) -> {
            double running = list.isEmpty() ? value : list.get(list.size() - 1) + value;
            list.add(running);
        };
    }

    @Override
    public BinaryOperator<List<Double>> combiner() {
        return (left, right) -> { throw new UnsupportedOperationException("Not for parallel"); };
    }

    @Override
    public Function<List<Double>, List<Double>> finisher() {
        return Function.identity();
    }

    @Override
    public Set<Characteristics> characteristics() {
        return Set.of(); // no CONCURRENT, no UNORDERED, no IDENTITY_FINISH
    }
}

// Usage
List<Double> runningTotal = Stream.of(10.0, 20.0, 30.0)
    .collect(new RunningTotalCollector());
// [10.0, 30.0, 60.0]

// Or use Collector.of (shorthand)
Collector<Double, ?, List<Double>> collector = Collector.of(
    ArrayList::new,
    (list, value) -> {
        double running = list.isEmpty() ? value : list.get(list.size() - 1) + value;
        list.add(running);
    },
    (left, right) -> { throw new UnsupportedOperationException(); },
    Function.identity()
);
```

---

## 23. Parallel Streams — When and When NOT to Use

### How Parallel Streams Work

`parallelStream()` splits the stream source using `Spliterator`, submits chunks to `ForkJoinPool.commonPool()`, processes them in parallel, and combines results.

```java
// Sequential (default)
long sum = numbers.stream().mapToLong(Long::valueOf).sum();

// Parallel (uses ForkJoinPool.commonPool())
long sum = numbers.parallelStream().mapToLong(Long::valueOf).sum();

// Parallel with custom pool (isolate from commonPool)
ForkJoinPool myPool = new ForkJoinPool(4);
long sum = myPool.submit(() -> numbers.parallelStream().mapToLong(Long::valueOf).sum()).get();
```

### The Decision Formula

**Only use parallel streams when N × Q > 10,000**, where:
- N = number of elements
- Q = cost per element operation (higher = more benefit)

### When Parallel Streams Help

- **Large datasets** (N > 10,000 elements)
- **CPU-bound** (no I/O, no locking)
- **Stateless operations** (each element independent)
- **Splittable sources**: `ArrayList`, `array`, `IntStream.range`

### When Parallel Streams HURT

| Anti-Pattern | Reason | Fix |
|---|---|---|
| I/O inside parallel stream | Blocks ForkJoinPool threads | Use virtual threads or CompletableFuture |
| Small collections (N < 1000) | Split/merge overhead > compute | Use sequential |
| `LinkedList` source | Poor splitting (O(n) to split) | Convert to ArrayList first |
| Shared mutable state in lambdas | Race conditions | Use thread-safe collectors |
| `sorted()` before `limit()` | All elements sorted even if not needed | Restructure |
| `forEachOrdered()` | Defeats parallelism by serializing output | Use `forEach()` or `collect()` |
| HTTP calls / database queries | Thread starvation in commonPool | Use `CompletableFuture` |

### Common Pitfall: Side Effects in Parallel Streams

```java
// WRONG: race condition — unsynchronized mutation
List<String> results = new ArrayList<>();
names.parallelStream()
    .filter(s -> s.length() > 3)
    .forEach(results::add); // concurrent ArrayList modification = disaster

// CORRECT: use collect — thread-safe by design
List<String> results = names.parallelStream()
    .filter(s -> s.length() > 3)
    .collect(Collectors.toList()); // thread-safe

// Or with custom concurrent container
List<String> results = names.parallelStream()
    .filter(s -> s.length() > 3)
    .collect(Collectors.toUnmodifiableList());
```

---

## 24. Stream Performance Traps & Best Practices

### The Boxing Penalty — 15x Slower

`Stream<Integer>` boxes/unboxes every element. `IntStream` avoids boxing entirely.

```java
// SLOW: boxing every integer
int sum = numbers.stream()                      // Stream<Integer>
    .mapToInt(Integer::intValue)
    .sum();

// FAST: no boxing — use primitive streams directly
int sum = numbers.stream()
    .mapToInt(Integer::intValue)                 // convert to IntStream
    .sum();

// FASTEST: start as IntStream
int sum = IntStream.range(0, 1_000_000).sum();

// Rule: for numeric work, always use IntStream / LongStream / DoubleStream
```

### Filter Early

Place the most restrictive filter first to reduce elements flowing through the rest of the pipeline:

```java
// LESS EFFICIENT: expensive operation first
orders.stream()
    .map(Order::enrichFromDatabase)     // expensive — runs on all orders
    .filter(o -> o.totalAmount() > 1000) // filters after expensive op
    .toList();

// MORE EFFICIENT: filter first
orders.stream()
    .filter(o -> o.totalAmount() > 1000)  // cheap filter first
    .map(Order::enrichFromDatabase)       // expensive op on fewer elements
    .toList();
```

### Never Use `peek()` for Logic

`peek()` is for debugging only. It may be skipped or reordered by optimizations:

```java
// WRONG: using peek for side effects
users.stream()
    .peek(u -> u.setActive(true))       // mutation in peek — unreliable
    .filter(User::isEligible)
    .toList();

// CORRECT: transform with map, not peek
users.stream()
    .map(u -> u.withActive(true))       // immutable transformation
    .filter(User::isEligible)
    .toList();
```

### Avoid `sorted()` Before Short-Circuiting

```java
// WRONG: O(n log n) + O(n) memory for a result that needs 1 element
Optional<String> first = names.stream()
    .sorted()
    .findFirst();

// CORRECT: O(n) with no materialization
Optional<String> first = names.stream()
    .min(Comparator.naturalOrder());
```

### Stream Reuse Problem

A stream can only be consumed **once**. Reuse throws `IllegalStateException`:

```java
Stream<String> stream = names.stream().filter(s -> s.length() > 3);
List<String> list1 = stream.toList();
List<String> list2 = stream.toList(); // THROWS IllegalStateException

// Fix: use Supplier to create fresh streams
Supplier<Stream<String>> streamSupplier = () -> names.stream().filter(s -> s.length() > 3);
List<String> list1 = streamSupplier.get().toList();
List<String> list2 = streamSupplier.get().toList();
```

### Production Checklist for Streams

- [ ] Use primitive streams (`IntStream`, `LongStream`) for numeric work
- [ ] Filter before expensive operations
- [ ] Don't use `sorted()` before `findFirst()`/`limit()` — use `min()`/`max()`
- [ ] Never mutate shared state in lambdas (thread-safety, even sequential)
- [ ] Use `peek()` only for debugging — remove before production
- [ ] Don't reuse streams — they are consumed once
- [ ] Parallel streams only for CPU-bound, large, stateless, splittable work
- [ ] Never do I/O inside parallel streams
- [ ] Use `toList()` (Java 16+) instead of `collect(Collectors.toList())`

---

## 25. Lead Interview Questions & Answers

### CompletableFuture (5 questions)

**Q1: What is the difference between `Future.get()` and CompletableFuture callbacks? Why does it matter in production?**

**A**: `Future.get()` blocks the calling thread until the result is available. In a thread pool, this means threads are blocked doing nothing — wasting OS resources. Under load, all threads block and the service starves. `CompletableFuture` callbacks (`thenApply`, `thenCompose`, etc.) are non-blocking: they register what to do when the result arrives, freeing the current thread immediately. A single thread can register thousands of callbacks without blocking. In production, this difference means the difference between 200 concurrent requests (limited by thread pool) and millions (limited by memory and callbacks). *(Section 1)*

**Q2: When do you use `thenApply` vs `thenCompose`? Give a real example where mixing them causes a bug.**

**A**: `thenApply(f)` is like `map` — use when your function returns a plain value. `thenCompose(f)` is like `flatMap` — use when your function returns another `CompletableFuture`. Mixing them creates `CompletableFuture<CompletableFuture<T>>` — a nested future you'd have to `.join()` twice to read:

```java
// BUG: returns CompletableFuture<CompletableFuture<Payment>>
findOrder(id).thenApply(order -> processPayment(order)); // processPayment returns CF<Payment>
// Must call .get().get() — blocking, ugly

// CORRECT: returns CompletableFuture<Payment>
findOrder(id).thenCompose(order -> processPayment(order));
```
The rule: if your lambda's return type is `CompletableFuture<X>`, use `thenCompose`, not `thenApply`. *(Section 3)*

**Q3: Your production service has random silent failures in async flows — no exceptions logged, no errors returned. How do you debug it?**

**A**: Classic `CompletableFuture` pitfall — exceptions are silently discarded if nobody observes the future. Steps: (1) Search codebase for `CompletableFuture.runAsync` or `supplyAsync` without `.exceptionally()` or `.whenComplete()` — these are the culprits. (2) Add `.whenComplete((v, ex) -> { if (ex != null) log.error("...", ex); })` to all fire-and-forget futures. (3) For pipelines, ensure every chain ends with `.exceptionally()` or `.handle()`. (4) Register an uncaught exception handler: `Thread.setDefaultUncaughtExceptionHandler(...)`. (5) Consider wrapping your executor to log unhandled exceptions. The rule: every `CompletableFuture` chain must have an error observer. *(Section 5)*

**Q4: You're seeing thread pool exhaustion in production. The stack trace shows threads blocked inside `CompletableFuture` callbacks. What's wrong?**

**A**: `.get()` is being called inside a callback — this is the "blocking inside async" anti-pattern. It turns async code into synchronous code and blocks the worker thread:
```java
.thenApply(v1 -> service2.call().get()) // blocks the callback thread
```
The fix: use `thenCompose` to chain the second async call instead of blocking for it. Also: check if ForkJoinPool.commonPool is being used for I/O — it should never be. Provide a dedicated executor with appropriate sizing. For Java 21+, consider switching to virtual threads where blocking is cheap. *(Sections 6, 10)*

**Q5: How do you propagate MDC (correlation ID) across CompletableFuture chains in Spring Boot?**

**A**: MDC is ThreadLocal — it doesn't cross thread boundaries automatically. Three approaches: (1) Capture MDC snapshot before submitting, restore it inside the lambda — error-prone and boilerplate-heavy. (2) Spring Boot: use `ContextPropagatingTaskDecorator` on the executor — automatically propagates MDC, SecurityContext, request attributes. (3) Micrometer Context Propagation library: wrap your `ExecutorService` with `ContextExecutorService.wrap(...)` — propagates all registered context holders. (4) Java 21+: use `ScopedValue` instead of `ThreadLocal` for new code — auto-propagated to child virtual threads in a scope. I always use option 2 or 3 in production — zero boilerplate, works for all async boundaries. *(Section 8)*

---

### Virtual Threads (4 questions)

**Q6: How do virtual threads work internally? Why are they better than platform threads for I/O-bound work?**

**A**: Virtual threads are JVM-managed lightweight threads. Internally, they mount onto **carrier threads** (a small pool of platform threads, typically sized to CPU count). When a virtual thread performs a blocking I/O operation (e.g., database query), the JVM **unmounts** it from its carrier — the carrier is freed immediately to run another virtual thread. When the I/O completes, the virtual thread is **remounted** on any available carrier. This means millions of virtual threads can be in-flight simultaneously with only N carrier threads (N = CPU cores), where each carrier is never idle waiting for I/O. Platform threads: each blocks an OS thread during I/O — expensive (1MB stack), context-switch cost, OS limits (~thousands). Virtual threads: each I/O suspension frees the carrier — cheap (~1KB stack), no OS context switch, millions possible. *(Section 12)*

**Q7: What was the pinning problem with Java 21 virtual threads and how was it resolved?**

**A**: In Java 21–23, when a virtual thread executed code inside a `synchronized` block and blocked on I/O, the JVM could NOT unmount it — the carrier thread was pinned (blocked) along with the virtual thread. With all carriers pinned, no other virtual threads could run — the system deadlocked. Netflix documented this exact failure in production: `synchronized` + blocking code inside HikariCP and ConcurrentHashMap caused all carrier threads to pin, crashing their service. JEP 491 (Java 24) fundamentally redesigned the monitor implementation so virtual threads can unmount even inside `synchronized`. On Java 24+ (and Java 25 LTS), `synchronized` no longer pins virtual threads — the problem is solved. Teams on Java 21 needed workarounds: use `ReentrantLock` instead of `synchronized` in their own code, and update to library versions that adopted `ReentrantLock` (HikariCP 5.1+, Caffeine 3.x+). *(Section 14)*

**Q8: When would you NOT use virtual threads?**

**A**: Three scenarios: (1) **CPU-bound work**: a virtual thread doing computation doesn't unmount — it occupies a carrier the entire time. For CPU-bound tasks, use a platform thread pool sized to CPU count. Virtual threads add overhead without benefit. (2) **ThreadLocal as a cache**: virtual threads are one-per-task (like one per request) — any `ThreadLocal` used as a cache creates a new instance per request, losing cache hits entirely. Use a shared concurrent cache instead. (3) **Overwhelming finite resources**: virtual threads are cheap but your database connection pool is still limited. 10,000 virtual threads hammering a 20-connection pool causes connection starvation. Use a `Semaphore` to throttle access to the pool. *(Section 13, 14)*

**Q9: Virtual Threads vs WebFlux — when do you pick each?**

**A**: Virtual Threads win when: starting a new service today, team knows blocking/imperative Java, existing codebase uses JPA/JDBC/RestTemplate, debugging matters (normal stack traces). WebFlux wins when: already invested in reactive, need event-driven/streaming (SSE, WebSocket), building on top of Netty directly, need true non-blocking all the way down, team knows reactive programming well. Benchmarks show virtual threads reach near-WebFlux throughput for I/O-bound workloads — the throughput gap that justified WebFlux's complexity is now largely gone. For new services in 2026, `spring.threads.virtual.enabled=true` with MVC is the pragmatic default. *(Section 13)*

---

### Java 21 Features (4 questions)

**Q10: What makes records "not just data classes"? What restrictions apply?**

**A**: Records are more than syntactic sugar for Java beans. They encode the **semantic contract that the primary form of this class is its data** — all equals/hashCode/toString are based on all components, always. Key restrictions: (1) Implicitly `final` — cannot be extended. (2) Cannot extend any class (implicitly extend `java.lang.Record`). (3) No extra instance fields beyond the declared components. (4) Components are implicitly `final`. The compact constructor can validate and normalize data but cannot assign components — the compiler emits the assignments after your block. Use cases: DTOs, value objects, event payloads, type-safe tuples, API responses. Don't use records for: mutable entities, JPA entities, classes needing inheritance. *(Section 16)*

**Q11: How do sealed classes + pattern matching for switch enable the compiler to enforce exhaustiveness?**

**A**: Sealed classes declare their complete subtype hierarchy at compile time. When you `switch` over a sealed type using pattern matching, the compiler knows all possible types. If your switch covers all permitted subtypes, no `default` is needed — and if you add a new subtype later, all existing exhaustive switches fail to compile, forcing you to handle the new case. Example:
```java
sealed interface Shape permits Circle, Rectangle {}
// This switch compiles without default:
double area(Shape s) { return switch (s) {
    case Circle c -> Math.PI * c.radius() * c.radius();
    case Rectangle r -> r.width() * r.height();
}; }
```
Add `Triangle` to Shape without updating this method → compile error. This is **algebraic data type safety** — the type system catches missing cases at compile time, not at runtime. *(Section 17, 18)*

**Q12: Explain record patterns and how they simplify data extraction.**

**A**: Record patterns extend pattern matching to destructure record instances directly inside `instanceof` or `switch` without calling accessor methods:

```java
// Old: multiple accessor calls
if (shape instanceof Circle c) {
    double radius = c.radius(); // explicit accessor
    return Math.PI * radius * radius;
}

// With record patterns: destructure inline
if (shape instanceof Circle(double radius)) {
    return Math.PI * radius * radius;
}

// In switch with nested records
switch (shape) {
    case Line(Point(var x1, var y1), Point(var x2, var y2)) ->
        Math.sqrt(Math.pow(x2-x1, 2) + Math.pow(y2-y1, 2));
}
```
Record patterns compose naturally with `when` guards and sealed class exhaustiveness. They're the foundation of data-oriented programming in Java — inspect shape and extract data in one operation. *(Section 18)*

**Q13: What is `SequencedCollection` and what problem does it solve?**

**A**: Before Java 21, Java had no consistent API for accessing the first/last elements of ordered collections:
- `List`: `list.get(0)` / `list.get(size-1)`
- `Deque`: `deque.peekFirst()` / `deque.peekLast()`
- `LinkedHashSet`: no direct first/last access at all

`SequencedCollection` (JEP 431) adds uniform methods `getFirst()`, `getLast()`, `addFirst()`, `addLast()`, `removeFirst()`, `removeLast()`, and `reversed()` to all ordered collections. `reversed()` returns a **live view** (not a copy) — mutations on the reversed view reflect in the original. `LinkedHashSet` finally gets first/last access. `SequencedMap` adds `firstEntry()`, `lastEntry()`, and `reversed()` for maps. *(Section 19)*

---

### Streams (7 questions)

**Q14: Explain lazy evaluation and short-circuiting in Java streams. Give an example where they interact.**

**A**: Lazy evaluation means intermediate operations (filter, map, flatMap) execute nothing until a terminal operation is invoked. Short-circuiting means certain terminal operations (findFirst, anyMatch, limit) stop processing as soon as they have the answer. Together:
```java
List.of(1, 2, 3, 4, 5).stream()
    .filter(n -> n % 2 == 0)  // lazy
    .map(n -> n * 10)          // lazy
    .findFirst();              // terminal, short-circuits at first match
// Only processes: 1 (odd, filtered), 2 (even, mapped to 20, returned)
// 3, 4, 5 are never touched
```
The trap: `sorted()` breaks short-circuiting by eagerly materializing all elements before emitting any. `sorted().findFirst()` is O(n log n) when `min()` would be O(n). *(Section 20)*

**Q15: `thenApply` vs `thenCompose` in CompletableFuture, `map` vs `flatMap` in streams — explain the pattern.**

**A**: It's the same concept: `map` (or `thenApply`) wraps the result; `flatMap` (or `thenCompose`) flattens nested containers. In streams: `map(x -> Stream.of(x))` gives `Stream<Stream<T>>` — you need `flatMap` to get `Stream<T>`. In `CompletableFuture`: `thenApply(x -> aFutureOf(x))` gives `CF<CF<T>>` — you need `thenCompose` to get `CF<T>`. The rule is universal: when your transformation function returns the same container type as the outer container, use the flatMap variant. *(Section 3, 21)*

**Q16: When would you use `Collectors.teeing()` and give a production example?**

**A**: `teeing` runs two collectors on the same stream in a single pass and merges their results. Use it when you need two different aggregations of the same data and don't want two separate stream iterations:
```java
// Compute order stats in one pass: count + total revenue
record OrderStats(long count, double totalRevenue) {}
OrderStats stats = orders.stream()
    .collect(Collectors.teeing(
        Collectors.counting(),
        Collectors.summingDouble(Order::amount),
        OrderStats::new
    ));

// Min and max price simultaneously
record PriceRange(Optional<Double> min, Optional<Double> max) {}
PriceRange range = products.stream()
    .map(Product::price)
    .collect(Collectors.teeing(
        Collectors.minBy(Comparator.naturalOrder()),
        Collectors.maxBy(Comparator.naturalOrder()),
        PriceRange::new
    ));
```
*(Section 22)*

**Q17: A parallel stream on a `LinkedList` performs worse than sequential. Why?**

**A**: `Spliterator` determines how efficiently a source can be split for parallel processing. `LinkedList`'s spliterator is O(n) to split — finding the midpoint requires traversing half the list. With a `LinkedList` of N elements, splitting for P parallel threads takes O(N) just for the splits, before any real work happens. `ArrayList`, arrays, and `IntStream.range` have O(1) splitting (index arithmetic) and benefit strongly from parallelism. Fix: `new ArrayList<>(linkedList).parallelStream()` — pay O(n) once to copy, then get good parallel splitting. Or better: question whether a `LinkedList` is the right structure at all — `LinkedList` is rarely the best choice in modern Java. *(Section 23)*

**Q18: A stream pipeline with `filter → map → collect` is 15x slower than expected on 1M integers. What's the likely cause?**

**A**: Almost certainly boxing overhead. `Stream<Integer>` (from a `List<Integer>`) boxes/unboxes every element through the pipeline. At 1M elements, that's 1M box allocations plus GC pressure. Fix:
```java
// SLOW: boxed Integer stream
int sum = intList.stream()
    .filter(n -> n > 0)
    .map(n -> n * 2)
    .mapToInt(Integer::intValue)  // converts to IntStream
    .sum();

// FAST: stay in primitive stream throughout
int sum = intList.stream()
    .mapToInt(Integer::intValue)   // convert immediately
    .filter(n -> n > 0)
    .map(n -> n * 2)
    .sum();
```
Benchmark with JMH before and after — the difference is typically 10-15x for numeric aggregation. *(Section 24)*

**Q19: Design a stream pipeline that computes, in a single pass, the top 3 customers by revenue and the overall average order value.**

**A**: Use `Collectors.teeing` for single-pass, combined with `groupingBy` + `summingDouble` for top customers:
```java
record DashboardStats(List<String> top3Customers, double avgOrderValue) {}

DashboardStats stats = orders.stream()
    .collect(Collectors.teeing(
        // Downstream 1: group by customer, sum revenue, take top 3
        Collectors.collectingAndThen(
            Collectors.groupingBy(Order::customerId, Collectors.summingDouble(Order::amount)),
            map -> map.entrySet().stream()
                .sorted(Map.Entry.<String, Double>comparingByValue().reversed())
                .limit(3)
                .map(Map.Entry::getKey)
                .toList()
        ),
        // Downstream 2: average order value
        Collectors.averagingDouble(Order::amount),
        // Merge
        DashboardStats::new
    ));
```
*(Section 22)*

**Q20: What happens if you use `peek()` with `anyMatch()` to count all processed elements? Will it work?**

**A**: No. `peek()` is an intermediate operation that may or may not execute for each element. With `anyMatch()`, the stream short-circuits — once it finds the first matching element, it stops. `peek()` is only called for elements that are actually processed before short-circuiting. Any count in `peek()` will be wrong:

```java
AtomicInteger count = new AtomicInteger();
boolean found = List.of("a", "bb", "ccc", "dddd").stream()
    .peek(s -> count.incrementAndGet())      // only called until match
    .anyMatch(s -> s.length() > 2);
// count is 3, not 4 — "dddd" was never peeked
```
Use `peek()` only for debugging. For counting, use `Collectors.counting()` or a terminal `count()`. *(Section 24)*

---

### Advanced Scenario Questions (4 questions)

**Q21: You need to call 5 microservices in parallel with a 3-second total timeout. If any fails, return a partial result. Design the solution.**

**A**: Use `CompletableFuture.allOf` with individual fallbacks and overall timeout:
```java
CompletableFuture<InventoryResult> inv = inventoryService.get()
    .orTimeout(2, TimeUnit.SECONDS)
    .exceptionally(ex -> InventoryResult.unknown());

CompletableFuture<PriceResult> price = priceService.get()
    .orTimeout(2, TimeUnit.SECONDS)
    .exceptionally(ex -> PriceResult.defaultPrice());

// ... 3 more similarly

CompletableFuture<AggregatedResult> result = CompletableFuture
    .allOf(inv, price, /* ... */)
    .orTimeout(3, TimeUnit.SECONDS)  // global cutoff
    .thenApply(v -> new AggregatedResult(
        inv.join(), price.join() /* ... */
    ));
```
Java 21+ with `StructuredTaskScope` is cleaner for this pattern — subtask lifetime is guaranteed, and cancellation propagates automatically with `joinUntil(Instant.now().plusSeconds(3))`. *(Sections 4, 15)*

**Q22: How would you migrate a legacy Spring MVC service from a 200-thread platform thread pool to virtual threads? What are the risks?**

**A**: (1) **Enable**: `spring.threads.virtual.enabled=true` (Spring Boot 3.2+) — one line. (2) **Audit**: check for `synchronized` + blocking I/O combinations in your own code and dependencies (critical for Java 21; not needed for Java 24+). Update HikariCP, Caffeine, and HTTP clients to virtual-thread-safe versions. (3) **ThreadLocal audit**: find any `ThreadLocal` used as a cache — they'll be ineffective with virtual threads. Replace with shared caches or `ScopedValue`. (4) **Connection pool sizing**: with virtual threads, you can hit your DB/Redis/Kafka connection limit quickly — ensure pool sizes are set correctly, add `Semaphore` throttling if needed. (5) **Load test**: run the same load that previously exhausted platform threads — virtual threads should handle it without degradation. (6) **Monitor**: track `jdk.VirtualThreadPinned` JFR events (Java 21-23), connection pool exhaustion metrics, and `jdk.VirtualThreadSubmitFailed`. Risk of this migration is low — it's largely transparent. *(Section 12, 14)*

**Q23: When would you choose `StructuredTaskScope` over `CompletableFuture`?**

**A**: `StructuredTaskScope` (Java 21+, finalized in Java 25) is the right choice when: (1) Request-scoped parallel I/O where **all subtasks must complete or all must cancel** — the scope guarantees no orphaned tasks. (2) Timeout must propagate: `joinUntil()` cancels all subtasks when the deadline expires. (3) Context propagation is needed — `ScopedValue`s are auto-inherited by forked subtasks; no manual wrapping. (4) Error handling must be clean — `throwIfFailed()` is explicit and structured. `CompletableFuture` remains the right choice for: non-blocking callback pipelines, reactive event-driven architectures, complex async composition that shouldn't block a thread at all, background/long-running tasks. The rule: `StructuredTaskScope` = request-scoped parallel work; `CompletableFuture` = non-blocking pipelines. *(Section 15)*

**Q24: A colleague argues parallel streams should replace virtual threads for I/O-bound work. How do you respond?**

**A**: Wrong tool for the wrong job. Parallel streams use `ForkJoinPool.commonPool()` for CPU-bound divide-and-conquer. Running I/O inside parallel streams: (1) Blocks ForkJoinPool worker threads, which are also shared by parallel stream operations elsewhere in the JVM. (2) The ForkJoinPool's work-stealing is optimized for CPU-bound tasks — it doesn't unmount on I/O. (3) You lose the short-circuit and pipeline fusion benefits when tasks are blocked waiting for I/O. (4) Thread count is limited to CPU cores in commonPool — poor concurrency for I/O. Virtual threads are designed exactly for I/O-bound work: unmount on I/O, millions possible, work on any executor including the virtual thread executor. For I/O: virtual threads + blocking code. For CPU-bound parallel computation on a large collection: parallel streams. They solve different problems. *(Section 12, 23)*

---

## Quick Reference Card

### CompletableFuture Decision Tree

```
Result available now?               → completedFuture() / failedFuture()
CPU-bound async?                    → supplyAsync(task, cpuExecutor)
I/O-bound async?                    → supplyAsync(task, virtualThreadExecutor)
Transform value?                    → thenApply()
Chain another CF?                   → thenCompose()
Merge two CFs?                      → thenCombine()
Wait for all?                       → allOf() → join each
First success?                      → anyOf() or applyToEither()
Side effects?                       → thenAccept() / whenComplete()
Error fallback?                     → exceptionally()
Handle both outcomes?               → handle()
Always log/metrics?                 → whenComplete()
Timeout (fail loud)?                → orTimeout()
Timeout (graceful)?                 → completeOnTimeout()
```

### Virtual Threads Cheat Sheet

```
I/O-bound + Java 21+?                → spring.threads.virtual.enabled=true
Custom async task?                   → Executors.newVirtualThreadPerTaskExecutor()
CPU-bound?                           → DO NOT use virtual threads → fixed platform pool
synchronized + blocking I/O?        → Java 21-23: use ReentrantLock; Java 24+: fine
ThreadLocal as cache?                → Replace with ConcurrentHashMap or ScopedValue
Overwhelming connection pool?        → Add Semaphore(poolSize)
Check pinning (Java 21-23)?         → JFR: jdk.VirtualThreadPinned
```

### Streams Cheat Sheet

```
Numbers → use IntStream / LongStream / DoubleStream (avoid boxing 15x penalty)
First element → findFirst() not sorted().findFirst() (use min/max instead)
Parallel → only CPU-bound, N > 10k, splittable source (ArrayList/array)
No I/O in parallel streams → use virtual threads for I/O
peek() → debugging only, never logic
stream reuse → not allowed, use Supplier<Stream<T>>
filter before map → put cheapest filters first
toList() → Java 16+ shorthand (immutable, unmodifiable)
```

---

*Sources: OpenJDK JEPs (440, 441, 444, 491, 505), Java official docs, Baeldung, Concurrency Deep Dives, TheCodeForge, Medium engineering blogs (April-May 2026), Reddit r/java production discussions, JavaCodeGeeks virtual threads war stories, devops-monk.com benchmarks, BackendBytes stream internals guide, Spring Boot official docs.*
