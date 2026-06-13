# Bulkhead Pattern

## Problem

One slow or failing dependency can consume all threads, connections, or memory in your service. Everything else — unrelated features — slows down or times out too.

## Core Idea

**Isolate resources per dependency**, like watertight compartments on a ship. If the payment API stalls, catalog and search still have their own thread pools and connection limits.

Common forms:

1. **Semaphore bulkhead** — cap concurrent in-flight calls to one dependency
2. **Thread pool bulkhead** — dedicated executor per dependency
3. **Connection pool bulkhead** — separate JDBC/HTTP pool limits per downstream

## Production Rules

1. Size bulkheads from load tests, not guesses
2. When full, **reject fast** or queue with a bounded wait — do not grow unbounded
3. Use **with** circuit breaker and timeout — bulkhead alone does not detect "bad" dependency
4. Monitor: available permits, rejected calls, pool queue depth
5. Do not share one giant pool for all external calls

## Runnable Example

See `examples/bulkhead/BulkheadDemo.java`.

```bash
javac examples/bulkhead/BulkheadDemo.java && java -cp examples/bulkhead BulkheadDemo
```

## Expert Playbook

Full depth: `docs/bulkhead-expert-playbook.md`

---

## How to Talk About Bulkheads in an Interview

> Plain English. Short sentences.

---

### "What is a bulkhead?"

It limits how much of your app's capacity one dependency can use.

If the payment service goes slow, you don't want all 200 threads stuck waiting on payment while users can't even load the product page.

You give payment its own small pool — say 20 concurrent calls max. The rest of the app keeps working with separate resources.

---

### Quick Answers

| Question | Say this |
|---|---|
| What problem? | One bad dependency exhausts shared threads/connections |
| Semaphore vs thread pool? | Semaphore limits concurrency; thread pool also isolates execution |
| vs circuit breaker? | Bulkhead limits resource usage; breaker stops calling when dependency is failing |
| vs rate limiter? | Rate limiter caps requests per time window; bulkhead caps concurrent in-flight |
| When full? | Fail fast or bounded queue — never unbounded wait |
