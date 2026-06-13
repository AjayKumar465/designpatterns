# Circuit Breaker Pattern

## Problem

When a downstream service is slow or failing, callers keep waiting, threads pile up, and the failure spreads to your whole app — cascading failure.

## Core Idea

A circuit breaker watches call outcomes. After enough failures it **opens** and **fails fast** instead of waiting on a dead dependency. After a cooldown it allows a **probe** in half-open state; success **closes** the circuit again.

States: **CLOSED** → **OPEN** → **HALF_OPEN** → **CLOSED**

## Production Rules

1. Pair with **timeouts** — slow calls must not hold threads forever
2. Provide a **fallback** (cached data, degraded response, route to legacy)
3. Do not stack **retry inside an open circuit** — fail fast when open
4. Emit metrics: state, failure rate, rejected calls
5. One breaker **per dependency**, not one global breaker for everything

## Runnable Example

See `examples/circuit-breaker/CircuitBreakerDemo.java`.

```bash
javac examples/circuit-breaker/CircuitBreakerDemo.java && java -cp examples/circuit-breaker CircuitBreakerDemo
```

## Expert Playbook

Full depth: `docs/circuit-breaker-expert-playbook.md`

---

## How to Talk About Circuit Breakers in an Interview

> Plain English. Short sentences.

---

### "What is a circuit breaker?"

It's like an electrical breaker in your house. If a downstream service keeps failing, the breaker trips and stops sending more requests there.

Instead of every request waiting 30 seconds and timing out, new requests fail immediately. That protects your app from running out of threads.

After a short wait, it tries one test request. If that works, it closes again and normal traffic resumes.

---

### Quick Answers

| Question | Say this |
|---|---|
| What problem? | Cascading failure when a dependency is down |
| Three states? | CLOSED (normal), OPEN (fail fast), HALF_OPEN (test probe) |
| vs retry? | Retry helps transient errors; breaker stops hammering a dead service |
| vs bulkhead? | Breaker stops calling a bad dependency; bulkhead limits how many calls can run at once |
| Must-have with breaker? | Timeout + fallback |
