# JVM Performance & Tuning — Expert Revision Playbook (Production, Kubernetes, Interviews)

> **Revision guide** — scan sections before interviews or on-call. Deep production depth with plain language. Targets Java Lead/Architect roles in Spring Boot shops running on Kubernetes.

A comprehensive end-to-end reference covering JVM memory model, container-aware tuning, garbage collectors, profiling, JIT warmup, virtual threads, Spring Boot production metrics, common failure modes, 25+ production scenarios, and 42+ lead-level interview Q&As. Sourced from OpenJDK docs, Oracle tuning guides, production war stories, r/java themes, and K8s Java-on-K8s runbooks.

**Related playbooks in this repo:**

- [Kubernetes Expert Playbook](kubernetes-expert-playbook.md) — Section 15 covers Spring Boot and Java on K8s (memory limits, probes, graceful shutdown)
- [Metrics & Observability Playbook](metrics-observability-playbook.md) — Micrometer, Prometheus, JVM/GC metrics, alerting on heap and GC pauses
- [Bulkhead Expert Playbook](bulkhead-expert-playbook.md) — isolate thread pools and connection pools to prevent resource exhaustion
- [Java Modern Concurrency & Streams Playbook](java-modern-concurrency-streams-playbook.md) — virtual threads, platform threads, pinning, structured concurrency

---

## Table of Contents

1. [JVM Memory Model — Heap, Metaspace, Direct Memory, Thread Stacks](#section-1-jvm-memory-model--heap-metaspace-direct-memory-thread-stacks)
2. [Container-Aware JVM and Kubernetes Memory](#section-2-container-aware-jvm-and-kubernetes-memory)
3. [Garbage Collection — G1, ZGC, When to Switch, Reading GC Logs](#section-3-garbage-collection--g1-zgc-when-to-switch-reading-gc-logs)
4. [Profiling and Diagnostics — async-profiler, jcmd, Dumps](#section-4-profiling-and-diagnostics--async-profiler-jcmd-dumps)
5. [JIT Compilation, Warmup, and Class Loading](#section-5-jit-compilation-warmup-and-class-loading)
6. [Virtual Threads vs Platform Threads on K8s](#section-6-virtual-threads-vs-platform-threads-on-k8s)
7. [Common Production Issues — Memory Leaks, Metaspace, GC Pauses, Thread Exhaustion](#section-7-common-production-issues--memory-leaks-metaspace-gc-pauses-thread-exhaustion)
8. [Spring Boot Production — Actuator, Pools, HTTP Clients](#section-8-spring-boot-production--actuator-pools-http-clients)
9. [Production Scenario Runbook — 25+ Scenarios](#section-9-production-scenario-runbook--25-scenarios)
10. [Lead Interview Questions — 42+ Logical and Production Scenarios](#section-10-lead-interview-questions--42-logical-and-production-scenarios)
11. [How to Talk About JVM Performance in an Interview](#section-11-how-to-talk-about-jvm-performance-in-an-interview)
12. [Appendix — JVM Flags Cheat Sheet, Decision Trees, Quick Reference](#section-12-appendix--jvm-flags-cheat-sheet-decision-trees-quick-reference)

---

## Section 1: JVM Memory Model — Heap, Metaspace, Direct Memory, Thread Stacks

### 1.1 The Big Picture — What the JVM Actually Allocates

When you set `memory: 1Gi` on a Kubernetes Pod, the **Linux cgroup** enforces that limit on **all memory** the process uses — not just the Java heap. The JVM splits its footprint across several regions:

```
┌─────────────────────────────────────────────────────────────────┐
│                    PROCESS MEMORY (cgroup limit)                │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │                    JAVA HEAP (G1/ZGC managed)             │  │
│  │  Young Gen (Eden + Survivor) │ Old Gen                    │  │
│  └───────────────────────────────────────────────────────────┘  │
│  ┌──────────────┐  ┌──────────────────────────────────────────┐ │
│  │  METASPACE   │  │  DIRECT / NATIVE (Netty, NIO, JNI)       │ │
│  │  (class meta)│  │  + Code Cache (JIT compiled code)        │ │
│  └──────────────┘  └──────────────────────────────────────────┘ │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │  THREAD STACKS (platform threads × stack size)              │  │
│  │  + JVM internal (GC threads, compiler threads, etc.)        │  │
│  └───────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

**Interview one-liner:** "Heap is only part of the story — metaspace, direct buffers, and thread stacks live outside `-Xmx` and can OOMKill your Pod even when heap looks fine."

### 1.2 Java Heap — Young and Old Generation

| Region | Purpose | GC interaction |
|--------|---------|----------------|
| **Eden** | New object allocations | Minor GC collects when Eden full |
| **Survivor (S0/S1)** | Objects surviving minor GCs | Promoted to Old after threshold |
| **Old Gen** | Long-lived objects | Major/full GC when Old full or concurrent cycle |

**Key facts:**

- Almost all `new` allocations happen in Eden (TLAB — Thread Local Allocation Buffer per thread for speed).
- Objects that survive enough minor GCs are **promoted** to Old Gen.
- **Large objects** may go directly to Old Gen (G1: Humongous regions).
- Heap size is controlled by `-Xmx` / `-Xms` or container-aware `MaxRAMPercentage`.

```bash
# Inspect heap at runtime
jcmd <pid> GC.heap_info
jcmd <pid> VM.native_memory summary
```

### 1.3 Metaspace (PermGen Replacement)

Since Java 8, **class metadata** (class definitions, method bytecode, constant pools, annotations) lives in **native memory** called **Metaspace** — not on the heap.

| Aspect | Detail |
|--------|--------|
| Default limit | No fixed cap unless `-XX:MaxMetaspaceSize` set |
| Growth | Allocates from native memory as classes load |
| OOM type | `java.lang.OutOfMemoryError: Metaspace` |
| Common triggers | Dynamic proxies (Hibernate, Spring), Groovy, OSGi, hot redeploy without restart |

**Production triggers:**

- Spring Boot with many `@Configuration` classes and CGLIB proxies
- Classpath scanning loading thousands of classes at startup
- **Classloader leak** — old WAR redeploys without GCing old metaspace (rare on K8s where Pods restart)
- Reflection-heavy frameworks generating many synthetic classes

```bash
# Metaspace flags
-XX:MetaspaceSize=256m          # initial; triggers GC when exceeded
-XX:MaxMetaspaceSize=512m       # hard cap — OOM when hit
```

**Monitor:** `jvm.memory.used` tag `area=nonheap`, `id=Metaspace` via Actuator/Micrometer.

### 1.4 Direct Memory (Off-Heap)

**Direct byte buffers** (`ByteBuffer.allocateDirect()`) allocate outside the heap in **native memory**. Netty, gRPC, NIO channels, and many HTTP clients use direct buffers for zero-copy I/O.

| Aspect | Detail |
|--------|--------|
| Default max | `-XX:MaxDirectMemorySize` defaults to `-Xmx` (same as max heap) |
| OOM type | `OutOfMemoryError: Direct buffer memory` |
| Leak pattern | Buffers not released; reference held; pool misconfigured |
| K8s impact | Counts toward cgroup memory limit — invisible in heap metrics |

```bash
# Limit direct memory explicitly in high-Netty workloads
-XX:MaxDirectMemorySize=256m
```

**War story:** Service with 1Gi Pod limit, 75% MaxRAMPercentage (~750Mi heap), heavy Netty reactive stack — direct buffers + heap exceeded 1Gi under load → OOMKilled with **no** Java heap OOM in logs. Grafana heap chart looked healthy.

### 1.5 Thread Stacks

Each **platform thread** consumes stack memory in native memory (default **1MB per thread** on Linux, configurable via `-Xss`).

```
200 Tomcat threads × 1MB stack ≈ 200MB native (not heap)
```

| Workload | Stack impact |
|----------|--------------|
| Traditional Spring MVC (200 platform threads) | 200–400MB stacks alone |
| Virtual threads (Java 21+) | Millions of virtual threads share carrier pool stacks — see Section 6 |
| Custom `ExecutorService` with large pool | Same stack multiplication |

```bash
-Xss512k    # reduce per-thread stack if deep stacks not needed (careful with recursion)
```

### 1.6 Code Cache

JIT-compiled native code lives in the **Code Cache** (native memory, separate from metaspace).

- Default: ~240MB (varies by JVM version)
- OOM: `OutOfMemoryError: CodeCache is full` — rare; usually means too many hot methods or deoptimization churn
- Flag: `-XX:ReservedCodeCacheSize=256m`

### 1.7 Native Memory Tracking (NMT)

Use NMT to see **where** native memory goes when cgroup OOM happens without heap OOM:

```bash
# Enable at startup (small overhead)
-XX:NativeMemoryTracking=summary

# In running pod (requires same flag at start)
jcmd <pid> VM.native_memory summary scale=MB

jcmd <pid> VM.native_memory detail.diff summary scale=MB
```

**NMT categories:** Java Heap, Class, Thread, Code, GC, Compiler, Internal, Other.

### 1.8 Memory Sizing Formula for Kubernetes

```
container memory limit ≥
    heap (MaxRAMPercentage of limit)
  + metaspace (steady + growth headroom ~128-256Mi)
  + direct memory (Netty/gRPC — often 128-512Mi)
  + thread stacks (platform threads × stack size)
  + code cache (~64-256Mi)
  + 10-15% safety margin
```

**Example — 1Gi Pod, Spring MVC, JDBC:**

| Component | Estimate |
|-----------|----------|
| Heap (75% MaxRAMPercentage) | ~768Mi |
| Metaspace | ~150Mi |
| Direct + stacks + code | ~100Mi |
| **Total** | **~1.02Gi** → **OOM risk** |

**Fix:** Either increase Pod to 1.25Gi, or reduce MaxRAMPercentage to 60-65%, or switch to virtual threads to cut stack memory.

Cross-ref: [kubernetes-expert-playbook.md Section 15.1](kubernetes-expert-playbook.md#section-15-spring-boot-and-java-on-kubernetes).

---

## Section 2: Container-Aware JVM and Kubernetes Memory

### 2.1 The cgroup Problem (Pre-Java 10)

Before container awareness, the JVM read **host** memory via `sysconf` and set default max heap accordingly. On a 64GB node with a 512Mi Pod:

```
JVM thinks: max heap ~16GB
cgroup limit: 512Mi
Result: OOMKilled within minutes
```

**Java 8u191+, Java 11+, Java 17+:** JVM reads cgroup v1/v2 limits and treats them as "available RAM" for default sizing.

### 2.2 MaxRAMPercentage — The Production Default

```bash
JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=75.0 -XX:InitialRAMPercentage=50.0"
```

| Flag | Meaning |
|------|---------|
| `MaxRAMPercentage` | Max heap as % of **container memory limit** (not request) |
| `InitialRAMPercentage` | Initial heap size at startup |
| `MinRAMPercentage` | Used when container limit < ~200MB |

**Important:** Percentage applies to **cgroup memory limit**. If you set only `requests` without `limits`, cgroup may be unlimited → JVM uses node memory for calculation.

### 2.3 MaxRAMPercentage Sizing Table

| Container limit | MaxRAMPercentage | Approx heap | Notes |
|-----------------|------------------|-------------|-------|
| 512Mi | 50-55% | 256-280Mi | Tight — metaspace/direct may exceed |
| 768Mi | 60-65% | 460-500Mi | Small microservices |
| 1Gi | 70-75% | 700-768Mi | Common Spring Boot default |
| 2Gi | 75% | ~1.5Gi | Standard production service |
| 4Gi | 75-80% | 3-3.2Gi | Heavy caching, batch |

**Leave headroom:** Never set MaxRAMPercentage to 90%+ in production unless you've validated metaspace/direct/stacks with NMT under load.

### 2.4 OOMKilled vs Java OutOfMemoryError

| Signal | Meaning | Where to look |
|--------|---------|---------------|
| **OOMKilled** (exit 137) | Linux cgroup killed process — total memory exceeded limit | `kubectl describe pod`, cgroup metrics |
| **Java heap OOM** | `OutOfMemoryError: Java heap space` | Heap dump, leak analysis |
| **Metaspace OOM** | `OutOfMemoryError: Metaspace` | Class loading, proxies |
| **Direct buffer OOM** | `OutOfMemoryError: Direct buffer memory` | Netty, NIO pools |

**Critical distinction:** OOMKilled often has **no** Java stack trace — the kernel killed the process. Java OOM throws inside JVM and may leave a heap dump if configured.

```bash
kubectl describe pod <pod> | grep -A5 "Last State"
# Reason: OOMKilled, Exit Code: 137
```

### 2.5 Common OOMKilled Causes on Kubernetes

| # | Cause | Diagnosis | Fix |
|---|-------|-----------|-----|
| 1 | MaxRAMPercentage too high | NMT summary; heap + non-heap > limit | Lower to 60-70% |
| 2 | No memory limit set | JVM sizes to node RAM; node pressure evicts | Set limits; use MaxRAMPercentage |
| 3 | Memory limit = request, no burst room | Spike during GC or allocation | Limit > request (Burstable) or increase |
| 4 | Direct buffer leak (Netty) | NMT Internal/Direct; direct OOM | Fix leak; cap MaxDirectMemorySize |
| 5 | Metaspace growth | nonheap Metaspace metric climbing | MaxMetaspaceSize; reduce class loading |
| 6 | Too many platform threads | NMT Thread category | Virtual threads; reduce pool sizes |
| 7 | Native library leak (JNI) | NMT Other/Internal growth | Fix native code; restart |
| 8 | Heap dump on OOM fills disk/memory | `-XX:+HeapDumpOnOutOfMemoryError` path on small volume | Write to emptyDir with size limit |
| 9 | Sidecar + app share Pod limit | Only app container OOMKilled | Separate containers or raise limit |
| 10 | HPA scales before JVM warms | Cold pods spike memory | Startup probe; min replicas; warmup |
| 11 | `-Xmx` hardcoded larger than limit | Old Dockerfile JAVA_OPTS | Remove -Xmx; use MaxRAMPercentage |
| 12 | G1 humongous objects | Heap regions; large byte arrays | Increase heap or reduce object size |

### 2.6 CPU Limits and JVM Performance

CPU **limits** cause **throttling** when the process exceeds its quota — GC threads and JIT compiler compete for CPU too.

```promql
# Throttling indicator
rate(container_cpu_cfs_throttled_seconds_total{container="app"}[5m])
```

| Strategy | Trade-off |
|----------|-----------|
| CPU request = expected usage | Good scheduling; may throttle on spikes if limit set |
| CPU limit omitted (common for Java) | No throttle; risk of noisy neighbor |
| CPU limit = 2× request | Balance; watch throttling metrics |

**GC pause spikes under throttle:** GC can't get enough CPU → longer pauses → timeouts → more load → worse. See Section 9 scenario 14.

Cross-ref: [kubernetes-expert-playbook.md Section 15.3](kubernetes-expert-playbook.md#153-cpu-for-java).

### 2.7 JAVA_TOOL_OPTIONS vs JAVA_OPTS

| Variable | Behavior |
|----------|----------|
| `JAVA_TOOL_OPTIONS` | Picked up automatically by JVM launcher — **preferred on K8s** |
| `JAVA_OPTS` | Convention in some images; must be wired in entrypoint script |

```yaml
env:
  - name: JAVA_TOOL_OPTIONS
    value: >-
      -XX:MaxRAMPercentage=75.0
      -XX:+UseG1GC
      -XX:+ExitOnOutOfMemoryError
      -XX:+HeapDumpOnOutOfMemoryError
      -XX:HeapDumpPath=/tmp/heapdumps
      -XX:NativeMemoryTracking=summary
```

### 2.8 ExitOnOutOfMemoryError — Production Best Practice

```bash
-XX:+ExitOnOutOfMemoryError
```

When JVM throws **any** `OutOfMemoryError`, process exits immediately → Kubernetes restarts Pod → alerts fire. Without this, JVM may limp in degraded state (partial GC failure, stuck threads).

**Pair with:** PodDisruptionBudget, readiness probes, circuit breakers so restart doesn't cascade.

### 2.9 Memory Requests vs Limits for Java

```
memory request  = steady-state working set (heap used + metaspace + direct steady)
memory limit    = request + GC headroom + spike buffer (or equal for Guaranteed QoS)
```

| QoS | Setting | Java implication |
|-----|---------|------------------|
| Guaranteed | limit = request | Predictable; no burst; OOM if underestimated |
| Burstable | limit > request | Spike room for GC; still OOMKilled if spike exceeds limit |

**VPA caution:** Vertical Pod Autoscaler changing memory limits **restarts Pods** and changes heap size on next boot — plan maintenance windows. Cross-ref K8s playbook Section 10.

### 2.10 Sample K8s Deployment JVM Block

```yaml
containers:
  - name: app
    resources:
      requests:
        cpu: "500m"
        memory: "1Gi"
      limits:
        memory: "1Gi"
    env:
      - name: JAVA_TOOL_OPTIONS
        value: >-
          -XX:MaxRAMPercentage=70.0
          -XX:InitialRAMPercentage=50.0
          -XX:MaxMetaspaceSize=256m
          -XX:MaxDirectMemorySize=256m
          -XX:+UseG1GC
          -XX:+ExitOnOutOfMemoryError
          -XX:NativeMemoryTracking=summary
```

---

## Section 3: Garbage Collection — G1, ZGC, When to Switch, Reading GC Logs

### 3.1 GC Basics — What You're Optimizing

| Goal | Metric | Target (typical SLA) |
|------|--------|---------------------|
| Throughput | % time in application vs GC | > 95% for batch |
| Latency | GC pause duration (P99) | < 200ms for APIs |
| Footprint | Heap size needed | Minimize for K8s cost |

**Two families:**

- **Stop-the-world (STW)** — all app threads pause during part of collection
- **Concurrent** — GC work overlaps with app (reduces pause, adds CPU overhead)

### 3.2 G1 GC — Default Since Java 9

**G1 (Garbage-First)** divides heap into **regions** (1-32MB). Collects regions with most garbage first.

```
┌────┬────┬────┬────┬────┬────┬────┬────┐
│ E  │ E  │ S  │ O  │ O  │ H  │ free│...│  Regions
└────┴────┴────┴────┴────┴────┴────┴────┘
 E=Eden  S=Survivor  O=Old  H=Humongous
```

**Collection types:**

| Event | Trigger | Pause typical |
|-------|---------|---------------|
| Young GC | Eden full | 10-50ms |
| Mixed GC | Old gen occupancy threshold | 50-200ms |
| Full GC | Humongous allocation failure, metaspace, explicit `System.gc()` | **Seconds** — avoid |

**Default flags (Java 17+):**

```bash
-XX:+UseG1GC
-XX:MaxGCPauseMillis=200        # soft target — not a guarantee
-XX:G1HeapRegionSize=4m         # auto by default
-XX:InitiatingHeapOccupancyPercent=45
```

**When G1 is right:**

- Heap 512MB – 32GB
- Default for Spring Boot on K8s
- Mixed latency/throughput workloads
- Team familiarity

### 3.3 ZGC — Low-Latency Collector

**ZGC** (Java 15+ production, Java 21+ generational ZGC) targets **sub-millisecond** pauses at large heaps.

```bash
-XX:+UseZGC
-XX:+ZGenerational    # Java 21+ — generational ZGC (recommended)
```

| Aspect | G1 | ZGC |
|--------|-----|-----|
| Pause target | 10-200ms typical | < 1ms typical |
| CPU overhead | Moderate | Higher (concurrent work) |
| Heap size sweet spot | 512MB-32GB | 4GB+ especially 16GB+ |
| K8s 1Gi Pod | **G1** | ZGC overhead may hurt |
| Large heap monolith 8GB+ | Good | **ZGC** often wins |

**When to switch to ZGC:**

- P99 GC pauses cause SLA breaches with G1 tuning exhausted
- Heap ≥ 4GB and pause-sensitive (trading, gaming backends, real-time APIs)
- Java 21+ with generational ZGC for allocation-heavy apps

**When NOT to switch:**

- Small heaps (< 2GB) — overhead dominates
- CPU-constrained Pods with tight limits
- Team has no ZGC tuning experience and G1 pauses are acceptable

### 3.4 Other Collectors (Brief)

| Collector | Use case | K8s note |
|-----------|----------|----------|
| **Serial** | Single-core, tiny heap | Embedded only |
| **Parallel GC** | Batch throughput, max GC throughput | Large pauses — avoid for APIs |
| **Shenandoah** | Similar to ZGC, Red Hat | Alternative low-latency |

### 3.5 GC Logging — Unified JVM Logging (Java 9+)

```bash
# Production GC logging
-Xlog:gc*,gc+age=trace,safepoint:file=/tmp/gc.log:utctime,uptime,level,tags:filecount=5,filesize=50M
```

**Legacy (Java 8):**

```bash
-XX:+PrintGCDetails -XX:+PrintGCDateStamps -Xloggc:/tmp/gc.log
```

### 3.6 Reading GC Logs — G1 Young GC

```
[2024-06-13T10:15:32.123+0000][12345.678s] GC(842) Pause Young (Normal) (G1 Evacuation Pause)
[2024-06-13T10:15:32.123+0000][12345.678s] GC(842) Using 8 workers of 8 for evacuation
[2024-06-13T10:15:32.145+0000][12345.700s] GC(842)   Pre Evacuate Collection Set: 0.2ms
[2024-06-13T10:15:32.156+0000][12345.711s] GC(842)   Merge Heap Roots: 0.1ms
[2024-06-13T10:15:32.189+0000][12345.744s] GC(842)   Evacuate Collection Set: 22.3ms
[2024-06-13T10:15:32.195+0000][12345.750s] GC(842)   Post Evacuate Collection Set: 5.8ms
[2024-06-13T10:15:32.196+0000][12345.751s] GC(842) Pause Young (Normal) 512M->180M(768M) 28.123ms
```

**Read this line:**

```
Pause Young (Normal) 512M->180M(768M) 28.123ms
```

| Field | Meaning |
|-------|---------|
| `512M->180M` | Heap before → after collection |
| `(768M)` | Current max heap |
| `28.123ms` | **Stop-the-world pause** — what users feel |

**Healthy:** Young GC pauses < 50ms, regular cadence under steady load.

### 3.7 Reading GC Logs — G1 Mixed GC

```
GC(901) Pause Mixed (G1 Evacuation Pause) 680M->320M(768M) 156.234ms
```

Mixed GC collects **both** young and **part** of old gen. Pauses 50-300ms are normal. **Frequent** mixed GCs with rising old gen → memory pressure or leak.

### 3.8 Reading GC Logs — Full GC (BAD)

```
GC(950) Pause Full (G1 Compaction) 750M->748M(768M) 2340.567ms
```

**Red flags:**

- Full GC taking **seconds**
- Heap after ≈ heap before (748M of 768M) — **almost no reclaim**
- Frequent full GCs → leak or heap undersized

**Action:** Heap dump, allocation profiling, increase heap, or fix leak.

### 3.9 Reading GC Logs — ZGC

```
[0.456s] GC(12) Garbage Collection (Allocation Rate) 2048M(60%)->2048M(60%)
[0.457s] GC(12) Pause Mark Start 0.234ms
[0.458s] GC(12) Concurrent Mark 12.456ms
[0.471s] GC(12) Pause Mark End 0.123ms
[0.472s] GC(12) Concurrent Relocate 8.234ms
```

Focus on **Pause** lines — should be sub-ms to low ms. Concurrent phases don't stop app threads.

### 3.10 GC Metrics in Prometheus

Cross-ref [metrics-observability-playbook.md](metrics-observability-playbook.md):

```promql
# GC pause rate (seconds spent in GC per second)
rate(jvm_gc_pause_seconds_sum[5m])

# GC pause count
rate(jvm_gc_pause_seconds_count[5m])

# Average pause duration
rate(jvm_gc_pause_seconds_sum[5m]) / rate(jvm_gc_pause_seconds_count[5m])
```

**Alert:** GC time > 5% of wall clock sustained → tuning or heap increase needed.

### 3.11 G1 Tuning Knobs (When Defaults Fail)

| Flag | Effect | Caution |
|------|--------|---------|
| `MaxGCPauseMillis=100` | Tries shorter pauses | May increase GC frequency |
| `G1HeapRegionSize=8m` | Larger regions | Fewer regions; larger humongous threshold |
| `InitiatingHeapOccupancyPercent=35` | Start mixed GC earlier | More frequent mixed GC |
| `-XX:G1ReservePercent=15` | Reserve for evacuation | Default 10 |

**Rule:** Measure before tuning. Change one flag at a time. Load test in staging with production-like data.

### 3.12 System.gc() and RMI — Hidden Full GC

Some libraries call `System.gc()` — triggers full GC on some collectors.

```bash
-XX:+DisableExplicitGC    # use with care — may break NIO direct buffer cleanup on old JDKs
```

**RMI DGC:** Java RMI periodically runs full GC — set:

```bash
-Dsun.rmi.dgc.client.gcInterval=0x7FFFFFFFFFFFFFFE
-Dsun.rmi.dgc.server.gcInterval=0x7FFFFFFFFFFFFFFE
```

### 3.13 GC Decision Tree

```
Start with G1 (default)
    │
    ├─ Pauses OK, throughput OK? → Done
    │
    ├─ Frequent Full GC, heap after ≈ before? → Leak or undersized heap
    │
    ├─ Young GC pauses high? → Check heap size, humongous objects, CPU throttle
    │
    ├─ Mixed GC pauses breach SLA, heap ≥ 4GB, Java 21+? → Try ZGenerational
    │
    └─ Batch job, pause irrelevant? → Consider Parallel GC (rare on K8s)
```

---

## Section 4: Profiling and Diagnostics — async-profiler, jcmd, Dumps

### 4.1 Diagnostic Toolkit Overview

| Tool | Purpose | Production safe? |
|------|---------|------------------|
| `jcmd` | JVM diagnostic commands | Yes — low overhead |
| `jstat` | GC/class stats | Yes |
| async-profiler | CPU, allocation, wall-clock profiling | Yes — short bursts |
| Heap dump | Memory leak analysis | **Pause** during dump — off-peak |
| Thread dump | Deadlocks, stuck threads | Yes |
| NMT | Native memory breakdown | Yes (if enabled at start) |
| JFR | Java Flight Recorder | Yes — built-in Java 11+ |

### 4.2 jcmd — The Swiss Army Knife

```bash
# List JVMs
jcmd

# All available commands
jcmd <pid> help

# Heap summary
jcmd <pid> GC.heap_info

# Class histogram (top objects by count)
jcmd <pid> GC.class_histogram | head -30

# Thread dump
jcmd <pid> Thread.print > /tmp/threads.txt

# Native memory
jcmd <pid> VM.native_memory summary scale=MB

# Force GC (diagnostic only — not production fix)
jcmd <pid> GC.run
```

**On Kubernetes:**

```bash
kubectl exec -it <pod> -- jcmd 1 GC.heap_info
# PID 1 is often the JVM in container images
```

If `jcmd` missing — use `kubectl debug` with JDK image. Cross-ref K8s playbook Section 2.4 ephemeral containers.

### 4.3 async-profiler — CPU and Allocation Profiling

[async-profiler](https://github.com/async-profiler/async-profiler) attaches to running JVM with minimal overhead.

```bash
# CPU profile 60 seconds, flame graph
./profiler.sh -d 60 -f /tmp/cpu.html <pid>

# Allocation profile
./profiler.sh -d 60 -e alloc -f /tmp/alloc.html <pid>

# Wall-clock (includes blocked time — great for I/O waits)
./profiler.sh -d 60 -e wall -f /tmp/wall.html <pid>
```

**Production workflow:**

1. Reproduce slowness or capture during incident
2. 30-60s profile — don't run hours
3. Upload HTML flame graph — read bottom-up (widest = most CPU)
4. Correlate with trace IDs from logs

**K8s:** Copy profiler into pod or use init container with shared volume. Some teams run profiler from **debug sidecar** with `shareProcessNamespace: true`.

### 4.4 Heap Dumps — Leak Investigation

```bash
# On OOM (configure at startup)
-XX:+HeapDumpOnOutOfMemoryError
-XX:HeapDumpPath=/tmp/heapdumps/
-XX:+ExitOnOutOfMemoryError

# Manual dump (causes STW pause — can be large)
jcmd <pid> GC.heap_dump /tmp/heap.hprof

# Or kill -3 (SIGQUIT) — thread dump only, NOT heap dump on modern JVM
```

**Analyze with:**

- Eclipse MAT (Memory Analyzer Tool)
- VisualVM
- `jhat` (deprecated)

**MAT workflow:**

1. Open heap dump
2. Leak Suspects Report
3. Dominator tree — largest retained objects
4. Find path to GC roots — what's holding references

**K8s caution:** 2GB heap → 2GB+ hprof file. Use `emptyDir` with size limit or copy to S3 via sidecar. Dump can **OOMKill** Pod if limit too tight.

### 4.5 Thread Dumps — Stuck and Deadlocked Threads

```bash
jcmd <pid> Thread.print
# or
kill -3 <pid>   # SIGQUIT — safe, no kill
```

**Look for:**

```
"http-nio-8080-exec-25" #123 daemon prio=5 os_prio=0 tid=0x... nid=0x... waiting on condition
   java.lang.Thread.State: WAITING (parking)
        at jdk.internal.misc.Unsafe.park(...)
        at java.util.concurrent.locks.LockSupport.park(...)
        at java.util.concurrent.FutureTask.await(...)
```

| State | Meaning |
|-------|---------|
| `RUNNABLE` | Running or waiting for OS (I/O) |
| `WAITING (parking)` | Blocked on lock, pool, or `park` |
| `BLOCKED` | Waiting for monitor enter |
| `deadlock` | MAT/jcmd reports deadlock |

**Thread pool exhaustion:** All `http-nio-8080-exec-*` threads in WAITING on same pool → downstream slowness or missing timeouts.

Cross-ref [bulkhead-expert-playbook.md](bulkhead-expert-playbook.md) Section 14.

### 4.6 Java Flight Recorder (JFR)

Built into OpenJDK 11+ — low overhead continuous profiling.

```bash
# Start with recording
-XX:StartFlightRecording=duration=60s,filename=/tmp/recording.jfr,settings=profile

# jcmd start/stop
jcmd <pid> JFR.start duration=60s filename=/tmp/rec.jfr settings=profile
jcmd <pid> JFR.dump filename=/tmp/snapshot.jfr
```

Analyze in JDK Mission Control (JMC) or IntelliJ.

**Production:** JFR without `duration` for always-on with `settings=default` (~1% overhead).

### 4.7 jstat — Quick GC Watch

```bash
jstat -gcutil <pid> 1000 10
# S0   S1   E    O    M   CCS  YGC  YGCT  FGC  FGCT  GCT
# 0.00 45.2 78.3 62.1 94.2 88.1  842  12.4    3  6.2  18.6

jstat -gccause <pid> 1000 5
# LGCC: G1 Evacuation Pause
# GCC:  G1 Compaction  ← full GC cause
```

### 4.8 Production Profiling Safety Checklist

- [ ] Profile duration ≤ 60-120s unless JFR default
- [ ] Heap dump only off-peak or from replica
- [ ] NMT enabled at startup if native OOM suspected
- [ ] Profiler binary matches OS/arch (glibc vs musl for Alpine)
- [ ] Don't leave heap dumps on unbounded emptyDir
- [ ] RBAC: restrict `kubectl exec` in production

---

## Section 5: JIT Compilation, Warmup, and Class Loading

### 5.1 Interpreter → JIT Pipeline

```
Bytecode → Interpreter (slow, immediate)
         → C1 compiler (fast compile, light optimization)
         → C2 compiler (slow compile, aggressive optimization)
```

**Hot methods** (frequently executed) get **compiled to native code** and stored in Code Cache.

**Cold start:** First thousands of requests run interpreted or C1 — **10-100× slower** than warmed state.

### 5.2 Warmup Impact on Kubernetes

| Scenario | Effect |
|----------|--------|
| Rolling deploy | New Pods cold → slow → readiness may pass before warm |
| HPA scale-up | New replicas cold at worst moment (peak load) |
| Serverless/KEDA scale-from-zero | Every cold start |
| GraalVM native | No JIT warmup — fast start, different peak performance |

**Mitigation:**

```yaml
# Startup probe — give JIT time before liveness kills
startupProbe:
  httpGet:
    path: /actuator/health/readiness
    port: 8080
  failureThreshold: 30
  periodSeconds: 10   # up to 300s warmup
```

**Synthetic warmup script** (after boot, before marking ready):

```bash
# Init or post-start hook — hit hot endpoints
for i in $(seq 1 500); do curl -s localhost:8080/api/health/hot-path; done
```

**AOT / CDS (Class Data Sharing):**

```bash
# Java 17+ — shared archive for faster class loading
java -Xshare:dump -XX:SharedArchiveFile=app.jsa -jar app.jar
java -XX:SharedArchiveFile=app.jsa -jar app.jar
```

Spring Boot 3.3+ experimental AOT for native — different path.

### 5.3 Class Loading at Spring Boot Startup

Spring Boot startup sequence loads **thousands of classes**:

1. JVM bootstrap classes
2. Spring framework (500+ classes)
3. Auto-configuration scanning
4. Hibernate/JPA entities
5. Jackson serializers
6. CGLIB proxies for `@Transactional`, `@Cacheable`

**Typical metaspace after startup:** 100-200MB for medium Spring app.

**Fat JAR:** Spring Boot loader adds minimal overhead; **classpath scanning** is the cost.

**Optimization (Spring Boot 2.2+):**

```properties
spring.main.lazy-initialization=true   # delays bean creation — faster boot, slower first request
spring.jmx.enabled=false
```

**Spring AOT (native):** Build-time processing — eliminates runtime classpath scan.

### 5.4 Tiered Compilation

```bash
-XX:+TieredCompilation    # default on server JVM
-XX:TieredStopAtLevel=1   # cap at C1 — faster startup, slower peak (rare)
```

### 5.5 Deoptimization

When JIT assumptions break (e.g., monomorphic call becomes polymorphic), code **deoptimizes** back to interpreter — sudden latency spike on that code path.

**Diagnose:** JFR events `Compilation` and `Deoptimization`.

### 5.6 Measuring Warmup

```promql
# Request latency drops over first 5 minutes after pod start
histogram_quantile(0.99,
  sum(rate(http_server_requests_seconds_bucket{pod=~"$pod"}[1m])) by (le, pod)
)
```

Compare P99 **minute 1** vs **minute 10** after deploy — large gap = warmup issue.

---

## Section 6: Virtual Threads vs Platform Threads on K8s

### 6.1 Platform Threads — Traditional Model

```
1 request → 1 Tomcat thread (platform thread) → 1 OS thread → 1MB stack
```

**Limits:** ~200-400 threads practical before context switch overhead and memory bite.

**On K8s:** 200 threads × 1MB = 200MB stacks + heap per request objects.

### 6.2 Virtual Threads — Java 21 Project Loom

```
1 request → 1 virtual thread → mounted on small pool of carrier (platform) threads
```

**Benefits:**

- Millions of virtual threads possible
- Blocking I/O doesn't waste a platform thread (when not pinned)
- **Lower native memory** — fewer large stacks
- **Better fit for 512Mi-1Gi Pods** with high concurrency

```java
// Spring Boot 3.2+
spring.threads.virtual.enabled=true
```

### 6.3 When to Use Virtual Threads

| Use virtual threads | Keep platform threads |
|---------------------|----------------------|
| Blocking JDBC, blocking HTTP clients | CPU-bound work on same executor |
| Spring MVC blocking stack | Heavy synchronized blocks in hot path (pinning) |
| High concurrent I/O | Native code / JNI heavy |
| Java 21+, Tomcat 10.1+ | Libraries not yet pin-aware |

### 6.4 Pinning — The Virtual Thread Killer

When virtual thread blocks on **synchronized** or native method, it **pins** to carrier — carrier can't serve other virtual threads.

```
synchronized(lock) { blockingIO(); }  // pins carrier during blockingIO
```

**Fix:** `ReentrantLock` instead of `synchronized`, or refactor blocking calls.

**Deep dive:** [java-modern-concurrency-streams-playbook.md Section 12-14](java-modern-concurrency-streams-playbook.md).

### 6.5 Virtual Threads on Kubernetes

| Aspect | Implication |
|--------|-------------|
| Memory | Lower stack memory → can lower Pod limit or raise effective concurrency |
| CPU | Same CPU for same work — not magic for CPU-bound |
| Metrics | `jvm.threads.live` shows virtual thread count |
| Thread dumps | Virtual threads visible in dumps — many may be parked |
| `@Async` | Ensure async uses virtual thread executor |

**Don't:** Create millions of virtual threads doing CPU work — saturates carriers.

### 6.6 Migration Checklist

- [ ] Java 21+, Spring Boot 3.2+
- [ ] Audit `synchronized` in hot paths (connection pools, caches)
- [ ] JDBC driver version supports virtual threads
- [ ] Load test with pinning detection: `-Djdk.tracePinnedThreads=full`
- [ ] Thread pool configs — don't cap Tomcat at 200 if using virtual threads
- [ ] Micrometer / tracing — verify context propagation

---

## Section 7: Common Production Issues — Memory Leaks, Metaspace, GC Pauses, Thread Exhaustion

### 7.1 Heap Memory Leak

**Symptoms:**

- Old gen climbs after each GC cycle
- Full GC reclaims little
- `jvm.memory.used` heap metric stair-steps up
- Eventually OOM or OOMKilled

**Common causes in Spring:**

| Cause | Pattern |
|-------|---------|
| Static `Map` cache without eviction | Dominator tree shows huge HashMap |
| `ThreadLocal` not removed | Leak on redeploy; thread pool threads |
| HTTP client response body not closed | Byte arrays retained |
| JPA persistence context leak | Entities in long-lived session |
| Listener/subscription not unsubscribed | Observable retention |
| `ConcurrentHashMap` as unbounded cache | Custom cache without TTL |

**Fix workflow:** Heap dump → MAT dominator tree → path to GC roots → code fix → verify with allocation profile.

### 7.2 Metaspace OOM

**Symptoms:**

- `OutOfMemoryError: Metaspace`
- `jvm.memory.used` nonheap Metaspace grows without plateau
- May occur hours after deploy (class loading on traffic patterns)

**Causes:**

- CGLIB proxy explosion (too many `@Transactional` methods on same class hierarchy)
- Dynamic class generation (Groovy, ByteBuddy, Mockito in prod accidentally)
- Classloader leak (rare on K8s)
- `-XX:MaxMetaspaceSize` too low

**Fix:**

```bash
-XX:MaxMetaspaceSize=512m
jcmd <pid> VM.classloader_stats
```

### 7.3 Direct Buffer Leak

**Symptoms:**

- `OutOfMemoryError: Direct buffer memory`
- OOMKilled with healthy heap
- NMT shows Internal or Other growth

**Causes:**

- Netty `ByteBuf` not released (`release()`)
- `allocateDirect` without cleaner
- gRPC channel misconfiguration

**Fix:** Leak detection `ResourceLeakDetector` in Netty (dev), `-Dio.netty.leakDetection.level=paranoid` (never prod — overhead).

### 7.4 GC Pause Spikes

**Symptoms:**

- P99 latency spikes correlate with GC metrics
- `jvm_gc_pause_seconds_max` spikes
- Users report periodic slowness every N seconds

**Causes:**

| Cause | Fix |
|-------|-----|
| Heap too small | Increase heap or Pod memory |
| Humongous objects | Reduce allocation size; increase region size |
| Full GC | Fix leak; DisableExplicitGC carefully |
| CPU throttling | Raise CPU limit or request |
| `-XX:MaxGCPauseMillis` too aggressive | Relax or increase heap |

### 7.5 Thread Pool Exhaustion

**Symptoms:**

- Requests hang then timeout
- Thread dump: all pool threads WAITING
- `tomcat.threads.busy` = max
- Actuator: `executor.active` at cap

**Causes:**

- Downstream slow without timeout
- Pool size too small for load
- Deadlock
- Blocking call on limited pool

**Fix:**

- Timeouts on all outbound calls
- Bulkhead per dependency — [bulkhead-expert-playbook.md](bulkhead-expert-playbook.md)
- Virtual threads for I/O bound
- Right-size pool from load test

```yaml
server:
  tomcat:
    threads:
      max: 200
      min-spare: 20
```

### 7.6 Connection Pool Exhaustion (Distinct from Threads)

**Symptoms:**

- Threads WAITING on `getConnection()`
- HikariCP metrics: `hikaricp.connections.pending` > 0
- DB shows few connections but app starved

**Fix:** Increase pool cautiously, fix slow queries, separate pools per datasource, bulkhead. Section 8.

### 7.7 CPU Throttling masquerading as GC issues

High `container_cpu_cfs_throttled_seconds_total` → long GC pauses, slow JIT, slow everything.

**Fix:** Remove CPU limit or increase; not a GC tuning problem.

### 7.8 Issue Summary Matrix

| Symptom | First check | Tool |
|---------|-------------|------|
| OOMKilled 137 | cgroup limit vs NMT | describe pod, NMT |
| heap OOM | old gen trend | GC logs, heap dump |
| Metaspace OOM | nonheap metric | class histogram |
| Direct OOM | NMT Internal | direct buffer flags |
| Periodic latency | GC pause correlation | Prometheus, GC log |
| Hang under load | thread dump | jcmd Thread.print |
| Slow after deploy | warmup curve | traces by pod age |

---

## Section 8: Spring Boot Production — Actuator, Pools, HTTP Clients

### 8.1 Actuator JVM and GC Metrics

```yaml
management:
  endpoints:
    web:
      exposure:
        include: health,info,prometheus,metrics
  metrics:
    tags:
      application: ${spring.application.name}
```

**Key Micrometer metrics:**

| Metric | Meaning |
|--------|---------|
| `jvm.memory.used` | Heap/nonheap by area |
| `jvm.memory.max` | Max per pool |
| `jvm.gc.pause` | GC pause timer |
| `jvm.threads.live` | Live threads |
| `jvm.threads.states` | by state |
| `process.cpu.usage` | App CPU |
| `process.uptime` | Seconds since start |

Cross-ref [metrics-observability-playbook.md](metrics-observability-playbook.md) Sections on JVM and Prometheus queries.

```promql
# Heap pressure %
jvm_memory_used_bytes{area="heap"} / jvm_memory_max_bytes{area="heap"}

# Live threads spike
jvm_threads_live_threads
```

### 8.2 Health Probes for K8s

```yaml
management:
  endpoint:
    health:
      probes:
        enabled: true
  health:
    livenessstate:
      enabled: true
    readinessstate:
      enabled: true
```

**Liveness:** Don't include DB — only app alive.

**Readiness:** Include DB, Redis, disk space.

Cross-ref [kubernetes-expert-playbook.md Section 15.5](kubernetes-expert-playbook.md#155-actuator-health-for-k8s).

### 8.3 HikariCP — Datasource Pool Sizing

```yaml
spring:
  datasource:
    hikari:
      maximum-pool-size: 20
      minimum-idle: 5
      connection-timeout: 3000      # fail fast — don't hang threads
      idle-timeout: 600000
      max-lifetime: 1800000
      leak-detection-threshold: 60000  # dev/staging only
```

**Sizing formula:**

```
pool size ≈ (core_count × 2) + effective_spindle_count   # traditional
# For K8s: per Pod pool — total DB connections = pool × replicas
# 10 replicas × 20 pool = 200 DB connections — watch DB max_connections
```

| Metric | Alert |
|--------|-------|
| `hikaricp.connections.active` | Near max sustained |
| `hikaricp.connections.pending` | > 0 sustained |
| `hikaricp.connections.timeout` | Any increase |

**Anti-pattern:** `maximum-pool-size: 200` on every Pod — exhausts PostgreSQL `max_connections`.

### 8.4 HTTP Client Pools — RestClient, WebClient, Apache HC

**Spring 6 RestClient / Apache HttpClient 5:**

```yaml
# Prefer explicit connection pool limits
spring:
  http:
    client:
      factory: http_components
```

```java
@Bean
ConnectionManager connectionManager() {
    return PoolingHttpClientConnectionManagerBuilder.create()
        .setMaxConnTotal(50)
        .setMaxConnPerRoute(20)
        .build();
}
```

**WebClient (Reactor Netty):**

```java
ConnectionProvider provider = ConnectionProvider.builder("custom")
    .maxConnections(50)
    .pendingAcquireTimeout(Duration.ofSeconds(3))
    .build();
```

| Problem | Symptom |
|---------|---------|
| No pool limit | Too many outbound TCP + direct memory |
| Pool too small | Pending acquire timeouts |
| No timeout | Thread/virtual thread pileup |

### 8.5 Tomcat / Embedded Server Tuning

```yaml
server:
  tomcat:
    threads:
      max: 200
      min-spare: 10
    accept-count: 100
    connection-timeout: 5s
  shutdown: graceful

spring:
  lifecycle:
    timeout-per-shutdown-phase: 30s
```

**Virtual threads enabled:**

```yaml
spring:
  threads:
    virtual:
      enabled: true
# Tomcat thread max less critical — carriers default to CPU count
```

### 8.6 Redis / Lettuce Pool

```yaml
spring:
  data:
    redis:
      lettuce:
        pool:
          max-active: 16
          max-idle: 8
          min-idle: 4
```

### 8.7 Observability Integration

Export pool metrics to Prometheus — alert before exhaustion:

```promql
hikaricp_connections_pending{pool="HikariPool-1"} > 0
```

Dashboard: heap + GC + pools + thread count on one panel per service.

### 8.8 Spring Boot 3 Sample Production application.yaml (JVM + Pools)

```yaml
server:
  port: 8080
  shutdown: graceful

spring:
  lifecycle:
    timeout-per-shutdown-phase: 30s
  threads:
    virtual:
      enabled: true
  datasource:
    hikari:
      maximum-pool-size: 15
      connection-timeout: 3000

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
    distribution:
      percentiles-histogram:
        http.server.requests: true
```

---

## Section 9: Production Scenario Runbook — 25+ Scenarios

### 9.1 Pod OOMKilled — No Java Stack Trace

**Symptoms:** `Reason: OOMKilled`, exit 137; logs end abruptly.

**Diagnosis:**

```bash
kubectl describe pod <pod> | grep -A10 "Last State"
kubectl top pod <pod> --containers
# If pod restarted: check previous container metrics in Prometheus
```

**Root causes:** MaxRAMPercentage too high; direct memory; metaspace; native leak.

**Fix:** NMT at next deploy; lower MaxRAMPercentage; increase limit; MaxDirectMemorySize cap.

### 9.2 Java Heap OutOfMemoryError

**Symptoms:** `java.lang.OutOfMemoryError: Java heap space` in logs.

**Diagnosis:** GC logs show full GC with minimal reclaim; heap dump if configured.

**Fix:** Heap dump analysis; fix leak; increase heap; `ExitOnOutOfMemoryError`.

### 9.3 Metaspace OutOfMemoryError

**Symptoms:** `OutOfMemoryError: Metaspace`; often after feature deploy.

**Diagnosis:**

```bash
jcmd <pid> GC.class_histogram | wc -l   # class count
jcmd <pid> VM.metaspace
```

**Fix:** Increase MaxMetaspaceSize; reduce dynamic proxies; audit classpath.

### 9.4 Direct Buffer OutOfMemoryError

**Symptoms:** `OutOfMemoryError: Direct buffer memory`.

**Fix:** Netty leak hunt; `-XX:MaxDirectMemorySize`; fix unclosed buffers.

### 9.5 Grafana Heap Healthy but OOMKilled

**Symptoms:** `jvm.memory.used` heap 60%; Pod OOMKilled.

**Cause:** Non-heap native memory exceeded cgroup limit.

**Fix:** NMT summary; check direct, thread, metaspace; don't trust heap-only dashboards.

### 9.6 GC Pause Spikes — P99 Latency

**Symptoms:** P99 latency spikes every 10-30s; GC metrics correlate.

**Diagnosis:** GC logs; `jvm_gc_pause_seconds_max`.

**Fix:** Increase heap; tune G1; try ZGC for large heap; check CPU throttle.

### 9.7 Full GC Every Few Minutes

**Symptoms:** `Pause Full (G1 Compaction)` in logs; long pauses.

**Fix:** Leak or undersized heap; MAT analysis; increase heap; check `System.gc()`.

### 9.8 CPU Throttling — Everything Slow

**Symptoms:** High `container_cpu_cfs_throttled_seconds_total`; GC pauses long; high CPU metric but limit low.

**Fix:** Increase CPU limit or remove; raise CPU request.

### 9.9 Thread Pool Exhausted — Tomcat

**Symptoms:** 503/timeout; all `http-nio-*` threads busy or waiting.

**Diagnosis:** `jcmd Thread.print`; `tomcat.threads.busy`.

**Fix:** Downstream timeouts; bulkhead; scale replicas; virtual threads.

### 9.10 HikariCP Connection Pool Starvation

**Symptoms:** `Connection is not available, request timed out after 3000ms`.

**Diagnosis:** `hikaricp.connections.pending`; thread dumps on `getConnection`.

**Fix:** Fix slow SQL; increase pool slightly; reduce replicas' pool; read replica.

### 9.11 HTTP Client Pool Exhaustion

**Symptoms:** Outbound call timeouts; `ConnectionPoolTimeoutException`.

**Fix:** Increase `maxConnections`; reduce concurrent calls via bulkhead; fix slow peer.

### 9.12 Memory Leak — Old Gen Staircase

**Symptoms:** Old gen usage climbs over days; weekly restart "fixes" it.

**Fix:** Heap dump; MAT; fix retention; deploy fix; verify over 72h metrics.

### 9.13 Slow Cold Start After Deploy

**Symptoms:** P99 high for 5-10 min after rollout; improves without code change.

**Fix:** Startup probe; warmup traffic; CDS; increase `minReadySeconds`.

### 9.14 Rolling Deploy Causes Errors

**Symptoms:** 502/503 during deploy despite maxUnavailable: 0.

**Fix:** preStop sleep; graceful shutdown; readiness includes warm check; PDB.

Cross-ref K8s playbook Section 15.4.

### 9.15 HPA Scaled Up — New Pods OOM

**Symptoms:** Scale event → new Pods OOMKilled during warmup.

**Fix:** Lower InitialRAMPercentage; startup probe longer; pre-warm Job; raise memory limit.

### 9.16 Single Pod Memory Spike at Midnight Batch

**Symptoms:** CronJob or scheduled task causes OOM.

**Fix:** Separate Deployment for batch; larger heap; off-peak isolation; KEDA separate scaler.

### 9.17 Native Memory Growth — No Heap Leak

**Symptoms:** NMT Internal/Other grows; OOMKilled without heap OOM.

**Fix:** Identify native allocator; JNI library update; `-XX:MaxMetaspaceSize`; direct cap.

### 9.18 classloader Leak Suspect — Metaspace Creep

**Symptoms:** Metaspace grows over days in long-lived Pod (no redeploy).

**Fix:** `jcmd VM.classloader_stats`; heap dump includes class loaders; fix custom loaders.

### 9.19 Humongous Allocations — G1

**Symptoms:** Frequent full GC; `G1 Humongous Allocation` in GCCause.

**Fix:** Reduce byte[] size (streaming); increase heap; `G1HeapRegionSize`.

### 9.20 JIT Deoptimization Latency Spike

**Symptoms:** Single endpoint sporadic slow; JFR shows deoptimization.

**Fix:** Code hot path audit; avoid megamorphic calls; JVM update.

### 9.21 Virtual Thread Pinning Detected

**Symptoms:** `-Djdk.tracePinnedThreads=full` logs pinning; poor scalability.

**Fix:** Replace `synchronized` with `ReentrantLock`; update library; JDBC driver upgrade.

Cross-ref concurrency playbook Section 14.

### 9.22 File Descriptor Exhaustion

**Symptoms:** `Too many open files`; accept failures.

**Fix:** Raise `ulimit` in container; fix connection leaks; close streams.

```yaml
securityContext:
  # Or via init to set ulimit
```

### 9.23 Heap Dump Fills Disk / OOM During Dump

**Symptoms:** Pod killed during `jcmd GC.heap_dump`.

**Fix:** Dump to sized emptyDir; off-peak; smaller replica; live heap dump tools.

### 9.24 SIGKILL After SIGTERM — Shutdown Too Slow

**Symptoms:** `terminationGracePeriodSeconds` exceeded; in-flight requests killed.

**Fix:** `server.shutdown=graceful`; reduce shutdown work; Kafka consumer stop; increase grace period.

### 9.25 Multi-Container Pod — Wrong Container OOM

**Symptoms:** Pod OOMKilled; app metrics fine.

**Fix:** Check sidecar (Envoy, log agent) memory; per-container limits if supported; split sidecars.

### 9.26 ZGC High CPU After Migration

**Symptoms:** CPU usage up 20-30%; pauses fine.

**Fix:** Expected ZGC trade-off; reduce CPU limit contention; validate cost vs SLA.

### 9.27 Database Connection Storm on Scale-Up

**Symptoms:** HPA adds 10 Pods → DB rejects connections.

**Fix:** Pool size × replicas ≤ DB max; connection pooler (PgBouncer); stagger scale.

### 9.28 Reactive Stack Direct Memory on Small Pod

**Symptoms:** WebFlux + Netty on 512Mi Pod OOMKilled.

**Fix:** Increase Pod; cap direct memory; reduce Netty pools; switch MVC + virtual threads.

### 9.29 Actuator Prometheus Scraping Overhead

**Symptoms:** CPU spike every scrape; large response body.

**Fix:** Limit exposed metrics; `management.metrics.enable`; separate management port.

### 9.30 Node Memory Pressure Eviction

**Symptoms:** Pod evicted `The node was low on resource: memory`.

**Fix:** Set proper requests; avoid BestEffort; reduce node overcommit; add nodes.

Cross-ref K8s playbook Section 14.9.

---

## Section 10: Lead Interview Questions — 42+ Logical and Production Scenarios

> Scenario-based questions for Java Lead interviews. Answers are revision-length — expand with war stories.

### JVM Memory Fundamentals

**Q1: Describe the JVM memory regions relevant to production tuning.**

**A:** **Heap** (Eden, Old, Survivor) for objects. **Metaspace** for class metadata (native). **Direct memory** for NIO/Netty buffers (native). **Thread stacks** per platform thread (native). **Code cache** for JIT code (native). cgroup limit covers all — not just heap. *(Section 1)*

**Q2: What is metaspace and when does it OOM?**

**A:** Native memory for class metadata, replaces PermGen. OOM when too many classes load (proxies, dynamic codegen) or `MaxMetaspaceSize` too low. Monitor `jvm.memory.used` nonheap Metaspace. *(Section 1.3)*

**Q3: What is direct memory and why doesn't `-Xmx` limit it?**

**A:** `ByteBuffer.allocateDirect()` allocates off-heap. Default max direct ≈ max heap but separate pool. Netty/gRPC use it heavily. OOM type `Direct buffer memory`. Counts toward K8s cgroup. *(Section 1.4)*

**Q4: How do thread stacks affect K8s memory sizing?**

**A:** Each platform thread uses ~1MB native stack by default. 200 Tomcat threads ≈ 200MB outside heap. Virtual threads drastically reduce this. *(Sections 1.5, 6)*

**Q5: Explain Native Memory Tracking.**

**A:** JVM flag `NativeMemoryTracking=summary` tracks native allocations by category (heap, class, thread, code, GC). `jcmd VM.native_memory summary` when heap metrics don't explain OOMKilled. *(Section 1.7)*

### Container-Aware JVM and K8s

**Q6: Why did Java apps OOMKilled on K8s before container-aware JVM?**

**A:** JVM read host RAM for default max heap, not cgroup limit. 512Mi Pod could spawn multi-GB heap default. Fixed in Java 8u191+/11+ with cgroup awareness. *(Section 2.1)*

**Q7: Explain MaxRAMPercentage.**

**A:** Sets max heap as percentage of **container memory limit**. `75.0` on 1Gi limit ≈ 768Mi heap. Preferred over hardcoded `-Xmx` on K8s. *(Section 2.2)*

**Q8: OOMKilled exit 137 vs Java OutOfMemoryError — difference?**

**A:** **137/OOMKilled** — Linux cgroup killed process; total memory exceeded limit; may be no Java stack trace. **Java OOM** — JVM throws inside process for specific pool (heap, metaspace, direct). *(Section 2.4)*

**Q9: How do you size memory for Spring Boot on K8s?**

**A:** Limit ≥ heap (MaxRAMPercentage) + metaspace + direct + stacks + margin. Set requests from steady working set. Actuator metrics + NMT validate. Cross-ref K8s playbook Section 15. *(Sections 2.8, 8)*

**Q10: Should you set CPU limits for Java on K8s?**

**A:** Contentious. Limits cause throttling during GC spikes. Many teams set CPU request without limit, or limit = 2× request. Monitor `container_cpu_cfs_throttled_seconds_total`. *(Section 2.6)*

**Q11: What is ExitOnOutOfMemoryError and why use it?**

**A:** JVM exits on any OOM → K8s restarts Pod → alerts fire. Prevents limping degraded state. Pair with PDB and circuit breakers. *(Section 2.8)*

**Q12: Guaranteed QoS vs Burstable for Java workloads?**

**A:** Guaranteed (limit=request) — predictable, no burst room for GC spikes. Burstable (limit>request) — spike headroom but can still OOMKilled above limit. Size limit for peak GC + allocation. *(Section 2.9)*

### Garbage Collection

**Q13: When is G1 the right choice?**

**A:** Default for Spring Boot. Heap 512MB-32GB. Balanced latency/throughput. Team knows G1. Most K8s Java workloads. *(Section 3.2)*

**Q14: When would you switch to ZGC?**

**A:** Pause-sensitive SLA with G1 tuning exhausted; heap ≥ 4GB; Java 21+ generational ZGC. Not for tiny 512Mi Pods — CPU overhead. *(Section 3.3)*

**Q15: How do you read `Pause Young (Normal) 512M->180M(768M) 28ms`?**

**A:** Young GC; heap before 512M after 180M; max 768M; **28ms STW pause**. Focus on pause ms and post-GC heap trend. *(Section 3.6)*

**Q16: What does frequent Full GC indicate?**

**A:** Bad — often leak or undersized heap. Full GC 750M→748M means almost nothing reclaimed. Heap dump and fix leak or increase heap. *(Section 3.8)*

**Q17: How do you monitor GC in production?**

**A:** Micrometer `jvm.gc.pause`; Prometheus recording rules; alert GC time > 5% CPU; GC logs to centralized logging for post-incident. Metrics playbook. *(Section 3.10)*

**Q18: What causes G1 humongous allocations?**

**A:** Objects > half G1 region size go to humongous regions. Large byte arrays, big JSON payloads. Can trigger full GC. Fix: stream/chunk; increase region size or heap. *(Section 9.19)*

### Profiling and Diagnostics

**Q19: How do you diagnose a memory leak in production?**

**A:** Confirm old gen stair-step in metrics → heap dump (off-peak) → MAT dominator tree → path to GC roots → allocation profile async-profiler `-e alloc` → fix → verify 72h. *(Sections 4, 7.1)*

**Q20: jcmd vs async-profiler?**

**A:** **jcmd** — lightweight diagnostics (heap info, thread dump, NMT, class histogram). **async-profiler** — CPU/allocation/wall flame graphs for hot path identification. Both production-safe in short bursts. *(Section 4)*

**Q21: Risks of heap dump in production?**

**A:** STW pause during dump; file size ≈ heap size; can OOMKill if limit tight; disk fill. Do on replica, off-peak, or sized emptyDir. *(Section 4.4)*

**Q22: How do you take a thread dump on K8s?**

**A:** `kubectl exec jcmd <pid> Thread.print` or `kill -3 <pid>` (SIGQUIT). Analyze WAITING/BLOCKED states and pool exhaustion. *(Section 4.5)*

### JIT and Warmup

**Q23: Why is the app slow for 5 minutes after deploy?**

**A:** JIT warmup — interpreter/C1 until hot methods compile to C2. Class loading at Spring startup. Fix: startup probe, synthetic warmup, CDS, GraalVM native for instant peak trade-off. *(Section 5)*

**Q24: What is tiered compilation?**

**A:** C1 fast compile → C2 aggressive optimization for hot methods. Default on server JVM. Cold code slow; hot code native speed. *(Section 5.4)*

### Virtual Threads

**Q25: Virtual threads vs platform threads for Spring on K8s?**

**A:** Virtual threads — millions of cheap threads, low stack memory, great for blocking I/O on small Pods. Platform — one OS thread per request, stack memory multiplies. Java 21 + Spring Boot 3.2+. Concurrency playbook Sections 12-14. *(Section 6)*

**Q26: What is pinning with virtual threads?**

**A:** Virtual thread blocks on `synchronized` or native code — pins to carrier platform thread. Reduces scalability. Fix: `ReentrantLock`, update libraries. `jdk.tracePinnedThreads`. *(Section 6.4)*

### Production Issues

**Q27: Thread pool exhaustion vs connection pool exhaustion?**

**A:** **Thread** — all Tomcat threads busy/waiting; fix timeouts, bulkhead, scale. **Connection** — threads WAITING on `getConnection()`; fix slow SQL, pool size, DB limits. Different metrics. *(Sections 7.5, 7.6)*

**Q28: How does CPU throttling affect GC?**

**A:** GC threads compete for cgroup CPU quota → longer pauses → latency spikes → more load. Fix CPU limit, not GC first. *(Sections 2.6, 7.7)*

**Q29: Grafana shows 60% heap but Pod OOMKilled — explain.**

**A:** Heap is only part of cgroup accounting. Metaspace, direct buffers, stacks, native libraries count. NMT reveals non-heap consumers. *(Section 9.5)*

### Spring Boot Specific

**Q30: Key Actuator metrics for JVM tuning?**

**A:** `jvm.memory.used/max`, `jvm.gc.pause`, `jvm.threads.live`, `hikaricp.connections.*`, `http.server.requests`, `process.cpu.usage`. Metrics playbook. *(Section 8.1)*

**Q31: How do you size HikariCP on K8s with 10 replicas?**

**A:** `pool × replicas ≤ DB max_connections`. Per-Pod pool modest (10-20). Use PgBouncer. Monitor `hikaricp.connections.pending`. *(Section 8.3)*

**Q32: Why connection-timeout on HikariCP?**

**A:** Fail fast in 3s instead of hanging thread indefinitely on pool starvation. Critical with finite thread pools. *(Section 8.3)*

**Q33: Graceful shutdown on K8s — what must align?**

**A:** `server.shutdown=graceful`, `spring.lifecycle.timeout-per-shutdown-phase`, `preStop` sleep, `terminationGracePeriodSeconds`, readiness fails on shutdown. K8s Section 15.4. *(Section 9.14)*

### Scenario-Based

**Q34: Walk through OOMKilled investigation.**

**A:** `describe pod` confirm 137 → check heap vs limit metrics → NMT summary → GC logs → class histogram → direct buffer suspects → adjust MaxRAMPercentage or fix leak → load test verify. *(Section 9.1)*

**Q35: P99 latency spikes every 20 seconds — investigate?**

**A:** Correlate with GC pause metrics and logs. If yes — heap tuning or ZGC. If no — check CPU throttle, downstream timeout, lock contention, thread dump. *(Section 9.6)*

**Q36: After HPA scale-up, DB returns "too many connections".**

**A:** Pool size × new replica count exceeded PostgreSQL `max_connections`. Reduce per-Pod pool, add PgBouncer, limit HPA max, stagger scale. *(Section 9.27)*

**Q37: How would you profile CPU hot path in prod without restart?**

**A:** async-profiler attach 60s `-f cpu.html`. Or JFR via `jcmd JFR.start`. Copy artifact out, flame graph analysis. *(Section 4.3)*

**Q38: Metaspace OOM after adding dynamic feature flags library?**

**A:** Library generates classes per flag combination. Increase MaxMetaspaceSize; cache generated classes; audit library config. *(Section 7.2)*

**Q39: Netty reactive service OOMKilled on 512Mi — approach?**

**A:** Direct memory + heap exceed limit. Increase Pod; `MaxDirectMemorySize`; reduce Netty pool; consider MVC + virtual threads. *(Section 9.28)*

**Q40: Difference between bulkhead and bigger thread pool?**

**A:** Bigger pool — one slow dependency can still exhaust all threads. Bulkhead caps per-dependency concurrency — failure isolated. Bulkhead playbook. *(bulkhead-expert-playbook.md)*

**Q41: VPA recommends 2Gi memory — safe to apply?**

**A:** VPA restart changes cgroup limit → JVM rescales heap on boot. Test in staging; watch cold start; may need warmup; coordinate with HPA. K8s Section 10.

**Q42: What JVM flags do you set by default on K8s?**

**A:** `MaxRAMPercentage=70-75`, `UseG1GC`, `ExitOnOutOfMemoryError`, `HeapDumpOnOutOfMemoryError` + path, `NativeMemoryTracking=summary`, optional `MaxMetaspaceSize`. No hardcoded `-Xmx`. *(Sections 2.7, 2.10)*

**Q43: How does JVM performance relate to observability playbook?**

**A:** Export JVM metrics via Micrometer → Prometheus → alert on heap, GC, pools. USE method on saturation (pool pending). RED on latency correlated with GC. Metrics playbook Sections on JVM queries.

**Q44: Native image vs JVM on K8s — trade-off?**

**A:** Native — fast start, low memory, no JIT warmup; build complexity, reflection config, peak throughput may differ. JVM — mature tuning, GC choice, easier debug. K8s Section 15.6.

---

## Section 11: How to Talk About JVM Performance in an Interview

> Plain English. Last-minute revision the night before.

---

### "What should I know about JVM memory on Kubernetes?"

The container memory limit is not just the Java heap. The heap is only part of what the process uses. Metaspace, network buffers, and thread stacks all count toward the same limit. If you set MaxRAMPercentage too high, the kernel kills your Pod with OOMKilled even when the heap chart looks fine.

---

### "What's the first flag you set for Java on K8s?"

`MaxRAMPercentage` around 70-75% so the heap scales with the container limit. Remove old hardcoded `-Xmx` from Dockerfiles. Add `ExitOnOutOfMemoryError` so the Pod restarts cleanly instead of limping.

---

### "G1 or ZGC?"

Start with G1 — it's the default and works for most Spring Boot services. Switch to ZGC if you have a large heap (4GB+) and GC pause times are breaking your latency SLA after you've already tuned G1.

---

### "How do you find a memory leak?"

Watch the old generation climb over hours and stay high after GC. Take a heap dump, open it in MAT, find the biggest objects retaining memory, trace who's holding references, fix the code, deploy, and confirm the metric flatlines over days.

---

### "Why is the app slow right after deploy?"

Cold JIT. The JVM runs bytecode in the interpreter first, then compiles hot paths over minutes. Use a startup probe so Kubernetes doesn't kill the Pod during warmup, and optionally hit hot endpoints before marking ready.

---

### "Virtual threads — when?"

When you're on Java 21 with blocking I/O (classic Spring MVC, JDBC) and want high concurrency without hundreds of platform thread stacks eating native memory. Watch for pinning on `synchronized` blocks.

---

### Quick Answers (Revision Table)

| Question | Say this |
|----------|----------|
| OOMKilled vs heap OOM? | 137 = cgroup killed total memory; heap OOM = Java throws for heap |
| Size 1Gi Pod heap? | ~70% MaxRAMPercentage ≈ 700Mi heap; leave room for non-heap |
| Default GC? | G1 for most; ZGC for large heap + strict pause SLA |
| Read GC log line? | Focus on pause ms and heap before→after |
| Full GC often? | Leak or heap too small — dump and analyze |
| Thread dump tool? | `jcmd Thread.print` — look for all threads WAITING |
| Profile CPU in prod? | async-profiler 60s flame graph or JFR |
| Metaspace OOM? | Too many classes — proxies, codegen, raise cap |
| Direct memory OOM? | Netty/gRPC buffers — off-heap, not in heap chart |
| Slow after rollout? | JIT warmup — startup probe, warm traffic |
| Pool exhausted? | Timeouts on outbound; bulkhead; right-size pool × replicas |
| CPU limit hurt Java? | Throttling lengthens GC pauses — watch throttle metric |
| Best K8s JVM flags? | MaxRAMPercentage, G1, ExitOnOOM, NMT summary |
| Virtual vs platform? | Virtual = cheap concurrency; platform = 1MB stack each |
| Debug native OOM? | NMT summary — categories show metaspace/direct/thread |

---

## Section 12: Appendix — JVM Flags Cheat Sheet, Decision Trees, Quick Reference

### 12.1 Production JVM Flags (K8s Spring Boot)

```bash
JAVA_TOOL_OPTIONS="
  -XX:MaxRAMPercentage=75.0
  -XX:InitialRAMPercentage=50.0
  -XX:MaxMetaspaceSize=256m
  -XX:MaxDirectMemorySize=256m
  -XX:+UseG1GC
  -XX:MaxGCPauseMillis=200
  -XX:+ExitOnOutOfMemoryError
  -XX:+HeapDumpOnOutOfMemoryError
  -XX:HeapDumpPath=/tmp/heapdumps
  -XX:NativeMemoryTracking=summary
  -Xlog:gc*:file=/tmp/gc.log:utctime,level,tags:filecount=5,filesize=50M
"
```

### 12.2 ZGC Alternative Block

```bash
JAVA_TOOL_OPTIONS="
  -XX:MaxRAMPercentage=75.0
  -XX:+UseZGC
  -XX:+ZGenerational
  -XX:+ExitOnOutOfMemoryError
  -XX:NativeMemoryTracking=summary
"
```

### 12.3 jcmd Quick Reference

```bash
jcmd <pid> help
jcmd <pid> GC.heap_info
jcmd <pid> GC.class_histogram
jcmd <pid> GC.heap_dump /tmp/heap.hprof
jcmd <pid> Thread.print
jcmd <pid> VM.native_memory summary scale=MB
jcmd <pid> VM.version
jcmd <pid> JFR.start duration=60s filename=/tmp/rec.jfr settings=profile
```

### 12.4 Prometheus Alerts (Starter)

```yaml
# Heap > 85% sustained
- alert: JvmHeapPressureHigh
  expr: |
    (jvm_memory_used_bytes{area="heap"} / jvm_memory_max_bytes{area="heap"}) > 0.85
  for: 10m

# GC consuming > 10% wall time
- alert: JvmGcPressureHigh
  expr: rate(jvm_gc_pause_seconds_sum[5m]) > 0.10
  for: 5m

# HikariCP pending connections
- alert: HikariPoolPending
  expr: hikaricp_connections_pending > 0
  for: 2m

# Pod OOMKilled
- alert: PodOOMKilled
  expr: kube_pod_container_status_last_terminated_reason{reason="OOMKilled"} == 1
```

Cross-ref [metrics-observability-playbook.md](metrics-observability-playbook.md).

### 12.5 Memory Sizing Decision Tree

```
Workload type?
├─ Blocking MVC + JDBC
│   ├─ Java 21+ → virtual threads + 1Gi Pod + MaxRAMPercentage 70%
│   └─ Java 17 → platform threads + 1.25Gi Pod + MaxRAMPercentage 65%
├─ WebFlux/Reactive Netty
│   └─ 1.5Gi+ Pod + MaxDirectMemorySize cap + monitor direct
├─ Batch/CronJob
│   └─ Separate Deployment + larger heap + Parallel GC optional
└─ Native image
    └─ Small limit (256-512Mi) + no JVM flags + fast start
```

### 12.6 GC Selection Decision Tree

```
Heap size?
├─ < 2GB → G1 (default)
├─ 2-4GB → G1; try ZGC if pause SLA missed
└─ > 4GB + pause-sensitive → ZGenerational (Java 21+)

Pause OK?
├─ Yes → G1 default tuning
└─ No → GC logs → tune G1 → ZGC → increase heap
```

### 12.7 OOM Investigation Decision Tree

```
OOM type?
├─ OOMKilled 137
│   ├─ NMT summary
│   ├─ Lower MaxRAMPercentage OR increase limit
│   └─ Check direct + metaspace + stacks
├─ heap space
│   ├─ GC logs (full GC?)
│   └─ heap dump → MAT
├─ Metaspace
│   ├─ class histogram count
│   └─ MaxMetaspaceSize + fix class leak
└─ Direct buffer memory
    ├─ Netty/gRPC audit
    └─ MaxDirectMemorySize
```

### 12.8 Cross-Reference Map

| Topic | Playbook | Section |
|-------|----------|---------|
| K8s memory limits, probes, preStop | kubernetes-expert-playbook.md | Section 15 |
| K8s OOMKilled runbook | kubernetes-expert-playbook.md | Section 14.2 |
| Micrometer JVM metrics | metrics-observability-playbook.md | JVM/GC queries |
| Thread pool isolation | bulkhead-expert-playbook.md | Sections 5-6 |
| Virtual threads, pinning | java-modern-concurrency-streams-playbook.md | Sections 12-14 |
| HPA scaling Java | kubernetes-expert-playbook.md | Section 10 |
| Graceful shutdown | kubernetes-expert-playbook.md | Section 15.4 |
| Connection pool bulkhead | bulkhead-expert-playbook.md | Section 6 |

### 12.9 Useful kubectl + JVM Commands

```bash
# Pod memory and CPU
kubectl top pod -l app=order-service --containers

# OOM check
kubectl describe pod <pod> | grep -E "OOM|Last State|Exit"

# Exec into JVM
kubectl exec -it <pod> -- bash
jcmd 1 GC.heap_info
jcmd 1 VM.native_memory summary scale=MB
jcmd 1 Thread.print | head -100

# Ephemeral debug with JDK
kubectl debug -it <pod> --image=eclipse-temurin:21 --target=app -- bash

# Copy heap dump out
kubectl cp <pod>:/tmp/heap.hprof ./heap.hprof
```

### 12.10 Spring Boot Memory Checklist (Pre-Production)

- [ ] `MaxRAMPercentage` set; no conflicting `-Xmx`
- [ ] Memory request/limit based on load test NMT
- [ ] `ExitOnOutOfMemoryError` enabled
- [ ] HikariCP `connection-timeout` ≤ 3s
- [ ] HTTP client pool limits configured
- [ ] Actuator prometheus + key JVM metrics
- [ ] GC logs or JFR strategy defined
- [ ] Startup probe for JIT warmup
- [ ] Graceful shutdown + preStop aligned
- [ ] Heap dump path on writable volume with size limit
- [ ] Pool size × max replicas ≤ DB connection limit
- [ ] Bulkhead on critical downstream calls
- [ ] Alerts: heap pressure, GC time, pool pending, OOMKilled

---

*End of JVM Performance & Tuning Expert Revision Playbook. Pair with [kubernetes-expert-playbook.md](kubernetes-expert-playbook.md) Section 15, [metrics-observability-playbook.md](metrics-observability-playbook.md), [bulkhead-expert-playbook.md](bulkhead-expert-playbook.md), and [java-modern-concurrency-streams-playbook.md](java-modern-concurrency-streams-playbook.md) for full production Java-on-K8s coverage.*
