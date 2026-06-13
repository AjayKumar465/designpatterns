# LRU Cache — Expert Playbook (Lead/Architect, Java, 10+ Years)

A comprehensive reference for the Least Recently Used cache — from data structure internals to production caching with Caffeine. Includes runnable examples, complexity analysis, thread-safety patterns, and 25+ lead-level interview Q&As.

---

## Table of Contents

1. [What Is an LRU Cache?](#1-what-is-an-lru-cache)
2. [When to Use LRU — and When Not To](#2-when-to-use-lru--and-when-not-to)
3. [Core Operations and Complexity](#3-core-operations-and-complexity)
4. [Data Structure Design — HashMap + Doubly Linked List](#4-data-structure-design--hashmap--doubly-linked-list)
5. [Full Java Implementation (From Scratch)](#5-full-java-implementation-from-scratch)
6. [Step-by-Step Trace (Capacity = 5)](#6-step-by-step-trace-capacity--5)
7. [LinkedHashMap — The Built-In Shortcut](#7-linkedhashmap--the-built-in-shortcut)
8. [Production Caching — Caffeine and Guava](#8-production-caching--caffeine-and-guava)
9. [Thread Safety in Production](#9-thread-safety-in-production)
10. [LRU vs LFU vs FIFO vs TTL](#10-lru-vs-lfu-vs-fifo-vs-ttl)
11. [Spring Boot Integration Patterns](#11-spring-boot-integration-patterns)
12. [Sizing, Memory, and Eviction Tuning](#12-sizing-memory-and-eviction-tuning)
13. [Common Interview Variants](#13-common-interview-variants)
14. [Production Pitfalls and Failure Modes](#14-production-pitfalls-and-failure-modes)
15. [Metrics and Observability](#15-metrics-and-observability)
16. [Production Issue Runbook](#16-production-issue-runbook)
17. [Lead Interview Questions & Answers](#17-lead-interview-questions--answers)
18. [How to Talk About LRU Cache in an Interview](#18-how-to-talk-about-lru-cache-in-an-interview)

---

## 1. What Is an LRU Cache?

**LRU = Least Recently Used.**

A fixed-capacity cache that evicts the item that has not been accessed for the longest time when space is needed.

| Operation | Behavior |
|---|---|
| `get(key)` | Return value if present; mark key as **most recently used** |
| `put(key, value)` | Insert or update; mark as most recently used; evict LRU if over capacity |

**Why LRU works:** Programs often exhibit **temporal locality** — recently accessed data is likely to be accessed again soon.

**Repo example:** `examples/lru/LruCacheDemo.java` — capacity 5, full trace with eviction.

---

## 2. When to Use LRU — and When Not To

### Use LRU when

- Cache size is bounded (memory limit)
- Access patterns show temporal locality
- You want simple, predictable eviction
- Examples: DB query results, API response cache, session token lookup, computed aggregates

### Do NOT use hand-rolled LRU when

- You need **production-grade** caching → use **Caffeine** or **Guava Cache**
- Access pattern is **frequency-based** (hot keys accessed rarely but repeatedly) → consider **LFU**
- You need **TTL/expiry** per entry → Caffeine with `expireAfterWrite`
- Cache must be **distributed across nodes** → Redis, Hazelcast, not local LRU

### Decision matrix

| Need | Choice |
|---|---|
| Interview / learning | HashMap + doubly linked list |
| Single JVM production cache | Caffeine |
| Distributed cache | Redis with TTL |
| Spring `@Cacheable` | Caffeine provider |

---

## 3. Core Operations and Complexity

| Operation | Time | Space |
|---|---|---|
| `get(key)` | O(1) | — |
| `put(key, value)` | O(1) | — |
| `evict()` | O(1) | — |
| Total space | — | O(capacity) |

**Requirement:** Both lookup **and** order update must be O(1). That is why we combine HashMap with a doubly linked list.

---

## 4. Data Structure Design — HashMap + Doubly Linked List

```
HashMap<K, Node>          Doubly Linked List (MRU ← → LRU)
┌─────────────┐           head ↔ [C] ↔ [B] ↔ [A] ↔ tail
│ key1 → Node │───┐
│ key2 → Node │───┼──→  Node has: key, value, prev, next
│ key3 → Node │───┘
└─────────────┘

head side = most recently used (MRU)
tail side = least recently used (LRU) ← evict from here
```

**Sentinel nodes** (`head`, `tail` dummy nodes) eliminate edge-case branching when list is empty or has one element.

**Why doubly linked?** Moving a node to front requires removing it from its current position — O(1) with `prev` and `next` pointers. Singly linked list cannot remove an arbitrary node without traversing from head.

**Why not Queue + HashMap?** Standard queue cannot move an element from middle to front on `get()` in O(1).

---

## 5. Full Java Implementation (From Scratch)

From `examples/lru/LruCacheDemo.java`:

```java
public final class LruCache<K, V> {

    private static final class Node<K, V> {
        K key;
        V value;
        Node<K, V> prev;
        Node<K, V> next;
    }

    private final int capacity;
    private final Map<K, Node<K, V>> index;
    private final Node<K, V> head = new Node<>();
    private final Node<K, V> tail = new Node<>();

    public LruCache(int capacity) {
        if (capacity <= 0) throw new IllegalArgumentException("capacity must be > 0");
        this.capacity = capacity;
        this.index = new HashMap<>();
        head.next = tail;
        tail.prev = head;
    }

    public V get(K key) {
        Node<K, V> node = index.get(key);
        if (node == null) return null;
        moveToFront(node);
        return node.value;
    }

    public void put(K key, V value) {
        Node<K, V> existing = index.get(key);
        if (existing != null) {
            existing.value = value;
            moveToFront(existing);
            return;
        }
        Node<K, V> created = new Node<>();
        created.key = key;
        created.value = value;
        index.put(key, created);
        addAfterHead(created);
        if (index.size() > capacity) {
            Node<K, V> lru = tail.prev;
            removeNode(lru);
            index.remove(lru.key);
        }
    }

    private void moveToFront(Node<K, V> node) {
        removeNode(node);
        addAfterHead(node);
    }

    private void addAfterHead(Node<K, V> node) {
        node.next = head.next;
        node.prev = head;
        head.next.prev = node;
        head.next = node;
    }

    private void removeNode(Node<K, V> node) {
        node.prev.next = node.next;
        node.next.prev = node.prev;
    }
}
```

---

## 6. Step-by-Step Trace (Capacity = 5)

Starting empty:

| Step | Operation | List (MRU → LRU) | Evicted |
|---|---|---|---|
| 1 | put(1,A) | [1] | — |
| 2 | put(2,B) | [2,1] | — |
| 3 | put(3,C) | [3,2,1] | — |
| 4 | put(4,D) | [4,3,2,1] | — |
| 5 | put(5,E) | [5,4,3,2,1] | — |
| 6 | get(2) | [2,5,4,3,1] | — |
| 7 | get(4) | [4,2,5,3,1] | — |
| 8 | put(6,F) | [6,4,2,5,3] | **1** |
| 9 | put(7,G) | [7,6,4,2,5] | **3** |
| 10 | get(5) | [5,7,6,4,2] | — |
| 11 | put(8,H) | [8,5,7,6,4] | **2** |

Final keys present: **8, 5, 7, 6, 4**

Run locally:

```bash
javac examples/lru/LruCacheDemo.java
java -cp examples/lru LruCacheDemo
```

---

## 7. LinkedHashMap — The Built-In Shortcut

Java's `LinkedHashMap` with `accessOrder=true` maintains access order internally.

```java
public final class LruCacheLinkedHashMap<K, V> extends LinkedHashMap<K, V> {

    private final int maxSize;

    public LruCacheLinkedHashMap(int maxSize) {
        super(maxSize, 0.75f, true);  // accessOrder = true
        this.maxSize = maxSize;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
        return size() > maxSize;
    }
}
```

**Repo:** `examples/lru/LruCacheLinkedHashMapDemo.java`

| Approach | Pros | Cons |
|---|---|---|
| HashMap + DLL | Full control, interview standard | More code |
| LinkedHashMap | ~10 lines, battle-tested | Less flexible for custom eviction |

---

## 8. Production Caching — Caffeine and Guava

**Never ship hand-rolled LRU in production** unless you have a very specific reason.

### Caffeine (recommended)

```java
@Bean
public Cache<String, Product> productCache() {
    return Caffeine.newBuilder()
        .maximumSize(10_000)
        .expireAfterWrite(Duration.ofMinutes(10))
        .recordStats()
        .build();
}

// Usage
Product p = productCache.get(key, k -> productRepository.findById(k));
```

### Spring `@Cacheable`

```java
@Configuration
@EnableCaching
public class CacheConfig {
    @Bean
    public CacheManager cacheManager() {
        CaffeineCacheManager manager = new CaffeineCacheManager("products", "users");
        manager.setCaffeine(Caffeine.newBuilder()
            .maximumSize(5000)
            .expireAfterWrite(Duration.ofMinutes(5))
            .recordStats());
        return manager;
    }
}

@Service
public class ProductService {
    @Cacheable(value = "products", key = "#id")
    public Product findById(String id) {
        return productRepository.findById(id);
    }
}
```

---

## 9. Thread Safety in Production

The demo `LruCache` is **not thread-safe**.

### Option 1: Synchronized wrapper

```java
public final class SynchronizedLruCache<K, V> {
    private final LruCache<K, V> delegate = new LruCache<>(capacity);

    public synchronized V get(K key) { return delegate.get(key); }
    public synchronized void put(K key, V value) { delegate.put(key, value); }
}
```

Simple but **one lock** — bottleneck under high concurrency.

### Option 2: Caffeine (built-in concurrent)

Caffeine uses lock striping and is designed for multi-threaded access.

### Option 3: ReadWriteLock

Rarely needed if using Caffeine.

---

## 10. LRU vs LFU vs FIFO vs TTL

| Policy | Evicts | Best when |
|---|---|---|
| **LRU** | Least recently accessed | Temporal locality |
| **LFU** | Least frequently accessed | Hot keys stay long-term |
| **FIFO** | Oldest inserted | Simple streaming buffers |
| **TTL** | Expired by time | Data freshness matters |

Caffeine uses **Window TinyLFU** — combines LRU recency with LFU frequency. Better than pure LRU for many real workloads.

---

## 11. Spring Boot Integration Patterns

### Cache-aside (lazy load)

```java
public Product getProduct(String id) {
    return cache.get(id, productId -> {
        metrics.recordCacheMiss();
        return repository.findById(productId);
    });
}
```

### Cache invalidation on write

```java
@CacheEvict(value = "products", key = "#product.id")
public void updateProduct(Product product) {
    repository.save(product);
}
```

### Do not cache

- User-specific sensitive data without encryption
- Data that changes every request
- Large objects that cause GC pressure

---

## 12. Sizing, Memory, and Eviction Tuning

```
maxSize = (available_heap_for_cache) / average_entry_size
```

Example: 512MB budget, ~2KB per entry → ~250,000 entries max.

**Watch for:**

- Cache holding references to large object graphs
- `maximumWeight` instead of `maximumSize` when entry sizes vary
- Soft/weak references — GC interaction, unpredictable evictions

---

## 13. Common Interview Variants

| Problem | Twist |
|---|---|
| LRU Cache (LeetCode 146) | Standard get/put O(1) |
| LFU Cache (LeetCode 460) | Track frequency, O(1) |
| LRU with TTL | Add timestamp per node |
| Thread-safe LRU | Locks or concurrent structure |
| Distributed LRU | Redis `maxmemory-policy allkeys-lru` |

---

## 14. Production Pitfalls and Failure Modes

| Pitfall | Impact | Fix |
|---|---|---|
| Cache stampede | Many threads miss cache, hammer DB | Single-flight / `@Cacheable(sync=true)` |
| Stale data served | Users see old prices | TTL + invalidation on write |
| Unbounded cache | OOM | Always set `maximumSize` or `maximumWeight` |
| Caching null | NullPointer or wasted slots | Use `Optional` or `NullValue` sentinel |
| Wrong key | Cross-tenant data leak | Include tenantId in cache key |
| No metrics | Cannot tune hit rate | `recordStats()` + Micrometer |

---

## 15. Metrics and Observability

Caffeine exposes stats:

```java
CacheStats stats = cache.stats();
// hitRate(), missRate(), evictionCount(), loadSuccessCount()
```

Micrometer binder:

```java
@Bean
public MeterBinder caffeineMetrics(Cache<String, Product> cache) {
    return registry -> CaffeineCacheMetrics.monitor(registry, cache, "productCache");
}
```

Alert on:

- Hit rate drops below threshold (cache not helping)
- Eviction rate spikes (cache too small)
- Load time p99 high (DB slow on miss)

---

## 16. Production Issue Runbook

### Symptom: Hit rate collapsed after deploy

1. Check if cache key format changed
2. Check if `@Cacheable` still active (AOP proxy issue?)
3. Check if cache was cleared on restart (expected for local cache)

### Symptom: OOM after adding cache

1. Check `maximumSize` is set
2. Profile average entry size
3. Switch to `maximumWeight` with weigher

### Symptom: Users see stale data

1. Check TTL configuration
2. Verify `@CacheEvict` on write paths
3. Consider shorter TTL for critical data

---

## 17. Lead Interview Questions & Answers

**Q1: Design an LRU cache with O(1) get and put.**

**A**: HashMap for O(1) key lookup to a node. Doubly linked list for O(1) move-to-front and O(1) eviction from tail. Sentinels at head/tail simplify edge cases. *(Sections 4–5)*

**Q2: Why doubly linked list, not singly linked?**

**A**: On `get(key)`, we must move an existing node to the front. That requires removing it from its current position knowing only the node reference. Doubly linked gives O(1) removal. Singly linked requires finding the predecessor — O(n). *(Section 4)*

**Q3: Would you use LinkedHashMap in production?**

**A**: For a quick internal utility, maybe. For production caching, no — use Caffeine. It adds concurrency, stats, TTL, weight-based eviction, and Window TinyLFU which beats pure LRU on real workloads. *(Sections 7–8)*

**Q4: How do you make LRU thread-safe?**

**A**: Simplest: synchronize the whole cache. Better: use Caffeine which is concurrent by design. Avoid fine-grained locking on hand-rolled LRU unless you have measured need. *(Section 9)*

**Q5: LRU vs LFU — when does LRU fail?**

**A**: Scan patterns — one full scan evicts all hot data because every key is "recently" touched once. LFU keeps truly hot keys. Example: periodic batch job reading entire catalog evicts user session cache entries. *(Section 10)*

**Q6: What is cache stampede and how do you prevent it?**

**A**: Many threads miss cache simultaneously and all hit the DB. Fix: single-flight loading (Caffeine `AsyncCache`), `@Cacheable(sync=true)`, or probabilistic early expiration. *(Section 14)*

**Q7: How do you cache null values?**

**A**: Either don't cache nulls, or use a sentinel `NullValue` object. Guava and Caffeine support `Optional`-like patterns. Caching null prevents repeated DB lookups for missing keys. *(Section 14)*

**Q8: How do you invalidate cache on update?**

**A**: `@CacheEvict` on write methods, or explicit `cache.invalidate(key)` after DB commit. For distributed: publish invalidation event via Redis pub/sub or Kafka. *(Section 11)*

**Q9: What metrics do you track for a production cache?**

**A**: Hit rate, miss rate, eviction count, load time (p99), size, weight. Alert on hit rate drop and eviction spikes. *(Section 15)*

**Q10: Implement LRU with TTL.**

**A**: Add `expiresAt` per node. On get/put, check expiry. Optionally lazy expiry on access + periodic cleanup thread. Caffeine: `expireAfterWrite()`. *(Section 13)*

**Q11: How do you size an LRU cache in production?**

**A**: Start with working-set estimate (unique keys accessed in a window). Monitor hit rate and eviction rate. Increase size until hit rate plateaus and memory stays within budget. Use `maximumWeight` if entry sizes vary. *(Sections 12, 15)*

**Q12: Redis LRU vs local Caffeine — when to use which?**

**A**: Local Caffeine for single-node, sub-millisecond, high-QPS hot data. Redis when multiple app instances must share cache or you need distributed invalidation. Redis eviction is approximate LRU (`maxmemory-policy allkeys-lru`). *(Sections 8, 11)*

**Q13: Explain Spring `@Cacheable` pitfalls.**

**A**: Self-invocation bypasses proxy (no cache). Default key generation can collide. No cache unless `@EnableCaching`. For `@Cacheable(sync=true)` prevents stampede on one key. Always pair writes with `@CacheEvict`. *(Section 11)*

**Q14: What causes memory leaks with caches?**

**A**: Unbounded caches, caching large objects with strong references to request-scoped data, ThreadLocal + cache holding per-user entries forever, static maps mistaken for LRU. Fix: bounded size, TTL, weak keys where appropriate. *(Section 14)*

**Q15: Design a distributed cache invalidation strategy.**

**A**: Write-through or write-around to DB first, then publish invalidation event (Kafka/Redis pub-sub). Consumers call `cache.invalidate(key)`. Prefer event-driven over TTL-only for correctness-sensitive data. *(Section 11)*

**Q16: What is Window TinyLFU and why does Caffeine use it?**

**A**: Caffeine's eviction policy approximates frequency in a recent window plus admission filter. It resists one-time scan pollution better than pure LRU while staying O(1). *(Section 8)*

**Q17: How do you handle cache warming on deploy?**

**A**: Preload hot keys from DB on startup (background thread), or rely on gradual fill with monitoring. Avoid blocking startup. For critical paths, warm top-N keys from analytics. *(Section 12)*

**Q18: LRU cache for a rate limiter — good idea?**

**A**: Often use sliding window counter or token bucket instead. LRU tracks access order, not request counts per time window. For "last N unique IPs" LRU can work. *(Section 2)*

**Q19: What happens on `get` for an evicted key?**

**A**: Cache miss — load from source (DB/API), insert at MRU, possibly evict LRU tail. Measure miss latency separately from hit latency. *(Sections 3, 15)*

**Q20: Implement thread-safe LRU without Caffeine.**

**A**: `ReentrantReadWriteLock` — read lock for get, write lock for put/evict. Or single lock for simplicity. Better: use `ConcurrentHashMap` + synchronized list operations carefully, but hand-rolling is error-prone — prefer Caffeine. *(Section 9)*

**Q21: Can `LinkedHashMap` removeEldestEntry be used in production?**

**A**: Only for single-threaded or externally synchronized use. `removeEldestEntry` gives O(1) eviction but no concurrency, stats, or TTL. Good for tests and small utilities. *(Section 7)*

**Q22: How do you test cache behavior?**

**A**: Unit test eviction order with fixed capacity trace (see `LruCacheDemo.java`). Integration test hit/miss with Caffeine stats. Load test hit rate under realistic access pattern. *(Section 6)*

**Q23: Negative caching — cache "not found" results?**

**A**: Yes, with short TTL to prevent repeated DB lookups for missing keys. Use sentinel or `Optional` pattern. Prevents thundering herd on invalid IDs. *(Section 14)*

**Q24: Multi-level cache (L1 + L2)?**

**A**: L1 local Caffeine (fast, small), L2 Redis (shared, larger). On miss: check L1 → L2 → DB. Populate both on load. Invalidate both on write. *(Sections 8, 11)*

**Q25: When would you NOT use any cache?**

**A**: Strong consistency required on every read, data changes every request, memory budget zero, or correctness bugs from staleness exceed latency gains. *(Section 2)*

---

## 18. How to Talk About LRU Cache in an Interview

> Plain English. Short sentences.

---

### "What is an LRU cache?"

It's a cache with a fixed size. When it's full and you add something new, it removes the item you haven't used for the longest time. The idea is — if you haven't touched something in a while, you probably won't need it soon.

---

### "How would you implement it?"

You need fast lookup and fast reordering. So you use a HashMap to find items instantly, and a doubly linked list to track order. Most recently used at the front. Least recently used at the back. When you access an item, move it to the front. When you need to evict, remove from the back.

---

### "Would you build this from scratch in production?"

No. In production I'd use Caffeine. It handles threading, expiry, stats, and better eviction than pure LRU. I'd only implement from scratch in an interview or for learning.

---

### Quick Answers

| Question | Say this |
|---|---|
| What is LRU? | Fixed-size cache that evicts the least recently used item when full |
| Data structures? | HashMap + doubly linked list — both O(1) |
| Complexity? | O(1) get, O(1) put |
| Java shortcut? | LinkedHashMap with accessOrder=true |
| Production choice? | Caffeine — thread-safe, stats, TTL, better eviction |
| LRU vs LFU? | LRU = recent access. LFU = frequent access |
| Thread safety? | Sync wrapper or use Caffeine |

---

*Runnable examples: `examples/lru/LruCacheDemo.java`, `examples/lru/LruCacheLinkedHashMapDemo.java`*
