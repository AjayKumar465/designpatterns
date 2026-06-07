# LRU Cache (Capacity = 5)

## Goal

Implement a fixed-size cache where:

1. `get(key)` returns value and marks key as most recently used
2. `put(key, value)` inserts/updates and marks key as most recently used
3. When capacity is exceeded, evict least recently used entry

Capacity in this example is `5`.

## Data Structure Choice

Use:

1. `HashMap<K, Node<K,V>>` for O(1) key lookup
2. Doubly linked list for O(1) recency updates and eviction

List ordering:

- Head side = most recently used (MRU)
- Tail side = least recently used (LRU)

Sentinel nodes (`head`, `tail`) remove edge-case branching.

## Complexity

1. `get`: O(1)
2. `put`: O(1)
3. `evict`: O(1)
4. Space: O(capacity)

## Core Operations

1. `get(key)`:
- If key missing: return `null`
- Else move node to head and return value

2. `put(key, value)`:
- If key exists: update value, move to head
- Else insert new node at head
- If size > capacity: remove `tail.prev` and delete from map

## End-to-End Trace (Capacity 5)

Starting empty:

1. `put(1,A)` -> `[1]`
2. `put(2,B)` -> `[2,1]`
3. `put(3,C)` -> `[3,2,1]`
4. `put(4,D)` -> `[4,3,2,1]`
5. `put(5,E)` -> `[5,4,3,2,1]`
6. `get(2)`   -> `[2,5,4,3,1]`
7. `get(4)`   -> `[4,2,5,3,1]`
8. `put(6,F)` -> `[6,4,2,5,3]` (evict `1`)
9. `put(7,G)` -> `[7,6,4,2,5]` (evict `3`)
10. `get(5)`  -> `[5,7,6,4,2]`
11. `put(8,H)`-> `[8,5,7,6,4]` (evict `2`)

Final keys present: `8,5,7,6,4`

## Java Example

Implementation file:

- `examples/lru/LruCacheDemo.java`

Run:

- `javac examples/lru/LruCacheDemo.java`
- `java -cp examples/lru LruCacheDemo`

## Production Notes

1. Thread safety:
- This demo is single-threaded.
- In multithreaded use, guard both map and list with one lock, or use segmented design.

2. Value semantics:
- Distinguish cache miss from stored `null` values if needed.

3. Memory:
- For object-heavy keys/values, monitor GC pressure and consider size-bytes policy instead of entry-count policy.

## Alternative Data Structures

Yes, there are alternatives.

1. `LinkedHashMap` (recommended built-in option)
- Java maintains a doubly linked order internally.
- With `accessOrder=true`, it acts like LRU.
- Override `removeEldestEntry` for fixed capacity eviction.
- Expected complexity: O(1) `get`/`put`.
- Example in repo: `examples/lru/LruCacheLinkedHashMapDemo.java`

2. `Queue + HashMap` (usually not ideal for strict LRU)
- Queue can track order, map can track values.
- But when a key is re-accessed, moving it from middle of queue is not O(1) for typical queue structures.
- You often need lazy deletion or extra indirection, which complicates correctness and can degrade performance.
- In practice, `HashMap + DoublyLinkedList` (custom) or `LinkedHashMap` (library) are the standard LRU choices.

---

## How to Talk About LRU Cache in an Interview (Human English)

---

### "What is an LRU Cache?"

> "LRU stands for Least Recently Used. It's a cache with a fixed capacity where, when you need to add something new and the cache is full, you evict the item that was accessed least recently. The idea is that if you haven't touched something in a while, you're probably not going to need it soon. Classic use case: database query results, expensive computation results, API responses — things that are expensive to regenerate. Instead of hitting the DB every time, you check the cache first. If it's there, return it. If not, compute it, cache it, and evict the oldest thing if you're at capacity."

---

### "How would you implement it efficiently?"

> "You need O(1) lookup and O(1) eviction. The key insight is: you need to quickly find an item by key, AND you need to quickly know which item is the least recently used. A HashMap alone gives you O(1) lookup but you don't know the order. A linked list gives you order but lookup is O(n). Combine them: HashMap for lookup, doubly linked list for order. The map stores a reference directly to the node in the list — so finding an item is O(1) via the map, and moving it to the front of the list (marking it as recently used) is O(1) pointer manipulation. Eviction is O(1) — just remove the tail of the list. The elegant shortcut in Java is `LinkedHashMap` with `accessOrder=true` and override `removeEldestEntry` — that's LRU in about 3 lines."

---

### "What's the time complexity?"

> "Both `get` and `put` are O(1). That's the whole point — you're paying O(capacity) space to get constant time operations. If you use a sorted structure or scan for the LRU on every operation, it degrades to O(log n) or O(n) and you lose the cache's performance advantage."

---

### "What about thread safety?"

> "The basic implementation with HashMap + doubly linked list is single-threaded. In production, if multiple threads are reading and writing, you need synchronization. The simplest fix is a `synchronized` wrapper around the whole cache — but that's a bottleneck. A better approach for high-concurrency is striped locking or using Caffeine, which is a high-performance production cache library with built-in LRU/LFU, expiry, and load-on-miss. In real production code I almost never implement LRU from scratch — I use Caffeine or Guava Cache. But knowing how to implement it is what interviewers want to verify."

---

### Quick Cheat Sheet

| Question | One-line answer |
|---|---|
| What is LRU? | Fixed-size cache that evicts the least recently accessed item when full |
| Data structures? | HashMap (O(1) lookup) + Doubly Linked List (O(1) order + eviction) |
| Complexity? | O(1) get, O(1) put, O(capacity) space |
| Java built-in? | `LinkedHashMap(capacity, loadFactor, true)` with `removeEldestEntry` override |
| Production cache? | Use Caffeine — high-performance, concurrent, configurable eviction and expiry |
| Thread safety? | Basic impl is not thread-safe — use Caffeine or explicit synchronization |

