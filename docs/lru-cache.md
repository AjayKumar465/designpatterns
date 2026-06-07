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

## How to Talk About LRU Cache in an Interview

> Plain and direct. How you would explain it on a whiteboard.

---

### "What is an LRU Cache?"

LRU means Least Recently Used. It's a cache with a fixed size.

When the cache is full and you add something new, it removes the item you haven't touched for the longest time. The idea is — if you haven't used something in a while, you probably won't need it soon.

Common use is caching database results. Instead of hitting the database every time, you check the cache first. Fast response, less load on the database.

---

### "How would you implement it?"

You need two things working together.

First — a HashMap for fast lookup. You can find any item instantly.

Second — a doubly linked list to track order. The most recently used item is at the front. The least recently used is at the back. When you use an item, you move it to the front. When you need to evict something, you remove from the back.

Both operations are instant — O(1). That's the whole point.

In Java, the easy way is to use `LinkedHashMap` with `accessOrder=true`. It handles all of this for you.

---

### "What about thread safety?"

The basic version is not safe for multiple threads. If two threads use it at the same time, things break.

The simple fix is to put a lock on the whole cache. But only one thread at a time can use it then.

In real production code I would use Caffeine. It's a high-performance cache library that handles all of this properly. I wouldn't build LRU from scratch in production — but knowing how it works is what the interview question is really about.

---

### Quick Answers

| Question | Say this |
|---|---|
| What is LRU? | A cache that removes the item used least recently when it gets full |
| Data structures? | HashMap for fast lookup + doubly linked list for order tracking |
| Time complexity? | O(1) for both get and put |
| Java built-in option? | LinkedHashMap with accessOrder=true and removeEldestEntry override |
| Production choice? | Use Caffeine — it's fast, thread-safe, and configurable |

