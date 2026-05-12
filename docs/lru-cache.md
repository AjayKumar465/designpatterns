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
