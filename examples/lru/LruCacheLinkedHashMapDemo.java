import java.util.LinkedHashMap;
import java.util.Map;

public class LruCacheLinkedHashMapDemo {

    static final class LruCache<K, V> extends LinkedHashMap<K, V> {
        private final int capacity;

        LruCache(int capacity) {
            // accessOrder=true maintains order by access (get/put), not insertion.
            super(capacity, 0.75f, true);
            if (capacity <= 0) {
                throw new IllegalArgumentException("capacity must be > 0");
            }
            this.capacity = capacity;
        }

        @Override
        protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
            return size() > capacity;
        }
    }

    public static void main(String[] args) {
        LruCache<Integer, String> cache = new LruCache<>(5);

        put(cache, 1, "A");
        put(cache, 2, "B");
        put(cache, 3, "C");
        put(cache, 4, "D");
        put(cache, 5, "E");

        get(cache, 2);
        get(cache, 4);

        put(cache, 6, "F"); // evicts 1
        put(cache, 7, "G"); // evicts 3

        get(cache, 5);
        put(cache, 8, "H"); // evicts 2

        System.out.println("Final cache (LRU -> MRU iteration order): " + cache);
        System.out.println("Contains 1? " + cache.containsKey(1));
        System.out.println("Contains 3? " + cache.containsKey(3));
        System.out.println("Contains 8? " + cache.containsKey(8));
    }

    private static void put(LruCache<Integer, String> cache, int key, String value) {
        cache.put(key, value);
        System.out.println("PUT " + key + " => " + value + " | " + cache);
    }

    private static void get(LruCache<Integer, String> cache, int key) {
        String value = cache.get(key);
        System.out.println("GET " + key + " => " + value + " | " + cache);
    }
}
