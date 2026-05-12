import java.util.HashMap;
import java.util.Map;

public class LruCacheDemo {

    static final class LruCache<K, V> {
        private static final class Node<K, V> {
            K key;
            V value;
            Node<K, V> prev;
            Node<K, V> next;

            Node() {}

            Node(K key, V value) {
                this.key = key;
                this.value = value;
            }
        }

        private final int capacity;
        private final Map<K, Node<K, V>> index;
        private final Node<K, V> head;
        private final Node<K, V> tail;

        LruCache(int capacity) {
            if (capacity <= 0) {
                throw new IllegalArgumentException("capacity must be > 0");
            }
            this.capacity = capacity;
            this.index = new HashMap<>();
            this.head = new Node<>();
            this.tail = new Node<>();
            head.next = tail;
            tail.prev = head;
        }

        public V get(K key) {
            Node<K, V> node = index.get(key);
            if (node == null) {
                return null;
            }
            moveToFront(node);
            return node.value;
        }

        public void put(K key, V value) {
            Node<K, V> node = index.get(key);
            if (node != null) {
                node.value = value;
                moveToFront(node);
                return;
            }

            Node<K, V> created = new Node<>(key, value);
            index.put(key, created);
            addAfterHead(created);

            if (index.size() > capacity) {
                Node<K, V> lru = tail.prev;
                removeNode(lru);
                index.remove(lru.key);
            }
        }

        public int size() {
            return index.size();
        }

        public String snapshotMostRecentToLeast() {
            StringBuilder sb = new StringBuilder("[");
            Node<K, V> cur = head.next;
            while (cur != tail) {
                sb.append(cur.key).append("=").append(cur.value);
                cur = cur.next;
                if (cur != tail) {
                    sb.append(", ");
                }
            }
            sb.append("]");
            return sb.toString();
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
            node.prev = null;
            node.next = null;
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

        put(cache, 6, "F"); // evicts key 1
        put(cache, 7, "G"); // evicts key 3

        get(cache, 5);
        put(cache, 8, "H"); // evicts key 2

        System.out.println("Final cache (MRU -> LRU): " + cache.snapshotMostRecentToLeast());
        System.out.println("Contains 1? " + (cache.get(1) != null));
        System.out.println("Contains 3? " + (cache.get(3) != null));
        System.out.println("Contains 8? " + (cache.get(8) != null));
    }

    private static void put(LruCache<Integer, String> cache, int key, String value) {
        cache.put(key, value);
        System.out.println("PUT " + key + " => " + value + " | " + cache.snapshotMostRecentToLeast());
    }

    private static void get(LruCache<Integer, String> cache, int key) {
        String value = cache.get(key);
        System.out.println("GET " + key + " => " + value + " | " + cache.snapshotMostRecentToLeast());
    }
}
