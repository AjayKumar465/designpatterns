import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Shows how a slow dependency can exhaust a shared thread pool,
 * and how a bulkhead (semaphore) limits blast radius.
 */
public class BulkheadDemo {

    static final class SlowDependency {
        private final long latencyMs;

        SlowDependency(long latencyMs) {
            this.latencyMs = latencyMs;
        }

        String call(String id) throws InterruptedException {
            Thread.sleep(latencyMs);
            return "DATA:" + id;
        }
    }

    static final class BulkheadRejectedException extends RuntimeException {
        BulkheadRejectedException(String message) {
            super(message);
        }
    }

    /** No bulkhead — every call occupies a pool thread until slow dependency returns. */
    static final class SharedPoolClient {
        private final SlowDependency dependency;
        private final ExecutorService pool;

        SharedPoolClient(SlowDependency dependency, int poolSize) {
            this.dependency = dependency;
            this.pool = Executors.newFixedThreadPool(poolSize);
        }

        Future<String> callAsync(String id) {
            return pool.submit(() -> dependency.call(id));
        }

        void shutdown() {
            pool.shutdownNow();
        }
    }

    /** Bulkhead limits concurrent calls to the slow dependency. */
    static final class BulkheadClient {
        private final SlowDependency dependency;
        private final ExecutorService pool;
        private final Semaphore bulkhead;

        BulkheadClient(SlowDependency dependency, int poolSize, int maxConcurrentToDependency) {
            this.dependency = dependency;
            this.pool = Executors.newFixedThreadPool(poolSize);
            this.bulkhead = new Semaphore(maxConcurrentToDependency);
        }

        Future<String> callAsync(String id) {
            return pool.submit(() -> {
                if (!bulkhead.tryAcquire()) {
                    throw new BulkheadRejectedException("Bulkhead full for slow dependency");
                }
                try {
                    return dependency.call(id);
                } finally {
                    bulkhead.release();
                }
            });
        }

        void shutdown() {
            pool.shutdownNow();
        }
    }

    static void runScenario(String title, Callable<List<String>> scenario) throws Exception {
        System.out.println("=== " + title + " ===");
        long start = System.nanoTime();
        List<String> results = scenario.call();
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        System.out.println("Results: " + results);
        System.out.println("Elapsed ms: " + elapsedMs);
        System.out.println();
    }

    static List<String> drainFutures(List<Future<String>> futures) throws Exception {
        List<String> out = new ArrayList<>();
        for (Future<String> f : futures) {
            try {
                out.add(f.get(2, TimeUnit.SECONDS));
            } catch (Exception ex) {
                out.add("ERR:" + ex.getClass().getSimpleName());
            }
        }
        return out;
    }

    public static void main(String[] args) throws Exception {
        SlowDependency slow = new SlowDependency(400);

        runScenario("Without bulkhead — 8 concurrent calls, pool size 4 (queue + timeouts)", () -> {
            SharedPoolClient client = new SharedPoolClient(slow, 4);
            List<Future<String>> futures = new ArrayList<>();
            for (int i = 1; i <= 8; i++) {
                futures.add(client.callAsync("req-" + i));
            }
            List<String> results = drainFutures(futures);
            client.shutdown();
            return results;
        });

        runScenario("With bulkhead — max 2 concurrent to slow dependency, fail fast when full", () -> {
            BulkheadClient client = new BulkheadClient(slow, 4, 2);
            List<Future<String>> futures = new ArrayList<>();
            for (int i = 1; i <= 8; i++) {
                futures.add(client.callAsync("req-" + i));
            }
            List<String> results = drainFutures(futures);
            client.shutdown();
            return results;
        });

        System.out.println("=== Thread pool bulkhead — separate pools per dependency ===");
        ExecutorService paymentPool = Executors.newFixedThreadPool(2);
        ExecutorService catalogPool = Executors.newFixedThreadPool(2);
        AtomicInteger paymentInflight = new AtomicInteger();
        CountDownLatch latch = new CountDownLatch(1);

        Future<String> payment = paymentPool.submit(() -> {
            paymentInflight.incrementAndGet();
            latch.await(2, TimeUnit.SECONDS);
            paymentInflight.decrementAndGet();
            return "payment-ok";
        });

        Future<String> catalog = catalogPool.submit(() -> "catalog-ok");

        System.out.println("Catalog still works while payment pool is busy: " + catalog.get(1, TimeUnit.SECONDS));
        System.out.println("Payment inflight (isolated pool): " + paymentInflight.get());
        latch.countDown();
        System.out.println("Payment completed: " + payment.get(1, TimeUnit.SECONDS));

        paymentPool.shutdownNow();
        catalogPool.shutdownNow();
    }
}
