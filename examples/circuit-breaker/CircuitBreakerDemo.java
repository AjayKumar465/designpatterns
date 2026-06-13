import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Minimal circuit breaker state machine — no external libraries.
 * Demonstrates CLOSED → OPEN → HALF_OPEN → CLOSED lifecycle.
 */
public class CircuitBreakerDemo {

    enum State { CLOSED, OPEN, HALF_OPEN }

    static final class RemoteService {
        private final AtomicInteger callCount = new AtomicInteger();
        private final int failUntilCall;

        RemoteService(int failUntilCall) {
            this.failUntilCall = failUntilCall;
        }

        String call(String input) {
            int n = callCount.incrementAndGet();
            if (n <= failUntilCall) {
                throw new RuntimeException("Remote unavailable (call #" + n + ")");
            }
            return "OK:" + input;
        }
    }

    static final class SimpleCircuitBreaker {
        private final int failureThreshold;
        private final Duration openDuration;
        private final RemoteService remote;

        private State state = State.CLOSED;
        private int consecutiveFailures;
        private Instant openedAt;
        private boolean halfOpenProbeInFlight;

        SimpleCircuitBreaker(RemoteService remote, int failureThreshold, Duration openDuration) {
            this.remote = remote;
            this.failureThreshold = failureThreshold;
            this.openDuration = openDuration;
        }

        State state() {
            return state;
        }

        String execute(String input) {
            transitionIfOpenWindowElapsed();

            if (state == State.OPEN) {
                throw new CallNotPermittedException("Circuit OPEN — fail fast");
            }

            if (state == State.HALF_OPEN) {
                if (halfOpenProbeInFlight) {
                    throw new CallNotPermittedException("Circuit HALF_OPEN — probe already running");
                }
                halfOpenProbeInFlight = true;
            }

            try {
                String result = remote.call(input);
                onSuccess();
                return result;
            } catch (RuntimeException ex) {
                onFailure();
                throw ex;
            } finally {
                if (state == State.HALF_OPEN || halfOpenProbeInFlight) {
                    halfOpenProbeInFlight = false;
                }
            }
        }

        private void transitionIfOpenWindowElapsed() {
            if (state == State.OPEN && Instant.now().isAfter(openedAt.plus(openDuration))) {
                state = State.HALF_OPEN;
                System.out.println("STATE -> HALF_OPEN (probe allowed)");
            }
        }

        private void onSuccess() {
            consecutiveFailures = 0;
            if (state == State.HALF_OPEN) {
                state = State.CLOSED;
                System.out.println("STATE -> CLOSED (probe succeeded)");
            }
        }

        private void onFailure() {
            consecutiveFailures++;
            if (state == State.HALF_OPEN) {
                tripOpen("probe failed");
                return;
            }
            if (state == State.CLOSED && consecutiveFailures >= failureThreshold) {
                tripOpen("failure threshold reached");
            }
        }

        private void tripOpen(String reason) {
            state = State.OPEN;
            openedAt = Instant.now();
            consecutiveFailures = 0;
            System.out.println("STATE -> OPEN (" + reason + ")");
        }
    }

    static final class CallNotPermittedException extends RuntimeException {
        CallNotPermittedException(String message) {
            super(message);
        }
    }

    static void attempt(SimpleCircuitBreaker breaker, String label) {
        try {
            String result = breaker.execute("req-" + label);
            System.out.println(label + " SUCCESS -> " + result + " [state=" + breaker.state() + "]");
        } catch (CallNotPermittedException ex) {
            System.out.println(label + " REJECTED -> " + ex.getMessage());
        } catch (RuntimeException ex) {
            System.out.println(label + " FAILED -> " + ex.getMessage() + " [state=" + breaker.state() + "]");
        }
    }

    public static void main(String[] args) throws InterruptedException {
        System.out.println("=== Scenario 1: Failures trip breaker; calls fail fast while OPEN ===");
        RemoteService flaky = new RemoteService(3);
        SimpleCircuitBreaker breaker = new SimpleCircuitBreaker(flaky, 3, Duration.ofMillis(500));

        for (int i = 1; i <= 6; i++) {
            attempt(breaker, "call-" + i);
        }

        System.out.println("\n=== Scenario 2: After wait, HALF_OPEN probe closes circuit ===");
        Thread.sleep(600);
        attempt(breaker, "probe-1");
        attempt(breaker, "after-recovery");

        System.out.println("\n=== Scenario 3: Fallback pattern (caller-side) ===");
        List<String> timeline = new ArrayList<>();
        for (int i = 1; i <= 4; i++) {
            timeline.add(callWithFallback(breaker, "user-" + i));
        }
        System.out.println("Timeline: " + timeline);
    }

    static String callWithFallback(SimpleCircuitBreaker breaker, String userId) {
        try {
            return breaker.execute(userId);
        } catch (CallNotPermittedException ex) {
            return "FALLBACK:cached-profile:" + userId;
        } catch (RuntimeException ex) {
            return "FALLBACK:cached-profile:" + userId;
        }
    }
}
