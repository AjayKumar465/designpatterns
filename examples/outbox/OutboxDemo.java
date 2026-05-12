import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

public class OutboxDemo {

    enum Status { PENDING, SENT }

    static final class Order {
        final String orderId;
        String status;

        Order(String orderId, String status) {
            this.orderId = orderId;
            this.status = status;
        }
    }

    static final class OutboxEvent {
        final String eventId;
        final String aggregateId;
        final String eventType;
        final String payload;
        final Instant createdAt;
        Status status;
        int attempts;

        OutboxEvent(String aggregateId, String eventType, String payload) {
            this.eventId = UUID.randomUUID().toString();
            this.aggregateId = aggregateId;
            this.eventType = eventType;
            this.payload = payload;
            this.createdAt = Instant.now();
            this.status = Status.PENDING;
            this.attempts = 0;
        }
    }

    static final class InMemoryStore {
        final List<Order> orders = new ArrayList<>();
        final List<OutboxEvent> outbox = new ArrayList<>();
    }

    static final class OrderService {
        private final InMemoryStore store;

        OrderService(InMemoryStore store) {
            this.store = store;
        }

        // Simulates one local DB transaction writing both order and outbox row.
        void createOrderAndOutbox(String orderId) {
            Order order = new Order(orderId, "CREATED");
            store.orders.add(order);

            String payload = "{orderId:" + orderId + ",status:CREATED}";
            OutboxEvent event = new OutboxEvent(orderId, "OrderCreated", payload);
            store.outbox.add(event);

            System.out.println("TX COMMIT: order + outbox event id=" + event.eventId);
        }
    }

    static final class Broker {
        final List<OutboxEvent> topic = new ArrayList<>();
        boolean failNextPublish = true;

        void publish(OutboxEvent e) {
            if (failNextPublish) {
                failNextPublish = false;
                throw new RuntimeException("Transient broker failure");
            }
            topic.add(e);
        }
    }

    static final class OutboxRelay {
        private final InMemoryStore store;
        private final Broker broker;

        OutboxRelay(InMemoryStore store, Broker broker) {
            this.store = store;
            this.broker = broker;
        }

        void pollAndPublishOnce() {
            for (OutboxEvent e : store.outbox) {
                if (e.status == Status.SENT) {
                    continue;
                }
                try {
                    e.attempts++;
                    broker.publish(e);
                    e.status = Status.SENT;
                    System.out.println("PUBLISH OK eventId=" + e.eventId + " attempts=" + e.attempts);
                } catch (RuntimeException ex) {
                    System.out.println("PUBLISH FAIL eventId=" + e.eventId + " attempts=" + e.attempts
                        + " reason=" + ex.getMessage());
                }
            }
        }
    }

    static final class IdempotentConsumer {
        private final Set<String> processedEventIds = new HashSet<>();

        void onEvent(OutboxEvent e) {
            if (!processedEventIds.add(e.eventId)) {
                System.out.println("DUPLICATE DROPPED eventId=" + e.eventId);
                return;
            }
            System.out.println("CONSUMED eventId=" + e.eventId + " payload=" + e.payload);
        }
    }

    public static void main(String[] args) {
        InMemoryStore store = new InMemoryStore();
        OrderService orderService = new OrderService(store);
        Broker broker = new Broker();
        OutboxRelay relay = new OutboxRelay(store, broker);
        IdempotentConsumer consumer = new IdempotentConsumer();

        System.out.println("=== Scenario: TX write + relay retry + idempotent consume ===");
        String orderId = UUID.randomUUID().toString();
        orderService.createOrderAndOutbox(orderId);

        // First relay poll fails publish.
        relay.pollAndPublishOnce();
        // Second relay poll retries and succeeds.
        relay.pollAndPublishOnce();

        // Deliver to consumer.
        for (OutboxEvent e : broker.topic) {
            consumer.onEvent(e);
            // Force duplicate delivery simulation (at-least-once semantics).
            consumer.onEvent(e);
        }
    }
}
