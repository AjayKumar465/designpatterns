import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

public class SagaDemo {

    interface Step {
        String name();
        void execute(Context ctx);
        void compensate(Context ctx);
    }

    static final class Context {
        final String sagaId = UUID.randomUUID().toString();
        final Set<String> executedStepKeys = new HashSet<>();
        final List<String> timeline = new ArrayList<>();

        boolean inventoryReserved;
        boolean paymentCaptured;
        boolean shipmentCreated;

        boolean markExecuted(String stepName) {
            return executedStepKeys.add(sagaId + ":" + stepName);
        }

        void log(String message) {
            timeline.add(message);
            System.out.println(message);
        }
    }

    static final class ReserveInventoryStep implements Step {
        private final boolean fail;
        ReserveInventoryStep(boolean fail) { this.fail = fail; }

        public String name() { return "ReserveInventory"; }

        public void execute(Context ctx) {
            if (!ctx.markExecuted(name())) {
                ctx.log("SKIP duplicate command for " + name());
                return;
            }
            if (fail) {
                throw new RuntimeException("Inventory unavailable");
            }
            ctx.inventoryReserved = true;
            ctx.log("Inventory reserved");
        }

        public void compensate(Context ctx) {
            if (ctx.inventoryReserved) {
                ctx.inventoryReserved = false;
                ctx.log("Inventory released");
            } else {
                ctx.log("Inventory compensation was idempotent no-op");
            }
        }
    }

    static final class CapturePaymentStep implements Step {
        private final boolean fail;
        CapturePaymentStep(boolean fail) { this.fail = fail; }

        public String name() { return "CapturePayment"; }

        public void execute(Context ctx) {
            if (!ctx.markExecuted(name())) {
                ctx.log("SKIP duplicate command for " + name());
                return;
            }
            if (fail) {
                throw new RuntimeException("Payment gateway timeout");
            }
            ctx.paymentCaptured = true;
            ctx.log("Payment captured");
        }

        public void compensate(Context ctx) {
            if (ctx.paymentCaptured) {
                ctx.paymentCaptured = false;
                ctx.log("Payment refunded");
            } else {
                ctx.log("Payment compensation was idempotent no-op");
            }
        }
    }

    static final class CreateShipmentStep implements Step {
        private final boolean fail;
        CreateShipmentStep(boolean fail) { this.fail = fail; }

        public String name() { return "CreateShipment"; }

        public void execute(Context ctx) {
            if (!ctx.markExecuted(name())) {
                ctx.log("SKIP duplicate command for " + name());
                return;
            }
            if (fail) {
                throw new RuntimeException("Shipping provider unavailable");
            }
            ctx.shipmentCreated = true;
            ctx.log("Shipment created");
        }

        public void compensate(Context ctx) {
            if (ctx.shipmentCreated) {
                ctx.shipmentCreated = false;
                ctx.log("Shipment cancelled");
            } else {
                ctx.log("Shipment compensation was idempotent no-op");
            }
        }
    }

    static final class SagaOrchestrator {
        private final List<Step> steps;

        SagaOrchestrator(List<Step> steps) {
            this.steps = steps;
        }

        void run(Context ctx) {
            List<Step> completed = new ArrayList<>();
            ctx.log("SAGA START id=" + ctx.sagaId);

            try {
                for (Step s : steps) {
                    ctx.log("EXECUTE " + s.name());
                    s.execute(ctx);
                    completed.add(s);
                }
                ctx.log("SAGA SUCCESS");
            } catch (Exception e) {
                ctx.log("SAGA FAILED reason=" + e.getMessage());
                for (int i = completed.size() - 1; i >= 0; i--) {
                    Step s = completed.get(i);
                    ctx.log("COMPENSATE " + s.name());
                    s.compensate(ctx);
                }
                ctx.log("SAGA COMPENSATED");
            }
        }
    }

    public static void main(String[] args) {
        System.out.println("=== Scenario 1: Happy Path ===");
        Context success = new Context();
        new SagaOrchestrator(Arrays.asList(
            new ReserveInventoryStep(false),
            new CapturePaymentStep(false),
            new CreateShipmentStep(false)
        )).run(success);

        System.out.println("\n=== Scenario 2: Shipment Failure -> Reverse Compensation ===");
        Context failed = new Context();
        new SagaOrchestrator(Arrays.asList(
            new ReserveInventoryStep(false),
            new CapturePaymentStep(false),
            new CreateShipmentStep(true)
        )).run(failed);

        System.out.println("\n=== Scenario 3: Duplicate Command Idempotency ===");
        Context duplicate = new Context();
        Step inventory = new ReserveInventoryStep(false);
        inventory.execute(duplicate);
        inventory.execute(duplicate);
        inventory.compensate(duplicate);
        inventory.compensate(duplicate);
    }
}
