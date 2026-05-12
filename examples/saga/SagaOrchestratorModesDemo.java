import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

public class SagaOrchestratorModesDemo {

    interface ServiceStep {
        String name();
        void execute(SagaContext ctx);
        void compensate(SagaContext ctx);
    }

    static final class SagaContext {
        final String sagaId = UUID.randomUUID().toString();
        final List<String> timeline = new ArrayList<>();
        final Set<String> executedKeys = new HashSet<>();
        final boolean failAtD;

        SagaContext(boolean failAtD) {
            this.failAtD = failAtD;
        }

        boolean firstExecution(String step) {
            return executedKeys.add(sagaId + ":" + step);
        }

        void log(String msg) {
            timeline.add(msg);
            System.out.println(msg);
        }
    }

    static final class ServiceA implements ServiceStep {
        public String name() { return "ServiceA"; }
        public void execute(SagaContext ctx) {
            if (!ctx.firstExecution(name())) {
                ctx.log("A execute duplicate ignored");
                return;
            }
            ctx.log("A DONE");
        }
        public void compensate(SagaContext ctx) { ctx.log("A COMPENSATED"); }
    }

    static final class ServiceB implements ServiceStep {
        public String name() { return "ServiceB"; }
        public void execute(SagaContext ctx) {
            if (!ctx.firstExecution(name())) {
                ctx.log("B execute duplicate ignored");
                return;
            }
            ctx.log("B DONE");
        }
        public void compensate(SagaContext ctx) { ctx.log("B COMPENSATED"); }
    }

    static final class ServiceC implements ServiceStep {
        public String name() { return "ServiceC"; }
        public void execute(SagaContext ctx) {
            if (!ctx.firstExecution(name())) {
                ctx.log("C execute duplicate ignored");
                return;
            }
            ctx.log("C DONE");
        }
        public void compensate(SagaContext ctx) { ctx.log("C COMPENSATED"); }
    }

    static final class ServiceD implements ServiceStep {
        public String name() { return "ServiceD"; }
        public void execute(SagaContext ctx) {
            if (!ctx.firstExecution(name())) {
                ctx.log("D execute duplicate ignored");
                return;
            }
            if (ctx.failAtD) {
                throw new RuntimeException("D FAILED");
            }
            ctx.log("D DONE");
        }
        public void compensate(SagaContext ctx) { ctx.log("D COMPENSATED"); }
    }

    static final class SyncOrchestrator {
        private final List<ServiceStep> forward;

        SyncOrchestrator(List<ServiceStep> forward) {
            this.forward = forward;
        }

        void run(SagaContext ctx) {
            ctx.log("SYNC ORCHESTRATION START sagaId=" + ctx.sagaId);
            List<ServiceStep> completed = new ArrayList<>();
            try {
                for (ServiceStep step : forward) {
                    ctx.log("CALL " + step.name());
                    step.execute(ctx);
                    completed.add(step);
                }
                ctx.log("SYNC SAGA SUCCESS");
            } catch (Exception ex) {
                ctx.log("SYNC SAGA FAILED reason=" + ex.getMessage());
                for (int i = completed.size() - 1; i >= 0; i--) {
                    ServiceStep step = completed.get(i);
                    ctx.log("ROLLBACK " + step.name());
                    step.compensate(ctx);
                }
                ctx.log("SYNC SAGA COMPENSATED");
            }
        }
    }

    enum EventType {
        DO_A, A_DONE, A_FAILED,
        DO_B, B_DONE, B_FAILED,
        DO_C, C_DONE, C_FAILED,
        DO_D, D_DONE, D_FAILED,
        UNDO_C, C_COMP_DONE,
        UNDO_B, B_COMP_DONE,
        UNDO_A, A_COMP_DONE
    }

    static final class Event {
        final EventType type;
        Event(EventType type) { this.type = type; }
    }

    static final class AsyncOrchestrator {
        private final ServiceA a = new ServiceA();
        private final ServiceB b = new ServiceB();
        private final ServiceC c = new ServiceC();
        private final ServiceD d = new ServiceD();
        private final Deque<Event> queue = new ArrayDeque<>();

        void run(SagaContext ctx) {
            ctx.log("ASYNC ORCHESTRATION START sagaId=" + ctx.sagaId);
            queue.add(new Event(EventType.DO_A));

            while (!queue.isEmpty()) {
                Event e = queue.poll();
                ctx.log("EVENT " + e.type);

                switch (e.type) {
                    case DO_A:
                        executeStepAsync(ctx, a, EventType.A_DONE, EventType.A_FAILED);
                        break;
                    case A_DONE:
                        queue.add(new Event(EventType.DO_B));
                        break;
                    case DO_B:
                        executeStepAsync(ctx, b, EventType.B_DONE, EventType.B_FAILED);
                        break;
                    case B_DONE:
                        queue.add(new Event(EventType.DO_C));
                        break;
                    case DO_C:
                        executeStepAsync(ctx, c, EventType.C_DONE, EventType.C_FAILED);
                        break;
                    case C_DONE:
                        queue.add(new Event(EventType.DO_D));
                        break;
                    case DO_D:
                        executeStepAsync(ctx, d, EventType.D_DONE, EventType.D_FAILED);
                        break;
                    case D_DONE:
                        ctx.log("ASYNC SAGA SUCCESS");
                        break;
                    case D_FAILED:
                        ctx.log("ASYNC FAILURE at D -> start compensation");
                        queue.add(new Event(EventType.UNDO_C));
                        break;
                    case UNDO_C:
                        c.compensate(ctx);
                        queue.add(new Event(EventType.C_COMP_DONE));
                        break;
                    case C_COMP_DONE:
                        queue.add(new Event(EventType.UNDO_B));
                        break;
                    case UNDO_B:
                        b.compensate(ctx);
                        queue.add(new Event(EventType.B_COMP_DONE));
                        break;
                    case B_COMP_DONE:
                        queue.add(new Event(EventType.UNDO_A));
                        break;
                    case UNDO_A:
                        a.compensate(ctx);
                        queue.add(new Event(EventType.A_COMP_DONE));
                        break;
                    case A_COMP_DONE:
                        ctx.log("ASYNC SAGA COMPENSATED");
                        break;
                    default:
                        throw new IllegalStateException("Unhandled event " + e.type);
                }
            }
        }

        private void executeStepAsync(SagaContext ctx, ServiceStep step, EventType ok, EventType failed) {
            try {
                step.execute(ctx);
                queue.add(new Event(ok));
            } catch (Exception ex) {
                ctx.log(step.name() + " execution failed: " + ex.getMessage());
                queue.add(new Event(failed));
            }
        }
    }

    public static void main(String[] args) {
        List<ServiceStep> flow = Arrays.asList(
            new ServiceA(),
            new ServiceB(),
            new ServiceC(),
            new ServiceD()
        );

        System.out.println("=== Sync Happy Path (A -> B -> C -> D) ===");
        new SyncOrchestrator(flow).run(new SagaContext(false));

        System.out.println("\n=== Sync Failure Path (D fails -> rollback C,B,A) ===");
        new SyncOrchestrator(flow).run(new SagaContext(true));

        System.out.println("\n=== Async Happy Path (A -> B -> C -> D) ===");
        new AsyncOrchestrator().run(new SagaContext(false));

        System.out.println("\n=== Async Failure Path (D fails -> rollback C,B,A) ===");
        new AsyncOrchestrator().run(new SagaContext(true));
    }
}
