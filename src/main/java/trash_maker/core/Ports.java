package trash_maker.core;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;

/** Ports implemented by the adapters (broker, metrics, tracing) and the local timer wake-up hub. */
public final class Ports {
    private Ports() {}

    /** Port to the message broker (Kafka in production, in-memory in tests). Must be idempotent per message. */
    public interface MessagePublisher {
        CompletableFuture<Void> publish(String topic, String key, Map<String, String> headers, String payload);
    }

    /** Port for metrics; tags must have bounded cardinality (action, kind, outcome, reason codes). */
    public interface EngineMetrics {
        EngineMetrics NOOP = new EngineMetrics() {};

        default void count(String name, String... tags) { count(name, 1, tags); }

        default void count(String name, long n, String... tags) {}

        /** Records a duration/lag in milliseconds into a histogram. */
        default void recordMs(String name, long millis, String... tags) {}
    }

    /** Port for trace propagation: headers of the current span (e.g. traceparent) to store in the outbox. */
    public interface TraceContext {
        TraceContext NONE = Map::of;

        Map<String, String> currentHeaders();
    }

    /** Notified after commit when a timer due soon was written, so local workers do not wait for the next poll. */
    public interface TimerWakeup {
        TimerWakeup NONE = due -> {};

        void hint(Instant dueAt);
    }

    /** Fans out local timer wake-ups to the workers of this instance. */
    public static final class WakeupHub implements TimerWakeup {
        private final List<TimerWakeup> listeners = new CopyOnWriteArrayList<>();

        public void register(TimerWakeup w) { listeners.add(w); }

        public void unregister(TimerWakeup w) { listeners.remove(w); }

        @Override
        public void hint(Instant dueAt) { for (TimerWakeup w : listeners) w.hint(dueAt); }
    }
}
