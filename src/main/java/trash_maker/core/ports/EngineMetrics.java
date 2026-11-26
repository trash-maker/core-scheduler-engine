package trash_maker.core.ports;

/** Port for metrics; tags must have bounded cardinality (action, kind, outcome, reason codes). */
public interface EngineMetrics {
    EngineMetrics NOOP = new EngineMetrics() {};

    default void count(String name, String... tags) { count(name, 1, tags); }

    default void count(String name, long n, String... tags) {}

    /** Records a duration/lag in milliseconds into a histogram. */
    default void recordMs(String name, long millis, String... tags) {}
}
