package trash_maker.core;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Engine metrics on the Quarkus Micrometer registry (prefix notif.). Tags must have bounded cardinality
 * (action, kind, outcome, reason codes). Meters are cached per name and tags.
 */
@Singleton
public class Metrics {
    private final MeterRegistry registry;
    private final Map<String, Counter> counters = new ConcurrentHashMap<>();
    private final Map<String, DistributionSummary> summaries = new ConcurrentHashMap<>();

    @Inject
    public Metrics(MeterRegistry registry) { this.registry = registry; }

    public void count(String name, String... tags) { count(name, 1, tags); }

    public void count(String name, long n, String... tags) {
        counters.computeIfAbsent(name + Arrays.toString(tags), k -> Counter.builder("notif." + name).tags(tags).register(registry)).increment(n);
    }

    /** Records a duration or lag in milliseconds (p50/p95/p99 published). */
    public void recordMs(String name, long millis, String... tags) {
        summaries.computeIfAbsent(name + Arrays.toString(tags), k -> DistributionSummary.builder("notif." + name + ".ms").baseUnit("milliseconds")
                .publishPercentiles(0.5, 0.95, 0.99).publishPercentileHistogram().tags(tags).register(registry)).record(millis);
    }
}
