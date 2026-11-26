package trash_maker.infra.ops;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import trash_maker.core.ports.EngineMetrics;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Micrometer adapter. Tags are bounded (action, kind, outcome, reason codes); meters are cached. */
public final class MicrometerMetrics implements EngineMetrics {
    private final MeterRegistry registry;
    private final Map<String, Counter> counters = new ConcurrentHashMap<>();
    private final Map<String, DistributionSummary> summaries = new ConcurrentHashMap<>();

    public MicrometerMetrics(MeterRegistry registry) { this.registry = registry; }

    @Override
    public void count(String name, long n, String... tags) {
        counters.computeIfAbsent(name + Arrays.toString(tags), k -> Counter.builder("notif." + name).tags(tags).register(registry)).increment(n);
    }

    @Override
    public void recordMs(String name, long millis, String... tags) {
        summaries.computeIfAbsent(name + Arrays.toString(tags), k -> DistributionSummary.builder("notif." + name + ".ms").baseUnit("milliseconds")
                .publishPercentiles(0.5, 0.95, 0.99).publishPercentileHistogram().tags(tags).register(registry)).record(millis);
    }
}
