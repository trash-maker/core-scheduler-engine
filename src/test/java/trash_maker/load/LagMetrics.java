package trash_maker.load;

import io.micrometer.core.instrument.MeterRegistry;
import jakarta.enterprise.inject.Alternative;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import trash_maker.core.Metrics;

import java.util.concurrent.ConcurrentLinkedQueue;

/** Metrics that also keep every timers.lag sample of PUBLISH timers (exact percentiles for the report). */
@Alternative
@Singleton
public class LagMetrics extends Metrics {
    final ConcurrentLinkedQueue<Long> timerLag = new ConcurrentLinkedQueue<>();

    @Inject
    public LagMetrics(MeterRegistry registry) { super(registry); }

    @Override
    public void recordMs(String name, long ms, String... tags) {
        super.recordMs(name, ms, tags);
        if (name.equals("timers.lag") && tags.length > 1 && tags[1].equals("PUBLISH")) timerLag.add(ms);
    }
}
