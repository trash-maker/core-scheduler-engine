package trash_maker.load;

import jakarta.enterprise.inject.Alternative;
import jakarta.inject.Singleton;
import trash_maker.core.ports.EngineMetrics;

import java.util.concurrent.ConcurrentLinkedQueue;

/** Captures timers.lag samples of PUBLISH timers. */
@Alternative
@Singleton
public class LagMetrics implements EngineMetrics {
    final ConcurrentLinkedQueue<Long> timerLag = new ConcurrentLinkedQueue<>();

    @Override
    public void recordMs(String name, long ms, String... tags) {
        if (name.equals("timers.lag") && tags.length > 1 && tags[1].equals("PUBLISH")) timerLag.add(ms);
    }
}
