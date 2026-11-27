package trash_maker.infra;

import io.quarkus.runtime.ShutdownEvent;
import io.smallrye.common.annotation.Blocking;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.jboss.logging.Logger;
import trash_maker.core.Engine;

import java.util.List;
import java.util.function.Supplier;

/**
 * Batch consumers of signals.in and notification.results. Each batch is processed in one DB transaction; the method
 * returns (and offsets are committed) only after the commit. Transient DB failures are retried in place to keep
 * per-partition order. The channels are enabled only on instances with the matching role.
 */
@ApplicationScoped
public class KafkaConsumers {
    private static final Logger LOG = Logger.getLogger(KafkaConsumers.class);

    @Inject Engine engine;
    private volatile boolean stopping;

    void onStop(@Observes ShutdownEvent e) { stopping = true; }

    @Incoming("signals-in")
    @Blocking
    public void signals(List<String> batch) {
        if (!batch.isEmpty()) retry("signals", () -> engine.ingest.ingest(batch));
    }

    @Incoming("results-in")
    @Blocking
    public void results(List<String> batch) {
        if (!batch.isEmpty()) retry("results", () -> engine.results.process(batch));
    }

    private <T> T retry(String what, Supplier<T> work) {
        long backoff = 100;
        while (true) {
            try {
                return work.get();
            } catch (RuntimeException e) {
                if (stopping) throw e; // not acknowledged: redelivered after restart
                LOG.errorf("%s batch failed, retrying in %d ms: %s", what, backoff, e.getMessage());
                try { Thread.sleep(backoff); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); throw e; }
                backoff = Math.min(backoff * 2, 5000);
            }
        }
    }
}
