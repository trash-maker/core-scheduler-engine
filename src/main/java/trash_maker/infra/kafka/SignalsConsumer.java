package trash_maker.infra.kafka;

import trash_maker.core.Engine;
import io.smallrye.common.annotation.Blocking;
import io.quarkus.runtime.ShutdownEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.jboss.logging.Logger;

import java.util.List;
import java.util.function.Supplier;

/**
 * signals.in batch consumer. The batch is processed in one DB transaction; the method returns (and offsets are
 * committed) only after the commit. Transient DB failures are retried in place to preserve per-partition order.
 */
@ApplicationScoped
public class SignalsConsumer {
    private static final Logger LOG = Logger.getLogger(SignalsConsumer.class);

    @Inject Engine engine;
    private volatile boolean stopping;

    void onStop(@Observes ShutdownEvent e) { stopping = true; }

    @Incoming("signals-in")
    @Blocking
    public void consume(List<String> batch) {
        if (batch.isEmpty()) return;
        retry("signals", () -> engine.ingest.ingest(batch), () -> stopping);
    }

    static <T> T retry(String what, Supplier<T> work, Supplier<Boolean> stopping) {
        long backoff = 100;
        while (true) {
            try {
                return work.get();
            } catch (RuntimeException e) {
                if (stopping.get()) throw e; // not acknowledged: redelivered after restart
                LOG.errorf("%s batch failed, retrying in %d ms: %s", what, backoff, e.getMessage());
                try { Thread.sleep(backoff); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); throw e; }
                backoff = Math.min(backoff * 2, 5000);
            }
        }
    }
}
