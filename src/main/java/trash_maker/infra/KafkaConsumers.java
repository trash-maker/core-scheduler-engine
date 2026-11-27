package trash_maker.infra;

import io.smallrye.common.annotation.Blocking;
import io.smallrye.faulttolerance.api.ExponentialBackoff;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.faulttolerance.Retry;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import trash_maker.core.Engine;

import java.time.temporal.ChronoUnit;
import java.util.List;

/**
 * Batch consumers of signals.in and notification.results. Each batch is processed in one DB transaction (the
 * services are @Transactional); the method returns, and offsets are committed, only after the commit.
 * Transient failures are retried in place (fault tolerance, outside the transaction) to keep per-partition order.
 * The channels are enabled only on instances with the matching role.
 */
@ApplicationScoped
public class KafkaConsumers {
    @Inject Engine engine;

    @Incoming("signals-in")
    @Blocking
    @Retry(maxRetries = -1, delay = 100, maxDuration = 0, jitter = 50)
    @ExponentialBackoff(factor = 2, maxDelay = 5, maxDelayUnit = ChronoUnit.SECONDS)
    public void signals(List<String> batch) {
        if (!batch.isEmpty()) engine.ingest.ingest(batch);
    }

    @Incoming("results-in")
    @Blocking
    @Retry(maxRetries = -1, delay = 100, maxDuration = 0, jitter = 50)
    @ExponentialBackoff(factor = 2, maxDelay = 5, maxDelayUnit = ChronoUnit.SECONDS)
    public void results(List<String> batch) {
        if (!batch.isEmpty()) engine.results.process(batch);
    }
}
