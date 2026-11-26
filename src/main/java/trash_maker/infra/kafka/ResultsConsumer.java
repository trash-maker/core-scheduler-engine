package trash_maker.infra.kafka;

import trash_maker.core.Engine;
import io.smallrye.common.annotation.Blocking;
import io.quarkus.runtime.ShutdownEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Incoming;

import java.util.List;

/** notification.results batch consumer: outcomes applied in one transaction, offsets committed after it. */
@ApplicationScoped
public class ResultsConsumer {
    @Inject Engine engine;
    private volatile boolean stopping;

    void onStop(@Observes ShutdownEvent e) { stopping = true; }

    @Incoming("results-in")
    @Blocking
    public void consume(List<String> batch) {
        if (batch.isEmpty()) return;
        SignalsConsumer.retry("results", () -> engine.results.process(batch), () -> stopping);
    }
}
