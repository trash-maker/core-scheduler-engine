package trash_maker.load;

import jakarta.annotation.PostConstruct;
import jakarta.enterprise.inject.Alternative;
import jakarta.inject.Singleton;
import org.eclipse.microprofile.config.ConfigProvider;
import trash_maker.common.Json;
import trash_maker.core.ports.MessagePublisher;
import trash_maker.infra.kafka.KafkaMessagePublisher;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

/** Broker that measures due -> ack lag; delegates to a real Kafka producer when -Dload.kafka=true. */
@Alternative
@Singleton
public class LoadSink implements MessagePublisher {
    final ConcurrentLinkedQueue<Long> publishLag = new ConcurrentLinkedQueue<>();
    final AtomicLong count = new AtomicLong();
    private MessagePublisher delegate;

    @PostConstruct
    void init() {
        if (LoadProfile.KAFKA)
            delegate = new KafkaMessagePublisher(ConfigProvider.getConfig().getValue("kafka.bootstrap.servers", String.class), Map.of());
    }

    void reset() { publishLag.clear(); count.set(0); }

    @Override
    public CompletableFuture<Void> publish(String topic, String key, Map<String, String> headers, String payload) {
        CompletableFuture<Void> f = delegate == null ? CompletableFuture.completedFuture(null) : delegate.publish(topic, key, headers, payload);
        return f.thenRun(() -> {
            String due = Json.parse(payload).path("due_at").asText(null);
            if (due != null) publishLag.add(Duration.between(Instant.parse(due), Instant.now()).toMillis());
            count.incrementAndGet();
        });
    }
}
