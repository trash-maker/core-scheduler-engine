package trash_maker.infra;

import io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.OnOverflow;
import trash_maker.core.Ports.MessagePublisher;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Broker port on the SmallRye Kafka channel {@code relay-out} (idempotent producer, acks=all; see application.properties).
 * Topic, key and headers travel as record metadata; the returned future completes on the broker ack.
 */
@Singleton
public class KafkaPublisher implements MessagePublisher {
    @Inject
    @Channel("relay-out")
    @OnOverflow(value = OnOverflow.Strategy.BUFFER, bufferSize = 8192)
    Emitter<String> emitter;

    @Override
    public CompletableFuture<Void> publish(String topic, String key, Map<String, String> headers, String payload) {
        RecordHeaders h = new RecordHeaders();
        headers.forEach((k, v) -> h.add(k, v.getBytes(StandardCharsets.UTF_8)));
        CompletableFuture<Void> acked = new CompletableFuture<>();
        Message<String> msg = Message.of(payload)
                .addMetadata(OutgoingKafkaRecordMetadata.<String>builder().withTopic(topic).withKey(key).withHeaders(h).build())
                .withAck(() -> { acked.complete(null); return CompletableFuture.completedFuture(null); })
                .withNack(t -> { acked.completeExceptionally(t); return CompletableFuture.completedFuture(null); });
        try {
            emitter.send(msg);
        } catch (RuntimeException e) {
            acked.completeExceptionally(e);
        }
        return acked;
    }
}
