package trash_maker.infra.kafka;

import trash_maker.core.ports.MessagePublisher;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

/** Idempotent Kafka producer (acks=all, enable.idempotence) used by the outbox relay. */
public final class KafkaMessagePublisher implements MessagePublisher, AutoCloseable {
    private final KafkaProducer<String, String> producer;

    public KafkaMessagePublisher(String bootstrap, Map<String, String> extra) {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        p.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        p.put(ProducerConfig.LINGER_MS_CONFIG, "5");
        p.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        p.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "15000");
        p.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "25000");
        p.put(ProducerConfig.CLIENT_ID_CONFIG, "notification-engine-relay");
        p.putAll(extra);
        producer = new KafkaProducer<>(p, new StringSerializer(), new StringSerializer());
    }

    @Override
    public CompletableFuture<Void> publish(String topic, String key, Map<String, String> headers, String payload) {
        ProducerRecord<String, String> rec = new ProducerRecord<>(topic, key, payload);
        headers.forEach((k, v) -> rec.headers().add(new RecordHeader(k, v.getBytes(StandardCharsets.UTF_8))));
        CompletableFuture<Void> f = new CompletableFuture<>();
        try {
            producer.send(rec, (md, ex) -> { if (ex != null) f.completeExceptionally(ex); else f.complete(null); });
        } catch (RuntimeException e) {
            f.completeExceptionally(e);
        }
        return f;
    }

    @Override
    public void close() { producer.close(java.time.Duration.ofSeconds(10)); }
}
