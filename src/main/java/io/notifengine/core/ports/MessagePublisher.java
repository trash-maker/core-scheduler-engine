package io.notifengine.core.ports;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/** Port to the message broker (Kafka in production, in-memory in tests). Must be idempotent per message. */
public interface MessagePublisher {
    CompletableFuture<Void> publish(String topic, String key, Map<String, String> headers, String payload);
}
