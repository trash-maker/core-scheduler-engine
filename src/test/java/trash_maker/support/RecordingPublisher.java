package trash_maker.support;

import com.fasterxml.jackson.databind.JsonNode;
import trash_maker.common.Json;
import trash_maker.core.ports.MessagePublisher;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/** In-memory broker double: records every published message; can be told to fail. */
public final class RecordingPublisher implements MessagePublisher {
    public record Msg(String topic, String key, Map<String, String> headers, JsonNode payload) {}

    public final List<Msg> messages = new CopyOnWriteArrayList<>();
    public final AtomicInteger failRemaining = new AtomicInteger();

    @Override
    public CompletableFuture<Void> publish(String topic, String key, Map<String, String> headers, String payload) {
        if (failRemaining.get() > 0 && failRemaining.getAndDecrement() > 0)
            return CompletableFuture.failedFuture(new RuntimeException("broker unavailable"));
        messages.add(new Msg(topic, key, Map.copyOf(headers), Json.parse(payload)));
        return CompletableFuture.completedFuture(null);
    }

    public List<Msg> on(String topic) { return messages.stream().filter(m -> m.topic().equals(topic)).toList(); }
}
