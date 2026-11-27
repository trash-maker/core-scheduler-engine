package trash_maker.support;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.enterprise.inject.Alternative;
import jakarta.inject.Singleton;
import trash_maker.common.Json;
import trash_maker.core.Ports.MessagePublisher;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/** In-memory broker double (CDI alternative): records every message; can fail or run a hook after recording. */
@Alternative
@Singleton
public class RecordingPublisher implements MessagePublisher {
    public record Msg(String topic, String key, Map<String, String> headers, JsonNode payload) {}

    public final List<Msg> messages = new CopyOnWriteArrayList<>();
    public final AtomicInteger failRemaining = new AtomicInteger();
    /** Runs after a message has been accepted by the "broker" (e.g. to simulate a crash before the DB commit). */
    public volatile Runnable afterPublish = () -> {};

    public void reset() {
        messages.clear();
        failRemaining.set(0);
        afterPublish = () -> {};
    }

    @Override
    public CompletableFuture<Void> publish(String topic, String key, Map<String, String> headers, String payload) {
        if (failRemaining.get() > 0 && failRemaining.getAndDecrement() > 0)
            return CompletableFuture.failedFuture(new RuntimeException("broker unavailable"));
        messages.add(new Msg(topic, key, Map.copyOf(headers), Json.parse(payload)));
        afterPublish.run();
        return CompletableFuture.completedFuture(null);
    }

    public List<Msg> on(String topic) { return messages.stream().filter(m -> m.topic().equals(topic)).toList(); }
}
