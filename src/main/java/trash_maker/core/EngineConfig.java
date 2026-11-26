package trash_maker.core;

import java.time.Duration;
import java.util.Set;

/** Runtime configuration of the core engine (mapped from notifications.* in application.properties). */
public record EngineConfig(
        String outputTopic,
        String dlqTopic,
        boolean storeUnreferencedSignals,
        Duration defaultSignalMaxAge,
        Set<String> allowedActions,
        int timerBatchSize,
        int timerMaxFailures,
        Duration timerFailureBackoff,
        Duration idleMin,
        Duration idleMax,
        int sweepPageSize,
        int relayBatchSize,
        Duration retention,
        Duration dedupRetention,
        Duration inboxRetention) {

    public static EngineConfig defaults() {
        return new EngineConfig("notifications.out", "notifications.dlq", false, null, Set.of(), 200, 5,
                Duration.ofSeconds(1), Duration.ofMillis(100), Duration.ofMillis(500), 500, 500,
                Duration.ofDays(30), Duration.ofDays(7), Duration.ofDays(7));
    }

    public EngineConfig withTimerBatchSize(int n) {
        return new EngineConfig(outputTopic, dlqTopic, storeUnreferencedSignals, defaultSignalMaxAge, allowedActions, n,
                timerMaxFailures, timerFailureBackoff, idleMin, idleMax, sweepPageSize, relayBatchSize, retention, dedupRetention, inboxRetention);
    }

    public EngineConfig withStoreUnreferenced(boolean b) {
        return new EngineConfig(outputTopic, dlqTopic, b, defaultSignalMaxAge, allowedActions, timerBatchSize,
                timerMaxFailures, timerFailureBackoff, idleMin, idleMax, sweepPageSize, relayBatchSize, retention, dedupRetention, inboxRetention);
    }

    public EngineConfig withSweepPageSize(int n) {
        return new EngineConfig(outputTopic, dlqTopic, storeUnreferencedSignals, defaultSignalMaxAge, allowedActions, timerBatchSize,
                timerMaxFailures, timerFailureBackoff, idleMin, idleMax, n, relayBatchSize, retention, dedupRetention, inboxRetention);
    }
}
