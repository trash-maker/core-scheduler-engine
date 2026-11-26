package trash_maker.support;

import io.quarkus.test.junit.QuarkusTestProfile;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Deterministic engine tests: real PostgreSQL, controllable clock, in-memory broker, no background workers,
 * consumers or scheduled jobs (tests drive timers / relay / ingestion explicitly).
 */
public class CoreProfile implements QuarkusTestProfile {
    @Override
    public Set<Class<?>> getEnabledAlternatives() { return Set.of(MutableClock.class, RecordingPublisher.class); }

    @Override
    public Map<String, String> getConfigOverrides() {
        return Map.of("notifications.roles", "api", "quarkus.scheduler.enabled", "false");
    }

    @Override
    public List<TestResourceEntry> testResources() { return List.of(new TestResourceEntry(PostgresResource.class)); }

    @Override
    public String getConfigProfile() { return "test"; }
}
