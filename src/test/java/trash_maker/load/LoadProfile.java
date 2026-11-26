package trash_maker.load;

import io.quarkus.test.junit.QuarkusTestProfile;
import trash_maker.support.KafkaResource;
import trash_maker.support.PostgresResource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Load test: real clock, real worker loops (timers + relay roles), measuring sink; -Dload.kafka=true adds a real broker. */
public class LoadProfile implements QuarkusTestProfile {
    static final boolean KAFKA = Boolean.getBoolean("load.kafka");

    @Override public Set<Class<?>> getEnabledAlternatives() { return Set.of(LoadSink.class, LagMetrics.class); }

    @Override
    public Map<String, String> getConfigOverrides() {
        return Map.of("notifications.roles", "timers,relay", "quarkus.scheduler.enabled", "false",
                "notifications.timers.workers", Integer.getInteger("load.workers", 4).toString(),
                "notifications.relay.workers", Integer.getInteger("load.relays", 2).toString(),
                "notifications.relay.idle", "PT0.02S", "quarkus.datasource.jdbc.max-size", "40",
                "quarkus.log.category.\"trash_maker\".level", "WARN");
    }

    @Override
    public List<TestResourceEntry> testResources() {
        List<TestResourceEntry> r = new ArrayList<>(List.of(new TestResourceEntry(PostgresResource.class)));
        if (KAFKA) r.add(new TestResourceEntry(KafkaResource.class));
        return r;
    }
}
