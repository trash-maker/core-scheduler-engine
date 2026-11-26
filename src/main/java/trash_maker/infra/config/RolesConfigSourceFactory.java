package trash_maker.infra.config;

import io.smallrye.config.ConfigSourceContext;
import io.smallrye.config.ConfigSourceFactory;
import io.smallrye.config.ConfigValue;
import io.smallrye.config.common.MapBackedConfigSource;
import org.eclipse.microprofile.config.spi.ConfigSource;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Derives the Kafka channel "enabled" flags from notifications.roles, so one artifact runs any combination of roles.
 * Ordinal 260: above application.properties (250), below environment variables (300) - an explicit
 * MP_MESSAGING_INCOMING_X_ENABLED still wins.
 */
public class RolesConfigSourceFactory implements ConfigSourceFactory {
    static final Map<String, String> CHANNEL_ROLE = Map.of(
            "mp.messaging.incoming.signals-in.enabled", "ingest",
            "mp.messaging.incoming.results-in.enabled", "results",
            "mp.messaging.incoming.fake-actuator-in.enabled", "fake-actuator",
            "mp.messaging.outgoing.fake-results-out.enabled", "fake-actuator");

    @Override
    public Iterable<ConfigSource> getConfigSources(ConfigSourceContext context) {
        ConfigValue roles = context.getValue("notifications.roles");
        Set<String> set = roles == null || roles.getValue() == null ? Set.of("ingest", "timers", "relay", "results", "api")
                : Arrays.stream(roles.getValue().split(",")).map(String::trim).collect(Collectors.toSet());
        Map<String, String> props = new HashMap<>();
        CHANNEL_ROLE.forEach((k, role) -> props.put(k, Boolean.toString(set.contains(role))));
        return List.of(new MapBackedConfigSource("notification-engine-roles", props, 260) {});
    }
}
