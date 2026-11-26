package io.notifengine.infra.config;

import io.smallrye.config.ConfigSourceInterceptor;
import io.smallrye.config.ConfigSourceInterceptorContext;
import io.smallrye.config.ConfigValue;
import io.smallrye.config.Priorities;
import jakarta.annotation.Priority;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Derives the Kafka channel "enabled" flags from notifications.roles, so one artifact runs any combination of roles
 * (an explicit mp.messaging.incoming.X.enabled always wins).
 */
@Priority(Priorities.APPLICATION + 100)
public class RolesConfigInterceptor implements ConfigSourceInterceptor {
    private static final Map<String, String> CHANNEL_ROLE = Map.of(
            "mp.messaging.incoming.signals-in.enabled", "ingest",
            "mp.messaging.incoming.results-in.enabled", "results",
            "mp.messaging.incoming.fake-actuator-in.enabled", "fake-actuator",
            "mp.messaging.outgoing.fake-results-out.enabled", "fake-actuator");

    @Override
    public ConfigValue getValue(ConfigSourceInterceptorContext context, String name) {
        ConfigValue explicit = context.proceed(name);
        String role = CHANNEL_ROLE.get(name);
        if (role == null || explicit != null) return explicit;
        ConfigValue roles = context.proceed("notifications.roles");
        Set<String> set = roles == null || roles.getValue() == null ? Set.of("ingest", "timers", "relay", "results", "api")
                : Arrays.stream(roles.getValue().split(",")).map(String::trim).collect(Collectors.toSet());
        return ConfigValue.builder().withName(name).withValue(Boolean.toString(set.contains(role))).build();
    }
}
