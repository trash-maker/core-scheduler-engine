package io.notifengine.infra.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.notifengine.common.Json;
import io.smallrye.reactive.messaging.kafka.Record;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.jboss.logging.Logger;

import java.time.Instant;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Demo actuator (role fake-actuator): consumes notifications.out with its own consumer group, filters by action,
 * and reports SUCCESS / FAILURE / PERMANENT_FAILURE or nothing (to exercise attempt timeouts).
 */
@ApplicationScoped
public class FakeActuator {
    private static final Logger LOG = Logger.getLogger(FakeActuator.class);

    @Inject @Channel("fake-results-out") Emitter<Record<String, String>> results;
    @ConfigProperty(name = "notifications.fake-actuator.actions", defaultValue = "*") Set<String> actions;
    @ConfigProperty(name = "notifications.fake-actuator.failure-rate", defaultValue = "0.1") double failureRate;
    @ConfigProperty(name = "notifications.fake-actuator.permanent-failure-rate", defaultValue = "0.02") double permanentRate;
    @ConfigProperty(name = "notifications.fake-actuator.silent-rate", defaultValue = "0.02") double silentRate;

    @Incoming("fake-actuator-in")
    public void handle(String payload) {
        JsonNode n = Json.parse(payload);
        String action = n.path("action").asText();
        if (!actions.contains("*") && !actions.contains(action)) return;
        double r = ThreadLocalRandom.current().nextDouble();
        if (r < silentRate) return;
        String outcome = r < silentRate + permanentRate ? "PERMANENT_FAILURE" : r < silentRate + permanentRate + failureRate ? "FAILURE" : "SUCCESS";
        ObjectNode o = Json.object();
        o.put("notification_id", n.path("notification_id").asText());
        o.put("attempt", n.path("attempt").asInt());
        o.put("action", action);
        o.put("tenant_id", n.path("tenant_id").asText());
        o.put("outcome", outcome);
        if (!outcome.equals("SUCCESS")) o.put("reason", "simulated " + outcome.toLowerCase());
        o.put("occurred_at", Instant.now().toString());
        results.send(Record.of(n.path("tenant_id").asText() + ":" + n.path("notification_id").asText(), Json.write(o)));
        LOG.debugf("fake actuator %s attempt %d -> %s", n.path("notification_id").asText(), n.path("attempt").asInt(), outcome);
    }
}
