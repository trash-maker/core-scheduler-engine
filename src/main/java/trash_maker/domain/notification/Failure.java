package trash_maker.domain.notification;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import trash_maker.common.Json;

import java.time.Instant;

/** Last failure of a notification, exposed to children as $parent.lastFailure. */
public record Failure(String outcome, String reason, int attempt, Instant at) {
    public ObjectNode toJson() {
        ObjectNode o = Json.object();
        o.put("outcome", outcome);
        if (reason != null) o.put("reason", reason);
        o.put("attempt", attempt);
        o.put("at", at.toString());
        return o;
    }

    public static Failure fromJson(JsonNode n) {
        if (n == null || n.isNull()) return null;
        return new Failure(n.path("outcome").asText(), n.hasNonNull("reason") ? n.get("reason").asText() : null,
                n.path("attempt").asInt(), Instant.parse(n.path("at").asText()));
    }
}
