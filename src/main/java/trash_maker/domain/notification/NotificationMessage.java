package trash_maker.domain.notification;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import trash_maker.common.Json;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/** The notifications.out contract (schemas/notification-v1.json): key, headers and payload of one attempt. Pure. */
public record NotificationMessage(UUID notificationId, int attempt, String idempotencyKey, String action, String tenantId,
                                  String correlationKey, String ruleId, int ruleVersion, String templateId, UUID seriesId,
                                  Integer occurrenceIndex, Instant dueAt, Instant emittedAt, JsonNode body) {
    public static final int SCHEMA_VERSION = 1;

    public String key() { return tenantId + ":" + notificationId; }

    public Map<String, String> headers(Map<String, String> trace) {
        Map<String, String> h = new LinkedHashMap<>(trace);
        h.put("action", action);
        h.put("tenant_id", tenantId);
        h.put("notification_id", notificationId.toString());
        h.put("attempt", Integer.toString(attempt));
        h.put("schema_version", Integer.toString(SCHEMA_VERSION));
        h.put("content-type", "application/json");
        return h;
    }

    public String payload() {
        ObjectNode o = Json.object();
        o.put("schema_version", SCHEMA_VERSION);
        o.put("notification_id", notificationId.toString());
        o.put("attempt", attempt);
        o.put("idempotency_key", idempotencyKey);
        o.put("action", action);
        o.put("tenant_id", tenantId);
        o.put("correlation_key", correlationKey);
        o.put("rule_id", ruleId);
        o.put("rule_version", ruleVersion);
        o.put("template_id", templateId);
        if (seriesId != null) o.put("series_id", seriesId.toString());
        if (occurrenceIndex != null) o.put("occurrence_index", occurrenceIndex);
        if (dueAt != null) o.put("due_at", dueAt.toString());
        o.put("emitted_at", emittedAt.toString());
        o.set("body", body == null ? Json.object() : body);
        return Json.write(o);
    }
}
