package io.notifengine.core.dispatch;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.notifengine.common.Json;
import io.notifengine.core.notifications.NotificationRow;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/** Builds notifications.out messages (schema: schemas/notification-v1.json). */
public final class MessageFactory {
    public static final int SCHEMA_VERSION = 1;

    private MessageFactory() {}

    public static String key(NotificationRow n) { return n.tenantId() + ":" + n.id(); }

    public static String payload(NotificationRow n, int attempt, Instant now) {
        ObjectNode o = Json.object();
        o.put("schema_version", SCHEMA_VERSION);
        o.put("notification_id", n.id().toString());
        o.put("attempt", attempt);
        o.put("idempotency_key", n.idempotencyKey());
        o.put("action", n.action());
        o.put("tenant_id", n.tenantId());
        o.put("correlation_key", n.correlationKey());
        o.put("rule_id", n.ruleId());
        o.put("rule_version", n.ruleVersion());
        o.put("template_id", n.templateId());
        if (n.seriesId() != null) o.put("series_id", n.seriesId().toString());
        if (n.occurrenceIndex() != null) o.put("occurrence_index", n.occurrenceIndex());
        if (n.dueAt() != null) o.put("due_at", n.dueAt().toString());
        o.put("emitted_at", now.toString());
        o.set("body", n.body() == null ? Json.object() : n.body());
        return Json.write(o);
    }

    public static Map<String, String> headers(NotificationRow n, int attempt, Map<String, String> trace) {
        Map<String, String> h = new LinkedHashMap<>(trace);
        h.put("action", n.action());
        h.put("tenant_id", n.tenantId());
        h.put("notification_id", n.id().toString());
        h.put("attempt", Integer.toString(attempt));
        h.put("schema_version", Integer.toString(SCHEMA_VERSION));
        h.put("content-type", "application/json");
        return h;
    }
}
