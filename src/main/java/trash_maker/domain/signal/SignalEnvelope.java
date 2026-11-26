package trash_maker.domain.signal;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import trash_maker.common.Json;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.regex.Pattern;

/** Envelope of a message on signals.in (schema: schemas/signal-v1.json). */
public record SignalEnvelope(String signalId, String tenantId, String correlationKey, String signal,
                             SignalKind kind, JsonNode rawValue, Value value, Instant occurredAt) {

    private static final Pattern NAME = Pattern.compile("[A-Za-z0-9_.:-]{1,128}");
    private static final int MAX_KEY = 256;

    public static SignalEnvelope parse(JsonNode n) {
        String signalId = text(n, "signal_id");
        String tenant = text(n, "tenant_id");
        String key = text(n, "correlation_key");
        String signal = text(n, "signal");
        String kindS = text(n, "kind");
        String occurred = text(n, "occurred_at");
        if (signalId.length() > MAX_KEY || key.length() > MAX_KEY) throw new IllegalArgumentException("signal_id/correlation_key too long");
        if (!NAME.matcher(tenant).matches()) throw new IllegalArgumentException("invalid tenant_id");
        if (!NAME.matcher(signal).matches()) throw new IllegalArgumentException("invalid signal name");
        SignalKind kind;
        try { kind = SignalKind.valueOf(kindS); } catch (IllegalArgumentException e) { throw new IllegalArgumentException("kind must be KPI or DATE"); }
        Instant at;
        try { at = Instant.parse(occurred); } catch (DateTimeParseException e) {
            try { at = java.time.OffsetDateTime.parse(occurred).toInstant(); } catch (DateTimeParseException e2) {
                throw new IllegalArgumentException("occurred_at must be ISO-8601 instant");
            }
        }
        JsonNode raw = n.get("value");
        Value v = Value.fromJson(kind, raw);
        return new SignalEnvelope(signalId, tenant, key, signal, kind, raw, v, at);
    }

    private static String text(JsonNode n, String f) {
        JsonNode v = n.get(f);
        if (v == null || !v.isTextual() || v.textValue().isBlank()) throw new IllegalArgumentException("missing or invalid field '" + f + "'");
        return v.textValue();
    }

    /** JSON form exposed to {"$trigger": "$.x"} references. */
    public ObjectNode toJson() {
        ObjectNode o = Json.object();
        o.put("signal_id", signalId);
        o.put("tenant_id", tenantId);
        o.put("correlation_key", correlationKey);
        o.put("signal", signal);
        o.put("kind", kind.name());
        o.set("value", rawValue);
        o.put("occurred_at", occurredAt.toString());
        return o;
    }

    public SignalValue asSignalValue() { return new SignalValue(signal, kind, value, occurredAt); }
}
