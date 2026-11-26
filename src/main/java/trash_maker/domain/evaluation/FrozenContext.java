package trash_maker.domain.evaluation;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import trash_maker.common.Json;
import trash_maker.domain.signal.SignalKind;
import trash_maker.domain.signal.SignalValue;
import trash_maker.domain.signal.Value;

import java.time.Instant;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;

/** Snapshot of everything needed to resolve deferred body parts later, frozen at firing time. */
public record FrozenContext(String tenantId, String ruleId, String correlationKey, Instant firedAt, ZoneId zone,
                            Map<String, SignalValue> signals, JsonNode trigger) {

    public ObjectNode toJson() {
        ObjectNode o = Json.object();
        o.put("tenantId", tenantId);
        o.put("ruleId", ruleId);
        o.put("correlationKey", correlationKey);
        o.put("firedAt", firedAt.toString());
        o.put("zone", zone.getId());
        ObjectNode s = o.putObject("signals");
        signals.forEach((k, v) -> {
            ObjectNode x = s.putObject(k);
            x.put("kind", v.kind().name());
            x.set("value", v.value().toJson());
            x.put("occurredAt", v.occurredAt().toString());
        });
        if (trigger != null) o.set("trigger", trigger);
        return o;
    }

    public static FrozenContext fromJson(JsonNode o) {
        Map<String, SignalValue> sig = new HashMap<>();
        o.path("signals").fields().forEachRemaining(e -> {
            SignalKind k = SignalKind.valueOf(e.getValue().path("kind").asText());
            sig.put(e.getKey(), new SignalValue(e.getKey(), k, Value.fromJson(k, e.getValue().get("value")),
                    Instant.parse(e.getValue().path("occurredAt").asText())));
        });
        return new FrozenContext(o.path("tenantId").asText(), o.path("ruleId").asText(), o.path("correlationKey").asText(),
                Instant.parse(o.path("firedAt").asText()), ZoneId.of(o.path("zone").asText("UTC")), sig,
                o.hasNonNull("trigger") ? o.get("trigger") : null);
    }
}
