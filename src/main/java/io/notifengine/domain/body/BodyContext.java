package io.notifengine.domain.body;

import com.fasterxml.jackson.databind.JsonNode;
import io.notifengine.domain.signal.SignalValue;

import java.time.Instant;
import java.time.ZoneId;
import java.util.Map;

/**
 * Inputs for body resolution.
 * parentDeferred/occurrenceDeferred: resolution is a creation-time check; the real values are bound later
 * (child activation / each occurrence) from the frozen context.
 */
public record BodyContext(Map<String, SignalValue> signals, JsonNode trigger, JsonNode parent, boolean parentDeferred,
                          String correlationKey, Instant now, ZoneId zone, Integer occurrenceIndex,
                          boolean occurrenceDeferred, String ruleId, String templateId) {

    public BodyContext withParent(JsonNode p) {
        return new BodyContext(signals, trigger, p, false, correlationKey, now, zone, occurrenceIndex, occurrenceDeferred, ruleId, templateId);
    }

    public BodyContext withOccurrence(int index, Instant at) {
        return new BodyContext(signals, trigger, parent, parentDeferred, correlationKey, at, zone, index, false, ruleId, templateId);
    }
}
