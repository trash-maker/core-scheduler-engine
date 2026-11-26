package trash_maker.domain.notification;

import com.fasterxml.jackson.databind.JsonNode;
import trash_maker.common.Json;

import java.time.Instant;
import java.util.UUID;

/** An actuator outcome from notification.results (schemas/notification-result-v1.json) and its pure validation. */
public record OutcomeMessage(UUID notificationId, int attempt, String action, OutcomeType outcome, String reason,
                             Instant occurredAt, String tenantId) {

    public enum Verdict { APPLY, UNKNOWN_NOTIFICATION, TENANT_MISMATCH, ACTION_MISMATCH }

    public static OutcomeMessage parse(String raw) {
        JsonNode n = Json.parse(raw);
        String id = n.path("notification_id").asText(null);
        if (id == null) throw new IllegalArgumentException("missing notification_id");
        if (!n.path("attempt").canConvertToInt() || n.path("attempt").asInt() < 1) throw new IllegalArgumentException("attempt must be >= 1");
        String action = n.path("action").asText(null);
        if (action == null) throw new IllegalArgumentException("missing action");
        OutcomeType o;
        try { o = OutcomeType.valueOf(n.path("outcome").asText()); }
        catch (IllegalArgumentException e) { throw new IllegalArgumentException("outcome must be SUCCESS, FAILURE or PERMANENT_FAILURE"); }
        Instant at = n.hasNonNull("occurred_at") ? Instant.parse(n.get("occurred_at").asText()) : Instant.EPOCH;
        return new OutcomeMessage(UUID.fromString(id), n.get("attempt").asInt(), action, o,
                n.hasNonNull("reason") ? n.get("reason").asText() : null, at, n.hasNonNull("tenant_id") ? n.get("tenant_id").asText() : null);
    }

    /**
     * Correlation check against the stored notification (null = not found): results are accepted only for the
     * notification's tenant and action. Attempt checks belong to the state machine.
     */
    public Verdict check(String storedTenant, String storedAction) {
        if (storedTenant == null) return Verdict.UNKNOWN_NOTIFICATION;
        if (tenantId != null && !tenantId.equals(storedTenant)) return Verdict.TENANT_MISMATCH;
        if (!action.equals(storedAction)) return Verdict.ACTION_MISMATCH;
        return Verdict.APPLY;
    }

    public Event.Outcome toEvent() { return new Event.Outcome(attempt, outcome, reason, occurredAt); }
}
