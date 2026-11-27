package trash_maker.domain.notification;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import trash_maker.common.Json;
import trash_maker.domain.rule.Rule.RetryPolicy;

import java.time.Duration;
import java.time.Instant;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/** Notification lifecycle types: statuses, events, effects, transitions, outcomes and the output message contract. */
public final class Lifecycle {
    private Lifecycle() {}

    public enum NotificationStatus {
        WAITING_ON_PARENT, CREATED, SCHEDULED, PUBLISHED, SUCCEEDED, FAILED, RETRY_SCHEDULED,
        RETRIES_EXHAUSTED, CANCELLED, EXPIRED, SKIPPED;

        /** States after which nothing happens on its own (RETRIES_EXHAUSTED may still accept a late SUCCESS or a manual re-trigger). */
        public static final Set<NotificationStatus> TERMINAL = EnumSet.of(SUCCEEDED, RETRIES_EXHAUSTED, CANCELLED, EXPIRED, SKIPPED);

        public static final Set<NotificationStatus> ACTIVE = EnumSet.of(WAITING_ON_PARENT, CREATED, SCHEDULED, PUBLISHED, RETRY_SCHEDULED);

        public boolean terminal() { return TERMINAL.contains(this); }
    }

    /** Inputs of the notification state machine. */
    public sealed interface Event {
        String name();

        /** Initial processing right after creation (CREATED). */
        record Start() implements Event { public String name() { return "START"; } }

        /** Parent trigger matched: WAITING_ON_PARENT -> CREATED -> ... with the computed due time. */
        record Activate(Instant dueAt) implements Event { public String name() { return "ACTIVATE"; } }

        /** PUBLISH timer fired (first publish or retry). */
        record PublishDue() implements Event { public String name() { return "PUBLISH_DUE"; } }

        record Outcome(int attempt, OutcomeType outcome, String reason, Instant occurredAt) implements Event { public String name() { return "OUTCOME_" + outcome; } }

        record AttemptTimeout(int attempt) implements Event { public String name() { return "ATTEMPT_TIMEOUT"; } }

        record Cancel(String reason) implements Event { public String name() { return "CANCEL"; } }

        record Expire() implements Event { public String name() { return "EXPIRE"; } }

        record Retrigger(String requestedBy) implements Event { public String name() { return "RETRIGGER"; } }

        record Skip(String reason) implements Event { public String name() { return "SKIP"; } }
    }

    /** Side effects requested by the state machine, applied by the caller in the same DB transaction. */
    public sealed interface Effect {
        record ScheduleTimer(TimerKind kind, Instant dueAt) implements Effect {}

        /** Remove pending timers of these kinds for the notification. */
        record CancelTimers(Set<TimerKind> kinds) implements Effect {}

        /** Write the message for {@code attempt} to the outbox. The attempt timeout timer is armed after the Kafka ack. */
        record Emit(int attempt, Duration attemptTimeout) implements Effect {}
    }

    /** One recorded status change (append-only history row). */
    public record StatusChange(NotificationStatus from, NotificationStatus to, String event, int attempt, String reason) {}

    /** Result of applying an event: new state, recorded changes, effects. {@code ignored} = no-op (stale/duplicate input). */
    public record Transition(NotificationState state, List<StatusChange> changes, List<Effect> effects, String ignored) {

        public boolean isIgnored() { return ignored != null; }

        public boolean changed() { return !changes.isEmpty(); }

        /** Every status entered by this transition, in order (used for child triggers). */
        public List<NotificationStatus> entered() { return changes.stream().map(StatusChange::to).collect(Collectors.toList()); }
    }

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

    /** The part of a notification the state machine reads and writes. Immutable. */
    public record NotificationState(UUID id, NotificationStatus status, String statusReason, int attempt, int cycleAttempts,
                                    RetryPolicy retryPolicy, Instant dueAt, Instant expiresAt, Long attemptTimerId,
                                    boolean failedOnce, Failure lastFailure, Instant publishedAt) {

        NotificationState with(NotificationStatus s, String reason) {
            return new NotificationState(id, s, reason, attempt, cycleAttempts, retryPolicy, dueAt, expiresAt, attemptTimerId, failedOnce, lastFailure, publishedAt);
        }

        NotificationState withAttempt(int attempt, int cycle, Instant publishedAt) {
            return new NotificationState(id, status, statusReason, attempt, cycle, retryPolicy, dueAt, expiresAt, null, failedOnce, lastFailure, publishedAt);
        }

        NotificationState withFailure(Failure f) {
            return new NotificationState(id, status, statusReason, attempt, cycleAttempts, retryPolicy, dueAt, expiresAt, null, true, f, publishedAt);
        }

        NotificationState withDue(Instant due) {
            return new NotificationState(id, status, statusReason, attempt, cycleAttempts, retryPolicy, due, expiresAt, attemptTimerId, failedOnce, lastFailure, publishedAt);
        }

        NotificationState withExpires(Instant exp) {
            return new NotificationState(id, status, statusReason, attempt, cycleAttempts, retryPolicy, dueAt, exp, attemptTimerId, failedOnce, lastFailure, publishedAt);
        }
    }

    public enum OutcomeType { SUCCESS, FAILURE, PERMANENT_FAILURE }

    public enum TimerKind { PUBLISH, ATTEMPT_TIMEOUT, EXPIRE, NEXT_OCCURRENCE, RULE_EVAL }

    public static class IllegalTransitionException extends RuntimeException {
        public IllegalTransitionException(String m) { super(m); }
    }

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
}
