package trash_maker.domain.rule;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import trash_maker.common.IsoAmount;
import trash_maker.common.Json;
import trash_maker.domain.expr.Evaluator;
import trash_maker.domain.expr.Expr.Node;
import trash_maker.domain.expr.Expr.OnMissing;
import trash_maker.domain.notification.Lifecycle.NotificationStatus;
import trash_maker.domain.recurrence.Recurrence.CronRecurrence;
import trash_maker.domain.recurrence.Recurrence.PeriodRecurrence;
import trash_maker.domain.recurrence.Recurrence;
import trash_maker.domain.signal.Signals.SignalValue;
import trash_maker.domain.signal.Signals.Value;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Rule model: definition, trigger, templates, time specs, retry policy and the compiled form. */
public final class Rule {
    private Rule() {}

    public record RuleDefinition(String id, int version, String tenantId, boolean enabled, ZoneId timezone,
                                 Enums.MissingValuePolicy onMissingValue, TriggerDef trigger,
                                 List<TemplateDef> templates, JsonNode raw) {}

    public record TriggerDef(Enums.FiringMode mode, OnMissing onMissing, Duration cooldown, Node expression, JsonNode expressionJson) {}

    public record TemplateDef(
            String id,
            Enums.NotificationType type,
            String action,
            TimeSpec dueAt,
            boolean dueAtFloorNow,
            TimeSpec expiresAt,
            IsoAmount expiresAfter,
            RecurrenceSpec recurrence,
            TimeSpec startAt,
            UntilSpec until,
            Integer maxOccurrences,
            Enums.MisfirePolicy misfirePolicy,
            Duration misfireGrace,
            List<String> cancelOnEvents,
            RetryPolicy retryPolicy,
            ParentSpec parent,
            JsonNode body,
            JsonNode bodySchema,
            Enums.MissingValuePolicy onMissingValue,
            JsonNode raw) {

        public boolean isRecurring() { return type == Enums.NotificationType.RECURRING; }

        public boolean isWhileInState() { return parent != null && parent.trigger() == Enums.ParentTrigger.WHILE_IN_STATE; }
    }

    public record ParentSpec(String templateId, Enums.ParentTrigger trigger, Duration delay,
                             Set<NotificationStatus> states, UntilSpec until, Integer maxOccurrences, boolean once) {}

    /**
     * Point in time: base (NOW or a DATE signal) + ISO offset, optionally set to a local time of day in the rule zone.
     * JSON: {"from":"NOW"|"$signal:due_date", "offset":"-P3D", "atTime":"09:00"}
     */
    public record TimeSpec(String fromSignal, IsoAmount offset, LocalTime atTime) {

        public static final TimeSpec NOW = new TimeSpec(null, IsoAmount.ZERO, null);

        public static class MissingBaseException extends RuntimeException {
            public MissingBaseException(String m) { super(m); }
        }

        public Instant resolve(Instant now, ZoneId zone, Map<String, SignalValue> signals) {
            ZonedDateTime base;
            if (fromSignal == null) base = now.atZone(zone);
            else {
                SignalValue sv = signals.get(fromSignal);
                if (sv == null || !(sv.value() instanceof Value.Date d)) throw new MissingBaseException("DATE signal '" + fromSignal + "' required for time computation is missing");
                base = d.at(zone);
            }
            ZonedDateTime t = offset.addTo(base);
            if (atTime != null) t = ZonedDateTime.ofLocal(t.toLocalDate().atTime(atTime), zone, t.getOffset());
            return t.toInstant();
        }

        public Instant resolveFrom(Instant start, ZoneId zone) {
            ZonedDateTime t = offset.addTo(start.atZone(zone));
            if (atTime != null) t = ZonedDateTime.ofLocal(t.toLocalDate().atTime(atTime), zone, t.getOffset());
            return t.toInstant();
        }
    }

    /** End of a series: either an absolute {@link TimeSpec} or an amount after the series start. */
    public record UntilSpec(TimeSpec at, IsoAmount afterStart) {}

    /** Series recurrence: cron or fixed ISO period anchored at the series start. */
    public record RecurrenceSpec(String cron, IsoAmount every) {
        public Recurrence build(Instant start, ZoneId zone) {
            return cron != null ? new CronRecurrence(cron, zone) : new PeriodRecurrence(start.atZone(zone), every);
        }
    }

    /**
     * Delay before retry n (n = 1 for the first retry) = min(initialDelay * multiplier^(n-1), maxDelay) +/- jitter.
     * jitter is a fraction (0.1 = +/-10%). maxAttempts counts all attempts (default 1 = no retry).
     */
    public record RetryPolicy(int maxAttempts, Enums.BackoffKind backoff, Duration initialDelay, double multiplier,
                              Duration maxDelay, double jitter, Duration attemptTimeout) {

        public static final RetryPolicy DEFAULT = new RetryPolicy(1, Enums.BackoffKind.FIXED, Duration.ofMinutes(1), 2.0,
                Duration.ofHours(1), 0.0, null);

        /** @param random uniform value in [0,1) (injected to keep this pure and testable) */
        public Duration delay(int retryNumber, double random) {
            double base = initialDelay.toMillis();
            if (backoff == Enums.BackoffKind.EXPONENTIAL) base = base * Math.pow(multiplier, Math.max(0, retryNumber - 1));
            if (maxDelay != null) base = Math.min(base, maxDelay.toMillis());
            if (jitter > 0) base = base * (1 + jitter * (2 * random - 1));
            return Duration.ofMillis(Math.max(0, Math.round(base)));
        }

        public ObjectNode toJson() {
            ObjectNode o = Json.object();
            o.put("maxAttempts", maxAttempts);
            o.put("backoff", backoff.name());
            o.put("initialDelay", initialDelay.toString());
            o.put("multiplier", multiplier);
            if (maxDelay != null) o.put("maxDelay", maxDelay.toString());
            o.put("jitter", jitter);
            if (attemptTimeout != null) o.put("attemptTimeout", attemptTimeout.toString());
            return o;
        }

        public static RetryPolicy fromJson(JsonNode n) {
            return new RetryPolicy(n.path("maxAttempts").asInt(1), Enums.BackoffKind.valueOf(n.path("backoff").asText("FIXED")),
                    Duration.parse(n.path("initialDelay").asText("PT1M")), n.path("multiplier").asDouble(2.0),
                    n.hasNonNull("maxDelay") ? Duration.parse(n.get("maxDelay").asText()) : null, n.path("jitter").asDouble(0),
                    n.hasNonNull("attemptTimeout") ? Duration.parse(n.get("attemptTimeout").asText()) : null);
        }
    }

    /** Holder for small rule enums. */
    public static final class Enums {
        private Enums() {}

        public enum FiringMode { ON_BECOMES_TRUE, EVERY_EVALUATION_TRUE }

        public enum NotificationType { IMMEDIATE, SCHEDULED, RECURRING }

        public enum MissingValuePolicy { FAIL, USE_DEFAULT, OMIT_FIELD }

        public enum ParentTrigger { ON_FAILED, ON_RETRIES_EXHAUSTED, ON_SUCCEEDED, ON_PUBLISHED, ON_CANCELLED, ON_EXPIRED, WHILE_IN_STATE }

        public enum BackoffKind { FIXED, EXPONENTIAL }

        public enum MisfirePolicy { SKIP }
    }

    /**
     * A validated rule version compiled once: expression tree plus derived indexes.
     *
     * @param exprSignals      signals referenced by the trigger expression (drive evaluation)
     * @param contextSignals   signals needed to build notifications (bodies, dueAt/startAt/expiresAt bases)
     * @param cancelSignals    signal name -> template ids cancelled by it
     * @param hasNowCondition  expression contains a NOW-based dateMatches (needs a per-rule sweep timer)
     * @param entityTimed      expression has per-entity time behaviour (maxAge, for, within, NOW-relative comparisons)
     */
    public record CompiledRule(RuleDefinition def, Set<String> exprSignals, Set<String> contextSignals,
                               Map<String, Set<String>> cancelSignals, boolean hasNowCondition, boolean entityTimed,
                               Map<String, TemplateDef> templates, List<TemplateDef> roots,
                               Map<String, List<TemplateDef>> children, Map<String, Integer> depth) {

        public String tenantId() { return def.tenantId(); }

        public String id() { return def.id(); }

        public int version() { return def.version(); }

        public Node expression() { return def.trigger().expression(); }

        /** All signal names this rule needs stored (expression, context and cancel events). */
        public Set<String> referencedSignals() {
            var s = new java.util.LinkedHashSet<>(exprSignals);
            s.addAll(contextSignals);
            s.addAll(cancelSignals.keySet());
            return s;
        }

        public Instant nextRuleChange(Instant from) { return hasNowCondition ? Evaluator.nextRuleChange(expression(), from) : null; }
    }
}
