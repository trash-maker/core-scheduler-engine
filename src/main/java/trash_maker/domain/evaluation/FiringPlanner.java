package trash_maker.domain.evaluation;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import trash_maker.common.Json;
import trash_maker.domain.body.BodyContext;
import trash_maker.domain.body.BodyResolutionException;
import trash_maker.domain.body.BodyResolver;
import trash_maker.domain.body.BodySchemaValidator;
import trash_maker.domain.notification.NotificationStatus;
import trash_maker.domain.recurrence.Recurrence;
import trash_maker.domain.rule.CompiledRule;
import trash_maker.domain.rule.Enums;
import trash_maker.domain.rule.RetryPolicy;
import trash_maker.domain.rule.TemplateDef;
import trash_maker.domain.rule.TimeSpec;
import trash_maker.domain.rule.UntilSpec;
import trash_maker.domain.signal.SignalValue;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Pure planner: given a rule firing, computes every notification / series to create, with resolved bodies.
 * Shared by real processing and dry-run, so both produce identical results. All-or-nothing: any failure
 * throws {@link PlanningException} and nothing must be created.
 */
public final class FiringPlanner {
    private FiringPlanner() {}

    public record EntityContext(String tenantId, String correlationKey, Map<String, SignalValue> signals, JsonNode trigger) {}

    public record PlannedNotification(String templateId, String parentTemplateId, Enums.ParentTrigger parentTrigger, int depth,
                                      Enums.NotificationType type, String action, NotificationStatus initialStatus,
                                      Instant dueAt, Instant expiresAt, JsonNode body, boolean bodyDeferred,
                                      RetryPolicy retryPolicy, List<String> cancelOn, String idempotencyKey, ObjectNode spec) {}

    public record PlannedSeries(String templateId, String parentTemplateId, boolean waiting, String action,
                                Instant startAt, Instant firstDueAt, Instant untilAt, Integer maxOccurrences,
                                List<String> cancelOn, String idempotencyKey, ObjectNode spec) {}

    public record Plan(List<PlannedNotification> notifications, List<PlannedSeries> series, FrozenContext context) {}

    public static String idempotencyKey(CompiledRule r, String key, long edgeSeq, String templateId) {
        return r.tenantId() + "|" + r.id() + "|" + key + "|" + edgeSeq + "|" + templateId;
    }

    public static ObjectNode spec(CompiledRule r, TemplateDef t) {
        ObjectNode s = Json.object();
        s.set("template", t.raw());
        s.put("zone", r.def().timezone().getId());
        s.put("ruleMissingValuePolicy", r.def().onMissingValue().name());
        s.put("ruleVersion", r.version());
        return s;
    }

    public static Plan plan(CompiledRule rule, EntityContext ec, Instant now, long edgeSeq, BodySchemaValidator schemas) {
        ZoneId zone = rule.def().timezone();
        Map<String, SignalValue> ctxSignals = new LinkedHashMap<>();
        for (String s : rule.contextSignals()) if (ec.signals().containsKey(s)) ctxSignals.put(s, ec.signals().get(s));
        FrozenContext frozen = new FrozenContext(ec.tenantId(), rule.id(), ec.correlationKey(), now, zone, ctxSignals, ec.trigger());
        List<PlannedNotification> ns = new ArrayList<>();
        List<PlannedSeries> ss = new ArrayList<>();
        for (TemplateDef t : rule.def().templates()) {
            String idem = idempotencyKey(rule, ec.correlationKey(), edgeSeq, t.id());
            int depth = rule.depth().getOrDefault(t.id(), 0);
            boolean hasParent = t.parent() != null;
            BodyContext bc = new BodyContext(ec.signals(), ec.trigger(), null, hasParent, ec.correlationKey(), now, zone,
                    null, t.isRecurring(), rule.id(), t.id());
            if (t.isRecurring()) {
                checkBody(t, bc, schemas, false);
                if (t.isWhileInState()) {
                    ss.add(new PlannedSeries(t.id(), t.parent().templateId(), true, t.action(), null, null, null,
                            effMax(t), t.cancelOnEvents(), idem, spec(rule, t)));
                } else {
                    Instant start = time(t, t.startAt() == null ? TimeSpec.NOW : t.startAt(), now, zone, ec.signals());
                    Recurrence rec = t.recurrence().build(start, zone);
                    ZonedDateTime first = rec.nextOrSame(start.atZone(zone));
                    Instant until = untilAt(t, t.until(), start, now, zone, ec.signals());
                    ss.add(new PlannedSeries(t.id(), null, false, t.action(), start, first == null ? null : first.toInstant(), until,
                            effMax(t), t.cancelOnEvents(), idem, spec(rule, t)));
                }
                continue;
            }
            if (hasParent) {
                // validate time bases now (they come from the frozen context), resolve the body now unless it uses $parent
                if (t.dueAt() != null) time(t, t.dueAt(), now, zone, ec.signals());
                if (t.expiresAt() != null) time(t, t.expiresAt(), now, zone, ec.signals());
                boolean deferred = BodyResolver.usesParent(t.body());
                JsonNode body = checkBody(t, bc, schemas, !deferred);
                ns.add(new PlannedNotification(t.id(), t.parent().templateId(), t.parent().trigger(), depth, t.type(), t.action(),
                        NotificationStatus.WAITING_ON_PARENT, null, null, deferred ? null : body, deferred,
                        t.retryPolicy(), t.cancelOnEvents(), idem, spec(rule, t)));
                continue;
            }
            Instant due = t.type() == Enums.NotificationType.IMMEDIATE ? now : time(t, t.dueAt(), now, zone, ec.signals());
            if (t.dueAtFloorNow() && due.isBefore(now)) due = now;
            Instant expires = expiry(t, due, now, zone, ec.signals());
            JsonNode body = checkBody(t, bc, schemas, true);
            ns.add(new PlannedNotification(t.id(), null, null, depth, t.type(), t.action(), NotificationStatus.CREATED,
                    due, expires, body, false, t.retryPolicy(), t.cancelOnEvents(), idem, spec(rule, t)));
        }
        return new Plan(ns, ss, frozen);
    }

    /** Child activation: due = max(now + delay, dueAt spec) and expiry, from the frozen context. */
    public static Instant[] activationTimes(TemplateDef t, FrozenContext fc, Instant now) {
        Duration delay = t.parent().delay() == null ? Duration.ZERO : t.parent().delay();
        Instant due = now.plus(delay);
        if (t.dueAt() != null) {
            Instant spec = time(t, t.dueAt(), now, fc.zone(), fc.signals());
            if (spec.isAfter(due)) due = spec;
        }
        return new Instant[]{due, expiry(t, due, now, fc.zone(), fc.signals())};
    }

    /** WHILE_IN_STATE series activation: start = now + delay; until relative to start. */
    public static Instant[] seriesActivation(TemplateDef t, FrozenContext fc, Instant now) {
        Duration delay = t.parent().delay() == null ? Duration.ZERO : t.parent().delay();
        Instant start = now.plus(delay);
        UntilSpec u = t.parent().until() != null ? t.parent().until() : t.until();
        Instant until = untilAt(t, u, start, now, fc.zone(), fc.signals());
        ZonedDateTime first = t.recurrence().build(start, fc.zone()).nextOrSame(start.atZone(fc.zone()));
        return new Instant[]{start, first == null ? null : first.toInstant(), until};
    }

    public static Integer effMax(TemplateDef t) {
        if (t.parent() != null && t.parent().maxOccurrences() != null) return t.parent().maxOccurrences();
        return t.maxOccurrences();
    }

    private static Instant untilAt(TemplateDef t, UntilSpec u, Instant start, Instant now, ZoneId zone, Map<String, SignalValue> s) {
        if (u == null) return null;
        if (u.afterStart() != null) return u.afterStart().addTo(start.atZone(zone)).toInstant();
        return time(t, u.at(), now, zone, s);
    }

    private static Instant expiry(TemplateDef t, Instant due, Instant now, ZoneId zone, Map<String, SignalValue> s) {
        if (t.expiresAfter() != null) return t.expiresAfter().addTo(due.atZone(zone)).toInstant();
        if (t.expiresAt() != null) return time(t, t.expiresAt(), now, zone, s);
        return null;
    }

    private static Instant time(TemplateDef t, TimeSpec ts, Instant now, ZoneId zone, Map<String, SignalValue> s) {
        try {
            return ts.resolve(now, zone, s);
        } catch (TimeSpec.MissingBaseException e) {
            throw new PlanningException(t.id(), "TIME_BASE_MISSING", e.getMessage());
        }
    }

    /** Resolves (or, when deferred, checks) the body; validates schema when fully resolved. */
    public static JsonNode checkBody(TemplateDef t, BodyContext bc, BodySchemaValidator schemas, boolean validateSchema) {
        JsonNode body;
        try {
            body = BodyResolver.resolve(t.body(), bc, t.onMissingValue());
        } catch (BodyResolutionException e) {
            throw new PlanningException(t.id(), "BODY_RESOLUTION_FAILED", e.getMessage());
        } catch (IllegalArgumentException e) {
            throw new PlanningException(t.id(), "BODY_RESOLUTION_FAILED", e.getMessage());
        }
        if (validateSchema && t.bodySchema() != null) {
            List<String> errs = schemas.validate(t.bodySchema(), body);
            if (!errs.isEmpty()) throw new PlanningException(t.id(), "BODY_SCHEMA_VIOLATION", String.join("; ", errs));
        }
        return body;
    }
}
