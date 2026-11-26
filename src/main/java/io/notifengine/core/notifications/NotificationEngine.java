package io.notifengine.core.notifications;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.notifengine.common.Json;
import io.notifengine.common.Uuid7;
import io.notifengine.core.EngineConfig;
import io.notifengine.core.dispatch.DeadLetters;
import io.notifengine.core.dispatch.MessageFactory;
import io.notifengine.core.dispatch.OutboxRepository;
import io.notifengine.core.ports.EngineMetrics;
import io.notifengine.core.ports.TraceContext;
import io.notifengine.core.timers.TimerRepository;
import io.notifengine.domain.body.BodyContext;
import io.notifengine.domain.body.BodySchemaValidator;
import io.notifengine.domain.evaluation.FiringPlanner;
import io.notifengine.domain.evaluation.FrozenContext;
import io.notifengine.domain.evaluation.PlanningException;
import io.notifengine.domain.notification.Dependencies;
import io.notifengine.domain.notification.Effect;
import io.notifengine.domain.notification.Event;
import io.notifengine.domain.notification.NotificationStatus;
import io.notifengine.domain.notification.StateMachine;
import io.notifengine.domain.notification.StatusChange;
import io.notifengine.domain.notification.TimerKind;
import io.notifengine.domain.notification.Transition;
import io.notifengine.domain.recurrence.Recurrence;
import io.notifengine.domain.rule.CompiledRule;
import io.notifengine.domain.rule.Enums;
import io.notifengine.domain.rule.RuleParser;
import io.notifengine.domain.rule.TemplateDef;
import org.jboss.logging.Logger;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * Applies the pure state machine to persistent notifications and executes the requested effects
 * (timers, outbox messages, history, children, series) in the caller's transaction.
 */
public final class NotificationEngine {
    private static final Logger LOG = Logger.getLogger(NotificationEngine.class);

    private final NotificationRepository notifs;
    private final SeriesRepository series;
    private final TimerRepository timers;
    private final OutboxRepository outbox;
    private final DeadLetters dlq;
    private final StateMachine sm;
    private final EngineConfig cfg;
    private final EngineMetrics metrics;
    private final TraceContext trace;
    private final BodySchemaValidator schemas;

    public NotificationEngine(NotificationRepository notifs, SeriesRepository series, TimerRepository timers, OutboxRepository outbox,
                              DeadLetters dlq, StateMachine sm, EngineConfig cfg, EngineMetrics metrics, TraceContext trace,
                              BodySchemaValidator schemas) {
        this.notifs = notifs;
        this.series = series;
        this.timers = timers;
        this.outbox = outbox;
        this.dlq = dlq;
        this.sm = sm;
        this.cfg = cfg;
        this.metrics = metrics;
        this.trace = trace;
        this.schemas = schemas;
    }

    public NotificationRepository repository() { return notifs; }

    public SeriesRepository seriesRepository() { return series; }

    // =====================================================================================================
    // Creation
    // =====================================================================================================

    public record Created(List<UUID> notifications, List<UUID> series, int duplicates) {}

    /** Persists a firing plan (all-or-nothing within the caller's transaction / savepoint). */
    public Created create(Connection c, CompiledRule rule, String tenant, String key, FiringPlanner.Plan plan, Instant now) throws SQLException {
        Instant createdAt = Uuid7.truncate(now);
        Map<String, UUID> ids = new HashMap<>();
        for (FiringPlanner.PlannedNotification p : plan.notifications()) ids.put(p.templateId(), Uuid7.generate(createdAt));
        List<NotificationRow> rows = new ArrayList<>();
        int dup = 0;
        ObjectNode frozen = plan.context().toJson();
        for (FiringPlanner.PlannedNotification p : plan.notifications()) {
            UUID id = ids.get(p.templateId());
            if (!notifs.claimIdempotencyKey(c, p.idempotencyKey(), id, now)) { dup++; continue; }
            ObjectNode spec = p.spec().deepCopy();
            List<TemplateDef> kids = rule.children().getOrDefault(p.templateId(), List.of());
            spec.put("hasChildren", kids.stream().anyMatch(k -> !k.isWhileInState()));
            spec.put("hasWhileChildren", kids.stream().anyMatch(TemplateDef::isWhileInState));
            boolean needsContext = p.initialStatus() == NotificationStatus.WAITING_ON_PARENT;
            rows.add(new NotificationRow(id, createdAt, now, tenant, rule.id(), rule.version(), p.templateId(), key, p.action(),
                    p.type().name(), p.initialStatus(), null, 0, 0, p.retryPolicy(), p.body(), spec, needsContext ? frozen : null,
                    p.dueAt(), p.expiresAt(), null, 0, p.idempotencyKey(),
                    p.parentTemplateId() == null ? null : ids.get(p.parentTemplateId()),
                    p.parentTrigger() == null ? null : p.parentTrigger().name(), p.depth(), null, null, p.cancelOn(),
                    false, null, null, null));
        }
        notifs.insert(c, rows);
        for (NotificationRow r : rows) {
            notifs.history(c, r, List.of(new StatusChange(null, r.status(), "CREATE", 0, null)), now);
            metrics.count("notifications.created", "action", r.action());
        }
        List<UUID> seriesIds = new ArrayList<>();
        for (FiringPlanner.PlannedSeries ps : plan.series()) {
            UUID sid = Uuid7.generate(createdAt);
            UUID parentId = ps.parentTemplateId() == null ? null : ids.get(ps.parentTemplateId());
            if (ps.parentTemplateId() != null && parentId == null) continue; // parent deduplicated
            SeriesRow s = new SeriesRow(sid, tenant, rule.id(), rule.version(), ps.templateId(), key, ps.action(),
                    ps.waiting() ? SeriesRow.WAITING : SeriesRow.ACTIVE, null, parentId, ps.spec(), frozen, ps.startAt(), ps.untilAt(),
                    ps.maxOccurrences(), 0, 0, ps.firstDueAt(), ps.cancelOn(), ps.idempotencyKey(), 0, now, now);
            if (!series.insert(c, s)) { dup++; continue; }
            seriesIds.add(sid);
            if (!ps.waiting()) scheduleFirstOccurrence(c, s, now);
        }
        // start root notifications (children wait for their parent)
        for (NotificationRow r : rows) {
            if (r.status() == NotificationStatus.CREATED) apply(c, r, new Event.Start(), now);
        }
        if (dup > 0) metrics.count("notifications.duplicates_suppressed", dup);
        return new Created(rows.stream().map(NotificationRow::id).toList(), seriesIds, dup);
    }

    private void scheduleFirstOccurrence(Connection c, SeriesRow s, Instant now) throws SQLException {
        if (s.nextDueAt() == null || (s.untilAt() != null && s.nextDueAt().isAfter(s.untilAt()))) {
            closeSeries(c, s, "LIMIT_REACHED", now);
            return;
        }
        timers.insert(c, TimerKind.NEXT_OCCURRENCE, s.nextDueAt(), s.tenantId(), null, s.id(), s.ruleId(), s.correlationKey(), null);
    }

    // =====================================================================================================
    // Transitions
    // =====================================================================================================

    /** Applies an event; persists state, history and effects; coordinates children. Returns the updated row. */
    public NotificationRow apply(Connection c, NotificationRow n, Event e, Instant now) throws SQLException {
        Transition t = sm.apply(n.toState(), e, now);
        if (t.isIgnored()) {
            metrics.count("notifications.events_ignored", "event", e.name().startsWith("OUTCOME") ? "OUTCOME" : e.name());
            LOG.debugf("notification %s: %s ignored: %s", n.id(), e.name(), t.ignored());
            return n;
        }
        NotificationStatus before = n.status();
        NotificationRow row = n.withState(t.state(), now);
        for (Effect ef : t.effects()) {
            switch (ef) {
                case Effect.ScheduleTimer st -> timers.insert(c, st.kind(), st.dueAt(), row.tenantId(), row.id(), null, row.ruleId(),
                        row.correlationKey(), null);
                case Effect.CancelTimers ct -> {
                    if (ct.kinds().equals(EnumSet.of(TimerKind.ATTEMPT_TIMEOUT)) && n.attemptTimerId() != null) timers.deleteById(c, n.attemptTimerId());
                    else timers.deleteForNotification(c, row.id(), ct.kinds());
                }
                case Effect.Emit em -> {
                    outbox.insert(c, cfg.outputTopic(), MessageFactory.key(row), MessageFactory.headers(row, em.attempt(), trace.currentHeaders()),
                            MessageFactory.payload(row, em.attempt(), now), row.tenantId(), row.id(), em.attempt(),
                            em.attemptTimeout() == null ? null : em.attemptTimeout().toMillis(), now);
                    if (em.attempt() > 1) metrics.count("notifications.retries", "action", row.action());
                }
            }
        }
        row = notifs.update(c, row);
        notifs.history(c, row, t.changes(), now);
        for (StatusChange ch : t.changes()) {
            metrics.count("notifications.transitions", "to", ch.to().name(), "action", row.action());
            if (ch.to() == NotificationStatus.RETRIES_EXHAUSTED) metrics.count("notifications.exhausted", "action", row.action());
        }
        coordinate(c, row, t, before, now);
        return row;
    }

    // =====================================================================================================
    // Parent / child
    // =====================================================================================================

    private void coordinate(Connection c, NotificationRow parent, Transition t, NotificationStatus before, Instant now) throws SQLException {
        JsonNode spec = parent.spec();
        if (spec.path("hasChildren").asBoolean(false)) {
            List<NotificationStatus> entered = t.entered();
            for (NotificationRow child : notifs.lockWaitingChildren(c, parent)) {
                Enums.ParentTrigger trig = Enums.ParentTrigger.valueOf(child.parentTrigger());
                switch (Dependencies.child(trig, entered, parent.status())) {
                    case ACTIVATE -> activateChild(c, child, parent, now);
                    case SKIP -> apply(c, child, new Event.Skip("parent " + parent.status() + " without " + trig), now);
                    case NONE -> { }
                }
            }
        }
        if (spec.path("hasWhileChildren").asBoolean(false)) {
            for (SeriesRow s : series.lockLatestForParent(c, parent.id())) {
                TemplateDef tpl = template(s.spec());
                Dependencies.SeriesState st = Dependencies.SeriesState.valueOf(s.status());
                switch (Dependencies.series(tpl.parent().states(), before, parent.status(), st, tpl.parent().once())) {
                    case START -> {
                        if (st == Dependencies.SeriesState.WAITING) activateSeries(c, s, tpl, now);
                        else reenterSeries(c, s, tpl, now);
                    }
                    case CLOSE_RECOVERED -> closeSeries(c, s, st == Dependencies.SeriesState.WAITING ? "PARENT_TERMINATED" : "PARENT_RECOVERED", now);
                    case CLOSE_TERMINATED -> closeSeries(c, s, "PARENT_TERMINATED", now);
                    case NONE -> { }
                }
            }
        }
    }

    static TemplateDef template(JsonNode spec) {
        return RuleParser.parseTemplate(spec.get("template"),
                Enums.MissingValuePolicy.valueOf(spec.path("ruleMissingValuePolicy").asText("FAIL")));
    }

    /** Data exposed to $parent references. */
    public static ObjectNode parentJson(NotificationRow p) {
        ObjectNode o = Json.object();
        o.put("id", p.id().toString());
        o.put("status", p.status().name());
        o.put("attempt", p.attempt());
        o.put("action", p.action());
        o.put("templateId", p.templateId());
        o.put("correlationKey", p.correlationKey());
        if (p.dueAt() != null) o.put("dueAt", p.dueAt().toString());
        if (p.publishedAt() != null) o.put("publishedAt", p.publishedAt().toString());
        if (p.lastFailure() != null) o.set("lastFailure", p.lastFailure().toJson());
        if (p.body() != null) o.set("body", p.body());
        return o;
    }

    private void activateChild(Connection c, NotificationRow child, NotificationRow parent, Instant now) throws SQLException {
        TemplateDef tpl = template(child.spec());
        FrozenContext fc = FrozenContext.fromJson(child.context());
        Instant[] times;
        JsonNode body = child.body();
        try {
            times = FiringPlanner.activationTimes(tpl, fc, now);
            if (body == null) {
                BodyContext bc = new BodyContext(fc.signals(), fc.trigger(), parentJson(parent), false, fc.correlationKey(), now,
                        fc.zone(), null, false, fc.ruleId(), tpl.id());
                body = FiringPlanner.checkBody(tpl, bc, schemas, true);
            }
        } catch (PlanningException ex) {
            dlq.record(c, "child-activation", child.tenantId(), ex.code(), "notification " + child.id() + ": " + ex.getMessage(), null, now);
            apply(c, child, new Event.Skip(ex.code()), now);
            return;
        }
        NotificationRow ready = child.withBody(body).withTimes(times[0], times[1]);
        apply(c, ready, new Event.Activate(times[0]), now);
    }

    // =====================================================================================================
    // Series
    // =====================================================================================================

    private void activateSeries(Connection c, SeriesRow s, TemplateDef tpl, Instant now) throws SQLException {
        FrozenContext fc = FrozenContext.fromJson(s.context());
        Instant[] t = FiringPlanner.seriesActivation(tpl, fc, now);
        SeriesRow active = SeriesRepository.with(s, SeriesRow.ACTIVE, null, t[0], t[2], 0, 0, t[1], now);
        series.update(c, active);
        active = new SeriesRow(active.id(), active.tenantId(), active.ruleId(), active.ruleVersion(), active.templateId(), active.correlationKey(),
                active.action(), active.status(), null, active.parentId(), active.spec(), active.context(), active.startAt(), active.untilAt(),
                active.maxOccurrences(), 0, 0, active.nextDueAt(), active.cancelOn(), active.idempotencyKey(), active.version() + 1,
                active.createdAt(), now);
        scheduleFirstOccurrence(c, active, now);
    }

    private void reenterSeries(Connection c, SeriesRow closed, TemplateDef tpl, Instant now) throws SQLException {
        UUID sid = Uuid7.generate(now);
        SeriesRow s = new SeriesRow(sid, closed.tenantId(), closed.ruleId(), closed.ruleVersion(), closed.templateId(), closed.correlationKey(),
                closed.action(), SeriesRow.WAITING, null, closed.parentId(), closed.spec(), closed.context(), null, null,
                closed.maxOccurrences(), 0, 0, null, closed.cancelOn(), closed.parentId() + "|" + closed.templateId() + "|" + sid, 0, now, now);
        series.insert(c, s);
        activateSeries(c, s, tpl, now);
    }

    public void closeSeries(Connection c, SeriesRow s, String reason, Instant now) throws SQLException {
        if (SeriesRow.CLOSED.equals(s.status())) return;
        series.update(c, SeriesRepository.with(s, SeriesRow.CLOSED, reason, s.startAt(), s.untilAt(), s.occurrences(), s.skipped(), null, now));
        timers.deleteForSeries(c, s.id());
        metrics.count("series.closed", "reason", reason);
    }

    /** NEXT_OCCURRENCE handler: re-checks the parent in this (claim) transaction before every occurrence. */
    public void occurrence(Connection c, UUID seriesId, Instant scheduledAt, Instant now) throws SQLException {
        Optional<SeriesRow> o = series.lock(c, seriesId);
        if (o.isEmpty() || !SeriesRow.ACTIVE.equals(o.get().status())) return;
        SeriesRow s = o.get();
        TemplateDef tpl = template(s.spec());
        FrozenContext fc = FrozenContext.fromJson(s.context());
        NotificationRow parent = null;
        if (s.parentId() != null) {
            parent = notifs.lock(c, s.parentId()).orElse(null);
            if (parent == null || !Dependencies.inStates(tpl.parent().states(), parent.status())) {
                boolean recovered = parent != null && (parent.status() == NotificationStatus.SUCCEEDED || !parent.status().terminal());
                closeSeries(c, s, recovered ? "PARENT_RECOVERED" : "PARENT_TERMINATED", now);
                return;
            }
        }
        if ((s.maxOccurrences() != null && s.occurrences() >= s.maxOccurrences()) || (s.untilAt() != null && scheduledAt.isAfter(s.untilAt()))) {
            closeSeries(c, s, "LIMIT_REACHED", now);
            return;
        }
        ZoneId zone = fc.zone();
        Recurrence rec = tpl.recurrence().build(s.startAt(), zone);
        int occurrences = s.occurrences();
        int skipped = s.skipped();
        Instant next;
        boolean misfire = tpl.misfireGrace() != null && now.isAfter(scheduledAt.plus(tpl.misfireGrace()));
        if (misfire) {
            skipped++;
            createOccurrence(c, s, tpl, fc, parent, occurrences + 1, scheduledAt, now, "MISFIRE");
            metrics.count("series.misfires", "action", s.action());
            ZonedDateTime n = rec.next(now.atZone(zone));
            next = n == null ? null : n.toInstant();
        } else {
            if (createOccurrence(c, s, tpl, fc, parent, occurrences + 1, scheduledAt, now, null)) occurrences++;
            else skipped++;
            ZonedDateTime n = rec.next(scheduledAt.atZone(zone));
            next = n == null ? null : n.toInstant();
        }
        SeriesRow updated = SeriesRepository.with(s, s.status(), null, s.startAt(), s.untilAt(), occurrences, skipped, next, now);
        if ((s.maxOccurrences() != null && occurrences >= s.maxOccurrences()) || next == null || (s.untilAt() != null && next.isAfter(s.untilAt()))) {
            series.update(c, SeriesRepository.with(updated, SeriesRow.CLOSED, "LIMIT_REACHED", s.startAt(), s.untilAt(), occurrences, skipped, null, now));
            timers.deleteForSeries(c, s.id());
            metrics.count("series.closed", "reason", "LIMIT_REACHED");
            return;
        }
        series.update(c, updated);
        timers.insert(c, TimerKind.NEXT_OCCURRENCE, next, s.tenantId(), null, s.id(), s.ruleId(), s.correlationKey(), null);
    }

    /** Creates one occurrence (or a SKIPPED record). Returns true if a publishable occurrence was created. */
    private boolean createOccurrence(Connection c, SeriesRow s, TemplateDef tpl, FrozenContext fc, NotificationRow parent, int index,
                                     Instant scheduledAt, Instant now, String skipReason) throws SQLException {
        Instant createdAt = Uuid7.truncate(now);
        UUID id = Uuid7.generate(createdAt);
        String idem = s.idempotencyKey() + "|" + s.id() + "|" + index + (skipReason == null ? "" : "|skip@" + scheduledAt);
        if (!notifs.claimIdempotencyKey(c, idem, id, now)) return false;
        JsonNode body = null;
        String reason = skipReason;
        if (reason == null) {
            BodyContext bc = new BodyContext(fc.signals(), fc.trigger(), parent == null ? null : parentJson(parent), false,
                    fc.correlationKey(), now, fc.zone(), index, false, fc.ruleId(), tpl.id());
            try {
                body = FiringPlanner.checkBody(tpl, bc, schemas, true);
            } catch (PlanningException ex) {
                dlq.record(c, "series-occurrence", s.tenantId(), ex.code(), "series " + s.id() + " occurrence " + index + ": " + ex.getMessage(), null, now);
                reason = ex.code();
            }
        }
        ObjectNode spec = s.spec().deepCopy();
        NotificationRow row = new NotificationRow(id, createdAt, now, s.tenantId(), s.ruleId(), s.ruleVersion(), s.templateId(), s.correlationKey(),
                s.action(), Enums.NotificationType.RECURRING.name(), reason == null ? NotificationStatus.CREATED : NotificationStatus.SKIPPED, reason,
                0, 0, tpl.retryPolicy(), body, spec, null, scheduledAt, null, null, 0, idem, null, null,
                0, s.id(), index, s.cancelOn(), false, null, null, reason == null ? null : now);
        notifs.insert(c, List.of(row));
        notifs.history(c, row, List.of(new StatusChange(null, row.status(), "OCCURRENCE", 0, reason)), now);
        if (reason != null) return false;
        metrics.count("notifications.created", "action", row.action());
        apply(c, row, new Event.Start(), now);
        return true;
    }

    // =====================================================================================================
    // Cancellation by event, API operations
    // =====================================================================================================

    public int cancelBySignal(Connection c, String tenant, String key, String signal, Instant now) throws SQLException {
        int n = 0;
        for (NotificationRow r : notifs.lockCancellable(c, tenant, key, signal)) {
            NotificationRow fresh = notifs.lock(c, r.id()).orElse(null);
            if (fresh == null || !NotificationStatus.ACTIVE.contains(fresh.status()) || !fresh.cancelOn().contains(signal)) continue;
            apply(c, fresh, new Event.Cancel("cancel event " + signal), now);
            n++;
        }
        for (SeriesRow s : series.lockCancellable(c, tenant, key, signal)) {
            closeSeries(c, s, "CANCELLED", now);
            n++;
        }
        if (n > 0) metrics.count("notifications.cancelled_by_event", n);
        return n;
    }

    public Optional<NotificationRow> cancel(Connection c, String tenant, UUID id, String by, Instant now) throws SQLException {
        Optional<NotificationRow> n = notifs.lock(c, id).filter(r -> r.tenantId().equals(tenant));
        if (n.isEmpty()) return n;
        NotificationRow r = apply(c, n.get(), new Event.Cancel("cancelled by " + by), now);
        if (r.seriesId() == null) {
            for (SeriesRow s : series.openForParent(c, r.id())) closeSeries(c, s, "CANCELLED", now);
        }
        return Optional.of(r);
    }

    public Optional<NotificationRow> retrigger(Connection c, String tenant, UUID id, String by, Instant now) throws SQLException {
        Optional<NotificationRow> n = notifs.lock(c, id).filter(r -> r.tenantId().equals(tenant));
        if (n.isEmpty()) return n;
        return Optional.of(apply(c, n.get(), new Event.Retrigger(by), now));
    }

    public Optional<SeriesRow> cancelSeries(Connection c, String tenant, UUID id, Instant now) throws SQLException {
        Optional<SeriesRow> s = series.lock(c, id).filter(r -> r.tenantId().equals(tenant));
        if (s.isPresent()) closeSeries(c, s.get(), "CANCELLED", now);
        return s.flatMap(x -> { try { return series.find(c, tenant, id); } catch (SQLException e) { throw new RuntimeException(e); } });
    }

    public static Set<TimerKind> notificationTimerKinds() { return EnumSet.of(TimerKind.PUBLISH, TimerKind.ATTEMPT_TIMEOUT, TimerKind.EXPIRE); }
}
