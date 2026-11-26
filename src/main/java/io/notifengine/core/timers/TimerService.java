package io.notifengine.core.timers;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.notifengine.common.Json;
import io.notifengine.core.EngineConfig;
import io.notifengine.core.db.Tx;
import io.notifengine.core.evaluation.EvaluationService;
import io.notifengine.core.evaluation.RuleStateRepository;
import io.notifengine.core.notifications.NotificationEngine;
import io.notifengine.core.notifications.NotificationRow;
import io.notifengine.core.ports.EngineMetrics;
import io.notifengine.core.rules.RuleRegistry;
import io.notifengine.core.signals.SignalStore;
import io.notifengine.domain.notification.Event;
import io.notifengine.domain.rule.CompiledRule;
import org.jboss.logging.Logger;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import io.notifengine.domain.notification.TimerKind;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.UUID;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Claims due timers and runs their handlers in the SAME transaction (crash = rollback = timers reappear; no leases).
 * Each handler runs under a savepoint: a failing handler is rolled back alone and re-queued with growing backoff,
 * and moved to timer_dead after N failures. Handlers are idempotent and state-guarded and do no remote calls.
 */
public final class TimerService {
    private static final Logger LOG = Logger.getLogger(TimerService.class);

    /** Test hook to inject handler failures. */
    public interface FaultInjector { void before(TimerRow t); }

    private final Tx tx;
    private final TimerRepository timers;
    private final NotificationEngine engine;
    private final EvaluationService evaluation;
    private final SignalStore signals;
    private final RuleRegistry registry;
    private final EngineConfig cfg;
    private final EngineMetrics metrics;
    private final Clock clock;
    private volatile FaultInjector faults = t -> {};

    public TimerService(Tx tx, TimerRepository timers, NotificationEngine engine, EvaluationService evaluation, SignalStore signals,
                        RuleRegistry registry, EngineConfig cfg, EngineMetrics metrics, Clock clock) {
        this.tx = tx;
        this.timers = timers;
        this.engine = engine;
        this.evaluation = evaluation;
        this.signals = signals;
        this.registry = registry;
        this.cfg = cfg;
        this.metrics = metrics;
        this.clock = clock;
    }

    public void setFaultInjector(FaultInjector f) { this.faults = f == null ? t -> {} : f; }

    /** Claims and handles one batch. Returns the number of timers claimed. */
    public int runOnce() {
        return tx.inTx(c -> {
            Instant now = clock.instant();
            List<TimerRow> batch = timers.claim(c, now, cfg.timerBatchSize());
            // consistent lock order across workers (deadlock avoidance)
            batch.sort(Comparator.comparing(TimerService::lockKey));
            for (TimerRow t : batch)
                metrics.recordMs("timers.lag", Math.max(0, Duration.between(t.dueAt(), now).toMillis()), "kind", t.kind().name());
            List<TimerRow> notifTimers = batch.stream().filter(t -> NOTIFICATION_KINDS.contains(t.kind())).toList();
            List<TimerRow> others = new ArrayList<>(batch.stream().filter(t -> !NOTIFICATION_KINDS.contains(t.kind())).toList());
            if (!notifTimers.isEmpty()) {
                try {
                    Tx.savepoint(c, cx -> { notificationTimers(cx, notifTimers, now); return null; });
                    for (TimerRow t : notifTimers) metrics.count("timers.fired", "kind", t.kind().name());
                } catch (RuntimeException | SQLException e) {
                    LOG.debugf("batched timer handling failed (%s), isolating per timer", e.getMessage());
                    others.addAll(0, notifTimers);
                }
            }
            for (TimerRow t : others) single(c, t, now);
            return batch.size();
        });
    }

    private static final Set<TimerKind> NOTIFICATION_KINDS = EnumSet.of(TimerKind.PUBLISH, TimerKind.EXPIRE, TimerKind.ATTEMPT_TIMEOUT);

    /** Handles one timer under its own savepoint; failures are re-queued with backoff or dead-lettered. */
    private void single(Connection c, TimerRow t, Instant now) throws SQLException {
        try {
            Tx.savepoint(c, cx -> { faults.before(t); handle(cx, t, now); return null; });
            metrics.count("timers.fired", "kind", t.kind().name());
        } catch (RuntimeException | SQLException e) {
            String err = e.getClass().getSimpleName() + ": " + e.getMessage();
            metrics.count("timers.failed", "kind", t.kind().name());
            if (t.failures() + 1 >= cfg.timerMaxFailures()) {
                LOG.errorf("timer %d (%s) moved to timer_dead after %d failures: %s", t.id(), t.kind(), t.failures() + 1, err);
                timers.dead(c, t, err, now);
                metrics.count("timers.dead", "kind", t.kind().name());
            } else {
                long backoff = cfg.timerFailureBackoff().toMillis() * (1L << Math.min(t.failures(), 10));
                LOG.warnf("timer %d (%s) failed (%d), retry in %d ms: %s", t.id(), t.kind(), t.failures() + 1, backoff, err);
                timers.reschedule(c, t, now.plusMillis(backoff), err);
            }
        }
    }

    /** Fast path: lock all target notifications at once and apply the events with batched writes. */
    private void notificationTimers(Connection c, List<TimerRow> list, Instant now) throws SQLException {
        for (TimerRow t : list) faults.before(t);
        Map<UUID, NotificationRow> rows = new HashMap<>();
        for (NotificationRow r : engine.repository().lockMany(c, list.stream().map(TimerRow::notificationId).toList())) rows.put(r.id(), r);
        Map<UUID, List<TimerRow>> byNotification = new LinkedHashMap<>();
        for (TimerRow t : list) byNotification.computeIfAbsent(t.notificationId(), k -> new ArrayList<>()).add(t);
        List<NotificationEngine.Step> steps = new ArrayList<>();
        List<TimerRow> sequential = new ArrayList<>();
        for (var e : byNotification.entrySet()) {
            NotificationRow r = rows.get(e.getKey());
            if (r == null) continue; // removed by retention
            if (e.getValue().size() == 1) steps.add(new NotificationEngine.Step(r, event(e.getValue().get(0))));
            else sequential.addAll(e.getValue());
        }
        engine.applyAll(c, steps, now);
        for (TimerRow t : sequential) handle(c, t, now);
    }

    private static Event event(TimerRow t) {
        return switch (t.kind()) {
            case PUBLISH -> new Event.PublishDue();
            case EXPIRE -> new Event.Expire();
            case ATTEMPT_TIMEOUT -> new Event.AttemptTimeout(t.payload().path("attempt").asInt());
            default -> throw new IllegalArgumentException(t.kind().name());
        };
    }

    private static String lockKey(TimerRow t) {
        if (t.notificationId() != null) return "n" + t.notificationId();
        if (t.seriesId() != null) return "s" + t.seriesId();
        return "r" + t.tenantId() + "|" + t.ruleId() + "|" + t.correlationKey();
    }

    public Instant nextDue() { return tx.read(timers::nextDue); }

    void handle(Connection c, TimerRow t, Instant now) throws SQLException {
        switch (t.kind()) {
            case PUBLISH -> notification(c, t, now, new Event.PublishDue());
            case EXPIRE -> notification(c, t, now, new Event.Expire());
            case ATTEMPT_TIMEOUT -> notification(c, t, now, new Event.AttemptTimeout(t.payload().path("attempt").asInt()));
            case NEXT_OCCURRENCE -> engine.occurrence(c, t.seriesId(), t.dueAt(), now);
            case RULE_EVAL -> {
                if (TimerRow.SWEEP_KEY.equals(t.correlationKey())) sweep(c, t, now);
                else evaluateEntity(c, t, now);
            }
        }
    }

    private void notification(Connection c, TimerRow t, Instant now, Event e) throws SQLException {
        Optional<NotificationRow> n = engine.repository().lock(c, t.notificationId());
        if (n.isEmpty()) return; // retention removed it
        engine.apply(c, n.get(), e, now);
    }

    private void evaluateEntity(Connection c, TimerRow t, Instant now) throws SQLException {
        CompiledRule rule = registry.rule(t.tenantId(), t.ruleId());
        if (rule == null) return; // rule disabled/deleted: drop
        evaluateEntities(c, rule, List.of(t.correlationKey()), now);
    }

    private void evaluateEntities(Connection c, CompiledRule rule, List<String> keys, Instant now) throws SQLException {
        var entities = keys.stream().map(k -> new SignalStore.EntityKey(rule.tenantId(), k)).toList();
        Map<SignalStore.EntityKey, Map<String, io.notifengine.domain.signal.SignalValue>> sig = signals.load(c, entities);
        var locked = evaluation.states().lockAll(c, keys.stream().map(k -> new RuleStateRepository.Key(rule.tenantId(), rule.id(), k)).toList());
        EvaluationService.Buffer buf = new EvaluationService.Buffer();
        for (SignalStore.EntityKey e : entities) {
            evaluation.evaluateInto(new EvaluationService.EntityWork(e.tenantId(), e.correlationKey(), sig.get(e), null, null, List.of(rule)), locked, now, buf);
        }
        evaluation.flush(c, buf, now);
        metrics.count("evaluation.time_triggered", keys.size());
    }

    /** Per-rule sweep for NOW-based conditions: one entity page per timer firing, cursor carried in the payload. */
    private void sweep(Connection c, TimerRow t, Instant now) throws SQLException {
        CompiledRule rule = registry.rule(t.tenantId(), t.ruleId());
        if (rule == null || !rule.hasNowCondition()) return;
        String cursor = t.payload() == null || t.payload().path("cursor").isNull() ? null : t.payload().path("cursor").asText(null);
        Instant at = t.payload() != null && t.payload().hasNonNull("at") ? Instant.parse(t.payload().get("at").asText()) : t.dueAt();
        Set<String> sigs = rule.referencedSignals();
        List<String> page = sigs.isEmpty() ? List.of() : signals.entitiesPage(c, rule.tenantId(), sigs, cursor, cfg.sweepPageSize());
        if (!page.isEmpty()) evaluateEntities(c, rule, page, now);
        ObjectNode payload = Json.object();
        if (page.size() == cfg.sweepPageSize()) {
            payload.put("cursor", page.get(page.size() - 1));
            payload.put("at", at.toString());
            timers.upsertSweep(c, rule.tenantId(), rule.id(), now, payload);
            return;
        }
        Instant next = rule.nextRuleChange(at);
        if (next != null && !next.isAfter(now)) next = rule.nextRuleChange(now);
        if (next == null) return;
        payload.put("at", next.toString());
        timers.upsertSweep(c, rule.tenantId(), rule.id(), next, payload);
    }
}
