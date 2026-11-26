package trash_maker.core.timers;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import jakarta.transaction.Transactional;

import trash_maker.common.Json;
import trash_maker.core.EngineConfig;
import trash_maker.core.db.Db;
import trash_maker.core.evaluation.EvaluationService;
import trash_maker.core.evaluation.RuleStateRepository;
import trash_maker.core.notifications.NotificationEngine;
import trash_maker.core.notifications.NotificationRow;
import trash_maker.core.ports.EngineMetrics;
import trash_maker.core.rules.RuleRegistry;
import trash_maker.core.signals.SignalStore;
import trash_maker.domain.notification.Event;
import trash_maker.domain.rule.CompiledRule;
import org.jboss.logging.Logger;
import org.jboss.logging.MDC;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import trash_maker.domain.notification.TimerKind;
import trash_maker.domain.timer.SweepStep;
import trash_maker.domain.timer.TimerFailurePolicy;
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
@Singleton
public class TimerService {
    private static final Logger LOG = Logger.getLogger(TimerService.class);

    /** Test hook to inject handler failures. */
    public interface FaultInjector { void before(TimerRow t); }

    private final Db db;
    private final TimerRepository timers;
    private final NotificationEngine engine;
    private final EvaluationService evaluation;
    private final SignalStore signals;
    private final RuleRegistry registry;
    private final EngineConfig cfg;
    private final EngineMetrics metrics;
    private final Clock clock;
    private volatile FaultInjector faults = t -> {};

    @Inject
    public TimerService(Db db, TimerRepository timers, NotificationEngine engine, EvaluationService evaluation, SignalStore signals,
                        RuleRegistry registry, EngineConfig cfg, EngineMetrics metrics, Clock clock) {
        this.db = db;
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
    @Transactional
    public int runOnce() {
        return db.with(c -> {
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
                    Db.savepoint(c, cx -> { notificationTimers(cx, notifTimers, now); return null; });
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
        MDC.put("timer_id", t.id());
        MDC.put("timer_kind", t.kind().name());
        MDC.put("tenant_id", t.tenantId());
        if (t.notificationId() != null) MDC.put("notification_id", t.notificationId().toString());
        if (t.seriesId() != null) MDC.put("series_id", t.seriesId().toString());
        try {
            Db.savepoint(c, cx -> { faults.before(t); handle(cx, t, now); return null; });
            metrics.count("timers.fired", "kind", t.kind().name());
        } catch (RuntimeException | SQLException e) {
            String err = e.getClass().getSimpleName() + ": " + e.getMessage();
            metrics.count("timers.failed", "kind", t.kind().name());
            switch (TimerFailurePolicy.decide(t.failures(), cfg.timerMaxFailures(), cfg.timerFailureBackoff(), now)) {
                case TimerFailurePolicy.Dead d -> {
                    LOG.errorf("timer %d (%s) moved to timer_dead after %d failures: %s", t.id(), t.kind(), d.failures(), err);
                    timers.dead(c, t, err, now);
                    metrics.count("timers.dead", "kind", t.kind().name());
                }
                case TimerFailurePolicy.Retry r -> {
                    LOG.warnf("timer %d (%s) failed (%d), retry at %s: %s", t.id(), t.kind(), r.failures(), r.dueAt(), err);
                    timers.reschedule(c, t, r.dueAt(), err);
                }
            }
        } finally {
            for (String k : new String[]{"timer_id", "timer_kind", "tenant_id", "notification_id", "series_id"}) MDC.remove(k);
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

    public Instant nextDue() { return db.read(timers::nextDue); }

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
        Map<SignalStore.EntityKey, Map<String, trash_maker.domain.signal.SignalValue>> sig = signals.load(c, entities);
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
        switch (SweepStep.after(page, cfg.sweepPageSize(), at, now, rule::nextRuleChange)) {
            case SweepStep.Continue k -> timers.upsertSweep(c, rule.tenantId(), rule.id(), now,
                    Json.object().put("cursor", k.cursor()).put("at", k.sweepInstant().toString()));
            case SweepStep.Schedule sc -> timers.upsertSweep(c, rule.tenantId(), rule.id(), sc.at(), Json.object().put("at", sc.at().toString()));
            case SweepStep.Stop st -> { }
        }
    }
}
