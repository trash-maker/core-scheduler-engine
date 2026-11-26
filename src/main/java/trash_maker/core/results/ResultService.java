package trash_maker.core.results;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import jakarta.transaction.Transactional;

import trash_maker.core.db.Db;
import trash_maker.core.dispatch.DeadLetters;
import trash_maker.core.notifications.NotificationEngine;
import trash_maker.core.notifications.NotificationRow;
import trash_maker.core.ports.EngineMetrics;
import trash_maker.domain.notification.Event;
import trash_maker.domain.notification.OutcomeMessage;
import trash_maker.domain.notification.OutcomeType;
import org.jboss.logging.Logger;
import org.jboss.logging.MDC;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Actuator outcomes (notification.results): batch -> lock notifications in id order -> validate attempt and action ->
 * state machine -> effects (retry timer, children, series) in one transaction.
 */
@Singleton
public class ResultService {
    private static final Logger LOG = Logger.getLogger(ResultService.class);

    private final Db db;
    private final NotificationEngine engine;
    private final DeadLetters dlq;
    private final EngineMetrics metrics;
    private final Clock clock;

    @Inject
    public ResultService(Db db, NotificationEngine engine, DeadLetters dlq, EngineMetrics metrics, Clock clock) {
        this.db = db;
        this.engine = engine;
        this.dlq = dlq;
        this.metrics = metrics;
        this.clock = clock;
    }

    private record Outcome(OutcomeMessage msg, String raw) {
        UUID notificationId() { return msg.notificationId(); }
        Instant occurredAt() { return msg.occurredAt(); }
        int attempt() { return msg.attempt(); }
        String tenantId() { return msg.tenantId(); }
        String action() { return msg.action(); }
        trash_maker.domain.notification.OutcomeType outcome() { return msg.outcome(); }
        static Outcome parse(String raw) { return new Outcome(OutcomeMessage.parse(raw), raw); }
    }

    private static Event event(Outcome o) { return o.msg().toEvent(); }

    public record Stats(int received, int invalid, int applied, int ignored) {}

    @Transactional
    public Stats process(List<String> payloads) {
        List<Outcome> valid = new ArrayList<>();
        List<String[]> invalid = new ArrayList<>();
        for (String p : payloads) {
            try { valid.add(Outcome.parse(p)); } catch (RuntimeException e) { invalid.add(new String[]{p, e.getMessage()}); }
        }
        metrics.count("results.received", payloads.size());
        return db.with(c -> {
            Instant now = clock.instant();
            for (String[] inv : invalid) dlq.record(c, "notification.results", null, "INVALID_MESSAGE", inv[1], inv[0], now);
            Map<UUID, NotificationRow> rows = new HashMap<>();
            for (NotificationRow r : engine.repository().lockMany(c, new LinkedHashSet<>(valid.stream().map(Outcome::notificationId).toList())))
                rows.put(r.id(), r);
            valid.sort(Comparator.comparing(Outcome::notificationId).thenComparing(Outcome::occurredAt).thenComparing(Outcome::attempt));
            Map<UUID, List<Outcome>> byId = new java.util.LinkedHashMap<>();
            int ignored = 0;
            for (Outcome o : valid) {
                NotificationRow r = rows.get(o.notificationId());
                OutcomeMessage.Verdict v = o.msg().check(r == null ? null : r.tenantId(), r == null ? null : r.action());
                if (v != OutcomeMessage.Verdict.APPLY) {
                    ignored++;
                    metrics.count("results.ignored", "verdict", v.name());
                    if (v == OutcomeMessage.Verdict.ACTION_MISMATCH) {
                        metrics.count("results.action_mismatch");
                        LOG.warnf("result for %s attempt %d ignored: action %s does not match %s", r.id(), o.attempt(), o.action(), r.action());
                    }
                    continue;
                }
                metrics.count("results.outcome", "outcome", o.outcome().name(), "action", r.action());
                byId.computeIfAbsent(o.notificationId(), k -> new ArrayList<>()).add(o);
            }
            List<NotificationEngine.Step> steps = new ArrayList<>();
            List<Outcome> sequential = new ArrayList<>();
            for (var e : byId.entrySet()) {
                if (e.getValue().size() == 1) steps.add(new NotificationEngine.Step(rows.get(e.getKey()), event(e.getValue().get(0))));
                else sequential.addAll(e.getValue());
            }
            int applied = 0;
            List<NotificationRow> results;
            try {
                results = Db.savepoint(c, cx -> engine.applyAll(cx, steps, now));
                for (int i = 0; i < steps.size(); i++) {
                    if (results.get(i) != steps.get(i).row()) applied++; else ignored++;
                }
            } catch (RuntimeException | java.sql.SQLException batchFailure) {
                LOG.warnf("batched outcome handling failed (%s), applying one by one", batchFailure.getMessage());
                for (var st : steps) sequential.add(byId.get(st.row().id()).get(0));
            }
            for (Outcome o : sequential) {
                NotificationRow r = rows.get(o.notificationId());
                MDC.put("notification_id", r.id().toString());
                MDC.put("tenant_id", r.tenantId());
                try {
                    NotificationRow updated = Db.savepoint(c, cx -> engine.apply(cx, r, event(o), now));
                    rows.put(r.id(), updated);
                    if (updated != r) applied++; else ignored++;
                } catch (RuntimeException e) {
                    LOG.errorf(e, "applying outcome for %s failed", r.id());
                    dlq.record(c, "notification.results", r.tenantId(), "OUTCOME_FAILED", e.getMessage(), o.raw(), now);
                } finally {
                    MDC.remove("notification_id");
                    MDC.remove("tenant_id");
                }
            }
            return new Stats(payloads.size(), invalid.size(), applied, ignored);
        });
    }
}
