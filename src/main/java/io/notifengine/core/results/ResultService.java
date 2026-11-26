package io.notifengine.core.results;

import com.fasterxml.jackson.databind.JsonNode;
import io.notifengine.common.Json;
import io.notifengine.core.db.Tx;
import io.notifengine.core.dispatch.DeadLetters;
import io.notifengine.core.notifications.NotificationEngine;
import io.notifengine.core.notifications.NotificationRow;
import io.notifengine.core.ports.EngineMetrics;
import io.notifengine.domain.notification.Event;
import io.notifengine.domain.notification.OutcomeType;
import org.jboss.logging.Logger;

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
public final class ResultService {
    private static final Logger LOG = Logger.getLogger(ResultService.class);

    private final Tx tx;
    private final NotificationEngine engine;
    private final DeadLetters dlq;
    private final EngineMetrics metrics;
    private final Clock clock;

    public ResultService(Tx tx, NotificationEngine engine, DeadLetters dlq, EngineMetrics metrics, Clock clock) {
        this.tx = tx;
        this.engine = engine;
        this.dlq = dlq;
        this.metrics = metrics;
        this.clock = clock;
    }

    public record Outcome(UUID notificationId, int attempt, String action, OutcomeType outcome, String reason, Instant occurredAt,
                          String tenantId, String raw) {
        static Outcome parse(String raw) {
            JsonNode n = Json.parse(raw);
            String id = n.path("notification_id").asText(null);
            if (id == null) throw new IllegalArgumentException("missing notification_id");
            if (!n.path("attempt").canConvertToInt() || n.path("attempt").asInt() < 1) throw new IllegalArgumentException("attempt must be >= 1");
            String action = n.path("action").asText(null);
            if (action == null) throw new IllegalArgumentException("missing action");
            OutcomeType o;
            try { o = OutcomeType.valueOf(n.path("outcome").asText()); } catch (IllegalArgumentException e) { throw new IllegalArgumentException("outcome must be SUCCESS, FAILURE or PERMANENT_FAILURE"); }
            Instant at = n.hasNonNull("occurred_at") ? Instant.parse(n.get("occurred_at").asText()) : Instant.EPOCH;
            return new Outcome(UUID.fromString(id), n.get("attempt").asInt(), action, o,
                    n.hasNonNull("reason") ? n.get("reason").asText() : null, at, n.hasNonNull("tenant_id") ? n.get("tenant_id").asText() : null, raw);
        }
    }

    public record Stats(int received, int invalid, int applied, int ignored) {}

    public Stats process(List<String> payloads) {
        List<Outcome> valid = new ArrayList<>();
        List<String[]> invalid = new ArrayList<>();
        for (String p : payloads) {
            try { valid.add(Outcome.parse(p)); } catch (RuntimeException e) { invalid.add(new String[]{p, e.getMessage()}); }
        }
        metrics.count("results.received", payloads.size());
        return tx.inTx(c -> {
            Instant now = clock.instant();
            for (String[] inv : invalid) dlq.record(c, "notification.results", null, "INVALID_MESSAGE", inv[1], inv[0], now);
            Map<UUID, NotificationRow> rows = new HashMap<>();
            for (NotificationRow r : engine.repository().lockMany(c, new LinkedHashSet<>(valid.stream().map(Outcome::notificationId).toList())))
                rows.put(r.id(), r);
            valid.sort(Comparator.comparing(Outcome::notificationId).thenComparing(Outcome::occurredAt).thenComparing(Outcome::attempt));
            int applied = 0, ignored = 0;
            for (Outcome o : valid) {
                NotificationRow r = rows.get(o.notificationId());
                if (r == null) { ignored++; metrics.count("results.unknown_notification"); continue; }
                if (o.tenantId() != null && !o.tenantId().equals(r.tenantId())) { ignored++; metrics.count("results.tenant_mismatch"); continue; }
                if (!o.action().equals(r.action())) {
                    ignored++;
                    metrics.count("results.action_mismatch");
                    LOG.warnf("result for %s attempt %d ignored: action %s does not match %s", r.id(), o.attempt(), o.action(), r.action());
                    continue;
                }
                metrics.count("results.outcome", "outcome", o.outcome().name(), "action", r.action());
                try {
                    NotificationRow updated = Tx.savepoint(c, cx -> engine.apply(cx, r, new Event.Outcome(o.attempt(), o.outcome(), o.reason(), o.occurredAt()), now));
                    rows.put(r.id(), updated);
                    if (updated != r) applied++; else ignored++;
                } catch (RuntimeException e) {
                    LOG.errorf(e, "applying outcome for %s failed", r.id());
                    dlq.record(c, "notification.results", r.tenantId(), "OUTCOME_FAILED", e.getMessage(), o.raw(), now);
                }
            }
            return new Stats(payloads.size(), invalid.size(), applied, ignored);
        });
    }
}
