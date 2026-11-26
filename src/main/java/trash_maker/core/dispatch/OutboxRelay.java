package trash_maker.core.dispatch;

import trash_maker.common.Json;
import trash_maker.core.EngineConfig;
import trash_maker.core.db.Tx;
import trash_maker.core.notifications.NotificationRepository;
import trash_maker.core.ports.EngineMetrics;
import trash_maker.core.ports.MessagePublisher;
import trash_maker.core.timers.TimerRepository;
import trash_maker.domain.notification.TimerKind;
import org.jboss.logging.Logger;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Outbox relay: claims a batch with SKIP LOCKED, publishes with the idempotent producer, waits for acks,
 * deletes acknowledged rows and arms attempt-timeout timers - all in one short transaction.
 * Unacknowledged rows stay for the next round: Kafka publish errors are never counted as attempts.
 */
public final class OutboxRelay {
    private static final Logger LOG = Logger.getLogger(OutboxRelay.class);

    private final Tx tx;
    private final OutboxRepository outbox;
    private final TimerRepository timers;
    private final NotificationRepository notifs;
    private final MessagePublisher publisher;
    private final EngineConfig cfg;
    private final EngineMetrics metrics;
    private final Clock clock;

    public OutboxRelay(Tx tx, OutboxRepository outbox, TimerRepository timers, NotificationRepository notifs, MessagePublisher publisher,
                       EngineConfig cfg, EngineMetrics metrics, Clock clock) {
        this.tx = tx;
        this.outbox = outbox;
        this.timers = timers;
        this.notifs = notifs;
        this.publisher = publisher;
        this.cfg = cfg;
        this.metrics = metrics;
        this.clock = clock;
    }

    public record Result(int claimed, int published, int failed) {}

    public Result relayOnce() {
        return tx.inTx(c -> {
            List<OutboxRepository.OutboxRow> rows = outbox.claim(c, cfg.relayBatchSize());
            if (rows.isEmpty()) return new Result(0, 0, 0);
            List<CompletableFuture<Void>> futures = new ArrayList<>(rows.size());
            for (var r : rows) futures.add(publisher.publish(r.topic(), r.key(), r.headers(), r.payload()));
            List<Long> acked = new ArrayList<>();
            List<OutboxRepository.OutboxRow> ackedRows = new ArrayList<>();
            int failed = 0;
            for (int i = 0; i < rows.size(); i++) {
                try {
                    futures.get(i).get(30, TimeUnit.SECONDS);
                    acked.add(rows.get(i).id());
                    ackedRows.add(rows.get(i));
                } catch (Exception e) {
                    failed++;
                    if (failed == 1) LOG.warnf("publish to %s failed (rows stay in outbox): %s", rows.get(i).topic(), e.getMessage());
                }
            }
            outbox.delete(c, acked);
            Instant now = clock.instant();
            for (var r : ackedRows) {
                metrics.recordMs("outbox.publish_lag", Math.max(0, Duration.between(r.createdAt(), now).toMillis()));
                if (r.notificationId() != null) {
                    metrics.count("notifications.published", "action", r.headers().getOrDefault("action", "unknown"));
                    if (r.attemptTimeoutMs() != null) {
                        long id = timers.insert(c, TimerKind.ATTEMPT_TIMEOUT, now.plusMillis(r.attemptTimeoutMs()), r.tenantId(), r.notificationId(),
                                null, null, null, Json.object().put("attempt", r.attempt()));
                        notifs.setAttemptTimer(c, r.notificationId(), r.attempt(), id);
                    }
                }
            }
            if (failed > 0) metrics.count("outbox.publish_errors", failed);
            return new Result(rows.size(), acked.size(), failed);
        });
    }
}
