package trash_maker.infra.ops;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import trash_maker.core.Engine;
import trash_maker.core.ops.Maintenance;
import trash_maker.infra.config.NotificationsConfig;
import io.quarkus.scheduler.Scheduled;
import io.agroal.api.AgroalDataSource;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/** Periodic jobs: rule hot reload, partition maintenance, retention (cluster-wide advisory lock), queue gauges. */
@ApplicationScoped
public class Jobs {
    private static final Logger LOG = Logger.getLogger(Jobs.class);
    private static final long RETENTION_LOCK = 0x6E6F7469L;

    @Inject Engine engine;
    @Inject NotificationsConfig cfg;
    @Inject MeterRegistry registry;
    @Inject AgroalDataSource ds;

    private final Map<String, AtomicLong> gauges = new ConcurrentHashMap<>();

    @Scheduled(every = "${notifications.rules.refresh-interval:2s}", concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    void refreshRules() { engine.registry.refresh(); }

    @Scheduled(every = "5m", delayed = "5m", concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    void reloadRules() { engine.registry.reloadAll(); }

    @Scheduled(cron = "0 17 3 * * ?", concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    void partitions() { engine.maintenance.ensurePartitions(cfg.retention().partitionsAhead()); }

    @Scheduled(every = "${notifications.retention.interval:1h}", delayed = "1m", concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    void retention() {
        try (Connection c = ds.getConnection()) {
            boolean locked;
            try (PreparedStatement ps = c.prepareStatement("SELECT pg_try_advisory_lock(?)")) {
                ps.setLong(1, RETENTION_LOCK);
                try (ResultSet rs = ps.executeQuery()) { rs.next(); locked = rs.getBoolean(1); }
            }
            if (!locked) return;
            try {
                engine.maintenance.retention();
            } finally {
                try (PreparedStatement ps = c.prepareStatement("SELECT pg_advisory_unlock(?)")) { ps.setLong(1, RETENTION_LOCK); ps.execute(); }
            }
        } catch (Exception e) {
            LOG.errorf("retention failed: %s", e.getMessage());
        }
    }

    @Scheduled(every = "15s", concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    void gauges() {
        Maintenance.QueueStats s = engine.maintenance.stats();
        for (String kind : new String[]{"PUBLISH", "ATTEMPT_TIMEOUT", "EXPIRE", "NEXT_OCCURRENCE", "RULE_EVAL"})
            gauge("notif.timers.pending", "kind", kind).set(s.pendingTimersByKind().getOrDefault(kind, 0L));
        gauge("notif.timers.oldest_due_lag.ms", null, null).set(s.oldestDueLagMs() == null ? 0 : s.oldestDueLagMs());
        gauge("notif.outbox.size", null, null).set(s.outboxSize());
        gauge("notif.outbox.oldest_age.ms", null, null).set(s.outboxOldestAgeMs() == null ? 0 : s.outboxOldestAgeMs());
        gauge("notif.timers.dead.total", null, null).set(s.deadTimers());
        gauge("notif.dlq.last24h", null, null).set(s.deadLetters24h());
    }

    private AtomicLong gauge(String name, String tag, String value) {
        return gauges.computeIfAbsent(name + tag + value, k -> {
            AtomicLong a = new AtomicLong();
            var b = Gauge.builder(name, a, AtomicLong::get);
            if (tag != null) b.tag(tag, value);
            b.register(registry);
            return a;
        });
    }
}
