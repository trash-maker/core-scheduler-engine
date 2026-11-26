package io.notifengine.core.ops;

import io.notifengine.core.EngineConfig;
import io.notifengine.core.db.Jdbc;
import io.notifengine.core.db.Tx;
import org.jboss.logging.Logger;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Clock;
import java.time.Instant;
import java.time.YearMonth;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Partition maintenance, retention and queue statistics. */
public final class Maintenance {
    private static final Logger LOG = Logger.getLogger(Maintenance.class);
    private static final Pattern PART = Pattern.compile("notification_p(\\d{4})_(\\d{2})");
    private static final int DELETE_BATCH = 5000;

    private final Tx tx;
    private final EngineConfig cfg;
    private final Clock clock;

    public Maintenance(Tx tx, EngineConfig cfg, Clock clock) {
        this.tx = tx;
        this.cfg = cfg;
        this.clock = clock;
    }

    /** Creates monthly partitions from last month to {@code monthsAhead} months ahead. */
    public int ensurePartitions(int monthsAhead) {
        return tx.inTx(c -> {
            try (PreparedStatement ps = c.prepareStatement("SELECT ensure_monthly_partitions(?, ?)")) {
                Jdbc.setTs(ps, 1, clock.instant());
                ps.setInt(2, monthsAhead);
                try (ResultSet rs = ps.executeQuery()) { rs.next(); return rs.getInt(1); }
            }
        });
    }

    public record RetentionReport(int notificationsDeleted, List<String> partitionsDropped, int dedupDeleted, int inboxDeleted,
                                  int deadLettersDeleted, int seriesDeleted) {}

    public RetentionReport retention() {
        Instant now = clock.instant();
        Instant cutoff = now.minus(cfg.retention());
        int notifs = deleteLoop("""
                DELETE FROM notification WHERE (id, created_at) IN (
                  SELECT id, created_at FROM notification WHERE terminal_at IS NOT NULL AND terminal_at < ? LIMIT %d)""".formatted(DELETE_BATCH), cutoff);
        List<String> dropped = dropOldPartitions(cutoff);
        int dedup = deleteLoop("DELETE FROM notification_dedup WHERE idempotency_key IN (SELECT idempotency_key FROM notification_dedup WHERE created_at < ? LIMIT %d)".formatted(DELETE_BATCH),
                now.minus(cfg.dedupRetention()));
        int inbox = deleteLoop("DELETE FROM signal_inbox WHERE (tenant_id, signal_id) IN (SELECT tenant_id, signal_id FROM signal_inbox WHERE received_at < ? LIMIT %d)".formatted(DELETE_BATCH),
                now.minus(cfg.inboxRetention()));
        int dl = deleteLoop("DELETE FROM dead_letter WHERE id IN (SELECT id FROM dead_letter WHERE created_at < ? LIMIT %d)".formatted(DELETE_BATCH), cutoff);
        int series = deleteLoop("DELETE FROM recurring_series WHERE id IN (SELECT id FROM recurring_series WHERE status = 'CLOSED' AND updated_at < ? LIMIT %d)".formatted(DELETE_BATCH), cutoff);
        deleteLoop("DELETE FROM timer_dead WHERE id IN (SELECT id FROM timer_dead WHERE died_at < ? LIMIT %d)".formatted(DELETE_BATCH), cutoff);
        RetentionReport r = new RetentionReport(notifs, dropped, dedup, inbox, dl, series);
        LOG.infof("retention: %s", r);
        return r;
    }

    private int deleteLoop(String sql, Instant cutoff) {
        int total = 0;
        while (true) {
            int n = tx.inTx(c -> {
                try (PreparedStatement ps = c.prepareStatement(sql)) {
                    Jdbc.setTs(ps, 1, cutoff);
                    return ps.executeUpdate();
                }
            });
            total += n;
            if (n < DELETE_BATCH) return total;
        }
    }

    /** Drops monthly partitions entirely older than the cutoff that hold no active notification. */
    private List<String> dropOldPartitions(Instant cutoff) {
        List<String> dropped = new ArrayList<>();
        List<String> parts = tx.read(c -> {
            List<String> out = new ArrayList<>();
            try (PreparedStatement ps = c.prepareStatement(
                    "SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid WHERE i.inhparent = 'notification'::regclass ORDER BY 1");
                 ResultSet rs = ps.executeQuery()) {
                while (rs.next()) out.add(rs.getString(1));
            }
            return out;
        });
        for (String p : parts) {
            Matcher m = PART.matcher(p);
            if (!m.matches()) continue;
            YearMonth ym = YearMonth.of(Integer.parseInt(m.group(1)), Integer.parseInt(m.group(2)));
            Instant upper = ym.plusMonths(1).atDay(1).atStartOfDay().toInstant(ZoneOffset.UTC);
            if (!upper.isBefore(cutoff)) continue;
            boolean ok = tx.inTx(c -> {
                try (PreparedStatement ps = c.prepareStatement("SELECT 1 FROM " + p + " WHERE terminal_at IS NULL LIMIT 1");
                     ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) return false;
                }
                String hist = p.replace("notification_p", "notification_history_p");
                try (var st = c.createStatement()) {
                    st.execute("ALTER TABLE notification DETACH PARTITION " + p);
                    st.execute("DROP TABLE " + p);
                    st.execute("DROP TABLE IF EXISTS " + hist);
                }
                return true;
            });
            if (ok) dropped.add(p);
        }
        return dropped;
    }

    public record QueueStats(Map<String, Long> pendingTimersByKind, Long oldestDueLagMs, long outboxSize, Long outboxOldestAgeMs,
                             long deadTimers, long deadLetters24h) {}

    public QueueStats stats() {
        Instant now = clock.instant();
        return tx.read(c -> {
            Map<String, Long> byKind = new LinkedHashMap<>();
            Long lag = null;
            try (PreparedStatement ps = c.prepareStatement("SELECT kind, count(*), min(due_at) FROM timer GROUP BY kind");
                 ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    byKind.put(rs.getString(1), rs.getLong(2));
                    Instant oldest = rs.getObject(3, java.time.OffsetDateTime.class).toInstant();
                    long l = Math.max(0, now.toEpochMilli() - oldest.toEpochMilli());
                    lag = lag == null ? l : Math.max(lag, l);
                }
            }
            long outbox;
            Long outboxAge = null;
            try (PreparedStatement ps = c.prepareStatement("SELECT count(*), min(created_at) FROM outbox"); ResultSet rs = ps.executeQuery()) {
                rs.next();
                outbox = rs.getLong(1);
                var o = rs.getObject(2, java.time.OffsetDateTime.class);
                if (o != null) outboxAge = Math.max(0, now.toEpochMilli() - o.toInstant().toEpochMilli());
            }
            long dead;
            try (PreparedStatement ps = c.prepareStatement("SELECT count(*) FROM timer_dead"); ResultSet rs = ps.executeQuery()) { rs.next(); dead = rs.getLong(1); }
            long dl;
            try (PreparedStatement ps = c.prepareStatement("SELECT count(*) FROM dead_letter WHERE created_at > ?")) {
                Jdbc.setTs(ps, 1, now.minusSeconds(86400));
                try (ResultSet rs = ps.executeQuery()) { rs.next(); dl = rs.getLong(1); }
            }
            return new QueueStats(byKind, lag, outbox, outboxAge, dead, dl);
        });
    }
}
