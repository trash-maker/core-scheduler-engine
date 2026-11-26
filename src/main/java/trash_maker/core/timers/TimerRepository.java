package trash_maker.core.timers;

import com.fasterxml.jackson.databind.JsonNode;
import trash_maker.core.db.Jdbc;
import trash_maker.core.db.Tx;
import trash_maker.core.ports.TimerWakeup;
import trash_maker.domain.notification.TimerKind;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

public final class TimerRepository {
    private final TimerWakeup wakeup;

    public TimerRepository(TimerWakeup wakeup) { this.wakeup = wakeup; }

    private void hint(Instant due) { Tx.afterCommit(() -> wakeup.hint(due)); }

    public long insert(Connection c, TimerKind kind, Instant due, String tenant, UUID notificationId, UUID seriesId,
                       String ruleId, String key, JsonNode payload) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("""
                INSERT INTO timer (kind, due_at, tenant_id, notification_id, series_id, rule_id, correlation_key, payload)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?::jsonb) RETURNING id""")) {
            ps.setString(1, kind.name());
            Jdbc.setTs(ps, 2, due);
            ps.setString(3, tenant);
            ps.setObject(4, notificationId);
            ps.setObject(5, seriesId);
            ps.setString(6, ruleId);
            ps.setString(7, key);
            Jdbc.setJson(ps, 8, payload);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                hint(due);
                return rs.getLong(1);
            }
        }
    }

    public record NewTimer(TimerKind kind, Instant due, String tenant, UUID notificationId, UUID seriesId, String ruleId, String key, JsonNode payload) {}

    /** Batched insert (one round trip with reWriteBatchedInserts). */
    public void insertAll(Connection c, List<NewTimer> timers) throws SQLException {
        if (timers.isEmpty()) return;
        try (PreparedStatement ps = c.prepareStatement("""
                INSERT INTO timer (kind, due_at, tenant_id, notification_id, series_id, rule_id, correlation_key, payload)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?::jsonb)""")) {
            Instant min = null;
            for (NewTimer t : timers) {
                ps.setString(1, t.kind().name());
                Jdbc.setTs(ps, 2, t.due());
                ps.setString(3, t.tenant());
                ps.setObject(4, t.notificationId());
                ps.setObject(5, t.seriesId());
                ps.setString(6, t.ruleId());
                ps.setString(7, t.key());
                Jdbc.setJson(ps, 8, t.payload());
                ps.addBatch();
                if (min == null || t.due().isBefore(min)) min = t.due();
            }
            ps.executeBatch();
            hint(min);
        }
    }

    public void deleteByIds(Connection c, List<Long> ids) throws SQLException {
        if (ids.isEmpty()) return;
        try (PreparedStatement ps = c.prepareStatement("DELETE FROM timer WHERE id = ANY(?)")) {
            ps.setArray(1, c.createArrayOf("bigint", ids.toArray()));
            ps.executeUpdate();
        }
    }

    public void deleteForNotifications(Connection c, List<UUID> ids, Set<TimerKind> kinds) throws SQLException {
        if (ids.isEmpty()) return;
        try (PreparedStatement ps = c.prepareStatement("DELETE FROM timer WHERE notification_id = ANY(?) AND kind = ANY(?)")) {
            ps.setArray(1, Jdbc.uuidArray(c, ids));
            ps.setArray(2, Jdbc.textArray(c, kinds.stream().map(Enum::name).toList()));
            ps.executeUpdate();
        }
    }

    /** One RULE_EVAL timer per (tenant, rule, key): moved to the new instant. */
    public void upsertRuleEval(Connection c, String tenant, String ruleId, String key, Instant due, JsonNode payload) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("""
                INSERT INTO timer (kind, due_at, tenant_id, rule_id, correlation_key, payload)
                VALUES ('RULE_EVAL', ?, ?, ?, ?, ?::jsonb)
                ON CONFLICT (tenant_id, rule_id, correlation_key) WHERE kind = 'RULE_EVAL'
                DO UPDATE SET due_at = excluded.due_at, payload = excluded.payload, failures = 0, last_error = NULL""")) {
            Jdbc.setTs(ps, 1, due);
            ps.setString(2, tenant);
            ps.setString(3, ruleId);
            ps.setString(4, key);
            Jdbc.setJson(ps, 5, payload);
            ps.executeUpdate();
            hint(due);
        }
    }

    /** Per-rule sweep timer: keeps the earliest due time (a running sweep's continuation is not pushed back). */
    public void upsertSweep(Connection c, String tenant, String ruleId, Instant due, JsonNode payload) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("""
                INSERT INTO timer (kind, due_at, tenant_id, rule_id, correlation_key, payload)
                VALUES ('RULE_EVAL', ?, ?, ?, '*', ?::jsonb)
                ON CONFLICT (tenant_id, rule_id, correlation_key) WHERE kind = 'RULE_EVAL'
                DO UPDATE SET due_at = least(timer.due_at, excluded.due_at),
                              payload = CASE WHEN excluded.due_at < timer.due_at THEN excluded.payload ELSE timer.payload END""")) {
            Jdbc.setTs(ps, 1, due);
            ps.setString(2, tenant);
            ps.setString(3, ruleId);
            Jdbc.setJson(ps, 4, payload);
            ps.executeUpdate();
            hint(due);
        }
    }

    public void upsertRuleEvalAll(Connection c, List<NewTimer> timers) throws SQLException {
        if (timers.isEmpty()) return;
        try (PreparedStatement ps = c.prepareStatement("""
                INSERT INTO timer (kind, due_at, tenant_id, rule_id, correlation_key, payload)
                VALUES ('RULE_EVAL', ?, ?, ?, ?, NULL)
                ON CONFLICT (tenant_id, rule_id, correlation_key) WHERE kind = 'RULE_EVAL'
                DO UPDATE SET due_at = excluded.due_at, failures = 0, last_error = NULL""")) {
            Instant min = null;
            for (NewTimer t : timers) {
                Jdbc.setTs(ps, 1, t.due());
                ps.setString(2, t.tenant());
                ps.setString(3, t.ruleId());
                ps.setString(4, t.key());
                ps.addBatch();
                if (min == null || t.due().isBefore(min)) min = t.due();
            }
            ps.executeBatch();
            hint(min);
        }
    }

    public void deleteRuleEvalAll(Connection c, List<String[]> tenantRuleKey) throws SQLException {
        if (tenantRuleKey.isEmpty()) return;
        try (PreparedStatement ps = c.prepareStatement("""
                DELETE FROM timer t USING unnest(?::text[], ?::text[], ?::text[]) AS x(tn, r, k)
                WHERE t.kind = 'RULE_EVAL' AND t.tenant_id = x.tn AND t.rule_id = x.r AND t.correlation_key = x.k""")) {
            ps.setArray(1, c.createArrayOf("text", tenantRuleKey.stream().map(a -> a[0]).toArray()));
            ps.setArray(2, c.createArrayOf("text", tenantRuleKey.stream().map(a -> a[1]).toArray()));
            ps.setArray(3, c.createArrayOf("text", tenantRuleKey.stream().map(a -> a[2]).toArray()));
            ps.executeUpdate();
        }
    }

    public void deleteRuleEval(Connection c, String tenant, String ruleId, String key) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("DELETE FROM timer WHERE kind='RULE_EVAL' AND tenant_id=? AND rule_id=? AND correlation_key=?")) {
            ps.setString(1, tenant);
            ps.setString(2, ruleId);
            ps.setString(3, key);
            ps.executeUpdate();
        }
    }

    public int deleteForNotification(Connection c, UUID notificationId, Set<TimerKind> kinds) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("DELETE FROM timer WHERE notification_id=? AND kind = ANY(?)")) {
            ps.setObject(1, notificationId);
            ps.setArray(2, Jdbc.textArray(c, kinds.stream().map(Enum::name).toList()));
            return ps.executeUpdate();
        }
    }

    public int deleteForSeries(Connection c, UUID seriesId) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("DELETE FROM timer WHERE series_id=?")) {
            ps.setObject(1, seriesId);
            return ps.executeUpdate();
        }
    }

    public int deleteById(Connection c, long id) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("DELETE FROM timer WHERE id=?")) {
            ps.setLong(1, id);
            return ps.executeUpdate();
        }
    }

    /** Claims due timers: rows are deleted in the claiming transaction (rollback = they reappear). */
    public List<TimerRow> claim(Connection c, Instant now, int limit) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("""
                DELETE FROM timer WHERE id IN (
                    SELECT id FROM timer WHERE due_at <= ? ORDER BY due_at LIMIT ? FOR UPDATE SKIP LOCKED)
                RETURNING *""")) {
            Jdbc.setTs(ps, 1, now);
            ps.setInt(2, limit);
            List<TimerRow> out = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) { while (rs.next()) out.add(map(rs)); }
            out.sort(java.util.Comparator.comparing(TimerRow::dueAt).thenComparing(TimerRow::id));
            return out;
        }
    }

    /** Re-inserts a failed timer with backoff (RULE_EVAL: a newer timer for the entity wins). */
    public void reschedule(Connection c, TimerRow t, Instant due, String error) throws SQLException {
        String conflict = t.kind() == TimerKind.RULE_EVAL ? " ON CONFLICT (tenant_id, rule_id, correlation_key) WHERE kind = 'RULE_EVAL' DO NOTHING" : "";
        try (PreparedStatement ps = c.prepareStatement("""
                INSERT INTO timer (kind, due_at, tenant_id, notification_id, series_id, rule_id, correlation_key, payload, failures, last_error, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?, ?, ?)""" + conflict)) {
            ps.setString(1, t.kind().name());
            Jdbc.setTs(ps, 2, due);
            ps.setString(3, t.tenantId());
            ps.setObject(4, t.notificationId());
            ps.setObject(5, t.seriesId());
            ps.setString(6, t.ruleId());
            ps.setString(7, t.correlationKey());
            Jdbc.setJson(ps, 8, t.payload());
            ps.setInt(9, t.failures() + 1);
            ps.setString(10, truncate(error));
            Jdbc.setTs(ps, 11, t.createdAt());
            ps.executeUpdate();
        }
    }

    public void dead(Connection c, TimerRow t, String error, Instant now) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("""
                INSERT INTO timer_dead (id, kind, due_at, tenant_id, notification_id, series_id, rule_id, correlation_key, payload, failures, last_error, created_at, died_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?, ?, ?, ?) ON CONFLICT (id) DO NOTHING""")) {
            ps.setLong(1, t.id());
            ps.setString(2, t.kind().name());
            Jdbc.setTs(ps, 3, t.dueAt());
            ps.setString(4, t.tenantId());
            ps.setObject(5, t.notificationId());
            ps.setObject(6, t.seriesId());
            ps.setString(7, t.ruleId());
            ps.setString(8, t.correlationKey());
            Jdbc.setJson(ps, 9, t.payload());
            ps.setInt(10, t.failures() + 1);
            ps.setString(11, truncate(error));
            Jdbc.setTs(ps, 12, t.createdAt());
            Jdbc.setTs(ps, 13, now);
            ps.executeUpdate();
        }
    }

    /** Runbook: move dead timers back to the queue (due now, failures reset). */
    public int requeueDead(Connection c, List<Long> ids, Instant now) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("""
                WITH moved AS (DELETE FROM timer_dead WHERE id = ANY(?) RETURNING *)
                INSERT INTO timer (kind, due_at, tenant_id, notification_id, series_id, rule_id, correlation_key, payload, failures, created_at)
                SELECT kind, ?, tenant_id, notification_id, series_id, rule_id, correlation_key, payload, 0, created_at FROM moved
                ON CONFLICT DO NOTHING""")) {
            ps.setArray(1, c.createArrayOf("bigint", ids.toArray()));
            Jdbc.setTs(ps, 2, now);
            int n = ps.executeUpdate();
            if (n > 0) hint(now);
            return n;
        }
    }

    public Instant nextDue(Connection c) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("SELECT min(due_at) FROM timer");
             ResultSet rs = ps.executeQuery()) {
            rs.next();
            var o = rs.getObject(1, java.time.OffsetDateTime.class);
            return o == null ? null : o.toInstant();
        }
    }

    public List<TimerRow> listDead(Connection c, String tenant, long afterId, int limit) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(
                "SELECT *, NULL::timestamptz AS x FROM timer_dead WHERE (?::text IS NULL OR tenant_id = ?) AND id > ? ORDER BY id LIMIT ?")) {
            ps.setString(1, tenant);
            ps.setString(2, tenant);
            ps.setLong(3, afterId);
            ps.setInt(4, limit);
            List<TimerRow> out = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) { while (rs.next()) out.add(map(rs)); }
            return out;
        }
    }

    static TimerRow map(ResultSet rs) throws SQLException {
        return new TimerRow(rs.getLong("id"), TimerKind.valueOf(rs.getString("kind")), Jdbc.getTs(rs, "due_at"), rs.getString("tenant_id"),
                Jdbc.getUuid(rs, "notification_id"), Jdbc.getUuid(rs, "series_id"), rs.getString("rule_id"), rs.getString("correlation_key"),
                Jdbc.getJson(rs, "payload"), rs.getInt("failures"), rs.getString("last_error"), Jdbc.getTs(rs, "created_at"));
    }

    private static String truncate(String s) { return s == null ? null : s.length() > 2000 ? s.substring(0, 2000) : s; }
}
