package trash_maker.core.notifications;

import jakarta.inject.Singleton;

import trash_maker.core.Uuid7;
import trash_maker.core.db.Jdbc;
import trash_maker.domain.notification.Failure;
import trash_maker.domain.notification.NotificationStatus;
import trash_maker.domain.notification.StatusChange;
import trash_maker.domain.rule.RetryPolicy;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/** Plain JDBC access to notification, notification_history and notification_dedup. */
@Singleton
public class NotificationRepository {

    public static class ConcurrentUpdateException extends RuntimeException {
        public ConcurrentUpdateException(UUID id) { super("concurrent update of notification " + id); }
    }

    private static final String INSERT = """
            INSERT INTO notification (id, created_at, updated_at, tenant_id, rule_id, rule_version, template_id, correlation_key,
              action, type, status, status_reason, attempt, cycle_attempts, retry_policy, body, spec, context, due_at, expires_at,
              attempt_timer_id, version, idempotency_key, parent_id, parent_trigger, depth, series_id, occurrence_index, cancel_on,
              failed_once, last_failure, published_at, terminal_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?::jsonb,?::jsonb,?::jsonb,?::jsonb,?,?,?,?,?,?,?,?,?,?,?,?,?::jsonb,?,?)""";

    public void insert(Connection c, List<NotificationRow> rows) throws SQLException {
        if (rows.isEmpty()) return;
        try (PreparedStatement ps = c.prepareStatement(INSERT)) {
            for (NotificationRow r : rows) {
                int i = 1;
                ps.setObject(i++, r.id());
                Jdbc.setTs(ps, i++, r.createdAt());
                Jdbc.setTs(ps, i++, r.updatedAt());
                ps.setString(i++, r.tenantId());
                ps.setString(i++, r.ruleId());
                ps.setInt(i++, r.ruleVersion());
                ps.setString(i++, r.templateId());
                ps.setString(i++, r.correlationKey());
                ps.setString(i++, r.action());
                ps.setString(i++, r.type());
                ps.setString(i++, r.status().name());
                ps.setString(i++, r.statusReason());
                ps.setInt(i++, r.attempt());
                ps.setInt(i++, r.cycleAttempts());
                Jdbc.setJson(ps, i++, r.retryPolicy().toJson());
                Jdbc.setJson(ps, i++, r.body());
                Jdbc.setJson(ps, i++, r.spec());
                Jdbc.setJson(ps, i++, r.context());
                Jdbc.setTs(ps, i++, r.dueAt());
                Jdbc.setTs(ps, i++, r.expiresAt());
                Jdbc.setLong(ps, i++, r.attemptTimerId());
                ps.setInt(i++, r.version());
                ps.setString(i++, r.idempotencyKey());
                ps.setObject(i++, r.parentId());
                ps.setString(i++, r.parentTrigger());
                ps.setInt(i++, r.depth());
                ps.setObject(i++, r.seriesId());
                Jdbc.setInt(ps, i++, r.occurrenceIndex());
                ps.setArray(i++, Jdbc.textArray(c, r.cancelOn()));
                ps.setBoolean(i++, r.failedOnce());
                Jdbc.setJson(ps, i++, r.lastFailure() == null ? null : r.lastFailure().toJson());
                Jdbc.setTs(ps, i++, r.publishedAt());
                Jdbc.setTs(ps, i, r.terminalAt());
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    /** Targeted optimistic update of the mutable columns. Returns the row with the incremented version. */
    public NotificationRow update(Connection c, NotificationRow r) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("""
                UPDATE notification SET updated_at=?, status=?, status_reason=?, attempt=?, cycle_attempts=?, body=?::jsonb,
                  due_at=?, expires_at=?, attempt_timer_id=?, failed_once=?, last_failure=?::jsonb, published_at=?, terminal_at=?,
                  version = version + 1
                WHERE id=? AND created_at=? AND version=?""")) {
            int i = 1;
            Jdbc.setTs(ps, i++, r.updatedAt());
            ps.setString(i++, r.status().name());
            ps.setString(i++, r.statusReason());
            ps.setInt(i++, r.attempt());
            ps.setInt(i++, r.cycleAttempts());
            Jdbc.setJson(ps, i++, r.body());
            Jdbc.setTs(ps, i++, r.dueAt());
            Jdbc.setTs(ps, i++, r.expiresAt());
            Jdbc.setLong(ps, i++, r.attemptTimerId());
            ps.setBoolean(i++, r.failedOnce());
            Jdbc.setJson(ps, i++, r.lastFailure() == null ? null : r.lastFailure().toJson());
            Jdbc.setTs(ps, i++, r.publishedAt());
            Jdbc.setTs(ps, i++, r.terminalAt());
            ps.setObject(i++, r.id());
            Jdbc.setTs(ps, i++, r.createdAt());
            ps.setInt(i, r.version());
            if (ps.executeUpdate() != 1) throw new ConcurrentUpdateException(r.id());
        }
        return r.withVersion(r.version() + 1);
    }

    /** Batched optimistic update (one round trip). Returns rows with incremented versions. */
    public List<NotificationRow> updateAll(Connection c, List<NotificationRow> rows) throws SQLException {
        if (rows.isEmpty()) return List.of();
        try (PreparedStatement ps = c.prepareStatement("""
                UPDATE notification SET updated_at=?, status=?, status_reason=?, attempt=?, cycle_attempts=?, body=?::jsonb,
                  due_at=?, expires_at=?, attempt_timer_id=?, failed_once=?, last_failure=?::jsonb, published_at=?, terminal_at=?,
                  version = version + 1
                WHERE id=? AND created_at=? AND version=?""")) {
            for (NotificationRow r : rows) {
                int i = 1;
                Jdbc.setTs(ps, i++, r.updatedAt());
                ps.setString(i++, r.status().name());
                ps.setString(i++, r.statusReason());
                ps.setInt(i++, r.attempt());
                ps.setInt(i++, r.cycleAttempts());
                Jdbc.setJson(ps, i++, r.body());
                Jdbc.setTs(ps, i++, r.dueAt());
                Jdbc.setTs(ps, i++, r.expiresAt());
                Jdbc.setLong(ps, i++, r.attemptTimerId());
                ps.setBoolean(i++, r.failedOnce());
                Jdbc.setJson(ps, i++, r.lastFailure() == null ? null : r.lastFailure().toJson());
                Jdbc.setTs(ps, i++, r.publishedAt());
                Jdbc.setTs(ps, i++, r.terminalAt());
                ps.setObject(i++, r.id());
                Jdbc.setTs(ps, i++, r.createdAt());
                ps.setInt(i, r.version());
                ps.addBatch();
            }
            int[] counts = ps.executeBatch();
            for (int k = 0; k < counts.length; k++) if (counts[k] != 1) throw new ConcurrentUpdateException(rows.get(k).id());
        }
        return rows.stream().map(r -> r.withVersion(r.version() + 1)).toList();
    }

    public record HistoryRow(NotificationRow n, StatusChange change) {}

    public void historyAll(Connection c, List<HistoryRow> rows, Instant now) throws SQLException {
        if (rows.isEmpty()) return;
        try (PreparedStatement ps = c.prepareStatement("""
                INSERT INTO notification_history (notification_id, notification_created_at, tenant_id, at, from_status, to_status, event, attempt, reason)
                VALUES (?,?,?,?,?,?,?,?,?)""")) {
            for (HistoryRow h : rows) {
                StatusChange ch = h.change();
                ps.setObject(1, h.n().id());
                Jdbc.setTs(ps, 2, h.n().createdAt());
                ps.setString(3, h.n().tenantId());
                Jdbc.setTs(ps, 4, now);
                ps.setString(5, ch.from() == null ? null : ch.from().name());
                ps.setString(6, ch.to().name());
                ps.setString(7, ch.event());
                ps.setInt(8, ch.attempt());
                ps.setString(9, ch.reason() == null ? null : ch.reason().length() > 500 ? ch.reason().substring(0, 500) : ch.reason());
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    /** Bulk idempotency claim; returns the keys that were new. */
    public java.util.Set<String> claimIdempotencyKeys(Connection c, List<String> keys, List<UUID> ids, Instant now) throws SQLException {
        java.util.Set<String> out = new java.util.HashSet<>();
        if (keys.isEmpty()) return out;
        try (PreparedStatement ps = c.prepareStatement("""
                INSERT INTO notification_dedup (idempotency_key, notification_id, created_at)
                SELECT k, i, ? FROM unnest(?::text[], ?::uuid[]) AS x(k, i) ON CONFLICT DO NOTHING RETURNING idempotency_key""")) {
            Jdbc.setTs(ps, 1, now);
            ps.setArray(2, Jdbc.textArray(c, keys));
            ps.setArray(3, Jdbc.uuidArray(c, ids));
            try (ResultSet rs = ps.executeQuery()) { while (rs.next()) out.add(rs.getString(1)); }
        }
        return out;
    }

    /** Arms the attempt-timeout timer reference if the attempt is still the published one. */
    public int setAttemptTimer(Connection c, UUID id, int attempt, long timerId) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(
                "UPDATE notification SET attempt_timer_id=? WHERE id=? AND created_at=? AND attempt=? AND status='PUBLISHED'")) {
            ps.setLong(1, timerId);
            ps.setObject(2, id);
            Jdbc.setTs(ps, 3, Uuid7.timestamp(id));
            ps.setInt(4, attempt);
            return ps.executeUpdate();
        }
    }

    public Optional<NotificationRow> lock(Connection c, UUID id) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("SELECT * FROM notification WHERE id=? AND created_at=? FOR UPDATE")) {
            ps.setObject(1, id);
            Jdbc.setTs(ps, 2, Uuid7.timestamp(id));
            try (ResultSet rs = ps.executeQuery()) { return rs.next() ? Optional.of(map(rs)) : Optional.empty(); }
        }
    }

    public Optional<NotificationRow> find(Connection c, String tenant, UUID id) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("SELECT * FROM notification WHERE id=? AND created_at=? AND tenant_id=?")) {
            ps.setObject(1, id);
            Jdbc.setTs(ps, 2, Uuid7.timestamp(id));
            ps.setString(3, tenant);
            try (ResultSet rs = ps.executeQuery()) { return rs.next() ? Optional.of(map(rs)) : Optional.empty(); }
        }
    }

    /** Locks several notifications in id order (deadlock avoidance). */
    public List<NotificationRow> lockMany(Connection c, Collection<UUID> ids) throws SQLException {
        if (ids.isEmpty()) return List.of();
        List<UUID> sorted = ids.stream().distinct().sorted().toList();
        try (PreparedStatement ps = c.prepareStatement("""
                SELECT n.* FROM notification n
                JOIN unnest(?::uuid[], ?::timestamptz[]) AS k(id, created_at) ON n.id = k.id AND n.created_at = k.created_at
                ORDER BY n.id FOR UPDATE OF n""")) {
            ps.setArray(1, Jdbc.uuidArray(c, sorted));
            ps.setArray(2, Jdbc.tsArray(c, sorted.stream().map(Uuid7::timestamp).toList()));
            return list(ps);
        }
    }

    /** Children are created in the same firing as the parent, hence share its created_at (partition pruning). */
    public List<NotificationRow> lockWaitingChildren(Connection c, NotificationRow parent) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("""
                SELECT * FROM notification WHERE parent_id=? AND created_at=? AND status='WAITING_ON_PARENT'
                ORDER BY id FOR UPDATE""")) {
            ps.setObject(1, parent.id());
            Jdbc.setTs(ps, 2, parent.createdAt());
            return list(ps);
        }
    }

    public List<NotificationRow> lockCancellable(Connection c, String tenant, String key, String signal) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("""
                SELECT * FROM notification
                WHERE tenant_id=? AND correlation_key=? AND cardinality(cancel_on) > 0
                  AND status IN ('WAITING_ON_PARENT','CREATED','SCHEDULED','PUBLISHED','RETRY_SCHEDULED')
                  AND ? = ANY(cancel_on)
                ORDER BY id FOR UPDATE""")) {
            ps.setString(1, tenant);
            ps.setString(2, key);
            ps.setString(3, signal);
            return list(ps);
        }
    }

    /** Inserts the idempotency key; false if it already existed (duplicate firing). */
    public boolean claimIdempotencyKey(Connection c, String key, UUID id, Instant now) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(
                "INSERT INTO notification_dedup (idempotency_key, notification_id, created_at) VALUES (?,?,?) ON CONFLICT DO NOTHING")) {
            ps.setString(1, key);
            ps.setObject(2, id);
            Jdbc.setTs(ps, 3, now);
            return ps.executeUpdate() == 1;
        }
    }

    public void history(Connection c, NotificationRow n, List<StatusChange> changes, Instant now) throws SQLException {
        if (changes.isEmpty()) return;
        try (PreparedStatement ps = c.prepareStatement("""
                INSERT INTO notification_history (notification_id, notification_created_at, tenant_id, at, from_status, to_status, event, attempt, reason)
                VALUES (?,?,?,?,?,?,?,?,?)""")) {
            for (StatusChange ch : changes) {
                ps.setObject(1, n.id());
                Jdbc.setTs(ps, 2, n.createdAt());
                ps.setString(3, n.tenantId());
                Jdbc.setTs(ps, 4, now);
                ps.setString(5, ch.from() == null ? null : ch.from().name());
                ps.setString(6, ch.to().name());
                ps.setString(7, ch.event());
                ps.setInt(8, ch.attempt());
                ps.setString(9, ch.reason() == null ? null : ch.reason().length() > 500 ? ch.reason().substring(0, 500) : ch.reason());
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    public record HistoryEntry(long id, Instant at, String from, String to, String event, Integer attempt, String reason) {}

    public List<HistoryEntry> historyOf(Connection c, String tenant, UUID id) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("""
                SELECT * FROM notification_history WHERE notification_id=? AND notification_created_at=? AND tenant_id=? ORDER BY id""")) {
            ps.setObject(1, id);
            Jdbc.setTs(ps, 2, Uuid7.timestamp(id));
            ps.setString(3, tenant);
            List<HistoryEntry> out = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) out.add(new HistoryEntry(rs.getLong("id"), Jdbc.getTs(rs, "at"), rs.getString("from_status"),
                        rs.getString("to_status"), rs.getString("event"), Jdbc.getInt(rs, "attempt"), rs.getString("reason")));
            }
            return out;
        }
    }

    public record SearchFilter(String tenantId, UUID id, NotificationStatus status, String action, String correlationKey,
                               Instant dueFrom, Instant dueTo, String ruleId, UUID cursor, int limit) {}

    /** Keyset pagination by id descending (UUIDv7 = creation order). Tenant filter always applied. */
    public List<NotificationRow> search(Connection c, SearchFilter f) throws SQLException {
        StringBuilder sql = new StringBuilder("SELECT * FROM notification WHERE tenant_id = ?");
        List<Object> args = new ArrayList<>();
        args.add(f.tenantId());
        if (f.id() != null) { sql.append(" AND id = ? AND created_at = ?"); args.add(f.id()); args.add(Jdbc.ts(Uuid7.timestamp(f.id()))); }
        if (f.status() != null) { sql.append(" AND status = ?"); args.add(f.status().name()); }
        if (f.action() != null) { sql.append(" AND action = ?"); args.add(f.action()); }
        if (f.correlationKey() != null) { sql.append(" AND correlation_key = ?"); args.add(f.correlationKey()); }
        if (f.ruleId() != null) { sql.append(" AND rule_id = ?"); args.add(f.ruleId()); }
        if (f.dueFrom() != null) { sql.append(" AND due_at >= ?"); args.add(Jdbc.ts(f.dueFrom())); }
        if (f.dueTo() != null) { sql.append(" AND due_at < ?"); args.add(Jdbc.ts(f.dueTo())); }
        if (f.cursor() != null) {
            sql.append(" AND id < ? AND created_at <= ?");
            args.add(f.cursor());
            args.add(Jdbc.ts(Uuid7.timestamp(f.cursor())));
        }
        sql.append(" ORDER BY id DESC LIMIT ?");
        args.add(f.limit());
        try (PreparedStatement ps = c.prepareStatement(sql.toString())) {
            for (int i = 0; i < args.size(); i++) ps.setObject(i + 1, args.get(i));
            return list(ps);
        }
    }

    public List<NotificationRow> bySeries(Connection c, UUID seriesId, int limit) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("SELECT * FROM notification WHERE series_id=? ORDER BY id DESC LIMIT ?")) {
            ps.setObject(1, seriesId);
            ps.setInt(2, limit);
            return list(ps);
        }
    }

    private static List<NotificationRow> list(PreparedStatement ps) throws SQLException {
        List<NotificationRow> out = new ArrayList<>();
        try (ResultSet rs = ps.executeQuery()) { while (rs.next()) out.add(map(rs)); }
        return out;
    }

    static NotificationRow map(ResultSet rs) throws SQLException {
        return new NotificationRow(Jdbc.getUuid(rs, "id"), Jdbc.getTs(rs, "created_at"), Jdbc.getTs(rs, "updated_at"),
                rs.getString("tenant_id"), rs.getString("rule_id"), rs.getInt("rule_version"), rs.getString("template_id"),
                rs.getString("correlation_key"), rs.getString("action"), rs.getString("type"),
                NotificationStatus.valueOf(rs.getString("status")), rs.getString("status_reason"), rs.getInt("attempt"),
                rs.getInt("cycle_attempts"), RetryPolicy.fromJson(Jdbc.getJson(rs, "retry_policy")), Jdbc.getJson(rs, "body"),
                Jdbc.getJson(rs, "spec"), Jdbc.getJson(rs, "context"), Jdbc.getTs(rs, "due_at"), Jdbc.getTs(rs, "expires_at"),
                Jdbc.getLong(rs, "attempt_timer_id"), rs.getInt("version"), rs.getString("idempotency_key"),
                Jdbc.getUuid(rs, "parent_id"), rs.getString("parent_trigger"), rs.getInt("depth"), Jdbc.getUuid(rs, "series_id"),
                Jdbc.getInt(rs, "occurrence_index"), Jdbc.getTextArray(rs, "cancel_on"), rs.getBoolean("failed_once"),
                Failure.fromJson(Jdbc.getJson(rs, "last_failure")), Jdbc.getTs(rs, "published_at"), Jdbc.getTs(rs, "terminal_at"));
    }
}
