package io.notifengine.core.notifications;

import io.notifengine.core.db.Jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public final class SeriesRepository {

    /** Inserts; returns false when the idempotency key already exists. */
    public boolean insert(Connection c, SeriesRow s) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("""
                INSERT INTO recurring_series (id, tenant_id, rule_id, rule_version, template_id, correlation_key, action, status,
                  close_reason, parent_id, spec, context, start_at, until_at, max_occurrences, occurrences, skipped, next_due_at,
                  cancel_on, idempotency_key, version, created_at, updated_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?::jsonb,?::jsonb,?,?,?,?,?,?,?,?,0,?,?) ON CONFLICT (idempotency_key) DO NOTHING""")) {
            int i = 1;
            ps.setObject(i++, s.id());
            ps.setString(i++, s.tenantId());
            ps.setString(i++, s.ruleId());
            ps.setInt(i++, s.ruleVersion());
            ps.setString(i++, s.templateId());
            ps.setString(i++, s.correlationKey());
            ps.setString(i++, s.action());
            ps.setString(i++, s.status());
            ps.setString(i++, s.closeReason());
            ps.setObject(i++, s.parentId());
            Jdbc.setJson(ps, i++, s.spec());
            Jdbc.setJson(ps, i++, s.context());
            Jdbc.setTs(ps, i++, s.startAt());
            Jdbc.setTs(ps, i++, s.untilAt());
            Jdbc.setInt(ps, i++, s.maxOccurrences());
            ps.setInt(i++, s.occurrences());
            ps.setInt(i++, s.skipped());
            Jdbc.setTs(ps, i++, s.nextDueAt());
            ps.setArray(i++, Jdbc.textArray(c, s.cancelOn()));
            ps.setString(i++, s.idempotencyKey());
            Jdbc.setTs(ps, i++, s.createdAt());
            Jdbc.setTs(ps, i, s.updatedAt());
            return ps.executeUpdate() == 1;
        }
    }

    public void update(Connection c, SeriesRow s) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("""
                UPDATE recurring_series SET status=?, close_reason=?, start_at=?, until_at=?, occurrences=?, skipped=?, next_due_at=?,
                  updated_at=?, version = version + 1
                WHERE id=? AND version=?""")) {
            int i = 1;
            ps.setString(i++, s.status());
            ps.setString(i++, s.closeReason());
            Jdbc.setTs(ps, i++, s.startAt());
            Jdbc.setTs(ps, i++, s.untilAt());
            ps.setInt(i++, s.occurrences());
            ps.setInt(i++, s.skipped());
            Jdbc.setTs(ps, i++, s.nextDueAt());
            Jdbc.setTs(ps, i++, s.updatedAt());
            ps.setObject(i++, s.id());
            ps.setInt(i, s.version());
            if (ps.executeUpdate() != 1) throw new IllegalStateException("concurrent update of series " + s.id());
        }
    }

    public Optional<SeriesRow> lock(Connection c, UUID id) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("SELECT * FROM recurring_series WHERE id=? FOR UPDATE")) {
            ps.setObject(1, id);
            try (ResultSet rs = ps.executeQuery()) { return rs.next() ? Optional.of(map(rs)) : Optional.empty(); }
        }
    }

    public Optional<SeriesRow> find(Connection c, String tenant, UUID id) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("SELECT * FROM recurring_series WHERE id=? AND tenant_id=?")) {
            ps.setObject(1, id);
            ps.setString(2, tenant);
            try (ResultSet rs = ps.executeQuery()) { return rs.next() ? Optional.of(map(rs)) : Optional.empty(); }
        }
    }

    /** Latest series per WHILE_IN_STATE template of a parent (non-closed first, then most recent). */
    public List<SeriesRow> lockLatestForParent(Connection c, UUID parentId) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("""
                SELECT DISTINCT ON (template_id) * FROM recurring_series WHERE parent_id=?
                ORDER BY template_id, (status = 'CLOSED'), created_at DESC""")) {
            ps.setObject(1, parentId);
            List<SeriesRow> latest = list(ps);
            List<SeriesRow> out = new ArrayList<>();
            for (SeriesRow s : latest) lock(c, s.id()).ifPresent(out::add);
            return out;
        }
    }

    public List<SeriesRow> lockCancellable(Connection c, String tenant, String key, String signal) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("""
                SELECT * FROM recurring_series WHERE tenant_id=? AND correlation_key=? AND status <> 'CLOSED'
                  AND cardinality(cancel_on) > 0 AND ? = ANY(cancel_on) ORDER BY id FOR UPDATE""")) {
            ps.setString(1, tenant);
            ps.setString(2, key);
            ps.setString(3, signal);
            return list(ps);
        }
    }

    public List<SeriesRow> openForParent(Connection c, UUID parentId) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("SELECT * FROM recurring_series WHERE parent_id=? AND status <> 'CLOSED' ORDER BY id FOR UPDATE")) {
            ps.setObject(1, parentId);
            return list(ps);
        }
    }

    private static List<SeriesRow> list(PreparedStatement ps) throws SQLException {
        List<SeriesRow> out = new ArrayList<>();
        try (ResultSet rs = ps.executeQuery()) { while (rs.next()) out.add(map(rs)); }
        return out;
    }

    static SeriesRow map(ResultSet rs) throws SQLException {
        return new SeriesRow(Jdbc.getUuid(rs, "id"), rs.getString("tenant_id"), rs.getString("rule_id"), rs.getInt("rule_version"),
                rs.getString("template_id"), rs.getString("correlation_key"), rs.getString("action"), rs.getString("status"),
                rs.getString("close_reason"), Jdbc.getUuid(rs, "parent_id"), Jdbc.getJson(rs, "spec"), Jdbc.getJson(rs, "context"),
                Jdbc.getTs(rs, "start_at"), Jdbc.getTs(rs, "until_at"), Jdbc.getInt(rs, "max_occurrences"), rs.getInt("occurrences"),
                rs.getInt("skipped"), Jdbc.getTs(rs, "next_due_at"), Jdbc.getTextArray(rs, "cancel_on"), rs.getString("idempotency_key"),
                rs.getInt("version"), Jdbc.getTs(rs, "created_at"), Jdbc.getTs(rs, "updated_at"));
    }

    public static SeriesRow with(SeriesRow s, String status, String closeReason, Instant startAt, Instant untilAt, int occurrences,
                                 int skipped, Instant nextDue, Instant now) {
        return new SeriesRow(s.id(), s.tenantId(), s.ruleId(), s.ruleVersion(), s.templateId(), s.correlationKey(), s.action(), status,
                closeReason, s.parentId(), s.spec(), s.context(), startAt, untilAt, s.maxOccurrences(), occurrences, skipped, nextDue,
                s.cancelOn(), s.idempotencyKey(), s.version(), s.createdAt(), now);
    }
}
