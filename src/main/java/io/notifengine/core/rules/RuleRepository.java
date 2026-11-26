package io.notifengine.core.rules;

import io.notifengine.core.db.Jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public final class RuleRepository {

    private static RuleRecord map(ResultSet rs) throws SQLException {
        return new RuleRecord(rs.getString("tenant_id"), rs.getString("rule_id"), rs.getInt("version"), rs.getBoolean("enabled"),
                rs.getBoolean("current"), Jdbc.getJson(rs, "definition"), rs.getLong("change_seq"), Jdbc.getTs(rs, "created_at"),
                rs.getString("created_by"));
    }

    public Optional<RuleRecord> current(Connection c, String tenant, String ruleId, boolean forUpdate) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("SELECT * FROM rule WHERE tenant_id=? AND rule_id=? AND current" + (forUpdate ? " FOR UPDATE" : ""))) {
            ps.setString(1, tenant);
            ps.setString(2, ruleId);
            try (ResultSet rs = ps.executeQuery()) { return rs.next() ? Optional.of(map(rs)) : Optional.empty(); }
        }
    }

    public Optional<RuleRecord> version(Connection c, String tenant, String ruleId, int version) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("SELECT * FROM rule WHERE tenant_id=? AND rule_id=? AND version=?")) {
            ps.setString(1, tenant);
            ps.setString(2, ruleId);
            ps.setInt(3, version);
            try (ResultSet rs = ps.executeQuery()) { return rs.next() ? Optional.of(map(rs)) : Optional.empty(); }
        }
    }

    public List<RuleRecord> versions(Connection c, String tenant, String ruleId) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("SELECT * FROM rule WHERE tenant_id=? AND rule_id=? ORDER BY version DESC LIMIT 200")) {
            ps.setString(1, tenant);
            ps.setString(2, ruleId);
            return list(ps);
        }
    }

    /** Keyset pagination over current rules of a tenant. */
    public List<RuleRecord> listCurrent(Connection c, String tenant, String afterRuleId, int limit) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("SELECT * FROM rule WHERE tenant_id=? AND current AND rule_id > ? ORDER BY rule_id LIMIT ?")) {
            ps.setString(1, tenant);
            ps.setString(2, afterRuleId == null ? "" : afterRuleId);
            ps.setInt(3, limit);
            return list(ps);
        }
    }

    public void insert(Connection c, RuleRecord r) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("""
                INSERT INTO rule (tenant_id, rule_id, version, enabled, current, definition, created_at, created_by)
                VALUES (?, ?, ?, ?, true, ?::jsonb, ?, ?)""")) {
            ps.setString(1, r.tenantId());
            ps.setString(2, r.ruleId());
            ps.setInt(3, r.version());
            ps.setBoolean(4, r.enabled());
            Jdbc.setJson(ps, 5, r.definition());
            Jdbc.setTs(ps, 6, r.createdAt());
            ps.setString(7, r.createdBy());
            ps.executeUpdate();
        }
    }

    public void markNotCurrent(Connection c, String tenant, String ruleId, int version) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(
                "UPDATE rule SET current=false, change_seq=nextval('rule_change_seq') WHERE tenant_id=? AND rule_id=? AND version=?")) {
            ps.setString(1, tenant);
            ps.setString(2, ruleId);
            ps.setInt(3, version);
            ps.executeUpdate();
        }
    }

    public int setEnabled(Connection c, String tenant, String ruleId, boolean enabled) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(
                "UPDATE rule SET enabled=?, change_seq=nextval('rule_change_seq'), definition = jsonb_set(definition, '{enabled}', to_jsonb(?::boolean)) WHERE tenant_id=? AND rule_id=? AND current")) {
            ps.setBoolean(1, enabled);
            ps.setBoolean(2, enabled);
            ps.setString(3, tenant);
            ps.setString(4, ruleId);
            return ps.executeUpdate();
        }
    }

    public long maxChangeSeq(Connection c) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("SELECT coalesce(max(change_seq), 0) FROM rule");
             ResultSet rs = ps.executeQuery()) {
            rs.next();
            return rs.getLong(1);
        }
    }

    /** Rule rows (current or superseded) changed after {@code seq}. */
    public List<RuleRecord> changedSince(Connection c, long seq) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("SELECT * FROM rule WHERE change_seq > ? ORDER BY change_seq")) {
            ps.setLong(1, seq);
            return list(ps);
        }
    }

    private static List<RuleRecord> list(PreparedStatement ps) throws SQLException {
        List<RuleRecord> out = new ArrayList<>();
        try (ResultSet rs = ps.executeQuery()) { while (rs.next()) out.add(map(rs)); }
        return out;
    }

    public static RuleRecord newRecord(String tenant, String id, int version, boolean enabled, com.fasterxml.jackson.databind.JsonNode def, Instant now, String by) {
        return new RuleRecord(tenant, id, version, enabled, true, def, 0, now, by);
    }
}
