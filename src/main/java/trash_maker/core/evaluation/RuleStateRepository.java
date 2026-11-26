package trash_maker.core.evaluation;

import jakarta.inject.Singleton;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import trash_maker.common.Json;
import trash_maker.core.db.Jdbc;
import trash_maker.domain.expr.LeafMemory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** rule_state: last result + edge_seq per (tenant, rule, correlation_key). Row locks serialize evaluation per entity. */
@Singleton
public class RuleStateRepository {

    public record Key(String tenantId, String ruleId, String correlationKey) implements Comparable<Key> {
        public int compareTo(Key o) {
            int c = tenantId.compareTo(o.tenantId);
            if (c != 0) return c;
            c = ruleId.compareTo(o.ruleId);
            return c != 0 ? c : correlationKey.compareTo(o.correlationKey);
        }
    }

    public record State(Key key, boolean lastResult, long edgeSeq, Instant lastFiredAt, Instant lastEvaluatedAt,
                        Integer ruleVersion, Map<String, LeafMemory> memory) {}

    /** Ensures rows exist and locks them in key order (deadlock avoidance). */
    public Map<Key, State> lockAll(Connection c, List<Key> keys) throws SQLException {
        Map<Key, State> out = new HashMap<>();
        if (keys.isEmpty()) return out;
        List<Key> sorted = keys.stream().distinct().sorted().toList();
        String[] t = sorted.stream().map(Key::tenantId).toArray(String[]::new);
        String[] r = sorted.stream().map(Key::ruleId).toArray(String[]::new);
        String[] k = sorted.stream().map(Key::correlationKey).toArray(String[]::new);
        try (PreparedStatement ps = c.prepareStatement("""
                INSERT INTO rule_state (tenant_id, rule_id, correlation_key)
                SELECT * FROM unnest(?::text[], ?::text[], ?::text[]) ON CONFLICT DO NOTHING""")) {
            ps.setArray(1, c.createArrayOf("text", t));
            ps.setArray(2, c.createArrayOf("text", r));
            ps.setArray(3, c.createArrayOf("text", k));
            ps.executeUpdate();
        }
        try (PreparedStatement ps = c.prepareStatement("""
                SELECT s.* FROM rule_state s JOIN unnest(?::text[], ?::text[], ?::text[]) AS x(t, r, k)
                  ON s.tenant_id = x.t AND s.rule_id = x.r AND s.correlation_key = x.k
                ORDER BY s.tenant_id, s.rule_id, s.correlation_key FOR UPDATE OF s""")) {
            ps.setArray(1, c.createArrayOf("text", t));
            ps.setArray(2, c.createArrayOf("text", r));
            ps.setArray(3, c.createArrayOf("text", k));
            try (ResultSet rs = ps.executeQuery()) { while (rs.next()) { State s = map(rs); out.put(s.key(), s); } }
        }
        return out;
    }

    public void updateAll(Connection c, List<State> states) throws SQLException {
        if (states.isEmpty()) return;
        try (PreparedStatement ps = c.prepareStatement("""
                UPDATE rule_state SET last_result=?, edge_seq=?, last_fired_at=?, last_evaluated_at=?, rule_version=?, leaf_state=?::jsonb
                WHERE tenant_id=? AND rule_id=? AND correlation_key=?""")) {
            for (State s : states) {
                ps.setBoolean(1, s.lastResult());
                ps.setLong(2, s.edgeSeq());
                Jdbc.setTs(ps, 3, s.lastFiredAt());
                Jdbc.setTs(ps, 4, s.lastEvaluatedAt());
                Jdbc.setInt(ps, 5, s.ruleVersion());
                Jdbc.setJson(ps, 6, memoryJson(s.memory()));
                ps.setString(7, s.key().tenantId());
                ps.setString(8, s.key().ruleId());
                ps.setString(9, s.key().correlationKey());
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    public Optional<State> find(Connection c, Key key) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("SELECT * FROM rule_state WHERE tenant_id=? AND rule_id=? AND correlation_key=?")) {
            ps.setString(1, key.tenantId());
            ps.setString(2, key.ruleId());
            ps.setString(3, key.correlationKey());
            try (ResultSet rs = ps.executeQuery()) { return rs.next() ? Optional.of(map(rs)) : Optional.empty(); }
        }
    }

    public int delete(Connection c, Key key) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("DELETE FROM rule_state WHERE tenant_id=? AND rule_id=? AND correlation_key=?")) {
            ps.setString(1, key.tenantId());
            ps.setString(2, key.ruleId());
            ps.setString(3, key.correlationKey());
            return ps.executeUpdate();
        }
    }

    public List<State> forEntity(Connection c, String tenant, String correlationKey) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("SELECT * FROM rule_state WHERE tenant_id=? AND correlation_key=? ORDER BY rule_id LIMIT 500")) {
            ps.setString(1, tenant);
            ps.setString(2, correlationKey);
            List<State> out = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) { while (rs.next()) out.add(map(rs)); }
            return out;
        }
    }

    static State map(ResultSet rs) throws SQLException {
        Map<String, LeafMemory> mem = new HashMap<>();
        JsonNode m = Jdbc.getJson(rs, "leaf_state");
        if (m != null) m.fields().forEachRemaining(e -> mem.put(e.getKey(), new LeafMemory(
                e.getValue().hasNonNull("trueSince") ? Instant.parse(e.getValue().get("trueSince").asText()) : null,
                e.getValue().hasNonNull("lastTrueAt") ? Instant.parse(e.getValue().get("lastTrueAt").asText()) : null,
                e.getValue().path("rawTrue").asBoolean(false))));
        return new State(new Key(rs.getString("tenant_id"), rs.getString("rule_id"), rs.getString("correlation_key")),
                rs.getBoolean("last_result"), rs.getLong("edge_seq"), Jdbc.getTs(rs, "last_fired_at"), Jdbc.getTs(rs, "last_evaluated_at"),
                Jdbc.getInt(rs, "rule_version"), mem);
    }

    public static ObjectNode memoryJson(Map<String, LeafMemory> mem) {
        ObjectNode o = Json.object();
        mem.forEach((k, v) -> {
            ObjectNode x = o.putObject(k);
            if (v.trueSince() != null) x.put("trueSince", v.trueSince().toString());
            if (v.lastTrueAt() != null) x.put("lastTrueAt", v.lastTrueAt().toString());
            x.put("rawTrue", v.rawTrue());
        });
        return o;
    }
}
