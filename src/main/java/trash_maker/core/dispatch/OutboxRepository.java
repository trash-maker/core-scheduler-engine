package trash_maker.core.dispatch;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import trash_maker.common.Json;
import trash_maker.core.db.Jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public final class OutboxRepository {

    public record OutboxRow(long id, String topic, String key, Map<String, String> headers, String payload, String tenantId,
                            UUID notificationId, Integer attempt, Long attemptTimeoutMs, Instant createdAt) {}

    public void insert(Connection c, String topic, String key, Map<String, String> headers, String payload, String tenant,
                       UUID notificationId, Integer attempt, Long timeoutMs, Instant now) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("""
                INSERT INTO outbox (topic, kafka_key, headers, payload, tenant_id, notification_id, attempt, attempt_timeout_ms, created_at)
                VALUES (?,?,?::jsonb,?,?,?,?,?,?)""")) {
            ObjectNode h = Json.object();
            headers.forEach(h::put);
            ps.setString(1, topic);
            ps.setString(2, key);
            Jdbc.setJson(ps, 3, h);
            ps.setString(4, payload);
            ps.setString(5, tenant);
            ps.setObject(6, notificationId);
            Jdbc.setInt(ps, 7, attempt);
            Jdbc.setLong(ps, 8, timeoutMs);
            Jdbc.setTs(ps, 9, now);
            ps.executeUpdate();
        }
    }

    public record NewMessage(String topic, String key, Map<String, String> headers, String payload, String tenant, UUID notificationId,
                             Integer attempt, Long timeoutMs) {}

    public void insertAll(Connection c, List<NewMessage> msgs, Instant now) throws SQLException {
        if (msgs.isEmpty()) return;
        try (PreparedStatement ps = c.prepareStatement("""
                INSERT INTO outbox (topic, kafka_key, headers, payload, tenant_id, notification_id, attempt, attempt_timeout_ms, created_at)
                VALUES (?,?,?::jsonb,?,?,?,?,?,?)""")) {
            for (NewMessage m : msgs) {
                ObjectNode h = Json.object();
                m.headers().forEach(h::put);
                ps.setString(1, m.topic());
                ps.setString(2, m.key());
                Jdbc.setJson(ps, 3, h);
                ps.setString(4, m.payload());
                ps.setString(5, m.tenant());
                ps.setObject(6, m.notificationId());
                Jdbc.setInt(ps, 7, m.attempt());
                Jdbc.setLong(ps, 8, m.timeoutMs());
                Jdbc.setTs(ps, 9, now);
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    public List<OutboxRow> claim(Connection c, int limit) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("SELECT * FROM outbox ORDER BY id LIMIT ? FOR UPDATE SKIP LOCKED")) {
            ps.setInt(1, limit);
            List<OutboxRow> out = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    JsonNode h = Jdbc.getJson(rs, "headers");
                    Map<String, String> headers = new LinkedHashMap<>();
                    h.fields().forEachRemaining(e -> headers.put(e.getKey(), e.getValue().asText()));
                    out.add(new OutboxRow(rs.getLong("id"), rs.getString("topic"), rs.getString("kafka_key"), headers,
                            rs.getString("payload"), rs.getString("tenant_id"), Jdbc.getUuid(rs, "notification_id"),
                            Jdbc.getInt(rs, "attempt"), Jdbc.getLong(rs, "attempt_timeout_ms"), Jdbc.getTs(rs, "created_at")));
                }
            }
            return out;
        }
    }

    public void delete(Connection c, List<Long> ids) throws SQLException {
        if (ids.isEmpty()) return;
        try (PreparedStatement ps = c.prepareStatement("DELETE FROM outbox WHERE id = ANY(?)")) {
            ps.setArray(1, c.createArrayOf("bigint", ids.toArray()));
            ps.executeUpdate();
        }
    }
}
