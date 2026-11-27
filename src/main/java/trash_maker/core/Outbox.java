package trash_maker.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import trash_maker.common.Json;
import trash_maker.core.NotificationRepository.NotificationRow;
import trash_maker.core.Ports.EngineMetrics;
import trash_maker.domain.notification.Lifecycle.NotificationMessage;

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

/**
 * Transactional outbox: rows written in the caller's transaction and deleted by the relay after the Kafka ack.
 * Also records dead letters (table + DLQ topic) and maps notifications to the output message contract.
 */
@Singleton
public class Outbox {
    private final EngineConfig cfg;
    private final EngineMetrics metrics;

    @Inject
    public Outbox(EngineConfig cfg, EngineMetrics metrics) {
        this.cfg = cfg;
        this.metrics = metrics;
    }

    /** The notifications.out contract for one attempt of a stored notification. */
    public static NotificationMessage message(NotificationRow n, int attempt, Instant now) {
        return new NotificationMessage(n.id(), attempt, n.idempotencyKey(), n.action(), n.tenantId(), n.correlationKey(), n.ruleId(),
                n.ruleVersion(), n.templateId(), n.seriesId(), n.occurrenceIndex(), n.dueAt(), now, n.body());
    }

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
    /**
     * @param source  e.g. signals.in, notification.results, rule-firing, child-activation
     * @param code    bounded reason code (INVALID_MESSAGE, BODY_RESOLUTION_FAILED, ...)
     * @param detail  human readable detail (no resolved body values)
     * @param payload original message, if any
     */
    public void record(Connection c, String source, String tenant, String code, String detail, String payload, Instant now) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(
                "INSERT INTO dead_letter (source, tenant_id, reason, detail, payload, created_at) VALUES (?,?,?,?,?,?)")) {
            ps.setString(1, source);
            ps.setString(2, tenant);
            ps.setString(3, code);
            ps.setString(4, detail == null ? null : detail.length() > 4000 ? detail.substring(0, 4000) : detail);
            ps.setString(5, payload);
            Jdbc.setTs(ps, 6, now);
            ps.executeUpdate();
        }
        ObjectNode m = Json.object();
        m.put("source", source);
        if (tenant != null) m.put("tenant_id", tenant);
        m.put("reason", code);
        if (detail != null) m.put("detail", detail);
        if (payload != null) m.put("payload", payload);
        m.put("at", now.toString());
        insert(c, cfg.dlqTopic(), tenant == null ? "unknown" : tenant, Map.of("reason", code, "source", source),
                Json.write(m), tenant == null ? "unknown" : tenant, null, null, null, now);
        metrics.count("dlq.events", "source", source, "reason", code);
    }
}
