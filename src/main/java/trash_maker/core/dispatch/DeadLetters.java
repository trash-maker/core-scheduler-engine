package trash_maker.core.dispatch;

import jakarta.inject.Singleton;
import jakarta.inject.Inject;

import com.fasterxml.jackson.databind.node.ObjectNode;
import trash_maker.common.Json;
import trash_maker.core.EngineConfig;
import trash_maker.core.db.Jdbc;
import trash_maker.core.ports.EngineMetrics;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Map;

/** Records a dead letter (table) and publishes it to the DLQ topic through the outbox, in the caller's transaction. */
@Singleton
public class DeadLetters {
    private final OutboxRepository outbox;
    private final EngineConfig cfg;
    private final EngineMetrics metrics;

    @Inject
    public DeadLetters(OutboxRepository outbox, EngineConfig cfg, EngineMetrics metrics) {
        this.outbox = outbox;
        this.cfg = cfg;
        this.metrics = metrics;
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
        outbox.insert(c, cfg.dlqTopic(), tenant == null ? "unknown" : tenant, Map.of("reason", code, "source", source),
                Json.write(m), tenant == null ? "unknown" : tenant, null, null, null, now);
        metrics.count("dlq.events", "source", source, "reason", code);
    }
}
