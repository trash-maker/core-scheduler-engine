package io.notifengine.support;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.notifengine.common.Json;
import io.notifengine.core.Engine;
import io.notifengine.core.EngineConfig;
import io.notifengine.core.notifications.NotificationRepository;
import io.notifengine.core.notifications.NotificationRow;
import io.notifengine.core.ports.EngineMetrics;
import io.notifengine.core.ports.TimerWakeup;
import io.notifengine.core.ports.TraceContext;
import io.notifengine.core.signals.SignalIngestService;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/** Test fixture: core engine on a real PostgreSQL with a controllable clock and an in-memory broker. */
public final class Fx {
    public static final String TENANT = "acme";
    public final MutableClock clock;
    public final RecordingPublisher pub = new RecordingPublisher();
    public final Engine e;
    private final AtomicLong seq = new AtomicLong();

    public Fx(Instant start) { this(start, EngineConfig.defaults()); }

    public Fx(Instant start, EngineConfig cfg) {
        Pg.truncateAll();
        clock = new MutableClock(start);
        e = new Engine(Pg.dataSource(), clock, cfg, pub, EngineMetrics.NOOP, TraceContext.NONE, TimerWakeup.NONE);
        e.maintenance.ensurePartitions(3);
    }

    public Instant now() { return clock.instant(); }

    public void advance(Duration d) { clock.advance(d); }

    public void rule(String json) { e.rules.create(TENANT, Json.parse(json), "test"); }

    public SignalIngestService.Stats signal(String key, String name, Object value) {
        return signal("s-" + seq.incrementAndGet(), key, name, "KPI", value, now());
    }

    public SignalIngestService.Stats date(String key, String name, String iso) {
        return signal("s-" + seq.incrementAndGet(), key, name, "DATE", iso, now());
    }

    public SignalIngestService.Stats signal(String id, String key, String name, String kind, Object value, Instant occurred) {
        return e.ingest.ingest(List.of(envelope(id, key, name, kind, value, occurred)));
    }

    public static String envelope(String id, String key, String name, String kind, Object value, Instant occurred) {
        ObjectNode o = Json.object();
        o.put("signal_id", id);
        o.put("tenant_id", TENANT);
        o.put("correlation_key", key);
        o.put("signal", name);
        o.put("kind", kind);
        o.set("value", Json.toTree(value));
        o.put("occurred_at", occurred.toString());
        return Json.write(o);
    }

    /** Runs timer batches until nothing is due at the current clock. */
    public int runTimers() {
        int total = 0;
        for (int i = 0; i < 1000; i++) {
            int n = e.timers.runOnce();
            total += n;
            if (n == 0) return total;
        }
        throw new IllegalStateException("timers did not settle");
    }

    /** Relays the outbox; returns messages published by this call. */
    public List<RecordingPublisher.Msg> relay() {
        int before = pub.messages.size();
        while (e.relay.relayOnce().published() > 0) { }
        return new ArrayList<>(pub.messages.subList(before, pub.messages.size()));
    }

    public List<RecordingPublisher.Msg> out() { return pub.on("notifications.out"); }

    public void result(UUID id, int attempt, String action, String outcome, String reason) {
        ObjectNode o = Json.object();
        o.put("notification_id", id.toString());
        o.put("attempt", attempt);
        o.put("action", action);
        o.put("outcome", outcome);
        if (reason != null) o.put("reason", reason);
        o.put("occurred_at", now().toString());
        e.results.process(List.of(Json.write(o)));
    }

    public List<NotificationRow> notifications(String key) {
        return e.tx.read(c -> e.notifications.repository().search(c, new NotificationRepository.SearchFilter(TENANT, null, null, null, key,
                null, null, null, null, 1000))).reversed();
    }

    public NotificationRow byTemplate(String key, String template) {
        List<NotificationRow> l = notifications(key).stream().filter(n -> n.templateId().equals(template)).toList();
        if (l.size() != 1) throw new AssertionError("expected 1 notification for template " + template + " but got " + l.size());
        return l.get(0);
    }

    public NotificationRow get(UUID id) { return e.tx.read(c -> e.notifications.repository().find(c, TENANT, id).orElseThrow()); }

    public List<String> history(UUID id) {
        return e.tx.read(c -> e.notifications.repository().historyOf(c, TENANT, id)).stream().map(h -> h.to()).toList();
    }

    public long count(String sql) {
        return e.tx.read(c -> {
            try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery(sql)) { rs.next(); return rs.getLong(1); }
        });
    }

    public JsonNode query(String sql) {
        return e.tx.read((Connection c) -> {
            try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery(sql)) {
                var arr = Json.array();
                while (rs.next()) arr.add(rs.getString(1));
                return arr;
            }
        });
    }
}
