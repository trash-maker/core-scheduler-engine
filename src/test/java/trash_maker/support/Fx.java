package trash_maker.support;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import trash_maker.common.Json;
import trash_maker.core.Engine;
import trash_maker.core.NotificationRepository.NotificationRow;
import trash_maker.core.NotificationRepository;
import trash_maker.core.SignalIngestService;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import javax.sql.DataSource;

/** Test fixture bean (CoreProfile): the engine on PostgreSQL with a controllable clock and an in-memory broker. */
@Singleton
public class Fx {
    public static final String TENANT = "acme";
    @Inject public Engine e;
    @Inject DataSource ds;
    @Inject Instance<MutableClock> clockInstance;
    @Inject Instance<RecordingPublisher> pubInstance;
    public MutableClock clock;
    public RecordingPublisher pub;
    private final AtomicLong seq = new AtomicLong();

    /** Fresh state for a test: empty tables, clock at {@code start}, empty broker, reloaded rules, partitions. */
    public Fx reset(Instant start) {
        clock = clockInstance.get();
        pub = pubInstance.get();
        truncateAll(ds);
        clock.set(start);
        pub.reset();
        e.timers.setFaultInjector(null);
        e.registry.reloadAll();
        e.maintenance.ensurePartitions(3);
        return this;
    }

    public static void truncateAll(DataSource ds) {
        try (Connection c = ds.getConnection(); Statement st = c.createStatement()) {
            st.execute("""
                    TRUNCATE signal_inbox, signal_state, rule, rule_state, notification, notification_history, notification_dedup,
                      recurring_series, timer, timer_dead, outbox, dead_letter""");
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
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
        return e.db.read(c -> e.notifications.repository().search(c, new NotificationRepository.SearchFilter(TENANT, null, null, null, key,
                null, null, null, null, 1000))).reversed();
    }

    public NotificationRow byTemplate(String key, String template) {
        List<NotificationRow> l = notifications(key).stream().filter(n -> n.templateId().equals(template)).toList();
        if (l.size() != 1) throw new AssertionError("expected 1 notification for template " + template + " but got " + l.size());
        return l.get(0);
    }

    public NotificationRow get(UUID id) { return e.db.read(c -> e.notifications.repository().find(c, TENANT, id).orElseThrow()); }

    public List<String> history(UUID id) {
        return e.db.read(c -> e.notifications.repository().historyOf(c, TENANT, id)).stream().map(h -> h.to()).toList();
    }

    public long count(String sql) {
        return e.db.read(c -> {
            try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery(sql)) { rs.next(); return rs.getLong(1); }
        });
    }

    public JsonNode query(String sql) {
        return e.db.read((Connection c) -> {
            try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery(sql)) {
                var arr = Json.array();
                while (rs.next()) arr.add(rs.getString(1));
                return arr;
            }
        });
    }
}
