package trash_maker.core.signals;

import jakarta.inject.Singleton;

import trash_maker.core.db.Jdbc;
import trash_maker.domain.signal.SignalEnvelope;
import trash_maker.domain.signal.SignalKind;
import trash_maker.domain.signal.SignalValue;
import trash_maker.domain.signal.Value;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** signal_inbox (idempotency) and signal_state (latest value per tenant/key/signal), set-based SQL. */
@Singleton
public class SignalStore {

    public record EntityKey(String tenantId, String correlationKey) implements Comparable<EntityKey> {
        public int compareTo(EntityKey o) {
            int c = tenantId.compareTo(o.tenantId);
            return c != 0 ? c : correlationKey.compareTo(o.correlationKey);
        }
    }

    /** Returns "tenant|signalId" of the signals not seen before. */
    public Set<String> insertInbox(Connection c, List<SignalEnvelope> signals, Instant now) throws SQLException {
        Set<String> fresh = new HashSet<>();
        if (signals.isEmpty()) return fresh;
        try (PreparedStatement ps = c.prepareStatement("""
                INSERT INTO signal_inbox (tenant_id, signal_id, received_at)
                SELECT t, s, ? FROM unnest(?::text[], ?::text[]) AS x(t, s)
                ON CONFLICT DO NOTHING RETURNING tenant_id, signal_id""")) {
            Jdbc.setTs(ps, 1, now);
            ps.setArray(2, Jdbc.textArray(c, signals.stream().map(SignalEnvelope::tenantId).toList()));
            ps.setArray(3, Jdbc.textArray(c, signals.stream().map(SignalEnvelope::signalId).toList()));
            try (ResultSet rs = ps.executeQuery()) { while (rs.next()) fresh.add(rs.getString(1) + "|" + rs.getString(2)); }
        }
        return fresh;
    }

    /**
     * Upserts latest values; a stored value changes only if occurred_at is newer (out-of-order safe).
     * Input must contain at most one row per (tenant, key, signal). Returns the keys actually changed.
     */
    public Set<String> upsert(Connection c, List<SignalEnvelope> reduced, Instant now) throws SQLException {
        Set<String> changed = new HashSet<>();
        if (reduced.isEmpty()) return changed;
        try (PreparedStatement ps = c.prepareStatement("""
                INSERT INTO signal_state (tenant_id, correlation_key, signal, kind, value, occurred_at, received_at, signal_id)
                SELECT t, k, s, kd, v::jsonb, o, ?, sid FROM unnest(?::text[], ?::text[], ?::text[], ?::text[], ?::text[], ?::timestamptz[], ?::text[])
                  AS x(t, k, s, kd, v, o, sid)
                ON CONFLICT (tenant_id, correlation_key, signal) DO UPDATE
                  SET kind = excluded.kind, value = excluded.value, occurred_at = excluded.occurred_at,
                      received_at = excluded.received_at, signal_id = excluded.signal_id
                  WHERE excluded.occurred_at > signal_state.occurred_at
                RETURNING tenant_id, correlation_key, signal""")) {
            Jdbc.setTs(ps, 1, now);
            ps.setArray(2, Jdbc.textArray(c, reduced.stream().map(SignalEnvelope::tenantId).toList()));
            ps.setArray(3, Jdbc.textArray(c, reduced.stream().map(SignalEnvelope::correlationKey).toList()));
            ps.setArray(4, Jdbc.textArray(c, reduced.stream().map(SignalEnvelope::signal).toList()));
            ps.setArray(5, Jdbc.textArray(c, reduced.stream().map(s -> s.kind().name()).toList()));
            ps.setArray(6, Jdbc.textArray(c, reduced.stream().map(s -> s.value().toJson().toString()).toList()));
            ps.setArray(7, Jdbc.tsArray(c, reduced.stream().map(SignalEnvelope::occurredAt).toList()));
            ps.setArray(8, Jdbc.textArray(c, reduced.stream().map(SignalEnvelope::signalId).toList()));
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) changed.add(rs.getString(1) + "|" + rs.getString(2) + "|" + rs.getString(3));
            }
        }
        return changed;
    }

    /** All stored signals of the given entities in one query. */
    public Map<EntityKey, Map<String, SignalValue>> load(Connection c, Collection<EntityKey> entities) throws SQLException {
        Map<EntityKey, Map<String, SignalValue>> out = new HashMap<>();
        if (entities.isEmpty()) return out;
        List<EntityKey> list = new ArrayList<>(entities);
        try (PreparedStatement ps = c.prepareStatement("""
                SELECT s.* FROM signal_state s
                JOIN unnest(?::text[], ?::text[]) AS e(t, k) ON s.tenant_id = e.t AND s.correlation_key = e.k""")) {
            ps.setArray(1, Jdbc.textArray(c, list.stream().map(EntityKey::tenantId).toList()));
            ps.setArray(2, Jdbc.textArray(c, list.stream().map(EntityKey::correlationKey).toList()));
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    EntityKey ek = new EntityKey(rs.getString("tenant_id"), rs.getString("correlation_key"));
                    SignalKind kind = SignalKind.valueOf(rs.getString("kind"));
                    String name = rs.getString("signal");
                    SignalValue sv = new SignalValue(name, kind, Value.fromJson(kind, Jdbc.getJson(rs, "value")), Jdbc.getTs(rs, "occurred_at"));
                    out.computeIfAbsent(ek, k -> new HashMap<>()).put(name, sv);
                }
            }
        }
        for (EntityKey e : entities) out.computeIfAbsent(e, k -> new HashMap<>());
        return out;
    }

    public record StoredSignal(String signal, String kind, String value, Instant occurredAt, Instant receivedAt, String signalId) {}

    public List<StoredSignal> raw(Connection c, String tenant, String key) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("SELECT * FROM signal_state WHERE tenant_id=? AND correlation_key=? ORDER BY signal")) {
            ps.setString(1, tenant);
            ps.setString(2, key);
            List<StoredSignal> out = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) out.add(new StoredSignal(rs.getString("signal"), rs.getString("kind"), rs.getString("value"),
                        Jdbc.getTs(rs, "occurred_at"), Jdbc.getTs(rs, "received_at"), rs.getString("signal_id")));
            }
            return out;
        }
    }

    /** Keyset page of entity keys having any of the given signals (sweeps for time-based rules). */
    public List<String> entitiesPage(Connection c, String tenant, Set<String> signals, String after, int limit) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("""
                SELECT DISTINCT correlation_key FROM signal_state
                WHERE tenant_id = ? AND signal = ANY(?) AND correlation_key > ?
                ORDER BY correlation_key LIMIT ?""")) {
            ps.setString(1, tenant);
            ps.setArray(2, Jdbc.textArray(c, signals));
            ps.setString(3, after == null ? "" : after);
            ps.setInt(4, limit);
            List<String> out = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) { while (rs.next()) out.add(rs.getString(1)); }
            return out;
        }
    }
}
