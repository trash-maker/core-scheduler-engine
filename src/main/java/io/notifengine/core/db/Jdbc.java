package io.notifengine.core.db;

import com.fasterxml.jackson.databind.JsonNode;
import io.notifengine.common.Json;

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

/** Small JDBC helpers (no ORM). */
public final class Jdbc {
    private Jdbc() {}

    public static OffsetDateTime ts(Instant i) { return i == null ? null : OffsetDateTime.ofInstant(i, ZoneOffset.UTC); }

    public static void setTs(PreparedStatement ps, int idx, Instant i) throws SQLException {
        if (i == null) ps.setNull(idx, Types.TIMESTAMP_WITH_TIMEZONE); else ps.setObject(idx, ts(i));
    }

    public static Instant getTs(ResultSet rs, String col) throws SQLException {
        OffsetDateTime o = rs.getObject(col, OffsetDateTime.class);
        return o == null ? null : o.toInstant();
    }

    public static void setJson(PreparedStatement ps, int idx, JsonNode n) throws SQLException {
        if (n == null) ps.setNull(idx, Types.OTHER); else ps.setString(idx, Json.write(n));
    }

    public static JsonNode getJson(ResultSet rs, String col) throws SQLException {
        String s = rs.getString(col);
        return s == null ? null : Json.parse(s);
    }

    public static void setLong(PreparedStatement ps, int idx, Long v) throws SQLException {
        if (v == null) ps.setNull(idx, Types.BIGINT); else ps.setLong(idx, v);
    }

    public static void setInt(PreparedStatement ps, int idx, Integer v) throws SQLException {
        if (v == null) ps.setNull(idx, Types.INTEGER); else ps.setInt(idx, v);
    }

    public static Long getLong(ResultSet rs, String col) throws SQLException {
        long v = rs.getLong(col);
        return rs.wasNull() ? null : v;
    }

    public static Integer getInt(ResultSet rs, String col) throws SQLException {
        int v = rs.getInt(col);
        return rs.wasNull() ? null : v;
    }

    public static UUID getUuid(ResultSet rs, String col) throws SQLException { return rs.getObject(col, UUID.class); }

    public static Array textArray(Connection c, Collection<String> v) throws SQLException { return c.createArrayOf("text", v.toArray()); }

    public static Array uuidArray(Connection c, Collection<UUID> v) throws SQLException { return c.createArrayOf("uuid", v.toArray()); }

    public static Array tsArray(Connection c, Collection<Instant> v) throws SQLException {
        return c.createArrayOf("timestamptz", v.stream().map(Jdbc::ts).toArray());
    }

    public static List<String> getTextArray(ResultSet rs, String col) throws SQLException {
        Array a = rs.getArray(col);
        if (a == null) return List.of();
        List<String> out = new ArrayList<>();
        for (Object o : (Object[]) a.getArray()) out.add((String) o);
        return out;
    }
}
