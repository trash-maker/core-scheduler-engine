package trash_maker.core;

import com.fasterxml.jackson.databind.JsonNode;
import trash_maker.common.Json;

import java.security.SecureRandom;
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

    /** RFC 9562 UUIDv7. The embedded millisecond timestamp is used as the partition key (created_at). */
    public static final class Uuid7 {
        private static final SecureRandom RANDOM = new SecureRandom();

        private Uuid7() {}

        public static UUID generate(Instant at) {
            long ms = at.toEpochMilli();
            long randA = RANDOM.nextInt(1 << 12);
            long msb = (ms << 16) | 0x7000L | randA;
            long lsb = (RANDOM.nextLong() & 0x3FFFFFFFFFFFFFFFL) | 0x8000000000000000L;
            return new UUID(msb, lsb);
        }

        /** Creation instant encoded in the id (millisecond precision). */
        public static Instant timestamp(UUID id) {
            return Instant.ofEpochMilli(id.getMostSignificantBits() >>> 16);
        }

        /** Truncate to the precision stored in the id so that created_at == timestamp(id). */
        public static Instant truncate(Instant at) {
            return Instant.ofEpochMilli(at.toEpochMilli());
        }
    }
}
