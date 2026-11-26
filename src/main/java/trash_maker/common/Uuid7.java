package trash_maker.common;

import java.security.SecureRandom;
import java.time.Instant;
import java.util.UUID;

/** RFC 9562 UUIDv7. The embedded millisecond timestamp is used as the partition key (created_at). */
public final class Uuid7 {
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
