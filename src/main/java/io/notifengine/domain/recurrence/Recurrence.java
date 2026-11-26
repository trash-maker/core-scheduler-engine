package io.notifengine.domain.recurrence;

import java.time.ZonedDateTime;

/**
 * A recurrence evaluated in a time zone. Implementations are immutable and thread-safe.
 * DST policy: a local time inside a spring-forward gap fires at the first instant after the gap;
 * a local time repeated by a fall-back overlap fires once (earlier offset).
 */
public interface Recurrence {
    /** First occurrence strictly after {@code after}, or null if none within the search horizon. */
    ZonedDateTime next(ZonedDateTime after);

    /** Latest occurrence at or before {@code at}, or null if none. */
    ZonedDateTime previousOrSame(ZonedDateTime at);

    /** First occurrence at or after {@code from}. */
    default ZonedDateTime nextOrSame(ZonedDateTime from) {
        ZonedDateTime p = previousOrSame(from);
        if (p != null && p.toInstant().equals(from.toInstant())) return p;
        return next(from);
    }
}
