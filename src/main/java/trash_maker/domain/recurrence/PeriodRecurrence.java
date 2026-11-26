package trash_maker.domain.recurrence;

import trash_maker.common.IsoAmount;

import java.time.ZonedDateTime;

/**
 * Occurrences anchor + n * every (n >= 0), computed from the anchor each time (not cumulatively),
 * so month-end anchors clamp per month (Jan 31, Feb 29, Mar 31 ...). Calendar parts use local time.
 */
public final class PeriodRecurrence implements Recurrence {
    private final ZonedDateTime anchor;
    private final IsoAmount every;

    public PeriodRecurrence(ZonedDateTime anchor, IsoAmount every) {
        if (!every.isPositive()) throw new IllegalArgumentException("recurrence period must be positive");
        this.anchor = anchor;
        this.every = every;
    }

    public ZonedDateTime occurrence(long n) { return every.addTo(anchor, n); }

    /** Largest n with occurrence(n) <= t, or -1. */
    private long index(ZonedDateTime t) {
        if (t.toInstant().isBefore(anchor.toInstant())) return -1;
        double secs = (t.toEpochSecond() - anchor.toEpochSecond());
        long n = (long) Math.floor(secs / every.approxSeconds());
        if (n < 0) n = 0;
        while (n > 0 && occurrence(n).toInstant().isAfter(t.toInstant())) n--;
        while (!occurrence(n + 1).toInstant().isAfter(t.toInstant())) n++;
        return n;
    }

    @Override
    public ZonedDateTime next(ZonedDateTime after) {
        long n = index(after);
        return occurrence(n + 1);
    }

    @Override
    public ZonedDateTime previousOrSame(ZonedDateTime at) {
        long n = index(at);
        return n < 0 ? null : occurrence(n);
    }

    @Override
    public String toString() { return "every(" + every + " from " + anchor + ")"; }
}
