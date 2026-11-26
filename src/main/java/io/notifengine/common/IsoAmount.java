package io.notifengine.common;

import java.time.Duration;
import java.time.Period;
import java.time.ZonedDateTime;
import java.util.Objects;

/**
 * ISO-8601 amount that may combine a calendar part (years/months/weeks/days, local-time arithmetic)
 * and a time part (hours/minutes/seconds, exact). Examples: "P1M", "-P3D", "PT15M", "P1DT2H".
 */
public record IsoAmount(Period period, Duration duration) {

    public static final IsoAmount ZERO = new IsoAmount(Period.ZERO, Duration.ZERO);

    public IsoAmount {
        Objects.requireNonNull(period);
        Objects.requireNonNull(duration);
    }

    public static IsoAmount parse(String text) {
        if (text == null || text.isBlank()) throw new IllegalArgumentException("empty ISO-8601 amount");
        String s = text.trim().toUpperCase();
        boolean negative = false;
        if (s.startsWith("-")) { negative = true; s = s.substring(1); }
        else if (s.startsWith("+")) { s = s.substring(1); }
        if (!s.startsWith("P")) throw new IllegalArgumentException("ISO-8601 amount must start with P: " + text);
        int t = s.indexOf('T');
        String datePart = t < 0 ? s : s.substring(0, t);
        String timePart = t < 0 ? "" : s.substring(t);
        Period p = datePart.length() > 1 ? Period.parse(datePart) : Period.ZERO;
        Duration d = timePart.isEmpty() ? Duration.ZERO : Duration.parse("P" + timePart);
        if (datePart.length() <= 1 && timePart.isEmpty()) throw new IllegalArgumentException("empty ISO-8601 amount: " + text);
        if (negative) { p = p.negated(); d = d.negated(); }
        return new IsoAmount(p, d);
    }

    public ZonedDateTime addTo(ZonedDateTime t) { return t.plus(period).plus(duration); }

    public ZonedDateTime addTo(ZonedDateTime t, long times) {
        return t.plus(period.multipliedBy(Math.toIntExact(times))).plus(duration.multipliedBy(times));
    }

    public boolean isPositive() {
        return !isZero() && !period.isNegative() && !duration.isNegative()
                && period.getYears() >= 0 && period.getMonths() >= 0 && period.getDays() >= 0;
    }

    public boolean isZero() { return period.isZero() && duration.isZero(); }

    /** Rough length in seconds, used only for estimates. */
    public double approxSeconds() {
        return period.toTotalMonths() * 30.436875 * 86400 + period.getDays() * 86400.0 + duration.toMillis() / 1000.0;
    }

    @Override
    public String toString() {
        if (period.isZero()) return duration.toString();
        if (duration.isZero()) return period.toString();
        return period + duration.toString().substring(1);
    }
}
