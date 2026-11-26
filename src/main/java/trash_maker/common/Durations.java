package trash_maker.common;

import java.time.Duration;
import java.time.format.DateTimeParseException;

public final class Durations {
    private Durations() {}

    /** Parses an exact ISO-8601 duration (days allowed, months/years rejected). */
    public static Duration parse(String text) {
        if (text == null) return null;
        try {
            return Duration.parse(text.trim().toUpperCase());
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException("invalid ISO-8601 duration '" + text + "' (use e.g. PT5M, P1D)");
        }
    }
}
