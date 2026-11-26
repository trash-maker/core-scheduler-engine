package trash_maker.domain.series;

import trash_maker.domain.notification.Dependencies;
import trash_maker.domain.notification.NotificationStatus;
import trash_maker.domain.recurrence.Recurrence;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Set;

/** Pure decisions for one NEXT_OCCURRENCE of a recurring series. */
public final class SeriesStep {
    private SeriesStep() {}

    /** Series facts needed for the decision. */
    public record Series(int occurrences, Integer maxOccurrences, Instant untilAt) {}

    /** WHILE_IN_STATE parent facts (null when the series has no parent). */
    public record Parent(boolean exists, NotificationStatus status, Set<NotificationStatus> states) {}

    public sealed interface Before permits Close, Proceed {}

    public record Close(String reason) implements Before {}

    /**
     * @param misfire true: the occurrence is late beyond misfireGrace and is skipped (SKIP policy)
     * @param index   occurrence index (1-based)
     * @param nextDue next occurrence (after the scheduled one, or after now on misfire); null = none
     */
    public record Proceed(boolean misfire, int index, Instant nextDue) implements Before {}

    public static Before before(Series s, Parent parent, Recurrence rec, ZoneId zone, Duration misfireGrace, Instant scheduledAt, Instant now) {
        if (parent != null) {
            if (!parent.exists()) return new Close("PARENT_TERMINATED");
            if (!Dependencies.inStates(parent.states(), parent.status())) {
                boolean recovered = parent.status() == NotificationStatus.SUCCEEDED || !parent.status().terminal();
                return new Close(recovered ? "PARENT_RECOVERED" : "PARENT_TERMINATED");
            }
        }
        if ((s.maxOccurrences() != null && s.occurrences() >= s.maxOccurrences()) || (s.untilAt() != null && scheduledAt.isAfter(s.untilAt())))
            return new Close("LIMIT_REACHED");
        boolean misfire = misfireGrace != null && now.isAfter(scheduledAt.plus(misfireGrace));
        ZonedDateTime n = rec.next((misfire ? now : scheduledAt).atZone(zone));
        return new Proceed(misfire, s.occurrences() + 1, n == null ? null : n.toInstant());
    }

    /** After the occurrence: close when the limit is reached or there is no next occurrence within {@code until}. */
    public static boolean closeAfter(int occurrencesAfter, Integer maxOccurrences, Instant nextDue, Instant untilAt) {
        return (maxOccurrences != null && occurrencesAfter >= maxOccurrences) || nextDue == null
                || (untilAt != null && nextDue.isAfter(untilAt));
    }
}
