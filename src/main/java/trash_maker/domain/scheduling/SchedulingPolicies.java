package trash_maker.domain.scheduling;

import trash_maker.domain.notification.Lifecycle.NotificationStatus;
import trash_maker.domain.notification.StateMachine.Dependencies;
import trash_maker.domain.recurrence.Recurrence;

import java.time.Duration;
import java.time.Instant;
import java.time.YearMonth;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Pure scheduling decisions: recurring occurrences, timer failure backoff, sweep continuation, partition retention. */
public final class SchedulingPolicies {
    private SchedulingPolicies() {}

    /** Pure decisions for one NEXT_OCCURRENCE of a recurring series. */
    public static final class SeriesStep {
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

    /** Pure decision for a failed timer handler: retry with exponential backoff, or move to timer_dead. */
    public static final class TimerFailurePolicy {
        private TimerFailurePolicy() {}

        public sealed interface Decision permits Retry, Dead {}

        public record Retry(Instant dueAt, int failures) implements Decision {}

        public record Dead(int failures) implements Decision {}

        /** @param previousFailures failures before this one */
        public static Decision decide(int previousFailures, int maxFailures, Duration baseBackoff, Instant now) {
            int failures = previousFailures + 1;
            if (failures >= maxFailures) return new Dead(failures);
            long backoff = baseBackoff.toMillis() * (1L << Math.min(previousFailures, 10));
            return new Retry(now.plusMillis(backoff), failures);
        }
    }

    /**
     * Pure continuation of a per-rule sweep (NOW-based conditions): after one entity page either continue with the
     * next page right away (cursor), schedule the sweep at the rule's next change instant, or stop.
     */
    public static final class SweepStep {
        private SweepStep() {}

        public sealed interface Next permits Continue, Schedule, Stop {}

        public record Continue(String cursor, Instant sweepInstant) implements Next {}

        public record Schedule(Instant at) implements Next {}

        public record Stop() implements Next {}

        /**
         * @param sweepInstant     the instant this sweep represents
         * @param nextRuleChange   rule's next change strictly after a given instant (null = none)
         */
        public static Next after(List<String> page, int pageSize, Instant sweepInstant, Instant now, Function<Instant, Instant> nextRuleChange) {
            if (!page.isEmpty() && page.size() == pageSize) return new Continue(page.get(page.size() - 1), sweepInstant);
            Instant next = nextRuleChange.apply(sweepInstant);
            if (next != null && !next.isAfter(now)) next = nextRuleChange.apply(now); // sweep took longer than the window
            return next == null ? new Stop() : new Schedule(next);
        }
    }

    /** Pure retention rules for monthly partitions named notification_pYYYY_MM. */
    public static final class RetentionPolicy {
        private static final Pattern PART = Pattern.compile("notification_p(\\d{4})_(\\d{2})");

        private RetentionPolicy() {}

        /** True when the partition ends before the cutoff (it may then be dropped if it holds no active row). */
        public static boolean expired(String partition, Instant cutoff) {
            Matcher m = PART.matcher(partition);
            if (!m.matches()) return false;
            YearMonth ym = YearMonth.of(Integer.parseInt(m.group(1)), Integer.parseInt(m.group(2)));
            return ym.plusMonths(1).atDay(1).atStartOfDay().toInstant(ZoneOffset.UTC).isBefore(cutoff);
        }

        public static String historyPartition(String notificationPartition) {
            return notificationPartition.replace("notification_p", "notification_history_p");
        }
    }
}
