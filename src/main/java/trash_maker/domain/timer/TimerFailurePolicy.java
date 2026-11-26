package trash_maker.domain.timer;

import java.time.Duration;
import java.time.Instant;

/** Pure decision for a failed timer handler: retry with exponential backoff, or move to timer_dead. */
public final class TimerFailurePolicy {
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
