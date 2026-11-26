package trash_maker.domain.timer;

import java.time.Instant;
import java.util.List;
import java.util.function.Function;

/**
 * Pure continuation of a per-rule sweep (NOW-based conditions): after one entity page either continue with the
 * next page right away (cursor), schedule the sweep at the rule's next change instant, or stop.
 */
public final class SweepStep {
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
