package trash_maker.domain;

import org.junit.jupiter.api.Test;
import trash_maker.domain.scheduling.SchedulingPolicies.SweepStep;
import trash_maker.domain.scheduling.SchedulingPolicies.TimerFailurePolicy;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class TimerAndSweepTest {
    static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");

    @Test
    void failedTimersBackOffExponentiallyThenDie() {
        assertThat(TimerFailurePolicy.decide(0, 5, Duration.ofSeconds(1), NOW)).isEqualTo(new TimerFailurePolicy.Retry(NOW.plusSeconds(1), 1));
        assertThat(TimerFailurePolicy.decide(3, 5, Duration.ofSeconds(1), NOW)).isEqualTo(new TimerFailurePolicy.Retry(NOW.plusSeconds(8), 4));
        assertThat(TimerFailurePolicy.decide(4, 5, Duration.ofSeconds(1), NOW)).isEqualTo(new TimerFailurePolicy.Dead(5));
    }

    @Test
    void sweepContinuesFullPagesThenSchedulesNextChange() {
        Instant at = NOW;
        assertThat(SweepStep.after(List.of("a", "b"), 2, at, NOW, i -> i.plusSeconds(60))).isEqualTo(new SweepStep.Continue("b", at));
        assertThat(SweepStep.after(List.of("a"), 2, at, NOW, i -> i.plusSeconds(60))).isEqualTo(new SweepStep.Schedule(at.plusSeconds(60)));
        // the sweep took longer than the window: next change computed from now
        assertThat(SweepStep.after(List.of(), 2, at, NOW.plusSeconds(120), i -> i.plusSeconds(60)))
                .isEqualTo(new SweepStep.Schedule(NOW.plusSeconds(180)));
        assertThat(SweepStep.after(List.of(), 2, at, NOW, i -> null)).isInstanceOf(SweepStep.Stop.class);
    }
}
