package trash_maker.domain;

import org.junit.jupiter.api.Test;
import trash_maker.common.IsoAmount;
import trash_maker.domain.recurrence.PeriodRecurrence;
import trash_maker.domain.series.SeriesStep;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.EnumSet;

import static org.assertj.core.api.Assertions.assertThat;
import static trash_maker.domain.notification.NotificationStatus.*;

class SeriesStepTest {
    static final Instant T = Instant.parse("2026-01-01T10:00:00Z");
    static final PeriodRecurrence HOURLY = new PeriodRecurrence(T.atZone(ZoneOffset.UTC), IsoAmount.parse("PT1H"));
    static final SeriesStep.Series FRESH = new SeriesStep.Series(0, 3, null);

    static SeriesStep.Before before(SeriesStep.Series s, SeriesStep.Parent p, Duration grace, Instant scheduled, Instant now) {
        return SeriesStep.before(s, p, HOURLY, ZoneOffset.UTC, grace, scheduled, now);
    }

    @Test
    void emitsAndComputesNext() {
        assertThat(before(FRESH, null, null, T, T)).isEqualTo(new SeriesStep.Proceed(false, 1, T.plusSeconds(3600)));
    }

    @Test
    void misfireSkipsAndJumpsAfterNow() {
        Instant late = T.plus(Duration.ofMinutes(70));
        assertThat(before(FRESH, null, Duration.ofMinutes(5), T, late)).isEqualTo(new SeriesStep.Proceed(true, 1, T.plusSeconds(7200)));
        assertThat(before(FRESH, null, Duration.ofHours(2), T, late)).isEqualTo(new SeriesStep.Proceed(false, 1, T.plusSeconds(3600)));
    }

    @Test
    void limitsAndParentStateClose() {
        assertThat(before(new SeriesStep.Series(3, 3, null), null, null, T, T)).isEqualTo(new SeriesStep.Close("LIMIT_REACHED"));
        assertThat(before(new SeriesStep.Series(0, null, T.minusSeconds(1)), null, null, T, T)).isEqualTo(new SeriesStep.Close("LIMIT_REACHED"));
        var states = EnumSet.of(FAILED, RETRY_SCHEDULED, RETRIES_EXHAUSTED);
        assertThat(before(FRESH, new SeriesStep.Parent(true, RETRY_SCHEDULED, states), null, T, T)).isInstanceOf(SeriesStep.Proceed.class);
        assertThat(before(FRESH, new SeriesStep.Parent(true, SUCCEEDED, states), null, T, T)).isEqualTo(new SeriesStep.Close("PARENT_RECOVERED"));
        assertThat(before(FRESH, new SeriesStep.Parent(true, PUBLISHED, states), null, T, T)).isEqualTo(new SeriesStep.Close("PARENT_RECOVERED"));
        assertThat(before(FRESH, new SeriesStep.Parent(true, CANCELLED, states), null, T, T)).isEqualTo(new SeriesStep.Close("PARENT_TERMINATED"));
        assertThat(before(FRESH, new SeriesStep.Parent(false, null, states), null, T, T)).isEqualTo(new SeriesStep.Close("PARENT_TERMINATED"));
    }

    @Test
    void closeAfter() {
        assertThat(SeriesStep.closeAfter(3, 3, T, null)).isTrue();
        assertThat(SeriesStep.closeAfter(1, 3, null, null)).isTrue();
        assertThat(SeriesStep.closeAfter(1, 3, T.plusSeconds(10), T)).isTrue();
        assertThat(SeriesStep.closeAfter(1, null, T, T)).isFalse();
    }
}
