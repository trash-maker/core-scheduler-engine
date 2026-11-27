package trash_maker.domain;

import org.junit.jupiter.api.Test;
import trash_maker.common.IsoAmount;
import trash_maker.domain.recurrence.Recurrence.CronRecurrence;
import trash_maker.domain.recurrence.Recurrence.PeriodRecurrence;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RecurrenceTest {
    static final ZoneId ROME = ZoneId.of("Europe/Rome");

    static ZonedDateTime z(String local) { return ZonedDateTime.of(LocalDateTime.parse(local), ROME); }

    @Test
    void cronMonthEnd() {
        CronRecurrence c = new CronRecurrence("0 9 L * *", ROME);
        assertThat(c.next(z("2024-01-31T09:00")).toLocalDateTime()).isEqualTo(LocalDateTime.parse("2024-02-29T09:00"));
        assertThat(c.next(z("2023-02-01T00:00")).toLocalDateTime()).isEqualTo(LocalDateTime.parse("2023-02-28T09:00"));
        assertThat(c.previousOrSame(z("2024-03-15T00:00")).toLocalDateTime()).isEqualTo(LocalDateTime.parse("2024-02-29T09:00"));
    }

    @Test
    void cronInTimezone() {
        CronRecurrence c = new CronRecurrence("0 0 1 * *", ROME);
        // midnight Rome on 1 April (CEST, UTC+2) = 22:00Z previous day
        assertThat(c.next(z("2024-03-15T00:00")).toInstant().toString()).isEqualTo("2024-03-31T22:00:00Z");
    }

    @Test
    void dstSpringForwardGapFiresAfterGap() {
        // 2024-03-31: 02:00 -> 03:00 in Rome
        CronRecurrence c = new CronRecurrence("30 2 * * *", ROME);
        ZonedDateTime n = c.next(z("2024-03-30T12:00"));
        assertThat(n.toInstant().toString()).isEqualTo("2024-03-31T01:00:00Z"); // 03:00 CEST
        assertThat(c.next(n).toLocalDateTime()).isEqualTo(LocalDateTime.parse("2024-04-01T02:30"));
    }

    @Test
    void dstFallBackFiresOnce() {
        // 2024-10-27: 03:00 -> 02:00 in Rome; 02:30 occurs twice
        CronRecurrence c = new CronRecurrence("30 2 * * *", ROME);
        ZonedDateTime first = c.next(z("2024-10-26T12:00"));
        assertThat(first.toInstant().toString()).isEqualTo("2024-10-27T00:30:00Z"); // earlier offset (+02:00)
        assertThat(c.next(first).toLocalDateTime()).isEqualTo(LocalDateTime.parse("2024-10-28T02:30"));
    }

    @Test
    void cronListsRangesStepsNames() {
        CronRecurrence c = new CronRecurrence("*/15 9-17 * JAN-MAR MON-FRI", ROME);
        assertThat(c.next(z("2024-01-06T10:00")).toLocalDateTime()).isEqualTo(LocalDateTime.parse("2024-01-08T09:00")); // Sat -> Mon
        assertThat(c.next(z("2024-01-08T09:00")).toLocalDateTime()).isEqualTo(LocalDateTime.parse("2024-01-08T09:15"));
        assertThatThrownBy(() -> new CronRecurrence("0 25 * * *", ROME)).hasMessageContaining("hour");
        assertThatThrownBy(() -> new CronRecurrence("0 0 *", ROME)).hasMessageContaining("5 fields");
    }

    @Test
    void periodWithMonthEndAnchorClampsPerMonth() {
        PeriodRecurrence p = new PeriodRecurrence(z("2024-01-31T09:00"), IsoAmount.parse("P1M"));
        ZonedDateTime a = p.next(z("2024-01-31T09:00"));
        assertThat(a.toLocalDateTime()).isEqualTo(LocalDateTime.parse("2024-02-29T09:00"));
        ZonedDateTime b = p.next(a);
        assertThat(b.toLocalDateTime()).isEqualTo(LocalDateTime.parse("2024-03-31T09:00")); // not cumulative clamping
        assertThat(p.previousOrSame(z("2024-03-30T00:00")).toLocalDateTime()).isEqualTo(LocalDateTime.parse("2024-02-29T09:00"));
        assertThat(p.previousOrSame(z("2024-01-30T00:00"))).isNull();
    }

    @Test
    void periodDailyKeepsLocalTimeAcrossDst() {
        PeriodRecurrence p = new PeriodRecurrence(z("2024-03-30T09:00"), IsoAmount.parse("P1D"));
        assertThat(p.next(z("2024-03-30T09:00")).toLocalDateTime()).isEqualTo(LocalDateTime.parse("2024-03-31T09:00"));
        PeriodRecurrence h = new PeriodRecurrence(z("2024-03-30T09:00"), IsoAmount.parse("PT24H"));
        assertThat(h.next(z("2024-03-30T09:00")).toLocalDateTime()).isEqualTo(LocalDateTime.parse("2024-03-31T10:00"));
    }

    @Test
    void isoAmount() {
        IsoAmount a = IsoAmount.parse("-P3D");
        assertThat(a.addTo(z("2024-03-04T09:00")).toLocalDateTime()).isEqualTo(LocalDateTime.parse("2024-03-01T09:00"));
        assertThat(IsoAmount.parse("P1DT2H").addTo(z("2024-03-04T09:00")).toLocalDateTime()).isEqualTo(LocalDateTime.parse("2024-03-05T11:00"));
    }
}
