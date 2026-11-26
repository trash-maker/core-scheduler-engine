package trash_maker.domain.rule;

import trash_maker.common.IsoAmount;
import trash_maker.domain.recurrence.CronRecurrence;
import trash_maker.domain.recurrence.PeriodRecurrence;
import trash_maker.domain.recurrence.Recurrence;

import java.time.Instant;
import java.time.ZoneId;

/** Series recurrence: cron or fixed ISO period anchored at the series start. */
public record RecurrenceSpec(String cron, IsoAmount every) {
    public Recurrence build(Instant start, ZoneId zone) {
        return cron != null ? new CronRecurrence(cron, zone) : new PeriodRecurrence(start.atZone(zone), every);
    }
}
