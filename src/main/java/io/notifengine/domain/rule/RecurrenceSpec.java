package io.notifengine.domain.rule;

import io.notifengine.common.IsoAmount;
import io.notifengine.domain.recurrence.CronRecurrence;
import io.notifengine.domain.recurrence.PeriodRecurrence;
import io.notifengine.domain.recurrence.Recurrence;

import java.time.Instant;
import java.time.ZoneId;

/** Series recurrence: cron or fixed ISO period anchored at the series start. */
public record RecurrenceSpec(String cron, IsoAmount every) {
    public Recurrence build(Instant start, ZoneId zone) {
        return cron != null ? new CronRecurrence(cron, zone) : new PeriodRecurrence(start.atZone(zone), every);
    }
}
