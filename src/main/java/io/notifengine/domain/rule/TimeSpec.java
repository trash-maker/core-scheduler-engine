package io.notifengine.domain.rule;

import io.notifengine.common.IsoAmount;
import io.notifengine.domain.signal.SignalValue;
import io.notifengine.domain.signal.Value;

import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Map;

/**
 * Point in time: base (NOW or a DATE signal) + ISO offset, optionally set to a local time of day in the rule zone.
 * JSON: {"from":"NOW"|"$signal:due_date", "offset":"-P3D", "atTime":"09:00"}
 */
public record TimeSpec(String fromSignal, IsoAmount offset, LocalTime atTime) {

    public static final TimeSpec NOW = new TimeSpec(null, IsoAmount.ZERO, null);

    public static class MissingBaseException extends RuntimeException {
        public MissingBaseException(String m) { super(m); }
    }

    public Instant resolve(Instant now, ZoneId zone, Map<String, SignalValue> signals) {
        ZonedDateTime base;
        if (fromSignal == null) base = now.atZone(zone);
        else {
            SignalValue sv = signals.get(fromSignal);
            if (sv == null || !(sv.value() instanceof Value.Date d)) throw new MissingBaseException("DATE signal '" + fromSignal + "' required for time computation is missing");
            base = d.at(zone);
        }
        ZonedDateTime t = offset.addTo(base);
        if (atTime != null) t = ZonedDateTime.ofLocal(t.toLocalDate().atTime(atTime), zone, t.getOffset());
        return t.toInstant();
    }

    public Instant resolveFrom(Instant start, ZoneId zone) {
        ZonedDateTime t = offset.addTo(start.atZone(zone));
        if (atTime != null) t = ZonedDateTime.ofLocal(t.toLocalDate().atTime(atTime), zone, t.getOffset());
        return t.toInstant();
    }
}
