package trash_maker.support;

import jakarta.enterprise.inject.Alternative;
import jakarta.inject.Singleton;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicReference;

/** Controllable clock, enabled as CDI alternative by {@link CoreProfile}. */
@Alternative
@Singleton
public class MutableClock extends Clock {
    private final AtomicReference<Instant> now = new AtomicReference<>(Instant.now());

    public void set(Instant i) { now.set(i); }

    public void advance(Duration d) { now.updateAndGet(i -> i.plus(d)); }

    @Override public ZoneId getZone() { return ZoneOffset.UTC; }

    @Override public Clock withZone(ZoneId zone) { return this; }

    @Override public Instant instant() { return now.get(); }
}
