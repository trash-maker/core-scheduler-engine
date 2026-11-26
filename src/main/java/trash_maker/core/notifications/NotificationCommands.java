package trash_maker.core.notifications;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import jakarta.transaction.Transactional;
import trash_maker.core.db.Db;
import trash_maker.core.timers.TimerRepository;

import java.time.Clock;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/** Transactional operator commands (API / runbook). */
@Singleton
public class NotificationCommands {
    private final Db db;
    private final NotificationEngine engine;
    private final TimerRepository timers;
    private final Clock clock;

    @Inject
    public NotificationCommands(Db db, NotificationEngine engine, TimerRepository timers, Clock clock) {
        this.db = db;
        this.engine = engine;
        this.timers = timers;
        this.clock = clock;
    }

    @Transactional
    public Optional<NotificationRow> cancel(String tenant, UUID id, String by) {
        return db.with(c -> engine.cancel(c, tenant, id, by, clock.instant()));
    }

    @Transactional
    public Optional<NotificationRow> retrigger(String tenant, UUID id, String by) {
        return db.with(c -> engine.retrigger(c, tenant, id, by, clock.instant()));
    }

    @Transactional
    public Optional<SeriesRow> cancelSeries(String tenant, UUID id) {
        return db.with(c -> engine.cancelSeries(c, tenant, id, clock.instant()));
    }

    @Transactional
    public int requeueDeadTimers(List<Long> ids) {
        return db.with(c -> timers.requeueDead(c, ids, clock.instant()));
    }
}
