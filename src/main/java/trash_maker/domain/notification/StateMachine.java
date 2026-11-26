package trash_maker.domain.notification;

import static trash_maker.domain.notification.NotificationStatus.*;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.DoubleSupplier;

/**
 * Pure notification state machine: (state, event, now) -> new state + effects + history.
 * No I/O; randomness (jitter) is injected. Illegal transitions throw {@link IllegalTransitionException};
 * stale or duplicate inputs (late timers, superseded outcomes) return an ignored transition.
 */
public final class StateMachine {

    static final Map<NotificationStatus, Set<NotificationStatus>> LEGAL = new EnumMap<>(NotificationStatus.class);

    static {
        LEGAL.put(WAITING_ON_PARENT, EnumSet.of(CREATED, SKIPPED, CANCELLED, EXPIRED));
        LEGAL.put(CREATED, EnumSet.of(SCHEDULED, PUBLISHED, CANCELLED, EXPIRED, SKIPPED));
        LEGAL.put(SCHEDULED, EnumSet.of(PUBLISHED, CANCELLED, EXPIRED));
        LEGAL.put(PUBLISHED, EnumSet.of(SUCCEEDED, FAILED, CANCELLED));
        LEGAL.put(FAILED, EnumSet.of(RETRY_SCHEDULED, RETRIES_EXHAUSTED));
        LEGAL.put(RETRY_SCHEDULED, EnumSet.of(PUBLISHED, SUCCEEDED, CANCELLED, EXPIRED));
        LEGAL.put(RETRIES_EXHAUSTED, EnumSet.of(SUCCEEDED, PUBLISHED));
        LEGAL.put(CANCELLED, EnumSet.of(PUBLISHED));
        LEGAL.put(EXPIRED, EnumSet.of(PUBLISHED));
        LEGAL.put(SUCCEEDED, EnumSet.noneOf(NotificationStatus.class));
        LEGAL.put(SKIPPED, EnumSet.noneOf(NotificationStatus.class));
    }

    public static boolean isLegal(NotificationStatus from, NotificationStatus to) { return LEGAL.get(from).contains(to); }

    private static final Set<TimerKind> ALL_TIMERS = EnumSet.of(TimerKind.PUBLISH, TimerKind.ATTEMPT_TIMEOUT, TimerKind.EXPIRE);

    private final DoubleSupplier random;

    public StateMachine(DoubleSupplier random) { this.random = random; }

    /** Mutable builder used while computing one transition. */
    private static final class B {
        NotificationState s;
        final List<StatusChange> changes = new ArrayList<>();
        final List<Effect> effects = new ArrayList<>();
        final String event;

        B(NotificationState s, String event) { this.s = s; this.event = event; }

        void move(NotificationStatus to, String reason) {
            if (!isLegal(s.status(), to)) throw new IllegalTransitionException("illegal transition " + s.status() + " -> " + to + " on " + event);
            changes.add(new StatusChange(s.status(), to, event, s.attempt(), reason));
            s = s.with(to, reason);
        }

        Transition done() { return new Transition(s, List.copyOf(changes), List.copyOf(effects), null); }
    }

    private static Transition ignore(NotificationState s, String why) { return new Transition(s, List.of(), List.of(), why); }

    public Transition apply(NotificationState s, Event e, Instant now) {
        B b = new B(s, e.name());
        return switch (e) {
            case Event.Start x -> {
                if (s.status() != CREATED) yield ignore(s, "not CREATED");
                startCreated(b, now);
                yield b.done();
            }
            case Event.Activate a -> {
                if (s.status() != WAITING_ON_PARENT) yield ignore(s, "not WAITING_ON_PARENT");
                b.s = b.s.withDue(a.dueAt());
                b.move(CREATED, "parent trigger matched");
                startCreated(b, now);
                yield b.done();
            }
            case Event.PublishDue x -> {
                if (s.status() != SCHEDULED && s.status() != RETRY_SCHEDULED) yield ignore(s, "stale publish timer (status " + s.status() + ")");
                if (s.expiresAt() != null && !now.isBefore(s.expiresAt())) {
                    b.effects.add(new Effect.CancelTimers(ALL_TIMERS));
                    b.move(EXPIRED, "expired before publish");
                } else publish(b, now, false);
                yield b.done();
            }
            case Event.Outcome o -> outcome(b, o, now);
            case Event.AttemptTimeout t -> {
                if (s.status() != PUBLISHED || t.attempt() != s.attempt()) yield ignore(s, "stale attempt timeout");
                fail(b, "FAILURE", "TIMEOUT", false, now);
                yield b.done();
            }
            case Event.Cancel c -> {
                if (s.status().terminal()) throw new IllegalTransitionException("cannot cancel a notification in status " + s.status());
                b.effects.add(new Effect.CancelTimers(ALL_TIMERS));
                b.move(CANCELLED, c.reason());
                yield b.done();
            }
            case Event.Expire x -> {
                if (!EnumSet.of(WAITING_ON_PARENT, CREATED, SCHEDULED, RETRY_SCHEDULED).contains(s.status())) yield ignore(s, "not expirable in " + s.status());
                if (s.expiresAt() == null || now.isBefore(s.expiresAt())) yield ignore(s, "not yet expired");
                b.effects.add(new Effect.CancelTimers(ALL_TIMERS));
                b.move(EXPIRED, "expiresAt reached");
                yield b.done();
            }
            case Event.Retrigger r -> {
                if (!EnumSet.of(RETRIES_EXHAUSTED, CANCELLED, EXPIRED).contains(s.status()))
                    throw new IllegalTransitionException("re-trigger only allowed from RETRIES_EXHAUSTED, CANCELLED or EXPIRED (is " + s.status() + ")");
                b.s = b.s.withExpires(null);
                b.effects.add(new Effect.CancelTimers(ALL_TIMERS));
                publish(b, now, true);
                yield b.done();
            }
            case Event.Skip k -> {
                if (s.status() != WAITING_ON_PARENT && s.status() != CREATED) yield ignore(s, "not skippable in " + s.status());
                b.move(SKIPPED, k.reason());
                yield b.done();
            }
        };
    }

    private void startCreated(B b, Instant now) {
        NotificationState s = b.s;
        if (s.expiresAt() != null && !now.isBefore(s.expiresAt())) {
            b.move(EXPIRED, "expired before first publish");
            return;
        }
        if (s.expiresAt() != null) b.effects.add(new Effect.ScheduleTimer(TimerKind.EXPIRE, s.expiresAt()));
        if (s.dueAt() == null || !s.dueAt().isAfter(now)) publish(b, now, false);
        else {
            b.move(SCHEDULED, null);
            b.effects.add(new Effect.ScheduleTimer(TimerKind.PUBLISH, s.dueAt()));
        }
    }

    private void publish(B b, Instant now, boolean resetCycle) {
        NotificationState s = b.s;
        int cycle = resetCycle ? 1 : s.cycleAttempts() + 1;
        b.s = s.withAttempt(s.attempt() + 1, cycle, now);
        b.move(PUBLISHED, resetCycle ? "manual re-trigger" : null);
        Duration timeout = s.retryPolicy().attemptTimeout();
        b.effects.add(new Effect.Emit(b.s.attempt(), timeout));
    }

    private Transition outcome(B b, Event.Outcome o, Instant now) {
        NotificationState s = b.s;
        if (o.attempt() > s.attempt() || o.attempt() < 1) return ignore(s, "unknown attempt " + o.attempt() + " (current " + s.attempt() + ")");
        if (o.outcome() == OutcomeType.SUCCESS) {
            if (s.status() == SUCCEEDED) return ignore(s, "duplicate SUCCESS");
            if (!EnumSet.of(PUBLISHED, RETRY_SCHEDULED, RETRIES_EXHAUSTED).contains(s.status()))
                return ignore(s, "SUCCESS ignored in status " + s.status());
            b.effects.add(new Effect.CancelTimers(ALL_TIMERS));
            String reason = s.status() == PUBLISHED && o.attempt() == s.attempt() ? null : "late SUCCESS for attempt " + o.attempt();
            b.move(SUCCEEDED, reason);
            return b.done();
        }
        if (s.status() != PUBLISHED || o.attempt() != s.attempt())
            return ignore(s, "late " + o.outcome() + " for superseded attempt " + o.attempt() + " (status " + s.status() + ", attempt " + s.attempt() + ")");
        fail(b, o.outcome().name(), o.reason(), o.outcome() == OutcomeType.PERMANENT_FAILURE, now);
        return b.done();
    }

    private void fail(B b, String outcome, String reason, boolean permanent, Instant now) {
        NotificationState s = b.s;
        b.effects.add(new Effect.CancelTimers(EnumSet.of(TimerKind.ATTEMPT_TIMEOUT)));
        b.s = s.withFailure(new Failure(outcome, reason, s.attempt(), now));
        b.move(FAILED, reason);
        trash_maker.domain.rule.RetryPolicy p = b.s.retryPolicy();
        if (permanent) { b.move(RETRIES_EXHAUSTED, "permanent failure"); return; }
        if (b.s.cycleAttempts() >= p.maxAttempts()) { b.move(RETRIES_EXHAUSTED, "max attempts " + p.maxAttempts() + " reached"); return; }
        Instant next = now.plus(p.delay(b.s.cycleAttempts(), random.getAsDouble()));
        if (b.s.expiresAt() != null && !next.isBefore(b.s.expiresAt())) { b.move(RETRIES_EXHAUSTED, "next retry would be past expiresAt"); return; }
        b.s = b.s.withDue(next);
        b.move(RETRY_SCHEDULED, "retry at " + next);
        b.effects.add(new Effect.ScheduleTimer(TimerKind.PUBLISH, next));
    }

}
