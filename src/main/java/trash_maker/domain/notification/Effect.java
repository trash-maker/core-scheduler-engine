package trash_maker.domain.notification;

import java.time.Duration;
import java.time.Instant;
import java.util.Set;

/** Side effects requested by the state machine, applied by the caller in the same DB transaction. */
public sealed interface Effect {
    record ScheduleTimer(TimerKind kind, Instant dueAt) implements Effect {}

    /** Remove pending timers of these kinds for the notification. */
    record CancelTimers(Set<TimerKind> kinds) implements Effect {}

    /** Write the message for {@code attempt} to the outbox. The attempt timeout timer is armed after the Kafka ack. */
    record Emit(int attempt, Duration attemptTimeout) implements Effect {}
}
