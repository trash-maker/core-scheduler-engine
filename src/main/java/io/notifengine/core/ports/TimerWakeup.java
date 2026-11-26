package io.notifengine.core.ports;

import java.time.Instant;

/** Notified after commit when a timer due soon was written, so local workers do not wait for the next poll. */
public interface TimerWakeup {
    TimerWakeup NONE = due -> {};

    void hint(Instant dueAt);
}
