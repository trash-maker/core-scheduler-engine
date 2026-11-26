package io.notifengine.core.timers;

import io.notifengine.core.ports.TimerWakeup;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/** Fans out local timer wake-ups to the workers of this instance. */
public final class WakeupHub implements TimerWakeup {
    private final List<TimerWakeup> listeners = new CopyOnWriteArrayList<>();

    public void register(TimerWakeup w) { listeners.add(w); }

    public void unregister(TimerWakeup w) { listeners.remove(w); }

    @Override
    public void hint(Instant dueAt) { for (TimerWakeup w : listeners) w.hint(dueAt); }
}
