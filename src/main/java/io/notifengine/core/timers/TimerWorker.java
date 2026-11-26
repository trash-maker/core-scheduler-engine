package io.notifengine.core.timers;

import io.notifengine.core.EngineConfig;
import io.notifengine.core.ports.TimerWakeup;
import org.jboss.logging.Logger;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Worker loop (run on a virtual thread). Polling is the source of truth; idle sleep is bounded to
 * [idleMin, idleMax] and shortened by local wake-ups when an imminent timer is committed.
 */
public final class TimerWorker implements Runnable, TimerWakeup {
    private static final Logger LOG = Logger.getLogger(TimerWorker.class);

    private final TimerService service;
    private final EngineConfig cfg;
    private final Clock clock;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition wake = lock.newCondition();
    private volatile boolean running = true;
    private volatile boolean signalled;

    public TimerWorker(TimerService service, EngineConfig cfg, Clock clock) {
        this.service = service;
        this.cfg = cfg;
        this.clock = clock;
    }

    @Override
    public void hint(Instant dueAt) {
        if (dueAt.isAfter(clock.instant().plus(cfg.idleMax()))) return;
        lock.lock();
        try { signalled = true; wake.signalAll(); } finally { lock.unlock(); }
    }

    public void stop() {
        running = false;
        hint(Instant.EPOCH);
    }

    @Override
    public void run() {
        long errorBackoff = 100;
        while (running) {
            try {
                int n = service.runOnce();
                errorBackoff = 100;
                if (n >= cfg.timerBatchSize()) continue;
                Instant next = service.nextDue();
                long waitMs = next == null ? cfg.idleMax().toMillis() : Duration.between(clock.instant(), next).toMillis();
                waitMs = Math.max(cfg.idleMin().toMillis(), Math.min(cfg.idleMax().toMillis(), waitMs));
                if (next != null && !next.isAfter(clock.instant())) waitMs = 0;
                await(waitMs);
            } catch (RuntimeException e) {
                LOG.errorf("timer worker error (retrying in %d ms): %s", errorBackoff, e.getMessage());
                await(errorBackoff);
                errorBackoff = Math.min(errorBackoff * 2, 5000);
            }
        }
    }

    private void await(long ms) {
        if (ms <= 0) return;
        lock.lock();
        try {
            if (!signalled && running) wake.await(ms, TimeUnit.MILLISECONDS);
            signalled = false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            running = false;
        } finally {
            lock.unlock();
        }
    }
}
