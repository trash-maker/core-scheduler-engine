package trash_maker.infra.ops;

import trash_maker.core.Engine;
import trash_maker.core.dispatch.OutboxRelay;
import trash_maker.core.timers.TimerWorker;
import trash_maker.core.timers.WakeupHub;
import trash_maker.infra.config.NotificationsConfig;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.List;

/** Starts timer workers and outbox relays on virtual threads according to notifications.roles; graceful stop. */
@ApplicationScoped
public class Workers {
    private static final Logger LOG = Logger.getLogger(Workers.class);

    @Inject Engine engine;
    @Inject NotificationsConfig cfg;
    @Inject WakeupHub hub;

    private final List<TimerWorker> timerWorkers = new ArrayList<>();
    private final List<Thread> threads = new ArrayList<>();
    private volatile boolean relayRunning = true;

    void start(@Observes @Priority(10) StartupEvent ev) {
        engine.maintenance.ensurePartitions(cfg.retention().partitionsAhead());
        engine.registry.refresh();
        if (cfg.hasRole("timers")) {
            engine.rules.ensureSweeps();
            for (int i = 0; i < cfg.timers().workers(); i++) {
                TimerWorker w = new TimerWorker(engine.timers, engine.config, engine.clock);
                hub.register(w);
                timerWorkers.add(w);
                threads.add(Thread.ofVirtual().name("timer-worker-" + i).start(w));
            }
        }
        if (cfg.hasRole("relay")) {
            for (int i = 0; i < cfg.relay().workers(); i++) threads.add(Thread.ofVirtual().name("outbox-relay-" + i).start(this::relayLoop));
        }
        var c = org.eclipse.microprofile.config.ConfigProvider.getConfig();
        LOG.infof("notification engine started: roles=%s timerWorkers=%d channels: signals-in=%s results-in=%s fake-actuator-in=%s",
                cfg.roles(), timerWorkers.size(),
                c.getOptionalValue("mp.messaging.incoming.signals-in.enabled", String.class).orElse("?"),
                c.getOptionalValue("mp.messaging.incoming.results-in.enabled", String.class).orElse("?"),
                c.getOptionalValue("mp.messaging.incoming.fake-actuator-in.enabled", String.class).orElse("?"));
    }

    private void relayLoop() {
        long backoff = 0;
        while (relayRunning) {
            try {
                OutboxRelay.Result r = engine.relay.relayOnce();
                if (r.failed() > 0) backoff = Math.min(Math.max(backoff * 2, 100), 5000);
                else backoff = 0;
                if (r.claimed() >= cfg.relay().batchSize() && backoff == 0) continue;
                Thread.sleep(backoff > 0 ? backoff : cfg.relay().idle().toMillis());
            } catch (InterruptedException e) {
                return;
            } catch (RuntimeException e) {
                LOG.errorf("outbox relay error: %s", e.getMessage());
                try { Thread.sleep(1000); } catch (InterruptedException ie) { return; }
            }
        }
    }

    void stop(@Observes ShutdownEvent ev) {
        relayRunning = false;
        timerWorkers.forEach(w -> { w.stop(); hub.unregister(w); });
        for (Thread t : threads) {
            try { t.join(10_000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
        LOG.info("notification engine workers stopped");
    }

    public boolean alive() { return threads.stream().allMatch(Thread::isAlive); }
}
