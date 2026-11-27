package trash_maker.infra;

import trash_maker.core.NotificationsConfig;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.eclipse.microprofile.health.HealthCheck;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.eclipse.microprofile.health.Liveness;
import org.jboss.logging.Logger;
import trash_maker.core.Engine;
import trash_maker.core.OutboxRelay;
import trash_maker.core.Ports.WakeupHub;
import trash_maker.core.TimerService.TimerWorker;

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

    void start(@Observes StartupEvent ev) {
        engine.maintenance.ensurePartitions(cfg.retention().partitionsAhead());
        engine.registry.refresh();
        if (cfg.role().timers()) {
            engine.rules.ensureSweeps();
            for (int i = 0; i < cfg.timers().workers(); i++) {
                TimerWorker w = new TimerWorker(engine.timers, engine.config, engine.clock);
                hub.register(w);
                timerWorkers.add(w);
                threads.add(Thread.ofVirtual().name("timer-worker-" + i).start(w));
            }
        }
        if (cfg.role().relay()) {
            for (int i = 0; i < cfg.relay().workers(); i++) threads.add(Thread.ofVirtual().name("outbox-relay-" + i).start(this::relayLoop));
        }
        var c = org.eclipse.microprofile.config.ConfigProvider.getConfig();
        LOG.infof("notification engine started: roles=%s timerWorkers=%d channels: signals-in=%s results-in=%s fake-actuator-in=%s",
                roles(), timerWorkers.size(),
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

    private String roles() {
        var r = cfg.role();
        return String.join(",", java.util.stream.Stream.of(r.ingest() ? "ingest" : null, r.timers() ? "timers" : null, r.relay() ? "relay" : null,
                r.results() ? "results" : null, r.api() ? "api" : null, r.fakeActuator() ? "fake-actuator" : null).filter(x -> x != null).toList());
    }

    public boolean alive() { return threads.stream().allMatch(Thread::isAlive); }

    /** Liveness: timer workers and relays are running (DB and Kafka readiness come from the extensions). */
    @Liveness
    @ApplicationScoped
    public static class WorkersHealth implements HealthCheck {
        @Inject Workers workers;

        @Override
        public HealthCheckResponse call() {
            return HealthCheckResponse.named("engine-workers").status(workers.alive()).build();
        }
    }
}
