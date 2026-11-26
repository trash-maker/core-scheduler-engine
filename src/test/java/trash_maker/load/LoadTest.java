package trash_maker.load;

import trash_maker.core.Engine;
import trash_maker.core.EngineConfig;
import trash_maker.core.ports.EngineMetrics;
import trash_maker.core.ports.MessagePublisher;
import trash_maker.core.ports.TraceContext;
import trash_maker.core.timers.TimerWorker;
import trash_maker.core.timers.WakeupHub;
import trash_maker.common.Json;
import trash_maker.infra.kafka.KafkaMessagePublisher;
import trash_maker.support.Fx;
import trash_maker.support.Pg;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.kafka.KafkaContainer;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Load test (opt-in: mvn verify -Pload). Real clock, real PostgreSQL, real worker loops.
 * Scenario A: N notifications whose due_at is spread uniformly over a window (sustained rate).
 * Scenario B: a burst of M notifications all due at the same instant (drain throughput).
 * Reports ingest rate, publish throughput, timers.lag (claim - due) and publish lag (broker ack - due) percentiles.
 * -Dload.kafka=true publishes to a real Kafka broker (Testcontainers) instead of an in-memory sink.
 */
@Tag("load")
class LoadTest {
    static final int RATE = Integer.getInteger("load.rate", 1000);
    static final int SECONDS = Integer.getInteger("load.seconds", 60);
    static final int BURST = Integer.getInteger("load.burst", 20000);
    static final int WORKERS = Integer.getInteger("load.workers", 4);
    static final int RELAYS = Integer.getInteger("load.relays", 2);

    static final class Lag implements EngineMetrics {
        final ConcurrentLinkedQueue<Long> timerLag = new ConcurrentLinkedQueue<>();
        @Override public void recordMs(String name, long ms, String... tags) {
            if (name.equals("timers.lag") && tags.length > 1 && tags[1].equals("PUBLISH")) timerLag.add(ms);
        }
    }

    static final class Sink implements MessagePublisher {
        final MessagePublisher delegate;
        final ConcurrentLinkedQueue<Long> publishLag = new ConcurrentLinkedQueue<>();
        final AtomicLong count = new AtomicLong();
        Sink(MessagePublisher d) { delegate = d; }
        @Override public CompletableFuture<Void> publish(String topic, String key, Map<String, String> headers, String payload) {
            CompletableFuture<Void> f = delegate == null ? CompletableFuture.completedFuture(null) : delegate.publish(topic, key, headers, payload);
            return f.thenRun(() -> {
                var due = Json.parse(payload).path("due_at").asText(null);
                if (due != null) publishLag.add(Duration.between(Instant.parse(due), Instant.now()).toMillis());
                count.incrementAndGet();
            });
        }
    }

    @Test
    void sustainedAndBurst() throws Exception {
        KafkaContainer kafka = null;
        MessagePublisher broker = null;
        if (Boolean.getBoolean("load.kafka")) {
            kafka = new KafkaContainer("apache/kafka:3.8.0");
            kafka.start();
            broker = new KafkaMessagePublisher(kafka.getBootstrapServers(), Map.of());
        }
        StringBuilder report = new StringBuilder();
        report.append("# Load test report\n\n").append("Generated ").append(Instant.now()).append(" on ").append(Runtime.getRuntime().availableProcessors())
                .append(" vCPU; PostgreSQL 16 (Testcontainers, fsync=off); broker: ").append(kafka == null ? "in-memory sink" : "Kafka 3.8 (Testcontainers)")
                .append("; timer workers: ").append(WORKERS).append(", relays: ").append(RELAYS).append(", batch 200/500.\n\n");
        report.append(run("A. sustained " + RATE + "/s for " + SECONDS + "s", RATE * SECONDS, Duration.ofSeconds(SECONDS), broker));
        report.append(run("B. burst " + BURST + " due at the same instant", BURST, Duration.ZERO, broker));
        System.out.println(report);
        Files.createDirectories(Path.of("target"));
        Files.writeString(Path.of("target/load-report" + (kafka == null ? "" : "-kafka") + ".md"), report.toString());
        if (kafka != null) kafka.stop();
    }

    private String run(String name, int n, Duration window, MessagePublisher broker) throws Exception {
        Pg.truncateAll();
        Lag lag = new Lag();
        Sink sink = new Sink(broker);
        WakeupHub hub = new WakeupHub();
        EngineConfig cfg = EngineConfig.defaults();
        Engine e = new Engine(Pg.dataSource(), Clock.systemUTC(), cfg, sink, lag, TraceContext.NONE, hub);
        e.maintenance.ensurePartitions(3);
        e.rules.create("acme", Json.parse("""
                {"id":"load","trigger":{"expressionText":"go = true"},
                 "templates":[{"id":"t","type":"SCHEDULED","action":"PUSH","dueAt":{"from":"$signal:due"},
                   "body":{"user":"${correlationKey}","msg":"hello ${signal.name}"}}]}"""), "load");
        // ---- preload: due_at spread over the window, starting after the preload has finished
        Instant start = Instant.now().plusSeconds(Long.getLong("load.preload-seconds", 20 + n / 1000));
        long t0 = System.nanoTime();
        List<String> batch = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            Instant due = window.isZero() ? start : start.plusMillis(window.toMillis() * i / n);
            String key = "u" + i;
            batch.add(Fx.envelope("n" + i, key, "name", "KPI", "user" + i, Instant.now()));
            batch.add(Fx.envelope("d" + i, key, "due", "DATE", due.toString(), Instant.now()));
            batch.add(Fx.envelope("g" + i, key, "go", "KPI", true, Instant.now()));
            if (batch.size() >= 1500) { e.ingest.ingest(batch); batch.clear(); }
        }
        if (!batch.isEmpty()) e.ingest.ingest(batch);
        double ingestSecs = (System.nanoTime() - t0) / 1e9;
        long scheduled = count(e, "SELECT count(*) FROM notification WHERE status='SCHEDULED'");
        if (Instant.now().isAfter(start)) throw new IllegalStateException("preload overran the start instant; lower load.rate");
        // ---- workers
        List<TimerWorker> workers = new ArrayList<>();
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < WORKERS; i++) {
            TimerWorker w = new TimerWorker(e.timers, cfg, Clock.systemUTC());
            hub.register(w);
            workers.add(w);
            threads.add(Thread.ofVirtual().start(w));
        }
        AtomicBoolean relayOn = new AtomicBoolean(true);
        for (int i = 0; i < RELAYS; i++) threads.add(Thread.ofVirtual().start(() -> {
            while (relayOn.get()) {
                if (e.relay.relayOnce().claimed() < cfg.relayBatchSize()) {
                    try { Thread.sleep(20); } catch (InterruptedException ie) { return; }
                }
            }
        }));
        Instant deadline = start.plus(window).plusSeconds(120);
        while (sink.count.get() < n && Instant.now().isBefore(deadline)) Thread.sleep(200);
        Instant end = Instant.now();
        workers.forEach(TimerWorker::stop);
        relayOn.set(false);
        for (Thread t : threads) t.join(5000);
        long published = sink.count.get();
        double secs = Math.max(0.001, Duration.between(start, end).toMillis() / 1000.0);
        long[] tl = lag.timerLag.stream().mapToLong(Long::longValue).sorted().toArray();
        long[] pl = sink.publishLag.stream().mapToLong(Long::longValue).sorted().toArray();
        return "## " + name + "\n\n| metric | value |\n|---|---|\n"
                + "| notifications created (SCHEDULED) | " + scheduled + " |\n"
                + "| signal ingest | " + (3L * n) + " signals in " + String.format("%.1f", ingestSecs) + " s = " + String.format("%.0f", 3 * n / ingestSecs) + " signals/s |\n"
                + "| published | " + published + " / " + n + " |\n"
                + "| publish throughput (first due -> last ack) | " + String.format("%.0f", published / secs) + " msg/s |\n"
                + "| timers.lag p50 / p95 / p99 / max (ms) | " + pct(tl, 50) + " / " + pct(tl, 95) + " / " + pct(tl, 99) + " / " + (tl.length == 0 ? 0 : tl[tl.length - 1]) + " |\n"
                + "| publish lag due->ack p50 / p95 / p99 / max (ms) | " + pct(pl, 50) + " / " + pct(pl, 95) + " / " + pct(pl, 99) + " / " + (pl.length == 0 ? 0 : pl[pl.length - 1]) + " |\n\n";
    }

    private static long count(Engine e, String sql) {
        return e.tx.read(c -> { try (var st = c.createStatement(); var rs = st.executeQuery(sql)) { rs.next(); return rs.getLong(1); } });
    }

    private static long pct(long[] a, int p) {
        if (a.length == 0) return 0;
        return a[Math.min(a.length - 1, (int) Math.ceil(p / 100.0 * a.length) - 1)];
    }

    @SuppressWarnings("unused")
    private static String fmt(long[] a) { return Arrays.toString(a); }
}
