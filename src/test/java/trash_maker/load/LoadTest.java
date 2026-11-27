package trash_maker.load;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import trash_maker.common.Json;
import trash_maker.core.Engine;
import trash_maker.support.Fx;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import javax.sql.DataSource;

/**
 * Load test (opt-in: ./mvnw verify -Pload [-Dload.kafka=true]). The Quarkus application runs its real timer workers
 * and outbox relays (roles timers,relay) on the real clock against PostgreSQL.
 * A: N notifications due uniformly over a window (sustained rate). B: M notifications due at the same instant.
 */
@Tag("load")
@QuarkusTest
@TestProfile(LoadProfile.class)
class LoadTest {
    static final int RATE = Integer.getInteger("load.rate", 1000);
    static final int SECONDS = Integer.getInteger("load.seconds", 60);
    static final int BURST = Integer.getInteger("load.burst", 20000);

    @Inject Engine e;
    @Inject LoadSink sink;
    @Inject LagMetrics lag;
    @Inject DataSource ds;

    @Test
    void sustainedAndBurst() throws Exception {
        StringBuilder report = new StringBuilder("# Load test report" + (LoadProfile.KAFKA ? " (Kafka)" : "") + "\n\nGenerated ")
                .append(Instant.now()).append(" on ").append(Runtime.getRuntime().availableProcessors())
                .append(" vCPU; Quarkus app (roles timers,relay; JTA transactions); PostgreSQL 16 (Testcontainers, fsync=off); broker: ")
                .append(LoadProfile.KAFKA ? "Kafka 3.8 (Testcontainers)" : "in-memory sink").append("; timer workers ")
                .append(Integer.getInteger("load.workers", 4)).append(", relays ").append(Integer.getInteger("load.relays", 2)).append(".\n\n");
        report.append(run("A. sustained " + RATE + "/s for " + SECONDS + "s", RATE * SECONDS, Duration.ofSeconds(SECONDS)));
        report.append(run("B. burst " + BURST + " due at the same instant", BURST, Duration.ZERO));
        System.out.println(report);
        Files.createDirectories(Path.of("target"));
        Files.writeString(Path.of("target/load-report" + (LoadProfile.KAFKA ? "-kafka" : "") + ".md"), report.toString());
    }

    private String run(String name, int n, Duration window) throws Exception {
        Fx.truncateAll(ds);
        e.registry.reloadAll();
        e.maintenance.ensurePartitions(3);
        sink.reset();
        lag.timerLag.clear();
        e.rules.create("acme", Json.parse("""
                {"id":"load","trigger":{"expressionText":"go = true"},
                 "templates":[{"id":"t","type":"SCHEDULED","action":"PUSH","dueAt":{"from":"$signal:due"},
                   "body":{"user":"${correlationKey}","msg":"hello ${signal.name}"}}]}"""), "load");
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
        long scheduled = e.db.read(c -> { try (var st = c.createStatement(); var rs = st.executeQuery("SELECT count(*) FROM notification WHERE status='SCHEDULED'")) { rs.next(); return rs.getLong(1); } });
        if (Instant.now().isAfter(start)) throw new IllegalStateException("preload overran the start instant; lower load.rate");
        Instant deadline = start.plus(window).plusSeconds(120);
        while (sink.count.get() < n && Instant.now().isBefore(deadline)) Thread.sleep(200);
        Instant end = Instant.now();
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

    private static long pct(long[] a, int p) {
        if (a.length == 0) return 0;
        return a[Math.min(a.length - 1, (int) Math.ceil(p / 100.0 * a.length) - 1)];
    }
}
