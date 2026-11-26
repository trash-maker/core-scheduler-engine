package trash_maker.it;

import com.fasterxml.jackson.databind.JsonNode;
import trash_maker.core.EngineConfig;
import trash_maker.core.notifications.NotificationRow;
import trash_maker.domain.notification.NotificationStatus;
import trash_maker.domain.notification.TimerKind;
import trash_maker.support.Fx;
import trash_maker.support.RecordingPublisher;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ReliabilityIntegrationTest {
    static final Instant T0 = Instant.parse("2026-11-26T10:00:00Z");
    static final String SCHEDULED_RULE = """
            {"id":"r","trigger":{"expressionText":"x = true"},
             "templates":[{"id":"t","type":"SCHEDULED","action":"PUSH","dueAt":{"from":"NOW","offset":"PT1M"},"body":{"k":"${correlationKey}"}}]}""";

    private static Fx withScheduled(int n, EngineConfig cfg) {
        Fx fx = new Fx(T0, cfg);
        fx.rule(SCHEDULED_RULE);
        List<String> batch = new ArrayList<>();
        for (int i = 0; i < n; i++) batch.add(Fx.envelope("s" + i, "k" + i, "x", "KPI", true, T0));
        for (int i = 0; i < n; i += 500) fx.e.ingest.ingest(batch.subList(i, Math.min(n, i + 500)));
        assertThat(fx.count("SELECT count(*) FROM timer WHERE kind='PUBLISH'")).isEqualTo(n);
        return fx;
    }

    /** Simulated kill: an Error escapes the handler, the whole claim transaction rolls back. */
    static final class Kill extends Error { Kill() { super("kill -9"); } }

    @Test
    void crashMidBatchLosesNothing() {
        Fx fx = withScheduled(50, EngineConfig.defaults());
        fx.advance(Duration.ofMinutes(1));
        AtomicInteger seen = new AtomicInteger();
        fx.e.timers.setFaultInjector(t -> { if (seen.incrementAndGet() == 25) throw new Kill(); });
        assertThatThrownBy(fx.e.timers::runOnce).isInstanceOf(Kill.class);
        // nothing of the half-processed batch is visible; all timers are back
        assertThat(fx.count("SELECT count(*) FROM timer WHERE kind='PUBLISH'")).isEqualTo(50);
        assertThat(fx.count("SELECT count(*) FROM outbox")).isZero();
        assertThat(fx.count("SELECT count(*) FROM notification WHERE status='PUBLISHED'")).isZero();
        fx.e.timers.setFaultInjector(null);
        fx.runTimers();
        // relay crash: broker down -> rows stay, no attempt consumed
        fx.pub.failRemaining.set(10_000);
        fx.e.relay.relayOnce();
        assertThat(fx.count("SELECT count(*) FROM outbox")).isEqualTo(50);
        fx.pub.failRemaining.set(0);
        List<RecordingPublisher.Msg> msgs = fx.relay();
        assertThat(msgs).hasSize(50);
        assertThat(msgs.stream().map(m -> m.payload().get("notification_id").asText()).distinct()).hasSize(50);
        assertThat(msgs).allMatch(m -> m.payload().get("attempt").asInt() == 1);
    }

    @Test
    void redeliveredSignalBatchIsIdempotent() {
        Fx fx = new Fx(T0);
        fx.rule(SCHEDULED_RULE);
        List<String> batch = List.of(Fx.envelope("a", "k1", "x", "KPI", true, T0), Fx.envelope("b", "k2", "x", "KPI", true, T0));
        fx.e.ingest.ingest(batch);
        // consumer crashed after DB commit but before the offset commit: same batch again
        var again = fx.e.ingest.ingest(batch);
        assertThat(again.duplicates()).isEqualTo(2);
        assertThat(fx.count("SELECT count(*) FROM notification")).isEqualTo(2);
    }

    @Test
    void publishAckedButCommitLostOnlyDuplicatesOnKafkaSide() {
        Fx fx = new Fx(T0);
        fx.rule(SCHEDULED_RULE.replace("SCHEDULED\",\"action\":\"PUSH\",\"dueAt\":{\"from\":\"NOW\",\"offset\":\"PT1M\"}", "IMMEDIATE\",\"action\":\"PUSH\""));
        fx.signal("k", "x", true);
        // the broker accepts the message but the relay transaction fails afterwards
        AtomicBoolean first = new AtomicBoolean(true);
        var flaky = new trash_maker.core.dispatch.OutboxRelay(fx.e.tx, new trash_maker.core.dispatch.OutboxRepository(), fx.e.timerRepository,
                fx.e.notifications.repository(), (topic, key, headers, payload) -> {
                    var f = fx.pub.publish(topic, key, headers, payload);
                    if (first.getAndSet(false)) throw new Kill();
                    return f;
                }, EngineConfig.defaults(), trash_maker.core.ports.EngineMetrics.NOOP, fx.clock);
        assertThatThrownBy(flaky::relayOnce).isInstanceOf(Kill.class);
        fx.relay();
        List<RecordingPublisher.Msg> out = fx.out();
        assertThat(out).hasSize(2);
        // a duplicate carries the same notification_id + attempt: deduplicable by actuators
        assertThat(out.stream().map(m -> m.payload().get("notification_id").asText() + "/" + m.payload().get("attempt").asText()).distinct()).hasSize(1);
        assertThat(fx.count("SELECT count(*) FROM notification WHERE attempt=1 AND status='PUBLISHED'")).isEqualTo(1);
    }

    @Test
    void concurrentWorkersNeverRunTheSameTimerOrPublishTheSameAttemptTwice() throws Exception {
        int n = 3000;
        Fx fx = withScheduled(n, EngineConfig.defaults().withTimerBatchSize(100));
        fx.advance(Duration.ofMinutes(1));
        ExecutorService pool = Executors.newFixedThreadPool(6);
        List<Future<?>> fs = new ArrayList<>();
        for (int w = 0; w < 4; w++) fs.add(pool.submit(() -> { while (fx.e.timers.runOnce() > 0) { } }));
        for (int r = 0; r < 2; r++) fs.add(pool.submit(() -> {
            for (int i = 0; i < 400; i++) { if (fx.e.relay.relayOnce().claimed() == 0) sleep(20); }
        }));
        for (Future<?> f : fs) f.get();
        pool.shutdown();
        fx.relay();
        List<RecordingPublisher.Msg> out = fx.out();
        Set<String> unique = new HashSet<>();
        for (RecordingPublisher.Msg m : out) unique.add(m.payload().get("notification_id").asText() + "/" + m.payload().get("attempt").asText());
        assertThat(out).hasSize(n);
        assertThat(unique).hasSize(n);
        assertThat(fx.count("SELECT count(*) FROM notification WHERE status='PUBLISHED' AND attempt=1")).isEqualTo(n);
        assertThat(fx.count("SELECT count(*) FROM notification_history WHERE to_status='PUBLISHED'")).isEqualTo(n);
    }

    @Test
    void failingHandlerDoesNotBlockItsBatchAndEndsInTimerDead() {
        Fx fx = withScheduled(20, EngineConfig.defaults());
        NotificationRow poisoned = fx.notifications("k7").get(0);
        fx.e.timers.setFaultInjector(t -> {
            if (poisoned.id().equals(t.notificationId())) throw new IllegalStateException("boom");
        });
        fx.advance(Duration.ofMinutes(1));
        fx.runTimers();
        assertThat(fx.count("SELECT count(*) FROM notification WHERE status='PUBLISHED'")).isEqualTo(19);
        for (int i = 0; i < 10; i++) { fx.advance(Duration.ofMinutes(10)); fx.runTimers(); }
        assertThat(fx.count("SELECT count(*) FROM timer_dead WHERE notification_id='" + poisoned.id() + "' AND failures=5")).isEqualTo(1);
        assertThat(fx.count("SELECT count(*) FROM timer WHERE notification_id='" + poisoned.id() + "'")).isZero();
        // runbook: fix the cause, requeue
        fx.e.timers.setFaultInjector(null);
        long deadId = fx.count("SELECT id FROM timer_dead");
        fx.e.tx.run(c -> fx.e.timerRepository.requeueDead(c, List.of(deadId), fx.now()));
        fx.runTimers();
        assertThat(fx.get(poisoned.id()).status()).isEqualTo(NotificationStatus.PUBLISHED);
    }

    @Test
    void retentionDeletesTerminalAndDropsOldPartitions() {
        Instant old = Instant.parse("2026-08-10T10:00:00Z");
        Fx fx = new Fx(old);
        fx.rule(SCHEDULED_RULE.replace("SCHEDULED\",\"action\":\"PUSH\",\"dueAt\":{\"from\":\"NOW\",\"offset\":\"PT1M\"}", "IMMEDIATE\",\"action\":\"PUSH\""));
        fx.signal("k1", "x", true);
        NotificationRow n1 = fx.notifications("k1").get(0);
        fx.relay();
        fx.result(n1.id(), 1, "PUSH", "SUCCESS", null);
        fx.clock.set(Instant.parse("2026-11-26T10:00:00Z"));
        fx.e.maintenance.ensurePartitions(3);
        var report = fx.e.maintenance.retention();
        assertThat(report.notificationsDeleted()).isEqualTo(1);
        assertThat(report.partitionsDropped()).contains("notification_p2026_08");
        assertThat(fx.count("SELECT count(*) FROM pg_class WHERE relname='notification_history_p2026_08'")).isZero();
    }

    @Test
    void hotQueriesUseIndexes() {
        Fx fx = withScheduled(200, EngineConfig.defaults());
        fx.e.tx.run(c -> { try (var st = c.createStatement()) { st.execute("ANALYZE"); } });
        UUID id = fx.notifications("k1").get(0).id();
        String claim = plan("EXPLAIN DELETE FROM timer WHERE id IN (SELECT id FROM timer WHERE due_at <= now() ORDER BY due_at LIMIT 100 FOR UPDATE SKIP LOCKED) RETURNING *", fx);
        assertThat(claim).contains("timer_due_idx");
        String byId = plan("EXPLAIN SELECT * FROM notification WHERE id='" + id + "' AND created_at='" + trash_maker.common.Uuid7.timestamp(id) + "'", fx);
        assertThat(byId).doesNotContain("Append"); // pruned to a single partition
        String search = plan("EXPLAIN SELECT * FROM notification WHERE tenant_id='acme' AND correlation_key='k1' ORDER BY id DESC LIMIT 50", fx);
        assertThat(search).contains("notification_p").containsIgnoringCase("index");
        String ruleEval = plan("EXPLAIN SELECT * FROM timer WHERE kind='RULE_EVAL' AND tenant_id='acme' AND rule_id='r' AND correlation_key='k1'", fx);
        assertThat(ruleEval).contains("timer_rule_eval_uq");
    }

    private static String plan(String sql, Fx fx) {
        JsonNode rows = fx.e.tx.read(c -> {
            try (var st = c.createStatement()) {
                st.execute("SET enable_seqscan = off");
                var arr = trash_maker.common.Json.array();
                try (var rs = st.executeQuery(sql)) { while (rs.next()) arr.add(rs.getString(1)); }
                st.execute("RESET enable_seqscan");
                return arr;
            }
        });
        return rows.toString();
    }

    private static void sleep(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }

    @SuppressWarnings("unused")
    private static TimerKind unused() { return TimerKind.PUBLISH; }
}
