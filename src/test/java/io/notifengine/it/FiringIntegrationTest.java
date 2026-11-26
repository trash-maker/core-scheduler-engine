package io.notifengine.it;

import io.notifengine.core.notifications.NotificationRow;
import io.notifengine.domain.notification.NotificationStatus;
import io.notifengine.support.Fx;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class FiringIntegrationTest {
    // the acceptance expression, IMMEDIATE template echoing the triggering signal
    static final String RULE = """
            {"id":"r1","timezone":"Europe/Rome",
             "trigger":{"expression":{"or":[
                {"and":[{"signal":"kpi1","op":">","value":5},
                        {"dateMatches":{"source":"NOW","recurrence":{"cron":"0 0 1 * *"},"tolerance":"PT1H"}}]},
                {"signal":"kpi2","op":"=","value":true,"maxAge":"P1D"}]}},
             "templates":[{"id":"main","type":"IMMEDIATE","action":"SEND_EMAIL",
                "body":{"kpi1":{"$signal":"kpi1","default":null},"cause":{"$trigger":"$.signal","default":"time"},"key":"${correlationKey}"}}]}""";

    // 2026-12-01 00:20 Europe/Rome (UTC+1)
    static final Instant FIRST = Instant.parse("2026-11-30T23:20:00Z");
    static final Instant MID = Instant.parse("2026-11-26T10:00:00Z");

    @Test
    void anyArrivalOrderFiresExactlyOnce() {
        String[][] signals = {
                {"a", "kpi1", "3", "-120"},  // oldest
                {"b", "kpi1", "7", "-60"},   // newest kpi1 -> true
                {"c", "kpi2", "false", "-30"}};
        List<List<Integer>> perms = List.of(List.of(0, 1, 2), List.of(0, 2, 1), List.of(1, 0, 2), List.of(1, 2, 0), List.of(2, 0, 1), List.of(2, 1, 0));
        for (List<Integer> p : perms) {
            Fx fx = new Fx(FIRST);
            fx.rule(RULE);
            for (int i : p) {
                String[] s = signals[i];
                Object v = s[2].equals("false") ? Boolean.FALSE : Integer.parseInt(s[2]);
                fx.signal(s[0], "k1", s[1], "KPI", v, FIRST.plusSeconds(Long.parseLong(s[3])));
                // duplicates of every signal are idempotent
                fx.signal(s[0], "k1", s[1], "KPI", v, FIRST.plusSeconds(Long.parseLong(s[3])));
            }
            assertThat(fx.notifications("k1")).as("order %s", p).hasSize(1);
            assertThat(fx.notifications("k1").get(0).body().get("kpi1").asInt()).isEqualTo(7);
        }
    }

    @Test
    void sameResultInOneBatch() {
        Fx fx = new Fx(FIRST);
        fx.rule(RULE);
        List<String> batch = new ArrayList<>(List.of(
                Fx.envelope("a", "k1", "kpi1", "KPI", 3, FIRST.minusSeconds(120)),
                Fx.envelope("b", "k1", "kpi1", "KPI", 7, FIRST.minusSeconds(60)),
                Fx.envelope("b", "k1", "kpi1", "KPI", 7, FIRST.minusSeconds(60)),
                Fx.envelope("c", "k2", "kpi2", "KPI", true, FIRST.minusSeconds(10))));
        Collections.shuffle(batch);
        var stats = fx.e.ingest.ingest(batch);
        assertThat(stats.duplicates()).isEqualTo(1);
        assertThat(fx.notifications("k1")).hasSize(1);
        assertThat(fx.notifications("k2")).hasSize(1);
    }

    @Test
    void timeConditionFiresWithoutInputAndReArms() {
        Fx fx = new Fx(MID);
        fx.rule(RULE);
        fx.signal("k1", "kpi1", 7);
        assertThat(fx.notifications("k1")).isEmpty();
        // jump to the 1st of the month: sweep timer evaluates the entity without any message
        fx.clock.set(Instant.parse("2026-11-30T23:00:00Z"));
        fx.runTimers();
        List<NotificationRow> n = fx.notifications("k1");
        assertThat(n).hasSize(1);
        assertThat(n.get(0).body().get("cause").asText()).isEqualTo("time"); // no $trigger for time-triggered firing
        fx.relay();
        assertThat(fx.out()).hasSize(1);
        // still inside the tolerance window: no second firing
        fx.advance(Duration.ofMinutes(30));
        fx.runTimers();
        assertThat(fx.notifications("k1")).hasSize(1);
        // window closes -> false (re-armed); next month fires again
        fx.clock.set(Instant.parse("2026-12-01T00:00:01Z"));
        fx.runTimers();
        fx.clock.set(Instant.parse("2026-12-31T23:00:00Z"));
        fx.e.maintenance.ensurePartitions(3);
        fx.runTimers();
        assertThat(fx.notifications("k1")).hasSize(2);
    }

    @Test
    void staleSignalStopsMatchingWithoutInput() {
        Fx fx = new Fx(MID);
        fx.rule(RULE);
        fx.signal("k1", "kpi2", true);
        assertThat(fx.notifications("k1")).hasSize(1);
        // maxAge P1D: RULE_EVAL timer flips the rule to false (re-arm) when kpi2 gets stale
        assertThat(fx.count("SELECT count(*) FROM timer WHERE kind='RULE_EVAL' AND correlation_key='k1'")).isEqualTo(1);
        fx.advance(Duration.ofDays(1).plusSeconds(1));
        fx.runTimers();
        assertThat(fx.count("SELECT count(*) FROM rule_state WHERE correlation_key='k1' AND NOT last_result")).isEqualTo(1);
        fx.signal("k1", "kpi2", true);
        assertThat(fx.notifications("k1")).hasSize(2);
    }

    @Test
    void onBecomesTrueFiresOnceAndReArms() {
        Fx fx = new Fx(MID);
        fx.rule(RULE);
        fx.signal("k1", "kpi2", true);
        fx.advance(Duration.ofSeconds(1));
        fx.signal("k1", "kpi2", true);
        fx.advance(Duration.ofSeconds(1));
        assertThat(fx.notifications("k1")).hasSize(1);
        fx.signal("k1", "kpi2", false);
        fx.advance(Duration.ofSeconds(1));
        fx.signal("k1", "kpi2", true);
        assertThat(fx.notifications("k1")).hasSize(2);
        assertThat(fx.notifications("k1")).extracting(NotificationRow::idempotencyKey)
                .containsExactly("acme|r1|k1|1|main", "acme|r1|k1|2|main");
    }

    @Test
    void everyEvaluationWithCooldown() {
        Fx fx = new Fx(MID);
        fx.rule("""
                {"id":"r2","trigger":{"mode":"EVERY_EVALUATION_TRUE","cooldown":"PT10M","expressionText":"temp > 30"},
                 "templates":[{"id":"t","type":"IMMEDIATE","action":"ALERT"}]}""");
        fx.signal("k", "temp", 31);
        fx.advance(Duration.ofMinutes(5));
        fx.signal("k", "temp", 32);
        assertThat(fx.notifications("k")).hasSize(1);
        fx.advance(Duration.ofMinutes(6));
        fx.signal("k", "temp", 33);
        assertThat(fx.notifications("k")).hasSize(2);
    }

    @Test
    void unreferencedSignalsAreDiscardedUnlessConfigured() {
        Fx fx = new Fx(MID);
        fx.rule(RULE);
        var s = fx.signal("k1", "unrelated", 1);
        assertThat(s.unreferenced()).isEqualTo(1);
        assertThat(fx.count("SELECT count(*) FROM signal_state")).isZero();
    }

    @Test
    void invalidMessagesGoToDlq() {
        Fx fx = new Fx(MID);
        fx.rule(RULE);
        var s = fx.e.ingest.ingest(List.of("{not json", "{\"signal_id\":\"x\"}"));
        assertThat(s.invalid()).isEqualTo(2);
        assertThat(fx.count("SELECT count(*) FROM dead_letter WHERE reason='INVALID_MESSAGE'")).isEqualTo(2);
        fx.relay();
        assertThat(fx.pub.on("notifications.dlq")).hasSize(2);
    }

    @Test
    void cancelOnEvents() {
        Fx fx = new Fx(MID);
        fx.rule("""
                {"id":"r3","trigger":{"expressionText":"overdue = true"},
                 "templates":[{"id":"t","type":"SCHEDULED","action":"REMIND","dueAt":{"from":"NOW","offset":"P1D"},"cancelOnEvents":["paid"]}]}""");
        fx.signal("k", "overdue", true);
        assertThat(fx.byTemplate("k", "t").status()).isEqualTo(NotificationStatus.SCHEDULED);
        fx.signal("k", "paid", true);
        assertThat(fx.byTemplate("k", "t").status()).isEqualTo(NotificationStatus.CANCELLED);
        fx.advance(Duration.ofDays(2));
        fx.runTimers();
        fx.relay();
        assertThat(fx.out()).isEmpty();
    }
}
