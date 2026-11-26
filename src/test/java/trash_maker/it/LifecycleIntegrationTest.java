package trash_maker.it;

import com.fasterxml.jackson.databind.JsonNode;
import trash_maker.common.Json;
import trash_maker.core.notifications.NotificationRow;
import trash_maker.domain.RuleParserTest;
import trash_maker.domain.evaluation.RuleEvaluation;
import trash_maker.domain.notification.NotificationStatus;
import trash_maker.domain.rule.CompiledRule;
import trash_maker.domain.signal.SignalKind;
import trash_maker.domain.signal.SignalValue;
import trash_maker.domain.signal.Value;
import trash_maker.support.Fx;
import trash_maker.support.RecordingPublisher;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static trash_maker.domain.notification.NotificationStatus.*;
import static org.assertj.core.api.Assertions.assertThat;

class LifecycleIntegrationTest {
    static final Instant T0 = Instant.parse("2026-11-26T10:00:00Z");

    static String exampleRule() { return RuleParserTest.EXAMPLE.replace("\"tenantId\":\"acme\",", ""); }

    private static Fx exampleFired() {
        Fx fx = new Fx(T0);
        fx.rule(exampleRule());
        fx.signal("cust-1", "customer_email", "a@b.c");
        fx.date("cust-1", "due_date", "2026-12-10");
        fx.signal("cust-1", "kpi2", true);
        return fx;
    }

    @Test
    void exampleRuleRetriesExhaustsAndActivatesFallback() {
        Fx fx = exampleFired();
        NotificationRow main = fx.byTemplate("cust-1", "main");
        NotificationRow fallback = fx.byTemplate("cust-1", "fallback");
        assertThat(main.status()).isEqualTo(SCHEDULED);
        // due = due_date - 3 days at 09:00 Europe/Rome
        assertThat(main.dueAt()).isEqualTo(Instant.parse("2026-12-07T08:00:00Z"));
        assertThat(fallback.status()).isEqualTo(WAITING_ON_PARENT);

        fx.clock.set(main.dueAt());
        fx.runTimers();
        List<RecordingPublisher.Msg> m1 = fx.relay();
        assertThat(m1).hasSize(1);
        JsonNode p = m1.get(0).payload();
        assertThat(p.get("action").asText()).isEqualTo("SEND_EMAIL");
        assertThat(m1.get(0).headers()).containsEntry("action", "SEND_EMAIL");
        assertThat(m1.get(0).key()).isEqualTo("acme:" + main.id());
        assertThat(p.get("body").toString()).isEqualTo("{\"to\":\"a@b.c\",\"subject\":\"Reminder cust-1\"}");

        // attempt 1: explicit failure -> retry in 1 min
        fx.result(main.id(), 1, "SEND_EMAIL", "FAILURE", "smtp 451");
        assertThat(fx.get(main.id()).status()).isEqualTo(RETRY_SCHEDULED);
        fx.advance(Duration.ofMinutes(1));
        fx.runTimers();
        assertThat(fx.relay()).hasSize(1);
        // attempt 2: no outcome within attemptTimeout (PT2M) -> TIMEOUT -> retry in 2 min
        fx.advance(Duration.ofMinutes(2));
        fx.runTimers();
        NotificationRow afterTimeout = fx.get(main.id());
        assertThat(afterTimeout.status()).isEqualTo(RETRY_SCHEDULED);
        assertThat(afterTimeout.lastFailure().reason()).isEqualTo("TIMEOUT");
        fx.advance(Duration.ofMinutes(2));
        fx.runTimers();
        assertThat(fx.relay()).hasSize(1);
        // attempt 3 fails -> exhausted -> fallback activated with delay 15 min, body from $parent
        fx.result(main.id(), 3, "SEND_EMAIL", "FAILURE", "mailbox full");
        assertThat(fx.get(main.id()).status()).isEqualTo(RETRIES_EXHAUSTED);
        NotificationRow fb = fx.get(fallback.id());
        assertThat(fb.status()).isEqualTo(SCHEDULED);
        assertThat(fb.body().get("failureReason").asText()).isEqualTo("mailbox full");
        fx.advance(Duration.ofMinutes(15));
        fx.runTimers();
        List<RecordingPublisher.Msg> sms = fx.relay();
        assertThat(sms).hasSize(1);
        assertThat(sms.get(0).payload().get("action").asText()).isEqualTo("SEND_SMS");
        assertThat(fx.history(main.id())).containsExactly("CREATED", "SCHEDULED", "PUBLISHED", "FAILED", "RETRY_SCHEDULED",
                "PUBLISHED", "FAILED", "RETRY_SCHEDULED", "PUBLISHED", "FAILED", "RETRIES_EXHAUSTED");
    }

    @Test
    void successStopsRetriesAndSkipsImpossibleChildren() {
        Fx fx = exampleFired();
        NotificationRow main = fx.byTemplate("cust-1", "main");
        fx.clock.set(main.dueAt());
        fx.runTimers();
        fx.relay();
        fx.result(main.id(), 1, "SEND_EMAIL", "SUCCESS", null);
        assertThat(fx.get(main.id()).status()).isEqualTo(SUCCEEDED);
        assertThat(fx.byTemplate("cust-1", "fallback").status()).isEqualTo(SKIPPED);
        assertThat(fx.count("SELECT count(*) FROM timer WHERE notification_id='" + main.id() + "'")).isZero();
        // duplicate outcome is idempotent
        fx.result(main.id(), 1, "SEND_EMAIL", "SUCCESS", null);
        fx.result(main.id(), 1, "SEND_EMAIL", "FAILURE", "late");
        assertThat(fx.get(main.id()).status()).isEqualTo(SUCCEEDED);
        fx.advance(Duration.ofHours(1));
        fx.runTimers();
        assertThat(fx.relay()).isEmpty();
    }

    @Test
    void lateSuccessAfterTimeoutAndActionMismatch() {
        Fx fx = exampleFired();
        NotificationRow main = fx.byTemplate("cust-1", "main");
        fx.clock.set(main.dueAt());
        fx.runTimers();
        fx.relay();
        fx.advance(Duration.ofMinutes(2));
        fx.runTimers(); // timeout -> RETRY_SCHEDULED
        assertThat(fx.get(main.id()).status()).isEqualTo(RETRY_SCHEDULED);
        fx.result(main.id(), 1, "SEND_SMS", "SUCCESS", null); // wrong action: ignored
        assertThat(fx.get(main.id()).status()).isEqualTo(RETRY_SCHEDULED);
        fx.result(main.id(), 1, "SEND_EMAIL", "FAILURE", "late failure"); // superseded: ignored
        assertThat(fx.get(main.id()).status()).isEqualTo(RETRY_SCHEDULED);
        fx.result(main.id(), 1, "SEND_EMAIL", "SUCCESS", null); // late success accepted
        assertThat(fx.get(main.id()).status()).isEqualTo(SUCCEEDED);
        fx.advance(Duration.ofMinutes(5));
        fx.runTimers();
        assertThat(fx.relay()).isEmpty(); // retry timer was cancelled
    }

    @Test
    void manualRetriggerResetsAttemptCounter() {
        Fx fx = new Fx(T0);
        fx.rule("""
                {"id":"r","trigger":{"expressionText":"x = true"},
                 "templates":[{"id":"t","type":"IMMEDIATE","action":"PUSH","retryPolicy":{"maxAttempts":2,"initialDelay":"PT10S"}}]}""");
        fx.signal("k", "x", true);
        NotificationRow n = fx.byTemplate("k", "t");
        fx.relay();
        fx.result(n.id(), 1, "PUSH", "FAILURE", "a");
        fx.advance(Duration.ofSeconds(10));
        fx.runTimers();
        fx.relay();
        fx.result(n.id(), 2, "PUSH", "FAILURE", "b");
        assertThat(fx.get(n.id()).status()).isEqualTo(RETRIES_EXHAUSTED);
        fx.e.tx.run(c -> fx.e.notifications.retrigger(c, "acme", n.id(), "ops", fx.now()));
        List<RecordingPublisher.Msg> m = fx.relay();
        assertThat(m.get(0).payload().get("attempt").asInt()).isEqualTo(3); // unique per message, actuators dedupe on id+attempt
        fx.result(n.id(), 3, "PUSH", "FAILURE", "c");
        assertThat(fx.get(n.id()).status()).isEqualTo(RETRY_SCHEDULED); // fresh retry budget
    }

    @Test
    void whileInStateRunsOnlyWhileParentIsInErrorAndStopsOnRecovery() {
        Fx fx = new Fx(T0);
        fx.rule("""
                {"id":"r","trigger":{"expressionText":"x = true"},
                 "templates":[
                  {"id":"p","type":"IMMEDIATE","action":"PUSH","retryPolicy":{"maxAttempts":5,"initialDelay":"PT1H"}},
                  {"id":"nag","type":"RECURRING","action":"OPS_ALERT","every":"PT10M",
                   "parent":{"templateId":"p","trigger":"WHILE_IN_STATE","states":["FAILED","RETRY_SCHEDULED","RETRIES_EXHAUSTED"],"maxOccurrences":10},
                   "body":{"n":{"$builtin":"occurrence.index"},"why":{"$parent":"$.lastFailure.reason"},"state":"${parent.status}"}}]}""");
        fx.signal("k", "x", true);
        NotificationRow p = fx.byTemplate("k", "p");
        fx.relay();
        assertThat(fx.count("SELECT count(*) FROM recurring_series WHERE status='WAITING'")).isEqualTo(1);
        fx.result(p.id(), 1, "PUSH", "FAILURE", "down");
        fx.runTimers(); // first occurrence immediately (delay 0)
        fx.advance(Duration.ofMinutes(10));
        fx.runTimers();
        List<RecordingPublisher.Msg> alerts = fx.relay().stream().filter(m -> m.payload().get("action").asText().equals("OPS_ALERT")).toList();
        assertThat(alerts).hasSize(2);
        assertThat(alerts.get(1).payload().get("body").toString()).isEqualTo("{\"n\":2,\"why\":\"down\",\"state\":\"RETRY_SCHEDULED\"}");
        assertThat(alerts.get(1).payload().get("occurrence_index").asInt()).isEqualTo(2);
        // parent recovers (late success) -> series closed PARENT_RECOVERED, no more occurrences
        fx.result(p.id(), 1, "PUSH", "SUCCESS", null);
        assertThat(fx.query("SELECT close_reason FROM recurring_series").get(0).asText()).isEqualTo("PARENT_RECOVERED");
        fx.advance(Duration.ofMinutes(30));
        fx.runTimers();
        assertThat(fx.relay()).isEmpty();
    }

    @Test
    void whileInStateStopsAtLimit() {
        Fx fx = new Fx(T0);
        fx.rule("""
                {"id":"r","trigger":{"expressionText":"x = true"},
                 "templates":[
                  {"id":"p","type":"IMMEDIATE","action":"PUSH"},
                  {"id":"nag","type":"RECURRING","action":"OPS_ALERT","every":"PT10M",
                   "parent":{"templateId":"p","trigger":"WHILE_IN_STATE","states":["RETRIES_EXHAUSTED"],"maxOccurrences":3}}]}""");
        fx.signal("k", "x", true);
        NotificationRow p = fx.byTemplate("k", "p");
        fx.relay();
        fx.result(p.id(), 1, "PUSH", "PERMANENT_FAILURE", "invalid token");
        for (int i = 0; i < 6; i++) { fx.runTimers(); fx.advance(Duration.ofMinutes(10)); }
        assertThat(fx.relay().stream().filter(m -> m.payload().get("action").asText().equals("OPS_ALERT"))).hasSize(3);
        assertThat(fx.query("SELECT close_reason FROM recurring_series").get(0).asText()).isEqualTo("LIMIT_REACHED");
    }

    @Test
    void topLevelRecurringSeriesWithUntilAndCancel() {
        Fx fx = new Fx(T0);
        fx.rule("""
                {"id":"r","timezone":"Europe/Rome","trigger":{"expressionText":"subscribed = true"},
                 "templates":[{"id":"daily","type":"RECURRING","action":"DIGEST","cron":"0 9 * * *","until":"P3D","cancelOnEvents":["unsubscribed"],
                   "body":{"i":"${occurrence.index}"}}]}""");
        fx.signal("k", "subscribed", true);
        for (int d = 0; d < 2; d++) { fx.advance(Duration.ofDays(1)); fx.runTimers(); }
        assertThat(fx.relay()).hasSize(2);
        fx.signal("k", "unsubscribed", true);
        fx.advance(Duration.ofDays(1));
        fx.runTimers();
        assertThat(fx.relay()).isEmpty();
        assertThat(fx.query("SELECT close_reason FROM recurring_series").get(0).asText()).isEqualTo("CANCELLED");
    }

    @Test
    void recurringMisfireIsSkipped() {
        Fx fx = new Fx(T0);
        fx.rule("""
                {"id":"r","trigger":{"expressionText":"on = true"},
                 "templates":[{"id":"s","type":"RECURRING","action":"TICK","every":"PT1H","maxOccurrences":5,"misfirePolicy":"SKIP","misfireGrace":"PT5M"}]}""");
        fx.signal("k", "on", true);
        fx.runTimers();
        assertThat(fx.relay()).hasSize(1);
        fx.advance(Duration.ofMinutes(70)); // occurrence at +1h is 10 minutes late -> SKIPPED, next at +2h
        fx.runTimers();
        assertThat(fx.relay()).isEmpty();
        assertThat(fx.count("SELECT count(*) FROM notification WHERE status='SKIPPED' AND status_reason='MISFIRE'")).isEqualTo(1);
        fx.advance(Duration.ofMinutes(50));
        fx.runTimers();
        assertThat(fx.relay()).hasSize(1);
    }

    @Test
    void failedResolutionGoesToDlqWithNoPartialSet() {
        Fx fx = new Fx(T0);
        fx.rule("""
                {"id":"r","trigger":{"expressionText":"x = true"},
                 "templates":[{"id":"a","type":"IMMEDIATE","action":"A","body":{"ok":"fine"}},
                              {"id":"b","type":"IMMEDIATE","action":"B","body":{"to":{"$signal":"email"}}}]}""");
        fx.signal("k", "x", true);
        assertThat(fx.notifications("k")).isEmpty();
        assertThat(fx.count("SELECT count(*) FROM dead_letter WHERE reason='BODY_RESOLUTION_FAILED' AND payload IS NOT NULL")).isEqualTo(1);
        assertThat(fx.relay()).extracting(RecordingPublisher.Msg::topic).containsExactly("notifications.dlq");
    }

    @Test
    void ruleEditsDoNotChangeStoredBodies() {
        Fx fx = new Fx(T0);
        String v1 = """
                {"id":"r","trigger":{"expressionText":"x = true"},
                 "templates":[{"id":"a","type":"SCHEDULED","action":"A","dueAt":{"from":"NOW","offset":"PT1H"},"body":{"msg":"v1 ${signal.name}"}},
                  {"id":"c","type":"IMMEDIATE","action":"C","parent":{"templateId":"a","trigger":"ON_PUBLISHED"},"body":{"m":"child v1"}}]}""";
        fx.rule(v1);
        fx.signal("k", "name", "Ann");
        fx.signal("k", "x", true);
        fx.e.rules.update("acme", "r", Json.parse(v1.replace("v1", "v2")), "test");
        fx.signal("k", "name", "Bob");
        fx.advance(Duration.ofHours(1));
        fx.runTimers();
        List<RecordingPublisher.Msg> m = fx.relay();
        assertThat(m).extracting(x -> x.payload().get("body").toString())
                .containsExactlyInAnyOrder("{\"msg\":\"v1 Ann\"}", "{\"m\":\"child v1\"}");
        assertThat(m.get(0).payload().get("rule_version").asInt()).isEqualTo(1);
    }

    @Test
    void dryRunEqualsRealProcessing() {
        Fx fx = new Fx(T0);
        fx.rule(exampleRule());
        CompiledRule rule = fx.e.registry.rule("acme", "r1");
        Map<String, SignalValue> s = Map.of(
                "customer_email", new SignalValue("customer_email", SignalKind.KPI, new Value.Str("a@b.c"), T0),
                "due_date", new SignalValue("due_date", SignalKind.DATE, Value.Date.parse("2026-12-10"), T0),
                "kpi2", new SignalValue("kpi2", SignalKind.KPI, new Value.Bool(true), T0));
        RuleEvaluation.Result dry = fx.e.dryRun.run(rule, "cust-1", s, null, T0, false, 0, null, Map.of());
        assertThat(dry.evaluation().nodes().get("$.trigger.expression.or[1]").value().name()).isEqualTo("TRUE");
        assertThat(dry.decision().fire()).isTrue();
        fx.signal("cust-1", "customer_email", "a@b.c");
        fx.date("cust-1", "due_date", "2026-12-10");
        fx.signal("cust-1", "kpi2", true);
        NotificationRow real = fx.byTemplate("cust-1", "main");
        var planned = dry.plan().notifications().stream().filter(p -> p.templateId().equals("main")).findFirst().orElseThrow();
        assertThat(planned.body()).isEqualTo(real.body());
        assertThat(planned.dueAt()).isEqualTo(real.dueAt());
        assertThat(planned.idempotencyKey()).isEqualTo(real.idempotencyKey());
        assertThat(planned.initialStatus()).isEqualTo(NotificationStatus.CREATED);
    }

    @Test
    void expiresBeforePublish() {
        Fx fx = new Fx(T0);
        fx.rule("""
                {"id":"r","trigger":{"expressionText":"x = true"},
                 "templates":[{"id":"a","type":"SCHEDULED","action":"A","dueAt":{"from":"NOW","offset":"PT2H"},"expiresAt":{"from":"NOW","offset":"PT1H"}},
                              {"id":"e","type":"IMMEDIATE","action":"E","parent":{"templateId":"a","trigger":"ON_EXPIRED"}}]}""");
        fx.signal("k", "x", true);
        fx.advance(Duration.ofHours(1));
        fx.runTimers();
        assertThat(fx.byTemplate("k", "a").status()).isEqualTo(EXPIRED);
        assertThat(fx.byTemplate("k", "e").status()).isEqualTo(PUBLISHED);
        fx.advance(Duration.ofHours(2));
        fx.runTimers();
        assertThat(fx.relay()).extracting(m -> m.payload().get("action").asText()).containsExactly("E");
    }
}
