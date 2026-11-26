package trash_maker.domain;

import trash_maker.common.Json;
import trash_maker.common.ValidationError;
import trash_maker.domain.rule.CompiledRule;
import trash_maker.domain.rule.RuleParser;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

public class RuleParserTest {
    public static final String EXAMPLE = """
            { "id":"r1","version":1,"tenantId":"acme","enabled":true,"timezone":"Europe/Rome","onMissingValue":"FAIL",
              "trigger":{"mode":"ON_BECOMES_TRUE","onMissing":"FALSE","expression":{"or":[
                {"and":[{"signal":"kpi1","op":">","value":5},
                        {"dateMatches":{"source":"NOW","recurrence":{"cron":"0 0 1 * *"},"tolerance":"PT1H"}}]},
                {"signal":"kpi2","op":"=","value":true,"maxAge":"P1D"}]}},
              "templates":[
                {"id":"main","type":"SCHEDULED","action":"SEND_EMAIL",
                 "dueAt":{"from":"$signal:due_date","offset":"-P3D","atTime":"09:00"},
                 "retryPolicy":{"maxAttempts":3,"backoff":"EXPONENTIAL","initialDelay":"PT1M","multiplier":2.0,"attemptTimeout":"PT2M"},
                 "body":{"to":{"$signal":"customer_email"},"subject":"Reminder ${correlationKey}"}},
                {"id":"fallback","type":"SCHEDULED","action":"SEND_SMS",
                 "parent":{"templateId":"main","trigger":"ON_RETRIES_EXHAUSTED","delay":"PT15M"},
                 "body":{"failureReason":{"$parent":"$.lastFailure.reason","default":"unknown"}}}]}""";

    @Test
    void exampleRuleCompiles() {
        CompiledRule r = RuleParser.compile(Json.parse(EXAMPLE), RuleParser.Options.DEFAULT);
        assertThat(r.exprSignals()).containsExactlyInAnyOrder("kpi1", "kpi2");
        assertThat(r.contextSignals()).containsExactlyInAnyOrder("customer_email", "due_date");
        assertThat(r.hasNowCondition()).isTrue();
        assertThat(r.entityTimed()).isTrue();
        assertThat(r.roots()).extracting(t -> t.id()).containsExactly("main");
        assertThat(r.children().get("main")).extracting(t -> t.id()).containsExactly("fallback");
    }

    static List<ValidationError> errors(String json) { return RuleParser.validate(Json.parse(json), new RuleParser.Options(Set.of("SEND_EMAIL", "SEND_SMS"), s -> null)); }

    @Test
    void cyclesDepthAndPathsAreReported() {
        String cyc = """
                {"id":"r","tenantId":"t","trigger":{"expression":{"signal":"a","op":">","value":1}},"templates":[
                  {"id":"root","type":"IMMEDIATE","action":"SEND_EMAIL"},
                  {"id":"a","type":"IMMEDIATE","action":"SEND_EMAIL","parent":{"templateId":"b","trigger":"ON_FAILED"}},
                  {"id":"b","type":"IMMEDIATE","action":"SEND_EMAIL","parent":{"templateId":"a","trigger":"ON_FAILED"}}]}""";
        assertThat(errors(cyc)).extracting(ValidationError::message).anyMatch(m -> m.contains("cycle"));
        StringBuilder deep = new StringBuilder("""
                {"id":"r","tenantId":"t","trigger":{"expression":{"signal":"a","op":">","value":1}},"templates":[
                  {"id":"t0","type":"IMMEDIATE","action":"SEND_EMAIL"}""");
        for (int i = 1; i <= 6; i++) deep.append(",{\"id\":\"t").append(i).append("\",\"type\":\"IMMEDIATE\",\"action\":\"SEND_EMAIL\",\"parent\":{\"templateId\":\"t")
                .append(i - 1).append("\",\"trigger\":\"ON_FAILED\"}}");
        deep.append("]}");
        assertThat(errors(deep.toString())).anyMatch(e -> e.path().equals("$.templates[6].parent") && e.message().contains("depth"));
    }

    @Test
    void preciseErrorPaths() {
        String bad = """
                {"id":"r 1","tenantId":"t","timezone":"Mars/Base","trigger":{"mode":"EVERY_EVALUATION_TRUE","expression":{"signal":"a","op":">","value":1}},
                 "templates":[
                  {"id":"m","type":"SCHEDULED","action":"send_email","retryPolicy":{"maxAttempts":0,"jitter":2}},
                  {"id":"w","type":"IMMEDIATE","action":"FAX","parent":{"templateId":"m","trigger":"WHILE_IN_STATE","states":["SUCCEEDED"]}},
                  {"id":"x","type":"RECURRING","action":"SEND_SMS","every":"P1D","parent":{"templateId":"zz","trigger":"WHILE_IN_STATE","states":["FAILED"]}}]}""";
        assertThat(errors(bad)).extracting(ValidationError::path).contains(
                "$.id", "$.timezone", "$.trigger.cooldown", "$.templates[0].action", "$.templates[0].dueAt",
                "$.templates[0].retryPolicy.maxAttempts", "$.templates[0].retryPolicy.jitter", "$.templates[1].action",
                "$.templates[1].parent.states[0]", "$.templates[1].parent.trigger", "$.templates[2].parent", "$.templates[2].parent.templateId");
    }

    @Test
    void textDslAccepted() {
        String r = """
                {"id":"r","tenantId":"t","trigger":{"expressionText":"kpi1 > 5 and not kpi3 in [1,2]"},
                 "templates":[{"id":"m","type":"IMMEDIATE","action":"SEND_EMAIL"}]}""";
        assertThat(errors(r)).isEmpty();
        assertThat(errors(r.replace("kpi1 > 5", "kpi1 >> 5"))).extracting(ValidationError::path).containsExactly("$.trigger.expressionText");
    }
}
