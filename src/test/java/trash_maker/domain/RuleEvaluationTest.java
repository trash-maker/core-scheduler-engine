package trash_maker.domain;

import org.junit.jupiter.api.Test;
import trash_maker.common.Json;
import trash_maker.domain.body.BodySchemaValidator;
import trash_maker.domain.evaluation.RuleEvaluation;
import trash_maker.domain.rule.CompiledRule;
import trash_maker.domain.rule.RuleParser;
import trash_maker.domain.signal.SignalKind;
import trash_maker.domain.signal.SignalValue;
import trash_maker.domain.signal.Value;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** The single evaluation pipeline (expression -> decision -> plan -> next state / timer) is a pure function. */
class RuleEvaluationTest {
    static final Instant T = Instant.parse("2026-01-01T10:00:00Z");
    static final CompiledRule RULE = RuleParser.compile(Json.parse("""
            {"id":"r","tenantId":"t","trigger":{"expressionText":"x = true maxAge PT1H"},
             "templates":[{"id":"a","type":"IMMEDIATE","action":"A","body":{"k":"${correlationKey}"}}]}"""), RuleParser.Options.DEFAULT);

    static RuleEvaluation.Result run(boolean x, RuleEvaluation.PriorState prior, Instant now) {
        return RuleEvaluation.run(RULE, "t", "k", Map.of("x", new SignalValue("x", SignalKind.KPI, new Value.Bool(x), T)), null, now,
                prior, null, BodySchemaValidator.NONE);
    }

    @Test
    void firesOncePerEdgeAndProducesNextStateAndTimer() {
        RuleEvaluation.Result r1 = run(true, RuleEvaluation.PriorState.INITIAL, T);
        assertThat(r1.decision().fire()).isTrue();
        assertThat(r1.plan().notifications()).singleElement().satisfies(p -> assertThat(p.body().get("k").asText()).isEqualTo("k"));
        RuleEvaluation.PriorState s1 = r1.nextState(RuleEvaluation.PriorState.INITIAL, T);
        assertThat(s1.edgeSeq()).isEqualTo(1);
        assertThat(s1.lastFiredAt()).isEqualTo(T);
        assertThat(r1.entityTimer(RULE.entityTimed()).at()).isEqualTo(T.plus(Duration.ofHours(1))); // staleness instant
        RuleEvaluation.Result r2 = run(true, s1, T.plusSeconds(60));
        assertThat(r2.decision().fire()).isFalse();
        assertThat(r2.nextState(s1, T.plusSeconds(60)).lastFiredAt()).isEqualTo(T);
        RuleEvaluation.Result stale = run(true, s1, T.plus(Duration.ofHours(2)));
        assertThat(stale.decision().newLastResult()).isFalse();
        assertThat(stale.entityTimer(true)).isEqualTo(RuleEvaluation.EntityTimer.DELETE);
        assertThat(stale.entityTimer(false)).isEqualTo(RuleEvaluation.EntityTimer.NONE);
    }
}
