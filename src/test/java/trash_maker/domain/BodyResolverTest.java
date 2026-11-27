package trash_maker.domain;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;
import trash_maker.common.Json;
import trash_maker.common.Validation.ValidationError;
import trash_maker.domain.body.BodyResolver.BodyContext;
import trash_maker.domain.body.BodyResolver.BodyResolutionException;
import trash_maker.domain.body.BodyResolver;
import trash_maker.domain.rule.Rule.Enums.MissingValuePolicy;
import trash_maker.domain.signal.Signals.SignalKind;
import trash_maker.domain.signal.Signals.SignalValue;
import trash_maker.domain.signal.Signals.Value;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BodyResolverTest {
    static final Instant NOW = Instant.parse("2024-05-01T10:00:00Z");
    static final Map<String, SignalValue> SIGNALS = Map.of(
            "email", new SignalValue("email", SignalKind.KPI, new Value.Str("a@b.c"), NOW),
            "amount", new SignalValue("amount", SignalKind.KPI, new Value.Num(new BigDecimal("1234.5")), NOW),
            "vip", new SignalValue("vip", SignalKind.KPI, new Value.Bool(true), NOW),
            "due", new SignalValue("due", SignalKind.DATE, Value.Date.parse("2024-05-20"), NOW));
    static final JsonNode TRIGGER = Json.parse("{\"signal\":\"kpi1\",\"value\":7}");
    static final JsonNode PARENT = Json.parse("{\"status\":\"RETRIES_EXHAUSTED\",\"lastFailure\":{\"reason\":\"bounce\"}}");

    static BodyContext ctx(JsonNode trigger, JsonNode parent) {
        return new BodyContext(SIGNALS, trigger, parent, false, "cust-1", NOW, ZoneId.of("Europe/Rome"), 3, false, "r1", "main");
    }

    static JsonNode resolve(String tpl, MissingValuePolicy p) { return BodyResolver.resolve(Json.parse(tpl), ctx(TRIGGER, PARENT), p); }

    @Test
    void referencesTypesAndTemplates() {
        JsonNode b = resolve("""
                {"to":{"$signal":"email"},"amount":{"$signal":"amount"},"vip":{"$signal":"vip"},"static":[1,"x",{"k":true}],
                 "cause":{"$trigger":"$.signal"},"why":{"$parent":"$.lastFailure.reason"},
                 "subject":"Reminder ${correlationKey} due ${signal.due|date:'dd/MM/yyyy'} for ${signal.amount|number:'#,##0.00'}",
                 "upper":{"$signal":"email","transform":"upper"},"idx":{"$builtin":"occurrence.index"},
                 "first":{"$coalesce":[{"$signal":"nope"},{"$signal":"email"}]},"esc":"$${literal}","lit":{"$literal":{"$signal":"x"}}}""",
                MissingValuePolicy.FAIL);
        assertThat(b.get("to").asText()).isEqualTo("a@b.c");
        assertThat(b.get("amount").isNumber()).isTrue();
        assertThat(b.get("vip").booleanValue()).isTrue();
        assertThat(b.get("cause").asText()).isEqualTo("kpi1");
        assertThat(b.get("why").asText()).isEqualTo("bounce");
        assertThat(b.get("subject").asText()).isEqualTo("Reminder cust-1 due 20/05/2024 for 1,234.50");
        assertThat(b.get("upper").asText()).isEqualTo("A@B.C");
        assertThat(b.get("idx").asInt()).isEqualTo(3);
        assertThat(b.get("first").asText()).isEqualTo("a@b.c");
        assertThat(b.get("esc").asText()).isEqualTo("${literal}");
        assertThat(b.get("lit").toString()).isEqualTo("{\"$signal\":\"x\"}");
        assertThat(b.get("static").toString()).isEqualTo("[1,\"x\",{\"k\":true}]");
    }

    @Test
    void missingValuePolicies() {
        String tpl = "{\"a\":{\"$signal\":\"nope\"},\"b\":\"x ${signal.nope}\",\"c\":[{\"$signal\":\"nope\"}],\"d\":{\"$signal\":\"nope\",\"default\":\"dflt\"}}";
        assertThatThrownBy(() -> resolve(tpl, MissingValuePolicy.FAIL)).isInstanceOf(BodyResolutionException.class).hasMessageContaining("$.a");
        JsonNode u = resolve(tpl, MissingValuePolicy.USE_DEFAULT);
        assertThat(u.get("a").isNull()).isTrue();
        assertThat(u.get("b").asText()).isEqualTo("x ");
        assertThat(u.get("d").asText()).isEqualTo("dflt");
        JsonNode o = resolve(tpl, MissingValuePolicy.OMIT_FIELD);
        assertThat(o.has("a")).isFalse();
        assertThat(o.has("b")).isFalse();
        assertThat(o.get("c").size()).isZero();
        assertThat(o.get("d").asText()).isEqualTo("dflt");
        // required overrides lenient policy
        assertThatThrownBy(() -> resolve("{\"a\":{\"$signal\":\"nope\",\"required\":true}}", MissingValuePolicy.OMIT_FIELD))
                .isInstanceOf(BodyResolutionException.class);
        // template default transform
        assertThat(resolve("{\"a\":\"${signal.nope|default:'n/a'}\"}", MissingValuePolicy.FAIL).get("a").asText()).isEqualTo("n/a");
        // $trigger absent for time-triggered evaluations
        assertThat(BodyResolver.resolve(Json.parse("{\"t\":{\"$trigger\":\"$.signal\",\"default\":\"timer\"}}"), ctx(null, null), MissingValuePolicy.FAIL)
                .get("t").asText()).isEqualTo("timer");
    }

    @Test
    void transformFailuresAndSizeLimit() {
        assertThatThrownBy(() -> resolve("{\"a\":{\"$signal\":\"email\",\"transform\":\"date:'yyyy'\"}}", MissingValuePolicy.FAIL))
                .isInstanceOf(BodyResolutionException.class);
        StringBuilder big = new StringBuilder("{\"x\":\"");
        big.append("a".repeat(BodyResolver.MAX_BODY_BYTES + 10)).append("\"}");
        assertThatThrownBy(() -> resolve(big.toString(), MissingValuePolicy.FAIL)).hasMessageContaining("limit");
    }

    @Test
    void deferredParentResolvesLater() {
        BodyContext deferred = new BodyContext(SIGNALS, null, null, true, "k", NOW, ZoneId.of("UTC"), null, false, "r", "t");
        BodyResolver.resolve(Json.parse("{\"why\":{\"$parent\":\"$.lastFailure.reason\"}}"), deferred, MissingValuePolicy.FAIL); // no throw
        assertThatThrownBy(() -> BodyResolver.resolve(Json.parse("{\"why\":{\"$parent\":\"$.x\"},\"e\":{\"$signal\":\"nope\"}}"), deferred, MissingValuePolicy.FAIL))
                .isInstanceOf(BodyResolutionException.class);
    }

    @Test
    void staticValidation() {
        List<ValidationError> errs = new ArrayList<>();
        BodyResolver.validate(Json.parse("""
                {"a":{"$signal":"x","bogus":1},"b":{"$parent":"$.x"},"c":"${unknown}","d":{"$signal":"x","transform":"exec:'rm'"},
                 "e":{"$builtin":"occurrence.index"},"f":{"$signal":"x","required":true,"default":1}}"""), "$.body", false, false, errs);
        assertThat(errs).extracting(ValidationError::path).contains("$.body.a.bogus", "$.body.b.$parent", "$.body.c", "$.body.d.transform",
                "$.body.e.$builtin", "$.body.f");
    }
}
