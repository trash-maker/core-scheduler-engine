package trash_maker.domain;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import trash_maker.common.Json;
import trash_maker.common.Validation.ValidationError;
import trash_maker.domain.expr.DslParser;
import trash_maker.domain.expr.Evaluator;
import trash_maker.domain.expr.Expr.EvalInput;
import trash_maker.domain.expr.Expr.EvalResult;
import trash_maker.domain.expr.Expr.LeafMemory;
import trash_maker.domain.expr.Expr.Node;
import trash_maker.domain.expr.Expr.OnMissing;
import trash_maker.domain.expr.Expr.Truth;
import trash_maker.domain.expr.ExpressionCompiler;
import trash_maker.domain.signal.Signals.SignalKind;
import trash_maker.domain.signal.Signals.SignalValue;
import trash_maker.domain.signal.Signals.Value;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ExpressionTest {
    static final ZoneId ROME = ZoneId.of("Europe/Rome");
    // Monday 2024-04-01 00:30 Rome (first of month)
    static final Instant FIRST = Instant.parse("2024-03-31T22:30:00Z");
    static final Instant MID = Instant.parse("2024-04-15T10:00:00Z");

    static Node compile(String json) {
        List<ValidationError> errs = new ArrayList<>();
        Node n = ExpressionCompiler.compile(Json.parse(json), "$", ROME, errs);
        assertThat(errs).isEmpty();
        return n;
    }

    static SignalValue num(String name, double v, Instant at) { return new SignalValue(name, SignalKind.KPI, new Value.Num(BigDecimal.valueOf(v)), at); }

    static SignalValue bool(String name, boolean v, Instant at) { return new SignalValue(name, SignalKind.KPI, new Value.Bool(v), at); }

    static EvalResult eval(Node n, Instant now, Map<String, SignalValue> s) { return eval(n, now, s, OnMissing.FALSE, Map.of()); }

    static EvalResult eval(Node n, Instant now, Map<String, SignalValue> s, OnMissing m, Map<String, LeafMemory> mem) {
        return Evaluator.evaluate(n, new EvalInput(now, ROME, s, mem, m, null));
    }

    static final String EXAMPLE = """
            {"or":[
              {"and":[{"signal":"kpi1","op":">","value":5},
                      {"dateMatches":{"source":"NOW","recurrence":{"cron":"0 0 1 * *"},"tolerance":"PT1H"}}]},
              {"signal":"kpi2","op":"=","value":true,"maxAge":"P1D"}]}""";

    /** Full truth table of the acceptance expression: kpi1 (missing/<=5/>5) x date (match/no) x kpi2 (missing/false/true/stale). */
    @ParameterizedTest(name = "kpi1={0} first={1} kpi2={2} -> {3}")
    @CsvSource({
            "MISSING,true,MISSING,false", "MISSING,true,false,false", "MISSING,true,true,true", "MISSING,true,stale,false",
            "MISSING,false,MISSING,false", "MISSING,false,true,true",
            "3,true,MISSING,false", "3,true,false,false", "3,true,true,true", "3,true,stale,false",
            "3,false,true,true", "3,false,false,false",
            "7,true,MISSING,true", "7,true,false,true", "7,true,true,true", "7,true,stale,true",
            "7,false,MISSING,false", "7,false,false,false", "7,false,true,true", "7,false,stale,false"})
    void acceptanceTruthTable(String kpi1, boolean first, String kpi2, boolean expected) {
        Node n = compile(EXAMPLE);
        Instant now = first ? FIRST : MID;
        Map<String, SignalValue> s = new HashMap<>();
        if (!kpi1.equals("MISSING")) s.put("kpi1", num("kpi1", Double.parseDouble(kpi1), now.minusSeconds(60)));
        switch (kpi2) {
            case "true" -> s.put("kpi2", bool("kpi2", true, now.minusSeconds(60)));
            case "false" -> s.put("kpi2", bool("kpi2", false, now.minusSeconds(60)));
            case "stale" -> s.put("kpi2", bool("kpi2", true, now.minus(Duration.ofDays(2))));
            default -> { }
        }
        assertThat(eval(n, now, s).value()).isEqualTo(expected);
    }

    @Test
    void reportsEveryNodeAndNextInstants() {
        Node n = compile(EXAMPLE);
        Instant now = FIRST;
        EvalResult r = eval(n, now, Map.of("kpi1", num("kpi1", 7, now), "kpi2", bool("kpi2", true, now.minusSeconds(3600))));
        assertThat(r.nodes()).containsKeys("$", "$.or[0]", "$.or[0].and[0]", "$.or[0].and[1]", "$.or[1]");
        // tolerance window ends 1h after Rome midnight
        assertThat(r.nextRuleChange()).isEqualTo(Instant.parse("2024-03-31T23:00:00Z"));
        // kpi2 becomes stale 1 day after it occurred
        assertThat(r.nextEntityChange()).isEqualTo(now.minusSeconds(3600).plus(Duration.ofDays(1)));
    }

    @Test
    void missingPolicies() {
        Node n = compile("{\"not\":{\"signal\":\"x\",\"op\":\">\",\"value\":1}}");
        assertThat(eval(n, MID, Map.of(), OnMissing.FALSE, Map.of()).value()).isTrue(); // leaf false -> not -> true
        assertThat(eval(n, MID, Map.of(), OnMissing.TRUE, Map.of()).value()).isFalse();
        EvalResult u = eval(n, MID, Map.of(), OnMissing.UNKNOWN, Map.of());
        assertThat(u.value()).isFalse();
        assertThat(u.truth()).isEqualTo(Truth.MISSING);
        Node or = compile("{\"or\":[{\"signal\":\"x\",\"op\":\">\",\"value\":1},{\"signal\":\"y\",\"op\":\"=\",\"value\":true}]}");
        assertThat(eval(or, MID, Map.of("y", bool("y", true, MID)), OnMissing.UNKNOWN, Map.of()).value()).isTrue();
    }

    @Test
    void operatorsAndTypes() {
        Map<String, SignalValue> s = Map.of(
                "n", num("n", 5, MID), "o", num("o", 7, MID),
                "st", new SignalValue("st", SignalKind.KPI, new Value.Str("GOLD"), MID),
                "d", new SignalValue("d", SignalKind.DATE, Value.Date.parse("2024-04-20"), MID));
        assertThat(eval(compile("{\"signal\":\"n\",\"op\":\"between\",\"value\":[5,6]}"), MID, s).value()).isTrue();
        assertThat(eval(compile("{\"signal\":\"n\",\"op\":\"in\",\"value\":[1,5]}"), MID, s).value()).isTrue();
        assertThat(eval(compile("{\"signal\":\"n\",\"op\":\"<\",\"valueSignal\":\"o\"}"), MID, s).value()).isTrue();
        assertThat(eval(compile("{\"signal\":\"st\",\"op\":\"in\",\"value\":[\"GOLD\",\"SILVER\"]}"), MID, s).value()).isTrue();
        assertThat(eval(compile("{\"signal\":\"st\",\"op\":\"!=\",\"value\":\"GOLD\"}"), MID, s).value()).isFalse();
        assertThat(eval(compile("{\"signal\":\"d\",\"op\":\">\",\"value\":\"2024-04-19\"}"), MID, s).value()).isTrue();
        // type mismatch is MISSING -> false (also for !=)
        assertThat(eval(compile("{\"signal\":\"st\",\"op\":\"!=\",\"value\":3}"), MID, s).value()).isFalse();
        // NOW-relative: due within 3 days?  d=2024-04-20 Rome; now=04-15 -> false, next change at 04-17 00:00 Rome
        EvalResult r = eval(compile("{\"signal\":\"d\",\"op\":\"<=\",\"value\":{\"now\":\"P3D\"}}"), MID, s);
        assertThat(r.value()).isFalse();
        assertThat(r.nextEntityChange()).isEqualTo(Instant.parse("2024-04-16T22:00:00Z"));
        assertThat(eval(compile("{\"signal\":\"d\",\"op\":\"<=\",\"value\":{\"now\":\"P3D\"}}"), Instant.parse("2024-04-16T22:00:00Z"), s).value()).isTrue();
    }

    @Test
    void forRequiresContinuousTruth() {
        Node n = compile("{\"signal\":\"t\",\"op\":\">\",\"value\":30,\"for\":\"PT10M\"}");
        Instant t0 = MID;
        Map<String, SignalValue> hot = Map.of("t", num("t", 35, t0));
        EvalResult r1 = eval(n, t0, hot, OnMissing.FALSE, Map.of());
        assertThat(r1.value()).isFalse();
        assertThat(r1.nextEntityChange()).isEqualTo(t0.plus(Duration.ofMinutes(10)));
        EvalResult r2 = eval(n, t0.plus(Duration.ofMinutes(10)), hot, OnMissing.FALSE, r1.memory());
        assertThat(r2.value()).isTrue();
        // drops below -> memory reset -> must wait again
        EvalResult r3 = eval(n, t0.plus(Duration.ofMinutes(11)), Map.of("t", num("t", 20, t0)), OnMissing.FALSE, r2.memory());
        assertThat(r3.value()).isFalse();
        EvalResult r4 = eval(n, t0.plus(Duration.ofMinutes(12)), hot, OnMissing.FALSE, r3.memory());
        assertThat(r4.value()).isFalse();
    }

    @Test
    void withinKeepsTrueForWindow() {
        Node n = compile("{\"signal\":\"e\",\"op\":\"=\",\"value\":true,\"within\":\"PT5M\"}");
        Instant t0 = MID;
        EvalResult r1 = eval(n, t0, Map.of("e", bool("e", true, t0)), OnMissing.FALSE, Map.of());
        assertThat(r1.value()).isTrue();
        EvalResult r2 = eval(n, t0.plusSeconds(60), Map.of("e", bool("e", false, t0)), OnMissing.FALSE, r1.memory());
        assertThat(r2.value()).isTrue();
        assertThat(r2.nextEntityChange()).isEqualTo(t0.plusSeconds(60).plus(Duration.ofMinutes(5)));
        EvalResult r3 = eval(n, t0.plusSeconds(60 + 300), Map.of("e", bool("e", false, t0)), OnMissing.FALSE, r2.memory());
        assertThat(r3.value()).isFalse();
    }

    @Test
    void dateMatchesOnDateSignal() {
        Node n = compile("{\"dateMatches\":{\"source\":\"due\",\"recurrence\":{\"cron\":\"0 0 L * *\"}}}");
        assertThat(eval(n, MID, Map.of("due", new SignalValue("due", SignalKind.DATE, Value.Date.parse("2024-02-29"), MID))).value()).isTrue();
        assertThat(eval(n, MID, Map.of("due", new SignalValue("due", SignalKind.DATE, Value.Date.parse("2024-02-28"), MID))).value()).isFalse();
    }

    @Test
    void dslCompilesToSameTree() {
        String dsl = "(kpi1 > 5 and dateMatches(NOW, cron '0 0 1 * *', tolerance PT1H)) or kpi2 = true maxAge P1D";
        assertThat(DslParser.parse(dsl).toString()).isEqualTo(Json.parse(EXAMPLE).toString());
        var j = DslParser.parse("not s in ['A','B'] and x between [1, 2] for PT1M and d <= now + P3D and y > signal(z) within PT1H");
        List<ValidationError> errs = new ArrayList<>();
        ExpressionCompiler.compile(j, "$", ROME, errs);
        assertThat(errs).isEmpty();
    }

    @Test
    void validationReportsPreciseErrors() {
        List<ValidationError> errs = new ArrayList<>();
        ExpressionCompiler.compile(Json.parse("{\"and\":[{\"signal\":\"a\",\"op\":\"~\",\"value\":1},{\"dateMatches\":{\"source\":\"NOW\",\"recurrence\":{\"cron\":\"61 * * * *\"},\"tolerance\":\"PT0S\"}}]}"),
                "$.trigger.expression", ROME, errs);
        assertThat(errs).extracting(ValidationError::path).contains("$.trigger.expression.and[0].op",
                "$.trigger.expression.and[1].dateMatches.recurrence.cron", "$.trigger.expression.and[1].dateMatches.tolerance");
        // nesting bound
        String deep = "{\"signal\":\"a\",\"op\":\"=\",\"value\":1}";
        for (int i = 0; i < 20; i++) deep = "{\"not\":" + deep + "}";
        errs.clear();
        ExpressionCompiler.compile(Json.parse(deep), "$", ROME, errs);
        assertThat(errs).anyMatch(e -> e.message().contains("nesting"));
    }
}
