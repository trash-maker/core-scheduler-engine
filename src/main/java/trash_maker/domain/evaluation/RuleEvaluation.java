package trash_maker.domain.evaluation;

import com.fasterxml.jackson.databind.JsonNode;
import trash_maker.domain.body.BodySchemaValidator;
import trash_maker.domain.expr.EvalInput;
import trash_maker.domain.expr.EvalResult;
import trash_maker.domain.expr.Evaluator;
import trash_maker.domain.expr.LeafMemory;
import trash_maker.domain.rule.CompiledRule;
import trash_maker.domain.signal.SignalValue;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;

/**
 * The single evaluation pipeline used by signal ingestion, RULE_EVAL timers and dry-run:
 * evaluate expression -> firing decision -> (if firing) plan notifications.
 */
public final class RuleEvaluation {
    private RuleEvaluation() {}

    public record PriorState(boolean lastResult, long edgeSeq, Instant lastFiredAt, Map<String, LeafMemory> memory) {
        public static final PriorState INITIAL = new PriorState(false, 0, null, Map.of());
    }

    /**
     * @param plan          null when not firing or when planning failed
     * @param planningError set when firing but the plan failed (nothing created, DLQ)
     */
    public record Result(EvalResult evaluation, FiringDecision.Decision decision, FiringPlanner.Plan plan,
                         PlanningException planningError) {}

    public static Result run(CompiledRule rule, String tenantId, String correlationKey, Map<String, SignalValue> signals,
                             JsonNode trigger, Instant now, PriorState prior, Duration defaultMaxAge, BodySchemaValidator schemas) {
        EvalResult er = Evaluator.evaluate(rule.expression(), new EvalInput(now, rule.def().timezone(), signals,
                prior.memory(), rule.def().trigger().onMissing(), defaultMaxAge));
        FiringDecision.Decision d = FiringDecision.decide(rule.def().trigger().mode(), rule.def().trigger().cooldown(),
                prior.lastResult(), prior.edgeSeq(), prior.lastFiredAt(), er.value(), now);
        FiringPlanner.Plan plan = null;
        PlanningException pe = null;
        if (d.fire()) {
            try {
                plan = FiringPlanner.plan(rule, new FiringPlanner.EntityContext(tenantId, correlationKey, signals, trigger), now, d.newEdgeSeq(), schemas);
            } catch (PlanningException e) {
                pe = e;
            }
        }
        return new Result(er, d, plan, pe);
    }
}
