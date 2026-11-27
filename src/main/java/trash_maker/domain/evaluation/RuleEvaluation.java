package trash_maker.domain.evaluation;

import com.fasterxml.jackson.databind.JsonNode;
import trash_maker.domain.body.BodyResolver.BodySchemaValidator;
import trash_maker.domain.evaluation.FiringPlanner.PlanningException;
import trash_maker.domain.expr.Evaluator;
import trash_maker.domain.expr.Expr.EvalInput;
import trash_maker.domain.expr.Expr.EvalResult;
import trash_maker.domain.expr.Expr.LeafMemory;
import trash_maker.domain.rule.Rule.CompiledRule;
import trash_maker.domain.rule.Rule.Enums.FiringMode;
import trash_maker.domain.signal.Signals.SignalValue;

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
                         PlanningException planningError) {

        /** New persisted rule state for this (rule, entity). */
        public PriorState nextState(PriorState prior, Instant now) {
            return new PriorState(decision.newLastResult(), decision.newEdgeSeq(), decision.fire() ? now : prior.lastFiredAt(),
                    evaluation.memory());
        }

        /** Per-entity RULE_EVAL timer: empty = none needed, Optional.of(null-free instant) = (re)schedule; delete otherwise. */
        public EntityTimer entityTimer(boolean entityTimed) {
            if (!entityTimed) return EntityTimer.NONE;
            return evaluation.nextEntityChange() == null ? EntityTimer.DELETE : new EntityTimer(evaluation.nextEntityChange());
        }
    }

    /** What to do with the per-entity RULE_EVAL timer: {@code at != null} schedule, {@link #DELETE}, {@link #NONE}. */
    public record EntityTimer(Instant at) {
        public static final EntityTimer NONE = new EntityTimer(Instant.MIN);
        public static final EntityTimer DELETE = new EntityTimer(null);

        public boolean schedule() { return at != null && !at.equals(Instant.MIN); }
    }

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

    /** Pure firing decision for one (rule, entity) evaluation. */
    public static final class FiringDecision {
        private FiringDecision() {}

        public record Decision(boolean fire, boolean newLastResult, long newEdgeSeq, String reason) {}

        public static Decision decide(FiringMode mode, Duration cooldown, boolean lastResult, long edgeSeq,
                                      Instant lastFiredAt, boolean result, Instant now) {
            if (!result) return new Decision(false, false, edgeSeq, lastResult ? "became false (re-armed)" : "false");
            return switch (mode) {
                case ON_BECOMES_TRUE -> lastResult
                        ? new Decision(false, true, edgeSeq, "still true (already fired for this edge)")
                        : new Decision(true, true, edgeSeq + 1, "became true");
                case EVERY_EVALUATION_TRUE -> {
                    Duration cd = cooldown == null ? Duration.ZERO : cooldown;
                    if (lastFiredAt != null && now.isBefore(lastFiredAt.plus(cd)))
                        yield new Decision(false, true, edgeSeq, "in cooldown until " + lastFiredAt.plus(cd));
                    yield new Decision(true, true, edgeSeq + 1, "true (every evaluation)");
                }
            };
        }
    }
}
