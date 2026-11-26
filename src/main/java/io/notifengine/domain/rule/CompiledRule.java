package io.notifengine.domain.rule;

import io.notifengine.domain.expr.Evaluator;
import io.notifengine.domain.expr.Node;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A validated rule version compiled once: expression tree plus derived indexes.
 *
 * @param exprSignals      signals referenced by the trigger expression (drive evaluation)
 * @param contextSignals   signals needed to build notifications (bodies, dueAt/startAt/expiresAt bases)
 * @param cancelSignals    signal name -> template ids cancelled by it
 * @param hasNowCondition  expression contains a NOW-based dateMatches (needs a per-rule sweep timer)
 * @param entityTimed      expression has per-entity time behaviour (maxAge, for, within, NOW-relative comparisons)
 */
public record CompiledRule(RuleDefinition def, Set<String> exprSignals, Set<String> contextSignals,
                           Map<String, Set<String>> cancelSignals, boolean hasNowCondition, boolean entityTimed,
                           Map<String, TemplateDef> templates, List<TemplateDef> roots,
                           Map<String, List<TemplateDef>> children, Map<String, Integer> depth) {

    public String tenantId() { return def.tenantId(); }

    public String id() { return def.id(); }

    public int version() { return def.version(); }

    public Node expression() { return def.trigger().expression(); }

    /** All signal names this rule needs stored (expression, context and cancel events). */
    public Set<String> referencedSignals() {
        var s = new java.util.LinkedHashSet<>(exprSignals);
        s.addAll(contextSignals);
        s.addAll(cancelSignals.keySet());
        return s;
    }

    public Instant nextRuleChange(Instant from) { return hasNowCondition ? Evaluator.nextRuleChange(expression(), from) : null; }
}
