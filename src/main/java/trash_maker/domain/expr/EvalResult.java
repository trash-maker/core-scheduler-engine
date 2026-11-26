package trash_maker.domain.expr;

import java.time.Instant;
import java.util.Map;

/**
 * @param value            final boolean result (MISSING counts as false)
 * @param nodes            truth value of every node by node id (path), with detail
 * @param nextEntityChange earliest future instant at which this entity's result may change without input
 *                         (maxAge expiry, for/within windows, NOW-relative comparisons), or null
 * @param nextRuleChange   earliest future instant at which a NOW-based dateMatches may change (same for all entities), or null
 * @param memory           updated leaf memory
 */
public record EvalResult(boolean value, Truth truth, Map<String, NodeResult> nodes,
                         Instant nextEntityChange, Instant nextRuleChange, Map<String, LeafMemory> memory) {

    public record NodeResult(Truth value, String detail) {}
}
