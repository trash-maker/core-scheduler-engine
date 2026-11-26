package io.notifengine.domain.evaluation;

import io.notifengine.domain.rule.Enums.FiringMode;

import java.time.Duration;
import java.time.Instant;

/** Pure firing decision for one (rule, entity) evaluation. */
public final class FiringDecision {
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
