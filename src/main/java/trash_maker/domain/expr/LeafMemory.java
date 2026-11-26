package trash_maker.domain.expr;

import java.time.Instant;

/** Per-leaf memory for {@code for} / {@code within} (persisted in rule_state.leaf_state). */
public record LeafMemory(Instant trueSince, Instant lastTrueAt, boolean rawTrue) {
    public static final LeafMemory EMPTY = new LeafMemory(null, null, false);
}
