package trash_maker.domain.expr;

import trash_maker.common.IsoAmount;
import trash_maker.domain.recurrence.Recurrence;
import trash_maker.domain.signal.Signals.SignalValue;
import trash_maker.domain.signal.Signals.Value;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;

/** Expression model: the compiled tree, operators, three-valued truth, evaluation input and result. */
public final class Expr {
    private Expr() {}

    /** Compiled, immutable expression tree. Each node has a stable id = its JSON path in the rule. */
    public sealed interface Node permits Node.And, Node.Or, Node.Not, Node.Compare, Node.DateMatches {

        String id();

        record And(String id, List<Node> children) implements Node {}

        record Or(String id, List<Node> children) implements Node {}

        record Not(String id, Node child) implements Node {}

        /** Right-hand side of a comparison. */
        sealed interface Operand permits Literal, SignalRef, NowRelative {}

        record Literal(Value value) implements Operand {}

        record SignalRef(String signal) implements Operand {}

        /** NOW + offset; makes the comparison time dependent (only valid against DATE signals). */
        record NowRelative(IsoAmount offset) implements Operand {}

        /**
         * signal op operand(s). For IN, {@code operands} holds the list; for BETWEEN, [low, high].
         * maxAge: signal older than this is MISSING. forDuration: raw condition must hold continuously.
         * within: true if the raw condition held at any time within the window.
         */
        record Compare(String id, String signal, Op op, List<Operand> operands,
                       Duration maxAge, Duration forDuration, Duration within) implements Node {
            public boolean stateful() { return forDuration != null || within != null; }
        }

        /** source = null means NOW; otherwise the name of a DATE signal. */
        record DateMatches(String id, String sourceSignal, Recurrence recurrence, ZoneId zone,
                           Duration tolerance, String recurrenceText) implements Node {
            public boolean isNow() { return sourceSignal == null; }
        }
    }

    public enum Op {
        EQ("="), NE("!="), GT(">"), GE(">="), LT("<"), LE("<="), IN("in"), BETWEEN("between");

        public final String symbol;

        Op(String s) { this.symbol = s; }

        public static Op fromSymbol(String s) {
            for (Op o : values()) if (o.symbol.equalsIgnoreCase(s)) return o;
            throw new IllegalArgumentException("unknown operator '" + s + "'");
        }

        public boolean ordering() { return this == GT || this == GE || this == LT || this == LE || this == BETWEEN; }
    }

    /** Three-valued truth used during evaluation. MISSING = signal absent, stale or not comparable. */
    public enum Truth {
        TRUE, FALSE, MISSING;

        public static Truth of(boolean b) { return b ? TRUE : FALSE; }

        public Truth not() { return this == TRUE ? FALSE : this == FALSE ? TRUE : MISSING; }
    }

    /**
     * How a leaf with a MISSING signal (never received / stale) evaluates.
     * FALSE (default): the leaf is false. TRUE: the leaf is true.
     * UNKNOWN: Kleene three-valued logic up the tree; an unknown overall result counts as false.
     */
    public enum OnMissing { FALSE, TRUE, UNKNOWN }

    /** Per-leaf memory for {@code for} / {@code within} (persisted in rule_state.leaf_state). */
    public record LeafMemory(Instant trueSince, Instant lastTrueAt, boolean rawTrue) {
        public static final LeafMemory EMPTY = new LeafMemory(null, null, false);
    }

    public record EvalInput(Instant now, ZoneId zone, Map<String, SignalValue> signals,
                            Map<String, LeafMemory> memory, OnMissing onMissing, Duration defaultMaxAge) {}

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
}
