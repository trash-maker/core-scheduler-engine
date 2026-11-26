package io.notifengine.domain.expr;

import io.notifengine.common.IsoAmount;
import io.notifengine.domain.recurrence.Recurrence;
import io.notifengine.domain.signal.Value;

import java.time.Duration;
import java.time.ZoneId;
import java.util.List;

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
