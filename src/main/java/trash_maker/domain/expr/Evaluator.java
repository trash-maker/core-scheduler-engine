package trash_maker.domain.expr;

import trash_maker.domain.signal.SignalKind;
import trash_maker.domain.signal.SignalValue;
import trash_maker.domain.signal.Value;

import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Pure, side-effect free evaluation of a compiled expression. */
public final class Evaluator {

    private final EvalInput in;
    private final Map<String, EvalResult.NodeResult> nodes = new LinkedHashMap<>();
    private final Map<String, LeafMemory> memory;
    private Instant nextEntity;
    private Instant nextRule;

    private Evaluator(EvalInput in) {
        this.in = in;
        this.memory = new HashMap<>(in.memory() == null ? Map.of() : in.memory());
    }

    public static EvalResult evaluate(Node root, EvalInput in) {
        Evaluator e = new Evaluator(in);
        Truth t = e.eval(root);
        return new EvalResult(t == Truth.TRUE, t, e.nodes, e.nextEntity, e.nextRule, e.memory);
    }

    /** Earliest instant strictly after {@code from} at which any NOW-based dateMatches in the tree may change. */
    public static Instant nextRuleChange(Node root, Instant from) {
        Instant[] best = {null};
        walk(root, n -> {
            if (n instanceof Node.DateMatches dm && dm.isNow()) {
                Instant c = dateMatchesNowChange(dm, from);
                if (c != null && (best[0] == null || c.isBefore(best[0]))) best[0] = c;
            }
        });
        return best[0];
    }

    public static void walk(Node n, java.util.function.Consumer<Node> f) {
        if (n == null) return;
        f.accept(n);
        switch (n) {
            case Node.And a -> a.children().forEach(c -> walk(c, f));
            case Node.Or o -> o.children().forEach(c -> walk(c, f));
            case Node.Not x -> walk(x.child(), f);
            default -> { }
        }
    }

    private void entityCandidate(Instant t) {
        if (t != null && t.isAfter(in.now()) && (nextEntity == null || t.isBefore(nextEntity))) nextEntity = t;
    }

    private void ruleCandidate(Instant t) {
        if (t != null && t.isAfter(in.now()) && (nextRule == null || t.isBefore(nextRule))) nextRule = t;
    }

    private Truth record(Node n, Truth t, String detail) {
        nodes.put(n.id(), new EvalResult.NodeResult(t, detail));
        return t;
    }

    private Truth eval(Node n) {
        return switch (n) {
            case Node.And a -> {
                Truth acc = Truth.TRUE;
                for (Node c : a.children()) {
                    Truth t = eval(c); // no short-circuit: every node reported + every timer candidate collected
                    if (t == Truth.FALSE) acc = Truth.FALSE;
                    else if (t == Truth.MISSING && acc == Truth.TRUE) acc = Truth.MISSING;
                }
                yield record(n, acc, null);
            }
            case Node.Or o -> {
                Truth acc = Truth.FALSE;
                for (Node c : o.children()) {
                    Truth t = eval(c);
                    if (t == Truth.TRUE) acc = Truth.TRUE;
                    else if (t == Truth.MISSING && acc == Truth.FALSE) acc = Truth.MISSING;
                }
                yield record(n, acc, null);
            }
            case Node.Not x -> record(n, eval(x.child()).not(), null);
            case Node.Compare c -> compare(c);
            case Node.DateMatches d -> dateMatches(d);
        };
    }

    private Truth missing(Node n, String why) {
        Truth t = switch (in.onMissing()) {
            case FALSE -> Truth.FALSE;
            case TRUE -> Truth.TRUE;
            case UNKNOWN -> Truth.MISSING;
        };
        return record(n, t, why);
    }

    /** Fresh value or null (missing/stale). Registers the staleness instant as a timer candidate. */
    private SignalValue fresh(String name, Duration maxAge, String[] why) {
        SignalValue sv = in.signals().get(name);
        if (sv == null) { why[0] = "signal '" + name + "' missing"; return null; }
        Duration age = maxAge != null ? maxAge : in.defaultMaxAge();
        if (age != null) {
            Instant staleAt = sv.occurredAt().plus(age);
            if (!in.now().isBefore(staleAt)) { why[0] = "signal '" + name + "' stale"; return null; }
            entityCandidate(staleAt);
        }
        return sv;
    }

    private Truth compare(Node.Compare c) {
        String[] why = {null};
        SignalValue lhs = fresh(c.signal(), c.maxAge(), why);
        Truth raw;
        String detail = null;
        if (lhs == null) {
            raw = Truth.MISSING;
            detail = why[0];
        } else {
            raw = rawCompare(c, lhs, why);
            detail = why[0];
        }
        if (!c.stateful()) {
            return raw == Truth.MISSING ? missing(c, detail) : record(c, raw, detail);
        }
        LeafMemory m = memory.getOrDefault(c.id(), LeafMemory.EMPTY);
        Instant now = in.now();
        boolean isTrue = raw == Truth.TRUE;
        Truth out;
        LeafMemory next;
        if (c.forDuration() != null) {
            if (isTrue) {
                Instant since = m.trueSince() != null ? m.trueSince() : now;
                Instant satisfiedAt = since.plus(c.forDuration());
                if (!now.isBefore(satisfiedAt)) { out = Truth.TRUE; detail = "true since " + since; }
                else { out = Truth.FALSE; detail = "pending 'for' until " + satisfiedAt; entityCandidate(satisfiedAt); }
                next = new LeafMemory(since, now, true);
            } else {
                out = raw;
                next = new LeafMemory(null, m.lastTrueAt(), false);
            }
        } else {
            Instant lastTrue = isTrue ? now : (m.rawTrue() ? now : m.lastTrueAt());
            if (isTrue) out = Truth.TRUE;
            else if (lastTrue != null && now.isBefore(lastTrue.plus(c.within()))) {
                out = Truth.TRUE;
                detail = "was true at " + lastTrue + " (within " + c.within() + ")";
                entityCandidate(lastTrue.plus(c.within()));
            } else out = raw;
            next = new LeafMemory(isTrue ? (m.trueSince() != null ? m.trueSince() : now) : null, lastTrue, isTrue);
        }
        if (!next.equals(m)) {
            if (next.equals(LeafMemory.EMPTY)) memory.remove(c.id()); else memory.put(c.id(), next);
        }
        return out == Truth.MISSING ? missing(c, detail) : record(c, out, detail);
    }

    private Truth rawCompare(Node.Compare c, SignalValue lhsSv, String[] why) {
        Value lhs = lhsSv.value();
        List<Node.Operand> ops = c.operands();
        Value[] rhs = new Value[ops.size()];
        for (int k = 0; k < ops.size(); k++) {
            Node.Operand o = ops.get(k);
            switch (o) {
                case Node.Literal l -> rhs[k] = l.value();
                case Node.SignalRef r -> {
                    SignalValue sv = fresh(r.signal(), c.maxAge(), why);
                    if (sv == null) return Truth.MISSING;
                    rhs[k] = sv.value();
                }
                case Node.NowRelative nr -> {
                    if (!(lhs instanceof Value.Date ld)) { why[0] = "NOW comparison requires a DATE signal"; return Truth.MISSING; }
                    ZonedDateTime target = nr.offset().addTo(in.now().atZone(in.zone()));
                    rhs[k] = Value.Date.of(target.toInstant());
                    // result flips when now + offset crosses the signal date
                    Instant lInst = ld.instant(in.zone());
                    Instant cross = nr.offset().addTo(lInst.atZone(in.zone()), -1).toInstant();
                    entityCandidate(cross);
                    entityCandidate(cross.plusMillis(1));
                }
            }
        }
        Integer cmp;
        switch (c.op()) {
            case IN -> {
                for (Value r : rhs) {
                    Integer x = cmp(lhs, r, c.op());
                    if (x != null && x == 0) return Truth.TRUE;
                }
                return Truth.FALSE;
            }
            case BETWEEN -> {
                Integer lo = cmp(lhs, rhs[0], c.op());
                Integer hi = cmp(lhs, rhs[1], c.op());
                if (lo == null || hi == null) { why[0] = "type mismatch"; return Truth.MISSING; }
                return Truth.of(lo >= 0 && hi <= 0);
            }
            default -> {
                cmp = cmp(lhs, rhs[0], c.op());
                if (cmp == null) { why[0] = "type mismatch (" + lhs.typeName() + " vs " + rhs[0].typeName() + ")"; return Truth.MISSING; }
                return Truth.of(switch (c.op()) {
                    case EQ -> cmp == 0;
                    case NE -> cmp != 0;
                    case GT -> cmp > 0;
                    case GE -> cmp >= 0;
                    case LT -> cmp < 0;
                    case LE -> cmp <= 0;
                    default -> false;
                });
            }
        }
    }

    /** Comparison of two values or null if incomparable for this operator. */
    private Integer cmp(Value l, Value r, Op op) {
        if (l instanceof Value.Num a && r instanceof Value.Num b) return a.v().compareTo(b.v());
        if (l instanceof Value.Date a) {
            Value.Date b = asDate(r);
            return b == null ? null : a.instant(in.zone()).compareTo(b.instant(in.zone()));
        }
        if (r instanceof Value.Date b && l instanceof Value.Str) {
            Value.Date a = asDate(l);
            return a == null ? null : a.instant(in.zone()).compareTo(b.instant(in.zone()));
        }
        if (l instanceof Value.Bool a && r instanceof Value.Bool b) {
            if (op.ordering()) return null;
            return a.v() == b.v() ? 0 : 1;
        }
        if (l instanceof Value.Str a && r instanceof Value.Str b) return a.v().compareTo(b.v());
        return null;
    }

    private static Value.Date asDate(Value v) {
        if (v instanceof Value.Date d) return d;
        if (v instanceof Value.Str s) {
            try { return Value.Date.parse(s.v()); } catch (IllegalArgumentException e) { return null; }
        }
        return null;
    }

    private Truth dateMatches(Node.DateMatches d) {
        ZonedDateTime t;
        if (d.isNow()) {
            t = in.now().atZone(d.zone());
            ruleCandidate(dateMatchesNowChange(d, in.now()));
        } else {
            String[] why = {null};
            SignalValue sv = fresh(d.sourceSignal(), null, why);
            if (sv == null) return missing(d, why[0]);
            if (sv.kind() != SignalKind.DATE || !(sv.value() instanceof Value.Date dv)) return missing(d, "signal '" + d.sourceSignal() + "' is not a DATE");
            t = dv.at(d.zone());
        }
        ZonedDateTime prev = d.recurrence().previousOrSame(t);
        boolean match;
        if (prev == null) match = false;
        else if (d.tolerance().isZero()) match = prev.toInstant().equals(t.toInstant());
        else match = t.toInstant().isBefore(prev.toInstant().plus(d.tolerance()));
        return record(d, Truth.of(match), prev == null ? "no occurrence" : "last occurrence " + prev.toOffsetDateTime());
    }

    static Instant dateMatchesNowChange(Node.DateMatches d, Instant now) {
        ZonedDateTime t = now.atZone(d.zone());
        ZonedDateTime prev = d.recurrence().previousOrSame(t);
        ZonedDateTime next = d.recurrence().next(t);
        Instant best = next == null ? null : next.toInstant();
        if (prev != null) {
            Instant end = prev.toInstant().plus(d.tolerance());
            if (end.isAfter(now) && (best == null || end.isBefore(best))) best = end;
        }
        return best;
    }
}
