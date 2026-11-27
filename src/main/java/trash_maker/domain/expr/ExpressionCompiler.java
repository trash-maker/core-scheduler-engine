package trash_maker.domain.expr;

import com.fasterxml.jackson.databind.JsonNode;
import trash_maker.common.IsoAmount.Durations;
import trash_maker.common.IsoAmount;
import trash_maker.common.Validation.ValidationError;
import trash_maker.domain.expr.Expr.Node;
import trash_maker.domain.expr.Expr.Op;
import trash_maker.domain.recurrence.Recurrence.CronRecurrence;
import trash_maker.domain.recurrence.Recurrence.PeriodRecurrence;
import trash_maker.domain.recurrence.Recurrence;
import trash_maker.domain.signal.Signals.Value;

import java.time.DateTimeException;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Compiles the JSON expression tree into an immutable {@link Node} tree, collecting precise
 * validation errors. Bounded: depth, node count and list sizes are limited (sandboxing).
 */
public final class ExpressionCompiler {
    public static final int MAX_DEPTH = 16;
    public static final int MAX_NODES = 256;
    public static final int MAX_LIST = 1000;
    static final Pattern SIGNAL_NAME = Pattern.compile("[A-Za-z0-9_.:-]{1,128}");
    private static final Set<String> COMPARE_KEYS = Set.of("signal", "op", "value", "valueSignal", "maxAge", "for", "within");
    private static final Set<String> DATE_KEYS = Set.of("source", "recurrence", "timezone", "tolerance");

    private final ZoneId defaultZone;
    private final List<ValidationError> errors;
    private int nodes;

    private ExpressionCompiler(ZoneId defaultZone, List<ValidationError> errors) {
        this.defaultZone = defaultZone;
        this.errors = errors;
    }

    /** Returns the compiled tree, or null if errors were added to {@code errors}. */
    public static Node compile(JsonNode json, String path, ZoneId zone, List<ValidationError> errors) {
        int before = errors.size();
        ExpressionCompiler c = new ExpressionCompiler(zone, errors);
        Node n = c.node(json, path, 1);
        return errors.size() == before ? n : null;
    }

    private void err(String path, String msg) { errors.add(new ValidationError(path, msg)); }

    private Node node(JsonNode j, String path, int depth) {
        if (depth > MAX_DEPTH) { err(path, "expression nesting deeper than " + MAX_DEPTH); return null; }
        if (++nodes > MAX_NODES) { if (nodes == MAX_NODES + 1) err(path, "expression has more than " + MAX_NODES + " nodes"); return null; }
        if (j == null || !j.isObject()) { err(path, "expression node must be an object"); return null; }
        if (j.has("and") || j.has("or")) {
            String k = j.has("and") ? "and" : "or";
            if (j.size() != 1) { err(path, "'" + k + "' node must have a single key"); return null; }
            JsonNode arr = j.get(k);
            if (!arr.isArray() || arr.isEmpty()) { err(path + "." + k, "must be a non-empty array"); return null; }
            List<Node> children = new ArrayList<>();
            for (int i = 0; i < arr.size(); i++) children.add(node(arr.get(i), path + "." + k + "[" + i + "]", depth + 1));
            String id = path;
            return k.equals("and") ? new Node.And(id, children) : new Node.Or(id, children);
        }
        if (j.has("not")) {
            if (j.size() != 1) { err(path, "'not' node must have a single key"); return null; }
            return new Node.Not(path, node(j.get("not"), path + ".not", depth + 1));
        }
        if (j.has("dateMatches")) {
            if (j.size() != 1) { err(path, "'dateMatches' node must have a single key"); return null; }
            return dateMatches(j.get("dateMatches"), path + ".dateMatches", path);
        }
        if (j.has("signal")) return compare(j, path);
        err(path, "unknown expression node; expected and/or/not/signal/dateMatches");
        return null;
    }

    private Node compare(JsonNode j, String path) {
        for (Iterator<String> it = j.fieldNames(); it.hasNext(); ) {
            String f = it.next();
            if (!COMPARE_KEYS.contains(f)) err(path + "." + f, "unknown field");
        }
        String signal = signalName(j.get("signal"), path + ".signal");
        Op op = null;
        JsonNode opN = j.get("op");
        if (opN == null || !opN.isTextual()) err(path + ".op", "required operator (= != > >= < <= in between)");
        else {
            try { op = Op.fromSymbol(opN.textValue()); } catch (IllegalArgumentException e) { err(path + ".op", e.getMessage()); }
        }
        List<Node.Operand> operands = new ArrayList<>();
        JsonNode value = j.get("value");
        JsonNode valueSignal = j.get("valueSignal");
        if (value != null && valueSignal != null) err(path, "use either 'value' or 'valueSignal'");
        else if (valueSignal != null) {
            String other = signalName(valueSignal, path + ".valueSignal");
            if (op == Op.IN || op == Op.BETWEEN) err(path + ".valueSignal", "not supported with '" + op.symbol + "'");
            operands.add(new Node.SignalRef(other));
        } else if (value == null) err(path + ".value", "required");
        else if (op != null) {
            if (op == Op.IN || op == Op.BETWEEN) {
                if (!value.isArray()) err(path + ".value", "'" + op.symbol + "' requires an array");
                else if (op == Op.BETWEEN && value.size() != 2) err(path + ".value", "'between' requires [low, high]");
                else if (op == Op.IN && (value.isEmpty() || value.size() > MAX_LIST)) err(path + ".value", "'in' requires 1.." + MAX_LIST + " values");
                else for (int i = 0; i < value.size(); i++) operands.add(operand(value.get(i), path + ".value[" + i + "]"));
            } else operands.add(operand(value, path + ".value"));
        }
        Duration maxAge = duration(j.get("maxAge"), path + ".maxAge");
        Duration forD = duration(j.get("for"), path + ".for");
        Duration within = duration(j.get("within"), path + ".within");
        if (forD != null && within != null) err(path, "'for' and 'within' cannot be combined on one leaf");
        return new Node.Compare(path, signal, op, List.copyOf(operands.stream().filter(java.util.Objects::nonNull).toList()), maxAge, forD, within);
    }

    private Node.Operand operand(JsonNode v, String path) {
        if (v.isObject()) {
            if (v.has("signal") && v.size() == 1) return new Node.SignalRef(signalName(v.get("signal"), path + ".signal"));
            if (v.has("now") && v.size() == 1) {
                try {
                    return new Node.NowRelative(v.get("now").asText().isBlank() || v.get("now").asText().equals("0")
                            ? IsoAmount.ZERO : IsoAmount.parse(v.get("now").asText()));
                } catch (RuntimeException e) { err(path + ".now", "invalid ISO-8601 offset: " + e.getMessage()); return null; }
            }
            err(path, "operand object must be {\"signal\":name} or {\"now\":offset}");
            return null;
        }
        try {
            return new Node.Literal(Value.literal(v));
        } catch (IllegalArgumentException e) {
            err(path, e.getMessage());
            return null;
        }
    }

    private Node dateMatches(JsonNode j, String path, String id) {
        if (j == null || !j.isObject()) { err(path, "must be an object"); return null; }
        for (Iterator<String> it = j.fieldNames(); it.hasNext(); ) {
            String f = it.next();
            if (!DATE_KEYS.contains(f)) err(path + "." + f, "unknown field");
        }
        String source = null;
        JsonNode s = j.get("source");
        if (s == null || !s.isTextual()) err(path + ".source", "required: \"NOW\" or a DATE signal name");
        else if (!s.textValue().equals("NOW")) {
            String name = s.textValue().startsWith("$signal:") ? s.textValue().substring(8) : s.textValue();
            source = signalName(com.fasterxml.jackson.databind.node.TextNode.valueOf(name), path + ".source");
        }
        ZoneId zone = defaultZone;
        if (j.has("timezone")) {
            try { zone = ZoneId.of(j.get("timezone").asText()); } catch (DateTimeException e) { err(path + ".timezone", "unknown time zone"); }
        }
        Duration tol = duration(j.get("tolerance"), path + ".tolerance");
        if (tol == null) tol = source == null ? Duration.ofMinutes(1) : Duration.ZERO;
        if (tol.isNegative()) err(path + ".tolerance", "must not be negative");
        if (source == null && tol.isZero()) err(path + ".tolerance", "must be > 0 when source is NOW");
        JsonNode r = j.get("recurrence");
        Recurrence rec = null;
        String recText = null;
        if (r == null || !r.isObject()) err(path + ".recurrence", "required: {\"cron\":...} or {\"every\":..., \"anchor\":...}");
        else if (r.has("cron")) {
            recText = "cron " + r.get("cron").asText();
            try { rec = new CronRecurrence(r.get("cron").asText(), zone); } catch (IllegalArgumentException e) { err(path + ".recurrence.cron", e.getMessage()); }
        } else if (r.has("every")) {
            recText = "every " + r.get("every").asText();
            IsoAmount every = null;
            try { every = IsoAmount.parse(r.get("every").asText()); if (!every.isPositive()) throw new IllegalArgumentException("must be positive"); }
            catch (RuntimeException e) { err(path + ".recurrence.every", "invalid ISO-8601 period: " + e.getMessage()); }
            JsonNode a = r.get("anchor");
            LocalDateTime anchor = null;
            if (a == null || !a.isTextual()) err(path + ".recurrence.anchor", "required local date-time anchor, e.g. 2024-01-31T00:00");
            else {
                try { anchor = LocalDateTime.parse(a.textValue()); }
                catch (DateTimeParseException e) {
                    try { anchor = LocalDate.parse(a.textValue()).atStartOfDay(); }
                    catch (DateTimeParseException e2) { err(path + ".recurrence.anchor", "invalid local date-time"); }
                }
            }
            if (every != null && anchor != null) rec = new PeriodRecurrence(java.time.ZonedDateTime.ofLocal(anchor, zone, null), every);
        } else err(path + ".recurrence", "expected 'cron' or 'every'");
        return new Node.DateMatches(id, source, rec, zone, tol, recText);
    }

    private String signalName(JsonNode n, String path) {
        if (n == null || !n.isTextual() || !SIGNAL_NAME.matcher(n.textValue()).matches()) {
            err(path, "required signal name matching " + SIGNAL_NAME.pattern());
            return null;
        }
        return n.textValue();
    }

    private Duration duration(JsonNode n, String path) {
        if (n == null || n.isNull()) return null;
        try {
            Duration d = Durations.parse(n.asText());
            if (d.isNegative()) { err(path, "must not be negative"); return null; }
            return d;
        } catch (IllegalArgumentException e) {
            err(path, e.getMessage());
            return null;
        }
    }
}
