package io.notifengine.domain.expr;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.notifengine.common.Json;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Optional text DSL compiled to the JSON expression tree (which is then compiled/validated as usual).
 * <pre>
 *   (kpi1 > 5 and dateMatches(NOW, cron '0 0 1 * *', tolerance PT1H)) or kpi2 = true maxAge P1D
 *   status in ['A','B'] and score between [1, 10] for PT10M and due_date &lt;= now + P3D
 *   kpi1 > signal(kpi2) within PT1H and not dateMatches(due_date, every P1M anchor '2024-01-31')
 * </pre>
 */
public final class DslParser {

    public static final class DslException extends IllegalArgumentException {
        public final int position;
        DslException(String msg, int position) { super(msg + " at position " + position); this.position = position; }
    }

    private record Tok(String type, String text, int pos) {}

    private final List<Tok> toks;
    private int i;

    private DslParser(List<Tok> toks) { this.toks = toks; }

    public static JsonNode parse(String text) {
        if (text.length() > 20_000) throw new DslException("expression text too long", 0);
        DslParser p = new DslParser(lex(text));
        JsonNode n = p.orExpr();
        if (!p.peek("EOF")) throw new DslException("unexpected '" + p.cur().text + "'", p.cur().pos);
        return n;
    }

    private static List<Tok> lex(String s) {
        List<Tok> out = new ArrayList<>();
        int i = 0;
        while (i < s.length()) {
            char c = s.charAt(i);
            if (Character.isWhitespace(c)) { i++; continue; }
            int start = i;
            if (c == '(' || c == ')' || c == '[' || c == ']' || c == ',' || c == '+' ) { out.add(new Tok(String.valueOf(c), String.valueOf(c), i)); i++; continue; }
            if (c == '-' && (i + 1 < s.length()) && !Character.isDigit(s.charAt(i + 1))) { out.add(new Tok("-", "-", i)); i++; continue; }
            if (c == '\'' || c == '"') {
                StringBuilder b = new StringBuilder();
                i++;
                while (i < s.length() && s.charAt(i) != c) {
                    if (s.charAt(i) == '\\' && i + 1 < s.length()) i++;
                    b.append(s.charAt(i++));
                }
                if (i >= s.length()) throw new DslException("unterminated string", start);
                i++;
                out.add(new Tok("STR", b.toString(), start));
                continue;
            }
            if (c == '>' || c == '<' || c == '!' || c == '=') {
                String two = i + 1 < s.length() ? s.substring(i, i + 2) : "";
                if (two.equals(">=") || two.equals("<=") || two.equals("!=") || two.equals("==")) { out.add(new Tok("OP", two.equals("==") ? "=" : two, i)); i += 2; continue; }
                if (c == '!') throw new DslException("unexpected '!'", i);
                out.add(new Tok("OP", String.valueOf(c), i)); i++; continue;
            }
            if (Character.isDigit(c) || c == '-') {
                i++;
                while (i < s.length() && (Character.isDigit(s.charAt(i)) || s.charAt(i) == '.' || s.charAt(i) == 'e' || s.charAt(i) == 'E')) i++;
                out.add(new Tok("NUM", s.substring(start, i), start));
                continue;
            }
            if (Character.isLetter(c) || c == '_' || c == '$') {
                i++;
                while (i < s.length() && (Character.isLetterOrDigit(s.charAt(i)) || "_.:-$".indexOf(s.charAt(i)) >= 0)) i++;
                out.add(new Tok("ID", s.substring(start, i), start));
                continue;
            }
            throw new DslException("unexpected character '" + c + "'", i);
        }
        out.add(new Tok("EOF", "<end>", s.length()));
        return out;
    }

    private Tok cur() { return toks.get(i); }
    private boolean peek(String type) { return cur().type.equals(type); }
    private boolean peekKw(String kw) { return peek("ID") && cur().text.equalsIgnoreCase(kw); }
    private Tok expect(String type, String what) {
        if (!peek(type)) throw new DslException("expected " + what + " but found '" + cur().text + "'", cur().pos);
        return toks.get(i++);
    }
    private void expectKw(String kw) {
        if (!peekKw(kw)) throw new DslException("expected '" + kw + "' but found '" + cur().text + "'", cur().pos);
        i++;
    }

    private JsonNode orExpr() {
        List<JsonNode> parts = new ArrayList<>(List.of(andExpr()));
        while (peekKw("or")) { i++; parts.add(andExpr()); }
        return combine("or", parts);
    }

    private JsonNode andExpr() {
        List<JsonNode> parts = new ArrayList<>(List.of(unary()));
        while (peekKw("and")) { i++; parts.add(unary()); }
        return combine("and", parts);
    }

    private static JsonNode combine(String op, List<JsonNode> parts) {
        if (parts.size() == 1) return parts.get(0);
        ObjectNode o = Json.object();
        ArrayNode a = o.putArray(op);
        parts.forEach(a::add);
        return o;
    }

    private JsonNode unary() {
        if (peekKw("not")) { i++; ObjectNode o = Json.object(); o.set("not", unary()); return o; }
        if (peek("(")) { i++; JsonNode e = orExpr(); expect(")", "')'"); return e; }
        if (peekKw("dateMatches")) return dateMatches();
        return comparison();
    }

    private JsonNode dateMatches() {
        i++;
        expect("(", "'('");
        ObjectNode dm = Json.object();
        Tok src = expect("ID", "NOW or a DATE signal name");
        dm.put("source", src.text.equalsIgnoreCase("now") ? "NOW" : src.text);
        expect(",", "','");
        ObjectNode rec = dm.putObject("recurrence");
        if (peekKw("cron")) { i++; rec.put("cron", expect("STR", "quoted cron expression").text); }
        else if (peekKw("every")) {
            i++;
            rec.put("every", expect("ID", "ISO-8601 period").text);
            expectKw("anchor");
            rec.put("anchor", expect("STR", "quoted anchor date-time").text);
        } else throw new DslException("expected 'cron' or 'every'", cur().pos);
        while (peek(",")) {
            i++;
            Tok k = expect("ID", "option name");
            switch (k.text.toLowerCase(Locale.ROOT)) {
                case "tolerance" -> dm.put("tolerance", expect("ID", "ISO-8601 duration").text);
                case "timezone" -> dm.put("timezone", expect("STR", "quoted time zone").text);
                default -> throw new DslException("unknown dateMatches option '" + k.text + "'", k.pos);
            }
        }
        expect(")", "')'");
        ObjectNode o = Json.object();
        o.set("dateMatches", dm);
        return o;
    }

    private JsonNode comparison() {
        Tok sig = expect("ID", "signal name");
        ObjectNode c = Json.object();
        c.put("signal", sig.text);
        if (peekKw("in")) {
            i++;
            c.put("op", "in");
            c.set("value", list());
        } else if (peekKw("between")) {
            i++;
            c.put("op", "between");
            c.set("value", list());
        } else {
            Tok op = expect("OP", "comparison operator");
            c.put("op", op.text);
            if (peekKw("signal")) {
                i++;
                expect("(", "'('");
                c.put("valueSignal", expect("ID", "signal name").text);
                expect(")", "')'");
            } else if (peekKw("now")) {
                i++;
                String off = "";
                if (peek("+") || peek("-")) {
                    String sign = toks.get(i++).text;
                    off = (sign.equals("-") ? "-" : "") + expect("ID", "ISO-8601 offset").text;
                }
                c.putObject("value").put("now", off.isEmpty() ? "PT0S" : off);
            } else c.set("value", literal());
        }
        while (true) {
            if (peekKw("maxAge")) { i++; c.put("maxAge", expect("ID", "ISO-8601 duration").text); }
            else if (peekKw("for")) { i++; c.put("for", expect("ID", "ISO-8601 duration").text); }
            else if (peekKw("within")) { i++; c.put("within", expect("ID", "ISO-8601 duration").text); }
            else break;
        }
        return c;
    }

    private ArrayNode list() {
        expect("[", "'['");
        ArrayNode a = Json.array();
        a.add(literal());
        while (peek(",")) { i++; a.add(literal()); }
        expect("]", "']'");
        return a;
    }

    private JsonNode literal() {
        Tok t = cur();
        switch (t.type) {
            case "NUM" -> { i++; try { return Json.MAPPER.getNodeFactory().numberNode(new BigDecimal(t.text)); } catch (NumberFormatException e) { throw new DslException("bad number", t.pos); } }
            case "STR" -> { i++; return Json.MAPPER.getNodeFactory().textNode(t.text); }
            case "ID" -> {
                if (t.text.equalsIgnoreCase("true") || t.text.equalsIgnoreCase("false")) { i++; return Json.MAPPER.getNodeFactory().booleanNode(Boolean.parseBoolean(t.text.toLowerCase(Locale.ROOT))); }
                throw new DslException("expected literal but found '" + t.text + "'", t.pos);
            }
            default -> throw new DslException("expected literal but found '" + t.text + "'", t.pos);
        }
    }
}
