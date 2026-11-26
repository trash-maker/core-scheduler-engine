package io.notifengine.domain.body;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.TextNode;
import io.notifengine.domain.signal.Value;

import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/** Whitelisted value transforms. Syntax: name or name:'argument'. */
public final class Transforms {
    public static final Set<String> NAMES = Set.of("date", "number", "upper", "lower", "capitalize", "trim", "default");

    public record Transform(String name, String arg) {}

    private Transforms() {}

    public static List<Transform> parseList(List<String> specs) {
        List<Transform> out = new ArrayList<>();
        for (String s : specs) out.add(parse(s));
        return out;
    }

    public static Transform parse(String spec) {
        String s = spec.trim();
        int colon = s.indexOf(':');
        String name = colon < 0 ? s : s.substring(0, colon).trim();
        String arg = null;
        if (colon >= 0) {
            String a = s.substring(colon + 1).trim();
            if (a.length() >= 2 && (a.startsWith("'") && a.endsWith("'") || a.startsWith("\"") && a.endsWith("\""))) arg = a.substring(1, a.length() - 1);
            else throw new IllegalArgumentException("transform argument must be quoted: " + spec);
        }
        if (!NAMES.contains(name)) throw new IllegalArgumentException("unknown transform '" + name + "' (allowed: " + NAMES + ")");
        if ((name.equals("date") || name.equals("number") || name.equals("default")) && arg == null)
            throw new IllegalArgumentException("transform '" + name + "' requires an argument");
        if (name.equals("date")) DateTimeFormatter.ofPattern(arg, Locale.ROOT);
        if (name.equals("number")) new DecimalFormat(arg, DecimalFormatSymbols.getInstance(Locale.ROOT));
        return new Transform(name, arg);
    }

    public static JsonNode apply(Transform t, JsonNode v, ZoneId zone) {
        return switch (t.name()) {
            case "default" -> v;
            case "upper" -> TextNode.valueOf(v.asText().toUpperCase(Locale.ROOT));
            case "lower" -> TextNode.valueOf(v.asText().toLowerCase(Locale.ROOT));
            case "trim" -> TextNode.valueOf(v.asText().trim());
            case "capitalize" -> {
                String s = v.asText();
                yield TextNode.valueOf(s.isEmpty() ? s : Character.toUpperCase(s.charAt(0)) + s.substring(1));
            }
            case "date" -> {
                Value.Date d;
                try { d = Value.Date.parse(v.asText()); } catch (IllegalArgumentException e) { throw new IllegalArgumentException("value is not a date"); }
                yield TextNode.valueOf(DateTimeFormatter.ofPattern(t.arg(), Locale.ROOT).format(d.at(zone)));
            }
            case "number" -> {
                BigDecimal n;
                try { n = v.isNumber() ? v.decimalValue() : new BigDecimal(v.asText().trim()); } catch (NumberFormatException e) { throw new IllegalArgumentException("value is not a number"); }
                yield TextNode.valueOf(new DecimalFormat(t.arg(), DecimalFormatSymbols.getInstance(Locale.ROOT)).format(n));
            }
            default -> throw new IllegalArgumentException("unknown transform " + t.name());
        };
    }
}
