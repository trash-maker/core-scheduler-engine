package io.notifengine.domain.body;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.List;

/** Minimal, safe JSONPath subset: $, .name, ['name'], [index]. No filters, no wildcards, no scripts. */
public final class JsonPath {
    private final List<Object> steps;
    private final String text;

    private JsonPath(String text, List<Object> steps) { this.text = text; this.steps = steps; }

    public static JsonPath compile(String p) {
        if (p == null || !p.startsWith("$")) throw new IllegalArgumentException("path must start with '$'");
        List<Object> steps = new ArrayList<>();
        int i = 1;
        while (i < p.length()) {
            char c = p.charAt(i);
            if (c == '.') {
                int j = i + 1;
                while (j < p.length() && p.charAt(j) != '.' && p.charAt(j) != '[') j++;
                String name = p.substring(i + 1, j);
                if (name.isEmpty() || name.equals("*")) throw new IllegalArgumentException("invalid path segment at " + i);
                steps.add(name);
                i = j;
            } else if (c == '[') {
                int j = p.indexOf(']', i);
                if (j < 0) throw new IllegalArgumentException("unclosed '[' at " + i);
                String in = p.substring(i + 1, j).trim();
                if (in.startsWith("'") && in.endsWith("'") && in.length() >= 2) steps.add(in.substring(1, in.length() - 1));
                else {
                    try { steps.add(Integer.parseInt(in)); } catch (NumberFormatException e) { throw new IllegalArgumentException("invalid index '" + in + "'"); }
                }
                i = j + 1;
            } else throw new IllegalArgumentException("unexpected '" + c + "' at " + i);
        }
        if (steps.size() > 32) throw new IllegalArgumentException("path too deep");
        return new JsonPath(p, List.copyOf(steps));
    }

    /** Returns the node or null if absent / JSON null. */
    public JsonNode read(JsonNode root) {
        JsonNode cur = root;
        for (Object s : steps) {
            if (cur == null) return null;
            cur = s instanceof Integer idx ? (cur.isArray() ? cur.get(idx) : null) : (cur.isObject() ? cur.get((String) s) : null);
        }
        return cur == null || cur.isNull() || cur.isMissingNode() ? null : cur;
    }

    @Override
    public String toString() { return text; }
}
