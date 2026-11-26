package trash_maker.domain.body;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import trash_maker.common.Json;
import trash_maker.common.ValidationError;
import trash_maker.domain.rule.Enums.MissingValuePolicy;
import trash_maker.domain.signal.SignalValue;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Resolves a body template. Each value is a literal, a reference object or a string template:
 * <pre>
 *  {"$signal":"kpi1"}  {"$trigger":"$.signal"}  {"$parent":"$.lastFailure.reason"}  {"$builtin":"correlationKey"}
 *  {"$coalesce":[{"$signal":"a"}, {"$signal":"b"}, "fallback"]}   {"$literal": {...}} (escape)
 *  options on references: "required": true, "default": any, "transform": "upper" | ["date:'dd/MM/yyyy'", ...]
 *  "Reminder ${correlationKey} due ${signal.due_date|date:'dd/MM/yyyy'}"   ($${ escapes)
 * </pre>
 * No code execution, no remote calls; transforms are whitelisted.
 */
public final class BodyResolver {
    public static final int MAX_BODY_BYTES = 256 * 1024;
    public static final Set<String> BUILTINS = Set.of("correlationKey", "now", "occurrence.index", "ruleId", "templateId");
    private static final Set<String> REF_KEYS = Set.of("$signal", "$trigger", "$parent", "$builtin", "$coalesce", "$literal");
    private static final Set<String> OPTION_KEYS = Set.of("required", "default", "transform");
    private static final Pattern PLACEHOLDER = Pattern.compile("(?<!\\$)\\$\\{([^}]*)}");
    private static final TextNode DEFERRED = TextNode.valueOf("<deferred>");

    private final BodyContext ctx;
    private final MissingValuePolicy policy;

    private BodyResolver(BodyContext ctx, MissingValuePolicy policy) {
        this.ctx = ctx;
        this.policy = policy;
    }

    public static JsonNode resolve(JsonNode template, BodyContext ctx, MissingValuePolicy policy) {
        if (template == null || template.isNull()) return Json.object();
        BodyResolver r = new BodyResolver(ctx, policy);
        JsonNode out = r.node(template, "$").orElse(Json.object());
        int size = Json.write(out).getBytes(StandardCharsets.UTF_8).length;
        if (size > MAX_BODY_BYTES) throw new BodyResolutionException("$", "resolved body is " + size + " bytes, limit " + MAX_BODY_BYTES);
        return out;
    }

    // ------------------------------------------------------------------ resolution

    private Optional<JsonNode> node(JsonNode t, String path) {
        if (t.isObject()) {
            if (isRef(t)) return ref((ObjectNode) t, path);
            ObjectNode out = Json.object();
            for (Iterator<Map.Entry<String, JsonNode>> it = t.fields(); it.hasNext(); ) {
                var e = it.next();
                node(e.getValue(), path + "." + e.getKey()).ifPresent(v -> out.set(e.getKey(), v));
            }
            return Optional.of(out);
        }
        if (t.isArray()) {
            ArrayNode out = Json.array();
            for (int i = 0; i < t.size(); i++) node(t.get(i), path + "[" + i + "]").ifPresent(out::add);
            return Optional.of(out);
        }
        if (t.isTextual() && t.textValue().contains("${")) return template(t.textValue(), path);
        return Optional.of(t.deepCopy());
    }

    static boolean isRef(JsonNode t) {
        for (Iterator<String> it = t.fieldNames(); it.hasNext(); ) if (it.next().startsWith("$")) return true;
        return false;
    }

    private Optional<JsonNode> ref(ObjectNode t, String path) {
        if (t.has("$literal")) return Optional.of(t.get("$literal").deepCopy());
        JsonNode value;
        if (t.has("$coalesce")) {
            value = null;
            for (JsonNode item : t.get("$coalesce")) {
                value = coalesceItem(item, path);
                if (value != null) break;
            }
        } else value = lookupRef(t);
        if (value != null) {
            List<String> specs = transforms(t.get("transform"));
            for (Transforms.Transform tr : Transforms.parseList(specs)) {
                try { value = Transforms.apply(tr, value, ctx.zone()); }
                catch (IllegalArgumentException e) { throw new BodyResolutionException(path, "transform '" + tr.name() + "' failed: " + e.getMessage()); }
            }
            return Optional.of(value);
        }
        if (t.has("default")) return Optional.of(t.get("default").deepCopy());
        String what = describe(t);
        if (t.path("required").asBoolean(false)) throw new BodyResolutionException(path, "required value " + what + " is missing");
        return missing(path, what);
    }

    private JsonNode coalesceItem(JsonNode item, String path) {
        if (item.isObject() && isRef(item)) return lookupRef(item);
        if (item.isTextual() && item.textValue().contains("${")) {
            try { return new BodyResolver(ctx, MissingValuePolicy.OMIT_FIELD).template(item.textValue(), path).orElse(null); }
            catch (BodyResolutionException e) { return null; }
        }
        return item.isNull() ? null : item.deepCopy();
    }

    private Optional<JsonNode> missing(String path, String what) {
        return switch (policy) {
            case FAIL -> throw new BodyResolutionException(path, "value " + what + " is missing");
            case USE_DEFAULT -> Optional.of(NullNode.getInstance());
            case OMIT_FIELD -> Optional.empty();
        };
    }

    private static String describe(JsonNode t) {
        for (String k : REF_KEYS) if (t.has(k)) return k + "=" + t.get(k);
        return t.toString();
    }

    private static List<String> transforms(JsonNode n) {
        if (n == null || n.isNull()) return List.of();
        if (n.isTextual()) return List.of(n.textValue());
        List<String> out = new ArrayList<>();
        n.forEach(x -> out.add(x.asText()));
        return out;
    }

    /** Value of a reference or null when missing. */
    private JsonNode lookupRef(JsonNode t) {
        if (t.has("$signal")) return signal(t.get("$signal").asText());
        if (t.has("$trigger")) return ctx.trigger() == null ? null : JsonPath.compile(t.get("$trigger").asText()).read(ctx.trigger());
        if (t.has("$parent")) {
            if (ctx.parentDeferred()) return DEFERRED;
            return ctx.parent() == null ? null : JsonPath.compile(t.get("$parent").asText()).read(ctx.parent());
        }
        if (t.has("$builtin")) return builtin(t.get("$builtin").asText());
        return null;
    }

    private JsonNode signal(String name) {
        SignalValue sv = ctx.signals().get(name);
        return sv == null ? null : sv.value().toJson();
    }

    private JsonNode builtin(String name) {
        return switch (name) {
            case "correlationKey" -> TextNode.valueOf(ctx.correlationKey());
            case "now" -> TextNode.valueOf(ctx.now().toString());
            case "ruleId" -> TextNode.valueOf(ctx.ruleId());
            case "templateId" -> TextNode.valueOf(ctx.templateId());
            case "occurrence.index" -> ctx.occurrenceDeferred() ? IntNode.valueOf(0)
                    : ctx.occurrenceIndex() == null ? null : IntNode.valueOf(ctx.occurrenceIndex());
            default -> null;
        };
    }

    private Optional<JsonNode> template(String s, String path) {
        StringBuilder out = new StringBuilder();
        Matcher m = PLACEHOLDER.matcher(s);
        int last = 0;
        while (m.find()) {
            out.append(s, last, m.start());
            last = m.end();
            String[] parts = splitPipes(m.group(1));
            JsonNode v = placeholder(parts[0].trim());
            String dflt = null;
            List<Transforms.Transform> trs = new ArrayList<>();
            for (int i = 1; i < parts.length; i++) {
                Transforms.Transform tr = Transforms.parse(parts[i]);
                if (tr.name().equals("default")) dflt = tr.arg(); else trs.add(tr);
            }
            if (v == null) {
                if (dflt != null) { out.append(dflt); continue; }
                switch (policy) {
                    case FAIL -> throw new BodyResolutionException(path, "placeholder ${" + m.group(1) + "} is missing");
                    case USE_DEFAULT -> { continue; }
                    case OMIT_FIELD -> { return Optional.empty(); }
                }
            }
            for (Transforms.Transform tr : trs) {
                try { v = Transforms.apply(tr, v, ctx.zone()); }
                catch (IllegalArgumentException e) { throw new BodyResolutionException(path, "transform '" + tr.name() + "' failed: " + e.getMessage()); }
            }
            out.append(v.isTextual() ? v.textValue() : v.toString());
        }
        out.append(s.substring(last));
        return Optional.of(TextNode.valueOf(out.toString().replace("$${", "${")));
    }

    private JsonNode placeholder(String expr) {
        if (expr.startsWith("signal.")) return signal(expr.substring(7));
        if (expr.startsWith("trigger.")) return ctx.trigger() == null ? null : JsonPath.compile("$." + expr.substring(8)).read(ctx.trigger());
        if (expr.startsWith("parent.")) {
            if (ctx.parentDeferred()) return DEFERRED;
            return ctx.parent() == null ? null : JsonPath.compile("$." + expr.substring(7)).read(ctx.parent());
        }
        return builtin(expr);
    }

    /** Splits on '|' outside quotes. */
    static String[] splitPipes(String s) {
        List<String> parts = new ArrayList<>();
        StringBuilder cur = new StringBuilder();
        char quote = 0;
        for (char c : s.toCharArray()) {
            if (quote != 0) { if (c == quote) quote = 0; cur.append(c); }
            else if (c == '\'' || c == '"') { quote = c; cur.append(c); }
            else if (c == '|') { parts.add(cur.toString()); cur.setLength(0); }
            else cur.append(c);
        }
        parts.add(cur.toString());
        return parts.toArray(String[]::new);
    }

    // ------------------------------------------------------------------ static validation & analysis

    /** Validates a body template at rule save time. */
    public static void validate(JsonNode t, String path, boolean parentAllowed, boolean occurrenceAllowed, List<ValidationError> errors) {
        if (t == null || t.isNull()) return;
        if (t.isObject()) {
            if (isRef(t)) { validateRef(t, path, parentAllowed, occurrenceAllowed, errors); return; }
            t.fields().forEachRemaining(e -> validate(e.getValue(), path + "." + e.getKey(), parentAllowed, occurrenceAllowed, errors));
        } else if (t.isArray()) {
            for (int i = 0; i < t.size(); i++) validate(t.get(i), path + "[" + i + "]", parentAllowed, occurrenceAllowed, errors);
        } else if (t.isTextual() && t.textValue().contains("${")) {
            validateTemplate(t.textValue(), path, parentAllowed, occurrenceAllowed, errors);
        }
    }

    private static void validateRef(JsonNode t, String path, boolean parentAllowed, boolean occAllowed, List<ValidationError> errors) {
        int refs = 0;
        for (Iterator<String> it = t.fieldNames(); it.hasNext(); ) {
            String k = it.next();
            if (REF_KEYS.contains(k)) refs++;
            else if (!OPTION_KEYS.contains(k)) errors.add(new ValidationError(path + "." + k, "unknown key in reference (allowed: " + REF_KEYS + " + " + OPTION_KEYS + ")"));
        }
        if (refs != 1) { errors.add(new ValidationError(path, "reference must contain exactly one of " + REF_KEYS)); return; }
        if (t.has("$literal")) return;
        if (t.path("required").asBoolean(false) && t.has("default")) errors.add(new ValidationError(path, "'required' and 'default' are mutually exclusive"));
        for (String spec : transforms(t.get("transform"))) {
            try { Transforms.parse(spec); } catch (IllegalArgumentException e) { errors.add(new ValidationError(path + ".transform", e.getMessage())); }
        }
        if (t.has("$coalesce")) {
            JsonNode items = t.get("$coalesce");
            if (!items.isArray() || items.isEmpty()) { errors.add(new ValidationError(path + ".$coalesce", "must be a non-empty array")); return; }
            for (int i = 0; i < items.size(); i++) {
                JsonNode item = items.get(i);
                if (item.isObject() && isRef(item)) {
                    if (item.has("$coalesce")) errors.add(new ValidationError(path + ".$coalesce[" + i + "]", "nested $coalesce not allowed"));
                    else validateRef(item, path + ".$coalesce[" + i + "]", parentAllowed, occAllowed, errors);
                } else validate(item, path + ".$coalesce[" + i + "]", parentAllowed, occAllowed, errors);
            }
            return;
        }
        if (t.has("$signal") && (!t.get("$signal").isTextual() || t.get("$signal").textValue().isBlank()))
            errors.add(new ValidationError(path + ".$signal", "must be a signal name"));
        for (String k : List.of("$trigger", "$parent")) {
            if (t.has(k)) {
                try { JsonPath.compile(t.get(k).asText()); } catch (IllegalArgumentException e) { errors.add(new ValidationError(path + "." + k, e.getMessage())); }
            }
        }
        if (t.has("$parent") && !parentAllowed) errors.add(new ValidationError(path + ".$parent", "$parent is only allowed in templates that have a parent"));
        if (t.has("$builtin")) {
            String b = t.get("$builtin").asText();
            if (!BUILTINS.contains(b)) errors.add(new ValidationError(path + ".$builtin", "unknown builtin (allowed: " + BUILTINS + ")"));
            if (b.equals("occurrence.index") && !occAllowed) errors.add(new ValidationError(path + ".$builtin", "occurrence.index only allowed in RECURRING templates"));
        }
    }

    private static void validateTemplate(String s, String path, boolean parentAllowed, boolean occAllowed, List<ValidationError> errors) {
        Matcher m = PLACEHOLDER.matcher(s);
        while (m.find()) {
            String[] parts = splitPipes(m.group(1));
            String expr = parts[0].trim();
            if (expr.startsWith("signal.")) {
                if (expr.length() == 7) errors.add(new ValidationError(path, "empty signal name in ${" + m.group(1) + "}"));
            } else if (expr.startsWith("trigger.") || expr.startsWith("parent.")) {
                if (expr.startsWith("parent.") && !parentAllowed) errors.add(new ValidationError(path, "${parent.*} is only allowed in templates that have a parent"));
                try { JsonPath.compile("$." + expr.substring(expr.indexOf('.') + 1)); } catch (IllegalArgumentException e) { errors.add(new ValidationError(path, e.getMessage())); }
            } else if (!BUILTINS.contains(expr)) {
                errors.add(new ValidationError(path, "unknown placeholder '" + expr + "' (use signal.X, trigger.X, parent.X or " + BUILTINS + ")"));
            } else if (expr.equals("occurrence.index") && !occAllowed) {
                errors.add(new ValidationError(path, "occurrence.index only allowed in RECURRING templates"));
            }
            for (int i = 1; i < parts.length; i++) {
                try { Transforms.parse(parts[i]); } catch (IllegalArgumentException e) { errors.add(new ValidationError(path, e.getMessage())); }
            }
        }
    }

    /** Signal names referenced by a body template. */
    public static Set<String> referencedSignals(JsonNode t) {
        Set<String> out = new LinkedHashSet<>();
        collect(t, out);
        return out;
    }

    private static void collect(JsonNode t, Set<String> out) {
        if (t == null) return;
        if (t.isObject()) {
            if (t.has("$signal")) out.add(t.get("$signal").asText());
            if (t.has("$literal")) return;
            t.elements().forEachRemaining(e -> collect(e, out));
        } else if (t.isArray()) t.elements().forEachRemaining(e -> collect(e, out));
        else if (t.isTextual()) {
            Matcher m = PLACEHOLDER.matcher(t.textValue());
            while (m.find()) {
                String e = splitPipes(m.group(1))[0].trim();
                if (e.startsWith("signal.")) out.add(e.substring(7));
            }
        }
    }

    public static boolean usesParent(JsonNode t) {
        if (t == null) return false;
        String s = t.toString();
        return s.contains("\"$parent\"") || s.contains("${parent.");
    }

    public static boolean usesOccurrence(JsonNode t) {
        return t != null && t.toString().contains("occurrence.index");
    }
}
