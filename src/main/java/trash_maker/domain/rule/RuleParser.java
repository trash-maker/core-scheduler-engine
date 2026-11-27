package trash_maker.domain.rule;

import com.fasterxml.jackson.databind.JsonNode;
import trash_maker.common.IsoAmount.Durations;
import trash_maker.common.IsoAmount;
import trash_maker.common.Validation.ValidationError;
import trash_maker.common.Validation.ValidationException;
import trash_maker.domain.body.BodyResolver;
import trash_maker.domain.expr.DslParser;
import trash_maker.domain.expr.Evaluator;
import trash_maker.domain.expr.Expr.Node;
import trash_maker.domain.expr.Expr.OnMissing;
import trash_maker.domain.expr.ExpressionCompiler;
import trash_maker.domain.notification.Lifecycle.NotificationStatus;
import trash_maker.domain.recurrence.Recurrence.CronRecurrence;
import trash_maker.domain.rule.Rule.CompiledRule;
import trash_maker.domain.rule.Rule.Enums.NotificationType;
import trash_maker.domain.rule.Rule.Enums.ParentTrigger;
import trash_maker.domain.rule.Rule.Enums;
import trash_maker.domain.rule.Rule.ParentSpec;
import trash_maker.domain.rule.Rule.RecurrenceSpec;
import trash_maker.domain.rule.Rule.RetryPolicy;
import trash_maker.domain.rule.Rule.RuleDefinition;
import trash_maker.domain.rule.Rule.TemplateDef;
import trash_maker.domain.rule.Rule.TimeSpec;
import trash_maker.domain.rule.Rule.TriggerDef;
import trash_maker.domain.rule.Rule.UntilSpec;

import java.time.DateTimeException;
import java.time.Duration;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Pattern;

/** Parses and validates a rule JSON document into a {@link CompiledRule}, reporting precise error paths. */
public final class RuleParser {
    public static final Pattern ID = Pattern.compile("[A-Za-z0-9_.-]{1,64}");
    public static final Pattern ACTION = Pattern.compile("[A-Z0-9_.-]{1,64}");
    public static final int MAX_DEPTH = 5;
    public static final int MAX_TEMPLATES = 32;
    public static final Set<NotificationStatus> WHILE_STATES = EnumSet.of(NotificationStatus.FAILED,
            NotificationStatus.RETRY_SCHEDULED, NotificationStatus.RETRIES_EXHAUSTED, NotificationStatus.PUBLISHED, NotificationStatus.SCHEDULED);

    private static final Set<String> RULE_KEYS = Set.of("id", "version", "tenantId", "enabled", "timezone", "onMissingValue", "trigger", "templates", "description");
    private static final Set<String> TRIGGER_KEYS = Set.of("mode", "onMissing", "cooldown", "expression", "expressionText");
    private static final Set<String> TEMPLATE_KEYS = Set.of("id", "type", "action", "dueAt", "dueAtFloor", "expiresAt", "every", "cron",
            "startAt", "until", "maxOccurrences", "misfirePolicy", "misfireGrace", "cancelOnEvents", "retryPolicy", "parent", "body",
            "bodySchema", "onMissingValue", "description");
    private static final Set<String> RETRY_KEYS = Set.of("maxAttempts", "backoff", "initialDelay", "multiplier", "maxDelay", "jitter", "attemptTimeout");
    private static final Set<String> PARENT_KEYS = Set.of("templateId", "trigger", "delay", "states", "until", "maxOccurrences", "once");
    private static final Set<String> TIME_KEYS = Set.of("from", "offset", "atTime");

    /**
     * @param allowedActions empty = any action matching the pattern
     * @param schemaChecker  returns an error message if the given JSON is not a valid JSON Schema, else null
     */
    public record Options(Set<String> allowedActions, Function<JsonNode, String> schemaChecker) {
        public static final Options DEFAULT = new Options(Set.of(), s -> null);
    }

    private final List<ValidationError> errors = new ArrayList<>();
    private final Options options;

    private RuleParser(Options options) { this.options = options; }

    public static CompiledRule compile(JsonNode json, Options options) {
        RuleParser p = new RuleParser(options);
        CompiledRule r = p.rule(json);
        if (!p.errors.isEmpty()) throw new ValidationException(p.errors);
        return r;
    }

    public static List<ValidationError> validate(JsonNode json, Options options) {
        RuleParser p = new RuleParser(options);
        p.rule(json);
        return p.errors;
    }

    private void err(String path, String msg) { errors.add(new ValidationError(path, msg)); }

    /** Re-parses a stored template snapshot (used at child activation / occurrences). */
    public static TemplateDef parseTemplate(JsonNode raw, Enums.MissingValuePolicy ruleMvp) {
        RuleParser p = new RuleParser(Options.DEFAULT);
        TemplateDef t = p.template(raw, "$", ruleMvp);
        if (!p.errors.isEmpty()) throw new ValidationException(p.errors);
        return t;
    }

    private void unknownKeys(JsonNode n, Set<String> allowed, String path) {
        for (Iterator<String> it = n.fieldNames(); it.hasNext(); ) {
            String f = it.next();
            if (!allowed.contains(f)) err(path + "." + f, "unknown field");
        }
    }

    private CompiledRule rule(JsonNode j) {
        if (j == null || !j.isObject()) { err("$", "rule must be a JSON object"); return null; }
        unknownKeys(j, RULE_KEYS, "$");
        String id = str(j, "id", "$.id", true);
        if (id != null && !ID.matcher(id).matches()) err("$.id", "must match " + ID.pattern());
        String tenant = str(j, "tenantId", "$.tenantId", true);
        if (tenant != null && !ID.matcher(tenant).matches()) err("$.tenantId", "must match " + ID.pattern());
        int version = j.path("version").asInt(1);
        if (j.has("version") && (!j.get("version").canConvertToInt() || version < 1)) err("$.version", "must be an integer >= 1");
        boolean enabled = j.path("enabled").asBoolean(true);
        ZoneId zone = ZoneOffset.UTC;
        if (j.has("timezone")) {
            try { zone = ZoneId.of(j.get("timezone").asText()); } catch (DateTimeException e) { err("$.timezone", "unknown time zone '" + j.get("timezone").asText() + "'"); }
        }
        Enums.MissingValuePolicy mvp = enumOf(Enums.MissingValuePolicy.class, j.get("onMissingValue"), "$.onMissingValue", Enums.MissingValuePolicy.FAIL);

        TriggerDef trigger = trigger(j.get("trigger"), zone);

        List<TemplateDef> templates = new ArrayList<>();
        JsonNode ts = j.get("templates");
        if (ts == null || !ts.isArray() || ts.isEmpty()) err("$.templates", "required non-empty array");
        else if (ts.size() > MAX_TEMPLATES) err("$.templates", "at most " + MAX_TEMPLATES + " templates");
        else for (int i = 0; i < ts.size(); i++) {
            TemplateDef t = template(ts.get(i), "$.templates[" + i + "]", mvp);
            if (t != null) templates.add(t);
        }
        RuleDefinition def = new RuleDefinition(id, version, tenant, enabled, zone, mvp, trigger, templates, j);
        return derive(def);
    }

    private TriggerDef trigger(JsonNode t, ZoneId zone) {
        String p = "$.trigger";
        if (t == null || !t.isObject()) { err(p, "required object"); return null; }
        unknownKeys(t, TRIGGER_KEYS, p);
        Enums.FiringMode mode = enumOf(Enums.FiringMode.class, t.get("mode"), p + ".mode", Enums.FiringMode.ON_BECOMES_TRUE);
        OnMissing onMissing = enumOf(OnMissing.class, t.get("onMissing"), p + ".onMissing", OnMissing.FALSE);
        Duration cooldown = duration(t.get("cooldown"), p + ".cooldown");
        if (mode == Enums.FiringMode.EVERY_EVALUATION_TRUE && cooldown == null) err(p + ".cooldown", "required for EVERY_EVALUATION_TRUE (use PT0S for none)");
        if (mode == Enums.FiringMode.ON_BECOMES_TRUE && cooldown != null) err(p + ".cooldown", "only valid with EVERY_EVALUATION_TRUE");
        JsonNode exprJson = t.get("expression");
        String exprPath = p + ".expression";
        if (t.has("expressionText")) {
            if (exprJson != null) err(p, "use either 'expression' or 'expressionText'");
            try { exprJson = DslParser.parse(t.get("expressionText").asText()); }
            catch (DslParser.DslException e) { err(p + ".expressionText", e.getMessage()); return null; }
        }
        if (exprJson == null) { err(exprPath, "required"); return null; }
        Node expr = ExpressionCompiler.compile(exprJson, exprPath, zone, errors);
        return new TriggerDef(mode, onMissing, cooldown, expr, exprJson);
    }

    private TemplateDef template(JsonNode t, String p, Enums.MissingValuePolicy ruleMvp) {
        if (t == null || !t.isObject()) { err(p, "template must be an object"); return null; }
        unknownKeys(t, TEMPLATE_KEYS, p);
        String id = str(t, "id", p + ".id", true);
        if (id != null && !ID.matcher(id).matches()) err(p + ".id", "must match " + ID.pattern());
        NotificationType type = enumOf(NotificationType.class, t.get("type"), p + ".type", null);
        if (type == null && !t.has("type")) err(p + ".type", "required: IMMEDIATE, SCHEDULED or RECURRING");
        String action = str(t, "action", p + ".action", true);
        if (action != null) {
            if (!ACTION.matcher(action).matches()) err(p + ".action", "must match " + ACTION.pattern());
            else if (!options.allowedActions().isEmpty() && !options.allowedActions().contains(action)) err(p + ".action", "action not in allow-list");
        }
        TimeSpec dueAt = t.has("dueAt") ? timeSpec(t.get("dueAt"), p + ".dueAt") : null;
        boolean floor = false;
        if (t.has("dueAtFloor")) {
            if (!"NOW".equals(t.get("dueAtFloor").asText())) err(p + ".dueAtFloor", "only \"NOW\" is supported");
            else floor = true;
        }
        TimeSpec expiresAt = null;
        IsoAmount expiresAfter = null;
        if (t.has("expiresAt")) {
            JsonNode e = t.get("expiresAt");
            if (e.isTextual() && e.textValue().startsWith("P") || e.isTextual() && e.textValue().startsWith("-")) {
                expiresAfter = amount(e, p + ".expiresAt");
                if (expiresAfter != null && !expiresAfter.isPositive()) err(p + ".expiresAt", "relative expiry must be positive");
            } else expiresAt = timeSpec(e, p + ".expiresAt");
        }
        RecurrenceSpec rec = null;
        if (t.has("every") && t.has("cron")) err(p, "use either 'every' or 'cron'");
        else if (t.has("cron")) {
            try { new CronRecurrence(t.get("cron").asText(), ZoneOffset.UTC); rec = new RecurrenceSpec(t.get("cron").asText(), null); }
            catch (IllegalArgumentException e) { err(p + ".cron", e.getMessage()); }
        } else if (t.has("every")) {
            IsoAmount a = amount(t.get("every"), p + ".every");
            if (a != null && !a.isPositive()) err(p + ".every", "must be positive");
            else if (a != null) rec = new RecurrenceSpec(null, a);
        }
        TimeSpec startAt = t.has("startAt") ? timeSpec(t.get("startAt"), p + ".startAt") : null;
        UntilSpec until = t.has("until") ? until(t.get("until"), p + ".until") : null;
        Integer maxOcc = posInt(t.get("maxOccurrences"), p + ".maxOccurrences");
        Enums.MisfirePolicy misfire = enumOf(Enums.MisfirePolicy.class, t.get("misfirePolicy"), p + ".misfirePolicy", Enums.MisfirePolicy.SKIP);
        Duration misfireGrace = duration(t.get("misfireGrace"), p + ".misfireGrace");
        List<String> cancelOn = new ArrayList<>();
        if (t.has("cancelOnEvents")) {
            JsonNode c = t.get("cancelOnEvents");
            if (!c.isArray()) err(p + ".cancelOnEvents", "must be an array of signal names");
            else for (int i = 0; i < c.size(); i++) {
                if (!c.get(i).isTextual() || c.get(i).textValue().isBlank()) err(p + ".cancelOnEvents[" + i + "]", "signal name required");
                else cancelOn.add(c.get(i).textValue());
            }
        }
        RetryPolicy retry = retry(t.get("retryPolicy"), p + ".retryPolicy");
        ParentSpec parent = t.has("parent") ? parent(t.get("parent"), p + ".parent") : null;
        Enums.MissingValuePolicy mvp = enumOf(Enums.MissingValuePolicy.class, t.get("onMissingValue"), p + ".onMissingValue", ruleMvp);
        JsonNode body = t.get("body");
        if (body != null && !body.isObject()) err(p + ".body", "must be a JSON object");
        JsonNode schema = t.get("bodySchema");
        if (schema != null) {
            String e = options.schemaChecker().apply(schema);
            if (e != null) err(p + ".bodySchema", e);
        }
        // type specific checks
        if (type != null) switch (type) {
            case IMMEDIATE -> {
                if (dueAt != null) err(p + ".dueAt", "not allowed for IMMEDIATE (use SCHEDULED)");
                if (rec != null) err(p, "'every'/'cron' only allowed for RECURRING");
            }
            case SCHEDULED -> {
                if (dueAt == null && parent == null) err(p + ".dueAt", "required for SCHEDULED");
                if (rec != null) err(p, "'every'/'cron' only allowed for RECURRING");
            }
            case RECURRING -> {
                if (rec == null) err(p, "RECURRING requires 'every' or 'cron'");
                if (dueAt != null) err(p + ".dueAt", "not allowed for RECURRING (use startAt)");
                if (expiresAt != null || expiresAfter != null) err(p + ".expiresAt", "not allowed for RECURRING (use until)");
                if (parent != null && parent.trigger() != ParentTrigger.WHILE_IN_STATE) err(p + ".parent.trigger", "a RECURRING child must use WHILE_IN_STATE");
            }
        }
        if (type != NotificationType.RECURRING) {
            if (startAt != null) err(p + ".startAt", "only allowed for RECURRING");
            if (until != null) err(p + ".until", "only allowed for RECURRING");
            if (maxOcc != null) err(p + ".maxOccurrences", "only allowed for RECURRING");
        }
        if (parent != null && parent.trigger() == ParentTrigger.WHILE_IN_STATE) {
            if (type != NotificationType.RECURRING) err(p + ".parent.trigger", "WHILE_IN_STATE requires type RECURRING");
            if (parent.until() == null && parent.maxOccurrences() == null && until == null && maxOcc == null)
                err(p + ".parent", "WHILE_IN_STATE requires 'until' or 'maxOccurrences'");
            if (startAt != null) err(p + ".startAt", "not allowed with WHILE_IN_STATE (series starts on state entry + delay)");
        }
        BodyResolver.validate(body, p + ".body", parent != null, type == NotificationType.RECURRING, errors);
        return new TemplateDef(id, type, action, dueAt, floor, expiresAt, expiresAfter, rec, startAt, until, maxOcc, misfire,
                misfireGrace, List.copyOf(cancelOn), retry, parent, body, schema, mvp, t);
    }

    private RetryPolicy retry(JsonNode r, String p) {
        if (r == null) return RetryPolicy.DEFAULT;
        if (!r.isObject()) { err(p, "must be an object"); return RetryPolicy.DEFAULT; }
        unknownKeys(r, RETRY_KEYS, p);
        int max = r.path("maxAttempts").asInt(1);
        if (r.has("maxAttempts") && (!r.get("maxAttempts").canConvertToInt() || max < 1 || max > 100)) err(p + ".maxAttempts", "must be 1..100");
        Enums.BackoffKind b = enumOf(Enums.BackoffKind.class, r.get("backoff"), p + ".backoff", Enums.BackoffKind.FIXED);
        Duration init = duration(r.get("initialDelay"), p + ".initialDelay");
        if (init == null) init = Duration.ofMinutes(1);
        double mult = r.path("multiplier").asDouble(2.0);
        if (mult < 1.0 || mult > 100) err(p + ".multiplier", "must be in [1, 100]");
        Duration maxDelay = duration(r.get("maxDelay"), p + ".maxDelay");
        if (maxDelay == null) maxDelay = Duration.ofHours(1);
        double jitter = r.path("jitter").asDouble(0);
        if (jitter < 0 || jitter > 1) err(p + ".jitter", "must be a fraction in [0, 1]");
        Duration timeout = duration(r.get("attemptTimeout"), p + ".attemptTimeout");
        if (timeout != null && timeout.isZero()) err(p + ".attemptTimeout", "must be > 0");
        return new RetryPolicy(Math.max(1, Math.min(100, max)), b, init, mult, maxDelay, jitter, timeout);
    }

    private ParentSpec parent(JsonNode n, String p) {
        if (!n.isObject()) { err(p, "must be an object"); return null; }
        unknownKeys(n, PARENT_KEYS, p);
        String tid = str(n, "templateId", p + ".templateId", true);
        ParentTrigger trig = enumOf(ParentTrigger.class, n.get("trigger"), p + ".trigger", null);
        if (trig == null && !n.has("trigger")) err(p + ".trigger", "required");
        Duration delay = duration(n.get("delay"), p + ".delay");
        Set<NotificationStatus> states = EnumSet.noneOf(NotificationStatus.class);
        if (n.has("states")) {
            JsonNode s = n.get("states");
            if (!s.isArray() || s.isEmpty()) err(p + ".states", "must be a non-empty array");
            else for (int i = 0; i < s.size(); i++) {
                NotificationStatus st = enumOf(NotificationStatus.class, s.get(i), p + ".states[" + i + "]", null);
                if (st != null && !WHILE_STATES.contains(st)) err(p + ".states[" + i + "]", "allowed: " + WHILE_STATES);
                else if (st != null) states.add(st);
            }
        }
        if (trig == ParentTrigger.WHILE_IN_STATE && !n.has("states")) err(p + ".states", "required for WHILE_IN_STATE");
        if (trig != ParentTrigger.WHILE_IN_STATE && trig != null) {
            for (String k : List.of("states", "until", "maxOccurrences", "once")) if (n.has(k)) err(p + "." + k, "only valid with WHILE_IN_STATE");
        }
        UntilSpec until = n.has("until") ? until(n.get("until"), p + ".until") : null;
        Integer maxOcc = posInt(n.get("maxOccurrences"), p + ".maxOccurrences");
        boolean once = n.path("once").asBoolean(false);
        return new ParentSpec(tid, trig, delay == null ? Duration.ZERO : delay, states, until, maxOcc, once);
    }

    private UntilSpec until(JsonNode n, String p) {
        if (n.isTextual() && (n.textValue().startsWith("P") || n.textValue().startsWith("p"))) {
            IsoAmount a = amount(n, p);
            if (a != null && !a.isPositive()) err(p, "must be positive");
            return a == null ? null : new UntilSpec(null, a);
        }
        TimeSpec ts = timeSpec(n, p);
        return ts == null ? null : new UntilSpec(ts, null);
    }

    private TimeSpec timeSpec(JsonNode n, String p) {
        if (n.isTextual()) return new TimeSpec(base(n.textValue(), p), IsoAmount.ZERO, null);
        if (!n.isObject()) { err(p, "must be \"NOW\", \"$signal:name\" or {from, offset, atTime}"); return null; }
        unknownKeys(n, TIME_KEYS, p);
        String from = n.has("from") ? base(n.get("from").asText(), p + ".from") : null;
        IsoAmount off = n.has("offset") ? amount(n.get("offset"), p + ".offset") : IsoAmount.ZERO;
        LocalTime at = null;
        if (n.has("atTime")) {
            try { at = LocalTime.parse(n.get("atTime").asText()); } catch (DateTimeParseException e) { err(p + ".atTime", "must be HH:mm[:ss]"); }
        }
        return new TimeSpec(from, off == null ? IsoAmount.ZERO : off, at);
    }

    private String base(String s, String p) {
        if (s.equals("NOW")) return null;
        if (s.startsWith("$signal:") && s.length() > 8) return s.substring(8);
        err(p, "must be \"NOW\" or \"$signal:<DATE signal>\"");
        return null;
    }

    // ------------------------------------------------------------------ derivation (graph checks, indexes)

    private CompiledRule derive(RuleDefinition def) {
        Map<String, TemplateDef> byId = new LinkedHashMap<>();
        for (int i = 0; i < def.templates().size(); i++) {
            TemplateDef t = def.templates().get(i);
            if (t.id() == null) continue;
            if (byId.putIfAbsent(t.id(), t) != null) err("$.templates[" + i + "].id", "duplicate template id '" + t.id() + "'");
        }
        Map<String, List<TemplateDef>> children = new HashMap<>();
        List<TemplateDef> roots = new ArrayList<>();
        Map<String, Integer> depth = new HashMap<>();
        for (int i = 0; i < def.templates().size(); i++) {
            TemplateDef t = def.templates().get(i);
            String p = "$.templates[" + i + "]";
            if (t.parent() == null) { roots.add(t); continue; }
            String pid = t.parent().templateId();
            if (pid == null) continue;
            TemplateDef parent = byId.get(pid);
            if (pid.equals(t.id())) err(p + ".parent.templateId", "a template cannot be its own parent");
            else if (parent == null) err(p + ".parent.templateId", "unknown template '" + pid + "'");
            else if (parent.type() == NotificationType.RECURRING) err(p + ".parent.templateId", "a RECURRING template cannot be a parent");
            else children.computeIfAbsent(pid, k -> new ArrayList<>()).add(t);
        }
        for (int i = 0; i < def.templates().size(); i++) {
            TemplateDef t = def.templates().get(i);
            if (t.id() == null) continue;
            Set<String> seen = new HashSet<>();
            TemplateDef cur = t;
            int d = 0;
            boolean cycle = false;
            while (cur != null && cur.parent() != null) {
                if (!seen.add(cur.id())) { cycle = true; break; }
                cur = byId.get(cur.parent().templateId());
                d++;
            }
            if (cycle) err("$.templates[" + i + "].parent", "cycle detected in parent chain");
            else if (d > MAX_DEPTH) err("$.templates[" + i + "].parent", "parent chain depth " + d + " exceeds " + MAX_DEPTH);
            depth.put(t.id(), d);
        }
        if (roots.isEmpty() && !def.templates().isEmpty()) err("$.templates", "at least one template without parent is required");

        Set<String> exprSignals = new LinkedHashSet<>();
        boolean[] hasNow = {false};
        boolean[] timed = {false};
        if (def.trigger() != null && def.trigger().expression() != null) {
            Evaluator.walk(def.trigger().expression(), n -> {
                if (n instanceof Node.Compare c) {
                    exprSignals.add(c.signal());
                    if (c.maxAge() != null || c.stateful() || c.operands().stream().anyMatch(o -> o instanceof Node.NowRelative)) timed[0] = true;
                    c.operands().forEach(o -> { if (o instanceof Node.SignalRef r) exprSignals.add(r.signal()); });
                } else if (n instanceof Node.DateMatches dm) {
                    if (dm.isNow()) hasNow[0] = true; else exprSignals.add(dm.sourceSignal());
                }
            });
        }
        Set<String> ctxSignals = new LinkedHashSet<>();
        Map<String, Set<String>> cancel = new HashMap<>();
        for (TemplateDef t : def.templates()) {
            ctxSignals.addAll(BodyResolver.referencedSignals(t.body()));
            for (TimeSpec ts : new TimeSpec[]{t.dueAt(), t.expiresAt(), t.startAt(), t.until() == null ? null : t.until().at(),
                    t.parent() == null || t.parent().until() == null ? null : t.parent().until().at()}) {
                if (ts != null && ts.fromSignal() != null) ctxSignals.add(ts.fromSignal());
            }
            for (String s : t.cancelOnEvents()) cancel.computeIfAbsent(s, k -> new LinkedHashSet<>()).add(t.id());
        }
        return new CompiledRule(def, exprSignals, ctxSignals, cancel, hasNow[0], timed[0], byId, roots, children, depth);
    }

    // ------------------------------------------------------------------ primitives

    private String str(JsonNode n, String f, String p, boolean required) {
        JsonNode v = n.get(f);
        if (v == null || v.isNull()) { if (required) err(p, "required"); return null; }
        if (!v.isTextual() || v.textValue().isBlank()) { err(p, "must be a non-empty string"); return null; }
        return v.textValue();
    }

    private <E extends Enum<E>> E enumOf(Class<E> type, JsonNode n, String p, E dflt) {
        if (n == null || n.isNull()) return dflt;
        try { return Enum.valueOf(type, n.asText()); }
        catch (IllegalArgumentException e) {
            err(p, "must be one of " + java.util.Arrays.toString(type.getEnumConstants()));
            return dflt;
        }
    }

    private Duration duration(JsonNode n, String p) {
        if (n == null || n.isNull()) return null;
        try {
            Duration d = Durations.parse(n.asText());
            if (d.isNegative()) { err(p, "must not be negative"); return null; }
            return d;
        } catch (IllegalArgumentException e) { err(p, e.getMessage()); return null; }
    }

    private IsoAmount amount(JsonNode n, String p) {
        try { return IsoAmount.parse(n.asText()); }
        catch (RuntimeException e) { err(p, "invalid ISO-8601 amount '" + n.asText() + "'"); return null; }
    }

    private Integer posInt(JsonNode n, String p) {
        if (n == null || n.isNull()) return null;
        if (!n.canConvertToInt() || n.asInt() < 1) { err(p, "must be a positive integer"); return null; }
        return n.asInt();
    }
}
