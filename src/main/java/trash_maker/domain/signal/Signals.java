package trash_maker.domain.signal;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.DecimalNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import trash_maker.common.Json;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.time.temporal.Temporal;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.regex.Pattern;

/** Signals: typed values, the signals.in envelope, stored values and pure batch selection. */
public final class Signals {
    private Signals() {}

    public enum SignalKind { KPI, DATE }

    /** A typed signal value: number, boolean, string or date. */
    public sealed interface Value permits Value.Num, Value.Bool, Value.Str, Value.Date {

        String typeName();

        JsonNode toJson();

        /** Display form used by string templates. */
        String asText();

        record Num(BigDecimal v) implements Value {
            public Num { Objects.requireNonNull(v); }
            public String typeName() { return "number"; }
            public JsonNode toJson() { return DecimalNode.valueOf(v); }
            public String asText() { return v.stripTrailingZeros().toPlainString(); }
        }

        record Bool(boolean v) implements Value {
            public String typeName() { return "boolean"; }
            public JsonNode toJson() { return BooleanNode.valueOf(v); }
            public String asText() { return Boolean.toString(v); }
        }

        record Str(String v) implements Value {
            public Str { Objects.requireNonNull(v); }
            public String typeName() { return "string"; }
            public JsonNode toJson() { return TextNode.valueOf(v); }
            public String asText() { return v; }
        }

        /** Date value: a LocalDate, LocalDateTime (interpreted in the rule zone) or OffsetDateTime. */
        record Date(Temporal t) implements Value {
            public Date {
                if (!(t instanceof LocalDate || t instanceof LocalDateTime || t instanceof OffsetDateTime)) {
                    throw new IllegalArgumentException("unsupported temporal " + t);
                }
            }
            public static Date of(Instant i) { return new Date(OffsetDateTime.ofInstant(i, java.time.ZoneOffset.UTC)); }
            public String typeName() { return "date"; }
            public JsonNode toJson() { return TextNode.valueOf(t.toString()); }
            public String asText() { return t.toString(); }

            public ZonedDateTime at(ZoneId zone) {
                if (t instanceof LocalDate d) return d.atStartOfDay(zone);
                if (t instanceof LocalDateTime ldt) return ZonedDateTime.ofLocal(ldt, zone, null);
                return ((OffsetDateTime) t).atZoneSameInstant(zone);
            }

            public Instant instant(ZoneId zone) { return at(zone).toInstant(); }

            public static Date parse(String s) {
                String x = s.trim();
                try { return new Date(OffsetDateTime.parse(x)); } catch (DateTimeParseException ignored) { }
                try { return new Date(OffsetDateTime.ofInstant(Instant.parse(x), java.time.ZoneOffset.UTC)); } catch (DateTimeParseException ignored) { }
                try { return new Date(LocalDateTime.parse(x)); } catch (DateTimeParseException ignored) { }
                try { return new Date(LocalDate.parse(x)); } catch (DateTimeParseException ignored) { }
                throw new IllegalArgumentException("not an ISO-8601 date: " + s);
            }
        }

        static Value fromJson(SignalKind kind, JsonNode n) {
            if (n == null || n.isNull() || n.isMissingNode()) throw new IllegalArgumentException("value is null");
            if (kind == SignalKind.DATE) {
                if (!n.isTextual()) throw new IllegalArgumentException("DATE value must be an ISO-8601 string");
                return Date.parse(n.textValue());
            }
            if (n.isNumber()) return new Num(n.decimalValue());
            if (n.isBoolean()) return new Bool(n.booleanValue());
            if (n.isTextual()) return new Str(n.textValue());
            throw new IllegalArgumentException("KPI value must be number, boolean or string");
        }

        /** Literal in a rule: numbers, booleans, strings; strings parseable as dates become dates when compared to dates. */
        static Value literal(JsonNode n) {
            if (n.isNumber()) return new Num(n.decimalValue());
            if (n.isBoolean()) return new Bool(n.booleanValue());
            if (n.isTextual()) return new Str(n.textValue());
            throw new IllegalArgumentException("literal must be number, boolean or string");
        }
    }

    /** Latest stored value of one signal for one entity. */
    public record SignalValue(String name, SignalKind kind, Value value, Instant occurredAt) {}

    /** Envelope of a message on signals.in (schema: schemas/signal-v1.json). */
    public record SignalEnvelope(String signalId, String tenantId, String correlationKey, String signal,
                                 SignalKind kind, JsonNode rawValue, Value value, Instant occurredAt) {

        private static final Pattern NAME = Pattern.compile("[A-Za-z0-9_.:-]{1,128}");
        private static final int MAX_KEY = 256;

        public static SignalEnvelope parse(JsonNode n) {
            String signalId = text(n, "signal_id");
            String tenant = text(n, "tenant_id");
            String key = text(n, "correlation_key");
            String signal = text(n, "signal");
            String kindS = text(n, "kind");
            String occurred = text(n, "occurred_at");
            if (signalId.length() > MAX_KEY || key.length() > MAX_KEY) throw new IllegalArgumentException("signal_id/correlation_key too long");
            if (!NAME.matcher(tenant).matches()) throw new IllegalArgumentException("invalid tenant_id");
            if (!NAME.matcher(signal).matches()) throw new IllegalArgumentException("invalid signal name");
            SignalKind kind;
            try { kind = SignalKind.valueOf(kindS); } catch (IllegalArgumentException e) { throw new IllegalArgumentException("kind must be KPI or DATE"); }
            Instant at;
            try { at = Instant.parse(occurred); } catch (DateTimeParseException e) {
                try { at = java.time.OffsetDateTime.parse(occurred).toInstant(); } catch (DateTimeParseException e2) {
                    throw new IllegalArgumentException("occurred_at must be ISO-8601 instant");
                }
            }
            JsonNode raw = n.get("value");
            Value v = Value.fromJson(kind, raw);
            return new SignalEnvelope(signalId, tenant, key, signal, kind, raw, v, at);
        }

        private static String text(JsonNode n, String f) {
            JsonNode v = n.get(f);
            if (v == null || !v.isTextual() || v.textValue().isBlank()) throw new IllegalArgumentException("missing or invalid field '" + f + "'");
            return v.textValue();
        }

        /** JSON form exposed to {"$trigger": "$.x"} references. */
        public ObjectNode toJson() {
            ObjectNode o = Json.object();
            o.put("signal_id", signalId);
            o.put("tenant_id", tenantId);
            o.put("correlation_key", correlationKey);
            o.put("signal", signal);
            o.put("kind", kind.name());
            o.set("value", rawValue);
            o.put("occurred_at", occurredAt.toString());
            return o;
        }

        public SignalValue asSignalValue() { return new SignalValue(signal, kind, value, occurredAt); }
    }

    /**
     * Pure selection of a signal batch: idempotency (inbox result + duplicates inside the batch), discarding of
     * unreferenced signals, and reduction to the latest value per (tenant, key, signal) (ties: last in batch wins).
     *
     * @param <T> the raw message carried along (for the trigger reference / DLQ)
     */
    public static final class SignalBatch<T> {
        public record Item<T>(SignalEnvelope env, T raw) {}

        public record Selection<T>(List<Item<T>> accepted, Map<String, Item<T>> latest, int duplicates, int unreferenced) {}

        private SignalBatch() {}

        public static String inboxKey(SignalEnvelope e) { return e.tenantId() + "|" + e.signalId(); }

        public static String stateKey(SignalEnvelope e) { return e.tenantId() + "|" + e.correlationKey() + "|" + e.signal(); }

        /**
         * @param fresh       inbox keys ({@link #inboxKey}) that were not seen before
         * @param referenced  (tenant, signal) -> referenced by an enabled rule
         */
        public static <T> Selection<T> select(List<Item<T>> valid, Set<String> fresh, BiPredicate<String, String> referenced,
                                              boolean storeUnreferenced) {
            List<Item<T>> accepted = new ArrayList<>();
            Set<String> seen = new HashSet<>();
            int dups = 0, unref = 0;
            for (Item<T> it : valid) {
                String id = inboxKey(it.env());
                if (!fresh.contains(id) || !seen.add(id)) { dups++; continue; }
                if (!storeUnreferenced && !referenced.test(it.env().tenantId(), it.env().signal())) { unref++; continue; }
                accepted.add(it);
            }
            Map<String, Item<T>> latest = new LinkedHashMap<>();
            for (Item<T> it : accepted) {
                String k = stateKey(it.env());
                Item<T> prev = latest.get(k);
                if (prev == null || !it.env().occurredAt().isBefore(prev.env().occurredAt())) latest.put(k, it);
            }
            return new Selection<>(accepted, latest, dups, unref);
        }

        /** The message reported as {@code $trigger}: the newest of the entity's changed signals. */
        public static <T> Item<T> trigger(List<Item<T>> changed) {
            return changed.stream().max((a, b) -> a.env().occurredAt().compareTo(b.env().occurredAt())).orElseThrow();
        }
    }
}
