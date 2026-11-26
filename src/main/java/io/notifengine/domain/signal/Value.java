package io.notifengine.domain.signal;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.DecimalNode;
import com.fasterxml.jackson.databind.node.TextNode;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.time.temporal.Temporal;
import java.util.Objects;

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
