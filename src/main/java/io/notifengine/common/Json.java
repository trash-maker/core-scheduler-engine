package io.notifengine.common;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

/** Shared, framework-agnostic Jackson mapper. */
public final class Json {
    public static final ObjectMapper MAPPER = new ObjectMapper()
            .findAndRegisterModules()
            .configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true)
            .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);

    private Json() {}

    public static ObjectNode object() { return JsonNodeFactory.instance.objectNode(); }

    public static ArrayNode array() { return JsonNodeFactory.instance.arrayNode(); }

    public static JsonNode parse(String s) {
        try {
            return MAPPER.readTree(s);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("invalid JSON: " + e.getOriginalMessage(), e);
        }
    }

    public static String write(Object node) {
        try {
            return MAPPER.writeValueAsString(node);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }

    public static JsonNode toTree(Object o) { return MAPPER.valueToTree(o); }

    public static <T> T convert(JsonNode n, Class<T> type) {
        try {
            return MAPPER.treeToValue(n, type);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException(e.getOriginalMessage(), e);
        }
    }
}
