package io.notifengine.domain.body;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.List;

/** Port: validates a resolved body against a template's JSON Schema; returns error messages (empty = valid). */
public interface BodySchemaValidator {
    BodySchemaValidator NONE = (schema, body) -> List.of();

    List<String> validate(JsonNode schema, JsonNode body);
}
