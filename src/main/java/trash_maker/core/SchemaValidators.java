package trash_maker.core;

import jakarta.inject.Singleton;

import com.fasterxml.jackson.databind.JsonNode;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
import trash_maker.domain.body.BodySchemaValidator;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/** JSON Schema (2020-12) validation of resolved bodies; compiled schemas are cached. No remote $ref resolution. */
@Singleton
public class SchemaValidators implements BodySchemaValidator {
    private static final JsonSchemaFactory FACTORY = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012,
            b -> b.schemaLoaders(l -> l.add(uri -> { throw new IllegalArgumentException("remote $ref not allowed: " + uri); })));
    private final Map<String, JsonSchema> cache = new ConcurrentHashMap<>();

    @Override
    public List<String> validate(JsonNode schema, JsonNode body) {
        JsonSchema s = cache.computeIfAbsent(schema.toString(), k -> FACTORY.getSchema(schema));
        Set<ValidationMessage> errs = s.validate(body);
        return errs.stream().map(ValidationMessage::getMessage).sorted().limit(20).toList();
    }

    /** Save-time check that a JSON document is a usable schema. */
    public static Function<JsonNode, String> checker() {
        return schema -> {
            if (!schema.isObject()) return "bodySchema must be a JSON object";
            try {
                FACTORY.getSchema(schema).validate(com.fasterxml.jackson.databind.node.NullNode.getInstance());
                return null;
            } catch (RuntimeException e) {
                return "invalid JSON Schema: " + e.getMessage();
            }
        };
    }
}
