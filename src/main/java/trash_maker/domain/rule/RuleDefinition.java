package trash_maker.domain.rule;

import com.fasterxml.jackson.databind.JsonNode;

import java.time.ZoneId;
import java.util.List;

public record RuleDefinition(String id, int version, String tenantId, boolean enabled, ZoneId timezone,
                             Enums.MissingValuePolicy onMissingValue, TriggerDef trigger,
                             List<TemplateDef> templates, JsonNode raw) {}
