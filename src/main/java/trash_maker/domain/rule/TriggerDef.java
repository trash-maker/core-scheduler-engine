package trash_maker.domain.rule;

import com.fasterxml.jackson.databind.JsonNode;
import trash_maker.domain.expr.Node;
import trash_maker.domain.expr.OnMissing;

import java.time.Duration;

public record TriggerDef(Enums.FiringMode mode, OnMissing onMissing, Duration cooldown, Node expression, JsonNode expressionJson) {}
