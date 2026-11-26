package io.notifengine.domain.rule;

import com.fasterxml.jackson.databind.JsonNode;
import io.notifengine.domain.expr.Node;
import io.notifengine.domain.expr.OnMissing;

import java.time.Duration;

public record TriggerDef(Enums.FiringMode mode, OnMissing onMissing, Duration cooldown, Node expression, JsonNode expressionJson) {}
