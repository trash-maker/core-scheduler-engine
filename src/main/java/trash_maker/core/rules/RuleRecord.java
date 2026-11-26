package trash_maker.core.rules;

import com.fasterxml.jackson.databind.JsonNode;

import java.time.Instant;

public record RuleRecord(String tenantId, String ruleId, int version, boolean enabled, boolean current,
                         JsonNode definition, long changeSeq, Instant createdAt, String createdBy) {}
