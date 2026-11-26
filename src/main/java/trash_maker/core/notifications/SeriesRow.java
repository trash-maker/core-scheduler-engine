package trash_maker.core.notifications;

import com.fasterxml.jackson.databind.JsonNode;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/** A recurring series: top-level RECURRING template, or a WHILE_IN_STATE child series of a parent notification. */
public record SeriesRow(UUID id, String tenantId, String ruleId, int ruleVersion, String templateId, String correlationKey,
                        String action, String status, String closeReason, UUID parentId, JsonNode spec, JsonNode context,
                        Instant startAt, Instant untilAt, Integer maxOccurrences, int occurrences, int skipped,
                        Instant nextDueAt, List<String> cancelOn, String idempotencyKey, int version,
                        Instant createdAt, Instant updatedAt) {

    public static final String WAITING = "WAITING";
    public static final String ACTIVE = "ACTIVE";
    public static final String CLOSED = "CLOSED";
}
