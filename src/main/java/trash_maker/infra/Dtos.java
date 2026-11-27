package trash_maker.infra;

import com.fasterxml.jackson.databind.JsonNode;
import trash_maker.core.NotificationRepository.NotificationRow;
import trash_maker.core.NotificationRepository;
import trash_maker.core.RuleRepository.RuleRecord;
import trash_maker.core.RuleStateRepository;
import trash_maker.core.SeriesRepository.SeriesRow;
import trash_maker.domain.notification.Lifecycle.Failure;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/** API representations. */
public final class Dtos {
    private Dtos() {}

    public record RuleDto(String tenantId, String id, int version, boolean enabled, boolean current, Instant createdAt, String createdBy, JsonNode definition) {
        static RuleDto of(RuleRecord r) { return new RuleDto(r.tenantId(), r.ruleId(), r.version(), r.enabled(), r.current(), r.createdAt(), r.createdBy(), r.definition()); }
    }

    public record Page<T>(List<T> items, String nextCursor) {}

    public record ValidationResult(boolean valid, List<trash_maker.common.Validation.ValidationError> errors) {}

    public record NotificationDto(UUID id, String tenantId, String ruleId, int ruleVersion, String templateId, String correlationKey,
                                  String action, String type, String status, String statusReason, int attempt, int cycleAttempts,
                                  JsonNode retryPolicy, Instant createdAt, Instant updatedAt, Instant dueAt, Instant expiresAt,
                                  Instant publishedAt, Instant terminalAt, UUID parentId, String parentTrigger, UUID seriesId,
                                  Integer occurrenceIndex, List<String> cancelOnEvents, Failure lastFailure, String idempotencyKey, JsonNode body) {
        static NotificationDto of(NotificationRow n) {
            return new NotificationDto(n.id(), n.tenantId(), n.ruleId(), n.ruleVersion(), n.templateId(), n.correlationKey(), n.action(), n.type(),
                    n.status().name(), n.statusReason(), n.attempt(), n.cycleAttempts(), n.retryPolicy().toJson(), n.createdAt(), n.updatedAt(),
                    n.dueAt(), n.expiresAt(), n.publishedAt(), n.terminalAt(), n.parentId(), n.parentTrigger(), n.seriesId(), n.occurrenceIndex(),
                    n.cancelOn(), n.lastFailure(), n.idempotencyKey(), n.body());
        }
    }

    public record SeriesDto(UUID id, String ruleId, String templateId, String correlationKey, String action, String status, String closeReason,
                            UUID parentId, Instant startAt, Instant untilAt, Integer maxOccurrences, int occurrences, int skipped, Instant nextDueAt) {
        static SeriesDto of(SeriesRow s) {
            return new SeriesDto(s.id(), s.ruleId(), s.templateId(), s.correlationKey(), s.action(), s.status(), s.closeReason(), s.parentId(),
                    s.startAt(), s.untilAt(), s.maxOccurrences(), s.occurrences(), s.skipped(), s.nextDueAt());
        }
    }

    public record HistoryDto(List<NotificationRepository.HistoryEntry> entries) {}

    public record RuleStateDto(String ruleId, String correlationKey, boolean lastResult, long edgeSeq, Instant lastFiredAt,
                               Instant lastEvaluatedAt, Integer ruleVersion, JsonNode leafState) {
        static RuleStateDto of(RuleStateRepository.State s) {
            return new RuleStateDto(s.key().ruleId(), s.key().correlationKey(), s.lastResult(), s.edgeSeq(), s.lastFiredAt(), s.lastEvaluatedAt(),
                    s.ruleVersion(), RuleStateRepository.memoryJson(s.memory()));
        }
    }

    /** Dry-run request: either an inline rule or the id of a stored rule; signals as {name: {kind, value, occurredAt}}. */
    public record DryRunRequest(JsonNode rule, String ruleId, String correlationKey, JsonNode signals, Instant now, JsonNode trigger,
                                PriorState priorState) {}

    public record PriorState(boolean lastResult, long edgeSeq, Instant lastFiredAt) {}
}
