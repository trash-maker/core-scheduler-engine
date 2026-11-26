package io.notifengine.core.notifications;

import com.fasterxml.jackson.databind.JsonNode;
import io.notifengine.domain.notification.Failure;
import io.notifengine.domain.notification.NotificationState;
import io.notifengine.domain.notification.NotificationStatus;
import io.notifengine.domain.rule.RetryPolicy;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/** Persistent notification aggregate (one row of the partitioned notification table). */
public record NotificationRow(
        UUID id, Instant createdAt, Instant updatedAt, String tenantId, String ruleId, int ruleVersion, String templateId,
        String correlationKey, String action, String type, NotificationStatus status, String statusReason,
        int attempt, int cycleAttempts, RetryPolicy retryPolicy, JsonNode body, JsonNode spec, JsonNode context,
        Instant dueAt, Instant expiresAt, Long attemptTimerId, int version, String idempotencyKey,
        UUID parentId, String parentTrigger, int depth, UUID seriesId, Integer occurrenceIndex, List<String> cancelOn,
        boolean failedOnce, Failure lastFailure, Instant publishedAt, Instant terminalAt) {

    public NotificationState toState() {
        return new NotificationState(id, status, statusReason, attempt, cycleAttempts, retryPolicy, dueAt, expiresAt,
                attemptTimerId, failedOnce, lastFailure, publishedAt);
    }

    public NotificationRow withState(NotificationState s, Instant now) {
        Instant term = s.status().terminal() ? (terminalAt != null && status == s.status() ? terminalAt : now) : null;
        return new NotificationRow(id, createdAt, now, tenantId, ruleId, ruleVersion, templateId, correlationKey, action, type,
                s.status(), s.statusReason(), s.attempt(), s.cycleAttempts(), retryPolicy, body, spec, context, s.dueAt(), s.expiresAt(),
                s.attemptTimerId(), version, idempotencyKey, parentId, parentTrigger, depth, seriesId, occurrenceIndex, cancelOn,
                s.failedOnce(), s.lastFailure(), s.publishedAt(), term);
    }

    public NotificationRow withBody(JsonNode b) {
        return new NotificationRow(id, createdAt, updatedAt, tenantId, ruleId, ruleVersion, templateId, correlationKey, action, type,
                status, statusReason, attempt, cycleAttempts, retryPolicy, b, spec, context, dueAt, expiresAt, attemptTimerId, version,
                idempotencyKey, parentId, parentTrigger, depth, seriesId, occurrenceIndex, cancelOn, failedOnce, lastFailure, publishedAt, terminalAt);
    }

    public NotificationRow withTimes(Instant due, Instant expires) {
        return new NotificationRow(id, createdAt, updatedAt, tenantId, ruleId, ruleVersion, templateId, correlationKey, action, type,
                status, statusReason, attempt, cycleAttempts, retryPolicy, body, spec, context, due, expires, attemptTimerId, version,
                idempotencyKey, parentId, parentTrigger, depth, seriesId, occurrenceIndex, cancelOn, failedOnce, lastFailure, publishedAt, terminalAt);
    }

    public NotificationRow withVersion(int v) {
        return new NotificationRow(id, createdAt, updatedAt, tenantId, ruleId, ruleVersion, templateId, correlationKey, action, type,
                status, statusReason, attempt, cycleAttempts, retryPolicy, body, spec, context, dueAt, expiresAt, attemptTimerId, v,
                idempotencyKey, parentId, parentTrigger, depth, seriesId, occurrenceIndex, cancelOn, failedOnce, lastFailure, publishedAt, terminalAt);
    }
}
