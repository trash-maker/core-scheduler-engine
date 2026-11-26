package io.notifengine.core.timers;

import com.fasterxml.jackson.databind.JsonNode;
import io.notifengine.domain.notification.TimerKind;

import java.time.Instant;
import java.util.UUID;

public record TimerRow(long id, TimerKind kind, Instant dueAt, String tenantId, UUID notificationId, UUID seriesId,
                       String ruleId, String correlationKey, JsonNode payload, int failures, String lastError, Instant createdAt) {

    /** correlation_key of the per-rule sweep timer (time-based NOW conditions). */
    public static final String SWEEP_KEY = "*";
}
