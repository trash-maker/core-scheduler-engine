package io.notifengine.domain.notification;

/** One recorded status change (append-only history row). */
public record StatusChange(NotificationStatus from, NotificationStatus to, String event, int attempt, String reason) {}
