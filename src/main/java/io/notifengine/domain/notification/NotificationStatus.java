package io.notifengine.domain.notification;

import java.util.EnumSet;
import java.util.Set;

public enum NotificationStatus {
    WAITING_ON_PARENT, CREATED, SCHEDULED, PUBLISHED, SUCCEEDED, FAILED, RETRY_SCHEDULED,
    RETRIES_EXHAUSTED, CANCELLED, EXPIRED, SKIPPED;

    /** States after which nothing happens on its own (RETRIES_EXHAUSTED may still accept a late SUCCESS or a manual re-trigger). */
    public static final Set<NotificationStatus> TERMINAL = EnumSet.of(SUCCEEDED, RETRIES_EXHAUSTED, CANCELLED, EXPIRED, SKIPPED);

    public static final Set<NotificationStatus> ACTIVE = EnumSet.of(WAITING_ON_PARENT, CREATED, SCHEDULED, PUBLISHED, RETRY_SCHEDULED);

    public boolean terminal() { return TERMINAL.contains(this); }
}
