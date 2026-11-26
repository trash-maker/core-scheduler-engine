package trash_maker.domain.notification;

import trash_maker.domain.rule.RetryPolicy;

import java.time.Instant;
import java.util.UUID;

/** The part of a notification the state machine reads and writes. Immutable. */
public record NotificationState(UUID id, NotificationStatus status, String statusReason, int attempt, int cycleAttempts,
                                RetryPolicy retryPolicy, Instant dueAt, Instant expiresAt, Long attemptTimerId,
                                boolean failedOnce, Failure lastFailure, Instant publishedAt) {

    NotificationState with(NotificationStatus s, String reason) {
        return new NotificationState(id, s, reason, attempt, cycleAttempts, retryPolicy, dueAt, expiresAt, attemptTimerId, failedOnce, lastFailure, publishedAt);
    }

    NotificationState withAttempt(int attempt, int cycle, Instant publishedAt) {
        return new NotificationState(id, status, statusReason, attempt, cycle, retryPolicy, dueAt, expiresAt, null, failedOnce, lastFailure, publishedAt);
    }

    NotificationState withFailure(Failure f) {
        return new NotificationState(id, status, statusReason, attempt, cycleAttempts, retryPolicy, dueAt, expiresAt, null, true, f, publishedAt);
    }

    NotificationState withDue(Instant due) {
        return new NotificationState(id, status, statusReason, attempt, cycleAttempts, retryPolicy, due, expiresAt, attemptTimerId, failedOnce, lastFailure, publishedAt);
    }

    NotificationState withExpires(Instant exp) {
        return new NotificationState(id, status, statusReason, attempt, cycleAttempts, retryPolicy, dueAt, exp, attemptTimerId, failedOnce, lastFailure, publishedAt);
    }
}
