package trash_maker.core.dispatch;

import trash_maker.core.notifications.NotificationRow;
import trash_maker.domain.notification.NotificationMessage;

import java.time.Instant;

/** Maps a stored notification to the pure {@link NotificationMessage} contract. */
public final class MessageFactory {
    private MessageFactory() {}

    public static NotificationMessage message(NotificationRow n, int attempt, Instant now) {
        return new NotificationMessage(n.id(), attempt, n.idempotencyKey(), n.action(), n.tenantId(), n.correlationKey(), n.ruleId(),
                n.ruleVersion(), n.templateId(), n.seriesId(), n.occurrenceIndex(), n.dueAt(), now, n.body());
    }
}
