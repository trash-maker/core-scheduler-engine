package io.notifengine.domain.notification;

import io.notifengine.domain.rule.Enums.ParentTrigger;

import java.util.List;
import java.util.Set;

/**
 * Pure parent/child dependency matrix.
 * <ul>
 *   <li>A waiting child is ACTIVATED when the parent enters the status its trigger names
 *       (ON_FAILED: first failure; ON_PUBLISHED: first publish; others: entering that status).</li>
 *   <li>It is SKIPPED when the parent reaches a terminal status without the trigger having matched
 *       (cascades to its own children).</li>
 *   <li>WHILE_IN_STATE series run while the parent is in one of the states; FAILED in {@code states}
 *       also matches RETRY_SCHEDULED/RETRIES_EXHAUSTED because FAILED is transient.</li>
 * </ul>
 */
public final class Dependencies {
    private Dependencies() {}

    public enum ChildAction { ACTIVATE, SKIP, NONE }

    public enum SeriesAction { START, CLOSE_RECOVERED, CLOSE_TERMINATED, NONE }

    public enum SeriesState { WAITING, ACTIVE, CLOSED }

    public static NotificationStatus statusFor(ParentTrigger t) {
        return switch (t) {
            case ON_FAILED -> NotificationStatus.FAILED;
            case ON_RETRIES_EXHAUSTED -> NotificationStatus.RETRIES_EXHAUSTED;
            case ON_SUCCEEDED -> NotificationStatus.SUCCEEDED;
            case ON_PUBLISHED -> NotificationStatus.PUBLISHED;
            case ON_CANCELLED -> NotificationStatus.CANCELLED;
            case ON_EXPIRED -> NotificationStatus.EXPIRED;
            case WHILE_IN_STATE -> throw new IllegalArgumentException("WHILE_IN_STATE is handled by series");
        };
    }

    public static ChildAction child(ParentTrigger trigger, List<NotificationStatus> entered, NotificationStatus parentNow) {
        if (entered.contains(statusFor(trigger))) return ChildAction.ACTIVATE;
        if (parentNow.terminal()) return ChildAction.SKIP;
        return ChildAction.NONE;
    }

    public static boolean inStates(Set<NotificationStatus> states, NotificationStatus s) {
        if (states.contains(s)) return true;
        return states.contains(NotificationStatus.FAILED)
                && (s == NotificationStatus.RETRY_SCHEDULED || s == NotificationStatus.RETRIES_EXHAUSTED);
    }

    /**
     * @param current state of the latest series for (parent, template)
     */
    public static SeriesAction series(Set<NotificationStatus> states, NotificationStatus before, NotificationStatus after,
                                      SeriesState current, boolean once) {
        boolean inAfter = inStates(states, after);
        boolean inBefore = inStates(states, before);
        return switch (current) {
            case ACTIVE -> inAfter ? SeriesAction.NONE
                    : (after == NotificationStatus.SUCCEEDED || !after.terminal()) ? SeriesAction.CLOSE_RECOVERED : SeriesAction.CLOSE_TERMINATED;
            case WAITING -> inAfter ? SeriesAction.START : after.terminal() ? SeriesAction.CLOSE_TERMINATED : SeriesAction.NONE;
            case CLOSED -> inAfter && !inBefore && !once ? SeriesAction.START : SeriesAction.NONE;
        };
    }
}
