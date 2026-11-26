package trash_maker.domain.notification;

import java.util.List;
import java.util.stream.Collectors;

/** Result of applying an event: new state, recorded changes, effects. {@code ignored} = no-op (stale/duplicate input). */
public record Transition(NotificationState state, List<StatusChange> changes, List<Effect> effects, String ignored) {

    public boolean isIgnored() { return ignored != null; }

    public boolean changed() { return !changes.isEmpty(); }

    /** Every status entered by this transition, in order (used for child triggers). */
    public List<NotificationStatus> entered() { return changes.stream().map(StatusChange::to).collect(Collectors.toList()); }
}
