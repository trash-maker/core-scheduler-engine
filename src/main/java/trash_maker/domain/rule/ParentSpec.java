package trash_maker.domain.rule;

import trash_maker.domain.notification.NotificationStatus;

import java.time.Duration;
import java.util.Set;

public record ParentSpec(String templateId, Enums.ParentTrigger trigger, Duration delay,
                         Set<NotificationStatus> states, UntilSpec until, Integer maxOccurrences, boolean once) {}
