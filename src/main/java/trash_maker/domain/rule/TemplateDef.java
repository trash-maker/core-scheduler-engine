package trash_maker.domain.rule;

import com.fasterxml.jackson.databind.JsonNode;
import trash_maker.common.IsoAmount;

import java.time.Duration;
import java.util.List;

public record TemplateDef(
        String id,
        Enums.NotificationType type,
        String action,
        TimeSpec dueAt,
        boolean dueAtFloorNow,
        TimeSpec expiresAt,
        IsoAmount expiresAfter,
        RecurrenceSpec recurrence,
        TimeSpec startAt,
        UntilSpec until,
        Integer maxOccurrences,
        Enums.MisfirePolicy misfirePolicy,
        Duration misfireGrace,
        List<String> cancelOnEvents,
        RetryPolicy retryPolicy,
        ParentSpec parent,
        JsonNode body,
        JsonNode bodySchema,
        Enums.MissingValuePolicy onMissingValue,
        JsonNode raw) {

    public boolean isRecurring() { return type == Enums.NotificationType.RECURRING; }

    public boolean isWhileInState() { return parent != null && parent.trigger() == Enums.ParentTrigger.WHILE_IN_STATE; }
}
