package io.notifengine.domain.notification;

import java.time.Instant;

/** Inputs of the notification state machine. */
public sealed interface Event {
    String name();

    /** Initial processing right after creation (CREATED). */
    record Start() implements Event { public String name() { return "START"; } }

    /** Parent trigger matched: WAITING_ON_PARENT -> CREATED -> ... with the computed due time. */
    record Activate(Instant dueAt) implements Event { public String name() { return "ACTIVATE"; } }

    /** PUBLISH timer fired (first publish or retry). */
    record PublishDue() implements Event { public String name() { return "PUBLISH_DUE"; } }

    record Outcome(int attempt, OutcomeType outcome, String reason, Instant occurredAt) implements Event { public String name() { return "OUTCOME_" + outcome; } }

    record AttemptTimeout(int attempt) implements Event { public String name() { return "ATTEMPT_TIMEOUT"; } }

    record Cancel(String reason) implements Event { public String name() { return "CANCEL"; } }

    record Expire() implements Event { public String name() { return "EXPIRE"; } }

    record Retrigger(String requestedBy) implements Event { public String name() { return "RETRIGGER"; } }

    record Skip(String reason) implements Event { public String name() { return "SKIP"; } }
}
