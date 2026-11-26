package trash_maker.domain.rule;

/** Holder for small rule enums. */
public final class Enums {
    private Enums() {}

    public enum FiringMode { ON_BECOMES_TRUE, EVERY_EVALUATION_TRUE }

    public enum NotificationType { IMMEDIATE, SCHEDULED, RECURRING }

    public enum MissingValuePolicy { FAIL, USE_DEFAULT, OMIT_FIELD }

    public enum ParentTrigger { ON_FAILED, ON_RETRIES_EXHAUSTED, ON_SUCCEEDED, ON_PUBLISHED, ON_CANCELLED, ON_EXPIRED, WHILE_IN_STATE }

    public enum BackoffKind { FIXED, EXPONENTIAL }

    public enum MisfirePolicy { SKIP }
}
