package trash_maker.common;

import java.util.List;

/** Validation errors with precise JSON paths. */
public final class Validation {
    private Validation() {}

    /** A validation problem with a precise JSON path (e.g. $.templates[1].retryPolicy.maxAttempts). */
    public record ValidationError(String path, String message) {
        @Override
        public String toString() { return path + ": " + message; }
    }

    public static class ValidationException extends RuntimeException {
        private final List<ValidationError> errors;

        public ValidationException(List<ValidationError> errors) {
            super(errors.size() == 1 ? errors.get(0).toString() : errors.size() + " validation errors: " + errors);
            this.errors = List.copyOf(errors);
        }

        public List<ValidationError> errors() { return errors; }
    }
}
