package trash_maker.common;

import java.util.List;

public class ValidationException extends RuntimeException {
    private final List<ValidationError> errors;

    public ValidationException(List<ValidationError> errors) {
        super(errors.size() == 1 ? errors.get(0).toString() : errors.size() + " validation errors: " + errors);
        this.errors = List.copyOf(errors);
    }

    public List<ValidationError> errors() { return errors; }
}
