package trash_maker.domain.evaluation;

/** A firing could not be planned (body resolution, schema, missing time base). Nothing is created; event goes to the DLQ. */
public class PlanningException extends RuntimeException {
    private final String templateId;
    private final String code;

    public PlanningException(String templateId, String code, String message) {
        super("template '" + templateId + "': " + code + ": " + message);
        this.templateId = templateId;
        this.code = code;
    }

    public String templateId() { return templateId; }

    public String code() { return code; }
}
