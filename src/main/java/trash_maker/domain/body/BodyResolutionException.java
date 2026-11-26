package trash_maker.domain.body;

/** Body could not be resolved; with policy FAIL the whole firing is rejected (DLQ, no partial set). */
public class BodyResolutionException extends RuntimeException {
    private final String path;

    public BodyResolutionException(String path, String message) {
        super(path + ": " + message);
        this.path = path;
    }

    public String path() { return path; }
}
