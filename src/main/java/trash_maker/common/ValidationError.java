package trash_maker.common;

/** A validation problem with a precise JSON path (e.g. $.templates[1].retryPolicy.maxAttempts). */
public record ValidationError(String path, String message) {
    @Override
    public String toString() { return path + ": " + message; }
}
