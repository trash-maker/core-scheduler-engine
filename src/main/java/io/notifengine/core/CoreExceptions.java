package io.notifengine.core;

/** Application-level errors mapped to HTTP status codes by the API layer. */
public final class CoreExceptions {
    private CoreExceptions() {}

    public static class NotFound extends RuntimeException {
        public NotFound(String m) { super(m); }
    }

    public static class Conflict extends RuntimeException {
        public Conflict(String m) { super(m); }
    }
}
