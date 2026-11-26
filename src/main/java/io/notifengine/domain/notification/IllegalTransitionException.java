package io.notifengine.domain.notification;

public class IllegalTransitionException extends RuntimeException {
    public IllegalTransitionException(String m) { super(m); }
}
