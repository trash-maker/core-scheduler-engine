package trash_maker.domain.expr;

/** Three-valued truth used during evaluation. MISSING = signal absent, stale or not comparable. */
public enum Truth {
    TRUE, FALSE, MISSING;

    public static Truth of(boolean b) { return b ? TRUE : FALSE; }

    public Truth not() { return this == TRUE ? FALSE : this == FALSE ? TRUE : MISSING; }
}
