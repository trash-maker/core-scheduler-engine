package io.notifengine.domain.expr;

public enum Op {
    EQ("="), NE("!="), GT(">"), GE(">="), LT("<"), LE("<="), IN("in"), BETWEEN("between");

    public final String symbol;

    Op(String s) { this.symbol = s; }

    public static Op fromSymbol(String s) {
        for (Op o : values()) if (o.symbol.equalsIgnoreCase(s)) return o;
        throw new IllegalArgumentException("unknown operator '" + s + "'");
    }

    public boolean ordering() { return this == GT || this == GE || this == LT || this == LE || this == BETWEEN; }
}
