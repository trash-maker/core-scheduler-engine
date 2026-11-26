package trash_maker.domain.signal;

import java.time.Instant;

/** Latest stored value of one signal for one entity. */
public record SignalValue(String name, SignalKind kind, Value value, Instant occurredAt) {}
