package trash_maker.domain.expr;

import trash_maker.domain.signal.SignalValue;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Map;

public record EvalInput(Instant now, ZoneId zone, Map<String, SignalValue> signals,
                        Map<String, LeafMemory> memory, OnMissing onMissing, Duration defaultMaxAge) {}
