package trash_maker.domain.rule;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import trash_maker.common.Json;

import java.time.Duration;

/**
 * Delay before retry n (n = 1 for the first retry) = min(initialDelay * multiplier^(n-1), maxDelay) +/- jitter.
 * jitter is a fraction (0.1 = +/-10%). maxAttempts counts all attempts (default 1 = no retry).
 */
public record RetryPolicy(int maxAttempts, Enums.BackoffKind backoff, Duration initialDelay, double multiplier,
                          Duration maxDelay, double jitter, Duration attemptTimeout) {

    public static final RetryPolicy DEFAULT = new RetryPolicy(1, Enums.BackoffKind.FIXED, Duration.ofMinutes(1), 2.0,
            Duration.ofHours(1), 0.0, null);

    /** @param random uniform value in [0,1) (injected to keep this pure and testable) */
    public Duration delay(int retryNumber, double random) {
        double base = initialDelay.toMillis();
        if (backoff == Enums.BackoffKind.EXPONENTIAL) base = base * Math.pow(multiplier, Math.max(0, retryNumber - 1));
        if (maxDelay != null) base = Math.min(base, maxDelay.toMillis());
        if (jitter > 0) base = base * (1 + jitter * (2 * random - 1));
        return Duration.ofMillis(Math.max(0, Math.round(base)));
    }

    public ObjectNode toJson() {
        ObjectNode o = Json.object();
        o.put("maxAttempts", maxAttempts);
        o.put("backoff", backoff.name());
        o.put("initialDelay", initialDelay.toString());
        o.put("multiplier", multiplier);
        if (maxDelay != null) o.put("maxDelay", maxDelay.toString());
        o.put("jitter", jitter);
        if (attemptTimeout != null) o.put("attemptTimeout", attemptTimeout.toString());
        return o;
    }

    public static RetryPolicy fromJson(JsonNode n) {
        return new RetryPolicy(n.path("maxAttempts").asInt(1), Enums.BackoffKind.valueOf(n.path("backoff").asText("FIXED")),
                Duration.parse(n.path("initialDelay").asText("PT1M")), n.path("multiplier").asDouble(2.0),
                n.hasNonNull("maxDelay") ? Duration.parse(n.get("maxDelay").asText()) : null, n.path("jitter").asDouble(0),
                n.hasNonNull("attemptTimeout") ? Duration.parse(n.get("attemptTimeout").asText()) : null);
    }
}
