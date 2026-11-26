package io.notifengine.infra.ops;

import io.notifengine.core.ports.TraceContext;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.context.Context;

import java.util.HashMap;
import java.util.Map;

/** Captures W3C trace context of the current span so it travels through the outbox to notifications.out headers. */
public final class OtelTraceContext implements TraceContext {
    private final OpenTelemetry otel;

    public OtelTraceContext(OpenTelemetry otel) { this.otel = otel; }

    @Override
    public Map<String, String> currentHeaders() {
        Map<String, String> h = new HashMap<>();
        otel.getPropagators().getTextMapPropagator().inject(Context.current(), h, (carrier, k, v) -> carrier.put(k, v));
        return h;
    }
}
