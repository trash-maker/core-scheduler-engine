package trash_maker.core.ports;

import java.util.Map;

/** Port for trace propagation: headers of the current span (e.g. traceparent) to store in the outbox. */
public interface TraceContext {
    TraceContext NONE = Map::of;

    Map<String, String> currentHeaders();
}
