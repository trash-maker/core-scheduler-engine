package trash_maker.domain.signal;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiPredicate;

/**
 * Pure selection of a signal batch: idempotency (inbox result + duplicates inside the batch), discarding of
 * unreferenced signals, and reduction to the latest value per (tenant, key, signal) (ties: last in batch wins).
 *
 * @param <T> the raw message carried along (for the trigger reference / DLQ)
 */
public final class SignalBatch<T> {
    public record Item<T>(SignalEnvelope env, T raw) {}

    public record Selection<T>(List<Item<T>> accepted, Map<String, Item<T>> latest, int duplicates, int unreferenced) {}

    private SignalBatch() {}

    public static String inboxKey(SignalEnvelope e) { return e.tenantId() + "|" + e.signalId(); }

    public static String stateKey(SignalEnvelope e) { return e.tenantId() + "|" + e.correlationKey() + "|" + e.signal(); }

    /**
     * @param fresh       inbox keys ({@link #inboxKey}) that were not seen before
     * @param referenced  (tenant, signal) -> referenced by an enabled rule
     */
    public static <T> Selection<T> select(List<Item<T>> valid, Set<String> fresh, BiPredicate<String, String> referenced,
                                          boolean storeUnreferenced) {
        List<Item<T>> accepted = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        int dups = 0, unref = 0;
        for (Item<T> it : valid) {
            String id = inboxKey(it.env());
            if (!fresh.contains(id) || !seen.add(id)) { dups++; continue; }
            if (!storeUnreferenced && !referenced.test(it.env().tenantId(), it.env().signal())) { unref++; continue; }
            accepted.add(it);
        }
        Map<String, Item<T>> latest = new LinkedHashMap<>();
        for (Item<T> it : accepted) {
            String k = stateKey(it.env());
            Item<T> prev = latest.get(k);
            if (prev == null || !it.env().occurredAt().isBefore(prev.env().occurredAt())) latest.put(k, it);
        }
        return new Selection<>(accepted, latest, dups, unref);
    }

    /** The message reported as {@code $trigger}: the newest of the entity's changed signals. */
    public static <T> Item<T> trigger(List<Item<T>> changed) {
        return changed.stream().max((a, b) -> a.env().occurredAt().compareTo(b.env().occurredAt())).orElseThrow();
    }
}
