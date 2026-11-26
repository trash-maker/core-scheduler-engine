package trash_maker.domain;

import org.junit.jupiter.api.Test;
import trash_maker.common.Json;
import trash_maker.domain.signal.SignalBatch;
import trash_maker.domain.signal.SignalEnvelope;

import java.time.Instant;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class SignalBatchTest {
    static final Instant T = Instant.parse("2026-01-01T00:00:00Z");

    static SignalBatch.Item<String> item(String id, String key, String signal, int value, int secs) {
        String raw = "{\"signal_id\":\"" + id + "\",\"tenant_id\":\"t\",\"correlation_key\":\"" + key + "\",\"signal\":\"" + signal
                + "\",\"kind\":\"KPI\",\"value\":" + value + ",\"occurred_at\":\"" + T.plusSeconds(secs) + "\"}";
        return new SignalBatch.Item<>(SignalEnvelope.parse(Json.parse(raw)), raw);
    }

    @Test
    void dedupFilterAndReduceToLatest() {
        var a = item("a", "k", "x", 1, 10);
        var aDup = item("a", "k", "x", 1, 10);
        var older = item("b", "k", "x", 2, 5);
        var seen = item("c", "k", "x", 3, 20);           // already in the inbox
        var unref = item("d", "k", "nobody", 4, 1);
        var other = item("e", "k2", "x", 5, 1);
        var sel = SignalBatch.select(List.of(a, aDup, older, seen, unref, other), Set.of("t|a", "t|b", "t|d", "t|e"),
                (tenant, s) -> s.equals("x"), false);
        assertThat(sel.duplicates()).isEqualTo(2);
        assertThat(sel.unreferenced()).isEqualTo(1);
        assertThat(sel.accepted()).containsExactly(a, older, other);
        assertThat(sel.latest()).containsEntry("t|k|x", a).containsEntry("t|k2|x", other);   // newer occurred_at wins
        assertThat(SignalBatch.select(List.of(unref), Set.of("t|d"), (t, s) -> false, true).accepted()).hasSize(1);
    }

    @Test
    void sameInstantLastInBatchWinsAndTriggerIsNewest() {
        var first = item("a", "k", "x", 1, 10);
        var second = item("b", "k", "x", 2, 10);
        assertThat(SignalBatch.select(List.of(first, second), Set.of("t|a", "t|b"), (t, s) -> true, false).latest().get("t|k|x")).isEqualTo(second);
        assertThat(SignalBatch.trigger(List.of(item("p", "k", "x", 1, 3), item("q", "k", "y", 1, 9))).env().signalId()).isEqualTo("q");
    }
}
