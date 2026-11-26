package trash_maker.domain;

import org.junit.jupiter.api.Test;
import trash_maker.common.Json;
import trash_maker.domain.notification.NotificationMessage;
import trash_maker.domain.notification.OutcomeMessage;
import trash_maker.domain.notification.OutcomeType;
import trash_maker.domain.retention.RetentionPolicy;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MessagesAndRetentionTest {
    static final UUID ID = UUID.fromString("01a21581-6e29-748f-87b5-e7d0b15596db");

    @Test
    void outcomeParsingAndCorrelation() {
        OutcomeMessage m = OutcomeMessage.parse("{\"notification_id\":\"" + ID + "\",\"attempt\":2,\"action\":\"PUSH\",\"outcome\":\"FAILURE\",\"reason\":\"x\",\"occurred_at\":\"2026-01-01T00:00:00Z\"}");
        assertThat(m.outcome()).isEqualTo(OutcomeType.FAILURE);
        assertThat(m.check("acme", "PUSH")).isEqualTo(OutcomeMessage.Verdict.APPLY);
        assertThat(m.check("acme", "SMS")).isEqualTo(OutcomeMessage.Verdict.ACTION_MISMATCH);
        assertThat(m.check(null, null)).isEqualTo(OutcomeMessage.Verdict.UNKNOWN_NOTIFICATION);
        OutcomeMessage t = OutcomeMessage.parse("{\"notification_id\":\"" + ID + "\",\"attempt\":1,\"action\":\"PUSH\",\"outcome\":\"SUCCESS\",\"tenant_id\":\"other\"}");
        assertThat(t.check("acme", "PUSH")).isEqualTo(OutcomeMessage.Verdict.TENANT_MISMATCH);
        assertThat(t.toEvent().attempt()).isEqualTo(1);
        assertThatThrownBy(() -> OutcomeMessage.parse("{\"notification_id\":\"" + ID + "\",\"attempt\":0,\"action\":\"A\",\"outcome\":\"SUCCESS\"}"))
                .hasMessageContaining("attempt");
        assertThatThrownBy(() -> OutcomeMessage.parse("{\"notification_id\":\"" + ID + "\",\"attempt\":1,\"action\":\"A\",\"outcome\":\"MAYBE\"}"))
                .hasMessageContaining("outcome");
    }

    @Test
    void notificationMessageContract() {
        var msg = new NotificationMessage(ID, 3, "acme|r|k|1|t", "PUSH", "acme", "k", "r", 2, "t", null, null,
                Instant.parse("2026-01-01T09:00:00Z"), Instant.parse("2026-01-01T09:00:01Z"), Json.parse("{\"a\":1}"));
        assertThat(msg.key()).isEqualTo("acme:" + ID);
        assertThat(msg.headers(Map.of("traceparent", "00-x"))).containsEntry("action", "PUSH").containsEntry("attempt", "3")
                .containsEntry("traceparent", "00-x").containsEntry("schema_version", "1");
        var p = Json.parse(msg.payload());
        assertThat(p.get("notification_id").asText()).isEqualTo(ID.toString());
        assertThat(p.get("attempt").asInt()).isEqualTo(3);
        assertThat(p.get("body").toString()).isEqualTo("{\"a\":1}");
        assertThat(p.has("series_id")).isFalse();
    }

    @Test
    void retention() {
        Instant cutoff = Instant.parse("2026-10-27T00:00:00Z");
        assertThat(RetentionPolicy.expired("notification_p2026_09", cutoff)).isTrue();
        assertThat(RetentionPolicy.expired("notification_p2026_10", cutoff)).isFalse();
        assertThat(RetentionPolicy.expired("something_else", cutoff)).isFalse();
        assertThat(RetentionPolicy.historyPartition("notification_p2026_09")).isEqualTo("notification_history_p2026_09");
    }
}
