package trash_maker.it;

import com.fasterxml.jackson.databind.JsonNode;
import trash_maker.common.Json;
import trash_maker.support.Containers;
import trash_maker.support.EndToEndProfile;
import trash_maker.support.Fx;
import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.security.TestSecurity;
import io.restassured.http.ContentType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.containsString;

/** Full stack: REST rule CRUD -> Kafka signals.in -> evaluation -> outbox -> notifications.out -> results -> SUCCEEDED. */
@QuarkusTest
@TestProfile(EndToEndProfile.class)
class EndToEndTest {
    static final String RULE = """
            {"id":"welcome","timezone":"Europe/Rome",
             "trigger":{"expressionText":"signup = true and score >= 10"},
             "templates":[{"id":"mail","type":"IMMEDIATE","action":"SEND_EMAIL",
                "retryPolicy":{"maxAttempts":2,"initialDelay":"PT1S","attemptTimeout":"PT30S"},
                "body":{"to":{"$signal":"email"},"subject":"Welcome ${correlationKey}"}}]}""";

    @Test
    @TestSecurity(user = "alice", roles = {"tenant:acme"})
    void signalToNotificationToOutcome() throws Exception {
        given().contentType(ContentType.JSON).body(RULE).post("/api/v1/tenants/acme/rules")
                .then().statusCode(201).body("version", equalTo(1));
        String key = "user-" + UUID.randomUUID();
        Instant now = Instant.now();
        try (KafkaProducer<String, String> p = new KafkaProducer<>(Map.of("bootstrap.servers", Containers.kafka().getBootstrapServers()), new StringSerializer(), new StringSerializer());
             KafkaConsumer<String, String> c = new KafkaConsumer<>(Map.of("bootstrap.servers", Containers.kafka().getBootstrapServers(), "group.id", "test-" + key,
                     "auto.offset.reset", "earliest"), new StringDeserializer(), new StringDeserializer())) {
            c.subscribe(List.of("notifications.out"));
            for (String[] s : new String[][]{{"email", "\"x@y.z\""}, {"score", "12"}, {"signup", "true"}}) {
                p.send(new ProducerRecord<>("signals.in", "acme|" + key, Fx.envelope(UUID.randomUUID().toString(), key, s[0], "KPI",
                        Json.parse(s[1]), now)));
            }
            p.flush();
            List<ConsumerRecord<String, String>> got = new ArrayList<>();
            await().atMost(Duration.ofSeconds(60)).until(() -> {
                c.poll(Duration.ofMillis(500)).forEach(r -> { if (r.value().contains(key)) got.add(r); });
                return !got.isEmpty();
            });
            ConsumerRecord<String, String> rec = got.get(0);
            JsonNode msg = Json.parse(rec.value());
            assertThat(new String(rec.headers().lastHeader("action").value(), StandardCharsets.UTF_8)).isEqualTo("SEND_EMAIL");
            assertThat(rec.key()).isEqualTo("acme:" + msg.get("notification_id").asText());
            assertThat(msg.get("body").toString()).isEqualTo("{\"to\":\"x@y.z\",\"subject\":\"Welcome " + key + "\"}");
            String id = msg.get("notification_id").asText();

            // actuator reports success
            p.send(new ProducerRecord<>("notification.results", "acme:" + id, Json.write(Json.object().put("notification_id", id).put("attempt", 1)
                    .put("action", "SEND_EMAIL").put("outcome", "SUCCESS").put("occurred_at", Instant.now().toString())))).get();
            await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                    given().get("/api/v1/tenants/acme/notifications/" + id).then().statusCode(200).body("status", equalTo("SUCCEEDED")));
            given().get("/api/v1/tenants/acme/notifications/" + id + "/history").then().statusCode(200)
                    .body("entries.to", hasItem("SUCCEEDED"));
            given().get("/api/v1/tenants/acme/notifications?correlationKey=" + key).then().statusCode(200).body("items.size()", equalTo(1));
            given().get("/api/v1/tenants/acme/entities/" + key + "/rules").then().statusCode(200).body("[0].lastResult", equalTo(true));
        }
    }

    @Test
    @TestSecurity(user = "bob", roles = {"tenant:other"})
    void tenantIsolation() {
        given().get("/api/v1/tenants/acme/notifications").then().statusCode(403);
        given().get("/api/v1/tenants/other/notifications").then().statusCode(200);
        given().get("/api/v1/ops/stats").then().statusCode(403);
    }

    @Test
    void anonymousIsRejected() {
        given().get("/api/v1/tenants/acme/notifications").then().statusCode(401);
    }

    @Test
    @TestSecurity(user = "alice", roles = {"tenant:acme"})
    void validationReportsPathsAndDryRunShowsEveryNode() {
        given().contentType(ContentType.JSON).body("""
                {"id":"bad","trigger":{"expression":{"signal":"a","op":"~","value":1}},"templates":[{"id":"t","type":"IMMEDIATE","action":"lower"}]}""")
                .post("/api/v1/tenants/acme/rules").then().statusCode(400)
                .body("errors.path", hasItem("$.trigger.expression.op")).body("errors.path", hasItem("$.templates[0].action"));
        given().contentType(ContentType.JSON).body("{\"rule\":" + RuleParserTestRule.RULE + ",\"correlationKey\":\"c1\",\"now\":\"2026-11-30T23:20:00Z\","
                        + "\"signals\":{\"kpi1\":{\"value\":7},\"customer_email\":{\"value\":\"a@b.c\"},\"due_date\":{\"kind\":\"DATE\",\"value\":\"2026-12-20\"}}}")
                .post("/api/v1/tenants/acme/rules/dry-run").then().statusCode(200)
                .body("result", equalTo(true))
                .body("nodes.'$.trigger.expression.or[0].and[1]'.value", equalTo("TRUE"))
                .body("nodes.'$.trigger.expression.or[1]'.value", equalTo("FALSE"))
                .body("notifications[0].body.subject", equalTo("Reminder c1"))
                .body("notifications[1].initialStatus", equalTo("WAITING_ON_PARENT"));
    }

    @Test
    @TestSecurity(user = "root", roles = {"admin"})
    void opsHealthMetricsAndOpenApi() {
        given().get("/api/v1/ops/stats").then().statusCode(200);
        given().get("/q/health/ready").then().statusCode(200);
        given().get("/q/health/live").then().statusCode(200);
        given().get("/q/metrics").then().statusCode(200).body(containsString("notif_"));
        given().get("/q/openapi").then().statusCode(200).body(containsString("/api/v1/tenants/{tenant}/rules/dry-run"));
    }

    static final class RuleParserTestRule {
        static final String RULE = trash_maker.domain.RuleParserTest.EXAMPLE.replace("\"tenantId\":\"acme\",", "");
    }
}
