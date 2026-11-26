package trash_maker.support;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.util.List;
import java.util.Map;

/** Dedicated PostgreSQL + Kafka for @QuarkusTest (isolated from the plain integration tests). */
public class E2EResources implements QuarkusTestResourceLifecycleManager {
    static PostgreSQLContainer pg;
    static KafkaContainer kafka;

    public static String bootstrap() { return kafka.getBootstrapServers(); }

    @Override
    public Map<String, String> start() {
        pg = new PostgreSQLContainer("postgres:16-alpine").withDatabaseName("notif").withCommand("postgres", "-c", "fsync=off");
        kafka = new KafkaContainer("apache/kafka:3.8.0");
        pg.start();
        kafka.start();
        try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", kafka.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic("signals.in", 3, (short) 1), new NewTopic("notification.results", 3, (short) 1),
                    new NewTopic("notifications.out", 3, (short) 1), new NewTopic("notifications.dlq", 1, (short) 1))).all().get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return Map.of(
                "quarkus.datasource.jdbc.url", pg.getJdbcUrl(),
                "quarkus.datasource.username", pg.getUsername(),
                "quarkus.datasource.password", pg.getPassword(),
                "kafka.bootstrap.servers", kafka.getBootstrapServers());
    }

    @Override
    public void stop() {
        if (kafka != null) kafka.stop();
        if (pg != null) pg.stop();
    }
}
