package trash_maker.support;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.util.List;
import java.util.Map;

/** One PostgreSQL and one Kafka container per JVM, shared by all Quarkus test profiles (Ryuk removes them). */
public final class Containers {
    private static PostgreSQLContainer pg;
    private static KafkaContainer kafka;

    private Containers() {}

    public static synchronized PostgreSQLContainer postgres() {
        if (pg == null) {
            pg = new PostgreSQLContainer("postgres:16-alpine").withDatabaseName("notif")
                    .withCommand("postgres", "-c", "max_connections=300", "-c", "fsync=off");
            pg.start();
        }
        return pg;
    }

    public static synchronized KafkaContainer kafka() {
        if (kafka == null) {
            kafka = new KafkaContainer("apache/kafka:3.8.0");
            kafka.start();
            try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", kafka.getBootstrapServers()))) {
                admin.createTopics(List.of(new NewTopic("signals.in", 3, (short) 1), new NewTopic("notification.results", 3, (short) 1),
                        new NewTopic("notifications.out", 3, (short) 1), new NewTopic("notifications.dlq", 1, (short) 1))).all().get();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return kafka;
    }

    public static Map<String, String> datasourceConfig() {
        PostgreSQLContainer p = postgres();
        return Map.of("quarkus.datasource.jdbc.url", p.getJdbcUrl(), "quarkus.datasource.username", p.getUsername(),
                "quarkus.datasource.password", p.getPassword());
    }
}
