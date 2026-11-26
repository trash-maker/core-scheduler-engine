package trash_maker.support;

import io.quarkus.test.junit.QuarkusTestProfile;

import java.util.List;

/** Full stack: all roles, real clock, real Kafka and PostgreSQL. */
public class EndToEndProfile implements QuarkusTestProfile {
    @Override
    public List<TestResourceEntry> testResources() {
        return List.of(new TestResourceEntry(PostgresResource.class), new TestResourceEntry(KafkaResource.class));
    }
}
