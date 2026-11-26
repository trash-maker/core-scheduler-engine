package trash_maker.support;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;

import java.util.Map;

public class KafkaResource implements QuarkusTestResourceLifecycleManager {
    @Override public Map<String, String> start() { return Map.of("kafka.bootstrap.servers", Containers.kafka().getBootstrapServers()); }

    @Override public void stop() { }
}
