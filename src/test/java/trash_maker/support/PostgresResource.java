package trash_maker.support;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;

import java.util.Map;

public class PostgresResource implements QuarkusTestResourceLifecycleManager {
    @Override public Map<String, String> start() { return Containers.datasourceConfig(); }

    @Override public void stop() { }
}
