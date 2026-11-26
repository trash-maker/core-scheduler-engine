package trash_maker.infra.config;

import io.micrometer.core.instrument.MeterRegistry;
import trash_maker.core.Engine;
import trash_maker.core.EngineConfig;
import trash_maker.core.timers.WakeupHub;
import trash_maker.infra.kafka.KafkaMessagePublisher;
import trash_maker.infra.ops.MicrometerMetrics;
import trash_maker.infra.ops.OtelTraceContext;
import io.opentelemetry.api.OpenTelemetry;
import io.agroal.api.AgroalDataSource;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Disposes;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/** CDI wiring of the framework-free engine. */
@ApplicationScoped
public class EngineProducer {

    @Produces
    @Singleton
    Clock clock() { return Clock.systemUTC(); }

    @Produces
    @Singleton
    WakeupHub wakeupHub() { return new WakeupHub(); }

    @Produces
    @Singleton
    KafkaMessagePublisher publisher(@ConfigProperty(name = "kafka.bootstrap.servers") String bootstrap,
                                    @ConfigProperty(name = "notifications.kafka.producer.extra", defaultValue = "") String extra) {
        Map<String, String> props = new HashMap<>();
        for (String kv : extra.split(",")) {
            int i = kv.indexOf('=');
            if (i > 0) props.put(kv.substring(0, i).trim(), kv.substring(i + 1).trim());
        }
        return new KafkaMessagePublisher(bootstrap, props);
    }

    void close(@Disposes KafkaMessagePublisher p) { p.close(); }

    @Produces
    @Singleton
    EngineConfig engineConfig(NotificationsConfig c) {
        return new EngineConfig(c.topics().out(), c.topics().dlq(), c.signals().storeUnreferenced(), c.signals().defaultMaxAge().orElse(null),
                c.actions().allowList().orElse(Set.of()), c.timers().batchSize(), c.timers().maxFailures(), c.timers().failureBackoff(),
                c.timers().idleMin(), c.timers().idleMax(), c.timers().sweepPageSize(), c.relay().batchSize(), c.retention().terminal(),
                c.retention().dedup(), c.retention().inbox());
    }

    @Produces
    @Singleton
    Engine engine(AgroalDataSource ds, Clock clock, EngineConfig cfg, KafkaMessagePublisher publisher, MeterRegistry registry,
                  OpenTelemetry otel, WakeupHub hub) {
        return new Engine(ds, clock, cfg, publisher, new MicrometerMetrics(registry), new OtelTraceContext(otel), hub);
    }
}
