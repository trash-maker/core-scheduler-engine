package trash_maker.infra.config;

import io.micrometer.core.instrument.MeterRegistry;
import trash_maker.core.SchemaValidators;
import trash_maker.core.ports.EngineMetrics;
import trash_maker.core.ports.MessagePublisher;
import trash_maker.core.ports.TraceContext;
import trash_maker.domain.notification.StateMachine;
import trash_maker.domain.rule.RuleParser;
import java.util.concurrent.ThreadLocalRandom;
import trash_maker.core.EngineConfig;
import trash_maker.core.timers.WakeupHub;
import trash_maker.infra.kafka.KafkaMessagePublisher;
import trash_maker.infra.ops.MicrometerMetrics;
import trash_maker.infra.ops.OtelTraceContext;
import io.opentelemetry.api.OpenTelemetry;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Disposes;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/** Produces the engine's ports and configuration (core services are CDI beans themselves). */
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
    MessagePublisher publisher(@ConfigProperty(name = "kafka.bootstrap.servers") String bootstrap, NotificationsConfig c) {
        Map<String, String> props = new HashMap<>();
        for (String kv : c.kafka().producer().extra().split(",")) {
            int i = kv.indexOf('=');
            if (i > 0) props.put(kv.substring(0, i).trim(), kv.substring(i + 1).trim());
        }
        return new KafkaMessagePublisher(bootstrap, props);
    }

    void close(@Disposes MessagePublisher p) throws Exception {
        if (p instanceof AutoCloseable a) a.close();
    }

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
    RuleParser.Options ruleOptions(EngineConfig cfg) { return new RuleParser.Options(cfg.allowedActions(), SchemaValidators.checker()); }

    @Produces
    @Singleton
    StateMachine stateMachine() { return new StateMachine(() -> ThreadLocalRandom.current().nextDouble()); }

    @Produces
    @Singleton
    EngineMetrics metrics(MeterRegistry registry) { return new MicrometerMetrics(registry); }

    @Produces
    @Singleton
    TraceContext trace(OpenTelemetry otel) { return new OtelTraceContext(otel); }
}
