package trash_maker.core;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.time.Clock;

/** Facade over the engine's CDI beans (used by adapters and tests). */
@Singleton
public class Engine {
    @Inject public EngineConfig config;
    @Inject public Clock clock;
    @Inject public Db db;
    @Inject public RuleRegistry registry;
    @Inject public RuleService rules;
    @Inject public SignalStore signalStore;
    @Inject public SignalIngestService ingest;
    @Inject public NotificationEngine notifications;
    @Inject public EvaluationService evaluation;
    @Inject public TimerRepository timerRepository;
    @Inject public TimerService timers;
    @Inject public OutboxRelay relay;
    @Inject public ResultService results;
    @Inject public Maintenance maintenance;
    @Inject public Outbox outbox;

    /** Application-level errors mapped to HTTP status codes by the API layer. */
    public static final class CoreExceptions {
        private CoreExceptions() {}

        public static class NotFound extends RuntimeException {
            public NotFound(String m) { super(m); }
        }

        public static class Conflict extends RuntimeException {
            public Conflict(String m) { super(m); }
        }
    }
}
