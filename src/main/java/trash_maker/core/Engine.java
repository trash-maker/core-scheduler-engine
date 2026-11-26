package trash_maker.core;

import trash_maker.core.db.Tx;
import trash_maker.core.dispatch.DeadLetters;
import trash_maker.core.dispatch.OutboxRelay;
import trash_maker.core.dispatch.OutboxRepository;
import trash_maker.core.evaluation.DryRunService;
import trash_maker.core.evaluation.EvaluationService;
import trash_maker.core.evaluation.RuleStateRepository;
import trash_maker.core.notifications.NotificationEngine;
import trash_maker.core.notifications.NotificationRepository;
import trash_maker.core.notifications.SeriesRepository;
import trash_maker.core.ops.Maintenance;
import trash_maker.core.ports.EngineMetrics;
import trash_maker.core.ports.MessagePublisher;
import trash_maker.core.ports.TimerWakeup;
import trash_maker.core.ports.TraceContext;
import trash_maker.core.results.ResultService;
import trash_maker.core.rules.RuleRegistry;
import trash_maker.core.rules.RuleRepository;
import trash_maker.core.rules.RuleService;
import trash_maker.core.signals.SignalIngestService;
import trash_maker.core.signals.SignalStore;
import trash_maker.core.timers.TimerRepository;
import trash_maker.core.timers.TimerService;
import trash_maker.domain.notification.StateMachine;
import trash_maker.domain.rule.RuleParser;

import javax.sql.DataSource;
import java.time.Clock;
import java.util.concurrent.ThreadLocalRandom;

/** Framework-free wiring of the engine (used by the Quarkus producer and by integration tests). */
public final class Engine {
    public final EngineConfig config;
    public final Clock clock;
    public final Tx tx;
    public final RuleRegistry registry;
    public final RuleService rules;
    public final SignalStore signalStore;
    public final SignalIngestService ingest;
    public final NotificationEngine notifications;
    public final EvaluationService evaluation;
    public final TimerRepository timerRepository;
    public final TimerService timers;
    public final OutboxRelay relay;
    public final ResultService results;
    public final DryRunService dryRun;
    public final Maintenance maintenance;
    public final DeadLetters deadLetters;

    public Engine(DataSource ds, Clock clock, EngineConfig cfg, MessagePublisher publisher, EngineMetrics metrics, TraceContext trace,
                  TimerWakeup wakeup) {
        this.config = cfg;
        this.clock = clock;
        this.tx = new Tx(ds);
        SchemaValidators schemas = new SchemaValidators();
        RuleParser.Options options = new RuleParser.Options(cfg.allowedActions(), SchemaValidators.checker());
        RuleRepository ruleRepo = new RuleRepository();
        this.registry = new RuleRegistry(tx, ruleRepo, options);
        this.timerRepository = new TimerRepository(wakeup);
        OutboxRepository outbox = new OutboxRepository();
        this.deadLetters = new DeadLetters(outbox, cfg, metrics);
        NotificationRepository notifRepo = new NotificationRepository();
        this.notifications = new NotificationEngine(notifRepo, new SeriesRepository(), timerRepository, outbox, deadLetters,
                new StateMachine(() -> ThreadLocalRandom.current().nextDouble()), cfg, metrics, trace, schemas);
        RuleStateRepository stateRepo = new RuleStateRepository();
        this.evaluation = new EvaluationService(stateRepo, notifications, timerRepository, deadLetters, cfg, metrics, schemas);
        this.signalStore = new SignalStore();
        this.rules = new RuleService(tx, ruleRepo, registry, timerRepository, stateRepo, options, clock);
        this.ingest = new SignalIngestService(tx, signalStore, registry, evaluation, notifications, deadLetters, cfg, metrics, clock);
        this.timers = new TimerService(tx, timerRepository, notifications, evaluation, signalStore, registry, cfg, metrics, clock);
        this.relay = new OutboxRelay(tx, outbox, timerRepository, notifRepo, publisher, cfg, metrics, clock);
        this.results = new ResultService(tx, notifications, deadLetters, metrics, clock);
        this.dryRun = new DryRunService(cfg, schemas);
        this.maintenance = new Maintenance(tx, cfg, clock);
    }
}
