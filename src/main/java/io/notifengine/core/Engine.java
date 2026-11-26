package io.notifengine.core;

import io.notifengine.core.db.Tx;
import io.notifengine.core.dispatch.DeadLetters;
import io.notifengine.core.dispatch.OutboxRelay;
import io.notifengine.core.dispatch.OutboxRepository;
import io.notifengine.core.evaluation.DryRunService;
import io.notifengine.core.evaluation.EvaluationService;
import io.notifengine.core.evaluation.RuleStateRepository;
import io.notifengine.core.notifications.NotificationEngine;
import io.notifengine.core.notifications.NotificationRepository;
import io.notifengine.core.notifications.SeriesRepository;
import io.notifengine.core.ops.Maintenance;
import io.notifengine.core.ports.EngineMetrics;
import io.notifengine.core.ports.MessagePublisher;
import io.notifengine.core.ports.TimerWakeup;
import io.notifengine.core.ports.TraceContext;
import io.notifengine.core.results.ResultService;
import io.notifengine.core.rules.RuleRegistry;
import io.notifengine.core.rules.RuleRepository;
import io.notifengine.core.rules.RuleService;
import io.notifengine.core.signals.SignalIngestService;
import io.notifengine.core.signals.SignalStore;
import io.notifengine.core.timers.TimerRepository;
import io.notifengine.core.timers.TimerService;
import io.notifengine.domain.notification.StateMachine;
import io.notifengine.domain.rule.RuleParser;

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
