package trash_maker.core;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import trash_maker.core.db.Db;
import trash_maker.core.dispatch.DeadLetters;
import trash_maker.core.dispatch.OutboxRelay;
import trash_maker.core.evaluation.DryRunService;
import trash_maker.core.evaluation.EvaluationService;
import trash_maker.core.notifications.NotificationCommands;
import trash_maker.core.notifications.NotificationEngine;
import trash_maker.core.ops.Maintenance;
import trash_maker.core.results.ResultService;
import trash_maker.core.rules.RuleRegistry;
import trash_maker.core.rules.RuleService;
import trash_maker.core.signals.SignalIngestService;
import trash_maker.core.signals.SignalStore;
import trash_maker.core.timers.TimerRepository;
import trash_maker.core.timers.TimerService;

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
    @Inject public NotificationCommands commands;
    @Inject public EvaluationService evaluation;
    @Inject public TimerRepository timerRepository;
    @Inject public TimerService timers;
    @Inject public OutboxRelay relay;
    @Inject public ResultService results;
    @Inject public DryRunService dryRun;
    @Inject public Maintenance maintenance;
    @Inject public DeadLetters deadLetters;
}
