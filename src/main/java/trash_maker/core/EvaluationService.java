package trash_maker.core;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import trash_maker.domain.body.BodyResolver.BodySchemaValidator;
import trash_maker.domain.evaluation.RuleEvaluation;
import trash_maker.domain.expr.Expr.LeafMemory;
import trash_maker.domain.rule.Rule.CompiledRule;
import trash_maker.domain.signal.Signals.SignalValue;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Evaluates rules for one entity inside the caller's transaction, with rule_state rows already locked.
 * Persists the new rule state, creates notifications on an edge, and maintains the per-entity RULE_EVAL timer.
 */
@Singleton
public class EvaluationService {

    private final RuleStateRepository states;
    private final NotificationEngine engine;
    private final TimerRepository timers;
    private final Outbox dlq;
    private final NotificationsConfig cfg;
    private final Metrics metrics;
    private final BodySchemaValidator schemas;

    @Inject
    public EvaluationService(RuleStateRepository states, NotificationEngine engine, TimerRepository timers, Outbox dlq,
                             NotificationsConfig cfg, Metrics metrics, BodySchemaValidator schemas) {
        this.states = states;
        this.engine = engine;
        this.timers = timers;
        this.dlq = dlq;
        this.cfg = cfg;
        this.metrics = metrics;
        this.schemas = schemas;
    }

    public RuleStateRepository states() { return states; }

    /** Work item: one entity, the rules to evaluate and the message that caused it (null when time-triggered). */
    public record EntityWork(String tenantId, String correlationKey, Map<String, SignalValue> signals, JsonNode trigger,
                             String triggerPayload, List<CompiledRule> rules) {}

    /** Writes produced by evaluations, flushed with batched statements. */
    public static final class Buffer {
        final List<RuleStateRepository.State> states = new ArrayList<>();
        final List<TimerRepository.NewTimer> ruleEvalUpserts = new ArrayList<>();
        final List<String[]> ruleEvalDeletes = new ArrayList<>();
        final List<NotificationEngine.Firing> firings = new ArrayList<>();
        final List<String[]> deadLetters = new ArrayList<>();

        public void addAll(Buffer o) {
            states.addAll(o.states);
            ruleEvalUpserts.addAll(o.ruleEvalUpserts);
            ruleEvalDeletes.addAll(o.ruleEvalDeletes);
            firings.addAll(o.firings);
            deadLetters.addAll(o.deadLetters);
        }

        public int firings() { return firings.size(); }
    }

    /** Evaluates the rules of one entity in memory (no I/O); writes go to {@code buf}. */
    public void evaluateInto(EntityWork w, Map<RuleStateRepository.Key, RuleStateRepository.State> locked, Instant now, Buffer buf) {
        for (CompiledRule rule : w.rules()) {
            RuleStateRepository.Key key = new RuleStateRepository.Key(w.tenantId(), rule.id(), w.correlationKey());
            RuleStateRepository.State st = locked.get(key);
            if (st == null) throw new IllegalStateException("rule_state not locked for " + key);
            long t0 = System.nanoTime();
            RuleEvaluation.Result res = RuleEvaluation.run(rule, w.tenantId(), w.correlationKey(), w.signals(), w.trigger(), now,
                    new RuleEvaluation.PriorState(st.lastResult(), st.edgeSeq(), st.lastFiredAt(), st.memory()),
                    cfg.signals().defaultMaxAge().orElse(null), schemas);
            metrics.recordMs("evaluation.time", (System.nanoTime() - t0) / 1_000_000);
            boolean fire = res.decision().fire();
            RuleEvaluation.PriorState next = res.nextState(new RuleEvaluation.PriorState(st.lastResult(), st.edgeSeq(), st.lastFiredAt(), st.memory()), now);
            buf.states.add(new RuleStateRepository.State(key, next.lastResult(), next.edgeSeq(), next.lastFiredAt(), now, rule.version(), next.memory()));
            if (fire) {
                if (res.planningError() != null) {
                    metrics.count("rules.fire_failed", "reason", res.planningError().code());
                    buf.deadLetters.add(new String[]{w.tenantId(), res.planningError().code(),
                            "rule " + rule.id() + " v" + rule.version() + " key " + w.correlationKey() + ": " + res.planningError().getMessage(),
                            w.triggerPayload()});
                } else {
                    buf.firings.add(new NotificationEngine.Firing(rule, w.tenantId(), w.correlationKey(), res.plan()));
                    metrics.count("rules.fired");
                }
            }
            RuleEvaluation.EntityTimer et = res.entityTimer(rule.entityTimed());
            if (et.schedule())
                buf.ruleEvalUpserts.add(new TimerRepository.NewTimer(trash_maker.domain.notification.Lifecycle.TimerKind.RULE_EVAL,
                        et.at(), w.tenantId(), null, null, rule.id(), w.correlationKey(), null));
            else if (et == RuleEvaluation.EntityTimer.DELETE) buf.ruleEvalDeletes.add(new String[]{w.tenantId(), rule.id(), w.correlationKey()});
        }
    }

    /** Persists a buffer: rule states, RULE_EVAL timers, dead letters and notifications, each with one batched statement. */
    public int flush(Connection c, Buffer b, Instant now) throws SQLException {
        states.updateAll(c, b.states);
        timers.upsertRuleEvalAll(c, b.ruleEvalUpserts);
        timers.deleteRuleEvalAll(c, b.ruleEvalDeletes);
        for (String[] d : b.deadLetters) dlq.record(c, "rule-firing", d[0], d[1], d[2], d[3], now);
        if (!b.firings.isEmpty()) engine.createAll(c, b.firings, now);
        return b.firings.size();
    }

    /** Dry-run: exactly the production pipeline on supplied inputs, without side effects. */
    public RuleEvaluation.Result dryRun(CompiledRule rule, String correlationKey, Map<String, SignalValue> signals, JsonNode trigger,
                                        Instant now, boolean lastResult, long edgeSeq, Instant lastFiredAt, Map<String, LeafMemory> memory) {
        return RuleEvaluation.run(rule, rule.tenantId(), correlationKey, signals, trigger, now,
                new RuleEvaluation.PriorState(lastResult, edgeSeq, lastFiredAt, memory == null ? Map.of() : memory),
                cfg.signals().defaultMaxAge().orElse(null), schemas);
    }

    public int evaluate(Connection c, EntityWork w, Map<RuleStateRepository.Key, RuleStateRepository.State> locked, Instant now) throws SQLException {
        Buffer b = new Buffer();
        evaluateInto(w, locked, now, b);
        return flush(c, b, now);
    }
}
