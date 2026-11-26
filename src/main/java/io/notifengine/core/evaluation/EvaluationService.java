package io.notifengine.core.evaluation;

import com.fasterxml.jackson.databind.JsonNode;
import io.notifengine.core.EngineConfig;
import io.notifengine.core.db.Tx;
import io.notifengine.core.dispatch.DeadLetters;
import io.notifengine.core.notifications.NotificationEngine;
import io.notifengine.core.ports.EngineMetrics;
import io.notifengine.core.timers.TimerRepository;
import io.notifengine.domain.body.BodySchemaValidator;
import io.notifengine.domain.evaluation.RuleEvaluation;
import io.notifengine.domain.rule.CompiledRule;
import io.notifengine.domain.signal.SignalValue;
import org.jboss.logging.Logger;

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
public final class EvaluationService {
    private static final Logger LOG = Logger.getLogger(EvaluationService.class);

    private final RuleStateRepository states;
    private final NotificationEngine engine;
    private final TimerRepository timers;
    private final DeadLetters dlq;
    private final EngineConfig cfg;
    private final EngineMetrics metrics;
    private final BodySchemaValidator schemas;

    public EvaluationService(RuleStateRepository states, NotificationEngine engine, TimerRepository timers, DeadLetters dlq,
                             EngineConfig cfg, EngineMetrics metrics, BodySchemaValidator schemas) {
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

    public int evaluate(Connection c, EntityWork w, Map<RuleStateRepository.Key, RuleStateRepository.State> locked, Instant now) throws SQLException {
        List<RuleStateRepository.State> updates = new ArrayList<>();
        int fired = 0;
        for (CompiledRule rule : w.rules()) {
            RuleStateRepository.Key key = new RuleStateRepository.Key(w.tenantId(), rule.id(), w.correlationKey());
            RuleStateRepository.State st = locked.get(key);
            if (st == null) throw new IllegalStateException("rule_state not locked for " + key);
            long t0 = System.nanoTime();
            RuleEvaluation.Result res = RuleEvaluation.run(rule, w.tenantId(), w.correlationKey(), w.signals(), w.trigger(), now,
                    new RuleEvaluation.PriorState(st.lastResult(), st.edgeSeq(), st.lastFiredAt(), st.memory()),
                    cfg.defaultSignalMaxAge(), schemas);
            metrics.recordMs("evaluation.time", (System.nanoTime() - t0) / 1_000_000);
            boolean fire = res.decision().fire();
            updates.add(new RuleStateRepository.State(key, res.decision().newLastResult(), res.decision().newEdgeSeq(),
                    fire ? now : st.lastFiredAt(), now, rule.version(), res.evaluation().memory()));
            if (fire) {
                if (res.planningError() != null) {
                    metrics.count("rules.fire_failed", "reason", res.planningError().code());
                    dlq.record(c, "rule-firing", w.tenantId(), res.planningError().code(),
                            "rule " + rule.id() + " v" + rule.version() + " key " + w.correlationKey() + ": " + res.planningError().getMessage(),
                            w.triggerPayload(), now);
                } else {
                    try {
                        Tx.savepoint(c, cx -> engine.create(cx, rule, w.tenantId(), w.correlationKey(), res.plan(), now));
                        fired++;
                        metrics.count("rules.fired");
                    } catch (RuntimeException e) {
                        LOG.errorf(e, "creating notifications failed for rule %s key %s", rule.id(), w.correlationKey());
                        dlq.record(c, "rule-firing", w.tenantId(), "CREATE_FAILED", "rule " + rule.id() + ": " + e.getMessage(), w.triggerPayload(), now);
                    }
                }
            }
            if (rule.entityTimed()) {
                if (res.evaluation().nextEntityChange() != null)
                    timers.upsertRuleEval(c, w.tenantId(), rule.id(), w.correlationKey(), res.evaluation().nextEntityChange(), null);
                else timers.deleteRuleEval(c, w.tenantId(), rule.id(), w.correlationKey());
            }
        }
        states.updateAll(c, updates);
        return fired;
    }
}
