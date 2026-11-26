package io.notifengine.core.evaluation;

import com.fasterxml.jackson.databind.JsonNode;
import io.notifengine.core.EngineConfig;
import io.notifengine.domain.body.BodySchemaValidator;
import io.notifengine.domain.evaluation.RuleEvaluation;
import io.notifengine.domain.expr.LeafMemory;
import io.notifengine.domain.rule.CompiledRule;
import io.notifengine.domain.signal.SignalValue;

import java.time.Instant;
import java.util.Map;

/** Dry-run: runs exactly the production evaluation pipeline on supplied inputs, without any side effect. */
public final class DryRunService {
    private final EngineConfig cfg;
    private final BodySchemaValidator schemas;

    public DryRunService(EngineConfig cfg, BodySchemaValidator schemas) {
        this.cfg = cfg;
        this.schemas = schemas;
    }

    public RuleEvaluation.Result run(CompiledRule rule, String correlationKey, Map<String, SignalValue> signals, JsonNode trigger,
                                     Instant now, boolean lastResult, long edgeSeq, Instant lastFiredAt, Map<String, LeafMemory> memory) {
        return RuleEvaluation.run(rule, rule.tenantId(), correlationKey, signals, trigger, now,
                new RuleEvaluation.PriorState(lastResult, edgeSeq, lastFiredAt, memory == null ? Map.of() : memory),
                cfg.defaultSignalMaxAge(), schemas);
    }
}
