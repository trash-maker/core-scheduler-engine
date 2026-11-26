package trash_maker.core.evaluation;

import jakarta.inject.Singleton;
import jakarta.inject.Inject;

import com.fasterxml.jackson.databind.JsonNode;
import trash_maker.core.EngineConfig;
import trash_maker.domain.body.BodySchemaValidator;
import trash_maker.domain.evaluation.RuleEvaluation;
import trash_maker.domain.expr.LeafMemory;
import trash_maker.domain.rule.CompiledRule;
import trash_maker.domain.signal.SignalValue;

import java.time.Instant;
import java.util.Map;

/** Dry-run: runs exactly the production evaluation pipeline on supplied inputs, without any side effect. */
@Singleton
public class DryRunService {
    private final EngineConfig cfg;
    private final BodySchemaValidator schemas;

    @Inject
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
