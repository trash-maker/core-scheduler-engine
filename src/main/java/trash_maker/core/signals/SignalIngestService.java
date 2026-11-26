package trash_maker.core.signals;

import trash_maker.common.Json;
import trash_maker.core.EngineConfig;
import trash_maker.core.db.Tx;
import trash_maker.core.dispatch.DeadLetters;
import trash_maker.core.evaluation.EvaluationService;
import trash_maker.core.evaluation.RuleStateRepository;
import trash_maker.core.notifications.NotificationEngine;
import trash_maker.core.ports.EngineMetrics;
import trash_maker.core.rules.RuleRegistry;
import trash_maker.domain.rule.CompiledRule;
import trash_maker.domain.signal.SignalEnvelope;
import org.jboss.logging.Logger;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Signal batch processing in ONE transaction: inbox dedup -> upsert latest state (newer occurred_at wins) ->
 * cancel events -> lock rule_state and evaluate indexed rules per entity -> create notifications/timers.
 * The caller commits Kafka offsets only after this returns (at-least-once input, idempotent processing).
 */
public final class SignalIngestService {
    private static final Logger LOG = Logger.getLogger(SignalIngestService.class);

    private final Tx tx;
    private final SignalStore store;
    private final RuleRegistry registry;
    private final EvaluationService evaluation;
    private final NotificationEngine engine;
    private final DeadLetters dlq;
    private final EngineConfig cfg;
    private final EngineMetrics metrics;
    private final Clock clock;

    public SignalIngestService(Tx tx, SignalStore store, RuleRegistry registry, EvaluationService evaluation, NotificationEngine engine,
                               DeadLetters dlq, EngineConfig cfg, EngineMetrics metrics, Clock clock) {
        this.tx = tx;
        this.store = store;
        this.registry = registry;
        this.evaluation = evaluation;
        this.engine = engine;
        this.dlq = dlq;
        this.cfg = cfg;
        this.metrics = metrics;
        this.clock = clock;
    }

    public record Stats(int received, int invalid, int duplicates, int unreferenced, int outOfOrder, int entitiesEvaluated, int fired, int cancelled) {}

    private record Parsed(SignalEnvelope env, String raw) {}

    public Stats ingest(List<String> payloads) {
        List<Parsed> valid = new ArrayList<>();
        List<String[]> invalid = new ArrayList<>();
        for (String p : payloads) {
            try {
                valid.add(new Parsed(SignalEnvelope.parse(Json.parse(p)), p));
            } catch (RuntimeException e) {
                invalid.add(new String[]{p, e.getMessage()});
            }
        }
        metrics.count("signals.received", payloads.size());
        if (!invalid.isEmpty()) metrics.count("signals.invalid", invalid.size());
        return tx.inTx(c -> {
            Instant now = clock.instant();
            for (String[] inv : invalid) dlq.record(c, "signals.in", null, "INVALID_MESSAGE", inv[1], inv[0], now);
            Set<String> fresh = store.insertInbox(c, valid.stream().map(Parsed::env).toList(), now);
            List<Parsed> accepted = new ArrayList<>();
            Set<String> seen = new HashSet<>();
            int dups = 0, unref = 0;
            for (Parsed p : valid) {
                String id = p.env().tenantId() + "|" + p.env().signalId();
                if (!fresh.contains(id) || !seen.add(id)) { dups++; continue; }
                RuleRegistry.TenantRules tr = registry.tenant(p.env().tenantId());
                if (!tr.isReferenced(p.env().signal()) && !cfg.storeUnreferencedSignals()) { unref++; continue; }
                accepted.add(p);
            }
            if (dups > 0) metrics.count("signals.duplicate", dups);
            if (unref > 0) metrics.count("signals.unreferenced", unref);

            // latest value per (tenant, key, signal) within the batch
            Map<String, Parsed> reduced = new LinkedHashMap<>();
            for (Parsed p : accepted) {
                String k = p.env().tenantId() + "|" + p.env().correlationKey() + "|" + p.env().signal();
                Parsed prev = reduced.get(k);
                if (prev == null || !p.env().occurredAt().isBefore(prev.env().occurredAt())) reduced.put(k, p);
            }
            Set<String> changed = store.upsert(c, reduced.values().stream().map(Parsed::env).toList(), now);
            int outOfOrder = reduced.size() - changed.size();
            if (outOfOrder > 0) metrics.count("signals.out_of_order_ignored", outOfOrder);

            // group per entity (sorted: stable lock order)
            Map<SignalStore.EntityKey, List<Parsed>> changedByEntity = new TreeMap<>();
            Map<SignalStore.EntityKey, List<Parsed>> acceptedByEntity = new TreeMap<>();
            for (Parsed p : accepted) acceptedByEntity.computeIfAbsent(key(p), k -> new ArrayList<>()).add(p);
            for (var e : reduced.entrySet()) if (changed.contains(e.getKey())) changedByEntity.computeIfAbsent(key(e.getValue()), k -> new ArrayList<>()).add(e.getValue());

            // cancel events (any newly received cancel signal, independent of ordering)
            int cancelled = 0;
            for (var e : acceptedByEntity.entrySet()) {
                RuleRegistry.TenantRules tr = registry.tenant(e.getKey().tenantId());
                Set<String> names = new LinkedHashSet<>();
                for (Parsed p : e.getValue()) if (!tr.rulesCancelledBy(p.env().signal()).isEmpty()) names.add(p.env().signal());
                for (String s : names) {
                    try {
                        cancelled += Tx.savepoint(c, cx -> engine.cancelBySignal(cx, e.getKey().tenantId(), e.getKey().correlationKey(), s, now));
                    } catch (RuntimeException ex) {
                        LOG.errorf(ex, "cancel by event %s failed for %s", s, e.getKey());
                        dlq.record(c, "signals.in", e.getKey().tenantId(), "CANCEL_FAILED", ex.getMessage(), null, now);
                    }
                }
            }

            // evaluation work
            Map<SignalStore.EntityKey, List<CompiledRule>> rulesByEntity = new LinkedHashMap<>();
            List<RuleStateRepository.Key> lockKeys = new ArrayList<>();
            for (var e : changedByEntity.entrySet()) {
                RuleRegistry.TenantRules tr = registry.tenant(e.getKey().tenantId());
                Map<String, CompiledRule> rules = new TreeMap<>();
                for (Parsed p : e.getValue()) for (CompiledRule r : tr.rulesForSignal(p.env().signal())) rules.put(r.id(), r);
                if (rules.isEmpty()) continue;
                rulesByEntity.put(e.getKey(), List.copyOf(rules.values()));
                for (CompiledRule r : rules.values()) lockKeys.add(new RuleStateRepository.Key(e.getKey().tenantId(), r.id(), e.getKey().correlationKey()));
            }
            int fired = 0;
            if (!rulesByEntity.isEmpty()) {
                var signals = store.load(c, rulesByEntity.keySet());
                var locked = evaluation.states().lockAll(c, lockKeys);
                Map<SignalStore.EntityKey, EvaluationService.Buffer> buffers = new LinkedHashMap<>();
                EvaluationService.Buffer all = new EvaluationService.Buffer();
                for (var e : rulesByEntity.entrySet()) {
                    List<Parsed> causes = changedByEntity.get(e.getKey());
                    Parsed trigger = causes.stream().max((a, b) -> a.env().occurredAt().compareTo(b.env().occurredAt())).orElseThrow();
                    var work = new EvaluationService.EntityWork(e.getKey().tenantId(), e.getKey().correlationKey(), signals.get(e.getKey()),
                            trigger.env().toJson(), trigger.raw(), e.getValue());
                    EvaluationService.Buffer b = new EvaluationService.Buffer();
                    try {
                        evaluation.evaluateInto(work, locked, now, b);
                        buffers.put(e.getKey(), b);
                        all.addAll(b);
                    } catch (RuntimeException ex) {
                        LOG.errorf(ex, "evaluation failed for %s", e.getKey());
                        metrics.count("evaluation.failed");
                        for (Parsed p : causes) dlq.record(c, "signals.in", p.env().tenantId(), "EVALUATION_FAILED", ex.getMessage(), p.raw(), now);
                    }
                }
                try {
                    fired = Tx.savepoint(c, cx -> evaluation.flush(cx, all, now));
                } catch (RuntimeException | java.sql.SQLException batchFailure) {
                    // isolate the poison entity: retry entity by entity, dead-letter the failing ones
                    LOG.warnf("batched flush failed (%s), retrying per entity", batchFailure.getMessage());
                    for (var e : buffers.entrySet()) {
                        try {
                            fired += Tx.savepoint(c, cx -> evaluation.flush(cx, e.getValue(), now));
                        } catch (RuntimeException | java.sql.SQLException ex) {
                            LOG.errorf(ex, "persisting evaluation failed for %s", e.getKey());
                            metrics.count("evaluation.failed");
                            for (Parsed p : changedByEntity.get(e.getKey()))
                                dlq.record(c, "signals.in", p.env().tenantId(), "EVALUATION_FAILED", ex.getMessage(), p.raw(), now);
                        }
                    }
                }
            }
            return new Stats(payloads.size(), invalid.size(), dups, unref, outOfOrder, rulesByEntity.size(), fired, cancelled);
        });
    }

    private static SignalStore.EntityKey key(Parsed p) { return new SignalStore.EntityKey(p.env().tenantId(), p.env().correlationKey()); }

    @SuppressWarnings("unused")
    private static <K, V> Map<K, V> map() { return new HashMap<>(); }
}
