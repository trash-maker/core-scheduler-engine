package trash_maker.core.rules;

import trash_maker.core.db.Tx;
import trash_maker.domain.rule.CompiledRule;
import trash_maker.domain.rule.RuleParser;
import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * In-memory cache of compiled current rule versions with reverse indexes signal -> rules.
 * Hot reload: incremental refresh by rule.change_seq (polled; also called after local writes).
 * Snapshots are immutable and swapped atomically.
 */
public final class RuleRegistry {
    private static final Logger LOG = Logger.getLogger(RuleRegistry.class);

    /** Per-tenant immutable view. */
    public record TenantRules(Map<String, CompiledRule> byId, Map<String, List<CompiledRule>> byExprSignal,
                              Map<String, List<CompiledRule>> byCancelSignal, Set<String> referenced) {
        static final TenantRules EMPTY = new TenantRules(Map.of(), Map.of(), Map.of(), Set.of());

        public List<CompiledRule> rulesForSignal(String s) { return byExprSignal.getOrDefault(s, List.of()); }

        public List<CompiledRule> rulesCancelledBy(String s) { return byCancelSignal.getOrDefault(s, List.of()); }

        public boolean isReferenced(String s) { return referenced.contains(s); }
    }

    private final Tx tx;
    private final RuleRepository repo;
    private final RuleParser.Options options;
    private final Map<String, Map<String, CompiledRule>> mutable = new HashMap<>();
    private volatile Map<String, TenantRules> snapshot = Map.of();
    private long seenSeq = -1;

    public RuleRegistry(Tx tx, RuleRepository repo, RuleParser.Options options) {
        this.tx = tx;
        this.repo = repo;
        this.options = options;
    }

    public TenantRules tenant(String tenant) { return snapshot.getOrDefault(tenant, TenantRules.EMPTY); }

    public CompiledRule rule(String tenant, String ruleId) { return tenant(tenant).byId().get(ruleId); }

    public Map<String, TenantRules> all() { return snapshot; }

    /** Full reload (safety net against change_seq values committed out of order). */
    public synchronized void reloadAll() {
        mutable.clear();
        seenSeq = -1;
        refresh();
    }

    /** Loads changes since the last refresh. Cheap when nothing changed (one indexed max() query). */
    public synchronized void refresh() {
        tx.read(c -> {
            long max = repo.maxChangeSeq(c);
            if (max == seenSeq) return null;
            List<RuleRecord> changed = repo.changedSince(c, seenSeq);
            for (RuleRecord r : changed) {
                Map<String, CompiledRule> t = mutable.computeIfAbsent(r.tenantId(), k -> new HashMap<>());
                if (!r.current()) {
                    CompiledRule existing = t.get(r.ruleId());
                    if (existing != null && existing.version() == r.version()) t.remove(r.ruleId());
                    continue;
                }
                if (!r.enabled()) { t.remove(r.ruleId()); continue; }
                try {
                    t.put(r.ruleId(), RuleParser.compile(r.definition(), options));
                } catch (RuntimeException e) {
                    LOG.errorf("rule %s/%s v%d failed to compile and is ignored: %s", r.tenantId(), r.ruleId(), r.version(), e.getMessage());
                    t.remove(r.ruleId());
                }
            }
            seenSeq = Math.max(max, changed.isEmpty() ? max : changed.get(changed.size() - 1).changeSeq());
            rebuild();
            return null;
        });
    }

    private void rebuild() {
        Map<String, TenantRules> next = new HashMap<>();
        mutable.forEach((tenant, rules) -> {
            Map<String, List<CompiledRule>> expr = new HashMap<>();
            Map<String, List<CompiledRule>> cancel = new HashMap<>();
            java.util.Set<String> referenced = new java.util.HashSet<>();
            for (CompiledRule r : rules.values()) {
                r.exprSignals().forEach(s -> expr.computeIfAbsent(s, k -> new ArrayList<>()).add(r));
                r.cancelSignals().keySet().forEach(s -> cancel.computeIfAbsent(s, k -> new ArrayList<>()).add(r));
                referenced.addAll(r.referencedSignals());
            }
            expr.replaceAll((k, v) -> List.copyOf(v));
            cancel.replaceAll((k, v) -> List.copyOf(v));
            next.put(tenant, new TenantRules(Map.copyOf(rules), Map.copyOf(expr), Map.copyOf(cancel), Set.copyOf(referenced)));
        });
        snapshot = Collections.unmodifiableMap(next);
    }
}
