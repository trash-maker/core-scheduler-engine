package io.notifengine.core.rules;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.notifengine.common.Json;
import io.notifengine.common.ValidationError;
import io.notifengine.common.ValidationException;
import io.notifengine.core.CoreExceptions;
import io.notifengine.core.db.Tx;
import io.notifengine.core.evaluation.RuleStateRepository;
import io.notifengine.core.timers.TimerRepository;
import io.notifengine.core.timers.TimerRow;
import io.notifengine.domain.rule.CompiledRule;
import io.notifengine.domain.rule.RuleParser;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Instant;
import java.util.List;

/** Rule CRUD with immutable versions, validation at save time and hot reload of the local registry. */
public final class RuleService {
    private final Tx tx;
    private final RuleRepository repo;
    private final RuleRegistry registry;
    private final TimerRepository timers;
    private final RuleStateRepository states;
    private final RuleParser.Options options;
    private final Clock clock;

    public RuleService(Tx tx, RuleRepository repo, RuleRegistry registry, TimerRepository timers, RuleStateRepository states,
                       RuleParser.Options options, Clock clock) {
        this.tx = tx;
        this.repo = repo;
        this.registry = registry;
        this.timers = timers;
        this.states = states;
        this.options = options;
        this.clock = clock;
    }

    public RuleParser.Options options() { return options; }

    /** Normalizes ids from the request path and validates. */
    public List<ValidationError> validate(String tenant, JsonNode def) {
        return RuleParser.validate(normalize(tenant, null, def, 1), options);
    }

    private static ObjectNode normalize(String tenant, String ruleId, JsonNode def, int version) {
        if (def == null || !def.isObject()) throw new ValidationException(List.of(new ValidationError("$", "rule must be a JSON object")));
        ObjectNode o = ((ObjectNode) def).deepCopy();
        if (o.hasNonNull("tenantId") && !o.get("tenantId").asText().equals(tenant))
            throw new ValidationException(List.of(new ValidationError("$.tenantId", "does not match the tenant in the path")));
        o.put("tenantId", tenant);
        if (ruleId != null) {
            if (o.hasNonNull("id") && !o.get("id").asText().equals(ruleId))
                throw new ValidationException(List.of(new ValidationError("$.id", "does not match the rule id in the path")));
            o.put("id", ruleId);
        }
        o.put("version", version);
        if (!o.has("enabled")) o.put("enabled", true);
        return o;
    }

    public RuleRecord create(String tenant, JsonNode def, String by) {
        ObjectNode n = normalize(tenant, null, def, 1);
        CompiledRule compiled = RuleParser.compile(n, options);
        RuleRecord rec = tx.inTx(c -> {
            if (repo.current(c, tenant, compiled.id(), true).isPresent() || repo.version(c, tenant, compiled.id(), 1).isPresent())
                throw new CoreExceptions.Conflict("rule '" + compiled.id() + "' already exists; use PUT to create a new version");
            RuleRecord r = RuleRepository.newRecord(tenant, compiled.id(), 1, compiled.def().enabled(), n, clock.instant(), by);
            repo.insert(c, r);
            scheduleSweep(c, compiled);
            return r;
        });
        registry.refresh();
        return rec;
    }

    public RuleRecord update(String tenant, String ruleId, JsonNode def, String by) {
        RuleRecord rec = tx.inTx(c -> {
            RuleRecord cur = repo.current(c, tenant, ruleId, true).orElseThrow(() -> new CoreExceptions.NotFound("rule " + ruleId));
            int next = cur.version() + 1;
            if (def.hasNonNull("version") && def.get("version").asInt() != next && def.get("version").asInt() != cur.version())
                throw new CoreExceptions.Conflict("stale version " + def.get("version").asInt() + " (current " + cur.version() + ")");
            ObjectNode n = normalize(tenant, ruleId, def, next);
            CompiledRule compiled = RuleParser.compile(n, options);
            repo.markNotCurrent(c, tenant, ruleId, cur.version());
            RuleRecord r = RuleRepository.newRecord(tenant, ruleId, next, compiled.def().enabled(), n, clock.instant(), by);
            repo.insert(c, r);
            scheduleSweep(c, compiled);
            return r;
        });
        registry.refresh();
        return rec;
    }

    public RuleRecord setEnabled(String tenant, String ruleId, boolean enabled) {
        RuleRecord rec = tx.inTx(c -> {
            if (repo.setEnabled(c, tenant, ruleId, enabled) == 0) throw new CoreExceptions.NotFound("rule " + ruleId);
            RuleRecord r = repo.current(c, tenant, ruleId, false).orElseThrow();
            if (enabled) scheduleSweep(c, RuleParser.compile(r.definition(), options));
            return r;
        });
        registry.refresh();
        return rec;
    }

    private void scheduleSweep(Connection c, CompiledRule r) throws SQLException {
        if (!r.def().enabled() || !r.hasNowCondition()) return;
        Instant now = clock.instant();
        Instant next = r.nextRuleChange(now);
        if (next != null) timers.upsertSweep(c, r.tenantId(), r.id(), next, Json.object().put("at", next.toString()));
    }

    /** Ensures every enabled NOW-based rule has a sweep timer (startup safety net). */
    public int ensureSweeps() {
        registry.refresh();
        int[] n = {0};
        tx.run(c -> {
            for (var tr : registry.all().values()) for (CompiledRule r : tr.byId().values()) {
                if (r.hasNowCondition()) { scheduleSweep(c, r); n[0]++; }
            }
        });
        return n[0];
    }

    public RuleRecord get(String tenant, String ruleId, Integer version) {
        return tx.read(c -> (version == null ? repo.current(c, tenant, ruleId, false) : repo.version(c, tenant, ruleId, version))
                .orElseThrow(() -> new CoreExceptions.NotFound("rule " + ruleId + (version == null ? "" : " v" + version))));
    }

    public List<RuleRecord> list(String tenant, String after, int limit) { return tx.read(c -> repo.listCurrent(c, tenant, after, limit)); }

    public List<RuleRecord> versions(String tenant, String ruleId) { return tx.read(c -> repo.versions(c, tenant, ruleId)); }

    public CompiledRule compiledCurrent(String tenant, String ruleId) {
        return RuleParser.compile(get(tenant, ruleId, null).definition(), options);
    }

    /** Resets the firing state of one entity for a rule (next true evaluation fires again). */
    public boolean resetState(String tenant, String ruleId, String key) {
        return tx.inTx(c -> {
            timers.deleteRuleEval(c, tenant, ruleId, key);
            return states.delete(c, new RuleStateRepository.Key(tenant, ruleId, key)) > 0;
        });
    }

    public static boolean isSweep(String key) { return TimerRow.SWEEP_KEY.equals(key); }
}
