package trash_maker.infra.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import trash_maker.common.Json;
import trash_maker.core.Engine;
import trash_maker.core.evaluation.RuleStateRepository;
import trash_maker.domain.evaluation.FiringPlanner;
import trash_maker.domain.evaluation.RuleEvaluation;
import trash_maker.domain.rule.CompiledRule;
import trash_maker.domain.rule.RuleParser;
import trash_maker.domain.signal.SignalKind;
import trash_maker.domain.signal.SignalValue;
import trash_maker.domain.signal.Value;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.security.SecurityRequirement;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Path("/api/v1/tenants/{tenant}/rules")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Tag(name = "rules")
@SecurityRequirement(name = "jwt")
public class RuleResource {
    @Inject Engine engine;
    @Inject TenantGuard guard;

    @GET
    @Operation(summary = "List current rule versions (keyset pagination by id)")
    public Dtos.Page<Dtos.RuleDto> list(@PathParam("tenant") String tenant, @QueryParam("after") String after,
                                        @QueryParam("limit") @DefaultValue("50") int limit) {
        guard.check(tenant);
        int l = Math.max(1, Math.min(limit, 500));
        var rows = engine.rules.list(tenant, after, l);
        return new Dtos.Page<>(rows.stream().map(Dtos.RuleDto::of).toList(), rows.size() == l ? rows.get(rows.size() - 1).ruleId() : null);
    }

    @POST
    @Operation(summary = "Create a rule (version 1). 400 with precise error paths when invalid")
    public Response create(@PathParam("tenant") String tenant, JsonNode def) {
        guard.check(tenant);
        return Response.status(201).entity(Dtos.RuleDto.of(engine.rules.create(tenant, def, guard.principal()))).build();
    }

    @POST
    @Path("/validate")
    @Operation(summary = "Validate a rule without saving it")
    public Dtos.ValidationResult validate(@PathParam("tenant") String tenant, JsonNode def) {
        guard.check(tenant);
        var errors = engine.rules.validate(tenant, def);
        return new Dtos.ValidationResult(errors.isEmpty(), errors);
    }

    @GET
    @Path("/{id}")
    public Dtos.RuleDto get(@PathParam("tenant") String tenant, @PathParam("id") String id, @QueryParam("version") Integer version) {
        guard.check(tenant);
        return Dtos.RuleDto.of(engine.rules.get(tenant, id, version));
    }

    @PUT
    @Path("/{id}")
    @Operation(summary = "Save a new version (hot reloaded; existing notifications keep their frozen bodies)")
    public Dtos.RuleDto update(@PathParam("tenant") String tenant, @PathParam("id") String id, JsonNode def) {
        guard.check(tenant);
        return Dtos.RuleDto.of(engine.rules.update(tenant, id, def, guard.principal()));
    }

    @GET
    @Path("/{id}/versions")
    public List<Dtos.RuleDto> versions(@PathParam("tenant") String tenant, @PathParam("id") String id) {
        guard.check(tenant);
        return engine.rules.versions(tenant, id).stream().map(Dtos.RuleDto::of).toList();
    }

    @POST
    @Path("/{id}/enable")
    public Dtos.RuleDto enable(@PathParam("tenant") String tenant, @PathParam("id") String id) {
        guard.check(tenant);
        return Dtos.RuleDto.of(engine.rules.setEnabled(tenant, id, true));
    }

    @POST
    @Path("/{id}/disable")
    public Dtos.RuleDto disable(@PathParam("tenant") String tenant, @PathParam("id") String id) {
        guard.check(tenant);
        return Dtos.RuleDto.of(engine.rules.setEnabled(tenant, id, false));
    }

    @GET
    @Path("/{id}/state/{key}")
    @Operation(summary = "Inspect rule state (last result, edge_seq, for/within memory) of one entity")
    public Dtos.RuleStateDto state(@PathParam("tenant") String tenant, @PathParam("id") String id, @PathParam("key") String key) {
        guard.check(tenant);
        return engine.db.read(c -> engine.evaluation.states().find(c, new RuleStateRepository.Key(tenant, id, key)))
                .map(Dtos.RuleStateDto::of).orElseThrow(() -> new trash_maker.core.CoreExceptions.NotFound("no state for " + key));
    }

    @DELETE
    @Path("/{id}/state/{key}")
    @Operation(summary = "Reset rule state of one entity (the next true evaluation fires again)")
    public Response reset(@PathParam("tenant") String tenant, @PathParam("id") String id, @PathParam("key") String key) {
        guard.check(tenant);
        return engine.rules.resetState(tenant, id, key) ? Response.noContent().build() : Response.status(404).build();
    }

    @POST
    @Path("/dry-run")
    @Operation(summary = "Dry-run: truth value of every expression node and the notifications (with resolved bodies) that would be created")
    public JsonNode dryRun(@PathParam("tenant") String tenant, Dtos.DryRunRequest req) {
        guard.check(tenant);
        CompiledRule rule;
        if (req.rule() != null) {
            ObjectNode def = ((ObjectNode) req.rule()).deepCopy();
            def.put("tenantId", tenant);
            if (!def.has("version")) def.put("version", 1);
            rule = RuleParser.compile(def, engine.rules.options());
        } else if (req.ruleId() != null) {
            rule = engine.rules.compiledCurrent(tenant, req.ruleId());
        } else throw new IllegalArgumentException("either 'rule' or 'ruleId' is required");
        Instant now = req.now() == null ? engine.clock.instant() : req.now();
        Map<String, SignalValue> signals = new HashMap<>();
        if (req.signals() != null) req.signals().fields().forEachRemaining(e -> {
            JsonNode v = e.getValue();
            SignalKind kind = SignalKind.valueOf(v.path("kind").asText("KPI"));
            Instant at = v.hasNonNull("occurredAt") ? Instant.parse(v.get("occurredAt").asText()) : now;
            signals.put(e.getKey(), new SignalValue(e.getKey(), kind, Value.fromJson(kind, v.get("value")), at));
        });
        String key = req.correlationKey() == null ? "dry-run" : req.correlationKey();
        var prior = req.priorState();
        RuleEvaluation.Result r = engine.dryRun.run(rule, key, signals, req.trigger(), now,
                prior != null && prior.lastResult(), prior == null ? 0 : prior.edgeSeq(), prior == null ? null : prior.lastFiredAt(), Map.of());
        ObjectNode out = Json.object();
        out.put("result", r.evaluation().value());
        out.put("truth", r.evaluation().truth().name());
        ObjectNode nodes = out.putObject("nodes");
        r.evaluation().nodes().forEach((path, nr) -> {
            ObjectNode n = nodes.putObject(path);
            n.put("value", nr.value().name());
            if (nr.detail() != null) n.put("detail", nr.detail());
        });
        if (r.evaluation().nextEntityChange() != null) out.put("nextEntityChange", r.evaluation().nextEntityChange().toString());
        if (r.evaluation().nextRuleChange() != null) out.put("nextRuleChange", r.evaluation().nextRuleChange().toString());
        ObjectNode d = out.putObject("decision");
        d.put("fire", r.decision().fire());
        d.put("reason", r.decision().reason());
        d.put("edgeSeq", r.decision().newEdgeSeq());
        if (r.planningError() != null) {
            ObjectNode pe = out.putObject("planningError");
            pe.put("templateId", r.planningError().templateId());
            pe.put("code", r.planningError().code());
            pe.put("message", r.planningError().getMessage());
        }
        ArrayNode ns = out.putArray("notifications");
        ArrayNode ss = out.putArray("series");
        if (r.plan() != null) {
            for (FiringPlanner.PlannedNotification p : r.plan().notifications()) {
                ObjectNode n = ns.addObject();
                n.put("templateId", p.templateId());
                n.put("type", p.type().name());
                n.put("action", p.action());
                n.put("initialStatus", p.initialStatus().name());
                if (p.parentTemplateId() != null) { n.put("parentTemplateId", p.parentTemplateId()); n.put("parentTrigger", p.parentTrigger().name()); }
                if (p.dueAt() != null) n.put("dueAt", p.dueAt().toString());
                if (p.expiresAt() != null) n.put("expiresAt", p.expiresAt().toString());
                n.put("idempotencyKey", p.idempotencyKey());
                n.put("bodyDeferredUntilActivation", p.bodyDeferred());
                if (p.body() != null) n.set("body", p.body());
                n.set("retryPolicy", p.retryPolicy().toJson());
            }
            for (FiringPlanner.PlannedSeries s : r.plan().series()) {
                ObjectNode n = ss.addObject();
                n.put("templateId", s.templateId());
                n.put("action", s.action());
                n.put("waitingOnParent", s.waiting());
                if (s.firstDueAt() != null) n.put("firstDueAt", s.firstDueAt().toString());
                if (s.untilAt() != null) n.put("untilAt", s.untilAt().toString());
                if (s.maxOccurrences() != null) n.put("maxOccurrences", s.maxOccurrences());
            }
        }
        return out;
    }
}
