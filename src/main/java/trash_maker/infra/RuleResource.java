package trash_maker.infra;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.quarkus.security.PermissionsAllowed;
import io.quarkus.security.identity.SecurityIdentity;
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
import trash_maker.common.Json;
import trash_maker.core.Engine;
import trash_maker.core.RuleStateRepository;
import trash_maker.domain.evaluation.RuleEvaluation;
import trash_maker.domain.rule.Rule.CompiledRule;
import trash_maker.domain.rule.RuleParser;
import trash_maker.domain.signal.Signals.SignalKind;
import trash_maker.domain.signal.Signals.SignalValue;
import trash_maker.domain.signal.Signals.Value;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Path("/api/v1/tenants/{tenant}/rules")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Tag(name = "rules")
@SecurityRequirement(name = "jwt")
@PermissionsAllowed("tenant")
public class RuleResource {
    @Inject Engine engine;
    @Inject SecurityIdentity identity;

    @GET
    @Operation(summary = "List current rule versions (keyset pagination by id)")
    public Dtos.Page<Dtos.RuleDto> list(@PathParam("tenant") String tenant, @QueryParam("after") String after,
                                        @QueryParam("limit") @DefaultValue("50") int limit) {
        int l = Math.max(1, Math.min(limit, 500));
        var rows = engine.rules.list(tenant, after, l);
        return new Dtos.Page<>(rows.stream().map(Dtos.RuleDto::of).toList(), rows.size() == l ? rows.get(rows.size() - 1).ruleId() : null);
    }

    @POST
    @Operation(summary = "Create a rule (version 1). 400 with precise error paths when invalid")
    public Response create(@PathParam("tenant") String tenant, JsonNode def) {
        return Response.status(201).entity(Dtos.RuleDto.of(engine.rules.create(tenant, def, ApiSupport.principal(identity)))).build();
    }

    @POST
    @Path("/validate")
    @Operation(summary = "Validate a rule without saving it")
    public Dtos.ValidationResult validate(@PathParam("tenant") String tenant, JsonNode def) {
        var errors = engine.rules.validate(tenant, def);
        return new Dtos.ValidationResult(errors.isEmpty(), errors);
    }

    @GET
    @Path("/{id}")
    public Dtos.RuleDto get(@PathParam("tenant") String tenant, @PathParam("id") String id, @QueryParam("version") Integer version) {
        return Dtos.RuleDto.of(engine.rules.get(tenant, id, version));
    }

    @PUT
    @Path("/{id}")
    @Operation(summary = "Save a new version (hot reloaded; existing notifications keep their frozen bodies)")
    public Dtos.RuleDto update(@PathParam("tenant") String tenant, @PathParam("id") String id, JsonNode def) {
        return Dtos.RuleDto.of(engine.rules.update(tenant, id, def, ApiSupport.principal(identity)));
    }

    @GET
    @Path("/{id}/versions")
    public List<Dtos.RuleDto> versions(@PathParam("tenant") String tenant, @PathParam("id") String id) {
        return engine.rules.versions(tenant, id).stream().map(Dtos.RuleDto::of).toList();
    }

    @POST
    @Path("/{id}/enable")
    public Dtos.RuleDto enable(@PathParam("tenant") String tenant, @PathParam("id") String id) {
        return Dtos.RuleDto.of(engine.rules.setEnabled(tenant, id, true));
    }

    @POST
    @Path("/{id}/disable")
    public Dtos.RuleDto disable(@PathParam("tenant") String tenant, @PathParam("id") String id) {
        return Dtos.RuleDto.of(engine.rules.setEnabled(tenant, id, false));
    }

    @GET
    @Path("/{id}/state/{key}")
    @Operation(summary = "Inspect rule state (last result, edge_seq, for/within memory) of one entity")
    public Dtos.RuleStateDto state(@PathParam("tenant") String tenant, @PathParam("id") String id, @PathParam("key") String key) {
        return engine.db.read(c -> engine.evaluation.states().find(c, new RuleStateRepository.Key(tenant, id, key)))
                .map(Dtos.RuleStateDto::of).orElseThrow(() -> new trash_maker.core.Engine.CoreExceptions.NotFound("no state for " + key));
    }

    @DELETE
    @Path("/{id}/state/{key}")
    @Operation(summary = "Reset rule state of one entity (the next true evaluation fires again)")
    public Response reset(@PathParam("tenant") String tenant, @PathParam("id") String id, @PathParam("key") String key) {
        return engine.rules.resetState(tenant, id, key) ? Response.noContent().build() : Response.status(404).build();
    }

    @POST
    @Path("/dry-run")
    @Operation(summary = "Dry-run: truth value of every expression node and the notifications (with resolved bodies) that would be created")
    public Dtos.DryRunResponse dryRun(@PathParam("tenant") String tenant, Dtos.DryRunRequest req) {
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
        RuleEvaluation.Result r = engine.evaluation.dryRun(rule, key, signals, req.trigger(), now,
                prior != null && prior.lastResult(), prior == null ? 0 : prior.edgeSeq(), prior == null ? null : prior.lastFiredAt(), Map.of());
        return Dtos.DryRunResponse.of(r);
    }
}
