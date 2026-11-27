package trash_maker.infra;

import io.quarkus.security.PermissionsAllowed;
import io.quarkus.security.identity.SecurityIdentity;
import jakarta.inject.Inject;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.security.SecurityRequirement;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;
import trash_maker.core.Engine.CoreExceptions;
import trash_maker.core.Engine;
import trash_maker.core.NotificationRepository.NotificationRow;
import trash_maker.core.NotificationRepository;
import trash_maker.domain.notification.Lifecycle.NotificationStatus;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Path("/api/v1/tenants/{tenant}")
@Produces(MediaType.APPLICATION_JSON)
@Tag(name = "notifications")
@SecurityRequirement(name = "jwt")
@PermissionsAllowed("tenant")
public class NotificationResource {
    @Inject Engine engine;
    @Inject SecurityIdentity identity;

    @GET
    @Path("/notifications")
    @Operation(summary = "Search notifications (keyset pagination: pass nextCursor as cursor)")
    public Dtos.Page<Dtos.NotificationDto> search(@PathParam("tenant") String tenant, @QueryParam("id") UUID id,
                                                  @QueryParam("status") NotificationStatus status, @QueryParam("action") String action,
                                                  @QueryParam("correlationKey") String key, @QueryParam("ruleId") String ruleId,
                                                  @QueryParam("dueFrom") Instant dueFrom, @QueryParam("dueTo") Instant dueTo,
                                                  @QueryParam("cursor") UUID cursor, @QueryParam("limit") @DefaultValue("50") int limit) {
        int l = Math.max(1, Math.min(limit, 500));
        List<NotificationRow> rows = engine.db.read(c -> engine.notifications.repository().search(c,
                new NotificationRepository.SearchFilter(tenant, id, status, action, key, dueFrom, dueTo, ruleId, cursor, l)));
        return new Dtos.Page<>(rows.stream().map(Dtos.NotificationDto::of).toList(), rows.size() == l ? rows.get(rows.size() - 1).id().toString() : null);
    }

    @GET
    @Path("/notifications/{id}")
    public Dtos.NotificationDto get(@PathParam("tenant") String tenant, @PathParam("id") UUID id) {
        return engine.db.read(c -> engine.notifications.repository().find(c, tenant, id)).map(Dtos.NotificationDto::of)
                .orElseThrow(() -> new CoreExceptions.NotFound("notification " + id));
    }

    @GET
    @Path("/notifications/{id}/history")
    @Operation(summary = "Append-only status history")
    public Dtos.HistoryDto history(@PathParam("tenant") String tenant, @PathParam("id") UUID id) {
        return new Dtos.HistoryDto(engine.db.read(c -> engine.notifications.repository().historyOf(c, tenant, id)));
    }

    @POST
    @Path("/notifications/{id}/cancel")
    @Operation(summary = "Cancel a non-terminal notification (409 if terminal); closes its WHILE_IN_STATE series")
    public Dtos.NotificationDto cancel(@PathParam("tenant") String tenant, @PathParam("id") UUID id) {
        return engine.notifications.cancel(tenant, id, ApiSupport.principal(identity))
                .map(Dtos.NotificationDto::of).orElseThrow(() -> new CoreExceptions.NotFound("notification " + id));
    }

    @POST
    @Path("/notifications/{id}/retrigger")
    @Operation(summary = "Re-publish now with a fresh retry budget (from RETRIES_EXHAUSTED, CANCELLED or EXPIRED)")
    public Dtos.NotificationDto retrigger(@PathParam("tenant") String tenant, @PathParam("id") UUID id) {
        return engine.notifications.retrigger(tenant, id, ApiSupport.principal(identity))
                .map(Dtos.NotificationDto::of).orElseThrow(() -> new CoreExceptions.NotFound("notification " + id));
    }

    @GET
    @Path("/series/{id}")
    public Dtos.SeriesDto series(@PathParam("tenant") String tenant, @PathParam("id") UUID id) {
        return engine.db.read(c -> engine.notifications.seriesRepository().find(c, tenant, id)).map(Dtos.SeriesDto::of)
                .orElseThrow(() -> new CoreExceptions.NotFound("series " + id));
    }

    @POST
    @Path("/series/{id}/cancel")
    public Dtos.SeriesDto cancelSeries(@PathParam("tenant") String tenant, @PathParam("id") UUID id) {
        return engine.notifications.cancelSeries(tenant, id).map(Dtos.SeriesDto::of)
                .orElseThrow(() -> new CoreExceptions.NotFound("series " + id));
    }

    @GET
    @Path("/entities/{key}/signals")
    @Operation(summary = "Inspect stored signal values of one entity")
    public Object signals(@PathParam("tenant") String tenant, @PathParam("key") String key) {
        return engine.db.read(c -> engine.signalStore.raw(c, tenant, key));
    }

    @GET
    @Path("/entities/{key}/rules")
    @Operation(summary = "Inspect rule states of one entity")
    public List<Dtos.RuleStateDto> ruleStates(@PathParam("tenant") String tenant, @PathParam("key") String key) {
        return engine.db.read(c -> engine.evaluation.states().forEntity(c, tenant, key)).stream().map(Dtos.RuleStateDto::of).toList();
    }
}
