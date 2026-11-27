package trash_maker.infra;

import io.quarkus.security.PermissionsAllowed;
import io.quarkus.security.identity.SecurityIdentity;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.security.SecurityRequirement;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;
import trash_maker.core.Engine;
import trash_maker.core.Maintenance;
import trash_maker.core.TimerRepository.TimerRow;

import java.util.List;
import java.util.Map;

/** Runbook endpoints (admin role): queue stats, dead timers, retention and partitions. */
@Path("/api/v1/ops")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Tag(name = "ops")
@SecurityRequirement(name = "jwt")
@PermissionsAllowed("admin")
public class OpsResource {
    @Inject Engine engine;
    @Inject SecurityIdentity identity;

    @GET
    @Path("/stats")
    public Maintenance.QueueStats stats() {
        return engine.maintenance.stats();
    }

    @GET
    @Path("/timers/dead")
    public List<TimerRow> dead(@QueryParam("tenant") String tenant, @QueryParam("after") @DefaultValue("0") long after,
                               @QueryParam("limit") @DefaultValue("100") int limit) {
        return engine.db.read(c -> engine.timerRepository.listDead(c, tenant, after, Math.min(limit, 1000)));
    }

    public record Ids(List<Long> ids) {}

    @POST
    @Path("/timers/dead/requeue")
    @Operation(summary = "Move dead timers back to the queue (due now, failures reset)")
    public Map<String, Integer> requeue(Ids ids) {
        return Map.of("requeued", engine.notifications.requeueDeadTimers(ids.ids()));
    }

    @POST
    @Path("/retention/run")
    public Maintenance.RetentionReport retention() {
        return engine.maintenance.retention();
    }

    @POST
    @Path("/partitions/ensure")
    public Map<String, Integer> partitions(@QueryParam("monthsAhead") @DefaultValue("3") int months) {
        return Map.of("created", engine.maintenance.ensurePartitions(months));
    }
}
