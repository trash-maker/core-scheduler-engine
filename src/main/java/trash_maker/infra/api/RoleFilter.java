package trash_maker.infra.api;

import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.Response;
import org.eclipse.microprofile.config.ConfigProvider;
import org.jboss.resteasy.reactive.server.ServerRequestFilter;

import java.util.Arrays;

/** Instances without the 'api' role do not serve the REST API (health and metrics stay available under /q). */
public class RoleFilter {
    private volatile Boolean apiEnabled;

    @ServerRequestFilter(preMatching = true)
    public Response filter(ContainerRequestContext ctx) {
        if (apiEnabled == null) {
            String roles = ConfigProvider.getConfig().getOptionalValue("notifications.roles", String.class).orElse("api");
            apiEnabled = Arrays.stream(roles.split(",")).map(String::trim).anyMatch("api"::equals);
        }
        if (!apiEnabled && ctx.getUriInfo().getPath().startsWith("/api/")) return Response.status(404).build();
        return null;
    }
}
