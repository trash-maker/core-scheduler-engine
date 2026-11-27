package trash_maker.infra;

import io.quarkus.security.ForbiddenException;
import io.quarkus.security.UnauthorizedException;
import io.quarkus.security.identity.SecurityIdentity;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.Response;
import org.eclipse.microprofile.config.ConfigProvider;
import org.jboss.resteasy.reactive.server.ServerExceptionMapper;
import org.jboss.resteasy.reactive.server.ServerRequestFilter;
import trash_maker.common.Validation.ValidationError;
import trash_maker.common.Validation.ValidationException;
import trash_maker.core.Engine.CoreExceptions;
import trash_maker.domain.notification.Lifecycle.IllegalTransitionException;

import java.util.Arrays;
import java.util.List;

/** API cross-cutting concerns: tenant guard, role filter and error mapping. */
public final class ApiSupport {
    private ApiSupport() {}

    /** Tenant isolation for every API call: caller needs role admin or tenant:&lt;id&gt; (from the JWT groups claim). */
    @RequestScoped
    public static class TenantGuard {
        @Inject SecurityIdentity identity;
        @Inject NotificationsConfig cfg;

        public void check(String tenant) {
            if (!cfg.api().authEnabled()) return;
            if (identity == null || identity.isAnonymous()) throw new UnauthorizedException();
            if (identity.hasRole(cfg.api().adminRole()) || identity.hasRole(cfg.api().tenantRolePrefix() + tenant)) return;
            throw new ForbiddenException("no access to tenant " + tenant);
        }

        public void admin() {
            if (!cfg.api().authEnabled()) return;
            if (identity == null || identity.isAnonymous()) throw new UnauthorizedException();
            if (!identity.hasRole(cfg.api().adminRole())) throw new ForbiddenException("admin role required");
        }

        public String principal() { return identity == null || identity.isAnonymous() ? "anonymous" : identity.getPrincipal().getName(); }
    }

    /** Instances without the 'api' role do not serve the REST API (health and metrics stay available under /q). */
    public static class RoleFilter {
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

    /** Maps application errors to RFC 7807-like JSON bodies with precise validation paths. */
    public static class ApiErrors {
        public record Problem(int status, String title, String detail, List<ValidationError> errors) {}

        @ServerExceptionMapper
        public Response validation(ValidationException e) {
            return Response.status(400).entity(new Problem(400, "validation failed", null, e.errors())).build();
        }

        @ServerExceptionMapper
        public Response notFound(CoreExceptions.NotFound e) { return Response.status(404).entity(new Problem(404, "not found", e.getMessage(), List.of())).build(); }

        @ServerExceptionMapper
        public Response conflict(CoreExceptions.Conflict e) { return Response.status(409).entity(new Problem(409, "conflict", e.getMessage(), List.of())).build(); }

        @ServerExceptionMapper
        public Response illegal(IllegalTransitionException e) {
            return Response.status(409).entity(new Problem(409, "illegal transition", e.getMessage(), List.of())).build();
        }

        @ServerExceptionMapper
        public Response badRequest(IllegalArgumentException e) {
            return Response.status(400).entity(new Problem(400, "bad request", e.getMessage(), List.of())).build();
        }
    }
}
