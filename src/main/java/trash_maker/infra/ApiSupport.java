package trash_maker.infra;

import trash_maker.core.NotificationsConfig;
import io.quarkus.security.PermissionChecker;
import io.quarkus.security.identity.AuthenticationRequestContext;
import io.quarkus.security.identity.SecurityIdentityAugmentor;
import io.quarkus.security.runtime.QuarkusPrincipal;
import io.quarkus.security.runtime.QuarkusSecurityIdentity;
import io.smallrye.mutiny.Uni;
import io.quarkus.security.identity.SecurityIdentity;
import jakarta.enterprise.context.ApplicationScoped;
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

import java.util.List;

/** API cross-cutting concerns: access rules, role filter and error mapping. */
public final class ApiSupport {
    private ApiSupport() {}

    /**
     * Declarative access rules used with {@code @PermissionsAllowed}: "tenant" needs role admin or tenant:&lt;id&gt;
     * (JWT groups claim) for the endpoint's {@code tenant} path parameter; "admin" needs the admin role.
     * With notifications.api.auth-enabled=false (local stack only) everything is allowed.
     */
    @ApplicationScoped
    public static class Permissions {
        @Inject NotificationsConfig cfg;

        @PermissionChecker("tenant")
        boolean tenant(SecurityIdentity identity, String tenant) {
            if (!cfg.api().authEnabled()) return true;
            return !identity.isAnonymous()
                    && (identity.hasRole(cfg.api().adminRole()) || identity.hasRole(cfg.api().tenantRolePrefix() + tenant));
        }

        @PermissionChecker("admin")
        boolean admin(SecurityIdentity identity) {
            return !cfg.api().authEnabled() || (!identity.isAnonymous() && identity.hasRole(cfg.api().adminRole()));
        }
    }

    /**
     * Local stack only (notifications.api.auth-enabled=false): the anonymous caller becomes the local administrator,
     * so the same declarative permissions apply without an OIDC provider.
     */
    @ApplicationScoped
    public static class LocalIdentity implements SecurityIdentityAugmentor {
        @Inject NotificationsConfig cfg;

        @Override
        public Uni<SecurityIdentity> augment(SecurityIdentity identity, AuthenticationRequestContext context) {
            if (cfg.api().authEnabled() || !identity.isAnonymous()) return Uni.createFrom().item(identity);
            return Uni.createFrom().item(QuarkusSecurityIdentity.builder(identity).setAnonymous(false)
                    .setPrincipal(new QuarkusPrincipal("local")).addRole(cfg.api().adminRole()).build());
        }
    }

    /** Name recorded as author of rule versions and operator actions. */
    public static String principal(SecurityIdentity identity) {
        return identity == null || identity.isAnonymous() ? "anonymous" : identity.getPrincipal().getName();
    }

    /** Instances without the 'api' role do not serve the REST API (health and metrics stay available under /q). */
    public static class RoleFilter {
        private volatile Boolean apiEnabled;

        @ServerRequestFilter(preMatching = true)
        public Response filter(ContainerRequestContext ctx) {
            if (apiEnabled == null) {
                apiEnabled = ConfigProvider.getConfig().getOptionalValue("notifications.role.api", Boolean.class).orElse(false);
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
