package trash_maker.infra.api;

import trash_maker.infra.config.NotificationsConfig;
import io.quarkus.security.ForbiddenException;
import io.quarkus.security.UnauthorizedException;
import io.quarkus.security.identity.SecurityIdentity;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;

/** Tenant isolation for every API call: caller needs role admin or tenant:&lt;id&gt; (from the JWT groups claim). */
@RequestScoped
public class TenantGuard {
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
