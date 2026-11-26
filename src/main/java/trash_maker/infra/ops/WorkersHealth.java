package trash_maker.infra.ops;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.health.HealthCheck;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.eclipse.microprofile.health.Liveness;

/** Liveness: timer workers and relays are running (DB and Kafka readiness come from the extensions). */
@Liveness
@ApplicationScoped
public class WorkersHealth implements HealthCheck {
    @Inject Workers workers;

    @Override
    public HealthCheckResponse call() {
        return HealthCheckResponse.named("engine-workers").status(workers.alive()).build();
    }
}
