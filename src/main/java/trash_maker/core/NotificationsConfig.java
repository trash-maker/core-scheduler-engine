package trash_maker.core;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import io.smallrye.config.WithName;

import java.time.Duration;
import java.util.Optional;
import java.util.Set;

/** Service configuration (prefix notifications.*). See README "Service configuration reference". */
@ConfigMapping(prefix = "notifications")
public interface NotificationsConfig {

    /**
     * Roles of this instance. Each role is a Quarkus profile (QUARKUS_PROFILE=timers,relay); the default prod, dev and
     * test profiles enable every role except fake-actuator. See application.properties.
     */
    Role role();

    interface Role {
        @WithDefault("false") boolean ingest();
        @WithDefault("false") boolean timers();
        @WithDefault("false") boolean relay();
        @WithDefault("false") boolean results();
        @WithDefault("false") boolean api();
        @WithName("fake-actuator") @WithDefault("false") boolean fakeActuator();
    }

    Topics topics();

    Signals signals();

    Actions actions();

    Timers timers();

    Relay relay();

    Retention retention();

    Api api();

    Rules rules();

    Consumers ingest();

    Consumers results();


    @WithName("fake-actuator")
    FakeActuator fakeActuator();

    interface Rules {
        @WithName("refresh-interval") @WithDefault("2s") String refreshInterval();
    }

    interface Consumers {
        /** Kafka consumer threads (partitions) for the channel. */
        @WithDefault("2") int consumers();
    }


    interface FakeActuator {
        @WithDefault("*") Set<String> actions();
        @WithName("failure-rate") @WithDefault("0.1") double failureRate();
        @WithName("permanent-failure-rate") @WithDefault("0.02") double permanentFailureRate();
        @WithName("silent-rate") @WithDefault("0.02") double silentRate();
    }

    interface Topics {
        @WithDefault("signals.in") String signals();
        @WithDefault("notification.results") String results();
        @WithDefault("notifications.out") String out();
        @WithDefault("notifications.dlq") String dlq();
    }

    interface Signals {
        /** Store signals no rule references (default: discard + metric). */
        @WithName("store-unreferenced-signals") @WithDefault("false") boolean storeUnreferenced();
        /** Optional global max age: older signals count as MISSING. */
        @WithName("default-max-age") Optional<Duration> defaultMaxAge();
    }

    interface Actions {
        /** Optional allow-list of actions accepted in rules (empty = any). */
        @WithName("allow-list") Optional<Set<String>> allowList();
    }

    interface Timers {
        @WithDefault("2") int workers();
        @WithName("batch-size") @WithDefault("200") int batchSize();
        @WithName("max-failures") @WithDefault("5") int maxFailures();
        @WithName("failure-backoff") @WithDefault("PT1S") Duration failureBackoff();
        @WithName("idle-min") @WithDefault("PT0.1S") Duration idleMin();
        @WithName("idle-max") @WithDefault("PT0.5S") Duration idleMax();
        @WithName("sweep-page-size") @WithDefault("500") int sweepPageSize();
    }

    interface Relay {
        @WithDefault("1") int workers();
        @WithName("batch-size") @WithDefault("500") int batchSize();
        @WithName("idle") @WithDefault("PT0.05S") Duration idle();
    }

    interface Retention {
        @WithName("terminal-notifications") @WithDefault("P30D") Duration terminal();
        @WithName("dedup") @WithDefault("P7D") Duration dedup();
        @WithName("inbox") @WithDefault("P7D") Duration inbox();
        @WithName("partitions-ahead") @WithDefault("3") int partitionsAhead();
        @WithDefault("1h") String interval();
    }

    interface Api {
        /** When false every caller may access every tenant (local development only). */
        @WithName("auth-enabled") @WithDefault("true") boolean authEnabled();
        @WithName("admin-role") @WithDefault("admin") String adminRole();
        /** Role granting access to tenant X is &lt;prefix&gt;X, e.g. tenant:acme. */
        @WithName("tenant-role-prefix") @WithDefault("tenant:") String tenantRolePrefix();
    }

}
