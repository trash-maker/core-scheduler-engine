package trash_maker.it;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import org.eclipse.microprofile.config.ConfigProvider;
import org.junit.jupiter.api.Test;
import trash_maker.support.CoreProfile;

import static org.assertj.core.api.Assertions.assertThat;

/** Kafka channels follow the role switches through property expressions (roles are set by profile). */
@QuarkusTest
@TestProfile(CoreProfile.class)
class RoleProfileTest {
    static String value(String key) { return ConfigProvider.getConfig().getValue(key, String.class); }

    @Test
    void channelsFollowRoles() {
        assertThat(value("notifications.role.api")).isEqualTo("true");
        assertThat(value("mp.messaging.incoming.signals-in.enabled")).isEqualTo("false");
        assertThat(value("mp.messaging.incoming.results-in.enabled")).isEqualTo("false");
        assertThat(value("mp.messaging.outgoing.relay-out.enabled")).isEqualTo("false");
        assertThat(value("mp.messaging.incoming.fake-actuator-in.enabled")).isEqualTo("false");
    }
}
