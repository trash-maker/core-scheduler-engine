package trash_maker.infra;

import trash_maker.infra.config.RolesConfigSourceFactory;
import io.smallrye.config.ConfigSourceContext;
import io.smallrye.config.ConfigValue;
import org.eclipse.microprofile.config.spi.ConfigSource;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class RolesConfigSourceFactoryTest {
    static ConfigSource source(String roles) {
        ConfigSourceContext ctx = new ConfigSourceContext() {
            public ConfigValue getValue(String name) {
                return name.equals("notifications.roles") && roles != null ? ConfigValue.builder().withName(name).withValue(roles).build() : null;
            }
            public List<String> getProfiles() { return List.of(); }
            public Iterator<String> iterateNames() { return List.<String>of().iterator(); }
        };
        return new RolesConfigSourceFactory().getConfigSources(ctx).iterator().next();
    }

    @Test
    void channelsFollowRoles() {
        ConfigSource s = source("fake-actuator");
        assertThat(s.getValue("mp.messaging.incoming.signals-in.enabled")).isEqualTo("false");
        assertThat(s.getValue("mp.messaging.incoming.results-in.enabled")).isEqualTo("false");
        assertThat(s.getValue("mp.messaging.incoming.fake-actuator-in.enabled")).isEqualTo("true");
        ConfigSource timersOnly = source("timers, relay");
        assertThat(timersOnly.getValue("mp.messaging.incoming.signals-in.enabled")).isEqualTo("false");
        ConfigSource dflt = source(null);
        assertThat(dflt.getValue("mp.messaging.incoming.signals-in.enabled")).isEqualTo("true");
        assertThat(dflt.getValue("mp.messaging.incoming.fake-actuator-in.enabled")).isEqualTo("false");
        assertThat(s.getOrdinal()).isBetween(251, 299); // above application.properties, below env vars
    }
}
