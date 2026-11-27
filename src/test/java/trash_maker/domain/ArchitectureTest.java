package trash_maker.domain;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Layering rules:
 * <ul>
 *   <li>domain + common: pure business logic - no framework, no I/O, no clock/random access, no mutable static state;</li>
 *   <li>core: Quarkus, CDI, JTA, Micrometer and OpenTelemetry allowed; no Kafka, JPA or messaging - the broker stays
 *       behind the MessagePublisher port implemented in infra.</li>
 * </ul>
 */
class ArchitectureTest {
    static final Pattern ANY_FRAMEWORK = Pattern.compile("^import\\s+(static\\s+)?(io\\.quarkus|jakarta\\.|javax\\.persistence|org\\.hibernate|org\\.apache\\.kafka|io\\.smallrye|io\\.micrometer|io\\.opentelemetry|org\\.eclipse\\.microprofile|io\\.agroal)");
    static final Pattern IO = Pattern.compile("^import\\s+(java\\.sql|javax\\.sql|java\\.net|java\\.io\\.File|java\\.nio\\.file|org\\.jboss\\.logging|trash_maker\\.(core|infra))");
    static final Pattern IMPURE_CALLS = Pattern.compile("(Instant\\.now\\(|System\\.currentTimeMillis|Clock\\.system|ThreadLocalRandom|Math\\.random|new Random\\(|SecureRandom|LocalDateTime\\.now\\(|ZonedDateTime\\.now\\()");
    static final Pattern MUTABLE_STATIC = Pattern.compile("^\\s*(private|public|protected)?\\s*static\\s+(?!final)[\\w<>\\[\\], ?]+\\s+\\w+\\s*(=|;)");
    static final Pattern CORE_FORBIDDEN = Pattern.compile("^import\\s+(static\\s+)?(javax\\.persistence|jakarta\\.persistence|org\\.hibernate|org\\.apache\\.kafka|io\\.smallrye\\.reactive|trash_maker\\.infra)");

    static List<String> violations(String dir, Pattern p) throws IOException {
        List<String> out = new ArrayList<>();
        try (Stream<Path> files = Files.walk(Path.of("src/main/java/trash_maker", dir))) {
            for (Path f : files.filter(x -> x.toString().endsWith(".java")).toList()) {
                for (String line : Files.readAllLines(f)) if (p.matcher(line.trim().startsWith("import") ? line.trim() : line).find()) out.add(f + ": " + line.trim());
            }
        }
        return out;
    }

    @Test
    void domainIsPure() throws IOException {
        for (String dir : List.of("domain", "common")) {
            assertThat(violations(dir, ANY_FRAMEWORK)).as("framework imports in " + dir).isEmpty();
            assertThat(violations(dir, IO)).as("I/O imports in " + dir).isEmpty();
            assertThat(violations(dir, IMPURE_CALLS)).as("clock/random access in " + dir).isEmpty();
            assertThat(violations(dir, MUTABLE_STATIC)).as("mutable static state in " + dir).isEmpty();
        }
    }

    @Test
    void coreUsesOnlyQuarkusAndPorts() throws IOException {
        assertThat(violations("core", CORE_FORBIDDEN)).isEmpty();
    }
}
