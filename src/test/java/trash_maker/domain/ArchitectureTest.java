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

/** Domain code is free of Quarkus/Kafka/JPA (and of I/O); core is free of Quarkus/Kafka/JPA. */
class ArchitectureTest {
    static final Pattern FRAMEWORK = Pattern.compile("^import\\s+(static\\s+)?(io\\.quarkus|jakarta\\.|javax\\.persistence|org\\.hibernate|org\\.apache\\.kafka|io\\.smallrye|io\\.micrometer|io\\.opentelemetry|org\\.eclipse\\.microprofile)");
    static final Pattern IO = Pattern.compile("^import\\s+(java\\.sql|javax\\.sql|org\\.jboss\\.logging|trash_maker\\.(core|infra))");

    static List<String> violations(String dir, Pattern p) throws IOException {
        List<String> out = new ArrayList<>();
        try (Stream<Path> files = Files.walk(Path.of("src/main/java/trash_maker", dir))) {
            for (Path f : files.filter(x -> x.toString().endsWith(".java")).toList()) {
                for (String line : Files.readAllLines(f)) if (p.matcher(line.trim()).find()) out.add(f + ": " + line.trim());
            }
        }
        return out;
    }

    @Test
    void domainIsPure() throws IOException {
        assertThat(violations("domain", FRAMEWORK)).isEmpty();
        assertThat(violations("domain", IO)).isEmpty();
        assertThat(violations("common", FRAMEWORK)).isEmpty();
    }

    @Test
    void coreIsFrameworkFree() throws IOException {
        assertThat(violations("core", FRAMEWORK)).isEmpty();
    }
}
