package io.notifengine.support;

import org.flywaydb.core.Flyway;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.postgresql.PostgreSQLContainer;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Statement;

/** One PostgreSQL container per JVM, migrated with the production Flyway scripts. */
public final class Pg {
    private static PostgreSQLContainer container;
    private static DataSource ds;

    private Pg() {}

    public static synchronized DataSource dataSource() {
        if (ds == null) {
            container = new PostgreSQLContainer("postgres:16-alpine").withCommand("postgres", "-c", "max_connections=200", "-c", "fsync=off");
            container.start();
            PGSimpleDataSource d = new PGSimpleDataSource();
            d.setUrl(container.getJdbcUrl() + "&reWriteBatchedInserts=true");
            d.setUser(container.getUsername());
            d.setPassword(container.getPassword());
            Flyway.configure().dataSource(d).locations("classpath:db/migration").load().migrate();
            ds = d;
        }
        return ds;
    }

    public static String jdbcUrl() { dataSource(); return container.getJdbcUrl(); }

    public static String user() { return container.getUsername(); }

    public static String password() { return container.getPassword(); }

    public static void truncateAll() {
        try (Connection c = dataSource().getConnection(); Statement st = c.createStatement()) {
            st.execute("""
                    TRUNCATE signal_inbox, signal_state, rule, rule_state, notification, notification_history, notification_dedup,
                      recurring_series, timer, timer_dead, outbox, dead_letter""");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
