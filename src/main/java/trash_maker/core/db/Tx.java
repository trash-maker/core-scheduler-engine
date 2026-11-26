package trash_maker.core.db;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.ArrayList;
import java.util.List;

/**
 * Minimal transaction helper over plain JDBC: short transactions, retry on serialization failure / deadlock,
 * after-commit hooks (used to wake local timer workers), savepoints for per-item isolation.
 */
public final class Tx {
    private static final ThreadLocal<List<Runnable>> AFTER_COMMIT = new ThreadLocal<>();
    private static final int MAX_RETRIES = 5;

    private final DataSource ds;

    public Tx(DataSource ds) { this.ds = ds; }

    @FunctionalInterface
    public interface Work<T> { T apply(Connection c) throws SQLException; }

    @FunctionalInterface
    public interface VoidWork { void apply(Connection c) throws SQLException; }

    public DataSource dataSource() { return ds; }

    public <T> T inTx(Work<T> work) {
        for (int attempt = 1; ; attempt++) {
            List<Runnable> hooks = new ArrayList<>();
            List<Runnable> outer = AFTER_COMMIT.get();
            AFTER_COMMIT.set(hooks);
            try (Connection c = ds.getConnection()) {
                c.setAutoCommit(false);
                try {
                    T r = work.apply(c);
                    c.commit();
                    AFTER_COMMIT.set(outer);
                    for (Runnable h : hooks) {
                        try { h.run(); } catch (RuntimeException ignored) { }
                    }
                    return r;
                } catch (Throwable e) {
                    try { c.rollback(); } catch (SQLException ignored) { }
                    throw e;
                } finally {
                    c.setAutoCommit(true);
                }
            } catch (SQLException e) {
                AFTER_COMMIT.set(outer);
                if (retryable(e) && attempt < MAX_RETRIES) {
                    sleep(attempt);
                    continue;
                }
                throw new DbException(e);
            } catch (RuntimeException | Error e) {
                AFTER_COMMIT.set(outer);
                Throwable cause = e.getCause();
                if (cause instanceof SQLException se && retryable(se) && attempt < MAX_RETRIES) {
                    sleep(attempt);
                    continue;
                }
                throw e;
            }
        }
    }

    public void run(VoidWork work) { inTx(c -> { work.apply(c); return null; }); }

    /** Read-only, auto-commit. */
    public <T> T read(Work<T> work) {
        try (Connection c = ds.getConnection()) {
            c.setAutoCommit(true);
            c.setReadOnly(true);
            try { return work.apply(c); } finally { c.setReadOnly(false); }
        } catch (SQLException e) {
            throw new DbException(e);
        }
    }

    /** Registers a hook run after the current transaction commits (dropped on rollback). */
    public static void afterCommit(Runnable r) {
        List<Runnable> hooks = AFTER_COMMIT.get();
        if (hooks != null) hooks.add(r); else r.run();
    }

    /** Runs {@code work} under a savepoint; on failure rolls back only that work and rethrows. */
    public static <T> T savepoint(Connection c, Work<T> work) throws SQLException {
        Savepoint sp = c.setSavepoint();
        try {
            T r = work.apply(c);
            c.releaseSavepoint(sp);
            return r;
        } catch (SQLException | RuntimeException e) {
            c.rollback(sp);
            throw e;
        }
    }

    static boolean retryable(SQLException e) {
        String s = e.getSQLState();
        return "40001".equals(s) || "40P01".equals(s);
    }

    private static void sleep(int attempt) {
        try { Thread.sleep(10L * attempt * attempt + (long) (Math.random() * 20)); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
    }

    public static class DbException extends RuntimeException {
        public DbException(SQLException e) { super(e.getMessage(), e); }
    }
}
