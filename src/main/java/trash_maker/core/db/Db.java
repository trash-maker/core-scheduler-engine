package trash_maker.core.db;

import io.quarkus.narayana.jta.QuarkusTransaction;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import jakarta.transaction.Status;
import jakarta.transaction.Synchronization;
import jakarta.transaction.TransactionSynchronizationRegistry;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicLong;

/**
 * JDBC access inside Quarkus-managed (JTA/Narayana) transactions.
 * <ul>
 *   <li>Transaction boundaries are declared with {@code @Transactional} on service entry points, or with
 *       {@link #inNewTx} ({@link QuarkusTransaction}) where one method needs several short transactions.</li>
 *   <li>{@link #with} borrows the connection enlisted in the current transaction (Agroal), or an auto-commit
 *       connection when no transaction is active (reads).</li>
 *   <li>Savepoints use SQL ({@code SAVEPOINT}/{@code ROLLBACK TO SAVEPOINT}): Agroal forbids
 *       {@code Connection.rollback(Savepoint)} on enlisted connections.</li>
 *   <li>After-commit hooks use the JTA {@link TransactionSynchronizationRegistry}.</li>
 * </ul>
 */
@Singleton
public class Db {
    private static final AtomicLong SAVEPOINTS = new AtomicLong();

    private final DataSource ds;
    private final TransactionSynchronizationRegistry tsr;

    @Inject
    public Db(DataSource ds, TransactionSynchronizationRegistry tsr) {
        this.ds = ds;
        this.tsr = tsr;
    }

    @FunctionalInterface
    public interface Work<T> { T apply(Connection c) throws SQLException; }

    @FunctionalInterface
    public interface VoidWork { void apply(Connection c) throws SQLException; }

    /** Runs {@code work} on the connection of the current transaction (or an auto-commit one). */
    public <T> T with(Work<T> work) {
        try (Connection c = ds.getConnection()) {
            return work.apply(c);
        } catch (SQLException e) {
            throw new DbException(e);
        }
    }

    public void run(VoidWork work) { with(c -> { work.apply(c); return null; }); }

    /** Read outside of any transaction requirement (joins the current one if present). */
    public <T> T read(Work<T> work) { return with(work); }

    /** Runs {@code work} in a new transaction, committed when it returns (suspends the caller's transaction). */
    public <T> T inNewTx(Work<T> work) { return QuarkusTransaction.requiringNew().call(() -> with(work)); }

    public void runInNewTx(VoidWork work) { inNewTx(c -> { work.apply(c); return null; }); }

    /** Runs {@code r} after the current transaction commits (dropped on rollback); immediately if none is active. */
    public void afterCommit(Runnable r) {
        if (tsr.getTransactionKey() == null) { r.run(); return; }
        tsr.registerInterposedSynchronization(new Synchronization() {
            @Override public void beforeCompletion() {}
            @Override public void afterCompletion(int status) {
                if (status == Status.STATUS_COMMITTED) {
                    try { r.run(); } catch (RuntimeException ignored) { }
                }
            }
        });
    }

    /** Runs {@code work} under a SQL savepoint; on failure only that work is rolled back and the exception rethrown. */
    public static <T> T savepoint(Connection c, Work<T> work) throws SQLException {
        String name = "sp_" + SAVEPOINTS.incrementAndGet();
        try (Statement st = c.createStatement()) { st.execute("SAVEPOINT " + name); }
        try {
            T r = work.apply(c);
            try (Statement st = c.createStatement()) { st.execute("RELEASE SAVEPOINT " + name); }
            return r;
        } catch (SQLException | RuntimeException e) {
            try (Statement st = c.createStatement()) { st.execute("ROLLBACK TO SAVEPOINT " + name); }
            throw e;
        }
    }

    public static class DbException extends RuntimeException {
        public DbException(SQLException e) { super(e.getMessage(), e); }
    }
}
