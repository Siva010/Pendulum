package io.pendulum.core.support;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.pendulum.core.Pendulum;
import io.pendulum.core.domain.Job;
import io.pendulum.core.domain.NewJob;
import io.pendulum.core.store.JobStore;
import io.pendulum.core.store.PostgresJobStore;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;

/**
 * A real Postgres for every test, via one container shared across the whole suite.
 *
 * <p>H2 is not an option here and that is not a preference. H2 does not implement
 * {@code SKIP LOCKED}, its advisory locks are not Postgres advisory locks, and its MVCC
 * behaviour differs precisely where this engine's correctness lives. A test suite that passes
 * on H2 and fails on Postgres tests nothing worth testing.
 *
 * <p>The container is started once in a static initialiser rather than per class with
 * {@code @Testcontainers}, because a fresh Postgres per test class costs several seconds each and
 * the suite is already dominated by deliberate waiting on lease expiry.
 */
public abstract class PostgresTestBase {

    protected static final HikariDataSource DATA_SOURCE;

    /**
     * Point the suite at an existing Postgres instead of starting a container.
     *
     * <p><strong>Every test truncates {@code jobs}.</strong> Only ever set this to a throwaway
     * database. The environment variable is the opt-in, and the database name is logged on startup
     * so a mistake is loud rather than silent.
     */
    private static final String EXTERNAL_URL_ENV = "PENDULUM_TEST_JDBC_URL";

    static {
        String externalUrl = System.getenv(EXTERNAL_URL_ENV);

        HikariConfig config = new HikariConfig();
        if (externalUrl != null && !externalUrl.isBlank()) {
            // Useful where Docker is unavailable, and useful on purpose: running the same suite
            // against a real managed Postgres is how you find out that your RDS parameter group
            // disagrees with your laptop.
            config.setJdbcUrl(externalUrl);
            config.setUsername(System.getenv("PENDULUM_TEST_DB_USER"));
            config.setPassword(System.getenv("PENDULUM_TEST_DB_PASSWORD"));
            System.out.println("[pendulum-test] using external database " + redact(externalUrl)
                    + " — every test TRUNCATEs the jobs table in it");
        } else {
            PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
                    DockerImageName.parse("postgres:16-alpine"))
                    .withDatabaseName("pendulum")
                    .withUsername("pendulum")
                    .withPassword("pendulum")
                    // Faster than the default for a throwaway container; the engine's durability
                    // story is about crashed workers, not crashed databases, so fsync buys the
                    // suite nothing.
                    .withCommand("postgres", "-c", "fsync=off", "-c", "max_connections=200");
            postgres.start();
            config.setJdbcUrl(postgres.getJdbcUrl());
            config.setUsername(postgres.getUsername());
            config.setPassword(postgres.getPassword());
        }

        config.setMaximumPoolSize(32);
        config.setPoolName("pendulum-test");
        DATA_SOURCE = new HikariDataSource(config);

        Pendulum.migrate(DATA_SOURCE);
    }

    /** Log the host and database, never anything that could carry a credential. */
    private static String redact(String jdbcUrl) {
        int query = jdbcUrl.indexOf('?');
        return query < 0 ? jdbcUrl : jdbcUrl.substring(0, query);
    }

    protected final JobStore store = new PostgresJobStore(DATA_SOURCE);

    @BeforeEach
    void resetSchema() {
        execute("TRUNCATE TABLE jobs, outbox");
    }

    // ------------------------------------------------------------- test helpers

    protected UUID enqueue(NewJob job) {
        return store.enqueue(job).id();
    }

    protected UUID enqueue(String jobType) {
        return enqueue(NewJob.of("tenant-a", jobType).build());
    }

    protected Job reload(UUID id) {
        return store.find(id).orElseThrow(() -> new AssertionError("job " + id + " vanished"));
    }

    protected String stateOf(UUID id) {
        return reload(id).state().discriminator();
    }

    /**
     * Force a lease to look expired without waiting for it.
     *
     * <p>This is how the lease race is made deterministic: the interesting scenario is a worker
     * that is merely slow, and reproducing that by actually sleeping would make the suite both
     * slow and flaky.
     */
    protected void expireLease(UUID id) {
        try (Connection connection = DATA_SOURCE.getConnection();
             PreparedStatement statement = connection.prepareStatement(
                     "UPDATE jobs SET lease_expires_at = now() - interval '1 second' WHERE id = ?")) {
            statement.setObject(1, id);
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("failed to expire lease for " + id, e);
        }
    }

    /**
     * Hand a job to a different owner with a fresh fencing token, atomically — what the reaper plus
     * a competing claim would do, minus the timing.
     *
     * <p>Doing it in one statement is what makes the "stalled worker" scenario deterministic: if
     * the test merely backdated the expiry, the stalled worker's own heartbeat could renew the
     * lease before the reaper noticed, and the test would pass or fail on scheduler luck.
     */
    protected long stealLease(UUID id, String newOwner) {
        try (Connection connection = DATA_SOURCE.getConnection();
             PreparedStatement statement = connection.prepareStatement("""
                     UPDATE jobs
                        SET state            = 'LEASED',
                            lease_token      = nextval('pendulum_lease_token_seq'),
                            lease_owner      = ?,
                            lease_expires_at = now() + interval '5 minutes',
                            updated_at       = now()
                      WHERE id = ?
                     RETURNING lease_token
                     """)) {
            statement.setString(1, newOwner);
            statement.setObject(2, id);
            try (var rs = statement.executeQuery()) {
                if (!rs.next()) throw new IllegalStateException("no such job " + id);
                return rs.getLong(1);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("failed to steal lease for " + id, e);
        }
    }

    protected long countInState(String state) {
        try (Connection connection = DATA_SOURCE.getConnection();
             PreparedStatement statement = connection.prepareStatement(
                     "SELECT count(*) FROM jobs WHERE state = ?")) {
            statement.setString(1, state);
            try (var rs = statement.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("failed to count jobs in state " + state, e);
        }
    }

    protected static void execute(String sql) {
        try (Connection connection = DATA_SOURCE.getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute(sql);
        } catch (SQLException e) {
            throw new IllegalStateException("failed to execute: " + sql, e);
        }
    }
}
