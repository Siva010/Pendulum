package io.pendulum.core.cron;

import io.pendulum.core.domain.Job;
import io.pendulum.core.store.JobQuery;
import io.pendulum.core.support.PostgresTestBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** The scheduler against a real database: firing, catch-up, and not double-firing. */
class CronSchedulerIT extends PostgresTestBase {

    private final CronScheduleStore schedules = new PostgresCronScheduleStore(DATA_SOURCE);
    private CronScheduler scheduler;

    @BeforeEach
    void setUp() {
        execute("TRUNCATE TABLE cron_schedules");
        scheduler = new CronScheduler(schedules, store,
                Duration.ofMillis(50), Duration.ofSeconds(30), 100);
    }

    @Test
    @DisplayName("a due schedule enqueues exactly one job")
    void a_due_schedule_fires_once() {
        UUID id = save(everyMinute("nightly"), store.databaseNow().minusSeconds(5));

        assertThat(scheduler.tick()).isEqualTo(1);

        List<Job> jobs = store.findJobs(new JobQuery(null, null, null, "report", 10, 0));
        assertThat(jobs).hasSize(1);
        assertThat(jobs.getFirst().idempotencyKey()).startsWith("cron:" + id + ":");
        assertThat(schedules.find(id).orElseThrow().fireCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("a schedule that is not due yet fires nothing")
    void a_future_schedule_does_not_fire() {
        save(everyMinute("later"), store.databaseNow().plusSeconds(3600));

        assertThat(scheduler.tick()).isZero();
        assertThat(countInState("PENDING")).isZero();
    }

    @Test
    @DisplayName("a disabled schedule is never claimed")
    void disabled_schedules_are_ignored() {
        save(CronSchedule.of("acme", "off", "* * * * *", "UTC", "report").enabled(false).build(),
                store.databaseNow().minusSeconds(60));

        assertThat(scheduler.tick()).isZero();
    }

    /**
     * The crash-safety claim, made concrete. A ticker that fires and dies before recording progress
     * comes back and computes the same occurrence again. The deterministic idempotency key means
     * the unique index rejects it, so the schedule fires once regardless — which is why running
     * this on a single leader is a load optimisation rather than a correctness requirement.
     */
    @Test
    @DisplayName("re-firing the same occurrence creates no second job")
    void the_same_occurrence_cannot_fire_twice() {
        Instant dueAt = store.databaseNow().minusSeconds(5);
        UUID id = save(everyMinute("nightly"), dueAt);

        assertThat(scheduler.tick()).isEqualTo(1);

        // Simulate the crash: put the schedule back as though the tick never recorded its progress.
        rewindTo(id, dueAt);
        assertThat(scheduler.tick()).as("the occurrence was already enqueued").isZero();

        assertThat(store.countJobs(new JobQuery(null, null, null, "report", 10, 0))).isEqualTo(1);
    }

    @Test
    @DisplayName("two schedulers racing the same schedule enqueue one job between them")
    void concurrent_tickers_do_not_double_fire() {
        Instant dueAt = store.databaseNow().minusSeconds(5);
        save(everyMinute("nightly"), dueAt);

        CronScheduler other = new CronScheduler(schedules, store,
                Duration.ofMillis(50), Duration.ofSeconds(30), 100);

        int first = scheduler.tick();
        int second = other.tick();

        assertThat(first + second).isEqualTo(1);
        assertThat(store.countJobs(new JobQuery(null, null, null, "report", 10, 0))).isEqualTo(1);
    }

    @Test
    @DisplayName("SKIP forgets everything missed during downtime")
    void skip_policy_fires_nothing() {
        // Two hours of a minutely schedule went by with nobody ticking.
        UUID id = save(CronSchedule.of("acme", "cache-refresh", "* * * * *", "UTC", "report")
                .misfirePolicy(MisfirePolicy.SKIP).build(),
                store.databaseNow().minus(Duration.ofHours(2)));

        assertThat(scheduler.tick()).isZero();
        assertThat(countInState("PENDING")).isZero();

        // And it resumes cleanly rather than staying stuck in the past.
        Instant next = schedules.find(id).orElseThrow().dueAt();
        assertThat(next).isAfter(store.databaseNow().minusSeconds(1));
    }

    @Test
    @DisplayName("FIRE_ONCE runs the most recent missed occurrence, not all of them")
    void fire_once_catches_up_with_a_single_run() {
        save(CronSchedule.of("acme", "nightly-report", "* * * * *", "UTC", "report")
                .misfirePolicy(MisfirePolicy.FIRE_ONCE).build(),
                store.databaseNow().minus(Duration.ofHours(2)));

        assertThat(scheduler.tick()).as("one report, not the 120 owed").isEqualTo(1);
        assertThat(store.countJobs(new JobQuery(null, null, null, "report", 500, 0))).isEqualTo(1);
    }

    @Test
    @DisplayName("FIRE_ALL replays every missed occurrence, oldest first")
    void fire_all_replays_the_backlog() {
        save(CronSchedule.of("acme", "hourly-rollup", "* * * * *", "UTC", "report")
                .misfirePolicy(MisfirePolicy.FIRE_ALL)
                .catchUpLimit(100)
                .build(),
                store.databaseNow().minus(Duration.ofMinutes(10)));

        int fired = scheduler.tick();

        assertThat(fired).as("roughly one per missed minute").isBetween(9, 12);
        assertThat(store.countJobs(new JobQuery(null, null, null, "report", 500, 0))).isEqualTo(fired);
    }

    /**
     * The guard that keeps a recovered outage from becoming a new one. A minutely schedule that was
     * down for a week owes 10,080 runs; enqueueing them all in a single tick is a self-inflicted
     * denial of service.
     */
    @Test
    @DisplayName("FIRE_ALL is bounded by catch_up_limit")
    void fire_all_is_bounded() {
        save(CronSchedule.of("acme", "greedy", "* * * * *", "UTC", "report")
                .misfirePolicy(MisfirePolicy.FIRE_ALL)
                .catchUpLimit(5)
                .build(),
                store.databaseNow().minus(Duration.ofHours(3)));

        assertThat(scheduler.tick()).isEqualTo(5);
    }

    @Test
    @DisplayName("a catch-up job carries the occurrence's own time, not the time it was enqueued")
    void catch_up_jobs_keep_their_occurrence_time() {
        Instant dueAt = store.databaseNow().minus(Duration.ofMinutes(30));
        save(CronSchedule.of("acme", "windowed", "* * * * *", "UTC", "report")
                .misfirePolicy(MisfirePolicy.FIRE_ONCE).build(), dueAt);

        scheduler.tick();

        Job job = store.findJobs(new JobQuery(null, null, null, "report", 10, 0)).getFirst();
        // A handler that reads run_at to decide which window to process must see the window it is
        // meant to process, not the moment the catch-up happened to run.
        assertThat(job.runAt()).isBefore(store.databaseNow().minusSeconds(1));
    }

    @Test
    @DisplayName("an unparseable schedule is disabled rather than retried forever")
    void invalid_schedules_are_disabled() {
        UUID id = save(everyMinute("broken"), store.databaseNow().minusSeconds(5));
        corruptExpression(id, "not a cron expression");

        assertThat(scheduler.tick()).isZero();

        CronSchedule disabled = schedules.find(id).orElseThrow();
        assertThat(disabled.enabled()).isFalse();
        assertThat(scheduler.tick()).as("and stays out of the way").isZero();
    }

    @Test
    @DisplayName("the timezone decides the fire instant")
    void timezone_is_honoured() {
        UUID london = save(CronSchedule.of("acme", "london", "0 9 * * *", "Europe/London", "report").build(),
                store.databaseNow().minusSeconds(5));
        UUID kolkata = save(CronSchedule.of("acme", "kolkata", "0 9 * * *", "Asia/Kolkata", "report").build(),
                store.databaseNow().minusSeconds(5));

        scheduler.tick();

        Instant nextLondon = schedules.find(london).orElseThrow().dueAt();
        Instant nextKolkata = schedules.find(kolkata).orElseThrow().dueAt();

        assertThat(nextLondon).isNotEqualTo(nextKolkata);
    }

    @Test
    @DisplayName("saving the same name twice updates rather than duplicating")
    void save_is_an_upsert_on_name() {
        save(everyMinute("nightly"), store.databaseNow().plusSeconds(600));
        save(CronSchedule.of("acme", "nightly", "0 3 * * *", "UTC", "report").build(),
                store.databaseNow().plusSeconds(600));

        List<CronSchedule> all = schedules.findAll("acme");
        assertThat(all).hasSize(1);
        assertThat(all.getFirst().cronExpression()).isEqualTo("0 3 * * *");
    }

    // ---------------------------------------------------------------- helpers

    private static CronSchedule everyMinute(String name) {
        return CronSchedule.of("acme", name, "* * * * *", "UTC", "report").build();
    }

    private UUID save(CronSchedule schedule, Instant firstFireAt) {
        return schedules.save(schedule, firstFireAt);
    }

    /** Put a schedule's due time back, as though a ticker had died before recording progress. */
    private void rewindTo(UUID id, Instant dueAt) {
        update("UPDATE cron_schedules SET next_fire_at = ?, fire_count = 0 WHERE id = ?",
                statement -> {
                    statement.setObject(1, java.time.OffsetDateTime.ofInstant(dueAt, java.time.ZoneOffset.UTC));
                    statement.setObject(2, id);
                });
    }

    private void corruptExpression(UUID id, String expression) {
        update("UPDATE cron_schedules SET cron_expression = ? WHERE id = ?",
                statement -> {
                    statement.setString(1, expression);
                    statement.setObject(2, id);
                });
    }

    @FunctionalInterface
    private interface Binder {
        void bind(PreparedStatement statement) throws SQLException;
    }

    private void update(String sql, Binder binder) {
        try (Connection connection = DATA_SOURCE.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            binder.bind(statement);
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("failed: " + sql, e);
        }
    }
}
