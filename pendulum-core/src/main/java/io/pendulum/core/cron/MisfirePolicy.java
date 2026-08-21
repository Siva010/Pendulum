package io.pendulum.core.cron;

/**
 * What to do about occurrences that elapsed while nothing was ticking — a deploy, an outage, a
 * leader election that took a while.
 *
 * <p>There is no universally right answer, which is exactly why it is a per-schedule setting. The
 * right choice depends entirely on whether the job is idempotent over time:
 */
public enum MisfirePolicy {

    /**
     * Forget them. Correct for jobs that recompute current state — a cache refresh missed at 02:00
     * is worthless at 09:00, because the 09:00 run will do the same work with better data.
     */
    SKIP,

    /**
     * Run the most recent missed occurrence once, then resume normally. The sane default: a nightly
     * report that did not run should still run, but you want one report, not the four you owe.
     */
    FIRE_ONCE,

    /**
     * Run every missed occurrence, oldest first, up to {@code catch_up_limit}. Correct only when
     * each occurrence does distinct work — per-hour billing rollups, where skipping an hour loses
     * that hour's data permanently and no later run will reconstruct it.
     */
    FIRE_ALL
}
