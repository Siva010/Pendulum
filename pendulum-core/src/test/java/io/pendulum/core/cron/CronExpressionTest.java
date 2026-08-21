package io.pendulum.core.cron;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Pure unit tests. No database, no container — and the DST cases are the point of the file. */
class CronExpressionTest {

    private static final ZoneId LONDON = ZoneId.of("Europe/London");
    private static final ZoneId NEW_YORK = ZoneId.of("America/New_York");
    private static final ZoneId KOLKATA = ZoneId.of("Asia/Kolkata");
    private static final ZoneId UTC = ZoneId.of("UTC");

    @Nested
    @DisplayName("basic scheduling")
    class Basics {

        @Test
        @DisplayName("every minute")
        void every_minute() {
            assertThat(next("* * * * *", "2026-03-01T10:00:30Z"))
                    .isEqualTo(ZonedDateTime.parse("2026-03-01T10:01Z"));
        }

        @Test
        @DisplayName("the next occurrence is strictly after, never equal")
        void never_returns_the_same_instant() {
            // A tick landing exactly on the fire time must not re-fire it.
            assertThat(next("0 * * * *", "2026-03-01T10:00:00Z"))
                    .isEqualTo(ZonedDateTime.parse("2026-03-01T11:00Z"));
        }

        @Test
        @DisplayName("a specific time of day")
        void daily_at_a_time() {
            assertThat(next("30 2 * * *", "2026-03-01T10:00:00Z"))
                    .isEqualTo(ZonedDateTime.parse("2026-03-02T02:30Z"));
        }

        @Test
        @DisplayName("steps, ranges and lists")
        void steps_ranges_and_lists() {
            assertThat(next("*/15 * * * *", "2026-03-01T10:02:00Z"))
                    .isEqualTo(ZonedDateTime.parse("2026-03-01T10:15Z"));
            assertThat(next("0 9-17 * * *", "2026-03-01T20:00:00Z"))
                    .isEqualTo(ZonedDateTime.parse("2026-03-02T09:00Z"));
            assertThat(next("0 0,12 * * *", "2026-03-01T06:00:00Z"))
                    .isEqualTo(ZonedDateTime.parse("2026-03-01T12:00Z"));
            assertThat(next("0 0 * * MON", "2026-03-01T06:00:00Z"))
                    .isEqualTo(ZonedDateTime.parse("2026-03-02T00:00Z"));
        }

        @Test
        @DisplayName("macros")
        void macros() {
            assertThat(next("@daily", "2026-03-01T10:00:00Z"))
                    .isEqualTo(ZonedDateTime.parse("2026-03-02T00:00Z"));
            assertThat(next("@hourly", "2026-03-01T10:30:00Z"))
                    .isEqualTo(ZonedDateTime.parse("2026-03-01T11:00Z"));
            assertThat(next("@monthly", "2026-03-05T10:00:00Z"))
                    .isEqualTo(ZonedDateTime.parse("2026-04-01T00:00Z"));
        }

        @Test
        @DisplayName("month and day names, and 7 as Sunday")
        void names_and_sunday_seven() {
            assertThat(next("0 0 1 JAN *", "2026-06-01T00:00:00Z"))
                    .isEqualTo(ZonedDateTime.parse("2027-01-01T00:00Z"));
            // Sunday is both 0 and 7; both must mean the same day.
            assertThat(next("0 0 * * 7", "2026-03-02T00:00:00Z"))
                    .isEqualTo(next("0 0 * * 0", "2026-03-02T00:00:00Z"));
        }

        @Test
        @DisplayName("29 February is found in a leap year and skipped otherwise")
        void leap_day() {
            assertThat(next("0 0 29 2 *", "2026-03-01T00:00:00Z"))
                    .isEqualTo(ZonedDateTime.parse("2028-02-29T00:00Z"));
        }
    }

    /**
     * The Vixie quirk. When both day-of-month and day-of-week are restricted, cron matches
     * <em>either</em>, not both — so this expression is "the 13th, plus every Friday", not
     * "Friday the 13th". It reads like a bug; it is the behaviour of every Unix cron in existence,
     * and quietly doing something more sensible is how you surprise an operator at 3am.
     */
    @Nested
    @DisplayName("day-of-month vs day-of-week")
    class DayFields {

        @Test
        @DisplayName("both restricted means OR, not AND")
        void both_restricted_is_or() {
            // 2026-03-13 is a Friday. The 6th is also a Friday, and comes first.
            assertThat(next("0 0 13 * FRI", "2026-03-01T00:00:00Z"))
                    .isEqualTo(ZonedDateTime.parse("2026-03-06T00:00Z"));

            // The 10th is a Tuesday: matched by day-of-month alone.
            assertThat(next("0 0 10 * FRI", "2026-03-07T00:00:00Z"))
                    .isEqualTo(ZonedDateTime.parse("2026-03-10T00:00Z"));
        }

        @Test
        @DisplayName("only one restricted means that one must match")
        void one_restricted_is_and() {
            assertThat(next("0 0 15 * *", "2026-03-01T00:00:00Z"))
                    .isEqualTo(ZonedDateTime.parse("2026-03-15T00:00Z"));
            assertThat(next("0 0 * * WED", "2026-03-01T00:00:00Z"))
                    .isEqualTo(ZonedDateTime.parse("2026-03-04T00:00Z"));
        }
    }

    /**
     * The two days a year when local time lies. Both cases below are real dates in real zones, and
     * both are the kind of thing that silently misfires in production for a year before anyone
     * notices.
     */
    @Nested
    @DisplayName("daylight saving")
    class DaylightSaving {

        /**
         * Spring forward: on 2026-03-29 in London the clocks jump 01:00 -> 02:00, so 01:30 never
         * happens. A daily 01:30 job must still run — firing late is recoverable, not firing at all
         * silently skips a day's reconciliation.
         */
        @Test
        @DisplayName("a time that does not exist still fires, shifted past the gap")
        void spring_forward_does_not_skip_the_job() {
            ZonedDateTime fire = next("30 1 * * *", "2026-03-28T12:00:00Z", LONDON);

            assertThat(fire.toLocalDate()).isEqualTo(java.time.LocalDate.parse("2026-03-29"));
            assertThat(fire.toInstant()).isEqualTo(java.time.Instant.parse("2026-03-29T01:30:00Z"));
            // 01:30 GMT does not exist locally, so it lands at 02:30 BST — the same instant.
            assertThat(fire.toLocalTime()).isEqualTo(java.time.LocalTime.parse("02:30"));
        }

        /**
         * Fall back: on 2026-10-25 in London 01:30 happens twice, once at BST and again at GMT.
         * "Daily at 01:30" means once. Firing twice would double-charge, double-email, double-post.
         */
        @Test
        @DisplayName("a time that happens twice fires only once")
        void fall_back_does_not_double_fire() {
            ZonedDateTime first = next("30 1 * * *", "2026-10-24T12:00:00Z", LONDON);
            assertThat(first.toInstant()).isEqualTo(java.time.Instant.parse("2026-10-25T00:30:00Z"));

            // Continuing from that fire, the next one is the following day — not the second 01:30.
            ZonedDateTime second = next("30 1 * * *", first, LONDON);
            assertThat(second.toLocalDate()).isEqualTo(java.time.LocalDate.parse("2026-10-26"));
        }

        /**
         * The consequence of wall-clock semantics, asserted rather than glossed over: on fall-back
         * day an hourly schedule fires 24 times, not 25. The repeated 01:00 is one wall-clock label
         * and therefore one fire, so the second pass through that hour is skipped.
         *
         * <p>This is what Vixie cron and Quartz both do, and it is not free — it is the other side
         * of the guarantee in the test above. One iteration strategy cannot give both "daily at
         * 01:30 fires once" and "hourly fires 25 times on a 25-hour day": iterating instants would
         * buy the second at the cost of double-firing every daily schedule, which is much worse.
         * A workload that genuinely needs elapsed-time semantics wants a fixed-interval schedule,
         * not a cron expression.
         */
        @Test
        @DisplayName("an hourly schedule fires 24 times on the 25-hour day, not 25")
        void fall_back_hourly_follows_wall_clock() {
            List<java.time.Instant> fires = fireTimes("0 * * * *", "2026-10-25T00:00:00Z", LONDON, 4);

            assertThat(fires).containsExactly(
                    java.time.Instant.parse("2026-10-25T02:00:00Z"),
                    java.time.Instant.parse("2026-10-25T03:00:00Z"),
                    java.time.Instant.parse("2026-10-25T04:00:00Z"),
                    java.time.Instant.parse("2026-10-25T05:00:00Z"));
            assertThat(fires).doesNotHaveDuplicates().isSorted();

            // The whole fall-back day: 24 distinct wall-clock hours across 25 elapsed hours.
            List<java.time.Instant> wholeDay = fireTimes("0 * * * *", "2026-10-24T22:59:00Z", LONDON, 24);
            assertThat(wholeDay).doesNotHaveDuplicates().isSorted();
            assertThat(java.time.Duration.between(wholeDay.getFirst(), wholeDay.getLast()).toHours())
                    .as("24 wall-clock fires span 24 hours of labels but 24 elapsed hours here")
                    .isEqualTo(23 + 1);
        }

        @Test
        @DisplayName("US transitions differ from European ones and both are honoured")
        void new_york_transition() {
            // New York springs forward 2026-03-08 at 02:00 local.
            ZonedDateTime fire = next("30 2 * * *", "2026-03-07T12:00:00Z", NEW_YORK);
            assertThat(fire.toLocalDate()).isEqualTo(java.time.LocalDate.parse("2026-03-08"));
            assertThat(fire.toLocalTime()).isEqualTo(java.time.LocalTime.parse("03:30"));
        }

        @Test
        @DisplayName("a half-hour-offset zone with no DST at all")
        void kolkata_has_no_transitions() {
            ZonedDateTime fire = next("30 9 * * *", "2026-03-28T00:00:00Z", KOLKATA);
            assertThat(fire.toLocalTime()).isEqualTo(java.time.LocalTime.parse("09:30"));
            assertThat(fire.toInstant()).isEqualTo(java.time.Instant.parse("2026-03-28T04:00:00Z"));
        }

        @Test
        @DisplayName("the same expression in two zones fires at different instants")
        void zones_are_honoured() {
            java.time.Instant london = next("0 9 * * *", "2026-06-01T00:00:00Z", LONDON).toInstant();
            java.time.Instant newYork = next("0 9 * * *", "2026-06-01T00:00:00Z", NEW_YORK).toInstant();

            assertThat(london).isNotEqualTo(newYork);
            assertThat(java.time.Duration.between(london, newYork).toHours()).isEqualTo(5);
        }

        @Test
        @DisplayName("consecutive fires are always strictly increasing across a transition")
        void fires_never_go_backwards() {
            for (ZoneId zone : List.of(LONDON, NEW_YORK, UTC)) {
                List<java.time.Instant> fires = fireTimes("*/20 * * * *", "2026-10-24T20:00:00Z", zone, 60);
                assertThat(fires).as("in %s", zone).isSorted().doesNotHaveDuplicates();
            }
        }
    }

    @Nested
    @DisplayName("rejecting nonsense")
    class Validation {

        @Test
        @DisplayName("wrong field count")
        void wrong_field_count() {
            assertThatThrownBy(() -> CronExpression.parse("* * * *"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("expected 5 fields");
        }

        @Test
        @DisplayName("out of range values")
        void out_of_range() {
            assertThatThrownBy(() -> CronExpression.parse("60 * * * *"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("out of range");
            assertThatThrownBy(() -> CronExpression.parse("* 24 * * *"))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> CronExpression.parse("* * * 13 *"))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        @DisplayName("inverted ranges, bad steps and non-numbers")
        void malformed_parts() {
            assertThatThrownBy(() -> CronExpression.parse("30-10 * * * *"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("inverted");
            assertThatThrownBy(() -> CronExpression.parse("*/0 * * * *"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("step must be positive");
            assertThatThrownBy(() -> CronExpression.parse("banana * * * *"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("not a number");
        }

        @Test
        @DisplayName("null and blank")
        void null_and_blank() {
            assertThatThrownBy(() -> CronExpression.parse(null))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> CronExpression.parse("   "))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        @DisplayName("a date that never occurs returns empty rather than looping forever")
        void impossible_date_terminates() {
            // 30 February.
            assertThat(CronExpression.parse("0 0 30 2 *")
                    .nextAfter(ZonedDateTime.parse("2026-01-01T00:00Z"), UTC))
                    .isEmpty();
        }
    }

    // ---------------------------------------------------------------- helpers

    private static ZonedDateTime next(String expression, String afterIso) {
        return next(expression, afterIso, UTC);
    }

    private static ZonedDateTime next(String expression, String afterIso, ZoneId zone) {
        return next(expression, ZonedDateTime.parse(afterIso), zone);
    }

    private static ZonedDateTime next(String expression, ZonedDateTime after, ZoneId zone) {
        return CronExpression.parse(expression).nextAfter(after, zone)
                .orElseThrow(() -> new AssertionError("no next fire time for '" + expression + "'"));
    }

    private static List<java.time.Instant> fireTimes(String expression, String afterIso, ZoneId zone, int count) {
        CronExpression cron = CronExpression.parse(expression);
        List<java.time.Instant> fires = new ArrayList<>(count);
        ZonedDateTime cursor = ZonedDateTime.parse(afterIso).withZoneSameInstant(zone);
        for (int i = 0; i < count; i++) {
            cursor = cron.nextAfter(cursor, zone).orElseThrow();
            fires.add(cursor.toInstant());
        }
        return fires;
    }
}
