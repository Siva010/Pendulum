package io.pendulum.core.cron;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.BitSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

/**
 * A five-field cron expression: {@code minute hour day-of-month month day-of-week}.
 *
 * <p>Hand-written rather than pulled from a library, because {@code pendulum-core} stays
 * dependency-light and because the interesting behaviour here is not the parsing — it is what
 * happens when a local time is asked to exist twice, or not at all.
 *
 * <p>Supported syntax per field: {@code *}, a number, {@code a-b}, {@code a-b/n}, {@code &#42;/n},
 * comma-separated lists of any of those, and three-letter names for months ({@code JAN}) and days
 * ({@code MON}). Plus the usual macros: {@code @hourly @daily @midnight @weekly @monthly @yearly}.
 *
 * <h2>Day-of-month vs day-of-week</h2>
 * When <em>both</em> are restricted, a date matches if <em>either</em> matches — not both. So
 * {@code 0 0 13 * FRI} means "the 13th, and also every Friday", not "Friday the 13th". This looks
 * like a bug and is in fact the behaviour of every Unix cron since Vixie's, so matching it is the
 * only option that will not surprise an operator.
 *
 * <h2>Wall-clock semantics</h2>
 * Occurrences are enumerated over <em>local wall-clock time</em>, not elapsed time. On the autumn
 * transition an hourly schedule therefore fires 24 times across a 25-hour day: the repeated hour is
 * one wall-clock label and so one fire. That is the same tradeoff Vixie cron and Quartz make, and
 * it is what makes "daily at 01:30" fire exactly once on that day rather than twice. One iteration
 * strategy cannot deliver both; double-firing every daily schedule would be the worse failure.
 * Workloads that need elapsed-time semantics want a fixed interval, not a cron expression.
 */
public final class CronExpression {

    private static final List<String> MONTH_NAMES =
            List.of("JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC");
    private static final List<String> DAY_NAMES =
            List.of("SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT");

    /** Roughly four years of days. A schedule with no match in four years is a typo, not a schedule. */
    private static final int MAX_DAYS_SEARCHED = 1500;

    private final String expression;
    private final BitSet minutes;
    private final BitSet hours;
    private final BitSet daysOfMonth;
    private final BitSet months;
    private final BitSet daysOfWeek;
    private final boolean dayOfMonthRestricted;
    private final boolean dayOfWeekRestricted;

    private CronExpression(String expression, BitSet minutes, BitSet hours, BitSet daysOfMonth,
                           BitSet months, BitSet daysOfWeek,
                           boolean dayOfMonthRestricted, boolean dayOfWeekRestricted) {
        this.expression = expression;
        this.minutes = minutes;
        this.hours = hours;
        this.daysOfMonth = daysOfMonth;
        this.months = months;
        this.daysOfWeek = daysOfWeek;
        this.dayOfMonthRestricted = dayOfMonthRestricted;
        this.dayOfWeekRestricted = dayOfWeekRestricted;
    }

    public static CronExpression parse(String raw) {
        if (raw == null || raw.isBlank()) {
            throw new IllegalArgumentException("cron expression is required");
        }
        String normalized = expandMacro(raw.trim());
        String[] fields = normalized.split("\\s+");
        if (fields.length != 5) {
            throw new IllegalArgumentException(
                    "expected 5 fields (minute hour day-of-month month day-of-week), got "
                    + fields.length + " in '" + raw + "'");
        }

        BitSet minutes = parseField(fields[0], 0, 59, List.of(), raw);
        BitSet hours = parseField(fields[1], 0, 23, List.of(), raw);
        BitSet daysOfMonth = parseField(fields[2], 1, 31, List.of(), raw);
        BitSet months = parseField(fields[3], 1, 12, MONTH_NAMES, raw);
        BitSet daysOfWeek = parseField(normalizeSunday(fields[4]), 0, 6, DAY_NAMES, raw);

        return new CronExpression(raw.trim(), minutes, hours, daysOfMonth, months, daysOfWeek,
                !"*".equals(fields[2]), !"*".equals(fields[4]));
    }

    /**
     * The next instant strictly after {@code after}, resolved in {@code zone}.
     *
     * <p>Strictly after, always. That single word is what stops a schedule from firing twice at the
     * same instant when a tick runs slightly early, and what makes the autumn DST repeat safe.
     */
    public Optional<ZonedDateTime> nextAfter(ZonedDateTime after, ZoneId zone) {
        ZonedDateTime origin = after.withZoneSameInstant(zone);
        LocalDateTime candidate = origin.toLocalDateTime()
                .withSecond(0).withNano(0)
                .plusMinutes(1);

        LocalDate date = candidate.toLocalDate();
        LocalTime time = candidate.toLocalTime();

        for (int day = 0; day < MAX_DAYS_SEARCHED; day++) {
            if (matchesDate(date)) {
                Optional<LocalTime> match = nextTimeOnOrAfter(time);
                if (match.isPresent()) {
                    ZonedDateTime resolved = resolve(LocalDateTime.of(date, match.get()), zone);
                    // A DST overlap maps two distinct local times onto the same offset-resolved
                    // instant, and a gap maps a local time forward. Either can produce an instant
                    // that is not actually in the future, so the guard is on the instant, never on
                    // the local time — the only comparison that means anything across a transition.
                    if (resolved.toInstant().isAfter(origin.toInstant())) {
                        return Optional.of(resolved);
                    }
                    // Not in the future after all: step past this local minute and keep looking.
                    time = match.get().plusMinutes(1);
                    if (time.equals(LocalTime.MIDNIGHT)) {
                        date = date.plusDays(1);
                    }
                    day--; // this day is not exhausted yet
                    continue;
                }
            }
            date = date.plusDays(1);
            time = LocalTime.MIDNIGHT;
        }
        return Optional.empty();
    }

    /**
     * Resolve a local time in a zone, coping with the two days a year when local time lies.
     *
     * <p><strong>Spring forward:</strong> 02:30 simply does not exist on the day the clocks jump
     * from 02:00 to 03:00. {@code ZonedDateTime.of} shifts it forward by the length of the gap, so
     * a 02:30 daily job fires at 03:30 that day rather than being skipped. Firing late beats not
     * firing: the nightly reconciliation still runs.
     *
     * <p><strong>Fall back:</strong> 02:30 happens twice. {@code ZonedDateTime.of} picks the
     * <em>earlier</em> offset, so the job fires on the first pass; the second 02:30 resolves to the
     * same local time and is rejected by the strictly-after guard in {@link #nextAfter}. One fire,
     * which is what "daily at 02:30" means.
     */
    private static ZonedDateTime resolve(LocalDateTime local, ZoneId zone) {
        return ZonedDateTime.of(local, zone);
    }

    private boolean matchesDate(LocalDate date) {
        if (!months.get(date.getMonthValue())) {
            return false;
        }
        boolean domMatches = daysOfMonth.get(date.getDayOfMonth());
        // DayOfWeek: Monday=1..Sunday=7; cron uses Sunday=0.
        boolean dowMatches = daysOfWeek.get(date.getDayOfWeek().getValue() % 7);

        if (dayOfMonthRestricted && dayOfWeekRestricted) {
            return domMatches || dowMatches;   // the Vixie quirk, preserved deliberately
        }
        return domMatches && dowMatches;
    }

    private Optional<LocalTime> nextTimeOnOrAfter(LocalTime from) {
        for (int hour = from.getHour(); hour < 24; hour++) {
            if (!hours.get(hour)) {
                continue;
            }
            int startMinute = (hour == from.getHour()) ? from.getMinute() : 0;
            int minute = minutes.nextSetBit(startMinute);
            if (minute >= 0 && minute < 60) {
                return Optional.of(LocalTime.of(hour, minute));
            }
        }
        return Optional.empty();
    }

    public String expression() {
        return expression;
    }

    @Override
    public String toString() {
        return expression;
    }

    // ------------------------------------------------------------- parsing

    private static String expandMacro(String raw) {
        return switch (raw.toLowerCase(Locale.ROOT)) {
            case "@yearly", "@annually" -> "0 0 1 1 *";
            case "@monthly"             -> "0 0 1 * *";
            case "@weekly"              -> "0 0 * * 0";
            case "@daily", "@midnight"  -> "0 0 * * *";
            case "@hourly"              -> "0 * * * *";
            default -> raw;
        };
    }

    /** Cron allows 7 for Sunday as well as 0; normalise before parsing so the bitset stays 0-6. */
    private static String normalizeSunday(String field) {
        return field.replaceAll("(?<![0-9])7(?![0-9])", "0");
    }

    private static BitSet parseField(String field, int min, int max, List<String> names, String raw) {
        BitSet values = new BitSet(max + 1);
        for (String part : field.split(",")) {
            parsePart(part.trim(), min, max, names, values, raw);
        }
        if (values.isEmpty()) {
            throw new IllegalArgumentException("field '" + field + "' matches nothing in '" + raw + "'");
        }
        return values;
    }

    private static void parsePart(String part, int min, int max, List<String> names,
                                  BitSet values, String raw) {
        int step = 1;
        String range = part;

        int slash = part.indexOf('/');
        if (slash >= 0) {
            range = part.substring(0, slash);
            step = parseInt(part.substring(slash + 1), raw);
            if (step <= 0) {
                throw new IllegalArgumentException("step must be positive in '" + part + "' of '" + raw + "'");
            }
        }

        int from;
        int to;
        if ("*".equals(range)) {
            from = min;
            to = max;
        } else {
            int dash = range.indexOf('-');
            if (dash >= 0) {
                from = parseValue(range.substring(0, dash), min, max, names, raw);
                to = parseValue(range.substring(dash + 1), min, max, names, raw);
            } else {
                from = parseValue(range, min, max, names, raw);
                // A bare number with a step means "from here to the end", as in `5/15`.
                to = (slash >= 0) ? max : from;
            }
        }

        if (from > to) {
            throw new IllegalArgumentException(
                    "range " + from + "-" + to + " is inverted in '" + raw + "'");
        }
        for (int value = from; value <= to; value += step) {
            values.set(value);
        }
    }

    private static int parseValue(String token, int min, int max, List<String> names, String raw) {
        String trimmed = token.trim();
        int index = names.indexOf(trimmed.toUpperCase(Locale.ROOT));
        int value = (index >= 0) ? index + (names == MONTH_NAMES ? 1 : 0) : parseInt(trimmed, raw);
        if (value < min || value > max) {
            throw new IllegalArgumentException(
                    "value " + value + " out of range " + min + "-" + max + " in '" + raw + "'");
        }
        return value;
    }

    private static int parseInt(String token, String raw) {
        try {
            return Integer.parseInt(token.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("'" + token + "' is not a number in '" + raw + "'", e);
        }
    }
}
