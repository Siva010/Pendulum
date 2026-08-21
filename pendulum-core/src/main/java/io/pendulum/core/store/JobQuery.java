package io.pendulum.core.store;

/**
 * Filters for the admin job listing.
 *
 * <p>Every field is optional and every one is bound as a parameter, never concatenated. An admin
 * console that interpolates a state name into SQL is a console with an injection hole in it, and
 * "it is only reachable by admins" has never been a defence worth making.
 *
 * @param limit  capped by the store, because an operator who asks for everything on a table with
 *               ten million terminal rows should get a page, not an outage.
 */
public record JobQuery(
        String tenantId,
        String queue,
        String state,
        String jobType,
        int limit,
        int offset
) {

    public static final int MAX_LIMIT = 500;

    public JobQuery {
        if (limit <= 0) limit = 50;
        if (limit > MAX_LIMIT) limit = MAX_LIMIT;
        if (offset < 0) offset = 0;
    }

    public static JobQuery of(String state, int limit) {
        return new JobQuery(null, null, state, null, limit, 0);
    }

    public static JobQuery deadLetters(int limit, int offset) {
        return new JobQuery(null, null, "DEAD_LETTERED", null, limit, offset);
    }
}
