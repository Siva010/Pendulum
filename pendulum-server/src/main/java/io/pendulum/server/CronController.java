package io.pendulum.server;

import io.pendulum.core.cron.CronSchedule;
import io.pendulum.core.cron.CronScheduleStore;
import io.pendulum.core.cron.MisfirePolicy;
import io.pendulum.core.json.JsonPayloads;
import io.pendulum.core.store.JobStore;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** Cron schedule administration. */
@RestController
@RequestMapping("/api/cron")
public class CronController {

    private final CronScheduleStore schedules;
    private final JobStore jobs;

    public CronController(CronScheduleStore schedules, JobStore jobs) {
        this.schedules = schedules;
        this.jobs = jobs;
    }

    /**
     * @param timezone an IANA zone id such as {@code Europe/London}, never a fixed offset like
     *                 {@code +01:00} — an offset means the wrong wall-clock time for half the year
     *                 and no tzdata update can rescue it.
     */
    public record ScheduleRequest(
            String tenantId,
            String name,
            String cron,
            String timezone,
            String jobType,
            String queue,
            Object payload,
            Integer priority,
            Integer maxAttempts,
            String misfirePolicy,
            Integer catchUpLimit
    ) {}

    public record ScheduleView(
            UUID id,
            String tenantId,
            String name,
            String cron,
            String timezone,
            String jobType,
            String queue,
            boolean enabled,
            String misfirePolicy,
            int catchUpLimit,
            Instant nextFireAt,
            Instant lastFiredAt,
            long fireCount
    ) {
        static ScheduleView of(CronSchedule schedule) {
            return new ScheduleView(schedule.id(), schedule.tenantId(), schedule.name(),
                    schedule.cronExpression(), schedule.timezone(), schedule.jobType(),
                    schedule.queue(), schedule.enabled(), schedule.misfirePolicy().name(),
                    schedule.catchUpLimit(), schedule.dueAt(), schedule.lastFiredAt(),
                    schedule.fireCount());
        }
    }

    @GetMapping
    public List<ScheduleView> list(@RequestParam(required = false) String tenantId) {
        return schedules.findAll(tenantId).stream().map(ScheduleView::of).toList();
    }

    /**
     * Create or replace a schedule by {@code (tenantId, name)}.
     *
     * <p>The expression and zone are validated here, synchronously, so a typo comes back as a 400
     * while the person who made it is still looking at the screen — rather than as a schedule that
     * silently disables itself at 3am when the ticker first tries to parse it.
     */
    @PostMapping
    public ResponseEntity<?> save(@RequestBody ScheduleRequest request) {
        CronSchedule schedule;
        try {
            CronSchedule.Builder builder = CronSchedule.of(
                    request.tenantId(), request.name(), request.cron(),
                    request.timezone() == null ? "UTC" : request.timezone(), request.jobType());

            if (request.queue() != null) builder.queue(request.queue());
            if (request.payload() != null) builder.payload(JsonPayloads.toJson(request.payload()));
            if (request.priority() != null) builder.priority(request.priority());
            if (request.maxAttempts() != null) builder.maxAttempts(request.maxAttempts());
            if (request.catchUpLimit() != null) builder.catchUpLimit(request.catchUpLimit());
            if (request.misfirePolicy() != null) {
                builder.misfirePolicy(MisfirePolicy.valueOf(request.misfirePolicy().toUpperCase()));
            }
            schedule = builder.build();
        } catch (RuntimeException e) {
            return ResponseEntity.badRequest().body(Map.of("error", String.valueOf(e.getMessage())));
        }

        Instant firstFire = schedule.nextFireAfter(jobs.databaseNow()).orElse(null);
        if (firstFire == null) {
            return ResponseEntity.badRequest().body(Map.of(
                    "error", "'" + request.cron() + "' has no occurrence in the next four years"));
        }

        UUID id = schedules.save(schedule, firstFire);
        return ResponseEntity.status(HttpStatus.CREATED).body(Map.of(
                "id", id, "nextFireAt", firstFire, "cron", schedule.cronExpression()));
    }

    @PostMapping("/{id}/disable")
    public ResponseEntity<Map<String, Object>> disable(@PathVariable UUID id,
                                                       @RequestParam(required = false) String reason) {
        if (schedules.find(id).isEmpty()) {
            return ResponseEntity.notFound().build();
        }
        schedules.disable(id, reason == null ? "disabled by operator" : reason);
        return ResponseEntity.ok(Map.of("id", id, "enabled", false));
    }

    /** Preview the next few fire times — the fastest way to check an expression means what you think. */
    @GetMapping("/preview")
    public ResponseEntity<?> preview(@RequestParam String cron,
                                     @RequestParam(defaultValue = "UTC") String timezone,
                                     @RequestParam(defaultValue = "5") int count) {
        try {
            io.pendulum.core.cron.CronExpression expression =
                    io.pendulum.core.cron.CronExpression.parse(cron);
            java.time.ZoneId zone = java.time.ZoneId.of(timezone);

            List<String> fires = new java.util.ArrayList<>();
            java.time.ZonedDateTime cursor =
                    java.time.ZonedDateTime.ofInstant(jobs.databaseNow(), zone);
            for (int i = 0; i < Math.min(count, 50); i++) {
                java.time.ZonedDateTime next = expression.nextAfter(cursor, zone).orElse(null);
                if (next == null) break;
                fires.add(next.toString());
                cursor = next;
            }
            return ResponseEntity.ok(Map.of("cron", cron, "timezone", timezone, "next", fires));
        } catch (RuntimeException e) {
            return ResponseEntity.badRequest().body(Map.of("error", String.valueOf(e.getMessage())));
        }
    }
}
