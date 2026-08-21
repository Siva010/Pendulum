# Pendulum

[![CI](https://github.com/Siva010/Pendulum/actions/workflows/ci.yml/badge.svg)](https://github.com/Siva010/Pendulum/actions/workflows/ci.yml)
[![Java 21](https://img.shields.io/badge/Java-21-orange)](https://openjdk.org/projects/jdk/21/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

A durable job execution engine in Java 21, built on Postgres.

Workers claim jobs with `SELECT ... FOR UPDATE SKIP LOCKED`, hold them on heartbeat-renewed
leases, and report results through writes fenced by a monotonic token. Kill a worker mid-execution
and its jobs come back on another worker — with no job lost and no result written twice.

---

## The guarantee, stated precisely

**At-least-once delivery, with fencing that bounds replay to a crashed worker's in-flight window.**

That phrasing is deliberate. What the engine guarantees:

- **No job is lost.** A worker that dies without warning has its leases reclaimed by the reaper and
  its jobs re-dispatched.
- **No stale worker can overwrite a live one.** Every write that reports an outcome is conditional
  on holding the current fencing token. A worker that stalled past its lease and comes back to life
  writes nothing.
- **Retries are bounded and classified.** A `503` retries on an exponential curve with full jitter;
  a `422` goes straight to the dead-letter state; an attempt budget is enforced by the database.

What it does **not** guarantee, and cannot:

- **Exactly-once execution.** A worker that completes a side effect and dies before recording it is
  indistinguishable from one that died before doing anything. That job runs again. Every durable
  execution system has this property; the ones that claim otherwise are describing exactly-once
  *effects* achieved by pairing at-least-once delivery with idempotent handlers, which is what
  Pendulum expects of you.

`ChaosIT` asserts exactly this and no more: every job finishes, nothing is dead-lettered, and
replays stay bounded by what the dying worker had in flight.

---

## Quickstart

```bash
docker compose up -d
```

```bash
./mvnw verify
```

`./mvnw test` runs the unit tests in about a second. `./mvnw verify` additionally runs the
integration and chaos suites against a real Postgres in Testcontainers — that needs Docker and
takes a few minutes, mostly spent deliberately waiting out lease expiry.

Run the server:

```bash
./mvnw -pl pendulum-server spring-boot:run
```

```bash
curl -X POST localhost:8080/api/jobs -H 'content-type: application/json' -d '{"tenantId":"acme","jobType":"noop","payload":{"hello":"world"}}'
```

```bash
curl localhost:8080/api/queues/default/depth
```

---

## Embedding it

The engine is a plain object graph over a `DataSource`. No Spring types appear anywhere in
`pendulum-core`.

```java
Pendulum pendulum = Pendulum.builder(dataSource)
        .migrate()
        .handler("send-welcome-email", ctx -> mailer.send(ctx.payload()))
        .workers(2)
        .build();

pendulum.start();
pendulum.enqueue(NewJob.of("acme", "send-welcome-email")
        .payload(json)
        .idempotencyKey("welcome:" + userId)
        .runAfter(Duration.ofMinutes(5))
        .build());
```

---

## How it works

### Dispatch

One statement claims a batch, issues a fencing token, sets the lease expiry, and returns the rows —
no select-then-update, because two workers running that pair both see the same row.

```sql
UPDATE jobs j
   SET state             = 'LEASED',
       lease_token       = nextval('pendulum_lease_token_seq'),
       lease_owner       = ?,
       lease_expires_at  = now() + make_interval(secs => ?),
       attempt           = j.attempt + 1
  FROM (SELECT id FROM jobs
         WHERE state = 'PENDING' AND queue = ? AND run_at <= now()
         ORDER BY priority DESC, run_at, id
         LIMIT ? FOR UPDATE SKIP LOCKED) candidate
 WHERE j.id = candidate.id
RETURNING j.*;
```

`SKIP LOCKED` is what makes N pollers contention-free: a row another worker is already claiming is
stepped over rather than waited on, so claim latency stays flat as workers are added instead of
degrading into a lock convoy.

The attempt counter advances **at claim time**, not at failure time. A handler that takes the JVM
down with it never reaches a failure path, so a counter that only advanced on clean failures would
let a poison pill cycle forever, killing a worker each time round.

### The lease race, and why timeouts alone cannot fix it

> Worker A's lease expires at T. Worker B picks the job up at T+1ms. Worker A — which was never
> dead, only slow, because a 400ms GC pause is indistinguishable from a crash to everyone else —
> finishes at T+2ms and writes its result.

There is no lease duration that fixes this. Short enough to out-run a stop-the-world pause is too
short to be useful; long enough to be useful is long enough for the race. The fix is not timing,
it is authority: every claim issues a monotonically increasing token, and every write that reports
an outcome carries it.

```sql
UPDATE jobs SET state = 'SUCCEEDED', ...
 WHERE id = ? AND lease_token = ? AND state IN ('LEASED','RUNNING');
```

Worker A's write matches zero rows. Its result is discarded; worker B's stands.

Fencing protects the *result*. It cannot un-call a payment API, which is why `JobContext.checkLease()`
exists for handlers doing something long and externally visible, and why handlers must be idempotent.

### Heartbeats and reaping

In-flight leases are renewed on a schedule. A worker that is alive but slow keeps its jobs; a worker
that is gone stops renewing, and after `leaseDuration` the reaper takes them back — dead-lettering
any that have exhausted their attempt budget rather than requeuing them forever.

A failed heartbeat *round trip* is not treated as lease loss. Losing a lease over a transient
database blip would be a self-inflicted duplicate execution, so the lease expiry stays the real
deadline. A heartbeat that returns "you don't own this any more" **is** lease loss: the execution is
marked lost, the handler is interrupted, and nothing is written.

### Time

Every timestamp comparison uses database `now()`. Worker clocks are never trusted, and even
"retry in five minutes" is resolved in SQL rather than in the JVM — hence `Schedule` keeping the
*intent* (`Now` / `After` / `At`) intact until the INSERT. Two workers with three seconds of drift
produce nondeterministic lease behaviour that costs a day to debug.

---

## Design decisions

**Postgres as the queue, not Redis or Kafka.** You need transactional enqueue alongside business
writes, a durable and queryable job history, and correctness over raw throughput. `SKIP LOCKED`
makes Postgres a genuinely good queue into the low tens of thousands of jobs/sec. Past that it is
the wrong tool — but Kafka cannot participate in your business transaction, which is the entire
point of the outbox pattern this is built to support.

**Virtual threads for execution, a platform thread for polling.** Job handlers are almost entirely
I/O-bound, so thread-per-job costs a few hundred bytes of heap instead of a megabyte of stack and
blocking code stays blocking code. The poll loop is one blocking JDBC call in a tight loop and
gains nothing from being virtual. Concurrency is bounded by a semaphore, because a virtual thread
executor is unbounded by construction and the backpressure has to come from somewhere.

**Plain JDBC, no ORM.** No ORM expresses `UPDATE ... FROM (SELECT ... FOR UPDATE SKIP LOCKED)
RETURNING *` without a native-query escape hatch, and at that point the ORM has stopped earning
its keep.

**No long transactions.** The engine's unit of atomicity is a single statement. A transaction held
open across a handler invocation would pin a connection and an MVCC snapshot for the whole job —
how a queue table accumulates bloat and a pool runs dry.

**Sealed interfaces over enums for state.** A leased job has a token, an owner and an expiry; a
dead-lettered one has a reason. With an enum those become nullable columns that every read site has
to reason about. Here the compiler does it, and adding a state breaks every non-exhaustive `switch`
at compile time.

**No Spring below the edge.** `pendulum-core` has no Spring on its classpath at all. That makes the
engine testable without an application context, embeddable in something that is not a Boot app, and
honest about what the framework is actually providing.

**No H2 in tests.** H2 does not implement `SKIP LOCKED`, its advisory locks are not Postgres
advisory locks, and its MVCC differs precisely where this engine's correctness lives. Every
integration test runs against a real Postgres 16 in Testcontainers.

---

## Measured

Run them yourself with `./mvnw verify -Pbenchmarks`. Excluded from CI on purpose — a number
measured on a shared runner describes the runner's neighbours, not this engine.

Hardware: Intel i5-8600K (6 cores), 16 GB RAM, Postgres 16 in Docker Desktop for Windows.

| | |
|---|---|
| Throughput, saturated queue | **1,646 jobs/sec** |
| Enqueue rate, single connection | 1,626 jobs/sec |
| Scheduling jitter, unsaturated (p50 / p95 / p99) | **61 / 125 / 256 ms** |
| Recovery after `kill -9`, 5s lease | **5.47s** to fully drain |
| Jobs lost across all runs | **0** |
| Jobs dead-lettered under fault injection | **0** |

Read these honestly:

**Throughput is engine overhead, not a workload prediction.** The handler is a no-op, so this
measures claim + mark-running + complete and nothing else — about three round trips per job.
Real jobs are dominated by their own I/O. It is also measured through Docker Desktop's network
layer on Windows, which is materially slower than a native Linux socket; treat it as a floor.

**Scheduling jitter is bounded below by the poll interval**, and that is the whole story: a
polling dispatcher cannot react faster than it polls. The p99 of 256ms reflects adaptive backoff
during idle stretches. Replacing polling with `LISTEN/NOTIFY` would remove that floor — a worker
woken by the enqueue itself starts in single-digit milliseconds.

**Recovery time is a restatement of the lease duration**, which is the point. A 5s lease recovers
in ~5s. Shortening it recovers faster and tolerates less GC pause; lengthening it does the
reverse. The engine does not remove that tradeoff — fencing just makes it *safe* to tune, because
a short lease can no longer turn a slow worker into a double execution.

### The obvious next optimization

Every job costs a `markRunning` round trip purely to make "leased but not yet started" observable.
Claiming straight into `RUNNING` would cut roughly a third of the per-job database cost, at the
price of losing that distinction in the admin view. Worth doing behind a flag.

---

## Test suite

| Suite | What it pins down |
|---|---|
| `RetryPolicyTest` | Backoff curves, jitter bounds, error classification through wrapped and cyclic cause chains. No database. |
| `LeaseDispatchIT` | Eligibility, priority ordering, queue isolation, monotonic tokens, per-tenant idempotent enqueue, and 8 workers draining 600 jobs with zero overlap. |
| `FencingIT` | The lease race: stale completions and stale failures rejected, heartbeat renewal, orphan requeue, dead-lettering exhausted orphans, attempt refund on graceful release. |
| `WorkerExecutionIT` | End to end. Exactly-once under normal operation, retry vs. dead-letter classification, unknown job types retried rather than written off, delayed jobs held back, drain on shutdown. |
| `ChaosIT` | `terminateAbruptly()` models `kill -9` — the crashed worker writes *nothing* further. A killed worker's jobs finish elsewhere; a fleet losing a worker mid-flight loses no jobs; a stalled worker is fenced off and never writes a stale result. |

---

## Not built yet

Milestone 1 is the durable dispatch core. Deliberately not here yet, roughly in build order:

- **Transactional outbox** — enqueue in the same transaction as the business write. The single
  most commercially valuable feature in the design.
- **Dead-letter inspection and replay** — the `DEAD_LETTERED` state exists; the operator surface
  does not.
- **Cron scheduling** — timezone-aware, DST-correct, with catch-up and misfire policies.
- **Leader election** via Postgres advisory lock, so cron ticking and reaping are singleton
  responsibilities with correct handling of lease loss.
- **Durable workflows** — `workflow_runs` and `step_results`, resuming at the last completed step.
- **Multi-tenant fairness** — weighted round-robin, per-queue and per-tenant concurrency caps.
- **Distributed rate limiting** and concurrency gates.
- **Async trace propagation** — W3C trace context serialized at enqueue, restored as a linked span
  at execution.
- **Micrometer/OTel binding** at the server edge, `LISTEN/NOTIFY` to replace polling, JMH benchmarks
  of virtual vs. platform threads, KEDA autoscaling on queue depth.

---

## Layout

```
pendulum-core/     the engine — plain Java 21 + JDBC, zero Spring
  domain/          Job, JobState (sealed), LeaseToken, Schedule, NewJob
  store/           JobStore + PostgresJobStore — all the SQL lives here
  retry/           BackoffPolicy, ErrorClassifier, RetryPolicy, RetryDecision
  engine/          Worker, LeaseReaper, HandlerRegistry, WorkerConfig
  json/            JsonPayloads — JSON only, never native serialization
  db/migration/    Flyway migrations
pendulum-server/   Spring Boot at the edges — DI, config, Actuator, admin API
```
