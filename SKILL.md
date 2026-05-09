---
name: bullmq-job-reliability-checklist
description: "Checklist and patterns for making BullMQ jobs production-reliable: idempotency keys, retry backoff, poison-job (dead-letter) handling, and observability fields. Use this skill whenever you are creating, modifying, or reviewing a BullMQ worker, queue definition, or job-dispatch call — even if the task just says 'add a queue job' or 'update the worker'. Also trigger for BullMQ error handling, retry config, job deduplication, DLQ setup, or any queue/worker reliability question."
---

# BullMQ Job Reliability Checklist

When writing or modifying BullMQ queue/worker code, work through each section in order. Skip a section only when it genuinely does not apply to the job type, and note why.

---

## 1. Job Contract — Define Before You Code

Every job needs an explicit contract before implementation. Settle these four fields first:

| Field | Purpose | Example |
|---|---|---|
| `name` | Stable job-type discriminator | `"send-welcome-email"` |
| `idempotencyKey` | Dedup token — same key → same job, no duplicate run | `"welcome:${userId}"` |
| `data` | Typed payload (no functions, no circular refs) | `{ userId, templateId }` |
| `opts.jobId` | Set to the idempotencyKey value to enforce dedup at enqueue | same as idempotencyKey |

**Template:**

```ts
await queue.add(
  'send-welcome-email',            // name — never a dynamic string
  { userId, templateId },           // data — serialisable only
  {
    jobId: `welcome:${userId}`,     // dedup key: prevents double-enqueue
    attempts: 5,
    backoff: { type: 'exponential', delay: 2000 },
    removeOnComplete: { count: 500 },
    removeOnFail: false,            // keep for DLQ inspection
  }
);
```

Why `jobId` as dedup key: BullMQ silently drops `queue.add()` if a job with the same `jobId` already exists in waiting/active/delayed states — this is the cheapest idempotency layer.

---

## 2. Retry Matrix

Choose retry settings based on failure type:

| Failure class | `attempts` | `backoff.type` | `backoff.delay` | Notes |
|---|---|---|---|---|
| Transient network/DB | 5 | `exponential` | 1 000–2 000 ms | Standard; jitter added automatically |
| External API (rate-limited) | 8 | `exponential` | 5 000 ms | Watch for Retry-After headers too |
| Non-retriable (validation, logic) | 1 | — | — | Throw `UnrecoverableError` to skip retries |
| Batch/large payload | 3 | `fixed` | 10 000 ms | Avoid thundering herd on partial failures |

```ts
import { UnrecoverableError } from 'bullmq';

// Worker processor
async function process(job: Job<Payload>) {
  const validated = schema.safeParse(job.data);
  if (!validated.success) {
    // Signal BullMQ to move straight to failed — no retry
    throw new UnrecoverableError(`Invalid payload: ${validated.error.message}`);
  }
  // ...recoverable work here
}
```

**Always handle `UnrecoverableError`** for validation, missing-record, or permanent-state failures. Retrying these wastes workers and obscures real bugs.

---

## 3. Idempotency Patterns

The `jobId` dedup covers the enqueue side. The **processor** must also be idempotent:

### Check-then-act pattern
```ts
async function process(job: Job<{ userId: string }>) {
  // Idempotency check before side-effects
  const alreadyDone = await db.jobResults.findUnique({
    where: { idempotencyKey: job.id },
  });
  if (alreadyDone) return alreadyDone; // early-exit, safe to repeat

  const result = await doSideEffect(job.data);

  await db.jobResults.create({
    data: { idempotencyKey: job.id, result, completedAt: new Date() },
  });
  return result;
}
```

### Upsert pattern (for DB writes)
Prefer `upsert` / `INSERT … ON CONFLICT DO UPDATE` over plain `insert` — naturally idempotent if the unique key is the job's idempotency token.

### External calls
For HTTP side-effects (email, webhook, payment), pass the `jobId` as an `Idempotency-Key` header if the downstream API supports it (Stripe, Twilio, etc.).

---

## 4. Dead-Letter Queue (DLQ) Policy

BullMQ moves jobs to the `failed` set after exhausting retries. Treat failed jobs as your DLQ.

### Required configuration
```ts
new Queue('my-queue', {
  defaultJobOptions: {
    removeOnFail: false,   // KEEP failed jobs — they are your DLQ
  },
});
```

### Worker-level DLQ hook
```ts
worker.on('failed', (job, err) => {
  if (job && job.attemptsMade >= (job.opts.attempts ?? 1)) {
    // All retries exhausted — this is a DLQ event
    logger.error({
      event: 'job.dlq',
      jobId: job.id,
      jobName: job.name,
      queue: job.queueName,
      attemptsMade: job.attemptsMade,
      data: job.data,
      error: err.message,
    });
    // Optional: alert, push to external DLQ (SQS, PubSub), open incident
  }
});
```

### Replay / reprocess
Retain a route or script to re-enqueue failed jobs (with new `jobId` suffix or explicit dedup bypass) after root-cause fix:

```ts
// Replay all failed jobs in a queue
const failed = await queue.getFailed();
for (const job of failed) {
  await job.retry();
}
```

---

## 5. Observability Fields

Every job must emit structured logs at key lifecycle points. Minimum required fields:

```ts
// Required on every log line
{
  event: 'job.started' | 'job.completed' | 'job.failed' | 'job.dlq',
  jobId: job.id,
  jobName: job.name,
  queue: job.queueName,
  attemptsMade: job.attemptsMade,
  durationMs?: number,  // completed/failed events
  error?: string,       // failed/dlq events
}
```

### Worker lifecycle hooks
```ts
worker.on('active',    (job) => logger.info({ event: 'job.started',   jobId: job.id, jobName: job.name, queue: job.queueName, attemptsMade: job.attemptsMade }));
worker.on('completed', (job, result) => logger.info({ event: 'job.completed', jobId: job.id, jobName: job.name, queue: job.queueName, attemptsMade: job.attemptsMade }));
worker.on('failed',    (job, err) => logger.warn({ event: 'job.failed', jobId: job?.id, jobName: job?.name, queue: job?.queueName, attemptsMade: job?.attemptsMade, error: err.message }));
```

### Progress updates for long-running jobs
```ts
await job.updateProgress(50); // percentage
// or structured:
await job.updateProgress({ stage: 'processing', recordsHandled: 500 });
```

---

## 6. Operational Verification Checklist

Before merging any queue/worker change, confirm:

- [ ] `jobId` set on enqueue (dedup key)
- [ ] Processor is idempotent (safe to re-run)
- [ ] `UnrecoverableError` thrown for non-retriable failures
- [ ] `attempts` and `backoff` appropriate for failure class (see retry matrix)
- [ ] `removeOnFail: false` on the queue (or job opts) — failed jobs kept for inspection
- [ ] `worker.on('failed')` logs DLQ events with all required fields
- [ ] Structured logs emitted on `active`, `completed`, `failed`
- [ ] A manual replay mechanism exists (route, script, or Bull Board action)
- [ ] Concurrency setting on Worker is intentional (`concurrency: N` default is 1)
- [ ] Graceful shutdown handled: `await worker.close()` on SIGTERM

```ts
// Graceful shutdown skeleton
process.on('SIGTERM', async () => {
  await worker.close();      // wait for active jobs to finish
  await connection.quit();   // close Redis connection
  process.exit(0);
});
```

---

## Quick-Reference: Common Mistakes

| Mistake | Fix |
|---|---|
| Dynamic job `name` (e.g. `"process-" + id`) | Use a static name + put the id in `data` |
| Missing `jobId` → duplicate jobs on retry | Always set `opts.jobId` to the dedup token |
| Throwing generic `Error` for bad input | Use `UnrecoverableError` to skip retries |
| `removeOnFail: true` → silent data loss | Set `removeOnFail: false` |
| No DLQ listener → failures go unnoticed | Add `worker.on('failed')` with alerting |
| Processor not idempotent → double side-effects on retry | Add check-then-act or upsert pattern |

---

*TEC Custom Skill — maintained by the Deltek Technical Services Engineering team.*
