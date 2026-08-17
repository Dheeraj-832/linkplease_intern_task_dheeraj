# FAILURES.md

Honest list of ways this system can still lose a DM, send a duplicate, or
report a wrong number. Each one is real and I can reproduce or explain it.

1. **The rate limiter lives in process memory, so it only holds for one
   instance.** If this ever runs on more than one Render instance (or you
   scale the plan up), each instance keeps its own 9-per-60s window and the
   *combined* send rate can exceed the mock API's 10/60s limit, producing
   429s. The whole design assumes exactly one running instance (the free
   plan gives one). Also, right after a restart the in-memory window starts
   empty, so sends made just before the crash aren't counted — a brief
   window where we could overshoot the limit for a few seconds.

2. **A crash in the ~milliseconds between `POST /v1/dm/send` returning 202
   and our DB commit re-sends the DM on restart.** The mock API already
   created the DM, but our row is still `queued`, so the worker sends again.
   This is *safe only because* we persist a stable `Idempotency-Key` at
   enqueue time and reuse it — the mock returns the original `dm_id` instead
   of a second DM. If the mock's idempotency store has expired our key by
   the time we retry, that retry becomes a genuine duplicate DM.

3. **A `comment.deleted` that arrives after we've already sent the DM
   cannot un-send it.** We only cancel DMs still in `queued`. If the delete
   lands once the row is `accepted` (202 received) or `sent`, the DM has
   already gone out. So a "should not have been sent" DM can still be
   delivered.

4. **Reconciliation gives up after ~40 status polls and marks the DM
   `failed`.** If the mock legitimately keeps a DM in `queued` on its side
   longer than that (a few minutes), we record it as `failed` even though it
   might deliver later. Effect: `sent` is under-counted and `failed`
   over-counted in that tail case. This is honest-low, not inflated, but
   it's still a wrong number.

5. **`/stats` is eventually-consistent, not transactionally consistent with
   in-flight sends.** A read that lands in the middle of a worker tick can
   show a DM as `queued` that is a millisecond away from `accepted`/`sent`.
   The numbers are exact at rest; under a 500-event burst they can lag the
   true state by up to ~1 second (one tick).

6. **Free Render instances spin down after 15 minutes idle.** The first
   webhook after an idle period waits ~1 minute for cold start. Events
   aren't lost (the mock redelivers ~8%, and once we're up we drain fast),
   but if the grading run starts against a cold instance, the earliest
   events are delayed. Mitigated by an external uptime ping around
   submission time, but a cold start at the exact start of the run will
   still delay the first few events.

7. **If you deploy with the default SQLite instead of Postgres, a heavy
   burst can throw "database is locked".** WAL mode + a 30s busy timeout
   make this rare, but under 500 events in 10s a webhook's enqueue could
   time out — we'd have returned `200` while the row never wrote, silently
   dropping that DM. Production uses Postgres (via `DATABASE_URL`), which
   avoids this; SQLite is dev-only.

8. **`duplicates_blocked` is scoped per (rule, user), not per user.** If one
   comment matches two different rules, that user correctly gets two DMs and
   nothing is counted as blocked. The counter only reflects a repeat of the
   *same* rule for the *same* user, which is exactly the assignment's
   dedup requirement — but if you expected user-level dedup, the number will
   look lower than you think.
