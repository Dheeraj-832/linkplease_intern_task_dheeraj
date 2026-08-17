"""
Background worker: the part that does the slow, failing network work.

The webhook handler NEVER makes a network call — it just writes rows to
the outbox and returns 200 fast. This loop, running in its own thread,
is the only thing that talks to the DM endpoints. That separation is why
we can return 200 within 5 seconds even when the mock API is timing out.

Each tick the loop:
  1. Reconciles DMs the API "accepted" (202) to learn if they actually
     delivered or failed. Reads are free (no rate limit), so we poll them.
  2. Sends queued DMs, respecting the 10-per-60s send rate limit and any
     Retry-After back-off the API asked for.
"""
import threading
import time
from collections import deque
from datetime import timedelta

from sqlalchemy import select

from . import config, pseudogram
from .db import (
    SessionLocal,
    Outbox,
    utcnow,
    STATUS_QUEUED,
    STATUS_ACCEPTED,
    STATUS_SENT,
    STATUS_FAILED,
)


class RateLimiter:
    """
    Sliding-window limiter: at most `max_calls` in `window` seconds.

    We record a timestamp for every POST /v1/dm/send we make (regardless
    of its response, because every request counts against the mock API's
    limit). Before sending we drop timestamps older than the window and
    check whether there is room.
    """

    def __init__(self, max_calls: int, window: int):
        self.max_calls = max_calls
        self.window = window
        self._calls: deque[float] = deque()
        # If the API 429s us, it tells us when to try again; we honour that
        # globally so we stop hammering it.
        self._blocked_until = 0.0

    def _prune(self, now: float) -> None:
        while self._calls and now - self._calls[0] > self.window:
            self._calls.popleft()

    def can_send(self) -> bool:
        now = time.monotonic()
        if now < self._blocked_until:
            return False
        self._prune(now)
        return len(self._calls) < self.max_calls

    def record_send(self) -> None:
        self._calls.append(time.monotonic())

    def block_for(self, seconds: float) -> None:
        self._blocked_until = max(self._blocked_until, time.monotonic() + seconds)


_rate_limiter = RateLimiter(config.RATE_LIMIT_MAX, config.RATE_LIMIT_WINDOW_SECONDS)


def _backoff_seconds(attempts: int) -> float:
    """Exponential back-off, capped, e.g. 2, 4, 8, 16, 32, 60, 60..."""
    return float(min(60, 2 ** attempts))


def reconcile_accepted(session) -> None:
    """
    For DMs the API accepted (202) but hasn't confirmed, poll their status.

    delivered -> sent (this is the only path that increments `sent`)
    failed    -> re-queue for another send attempt (or fail if exhausted)
    queued    -> still pending, check again later
    """
    now = utcnow()
    rows = (
        session.execute(
            select(Outbox)
            .where(Outbox.status == STATUS_ACCEPTED)
            .where(Outbox.next_attempt_at <= now)
            .limit(100)
        )
        .scalars()
        .all()
    )

    for row in rows:
        if not row.dm_id:
            # Shouldn't happen, but if we have no dm_id we can't reconcile;
            # push it back to be re-sent.
            row.status = STATUS_QUEUED
            row.next_attempt_at = now
            continue

        result = pseudogram.get_dm_status(row.dm_id)
        row.reconcile_attempts += 1

        if result.outcome == "ok" and result.status == "delivered":
            row.status = STATUS_SENT
        elif result.outcome == "ok" and result.status == "failed":
            # The API accepted it, then it failed downstream. Retry it.
            if row.attempts < config.MAX_SEND_ATTEMPTS:
                row.status = STATUS_QUEUED
                row.dm_id = None
                row.next_attempt_at = now
            else:
                row.status = STATUS_FAILED
        else:
            # Still "queued" on their side, or the read errored.
            if row.reconcile_attempts >= config.MAX_RECONCILE_ATTEMPTS:
                row.status = STATUS_FAILED
            else:
                row.next_attempt_at = now + timedelta(
                    seconds=config.RECONCILE_INTERVAL_SECONDS
                )

    session.commit()


def send_queued(session) -> None:
    """Send queued DMs until we run out of rate-limit budget or work."""
    now = utcnow()
    rows = (
        session.execute(
            select(Outbox)
            .where(Outbox.status == STATUS_QUEUED)
            .where(Outbox.next_attempt_at <= now)
            .order_by(Outbox.next_attempt_at)
            .limit(50)
        )
        .scalars()
        .all()
    )

    for row in rows:
        if not _rate_limiter.can_send():
            break  # No budget right now; try again next tick.

        row.attempts += 1
        _rate_limiter.record_send()
        result = pseudogram.send_dm(
            recipient_user_id=row.recipient_user_id,
            message=row.message,
            comment_id=row.comment_id,
            idempotency_key=row.idempotency_key,
        )

        if result.outcome == "accepted":
            row.dm_id = result.dm_id
            row.status = STATUS_ACCEPTED
            # Give the DM a moment before the first reconcile poll.
            row.next_attempt_at = utcnow() + timedelta(
                seconds=config.RECONCILE_INTERVAL_SECONDS
            )

        elif result.outcome == "rate_limited":
            # We guessed wrong on budget; honour their Retry-After and try
            # this row again after the back-off. Not counted as an attempt
            # failure since the request never really ran.
            row.attempts -= 1
            wait = result.retry_after or 5.0
            _rate_limiter.block_for(wait)
            row.next_attempt_at = utcnow() + timedelta(seconds=wait)
            break  # Stop sending this tick; we're being throttled.

        elif result.outcome == "server_error" or result.outcome == "network_error":
            if row.attempts < config.MAX_SEND_ATTEMPTS:
                row.next_attempt_at = utcnow() + timedelta(
                    seconds=_backoff_seconds(row.attempts)
                )
            else:
                row.status = STATUS_FAILED

        elif result.outcome == "bad_request":
            # Malformed and unfixable by retrying.
            row.status = STATUS_FAILED

    session.commit()


def run_once() -> None:
    """One full tick. Kept public so tests can drive it deterministically."""
    session = SessionLocal()
    try:
        reconcile_accepted(session)
        send_queued(session)
    finally:
        session.close()


def _loop(stop_event: threading.Event) -> None:
    while not stop_event.is_set():
        try:
            run_once()
        except Exception as exc:  # never let the worker thread die
            print(f"[worker] tick error: {exc}", flush=True)
        stop_event.wait(1.0)  # ~1 tick per second


def start_worker() -> tuple[threading.Thread, threading.Event]:
    """Start the background loop as a daemon thread."""
    stop_event = threading.Event()
    thread = threading.Thread(target=_loop, args=(stop_event,), daemon=True)
    thread.start()
    return thread, stop_event
