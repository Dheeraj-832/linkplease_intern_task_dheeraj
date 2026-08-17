"""
Focused tests for the worker's failure handling (retries, giving up,
reconcile-then-retry, and rate limiting). Offline, deterministic.

Run:  python test_worker.py
"""
import os
import threading

os.environ["PSEUDOGRAM_API_KEY"] = "k"
os.environ["VERIFY_SIGNATURE"] = "false"
os.environ["DATABASE_URL"] = "sqlite:///./test_worker.db"
os.environ["RECONCILE_INTERVAL_SECONDS"] = "0"
os.environ["MAX_SEND_ATTEMPTS"] = "4"
# High global limit so scenarios 1-3 aren't throttled; scenario 4 installs
# its own small limiter to test rate limiting in isolation.
os.environ["RATE_LIMIT_MAX"] = "100"

for suffix in ("", "-wal", "-shm"):
    try:
        os.remove(f"./test_worker.db{suffix}")
    except FileNotFoundError:
        pass

from app import pseudogram, worker  # noqa: E402
from app.db import (  # noqa: E402
    SessionLocal, init_db, Outbox, utcnow,
    STATUS_QUEUED, STATUS_ACCEPTED, STATUS_SENT, STATUS_FAILED,
)

init_db()
worker.start_worker = lambda: (None, threading.Event())


def check(name, cond):
    print(("PASS " if cond else "FAIL ") + name)
    assert cond, name


def add_dm(key, user="u", comment="c"):
    s = SessionLocal()
    try:
        row = Outbox(
            rule_id="r", recipient_user_id=user, comment_id=comment,
            message="m", idempotency_key=key, status=STATUS_QUEUED,
            next_attempt_at=utcnow(),
        )
        s.add(row)
        s.commit()
        return row.id
    finally:
        s.close()


def get(row_id):
    s = SessionLocal()
    try:
        return s.get(Outbox, row_id)
    finally:
        s.close()


def force_ready(row_id):
    # Skip back-off waits so the test doesn't sleep.
    s = SessionLocal()
    try:
        r = s.get(Outbox, row_id)
        r.next_attempt_at = utcnow()
        s.commit()
    finally:
        s.close()


# --- Scenario 1: two 500s then accepted, then delivered -> sent ----------
calls = {"send": 0}


def send_flaky(recipient_user_id, message, comment_id, idempotency_key):
    calls["send"] += 1
    if calls["send"] <= 2:
        return pseudogram.SendResult(outcome="server_error")
    return pseudogram.SendResult(outcome="accepted", dm_id="dm_ok")


pseudogram.send_dm = send_flaky
pseudogram.get_dm_status = lambda dm_id: pseudogram.StatusResult(
    outcome="ok", status="delivered"
)

id1 = add_dm("key1")
worker.run_once(); force_ready(id1)   # attempt 1 -> 500
worker.run_once(); force_ready(id1)   # attempt 2 -> 500
worker.run_once()                     # attempt 3 -> accepted
worker.run_once()                     # reconcile -> delivered -> sent
check("flaky DM eventually SENT after retries", get(id1).status == STATUS_SENT)
check("it took 3 send attempts", get(id1).attempts == 3)

# --- Scenario 2: 400 bad_request -> failed immediately -------------------
pseudogram.send_dm = lambda **kw: pseudogram.SendResult(outcome="bad_request")
id2 = add_dm("key2", user="u2")
worker.run_once()
check("bad_request DM is FAILED (no retry)", get(id2).status == STATUS_FAILED)

# --- Scenario 3: accepted, reconcile says failed, retry, then delivered --
state = {"phase": 0}


def send_ok(**kw):
    return pseudogram.SendResult(outcome="accepted", dm_id=f"dm_{state['phase']}")


def status_fail_then_ok(dm_id):
    # First reconcile -> failed; after re-send -> delivered.
    if state["phase"] == 0:
        state["phase"] = 1
        return pseudogram.StatusResult(outcome="ok", status="failed")
    return pseudogram.StatusResult(outcome="ok", status="delivered")


pseudogram.send_dm = send_ok
pseudogram.get_dm_status = status_fail_then_ok
id3 = add_dm("key3", user="u3")
# Call the two stages explicitly so we can observe the intermediate states.
worker.send_queued(SessionLocal())          # send -> accepted
check("accepted after first send", get(id3).status == STATUS_ACCEPTED)
force_ready(id3)
worker.reconcile_accepted(SessionLocal())   # reconcile -> failed -> requeued
check("requeued after downstream failure", get(id3).status == STATUS_QUEUED)
force_ready(id3)
worker.send_queued(SessionLocal())          # send again -> accepted
force_ready(id3)
worker.reconcile_accepted(SessionLocal())   # reconcile -> delivered -> sent
check("recovered to SENT after reconcile-retry", get(id3).status == STATUS_SENT)

# --- Scenario 4: rate limit caps sends per tick --------------------------
pseudogram.send_dm = lambda **kw: pseudogram.SendResult(outcome="accepted", dm_id="d")
pseudogram.get_dm_status = lambda dm_id: pseudogram.StatusResult(
    outcome="ok", status="queued"  # keep them 'accepted' so we can count sends
)
ids = [add_dm(f"rl{i}", user=f"rl{i}") for i in range(10)]
# Fresh limiter to isolate this scenario.
worker._rate_limiter = worker.RateLimiter(3, 60)
worker.send_queued(SessionLocal())
accepted_now = sum(1 for i in ids if get(i).status == STATUS_ACCEPTED)
check("rate limit capped sends at 3 in one window", accepted_now == 3)

print("\nAll worker checks passed ✅")
