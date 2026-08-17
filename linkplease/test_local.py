"""
Local self-test. Runs the whole app in-process against a FAKE mock API,
so it needs no network and no API key. It exercises every correctness
guarantee the assignment cares about:

  - duplicate event_id is ignored (redelivery dedup)
  - same user + same rule is only DMed once (duplicate blocked)
  - a non-matching comment produces no DM
  - comment.deleted BEFORE the create => no DM
  - comment.deleted AFTER enqueue but before send => DM canceled
  - forged signature => 401
  - only API-confirmed 'delivered' DMs count as `sent`

Run:  python test_local.py
"""
import hashlib
import hmac
import json
import os
import threading

# --- Configure the app BEFORE importing it (config reads env at import) ---
API_KEY = "test_secret_key"
os.environ["PSEUDOGRAM_API_KEY"] = API_KEY
os.environ["VERIFY_SIGNATURE"] = "true"
os.environ["DATABASE_URL"] = "sqlite:///./test_local.db"
os.environ["RECONCILE_INTERVAL_SECONDS"] = "0"  # reconcile immediately in tests

# Start from a clean database each run.
for suffix in ("", "-wal", "-shm"):
    try:
        os.remove(f"./test_local.db{suffix}")
    except FileNotFoundError:
        pass

from app import pseudogram, worker, main  # noqa: E402


# --- Fake the mock API so the test is deterministic and offline ---------
class FakeAPI:
    def __init__(self):
        self.by_key = {}       # idempotency_key -> dm_id  (proves idempotency)
        self.status = {}       # dm_id -> status
        self._n = 0

    def send_dm(self, recipient_user_id, message, comment_id, idempotency_key):
        if idempotency_key in self.by_key:
            dm_id = self.by_key[idempotency_key]  # idempotent replay
        else:
            self._n += 1
            dm_id = f"dm_fake_{self._n}"
            self.by_key[idempotency_key] = dm_id
            self.status[dm_id] = "delivered"  # this fake always delivers
        return pseudogram.SendResult(outcome="accepted", dm_id=dm_id)

    def get_dm_status(self, dm_id):
        return pseudogram.StatusResult(
            outcome="ok", status=self.status.get(dm_id, "queued")
        )


fake = FakeAPI()
pseudogram.send_dm = fake.send_dm
pseudogram.get_dm_status = fake.get_dm_status
# Don't start the real background thread; we drive ticks by hand.
worker.start_worker = lambda: (None, threading.Event())

from fastapi.testclient import TestClient  # noqa: E402

client = TestClient(main.app)


def post_webhook(payload, *, key=API_KEY):
    raw = json.dumps(payload).encode("utf-8")
    sig = "sha256=" + hmac.new(key.encode(), raw, hashlib.sha256).hexdigest()
    return client.post(
        "/webhook",
        content=raw,
        headers={
            "X-PseudoGram-Signature": sig,
            "Content-Type": "application/json",
        },
    )


def created(event_id, comment_id, user_id, text):
    return {
        "event_id": event_id,
        "event_type": "comment.created",
        "sent_at": "2026-08-17T09:00:00.000Z",
        "data": {
            "comment_id": comment_id,
            "post_id": "post_1",
            "text": text,
            "created_at": "2026-08-17T09:00:00.000Z",
            "from": {"user_id": user_id, "username": user_id + "_name"},
        },
    }


def deleted(event_id, comment_id):
    return {
        "event_id": event_id,
        "event_type": "comment.deleted",
        "sent_at": "2026-08-17T09:00:00.000Z",
        "data": {"comment_id": comment_id},
    }


def check(name, cond):
    print(("PASS " if cond else "FAIL ") + name)
    assert cond, name


with client:  # triggers startup (init_db)
    # Create the rule.
    r = client.post("/rules", json={"keyword": "PRICE", "dm_message": "Price list!"})
    check("POST /rules returns 201", r.status_code == 201)
    check("rule_id present", "rule_id" in r.json())

    # User A comments PRICE -> should enqueue one DM.
    check("A first comment 200", post_webhook(
        created("evt_A1", "cmt_A1", "usr_A", "PRICE pls 🙏")).status_code == 200)

    # Exact same event redelivered -> duplicate, no second DM.
    dup = post_webhook(created("evt_A1", "cmt_A1", "usr_A", "PRICE pls 🙏"))
    check("redelivered event flagged duplicate", dup.json().get("status") == "duplicate")

    # User A comments again (new event, new comment) -> duplicate BLOCKED.
    check("A second comment 200", post_webhook(
        created("evt_A2", "cmt_A2", "usr_A", "what's the PRICE")).status_code == 200)

    # User B comments with no keyword -> no DM.
    post_webhook(created("evt_B1", "cmt_B1", "usr_B", "love this post"))

    # Case-insensitive + matches anywhere: "price" lowercase mid-sentence.
    post_webhook(created("evt_E1", "cmt_E1", "usr_E", "the price?"))

    # Delete-before-create: deletion arrives first for cmt_C.
    post_webhook(deleted("evt_Cdel", "cmt_C"))
    post_webhook(created("evt_C1", "cmt_C", "usr_C", "PRICE"))  # should be skipped

    # Delete-after-enqueue: create then delete before the worker sends.
    post_webhook(created("evt_D1", "cmt_D", "usr_D", "PRICE"))
    post_webhook(deleted("evt_Ddel", "cmt_D"))  # cancels the queued DM

    # Forged signature -> 401 (Part B).
    forged = post_webhook(created("evt_X", "cmt_X", "usr_X", "PRICE"), key="wrong_key")
    check("forged signature rejected 401", forged.status_code == 401)

    # Drive the worker a few ticks: send, then reconcile to 'delivered'.
    for _ in range(5):
        worker.run_once()

    s = client.get("/stats").json()
    print("STATS:", s)

    # A and E should be delivered (2 sent). C skipped, D canceled, B no match.
    check("sent == 2 (A + E, confirmed delivered)", s["sent"] == 2)
    check("failed == 0", s["failed"] == 0)
    check("queued == 0", s["queued"] == 0)
    check("duplicates_blocked == 1 (A's second comment)",
          s["duplicates_blocked"] == 1)

print("\nAll checks passed ✅")
