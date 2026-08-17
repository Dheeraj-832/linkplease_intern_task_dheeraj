"""
Database layer: SQLAlchemy engine, session factory, and models.

Design note: ALL the correctness guarantees the assignment asks for are
enforced by database constraints, not by application-level "if exists"
checks. That is deliberate. Two webhook deliveries of the same event can
race each other; two comments from the same user can race each other.
A check-then-insert in Python has a window between the check and the
write where both racers pass. A UNIQUE constraint does the check and the
write as one atomic operation, so exactly one racer wins and the other
gets an IntegrityError we can catch. That is the whole trick.
"""
from datetime import datetime, timezone

from sqlalchemy import (
    create_engine,
    String,
    Integer,
    Text,
    DateTime,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    mapped_column,
    sessionmaker,
)

from . import config


def utcnow() -> datetime:
    """Timezone-aware UTC now. Used for all timestamps."""
    return datetime.now(timezone.utc)


# SQLite needs check_same_thread=False because the FastAPI request threads
# and the background worker thread all touch the same connection pool.
connect_args = {}
engine_kwargs = {"pool_pre_ping": True}
if config.DATABASE_URL.startswith("sqlite"):
    connect_args = {"check_same_thread": False, "timeout": 30}

engine = create_engine(
    config.DATABASE_URL,
    connect_args=connect_args,
    **engine_kwargs,
)

# WAL mode lets readers and one writer work concurrently on SQLite, which
# matters when 500 events arrive in 10 seconds. No effect on Postgres.
if config.DATABASE_URL.startswith("sqlite"):
    from sqlalchemy import event

    @event.listens_for(engine, "connect")
    def _set_sqlite_pragma(dbapi_conn, _):
        cur = dbapi_conn.cursor()
        cur.execute("PRAGMA journal_mode=WAL")
        cur.execute("PRAGMA busy_timeout=30000")
        cur.close()


SessionLocal = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)


class Base(DeclarativeBase):
    pass


class Rule(Base):
    """A creator's automation: when `keyword` appears, DM `dm_message`."""

    __tablename__ = "rules"

    rule_id: Mapped[str] = mapped_column(String, primary_key=True)
    keyword: Mapped[str] = mapped_column(String, nullable=False)
    dm_message: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)


class ProcessedEvent(Base):
    """
    One row per webhook event_id we have already handled.

    The mock API redelivers ~8% of events (same event_id). We INSERT the
    event_id here with the PK doing the dedup; a duplicate delivery hits
    the primary-key constraint and we skip it. Because it is a DB write,
    two near-simultaneous duplicates can't both "pass" — one wins.
    """

    __tablename__ = "processed_events"

    event_id: Mapped[str] = mapped_column(String, primary_key=True)
    received_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)


class DeletedComment(Base):
    """
    Comments we were told were deleted.

    Needed because events arrive out of order: a comment.deleted can show
    up BEFORE the comment.created it refers to. We record the deletion so
    that when the (late) created event arrives, we know not to DM.
    """

    __tablename__ = "deleted_comments"

    comment_id: Mapped[str] = mapped_column(String, primary_key=True)
    received_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)


# DM lifecycle states.
#   queued    -> waiting to be sent (or waiting for a retry)
#   accepted  -> mock API returned 202; NOT delivered yet, awaiting reconcile
#   sent      -> mock API confirmed status == delivered (this is what /stats counts)
#   failed    -> we gave up after retries, or the payload was rejected
#   canceled  -> the comment was deleted before we sent, so we didn't send
STATUS_QUEUED = "queued"
STATUS_ACCEPTED = "accepted"
STATUS_SENT = "sent"
STATUS_FAILED = "failed"
STATUS_CANCELED = "canceled"


class Outbox(Base):
    """
    One row per DM we intend to send. This is our durable to-do list.

    The UNIQUE(rule_id, recipient_user_id) constraint is the single most
    important line in this file: it guarantees the same user is never
    DMed twice for the same rule, no matter how many times they comment
    or how many duplicate events arrive. When the insert fails, we count
    a "duplicate blocked" instead of enqueuing a second DM.
    """

    __tablename__ = "outbox"
    __table_args__ = (
        UniqueConstraint("rule_id", "recipient_user_id", name="uq_rule_recipient"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    rule_id: Mapped[str] = mapped_column(String, nullable=False)
    recipient_user_id: Mapped[str] = mapped_column(String, nullable=False)
    comment_id: Mapped[str] = mapped_column(String, nullable=False)
    message: Mapped[str] = mapped_column(Text, nullable=False)

    # Stable per-DM key reused across every retry, so retrying a request
    # that actually went through cannot create a second DM. Sent to the
    # mock API as the Idempotency-Key header.
    idempotency_key: Mapped[str] = mapped_column(String, unique=True, nullable=False)

    # The mock API's id for this DM, learned from the 202 response.
    dm_id: Mapped[str | None] = mapped_column(String, nullable=True)

    status: Mapped[str] = mapped_column(String, default=STATUS_QUEUED, index=True)

    # How many times we have POSTed /v1/dm/send for this row.
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    # How many times we have polled GET /v1/dm/{id} for this row.
    reconcile_attempts: Mapped[int] = mapped_column(Integer, default=0)

    # The next moment the worker should act on this row (send or reconcile).
    next_attempt_at: Mapped[datetime] = mapped_column(
        DateTime, default=utcnow, index=True
    )

    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=utcnow, onupdate=utcnow
    )


class Counter(Base):
    """
    Simple named integer counters that must survive restarts.

    We use it for `duplicates_blocked`, which is not derivable from the
    outbox table (a blocked duplicate leaves no row), so it must be
    incremented and persisted explicitly.
    """

    __tablename__ = "counters"

    name: Mapped[str] = mapped_column(String, primary_key=True)
    value: Mapped[int] = mapped_column(Integer, default=0)


def init_db() -> None:
    """Create tables if they don't exist. Safe to call on every startup."""
    Base.metadata.create_all(engine)


def bump_counter(session, name: str, amount: int = 1) -> None:
    """Increment a named counter, creating it at `amount` if missing."""
    row = session.get(Counter, name)
    if row is None:
        session.add(Counter(name=name, value=amount))
    else:
        row.value += amount


def get_counter(session, name: str) -> int:
    row = session.get(Counter, name)
    return row.value if row else 0
