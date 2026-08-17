"""
Central configuration, read from environment variables.

Everything the app needs to talk to the mock PseudoGram API and to
persist state lives here so there is exactly one place to look.
"""
import os


# Your PseudoGram API key. Used for TWO things:
#   1) as the X-API-Key header on every call we make to the mock API
#   2) as the HMAC secret to verify incoming webhook signatures
API_KEY = os.environ.get("PSEUDOGRAM_API_KEY", "")

# Base URL of the mock API we send DMs through.
BASE_URL = os.environ.get(
    "PSEUDOGRAM_BASE_URL", "https://pseudogram-api.onrender.com"
).rstrip("/")

# Where to store our state. Defaults to a local SQLite file so the app
# runs with zero setup. On Render we set DATABASE_URL to a Postgres URL
# so state survives restarts (a free web instance has an ephemeral disk,
# which would wipe a SQLite file on every redeploy/restart).
DATABASE_URL = os.environ.get("DATABASE_URL", "sqlite:///./linkplease.db")

# SQLAlchemy wants "postgresql+psycopg://", but Render hands out
# "postgres://". Normalise it so either form works.
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+psycopg://", 1)
elif DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = DATABASE_URL.replace(
        "postgresql://", "postgresql+psycopg://", 1
    )

# Turn webhook signature verification on/off. On by default (Part B).
# We keep a switch so you can send unsigned test traffic during local dev.
VERIFY_SIGNATURE = os.environ.get("VERIFY_SIGNATURE", "true").lower() == "true"

# --- Sending / retry behaviour -------------------------------------------

# The mock API allows 10 POST /v1/dm/send calls per rolling 60 seconds.
# We stay one under to leave headroom for clock skew.
RATE_LIMIT_MAX = int(os.environ.get("RATE_LIMIT_MAX", "9"))
RATE_LIMIT_WINDOW_SECONDS = 60

# How many times we try to get a DM delivered before giving up ("failed").
MAX_SEND_ATTEMPTS = int(os.environ.get("MAX_SEND_ATTEMPTS", "6"))

# How many times we poll a 202-accepted DM's status before giving up.
MAX_RECONCILE_ATTEMPTS = int(os.environ.get("MAX_RECONCILE_ATTEMPTS", "40"))

# Seconds between reconciliation polls of an accepted-but-not-yet-terminal DM.
RECONCILE_INTERVAL_SECONDS = int(os.environ.get("RECONCILE_INTERVAL_SECONDS", "4"))

# HTTP timeout for calls to the mock API.
HTTP_TIMEOUT_SECONDS = 10
