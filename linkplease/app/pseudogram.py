"""
Thin client for the mock PseudoGram API.

Each function does exactly one HTTP call and returns a small, explicit
result object. Interpreting those results (retry? give up? back off?) is
the worker's job, not this file's job — keeping the two separate makes
both easy to reason about.
"""
from dataclasses import dataclass

import requests

from . import config


@dataclass
class SendResult:
    """Outcome of a single POST /v1/dm/send call."""

    # One of: "accepted", "rate_limited", "server_error",
    #         "bad_request", "network_error"
    outcome: str
    dm_id: str | None = None
    retry_after: float | None = None  # seconds, from the 429 Retry-After header
    detail: str | None = None


def _headers(idempotency_key: str | None = None) -> dict:
    headers = {
        "X-API-Key": config.API_KEY,
        "Content-Type": "application/json",
    }
    if idempotency_key:
        # Same key on every retry => the mock API returns the original
        # dm_id instead of sending a second DM.
        headers["Idempotency-Key"] = idempotency_key
    return headers


def send_dm(
    recipient_user_id: str,
    message: str,
    comment_id: str,
    idempotency_key: str,
) -> SendResult:
    """POST a DM. Never raises; always returns a SendResult."""
    url = f"{config.BASE_URL}/v1/dm/send"
    payload = {
        "recipient_user_id": recipient_user_id,
        "message": message,
        "comment_id": comment_id,
    }
    try:
        resp = requests.post(
            url,
            json=payload,
            headers=_headers(idempotency_key),
            timeout=config.HTTP_TIMEOUT_SECONDS,
        )
    except requests.RequestException as exc:
        # Timeout / connection reset / DNS, etc. The DM may or may not have
        # been created; the idempotency key makes a retry safe either way.
        return SendResult(outcome="network_error", detail=str(exc))

    if resp.status_code == 202:
        dm_id = None
        try:
            dm_id = resp.json().get("dm_id")
        except ValueError:
            pass
        return SendResult(outcome="accepted", dm_id=dm_id)

    if resp.status_code == 429:
        retry_after = _parse_retry_after(resp)
        return SendResult(outcome="rate_limited", retry_after=retry_after)

    if resp.status_code == 400:
        # Malformed payload. Retrying will not help — this is terminal.
        return SendResult(outcome="bad_request", detail=resp.text[:500])

    if resp.status_code >= 500:
        # Random ~20% internal errors. Safe to retry.
        return SendResult(outcome="server_error", detail=resp.text[:500])

    # Anything unexpected: treat like a server error and let retries handle it.
    return SendResult(
        outcome="server_error", detail=f"HTTP {resp.status_code}: {resp.text[:300]}"
    )


def _parse_retry_after(resp: requests.Response) -> float:
    """Read Retry-After (seconds). Fall back to a sane default."""
    raw = resp.headers.get("Retry-After")
    if raw:
        try:
            return float(raw)
        except ValueError:
            pass
    return 5.0


@dataclass
class StatusResult:
    """Outcome of a GET /v1/dm/{dm_id} call."""

    # "ok" if we got a status back, "error" if the call itself failed.
    outcome: str
    status: str | None = None  # queued | delivered | failed
    detail: str | None = None


def get_dm_status(dm_id: str) -> StatusResult:
    """GET a DM's current status. Reads do NOT count against the rate limit."""
    url = f"{config.BASE_URL}/v1/dm/{dm_id}"
    try:
        resp = requests.get(
            url,
            headers=_headers(),
            timeout=config.HTTP_TIMEOUT_SECONDS,
        )
    except requests.RequestException as exc:
        return StatusResult(outcome="error", detail=str(exc))

    if resp.status_code == 200:
        try:
            return StatusResult(outcome="ok", status=resp.json().get("status"))
        except ValueError:
            return StatusResult(outcome="error", detail="bad json")

    return StatusResult(outcome="error", detail=f"HTTP {resp.status_code}")
