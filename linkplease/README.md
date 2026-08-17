# LinkPlease — comment → DM automation

Someone comments a keyword on a post; we DM them the matching message,
exactly once, without losing DMs when the API fails, rate-limits, or
delivers events out of order and twice.

Built with **FastAPI + Postgres**. One web process runs the API *and* a
background worker thread that does all the slow, failing network work.

## How it works (30-second tour)

- **`POST /webhook`** does almost nothing on the request path: it verifies
  the HMAC signature, drops duplicate `event_id`s (a primary-key insert, so
  the check and the write are atomic), matches the comment text against
  rules, and writes one row per DM into an `outbox` table. Then it returns
  `200`. No DM is sent here — that's why it always answers within 5s.
- **The background worker** (in `app/worker.py`) is the only thing that
  calls the DM endpoints. Each second it: reconciles DMs the API "accepted"
  (202) by polling their real status, then sends queued DMs while respecting
  a 9-per-60s rate limit and any `Retry-After` back-off.
- **Correctness comes from DB constraints, not Python `if` checks.**
  `UNIQUE(event_id)` kills redelivered events; `UNIQUE(rule_id,
  recipient_user_id)` guarantees one DM per person per rule; a stable
  `Idempotency-Key` per DM makes every retry safe.
- **`sent` only counts DMs the API confirmed as `delivered`** (via the
  reconcile poll), never the 202-accepted ones — a 202 means *accepted*, and
  ~15% of those still fail.

Parts done: **A + B + C** (dedup, no-lost-DMs, signature verification, live
`/stats`, delivery reconciliation with retry, and `comment.deleted`
handling including the out-of-order case).

## 1. Get your API key

```bash
# Apply (once)
curl -X POST https://pseudogram-api.onrender.com/v1/apply \
  -H 'Content-Type: application/json' \
  -d '{"name":"YOUR NAME","email":"you@example.com","phone":"+91...","linkedin_url":"https://linkedin.com/in/you"}'

# Get your key (same email)
curl -X POST https://pseudogram-api.onrender.com/v1/keygen \
  -H 'Content-Type: application/json' \
  -d '{"email":"you@example.com"}'
# -> {"api_key":"...","email":"..."}
```

Keep that `api_key`.

## 2. Run it locally

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env          # then paste your api_key into .env
export $(grep -v '^#' .env | xargs)   # load env (or use a dotenv runner)
uvicorn app.main:app --reload --port 8000
```

Check it:

```bash
curl localhost:8000/
curl -X POST localhost:8000/rules -H 'Content-Type: application/json' \
  -d '{"keyword":"PRICE","dm_message":"Here is the price list: ..."}'
curl localhost:8000/stats
```

Run the tests (offline, no key needed):

```bash
python test_local.py    # end-to-end: dedup, dup-block, deletes, signature
python test_worker.py   # retries, give-up, reconcile-retry, rate limiting
```

## 3. Deploy to Render (free, stays live 7 days, no card)

1. Push this repo to a **public GitHub repo**.
2. In Render: **New + → Blueprint**, select your repo. Render reads
   `render.yaml` and creates the web service **and a free Postgres**.
3. When prompted, set **`PSEUDOGRAM_API_KEY`** to your key. (Everything else
   is already wired: `DATABASE_URL` comes from the Postgres, and
   `VERIFY_SIGNATURE=true`.)
4. Deploy. Your base URL is `https://<your-service>.onrender.com`. Your
   webhook is that URL + `/webhook`.

> Free instances sleep after 15 min idle and cold-start in ~1 min. Before
> you submit, keep it warm with a free pinger (cron-job.org / UptimeRobot)
> hitting `/stats` every ~10 minutes.

## 4. Test yourself against the real grader data

```bash
# Fire 500 events at your deployed webhook
curl -X POST https://pseudogram-api.onrender.com/v1/simulate/start \
  -H 'X-API-Key: YOUR_KEY' -H 'Content-Type: application/json' \
  -d '{"webhook_url":"https://<you>.onrender.com/webhook","count":500,"duration_seconds":10}'
# -> {"run_id":"..."}

# Wait ~1-2 min for retries + reconciliation to settle, then:
curl https://<you>.onrender.com/stats

# Compare against ground truth
curl -H 'X-API-Key: YOUR_KEY' \
  https://pseudogram-api.onrender.com/v1/simulate/<run_id>/truth
```

Your `sent + duplicates_blocked` should line up with the matched-users
truth, `queued` should drain to 0 once reconciliation finishes, and numbers
should never be *higher* than truth.

## 5. Submit

```bash
curl -X POST https://pseudogram-api.onrender.com/v1/submit \
  -H 'Content-Type: application/json' \
  -d '{
    "email":"you@example.com",
    "github_repo":"https://github.com/you/linkplease",
    "working_url":"https://<you>.onrender.com",
    "loom_url":"https://loom.com/share/...",
    "parts_completed":"A+B+C",
    "start_date":"2026-08-17"
  }'
```

You can resubmit; the same email overwrites. Send an early draft, then the
final.

## Config (env vars)

| Var | Default | Meaning |
|---|---|---|
| `PSEUDOGRAM_API_KEY` | — | your key; used as X-API-Key and HMAC secret |
| `DATABASE_URL` | sqlite file | Postgres URL on Render |
| `VERIFY_SIGNATURE` | `true` | reject forged webhooks |
| `RATE_LIMIT_MAX` | `9` | sends per rolling 60s (one under the limit) |
| `MAX_SEND_ATTEMPTS` | `6` | retries before marking `failed` |

See `FAILURES.md` for the honest list of remaining ways this can be wrong.
