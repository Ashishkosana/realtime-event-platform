# Realtime event platform

A **durable notification pipeline**: persist the event first, fan out to a per-user inbox, catch up over SSE with a cursor, deliver webhooks at-least-once. Not the workflow engine. Not the AI control plane.

**Stack:** Python 3.12 · FastAPI · PostgreSQL 16 · psycopg 3 · SSE (not WebSocket)

**Problems this repo actually solves:** writing a socket in the same request as the row (process dies → live client never sees it, no cursor); all-or-nothing SQL for 10k recipients; mute racing an insert; a slow webhook stalling email; reconnecting without replaying the whole history.

**Why it is interesting:** the live path is a **query plus a wake**, not a socket buffer. Inbox id is the cursor. Delivery is at-least-once; the event id is what consumers must dedup. There is no Redis cache in this system — Postgres is the log.

**Guarantee: at-least-once delivery** of inbox items and webhooks. Not exactly-once.

## How an event moves

```mermaid
flowchart TB
  Client -->|POST /events idempotency key + recipients| API
  API --> PG[(PostgreSQL)]
  PG --> Events[ntf_events]
  PG --> Fanout[ntf_fanout]
  PG --> Inbox[ntf_inbox]
  PG --> Deliv[ntf_deliveries]
  Browser -->|GET /stream?cursor=| SSE
  SSE -->|catch-up id greater than cursor, then poll| Inbox
  Worker -->|SKIP LOCKED deliveries and fan-out chunks| PG
  API -->|NOTIFY inbox_wake| Worker
  Worker -->|HMAC webhook / fake email| Dest[destinations]
```

Small recipient lists finish inbox rows in the publish transaction (`FANOUT_CHUNK`, default 200). Larger lists stay `fanout_status=pending` and workers apply chunks with `SKIP LOCKED` (V2). Kill mid-fan-out resumes. Partial inboxes until `fanout_status=complete` are documented, not hidden.

SSE live tail is **poll** (`POLL_INTERVAL_SECONDS`). `NOTIFY inbox_wake` wakes **workers** (`WAKE_MODE=listen`); it is not the browser transport. There is **no application cache**. SSE sets `Cache-Control: no-cache` so intermediaries do not buffer the stream. Consistency is “what is in `ntf_inbox`.”

```mermaid
sequenceDiagram
  participant C as Publisher
  participant A as API
  participant P as Postgres
  participant S as SSE client
  participant W as Worker
  C->>A: POST /events
  A->>P: INSERT event unique tenant+type+key
  A->>P: fan-out rows / inbox + deliveries
  A->>P: NOTIFY inbox_wake
  S->>P: SELECT inbox WHERE id > cursor
  loop live tail
    S->>S: sleep poll interval
    S->>P: SELECT id > last
  end
  W->>P: claim delivery SKIP LOCKED
  W->>W: webhook HMAC or fake email
```

## What it does

- `POST /events` with tenant, type, payload, **idempotency key**, explicit recipients
- Persist the event once; write-time fan-out into a per-user inbox
- Preferences: mute an `event_type` before inbox insert (a mute after insert **may** deliver one extra)
- **SSE** `/stream?tenant&user_id&cursor` — catch-up `id > cursor`, then wait; `REPLAY_CAP` (500) → `cursor_expired` and `/inbox`
- Webhook + fake-email workers: leases, backoff, dead-letter, HMAC `x-notify-signature`
- Fake destinations `fake://ok|fail|slow` for drills
- V2 resumable chunked fan-out when `len(recipients) > FANOUT_CHUNK`

## 60-second demo

Publish → inbox row exists even if SSE was down → reconnect with cursor. Mute and poison webhook. Commands: [DEMO.md](DEMO.md).

## Run locally

Python 3.12, PostgreSQL 16, dedicated database `notify` (not the workflow or AICP database).

```bash
python3.12 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
cp .env.example .env
docker compose up -d postgres   # if you have Docker

export DATABASE_URL=postgresql://workflow:workflow@127.0.0.1:5432/notify
export DEMO_MODE=true
python -m notify_platform.api      # http://127.0.0.1:43200
python -m notify_platform.worker   # another terminal
```

```bash
notify publish --key k1 --to u1,u2 --payload '{"order":"1"}'
notify inbox --user u1
notify mute --user u1 --type order.paid
```

Open `/` for the engineering console. Connect SSE, publish, kill the API, reconnect with the last inbox id — the missed item is in the inbox, not in a socket buffer.

CLI `notify` talks to the API; the worker process is required for webhook/email and for V2 chunked fan-out.

## Tests and CI

```bash
export TEST_DATABASE_URL=postgresql://workflow:workflow@127.0.0.1:5432/notify_test
pytest -q
ruff check src tests drills scripts
ruff format --check src tests drills scripts
```

GitHub Actions: `.github/workflows/ci.yml` (ruff, format, pytest) with Postgres 16.

## Benchmarks

Measured on this agent VM. **Not** a capacity rating. [BENCHMARKS.md](BENCHMARKS.md):

| Bench | Result |
| --- | --- |
| 50 publishes, 1 recipient | **449 publishes/s**, 0.11 s total |
| 1 publish × 200 recipients (sync path, `FANOUT_CHUNK=200`) | **0.15 s** to durable inbox |

Kafka was not added.

## Docs

| File | What it is |
| --- | --- |
| [DEMO.md](DEMO.md) | Publish, inbox, mute, reconnect |
| [ARCHITECTURE.md](ARCHITECTURE.md) | Tables, SSE, workers |
| [DECISIONS.md](DECISIONS.md) | SSE vs WebSocket, mute race, why not Kafka |
| [FAILURE_DRILLS.md](FAILURE_DRILLS.md) | Duplicate key, mute, poison, slow webhook, chunked fan-out |
| [INTERVIEW_GUIDE.md](INTERVIEW_GUIDE.md) | How to defend at-least-once + cursors |
| [SECURITY.md](SECURITY.md) | No auth; webhook HMAC |
| [V2_PROPOSAL.md](V2_PROPOSAL.md) | Chunked fan-out (implemented) |

## Limitations

- At-least-once, not exactly-once. Consumers must treat event id as the dedup key.
- Mute-after-insert may deliver one extra notification (deliberate: locking prefs on every publish would serialize a hot user).
- No Redis, no Kafka, no WebSocket, no APNs, no push coalescing.
- No authentication on the HTTP adapter.
- SSE replay is capped; old cursors get `cursor_expired`, not an infinite log.
- Partial inbox during V2 fan-out until `fanout_status=complete`.
