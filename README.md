# Real-time event and notification platform

This repository is **Project 3 only**. It is not the workflow engine and not the AI control plane.

Products need to tell a person or another system that something happened. The naive design writes a row and also writes a socket in the same request handler. If the process dies after the row commits, the live client never sees it and there is no cursor to catch up.

**Guarantee: at-least-once delivery** of inbox items and webhooks. The event id is the idempotency token for consumers. This system does not claim exactly-once.

## What it does

- `POST /events` with tenant, type, payload, **idempotency key**, explicit recipients
- Persist the event once; write-time fan-out into a per-user inbox (same transaction when the list fits `FANOUT_CHUNK`)
- Preferences: mute an `event_type` before inbox insert
- **SSE** live stream with `cursor` / `Last-Event-ID` catch-up, then poll/listen for new rows
- Webhook + fake-email workers with leases, backoff, dead-letter, HMAC signatures
- V2: **resumable chunked fan-out** when recipient count exceeds `FANOUT_CHUNK`

## Run locally

Python 3.12, PostgreSQL 16, dedicated database `notify`.

```bash
python3.12 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
createdb notify   # user workflow / workflow in this lab

export DATABASE_URL=postgresql://workflow:workflow@127.0.0.1:5432/notify
export DEMO_MODE=true
python -m notify_platform.api      # http://127.0.0.1:43200
python -m notify_platform.worker   # another terminal
```

```bash
notify publish --key k1 --to u1,u2 --payload '{"order":"1"}'
notify inbox --user u1
```

Open `/` for the engineering console. Connect SSE, kill the API, reconnect with the last inbox id — the missed item is in the inbox, not in a socket buffer.

## Remotes

Own git history. Push to Origin **and** GitHub when those remotes exist (`origin` + `github`). This agent cannot create GitHub.com repos without `gh auth login`.
