# Architecture

```text
client POST /events → Postgres (event + fanout + inbox + deliveries) → NOTIFY
                         ↑                                ↓
              SSE /stream (cursor replay)          worker processes (SKIP LOCKED)
```

Inbox id is monotonic per insert. Catch-up is `id > cursor`. Replay capped at `REPLAY_CAP`; beyond that the stream emits `cursor_expired` and points at `/inbox`.

Webhook workers are a separate pool from the API. A slow `fake://slow` destination holds one lease; other deliveries proceed.
