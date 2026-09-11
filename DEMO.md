# 60-second demo — Realtime event platform

Goal: show **persist first, then live**. The inbox row is the source of truth. SSE is catch-up plus a wake, not a buffer of unsaved messages.

Start API and worker:

```bash
export DATABASE_URL=postgresql://workflow:workflow@127.0.0.1:5432/notify
export DEMO_MODE=true
python -m notify_platform.api       # terminal 1, http://127.0.0.1:43200
python -m notify_platform.worker    # terminal 2
```

## Script

```bash
# 1. Publish (idempotent). Recipients are explicit.
notify publish --key demo-1 --to u1,u2 --payload '{"order":"1"}'
notify inbox --user u1
# Expect: one row for u1. Repeat the same --key → created=false, still one row.

# 2. Mute then publish another type-key
notify mute --user u1 --type order.paid
notify publish --key demo-2 --to u1,u2 --payload '{"order":"2"}'
notify inbox --user u1
# u1 should not gain a new order.paid row; u2 still should.

# 3. Reconnect story (console or curl)
# Open / and connect SSE for tenant=acme user=u2, or:
#   curl -N "http://127.0.0.1:43200/stream?tenant=acme&user_id=u2&cursor=0"
# Publish demo-3. Kill the API process. Start API again.
# Reconnect with cursor=<last inbox id>. The durable row is still in GET /inbox.
```

Poison / slow webhook: set a tenant endpoint to `fake://fail` or `fake://slow` via the console and watch deliveries dead-letter or not block the other slot. Automated: `pytest drills/ -q`.

V2 chunked fan-out: publish more than `FANOUT_CHUNK` (default 200) recipients, or lower the setting in `.env`. Workers `continue_fanout` until `fanout_status=complete`. Partial inboxes before that are expected.

## Recording a GIF (optional)

Record publish + inbox JSON, then a browser SSE panel reconnect after restarting the API. Do not invent “cache hit” output — this repo has no cache. The clip should show the inbox id used as cursor.
