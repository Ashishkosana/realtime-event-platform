# Interview guide — Realtime event platform

Defend inbox + cursor, not “we used Kafka.” Point at `plane.py`, `schema.sql`, and `worker.py`.

## 30-second explanation (recruiter)

I built a notification pipeline where the database is the log. You publish an event with an idempotency key and a recipient list. We write a per-user inbox in Postgres, then clients subscribe with SSE using the last inbox id as a cursor. If the API process dies, the row is still there — the live connection is not the store. Webhooks retry with leases. Delivery is at-least-once; consumers dedup on event id.

## 2-minute technical explanation

**Problem.** The naive design writes a row and a socket in one handler. If the process dies after commit, the live client never sees it and there is no catch-up. Huge recipient lists in one transaction do not belong in V1. A mute that races an insert is a real product choice, not a bug you paper over.

**Architecture.** `POST /events` → `ntf_events` unique `(tenant, event_type, idempotency_key)` → `ntf_fanout` rows → inbox + `ntf_deliveries` (webhook if endpoint exists, always fake-email in the lab). SSE `/stream` reads `id > cursor`, caps replay at `REPLAY_CAP`, then **polls**. Workers `LISTEN inbox_wake` (with poll timeout) so delivery/fan-out claims do not always wait a full idle interval. HMAC `x-notify-signature` on real HTTP, backoff, dead-letter. V2: if `len(recipients) > FANOUT_CHUNK`, fan-out stays pending and workers apply chunks.

**Hardest challenge.** Write-time fan-out vs a giant transaction; head-of-line blocking on a slow destination; explaining at-least-once without promising exactly-once.

**Solution.** Chunked fan-out with skip-locked workers; separate delivery leases so `fake://slow` holds one slot; event id as consumer idempotency; mute checked at insert time with an explicit “one extra allowed” race.

**Evidence.** `FAILURE_DRILLS.md`, `drills/test_failure_drills.py`. Benches: 50 publishes **449/s**; 200-recipient sync fan-out **0.15 s** (`BENCHMARKS.md`). Not a capacity rating. Not Kafka.

## Architecture walkthrough

1. Publisher sends tenant, type, payload, idempotency key, explicit recipients (`plane.publish`).
2. Duplicate key → `created: false`, existing event row, no second fan-out.
3. New event: fan-out rows for each user. If N ≤ `FANOUT_CHUNK`, `_apply_fanout_rows` runs in the same transaction (mute → no inbox; else inbox + deliveries). `NOTIFY inbox_wake`.
4. If N is larger, `fanout_status=pending`. Workers `continue_fanout` claim the event with `SKIP LOCKED`, apply a chunk, notify again, until no pending fan-out rows.
5. Live client: `GET /stream?tenant&user_id&cursor` (or `Last-Event-ID`). Catch-up query. If gap > `REPLAY_CAP`, emit `cursor_expired` and point at `/inbox`. Then `asyncio.sleep(POLL_INTERVAL_SECONDS)` and query again. Workers, not SSE, `LISTEN inbox_wake`.
6. Delivery worker: claim pending/expired running delivery, fence, POST webhook or insert `ntf_lab_email`. Failure → jittered `run_after` or `dead_lettered`.

## Important concepts

**Write fan-out vs read fan-out.** We copy into a per-user inbox at publish (or chunked shortly after). Readers do not scan the global event table. Cost is write amplification; gain is simple `id > cursor` per user.

**Transactional outbox lesson.** “DB then socket” drops messages. Here the inbox *is* the outbox. SSE is a reader of the inbox.

**Cursor.** Monotonic `ntf_inbox.id`. Not a Kafka offset spanning partitions. Too-old cursor → `cursor_expired`, not a silent hole.

**At-least-once webhooks.** Retries exist. Receivers must store event id. HMAC authenticates the lab/real HTTP path; it does not dedup.

**Lease + fence on deliveries.** Same idea as Platform Forge, applied to webhook attempts, not workflow steps. Do not blur the two products in an interview — different remotes, different tables.

**Head-of-line.** One `fake://slow` destination holds one claimed row. Other deliveries on other workers/slots proceed. A single-threaded shared queue would stall everyone.

**Mute race.** Mute is checked when the inbox row would be written. Mute after that insert can still deliver once. Locking the preference row on every publish would serialize a hot user. Documented in DECISIONS.md.

**NOTIFY is a worker wake.** SSE does not LISTEN. Replay and live tail are SQL. `WAKE_MODE=listen` is the worker loop.

**No cache.** Do not invent Redis. `Cache-Control: no-cache` on SSE is HTTP hygiene, not an application cache.

## Design decisions

[DECISIONS.md](DECISIONS.md): SSE not WebSocket (catch-up is an HTTP query); Postgres is the log; at-least-once + event id; mute race accepted; V2 chunks rather than Kafka.

## Tradeoffs

| Choice | Alternative | Why here |
| --- | --- | --- |
| SSE | WebSocket | Reconnect + `Last-Event-ID` is boring HTTP; no ping protocol |
| Postgres inbox | Kafka log | One inbox consumer + webhook consumer, modest retention, inspect with SQL |
| Write-time fan-out | Read-time expand | Cursor query stays `id > n` per user |
| Chunked SQL fan-out | Kafka / SQS | Same database; kill mid-fan-out resumes |
| One extra mute delivery | Serializing prefs | Hot-user publish latency |

Kafka becomes interesting with many independent consumers and long retention. That is not this V1 inbox.

## Failure scenarios

| Event | Behavior |
| --- | --- |
| Duplicate idempotency key | One event, no second inbox set |
| API killed after commit | Inbox row remains; SSE reconnects with cursor |
| Cursor older than `REPLAY_CAP` | `cursor_expired`; client uses `/inbox` |
| Mute before fan-out | No inbox row for that user |
| Mute after insert | One extra notification allowed |
| `fake://fail` webhook | Dead-letter after max attempts |
| `fake://slow` | Occupies a lease; other channels continue |
| N > `FANOUT_CHUNK` + worker kill | Chunk resume; partial inbox until complete |
| Missed NOTIFY | SSE/worker poll interval |

## Limitations

- Not exactly-once. Not Kafka. Not a global ordered log for all tenants.
- No auth on the HTTP API.
- No push providers (APNs/FCM). Fake email table is a lab sink.
- Replay is not infinite.
- Benches are 50 publishes / 200 recipients on one VM.

## Interview questions (answers from this tree)

1. **Delivery guarantee?** At-least-once inbox and webhooks. Event id is consumer idempotency. README.

2. **Why SSE not WebSocket?** Catch-up is `SELECT … WHERE id > cursor`. WebSocket would still need that query after reconnect. DECISIONS.md.

3. **What if the process dies after INSERT?** The row is committed. Live clients reconnect. That is the point versus socket-as-store.

4. **How do duplicates get suppressed on publish?** Unique `(tenant, event_type, idempotency_key)` and `ON CONFLICT DO NOTHING`.

5. **Write vs read fan-out?** We write inbox rows at publish (or in V2 chunks). Readers never expand the recipient list.

6. **What is `FANOUT_CHUNK`?** Default 200. ≤ chunk: same transaction. `>`: pending `ntf_fanout` + workers. V2.md.

7. **Partial inbox during V2?** Yes until `fanout_status=complete`. Documented, not hidden.

8. **Mute semantics?** Checked at inbox insert. Race after insert may deliver one extra.

9. **How does SSE catch up?** `inbox_since` `id > cursor`. Cap `REPLAY_CAP=500` → `cursor_expired`.

10. **Is NOTIFY the log?** No. Workers may LISTEN `inbox_wake`. SSE polls. Replay is always `SELECT` on `ntf_inbox`.

11. **Webhook authenticity?** HMAC header `x-notify-signature`. Still at-least-once.

12. **Poison destination?** `fake://fail` → max attempts → `dead_lettered`, not a tight loop.

13. **Slow destination vs email?** Separate delivery rows and skip-locked claims. Email can finish while webhook sleeps.

14. **Why not Kafka in V1?** One inbox consumer, modest N, SQL inspectability. Kafka for many consumer groups / long retention.

15. **Is there a cache?** No. Do not say Redis. SSE `Cache-Control: no-cache` only.

16. **Relation to Platform Forge?** Same Postgres *server* allowed, different database and git remote. Similar lease/fence on *deliveries*, different product.

17. **Idempotency key vs event id?** Key is publisher dedup. Event id is the stable id consumers store.

18. **Head-of-line blocking?** Avoided by not putting all destinations on one in-memory queue. Slow holds a lease.

19. **Auth?** None on HTTP. Bind localhost. SECURITY.md.

20. **What would justify a broker?** Independent consumer groups, retention beyond inbox, or skip-locked fan-out saturating while CPUs are idle — not shown at 200 recipients / 0.15 s.
