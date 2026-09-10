# Failure drills

1. Duplicate idempotency key → one event, one inbox row per recipient.
2. Mute → no inbox row for that user; others still receive.
3. Poison webhook `fake://fail` → dead-letter after max attempts, not a tight loop.
4. Slow webhook vs email on two worker slots → email completes while slow sleeps.
5. Recipients > `FANOUT_CHUNK` → pending fan-out; chunk workers complete it (V2).
6. SSE reconnect with cursor → replay then live. Kill API after commit; reconnect still sees the inbox row.

Preference race: mute after inbox insert is too late; one extra notification is allowed.
