# V2 — resumable fan-out

**Evidence:** V1 all-or-nothing inserts do not belong in one transaction at large N (plan: 1 → 1k / 10k).

**Change:** if recipient count > `FANOUT_CHUNK` (default 200), persist `ntf_events` + `ntf_fanout` rows and let workers apply chunks with `SKIP LOCKED`. Kill mid-fan-out resumes. Partial inboxes are possible until `fanout_status=complete` — documented, not hidden.

Not added: Kafka, coalescing, APNs.
