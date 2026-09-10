# Interview guide

- Write fan-out vs read fan-out, with your `FANOUT_CHUNK` number
- Transactional outbox: why “DB then socket” drops messages
- Cursors, `cursor_expired`, retention
- At-least-once webhooks and consumer idempotency
- Head-of-line blocking: one slow subscriber vs a shared pool
- When Kafka is justified (it is not the V1 inbox)
