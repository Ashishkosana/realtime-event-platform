# Decisions

1. SSE, not WebSocket — catch-up is an HTTP query, not a ping protocol.
2. Postgres is the log. NOTIFY is a wake, not the store.
3. At-least-once webhooks + event id. Receivers must dedup.
4. Mute is checked at fan-out. A mute racing an in-flight insert **may deliver one extra**. Defended: locking the preference row on every publish would serialize a hot user.
5. V2 chunked fan-out rather than Kafka. Kafka is for many independent consumers and long retention; we have one inbox consumer and a webhook consumer.
