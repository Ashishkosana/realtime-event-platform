# Security

- No authentication on the HTTP adapter. Bind to localhost on a shared machine.
- Webhooks that are real HTTP carry HMAC header `x-notify-signature`. Fake transports (`fake://ok|fail|slow`, `ntf_lab_email`) are laboratory sinks, not a mail provider.
- `.env` is gitignored. `.env.example` uses local user `workflow` / password `workflow` — lab only.
- Request bodies capped (`MAX_BODY_BYTES`). SQL is parameterized.
- `DEMO_MODE` does not expose process-kill routes in this repo.
- Inbox payloads are whatever the publisher sent. Do not put production PII in the lab.
